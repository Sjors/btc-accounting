use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDate, Utc};

use crate::exchange_rate::ExchangeRateProvider;
use crate::export::{Entry, Statement, booking_date_to_date};
use crate::import::{TxCategory, WalletTransaction};

/// Configuration for the accounting engine.
pub struct AccountingConfig {
    pub fiat_mode: bool,
    pub currency: String,
    pub account_iban: String,
    pub candle_interval_minutes: u32,
    /// Only include transactions on or after this date.
    pub start_date: Option<NaiveDate>,
    /// Opening balance in cents (fiat) or sats (BTC mode) from a previous export.
    pub opening_balance_cents: i64,
    /// Optional bank/institution name.
    pub bank_name: Option<String>,
    /// Current wallet balance in sats (from getbalance), for sanity checking.
    pub wallet_balance_sats: Option<i64>,
    /// If true, warn instead of error on forward/backward balance mismatch.
    pub ignore_balance_mismatch: bool,
}

/// Build a Statement from wallet transactions.
pub fn build_statement(
    transactions: &[WalletTransaction],
    provider: &dyn ExchangeRateProvider,
    config: &AccountingConfig,
) -> Result<Statement> {
    let mut entries = Vec::new();
    let mut balance_sats: i64 = 0;
    let mut balance_cents: i64 = config.opening_balance_cents;
    let mut computed_opening_balance: Option<i64> = None;
    let mut opening_sats: i64 = 0;
    let mut opening_rate: Option<f64> = None;
    let mut total_post_start_fees: i64 = 0;

    // When getbalance is available, compute the opening balance backwards from it.
    // This is more reliable than forward replay of pre-start transactions, which can
    // be thrown off by RBF transactions, self-sends, and other wallet quirks.
    let backwards_opening_sats: Option<i64> = if let Some(wallet_sats) = config.wallet_balance_sats {
        let mut backwards_sats = wallet_sats;
        let mut back_fee_txids = std::collections::HashSet::new();
        for tx in transactions.iter().rev() {
            if let Some(start) = config.start_date {
                let tx_date = timestamp_to_date(tx.block_time)?;
                if tx_date < start {
                    break;
                }
            }
            backwards_sats -= tx.amount_sats;
            if let Some(fee) = tx.fee_sats {
                if fee != 0 && back_fee_txids.insert(tx.txid.clone()) {
                    backwards_sats += fee.unsigned_abs() as i64;
                }
            }
        }
        Some(backwards_sats)
    } else {
        None
    };

    // Track which txids already had a fee entry (fees are per-txid, not per-vout).
    let mut fee_txids = std::collections::HashSet::new();

    for tx in transactions {
        if let Some(start) = config.start_date {
            let tx_date = timestamp_to_date(tx.block_time)?;
            if tx_date < start {
                balance_sats += tx.amount_sats;
                if let Some(fee) = tx.fee_sats {
                    if fee != 0 && fee_txids.insert(tx.txid.clone()) {
                        balance_sats -= fee.unsigned_abs() as i64;
                    }
                }
                continue;
            }
        }

        if computed_opening_balance.is_none() {
            opening_sats = backwards_opening_sats.unwrap_or(balance_sats);
            balance_sats = opening_sats;
            if config.fiat_mode && opening_sats != 0 {
                if let Some(start) = config.start_date {
                    let start_ts = start.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
                    let rate = provider
                        .get_vwap(start_ts, config.candle_interval_minutes)
                        .context("failed to get rate at start_date for opening balance")?;
                    opening_rate = Some(rate);
                    balance_cents =
                        config.opening_balance_cents + convert_to_cents(opening_sats, Some(rate));
                }
            }
            computed_opening_balance = Some(balance_cents);
        }

        let booking_date = timestamp_to_date_string(tx.block_time)?;
        let rate_cents_per_btc = if config.fiat_mode {
            Some(
                provider
                    .get_vwap(tx.block_time, config.candle_interval_minutes)
                    .with_context(|| {
                        format!("failed to get rate for tx {} at {}", tx.txid, tx.block_time)
                    })?,
            )
        } else {
            None
        };

        match tx.category {
            TxCategory::Receive => {
                balance_sats += tx.amount_sats;
                let amount_cents = convert_to_cents(tx.amount_sats, rate_cents_per_btc);
                balance_cents += amount_cents;

                let entry_ref = format_entry_ref(tx.block_height, &tx.txid, tx.vout);
                let full_ref = format_full_ref(&tx.block_hash, &tx.txid, tx.vout);
                let label = if tx.label.is_empty() { &tx.address } else { &tx.label };
                let description =
                    format_description(label, "Received", tx.amount_sats, rate_cents_per_btc);

                entries.push(Entry {
                    entry_ref,
                    full_ref,
                    booking_date: booking_date.clone(),
                    amount_cents,
                    is_credit: true,
                    description,
                    is_fee: false,
                });
            }
            TxCategory::Send => {
                balance_sats += tx.amount_sats;
                let abs_sats = tx.amount_sats.unsigned_abs() as i64;
                let amount_cents = convert_to_cents(abs_sats, rate_cents_per_btc);
                balance_cents -= amount_cents;

                let entry_ref = format_entry_ref(tx.block_height, &tx.txid, tx.vout);
                let full_ref = format_full_ref(&tx.block_hash, &tx.txid, tx.vout);
                let label = if tx.label.is_empty() { &tx.address } else { &tx.label };
                let description =
                    format_description(label, "Sent", abs_sats, rate_cents_per_btc);

                entries.push(Entry {
                    entry_ref,
                    full_ref,
                    booking_date: booking_date.clone(),
                    amount_cents,
                    is_credit: false,
                    description,
                    is_fee: false,
                });
            }
        }

        if let Some(fee_sats) = tx.fee_sats {
            if fee_sats != 0 && fee_txids.insert(tx.txid.clone()) {
                let abs_fee = fee_sats.unsigned_abs() as i64;
                balance_sats -= abs_fee;
                total_post_start_fees += abs_fee;
                let fee_cents = convert_to_cents(abs_fee, rate_cents_per_btc);
                balance_cents -= fee_cents;

                entries.push(Entry {
                    entry_ref: format!(
                        ":{}:{}:fee",
                        tx.block_height,
                        &tx.txid[..20.min(tx.txid.len())]
                    ),
                    full_ref: format!(":{}:{}:fee", tx.block_hash, tx.txid),
                    booking_date,
                    amount_cents: fee_cents,
                    is_credit: false,
                    description: format!("Mining fee ({} sat)", abs_fee),
                    is_fee: true,
                });
            }
        }
    }

    if let Some(wallet_sats) = config.wallet_balance_sats {
        if balance_sats != wallet_sats {
            let msg = format!(
                "balance inconsistency: replay from opening balance ({} sats) + \
                 post-start transactions gives {} sats, \
                 but getbalance reports {} sats \
                 (difference: {} sats, post-start fees: {} sats)",
                opening_sats,
                balance_sats,
                wallet_sats,
                balance_sats - wallet_sats,
                total_post_start_fees,
            );
            if !config.ignore_balance_mismatch {
                bail!("{msg}\n\nUse --ignore-balance-mismatch to proceed anyway.");
            }
            eprintln!("Warning: {msg}");
        }
    }

    let opening_date = config
        .start_date
        .map(|d| d.format("%Y-%m-%d").to_string())
        .or_else(|| entries.first().map(|e| booking_date_to_date(&e.booking_date).to_owned()))
        .unwrap_or_else(|| "1970-01-01".to_owned());

    let statement_date = entries
        .last()
        .map(|e| booking_date_to_date(&e.booking_date).to_owned())
        .unwrap_or_else(|| opening_date.clone());

    let statement_id = format!("STMT-{statement_date}");
    let opening_balance_cents = computed_opening_balance.unwrap_or(balance_cents);

    Ok(Statement {
        account_iban: config.account_iban.clone(),
        currency: config.currency.clone(),
        opening_balance_cents,
        opening_balance_sats: opening_sats,
        opening_rate,
        entries,
        closing_balance_cents: balance_cents,
        opening_date,
        statement_date,
        statement_id,
        bank_name: config.bank_name.clone(),
        descriptors: Vec::new(),
    })
}

fn convert_to_cents(sats: i64, rate_cents_per_btc: Option<f64>) -> i64 {
    match rate_cents_per_btc {
        Some(rate) => sats_to_cents(sats, rate),
        None => sats,
    }
}

/// Convert satoshis to fiat cents using a rate expressed as fiat units per BTC.
fn sats_to_cents(sats: i64, rate_per_btc: f64) -> i64 {
    let cents_f64 = (sats as f64) * rate_per_btc * 100.0 / 100_000_000.0;
    cents_f64.round() as i64
}

fn format_entry_ref(height: u32, txid: &str, vout: u32) -> String {
    let prefix_len = 20.min(txid.len());
    format!("{height}:{}:{vout}", &txid[..prefix_len])
}

fn format_full_ref(block_hash: &str, txid: &str, vout: u32) -> String {
    format!("{block_hash}:{txid}:{vout}")
}

fn format_description(label: &str, verb: &str, sats: i64, rate_cents_per_btc: Option<f64>) -> String {
    let btc = sats as f64 / 100_000_000.0;
    match rate_cents_per_btc {
        Some(rate) => format!("{label} - {verb} {btc:.8} BTC @ {rate:.2}"),
        None => format!("{label} - {verb} {btc:.8} BTC"),
    }
}

fn timestamp_to_date(timestamp: i64) -> Result<NaiveDate> {
    let dt = DateTime::<Utc>::from_timestamp(timestamp, 0)
        .with_context(|| format!("invalid timestamp {timestamp}"))?;
    Ok(dt.date_naive())
}

fn timestamp_to_date_string(timestamp: i64) -> Result<String> {
    let dt = DateTime::<Utc>::from_timestamp(timestamp, 0)
        .with_context(|| format!("invalid timestamp {timestamp}"))?;
    Ok(dt.naive_utc().format("%Y-%m-%dT%H:%M:%S").to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange_rate::ExchangeRateProvider;

    struct FixedRateProvider(f64);
    impl ExchangeRateProvider for FixedRateProvider {
        fn get_vwap(&self, _ts: i64, _interval: u32) -> Result<f64> {
            Ok(self.0)
        }
    }

    fn make_receive(sats: i64, block_time: i64, height: u32) -> WalletTransaction {
        WalletTransaction {
            txid: "aa".repeat(32),
            vout: 0,
            amount_sats: sats,
            fee_sats: None,
            category: TxCategory::Receive,
            block_time,
            block_height: height,
            block_hash: "bb".repeat(32),
            address: "bc1qtest".to_owned(),
            label: String::new(),
        }
    }

    fn make_send(sats: i64, fee: i64, block_time: i64, height: u32) -> WalletTransaction {
        WalletTransaction {
            txid: "cc".repeat(32),
            vout: 0,
            amount_sats: -sats,
            fee_sats: Some(-fee),
            category: TxCategory::Send,
            block_time,
            block_height: height,
            block_hash: "dd".repeat(32),
            address: "bc1qother".to_owned(),
            label: String::new(),
        }
    }

    #[test]
    fn receive_in_fiat_mode() {
        let provider = FixedRateProvider(95_000.0);
        let txs = vec![make_receive(5_000_000, 1_735_700_000, 100)];
        let config = AccountingConfig {
            fiat_mode: true,
            currency: "EUR".to_owned(),
            account_iban: "NL00XBTC0000000000".to_owned(),
            candle_interval_minutes: 1440,
            start_date: None,
            opening_balance_cents: 0,
            bank_name: None,
            wallet_balance_sats: None,
            ignore_balance_mismatch: false,
        };

        let stmt = build_statement(&txs, &provider, &config).unwrap();

        assert_eq!(stmt.entries.len(), 1);
        assert!(stmt.entries[0].is_credit);
        assert_eq!(stmt.entries[0].amount_cents, 475_000);
        assert_eq!(stmt.closing_balance_cents, 475_000);
    }

    #[test]
    fn send_with_fee_in_fiat_mode() {
        let provider = FixedRateProvider(95_000.0);
        let txs = vec![
            make_receive(10_000_000, 1_735_700_000, 100),
            make_send(5_000_000, 1_000, 1_735_800_000, 101),
        ];
        let config = AccountingConfig {
            fiat_mode: true,
            currency: "EUR".to_owned(),
            account_iban: "NL00XBTC0000000000".to_owned(),
            candle_interval_minutes: 1440,
            start_date: None,
            opening_balance_cents: 0,
            bank_name: None,
            wallet_balance_sats: None,
            ignore_balance_mismatch: false,
        };

        let stmt = build_statement(&txs, &provider, &config).unwrap();

        assert_eq!(stmt.entries.len(), 3);
        assert!(stmt.entries[0].is_credit);
        assert!(!stmt.entries[1].is_credit);
        assert!(!stmt.entries[2].is_credit);
        assert!(stmt.entries[2].is_fee);
        assert_eq!(stmt.entries[2].amount_cents, 95);
    }

    #[test]
    fn btc_mode_uses_sats_directly() {
        let provider = FixedRateProvider(0.0);
        let txs = vec![make_receive(5_000_000, 1_735_700_000, 100)];
        let config = AccountingConfig {
            fiat_mode: false,
            currency: "BTC".to_owned(),
            account_iban: "NL00XBTC0000000000".to_owned(),
            candle_interval_minutes: 1440,
            start_date: None,
            opening_balance_cents: 0,
            bank_name: None,
            wallet_balance_sats: None,
            ignore_balance_mismatch: false,
        };

        let stmt = build_statement(&txs, &provider, &config).unwrap();
        assert_eq!(stmt.entries[0].amount_cents, 5_000_000);
    }

    #[test]
    fn sats_to_cents_rounding() {
        assert_eq!(sats_to_cents(1, 95_000.0), 0);
        assert_eq!(sats_to_cents(100, 95_000.0), 10);
        assert_eq!(sats_to_cents(100_000_000, 95_000.0), 9_500_000);
    }

    #[test]
    fn entry_ref_format() {
        let txid = "aabbccddee11223344556677889900aabbccddee11223344556677889900aabb";
        assert_eq!(
            format_entry_ref(100, txid, 0),
            "100:aabbccddee1122334455:0"
        );
    }
}
