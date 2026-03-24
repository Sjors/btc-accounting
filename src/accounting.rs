use std::collections::VecDeque;

use anyhow::{Result, Context, bail};
use chrono::{DateTime, Datelike, NaiveDate, Utc};

use crate::exchange_rate::ExchangeRateProvider;
use crate::export::{Entry, Statement, booking_date_to_date};
use crate::import::{TxCategory, TxKind, WalletTransaction};

/// Configuration for the accounting engine.
pub struct AccountingConfig {
    pub fiat_mode: bool,
    pub mark_to_market: bool,
    /// Use FIFO lot tracking: each receive creates a lot, each send consumes
    /// lots oldest-first and emits a realized gain/loss entry.
    pub fifo: bool,
    pub currency: String,
    pub account_iban: String,
    pub candle_interval_minutes: u32,
    /// Only include transactions on or after this date.
    pub start_date: Option<NaiveDate>,
    /// Opening balance in cents (fiat) or sats (BTC mode) — from previous export.
    pub opening_balance_cents: i64,
    /// Optional bank/institution name.
    pub bank_name: Option<String>,
    /// Current wallet balance in sats (from getbalance), for sanity checking.
    pub wallet_balance_sats: Option<i64>,
    /// If true, warn instead of error on forward/backward balance mismatch.
    pub ignore_balance_mismatch: bool,
}

/// A FIFO lot: tracks remaining satoshis and their cost basis.
struct Lot {
    remaining_sats: i64,
    /// Cost basis in cents for `remaining_sats`.
    cost_cents: i64,
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
    let mut last_year_end_done: Option<i32> = None;
    let mut computed_opening_balance: Option<i64> = None;
    let mut opening_sats: i64 = 0;
    let mut opening_rate: Option<f64> = None;
    let mut total_post_start_fees: i64 = 0;

    // FIFO lot queue: each receive pushes (remaining_sats, cost_cents_per_sat).
    // On send, lots are consumed from the front.
    let mut lots: VecDeque<Lot> = VecDeque::new();

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

    // Track which txids already had a fee entry (fees are per-txid, not per-vout)
    let mut fee_txids = std::collections::HashSet::new();

    for tx in transactions {
        if let Some(start) = config.start_date {
            let tx_date = timestamp_to_date(tx.block_time)?;
            if tx_date < start {
                // Accumulate pre-start balance (used as fallback when no getbalance available)
                balance_sats += tx.amount_sats;
                if let Some(fee) = tx.fee_sats {
                    if fee != 0 && fee_txids.insert(tx.txid.clone()) {
                        balance_sats -= fee.unsigned_abs() as i64;
                    }
                }
                continue;
            }
        }

        // Snapshot opening balance once, right before the first exported entry.
        // Use backwards-computed sats when available (more reliable), otherwise
        // fall back to the forward pre-start accumulation.
        if computed_opening_balance.is_none() {
            opening_sats = backwards_opening_sats.unwrap_or(balance_sats);
            // Sync balance_sats to the canonical opening value
            balance_sats = opening_sats;
            if config.fiat_mode && opening_sats != 0 {
                if let Some(start) = config.start_date {
                    let start_ts = start.and_hms_opt(0, 0, 0).unwrap().and_utc().timestamp();
                    let rate = provider.get_vwap(start_ts, config.candle_interval_minutes)
                        .context("failed to get rate at start_date for opening balance")?;
                    opening_rate = Some(rate);
                    balance_cents = config.opening_balance_cents + convert_to_cents(opening_sats, Some(rate));
                }
            }
            computed_opening_balance = Some(balance_cents);
        }

        // Mark-to-market at year-end boundaries
        if config.mark_to_market && config.fiat_mode {
            let tx_date = timestamp_to_date(tx.block_time)?;
            let tx_year = tx_date.year();

            // Check if we crossed a year boundary
            let check_year = match last_year_end_done {
                Some(y) => y + 1,
                None => {
                    if let Some(start) = config.start_date {
                        start.year()
                    } else {
                        tx_year
                    }
                }
            };

            for year in check_year..tx_year {
                if !is_future_year_end(year) {
                    let mtm_entry = mark_to_market_entry(
                        year,
                        balance_sats,
                        &mut balance_cents,
                        provider,
                        config.candle_interval_minutes,
                    )?;
                    if mtm_entry.amount_cents != 0 {
                        entries.push(mtm_entry);
                    }
                }
                last_year_end_done = Some(year);
            }
        }

        let booking_date = timestamp_to_date_string(tx.block_time)?;
        let rate_cents_per_btc = if config.fiat_mode {
            Some(provider.get_vwap(tx.block_time, config.candle_interval_minutes)
                .with_context(|| format!("failed to get rate for tx {} at {}", tx.txid, tx.block_time))?)
        } else {
            None
        };

        match tx.category {
            TxCategory::Receive => {
                balance_sats += tx.amount_sats;
                let amount_cents = convert_to_cents(tx.amount_sats, rate_cents_per_btc);
                balance_cents += amount_cents;

                if config.fifo && config.fiat_mode {
                    lots.push_back(Lot {
                        remaining_sats: tx.amount_sats,
                        cost_cents: amount_cents,
                    });
                }

                let entry_ref = format_entry_ref(tx.block_height, &tx.txid, tx.vout);
                let full_ref = format_full_ref(&tx.block_hash, &tx.txid, tx.vout);

                let label = if tx.label.is_empty() { &tx.address } else { &tx.label };
                let description = format_description(label, "Received", tx.amount_sats, rate_cents_per_btc);

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
                // amount_sats is negative for sends
                balance_sats += tx.amount_sats;
                let abs_sats = tx.amount_sats.unsigned_abs() as i64;
                let amount_cents = convert_to_cents(abs_sats, rate_cents_per_btc);
                balance_cents -= amount_cents;

                let entry_ref = format_entry_ref(tx.block_height, &tx.txid, tx.vout);
                let full_ref = format_full_ref(&tx.block_hash, &tx.txid, tx.vout);

                let label = if tx.label.is_empty() { &tx.address } else { &tx.label };
                let description = format_description(label, "Sent", abs_sats, rate_cents_per_btc);

                entries.push(Entry {
                    entry_ref,
                    full_ref,
                    booking_date: booking_date.clone(),
                    amount_cents,
                    is_credit: false,
                    description,
                    is_fee: false,
                });

                // FIFO: consume lots and emit realized gain/loss
                if config.fifo && config.fiat_mode {
                    let mut remaining = abs_sats;
                    let mut total_cost = 0i64;
                    while remaining > 0 {
                        let lot = lots.front_mut()
                            .context("FIFO: no lots available to cover send")?;
                        let consume = remaining.min(lot.remaining_sats);
                        // Proportional cost basis for the consumed portion
                        let lot_cost = if consume == lot.remaining_sats {
                            lot.cost_cents
                        } else {
                            (lot.cost_cents as f64 * consume as f64
                                / lot.remaining_sats as f64)
                                .round() as i64
                        };
                        total_cost += lot_cost;
                        lot.remaining_sats -= consume;
                        lot.cost_cents -= lot_cost;
                        remaining -= consume;
                        if lot.remaining_sats == 0 {
                            lots.pop_front();
                        }
                    }

                    let gain = amount_cents as i64 - total_cost;
                    if gain != 0 {
                        let (is_credit, gain_cents) = if gain > 0 {
                            (true, gain)
                        } else {
                            (false, -gain)
                        };
                        let gain_ref = format!(":fifo:{}:{}:{}", tx.block_height,
                            &tx.txid[..20.min(tx.txid.len())], tx.vout);
                        entries.push(Entry {
                            entry_ref: gain_ref.clone(),
                            full_ref: gain_ref,
                            booking_date: booking_date.clone(),
                            amount_cents: gain_cents,
                            is_credit,
                            description: format!("FIFO realized {} {}{:.2}",
                                if is_credit { "gain" } else { "loss" },
                                &config.currency,
                                gain_cents as f64 / 100.0),
                            is_fee: false,
                        });
                        // Adjust balance_cents: the send was booked at spot, but
                        // the cost basis differs. The gain/loss entry corrects
                        // balance_cents to reflect the actual cost consumed.
                        // After the send: balance_cents decreased by amount_cents (spot).
                        // It should have decreased by total_cost (cost basis).
                        // Difference = amount_cents - total_cost = gain.
                        // So we add the gain back (credit) or subtract (loss).
                        balance_cents += gain;
                    }
                }
            }
        }

        // Fee entry (only for sends, once per txid)
        if let Some(fee_sats) = tx.fee_sats {
            if fee_sats != 0 && fee_txids.insert(tx.txid.clone()) {
                let abs_fee = fee_sats.unsigned_abs() as i64;
                balance_sats -= abs_fee; // fees reduce balance
                total_post_start_fees += abs_fee;
                let fee_cents = convert_to_cents(abs_fee, rate_cents_per_btc);
                balance_cents -= fee_cents;

                // FIFO: fees also consume lots
                if config.fifo && config.fiat_mode {
                    let mut remaining = abs_fee;
                    let mut total_cost = 0i64;
                    while remaining > 0 {
                        let lot = lots.front_mut()
                            .context("FIFO: no lots available to cover fee")?;
                        let consume = remaining.min(lot.remaining_sats);
                        let lot_cost = if consume == lot.remaining_sats {
                            lot.cost_cents
                        } else {
                            (lot.cost_cents as f64 * consume as f64
                                / lot.remaining_sats as f64)
                                .round() as i64
                        };
                        total_cost += lot_cost;
                        lot.remaining_sats -= consume;
                        lot.cost_cents -= lot_cost;
                        remaining -= consume;
                        if lot.remaining_sats == 0 {
                            lots.pop_front();
                        }
                    }
                    // Adjust balance_cents: fee was booked at spot, correct for cost basis
                    let gain = fee_cents - total_cost;
                    if gain != 0 {
                        balance_cents += gain;
                    }
                }

                entries.push(Entry {
                    entry_ref: format!(":{}:{}:fee", tx.block_height, &tx.txid[..20.min(tx.txid.len())]),
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

    // Final mark-to-market for the last year
    if config.mark_to_market && config.fiat_mode {
        let last_entry_year = entries
            .last()
            .and_then(|e| NaiveDate::parse_from_str(booking_date_to_date(&e.booking_date), "%Y-%m-%d").ok())
            .map(|d| d.year());

        if let Some(year) = last_entry_year {
            let should_do = match last_year_end_done {
                Some(y) => year > y,
                None => true,
            };
            if should_do && !is_future_year_end(year) {
                let mtm_entry = mark_to_market_entry(
                    year,
                    balance_sats,
                    &mut balance_cents,
                    provider,
                    config.candle_interval_minutes,
                )?;
                if mtm_entry.amount_cents != 0 {
                    entries.push(mtm_entry);
                }
            }
        }
    }

    // Sanity check: verify that the forward replay of post-start transactions,
    // starting from the backwards-computed opening balance, equals getbalance.
    // A mismatch here indicates post-start transaction accounting issues.
    if let Some(wallet_sats) = config.wallet_balance_sats {
        if balance_sats != wallet_sats {
            let msg = format!(
                "balance inconsistency: replay from opening balance ({} sats) + \
                 post-start transactions gives {} sats, \
                 but getbalance reports {} sats \
                 (difference: {} sats, post-start fees: {} sats)",
                opening_sats, balance_sats, wallet_sats,
                balance_sats - wallet_sats,
                total_post_start_fees,
            );
            if !config.ignore_balance_mismatch {
                bail!("{msg}\n\nUse --ignore-balance-mismatch to proceed anyway.");
            }
            eprintln!("Warning: {msg}");
        }
    }

    let opening_date = config.start_date
        .map(|d| d.format("%Y-%m-%d").to_string())
        .or_else(|| entries.first().map(|e| booking_date_to_date(&e.booking_date).to_owned()))
        .unwrap_or_else(|| "1970-01-01".to_owned());

    let statement_date = entries
        .last()
        .map(|e| booking_date_to_date(&e.booking_date).to_owned())
        .unwrap_or_else(|| opening_date.clone());

    let statement_id = format!("STMT-{}", statement_date);

    // The opening balance reflects the wallet balance at start_date converted
    // at the rate on that date, or the closing balance of an existing file
    // (append mode, passed in via config.opening_balance_cents).
    let opening_balance_cents = computed_opening_balance.unwrap_or(balance_cents);

    Ok(Statement {
        account_iban: config.account_iban.clone(),
        currency: config.currency.clone(),
        opening_balance_cents,
        opening_balance_sats: opening_sats,
        opening_rate,
        entries,
        closing_balance_cents: balance_cents,
        closing_balance_sats: balance_sats,
        opening_date,
        statement_date,
        statement_id,
        bank_name: config.bank_name.clone(),
        descriptors: Vec::new(),
    })
}

/// Returns true if Dec 31 of the given year is still in the future.
fn is_future_year_end(year: i32) -> bool {
    let dec31 = NaiveDate::from_ymd_opt(year, 12, 31).expect("valid date");
    dec31 >= Utc::now().date_naive()
}

fn mark_to_market_entry(
    year: i32,
    balance_sats: i64,
    balance_cents: &mut i64,
    provider: &dyn ExchangeRateProvider,
    interval_minutes: u32,
) -> Result<Entry> {
    // Get rate at midnight CET on Jan 1 of the next year (= Dec 31 23:00 UTC)
    let year_end_midnight_cet = NaiveDate::from_ymd_opt(year + 1, 1, 1)
        .expect("valid date")
        .and_hms_opt(0, 0, 0)
        .expect("valid time");

    // CET is UTC+1
    let year_end_utc = year_end_midnight_cet.and_utc().timestamp() - 3600;

    let rate = provider.get_vwap(year_end_utc, interval_minutes)
        .with_context(|| format!("failed to get year-end rate for {year}"))?;

    let target_cents = sats_to_cents(balance_sats, rate);
    let adjustment = target_cents - *balance_cents;
    *balance_cents = target_cents;

    let (is_credit, amount_cents) = if adjustment >= 0 {
        (true, adjustment)
    } else {
        (false, -adjustment)
    };

    Ok(Entry {
        entry_ref: format!(":mtm:{year}-12-31"),
        full_ref: format!(":mtm:{year}-12-31"),
        booking_date: format!("{year}-12-31"),
        amount_cents,
        is_credit,
        description: format!("Year-end mark-to-market adjustment {year}"),
        is_fee: false,
    })
}

fn convert_to_cents(sats: i64, rate_cents_per_btc: Option<f64>) -> i64 {
    match rate_cents_per_btc {
        Some(rate) => sats_to_cents(sats, rate),
        None => sats, // In BTC mode, "cents" are actually satoshis
    }
}

/// Convert satoshis to fiat cents using a rate expressed as fiat units per BTC.
/// Rate is in fiat units (e.g., EUR), not cents. So we multiply by 100.
fn sats_to_cents(sats: i64, rate_per_btc: f64) -> i64 {
    // rate_per_btc is e.g. 95000.0 (EUR per BTC)
    // cents = sats * rate * 100 / 100_000_000
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
            payment_hash: None,
            kind: TxKind::Default,
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
            payment_hash: None,
            kind: TxKind::Default,
        }
    }

    #[test]
    fn receive_in_fiat_mode() {
        let provider = FixedRateProvider(95_000.0);
        let txs = vec![make_receive(5_000_000, 1_735_700_000, 100)];
        let config = AccountingConfig {
            fiat_mode: true,
            mark_to_market: false,
            fifo: false,
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
        // 5_000_000 sats * 95000 * 100 / 100_000_000 = 4_750_00 cents
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
            mark_to_market: false,
            fifo: false,
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

        // 3 entries: receive, send, fee
        assert_eq!(stmt.entries.len(), 3);
        assert!(stmt.entries[0].is_credit); // receive
        assert!(!stmt.entries[1].is_credit); // send
        assert!(!stmt.entries[2].is_credit); // fee
        assert!(stmt.entries[2].is_fee);
        // Fee: 1000 sats * 95000 * 100 / 100_000_000 = 95 cents
        assert_eq!(stmt.entries[2].amount_cents, 95);
    }

    #[test]
    fn btc_mode_uses_sats_directly() {
        let provider = FixedRateProvider(0.0); // should not be called
        let txs = vec![make_receive(5_000_000, 1_735_700_000, 100)];
        let config = AccountingConfig {
            fiat_mode: false,
            mark_to_market: false,
            fifo: false,
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
        assert_eq!(stmt.entries[0].amount_cents, 5_000_000); // sats directly
    }

    #[test]
    fn sats_to_cents_rounding() {
        // 1 sat at 95000 EUR/BTC = 0.095 cents → rounds to 0
        assert_eq!(sats_to_cents(1, 95_000.0), 0);
        // 100 sats at 95000 EUR/BTC = 9.5 cents → rounds to 10
        assert_eq!(sats_to_cents(100, 95_000.0), 10);
        // 100_000_000 sats (1 BTC) at 95000 = 9_500_000 cents
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

    /// When all BTC is sold before year-end, balance_sats is 0 but balance_cents
    /// may be non-zero due to rate differences. MTM should still fire to reconcile.
    #[test]
    fn mtm_fires_with_zero_btc_balance() {
        // Receive at 90k, send at 95k → fiat balance is negative (more deducted
        // than credited), so MTM needs to zero it out.
        struct TwoRateProvider;
        impl ExchangeRateProvider for TwoRateProvider {
            fn get_vwap(&self, ts: i64, _interval: u32) -> Result<f64> {
                if ts < 1_740_000_000 {
                    Ok(90_000.0) // receive rate
                } else {
                    Ok(95_000.0) // send + year-end rate
                }
            }
        }

        let txs = vec![
            // Receive 0.05 BTC on 2025-01-15
            make_receive(5_000_000, 1_736_899_200, 100),
            // Send all 0.05 BTC on 2025-03-15 (different txid needed)
            {
                let mut tx = make_send(5_000_000, 0, 1_742_025_600, 101);
                tx.txid = "ee".repeat(32);
                tx
            },
        ];

        let config = AccountingConfig {
            fiat_mode: true,
            mark_to_market: true,
            fifo: false,
            currency: "EUR".to_owned(),
            account_iban: "NL00XBTC0000000000".to_owned(),
            candle_interval_minutes: 1440,
            start_date: None,
            opening_balance_cents: 0,
            bank_name: None,
            wallet_balance_sats: None,
            ignore_balance_mismatch: false,
        };

        let provider = TwoRateProvider;
        let stmt = build_statement(&txs, &provider, &config).unwrap();

        // Receive: 5_000_000 * 90_000 * 100 / 100_000_000 = 450_000 cents (credit)
        // Send:    5_000_000 * 95_000 * 100 / 100_000_000 = 475_000 cents (debit)
        // balance_cents = 450_000 - 475_000 = -25_000
        // MTM target = 0 sats at 95k = 0 cents
        // adjustment = 0 - (-25_000) = 25_000 (credit)
        let mtm = stmt.entries.iter().find(|e| e.entry_ref.starts_with(":mtm:"));
        assert!(mtm.is_some(), "MTM entry should be generated even with zero BTC balance");
        let mtm = mtm.unwrap();
        assert_eq!(mtm.amount_cents, 25_000);
        assert!(mtm.is_credit);
        assert_eq!(stmt.closing_balance_cents, 0);
    }

    #[test]
    fn fifo_realized_gain() {
        // Receive at 90k, send at 95k → FIFO gain = spot - cost basis
        struct TwoRateProvider;
        impl ExchangeRateProvider for TwoRateProvider {
            fn get_vwap(&self, ts: i64, _interval: u32) -> Result<f64> {
                if ts < 1_740_000_000 {
                    Ok(90_000.0)
                } else {
                    Ok(95_000.0)
                }
            }
        }

        let txs = vec![
            make_receive(5_000_000, 1_736_899_200, 100),
            {
                let mut tx = make_send(5_000_000, 0, 1_742_025_600, 101);
                tx.txid = "ee".repeat(32);
                tx
            },
        ];

        let config = AccountingConfig {
            fiat_mode: true,
            mark_to_market: true,
            fifo: true,
            currency: "EUR".to_owned(),
            account_iban: "NL00XBTC0000000000".to_owned(),
            candle_interval_minutes: 1440,
            start_date: None,
            opening_balance_cents: 0,
            bank_name: None,
            wallet_balance_sats: None,
            ignore_balance_mismatch: false,
        };

        let provider = TwoRateProvider;
        let stmt = build_statement(&txs, &provider, &config).unwrap();

        // Receive: 5_000_000 * 90_000 * 100 / 100_000_000 = 450_000 cents
        // Send:    5_000_000 * 95_000 * 100 / 100_000_000 = 475_000 cents
        // FIFO gain: 475_000 - 450_000 = 25_000 cents (credit)
        let fifo = stmt.entries.iter().find(|e| e.entry_ref.starts_with(":fifo:"));
        assert!(fifo.is_some(), "FIFO entry should be generated");
        let fifo = fifo.unwrap();
        assert_eq!(fifo.amount_cents, 25_000);
        assert!(fifo.is_credit);
        assert!(fifo.description.contains("gain"));

        // With FIFO, balance_cents should be 0 after selling all at a gain,
        // because the gain entry corrected balance_cents.
        // MTM should then be 0 (no adjustment needed).
        let mtm = stmt.entries.iter().find(|e| e.entry_ref.starts_with(":mtm:"));
        assert!(mtm.is_none(), "MTM should not fire when FIFO already zeroed the balance");
        assert_eq!(stmt.closing_balance_cents, 0);
    }

    #[test]
    fn fifo_partial_lot_consumption() {
        // Receive 0.1 BTC at 90k, send 0.04 BTC at 95k → partial lot consumed
        struct TwoRateProvider;
        impl ExchangeRateProvider for TwoRateProvider {
            fn get_vwap(&self, ts: i64, _interval: u32) -> Result<f64> {
                if ts < 1_740_000_000 {
                    Ok(90_000.0)
                } else {
                    Ok(95_000.0)
                }
            }
        }

        let txs = vec![
            make_receive(10_000_000, 1_736_899_200, 100),
            {
                let mut tx = make_send(4_000_000, 0, 1_742_025_600, 101);
                tx.txid = "ee".repeat(32);
                tx
            },
        ];

        let config = AccountingConfig {
            fiat_mode: true,
            mark_to_market: true,
            fifo: true,
            currency: "EUR".to_owned(),
            account_iban: "NL00XBTC0000000000".to_owned(),
            candle_interval_minutes: 1440,
            start_date: None,
            opening_balance_cents: 0,
            bank_name: None,
            wallet_balance_sats: None,
            ignore_balance_mismatch: false,
        };

        let provider = TwoRateProvider;
        let stmt = build_statement(&txs, &provider, &config).unwrap();

        // Receive: 10_000_000 * 90_000 / 1e8 * 100 = 900_000 cents
        // Send:    4_000_000 * 95_000 / 1e8 * 100 = 380_000 cents (spot)
        // Cost basis of 4M sats from lot: 900_000 * 4/10 = 360_000
        // FIFO gain: 380_000 - 360_000 = 20_000
        let fifo = stmt.entries.iter().find(|e| e.entry_ref.starts_with(":fifo:")).unwrap();
        assert_eq!(fifo.amount_cents, 20_000);
        assert!(fifo.is_credit);

        // Remaining lot: 6_000_000 sats, cost = 900_000 - 360_000 = 540_000
        // balance_cents after send+fifo = 900_000 - 380_000 + 20_000 = 540_000
        // MTM: 6_000_000 * 95_000 / 1e8 * 100 = 570_000 target
        // adjustment = 570_000 - 540_000 = 30_000 (credit)
        let mtm = stmt.entries.iter().find(|e| e.entry_ref.starts_with(":mtm:")).unwrap();
        assert_eq!(mtm.amount_cents, 30_000);
        assert!(mtm.is_credit);
        assert_eq!(stmt.closing_balance_cents, 570_000);
    }

    #[test]
    fn fifo_multiple_lots() {
        // Two receives at different rates, one send consuming both
        struct MultiRateProvider;
        impl ExchangeRateProvider for MultiRateProvider {
            fn get_vwap(&self, ts: i64, _interval: u32) -> Result<f64> {
                if ts < 1_737_000_000 {
                    Ok(90_000.0) // first receive
                } else if ts < 1_738_000_000 {
                    Ok(100_000.0) // second receive
                } else {
                    Ok(95_000.0) // send + year-end
                }
            }
        }

        let txs = vec![
            { // receive 0.03 BTC at 90k
                let mut tx = make_receive(3_000_000, 1_736_500_000, 100);
                tx.txid = "a1".repeat(32);
                tx
            },
            { // receive 0.02 BTC at 100k
                let mut tx = make_receive(2_000_000, 1_737_500_000, 101);
                tx.txid = "a2".repeat(32);
                tx
            },
            { // send 0.04 BTC at 95k — consumes all of lot 1 + part of lot 2
                let mut tx = make_send(4_000_000, 0, 1_742_025_600, 102);
                tx.txid = "ee".repeat(32);
                tx
            },
        ];

        let config = AccountingConfig {
            fiat_mode: true,
            mark_to_market: true,
            fifo: true,
            currency: "EUR".to_owned(),
            account_iban: "NL00XBTC0000000000".to_owned(),
            candle_interval_minutes: 1440,
            start_date: None,
            opening_balance_cents: 0,
            bank_name: None,
            wallet_balance_sats: None,
            ignore_balance_mismatch: false,
        };

        let provider = MultiRateProvider;
        let stmt = build_statement(&txs, &provider, &config).unwrap();

        // Lot 1: 3_000_000 sats, cost = 3_000_000 * 90_000 / 1e8 * 100 = 270_000
        // Lot 2: 2_000_000 sats, cost = 2_000_000 * 100_000 / 1e8 * 100 = 200_000
        // Send 4_000_000 at 95k: spot = 4_000_000 * 95_000 / 1e8 * 100 = 380_000
        // Consumed: all of lot 1 (270_000) + 1_000_000 from lot 2 (cost = 200_000 * 1/2 = 100_000)
        // Total cost = 270_000 + 100_000 = 370_000
        // FIFO gain = 380_000 - 370_000 = 10_000
        let fifo = stmt.entries.iter().find(|e| e.entry_ref.starts_with(":fifo:")).unwrap();
        assert_eq!(fifo.amount_cents, 10_000);
        assert!(fifo.is_credit);

        // Remaining: 1_000_000 sats from lot 2, cost = 100_000
        // balance_cents = 270_000 + 200_000 - 380_000 + 10_000 = 100_000
        // MTM target = 1_000_000 * 95_000 / 1e8 * 100 = 95_000
        // adjustment = 95_000 - 100_000 = -5_000 (debit/loss)
        let mtm = stmt.entries.iter().find(|e| e.entry_ref.starts_with(":mtm:")).unwrap();
        assert_eq!(mtm.amount_cents, 5_000);
        assert!(!mtm.is_credit); // loss
        assert_eq!(stmt.closing_balance_cents, 95_000);
    }
}
