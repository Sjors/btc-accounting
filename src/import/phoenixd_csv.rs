use std::io::Read;
use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::DateTime;

use super::{TransactionSource, TxCategory, WalletTransaction};

/// Phoenixd CSV transaction source.
#[derive(Debug)]
pub struct PhoenixdCsv {
    records: Vec<CsvRecord>,
}

#[derive(Debug)]
struct CsvRecord {
    date: String,
    id: String,
    tx_type: String,
    amount_msat: i64,
    fee_credit_msat: i64,
    mining_fee_sat: i64,
    service_fee_msat: i64,
    payment_hash: String,
    tx_id: String,
}

impl PhoenixdCsv {
    pub fn from_path(path: &Path) -> Result<Self> {
        let mut file = std::fs::File::open(path)
            .with_context(|| format!("failed to open CSV file {}", path.display()))?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .with_context(|| format!("failed to read CSV file {}", path.display()))?;
        Self::from_str(&contents)
    }

    /// Accounting balance computed from raw msat values, rounded once.
    /// This avoids rounding drift from summing individually rounded sat values.
    /// Matches the accounting engine's semantics: credits are income, sends
    /// include fees in amount_msat, and channel-opening credit consumption is
    /// reflected via fee_credit_msat.
    pub fn wallet_balance_sats(&self) -> i64 {
        let total_msat: i64 = self.records.iter().map(|r| {
            if r.tx_type == "liquidity_purchase" {
                // Standalone fee: accounting debits gross fee, ignoring
                // amount_msat (which equals the fee total).
                -(r.mining_fee_sat * 1000 + r.service_fee_msat)
            } else {
                let is_channel_opening = r.tx_type == "lightning_received"
                    && (r.mining_fee_sat > 0 || r.service_fee_msat > 0);
                if is_channel_opening {
                    // Channel opening: account_balance = channel + fee_credit.
                    // amount_msat enters the channel, fee_credit_msat is
                    // consumed (negative).  Sum = account balance change.
                    r.amount_msat + r.fee_credit_msat
                } else {
                    // Non-channel: positive credit is income, negative
                    // credit doesn't occur (only on channel openings).
                    r.amount_msat + r.fee_credit_msat.max(0)
                }
            }
        }).sum();
        msat_to_sat(total_msat)
    }

    pub fn from_str(csv: &str) -> Result<Self> {
        let mut records = Vec::new();
        let mut lines = csv.lines();

        // Skip header
        let header = lines.next().context("empty CSV file")?;
        if !header.starts_with("date,") {
            bail!("unexpected CSV header: {header}");
        }

        for line in lines {
            if line.trim().is_empty() {
                continue;
            }
            let fields = parse_csv_line(line)?;
            if fields.len() < 9 {
                bail!("expected 9 CSV fields, got {}: {line}", fields.len());
            }

            records.push(CsvRecord {
                date: fields[0].clone(),
                id: fields[1].clone(),
                tx_type: fields[2].clone(),
                amount_msat: fields[3].parse::<i64>()
                    .with_context(|| format!("invalid amount_msat: {}", fields[3]))?,
                fee_credit_msat: fields[4].parse::<i64>()
                    .with_context(|| format!("invalid fee_credit_msat: {}", fields[4]))?,
                mining_fee_sat: fields[5].parse::<i64>()
                    .with_context(|| format!("invalid mining_fee_sat: {}", fields[5]))?,
                service_fee_msat: fields[6].parse::<i64>()
                    .with_context(|| format!("invalid service_fee_msat: {}", fields[6]))?,
                payment_hash: fields[7].clone(),
                tx_id: fields[8].clone(),
            });
        }

        Ok(Self { records })
    }
}

impl TransactionSource for PhoenixdCsv {
    fn list_transactions(&self) -> Result<Vec<WalletTransaction>> {
        let mut transactions = Vec::new();

        for rec in &self.records {
            // Only accept types we have test coverage for.
            match rec.tx_type.as_str() {
                "lightning_received" | "lightning_sent" | "swap_out"
                | "channel_close" | "liquidity_purchase" => {}
                other => bail!("unsupported phoenixd transaction type: {other}"),
            }

            let timestamp = DateTime::parse_from_rfc3339(&rec.date)
                .with_context(|| format!("invalid date: {}", rec.date))?
                .timestamp();

            // Channel opening: a lightning_received with on-chain fees is split
            // into a plain receive (zero fees) plus a liquidity purchase fee
            // entry.  When an automatic liquidity purchase coincides with a
            // lightning receive, phoenixd rolls the fees into this row instead
            // of emitting a separate liquidity_purchase row.
            let is_channel_opening = rec.tx_type == "lightning_received"
                && (rec.mining_fee_sat > 0 || rec.service_fee_msat > 0);

            if is_channel_opening {
                let payment_hash = if !rec.payment_hash.is_empty() {
                    Some(rec.payment_hash.clone())
                } else {
                    None
                };

                // account_balance = channel_balance + fee_credit.
                // Gross up by the net fee (= gross − consumed credits) to
                // reconstruct the amount the customer paid.  When credits
                // were consumed, some of that fee was pre-funded by earlier
                // fee-credit income; the gross fee debit implicitly "spends"
                // those credits.  If BTCPay detects a shortfall (because
                // phoenixd only credited amount_msat to the invoice) and the
                // customer pays the difference, that second payment appears
                // as a separate lightning_received row — both credits are
                // real income.
                let gross_fee_msat = rec.mining_fee_sat * 1000 + rec.service_fee_msat;
                let credit_consumed_msat = (-rec.fee_credit_msat).max(0);
                let net_fee_msat = (gross_fee_msat - credit_consumed_msat).max(0);
                let invoice_msat = rec.amount_msat + net_fee_msat;

                // 1. Lightning receive at the grossed-up amount.
                transactions.push(WalletTransaction {
                    txid: rec.id.clone(),
                    vout: 0,
                    amount_sats: msat_to_sat(invoice_msat),
                    fee_sats: None,
                    category: TxCategory::Receive,
                    block_time: timestamp,
                    block_height: 0,
                    block_hash: String::new(),
                    address: String::new(),
                    label: rec.tx_type.clone(),
                    payment_hash,
                    kind: super::TxKind::Default,
                });

                // 2. Liquidity purchase fee (gross).
                if let Some(fee_tx) = liquidity_fee_tx(rec, timestamp) {
                    transactions.push(fee_tx);
                }

                continue;
            }

            // Standalone liquidity purchase: amount_msat IS the fee total
            // (miningFee + serviceFee) per lightning-kmp's
            // `AutomaticLiquidityPurchasePayment.amount = fees`.
            // The purchased inbound capacity is not included.
            if rec.tx_type == "liquidity_purchase" {
                if let Some(fee_tx) = liquidity_fee_tx(rec, timestamp) {
                    transactions.push(fee_tx);
                }
                continue;
            }

            // Positive fee_credit_msat is income: credits are a coupon that
            // has real value (it will offset a future channel-opening fee).
            // Accounting balance tracks wallet sats + outstanding credits.
            let effective_amount_msat = rec.amount_msat + rec.fee_credit_msat.max(0);

            // Total fee in msat (mining fee is already in sat)
            let fee_msat = rec.mining_fee_sat * 1000 + rec.service_fee_msat;

            // For sends, phoenixd's amount_msat includes fees (e.g. a
            // 3_000_000 sat invoice with 12_004 sat routing fee is reported as
            // amount_msat = −3_012_004_000).  Strip the fee so the accounting
            // engine — which subtracts fee_sats separately — doesn't
            // double-count.
            let adjusted_msat = if effective_amount_msat < 0 && fee_msat > 0 {
                effective_amount_msat + fee_msat
            } else {
                effective_amount_msat
            };

            let amount_sats = msat_to_sat(adjusted_msat);
            let category = if amount_sats >= 0 {
                TxCategory::Receive
            } else {
                TxCategory::Send
            };

            // Total fee in sats (mining fee is already in sat, service fee is in msat)
            let total_fee_sats = rec.mining_fee_sat + msat_to_sat(rec.service_fee_msat);
            let fee_sats = if total_fee_sats != 0 {
                Some(total_fee_sats)
            } else {
                None
            };

            let label = rec.tx_type.clone();
            let payment_hash = if !rec.payment_hash.is_empty() {
                Some(rec.payment_hash.clone())
            } else {
                None
            };

            transactions.push(WalletTransaction {
                txid: rec.id.clone(),
                vout: 0,
                amount_sats,
                fee_sats,
                category,
                block_time: timestamp,
                block_height: 0,
                block_hash: String::new(),
                address: String::new(),
                label,
                payment_hash,
                kind: super::TxKind::Default,
            });
        }

        // Already chronological from phoenixd, but sort to be safe
        transactions.sort_by_key(|tx| tx.block_time);

        Ok(transactions)
    }
}

/// Build a `LiquidityPurchase` fee transaction from a CSV record's gross
/// mining and service fees.  Returns `None` when both fees are zero.
///
/// The fee is always gross (before fee-credit offset) because credits were
/// already booked as income on earlier receives.  Charging the gross fee
/// here implicitly "spends" those credits.
fn liquidity_fee_tx(rec: &CsvRecord, block_time: i64) -> Option<WalletTransaction> {
    let gross_fee_msat = rec.mining_fee_sat * 1000 + rec.service_fee_msat;
    let total_fee = msat_to_sat(gross_fee_msat);
    if total_fee == 0 {
        return None;
    }
    let mut parts = Vec::new();
    if rec.mining_fee_sat > 0 {
        parts.push(format!("mining fee ({} sat)", rec.mining_fee_sat));
    }
    let service_sats = msat_to_sat(rec.service_fee_msat);
    if service_sats > 0 {
        parts.push(format!("service fee ({service_sats} sat)"));
    }
    // Use tx_id when available (on-chain tx), otherwise derive from the row id.
    let txid = if rec.tx_id.is_empty() {
        format!("{}:fee", rec.id)
    } else {
        rec.tx_id.clone()
    };
    Some(WalletTransaction {
        txid,
        vout: 0,
        amount_sats: -total_fee,
        fee_sats: None,
        category: TxCategory::Send,
        block_time,
        block_height: 0,
        block_hash: String::new(),
        address: String::new(),
        label: String::new(),
        payment_hash: None,
        kind: super::TxKind::LiquidityPurchase {
            description: format!("Liquidity purchase {}", parts.join(" + ")),
        },
    })
}

/// Convert millisatoshis to satoshis, rounding toward zero.
fn msat_to_sat(msat: i64) -> i64 {
    if msat >= 0 {
        (msat + 500) / 1000
    } else {
        (msat - 500) / 1000
    }
}

/// Parse a single CSV line handling quoted fields.
fn parse_csv_line(line: &str) -> Result<Vec<String>> {
    let mut fields = Vec::new();
    let mut pos = 0;
    let bytes = line.as_bytes();

    while pos <= bytes.len() {
        if pos == bytes.len() {
            // Trailing comma produced an empty final field
            if !fields.is_empty() {
                fields.push(String::new());
            }
            break;
        }

        if bytes[pos] == b'"' {
            pos += 1;
            let mut field = String::new();
            while pos < bytes.len() {
                if bytes[pos] == b'"' {
                    pos += 1;
                    if pos < bytes.len() && bytes[pos] == b'"' {
                        field.push('"');
                        pos += 1;
                    } else {
                        break;
                    }
                } else {
                    field.push(bytes[pos] as char);
                    pos += 1;
                }
            }
            fields.push(field);
            if pos < bytes.len() && bytes[pos] == b',' {
                pos += 1;
            }
        } else {
            let start = pos;
            while pos < bytes.len() && bytes[pos] != b',' {
                pos += 1;
            }
            fields.push(String::from_utf8_lossy(&bytes[start..pos]).into_owned());
            if pos < bytes.len() {
                pos += 1; // skip comma
            } else {
                break;
            }
        }
    }

    Ok(fields)
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_CSV: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/phoenixd_sample.csv"
    ));

    #[test]
    fn parses_csv_records() {
        let source = PhoenixdCsv::from_str(SAMPLE_CSV).expect("parse CSV");
        let txs = source.list_transactions().expect("list transactions");

        // 7 CSV rows → 8 transactions
        // (channel-opening lightning_received splits into receive + fee entry)
        assert_eq!(txs.len(), 8);

        // Row 1: lightning_received  500_000 msat → 500 sat
        assert_eq!(txs[0].category, TxCategory::Receive);
        assert_eq!(txs[0].amount_sats, 500);
        assert!(txs[0].fee_sats.is_none());

        // Row 2: swap_out  -15_579_000 msat, mining fee 579 sat.
        // amount_msat includes the fee; strip it so the accounting engine
        // (which subtracts fee_sats) doesn't double-count.
        // Adjusted: -15_579_000 + 579_000 = -15_000_000 → -15000 sat.
        assert_eq!(txs[1].category, TxCategory::Send);
        assert_eq!(txs[1].amount_sats, -15000);
        assert_eq!(txs[1].fee_sats, Some(579));

        // Row 3: lightning_received with fee_credit_msat=503_000 but amount=0.
        // Credits are income: accounting balance = wallet sats + outstanding credits.
        assert_eq!(txs[2].category, TxCategory::Receive);
        assert_eq!(txs[2].amount_sats, 503);
        assert!(txs[2].fee_sats.is_none());

        // Row 4: channel_close  -308_594_000 msat → -308594 sat
        assert_eq!(txs[3].category, TxCategory::Send);
        assert_eq!(txs[3].amount_sats, -308594);
        assert!(txs[3].fee_sats.is_none());

        // Row 5: lightning_sent  -1_242_936 msat, service fee 8_936 msat.
        // Adjusted: -1_242_936 + 8_936 = -1_234_000 → -1234 sat, fee 9 sat.
        assert_eq!(txs[4].category, TxCategory::Send);
        assert_eq!(txs[4].amount_sats, -1234);
        assert_eq!(txs[4].fee_sats, Some(9));

        // Row 6: lightning_sent  -3_012_004_000 msat, service fee 12_004_000 msat.
        // Adjusted: -3_012_004_000 + 12_004_000 = -3_000_000_000 → -3000000 sat, fee 12004 sat.
        assert_eq!(txs[5].category, TxCategory::Send);
        assert_eq!(txs[5].amount_sats, -3000000);
        assert_eq!(txs[5].fee_sats, Some(12004));

        // Row 7: channel-opening lightning_received splits into receive + fee.
        // Gross up: amount_msat (300M) + net_fee (41364000) → 341364 sat.
        // Fee: gross 41364 sat.
        assert_eq!(txs[6].category, TxCategory::Receive);
        assert_eq!(txs[6].amount_sats, 341364);
        assert!(txs[6].fee_sats.is_none());
        assert_eq!(txs[6].label, "lightning_received");

        assert_eq!(txs[7].category, TxCategory::Send);
        assert_eq!(txs[7].amount_sats, -41364); // 18364 mining + 23000 service
        assert!(matches!(txs[7].kind, crate::import::TxKind::LiquidityPurchase { .. }));
    }

    #[test]
    fn msat_to_sat_rounds_correctly() {
        assert_eq!(msat_to_sat(1000), 1);
        assert_eq!(msat_to_sat(1499), 1);
        assert_eq!(msat_to_sat(1500), 2);
        assert_eq!(msat_to_sat(-1000), -1);
        assert_eq!(msat_to_sat(-1500), -2);
        assert_eq!(msat_to_sat(0), 0);
        assert_eq!(msat_to_sat(499), 0);
        assert_eq!(msat_to_sat(500), 1);
    }

    #[test]
    fn rejects_bad_header() {
        let csv = "wrong_header\n1,2,3";
        let err = PhoenixdCsv::from_str(csv).expect_err("should reject bad header");
        assert!(err.to_string().contains("unexpected CSV header"));
    }

    #[test]
    fn parses_quoted_csv_fields() {
        let fields = parse_csv_line(r#"hello,"world, ""quoted""",end"#).unwrap();
        assert_eq!(fields, vec!["hello", "world, \"quoted\"", "end"]);
    }

    #[test]
    fn labels_and_payment_hashes() {
        let source = PhoenixdCsv::from_str(SAMPLE_CSV).expect("parse CSV");
        let txs = source.list_transactions().expect("list transactions");

        assert_eq!(txs[0].label, "lightning_received");
        assert_eq!(txs[1].label, "swap_out");
        assert_eq!(txs[3].label, "channel_close");
        assert_eq!(txs[4].label, "lightning_sent");

        assert_eq!(txs[0].payment_hash.as_deref(), Some("462f75bff2bd054c7d1f28f3524bc6c5e1022f36369d4e0a35324674fd2b6922"));
        assert_eq!(txs[4].payment_hash.as_deref(), Some("f6ce6b8bb04a6639cb93d0ec5d3ed1eb33448e60ac91e82f90ff76fbe84f36e1"));
        assert!(txs[1].payment_hash.is_none()); // swap_out has no payment hash
    }

    #[test]
    fn rejects_legacy_types() {
        let csv = "date,id,type,amount_msat,fee_credit_msat,mining_fee_sat,service_fee_msat,payment_hash,tx_id\n\
                   2024-01-01T00:00:00.000Z,a,legacy_pay_to_open,1000,0,0,0,abc,\n";
        let err = PhoenixdCsv::from_str(csv).unwrap().list_transactions().expect_err("should reject legacy type");
        assert!(err.to_string().contains("unsupported phoenixd transaction type: legacy_pay_to_open"));
    }

    #[test]
    fn rejects_unknown_type() {
        let csv = "date,id,type,amount_msat,fee_credit_msat,mining_fee_sat,service_fee_msat,payment_hash,tx_id\n\
                   2024-01-01T00:00:00.000Z,a,fee_bumping,1000,0,0,0,,\n";
        let err = PhoenixdCsv::from_str(csv).unwrap().list_transactions().expect_err("should reject unknown type");
        assert!(err.to_string().contains("unsupported phoenixd transaction type: fee_bumping"));
    }

    /// Receives accumulate fee credits, a channel opening consumes them, then a
    /// send drains the balance to zero.  account_balance = channel + fee_credit.
    /// The receive is grossed up by the net fee (gross − credits consumed);
    /// the fee entry shows the gross fee.
    #[test]
    fn fee_credit_drain_to_zero() {
        // R1: receive 0 msat, earn 503_000 msat credit → 503 sat income.
        // R2: channel opening with amount_msat=300_000_000.
        //     Gross fee = 18_364 sat mining + 23_000_000 msat service = 41_364_000 msat.
        //     Credit consumed = 503_000 msat.  Net fee = 40_861_000 msat.
        //     Invoice (receive) = 300_000_000 + 40_861_000 = 340_861_000 → 340_861 sat.
        //     Fee = gross 41_364_000 → 41_364 sat.
        //     Account Δ = 340_861 − 41_364 = 299_497 sat.
        //     Accounting: 503 + 299_497 = 300_000.  ✓
        // S1: send -300_000_000 msat to drain to 0.
        let csv = "\
date,id,type,amount_msat,fee_credit_msat,mining_fee_sat,service_fee_msat,payment_hash,tx_id
2024-01-01T00:00:00.000Z,r1,lightning_received,0,503000,0,0,r1hash,
2024-01-02T00:00:00.000Z,r2,lightning_received,300000000,-503000,18364,23000000,r2hash,r2txid
2024-01-03T00:00:00.000Z,s1,lightning_sent,-300000000,0,0,0,s1hash,\n";

        let source = PhoenixdCsv::from_str(csv).unwrap();
        let txs = source.list_transactions().unwrap();

        // Accounting balance matches: credits earned and consumed cancel out.
        assert_eq!(source.wallet_balance_sats(), 0);

        // r1 → 503 sat (credit income)
        // r2 → 340_861 sat receive + 41_364 sat gross fee
        // s1 → −300_000 sat send
        let balance: i64 = txs.iter().map(|tx| {
            let fee = tx.fee_sats.unwrap_or(0).unsigned_abs() as i64;
            tx.amount_sats - if tx.category == TxCategory::Send { fee } else { 0 }
        }).sum();

        assert_eq!(balance, 0, "sat balance must be zero when the actual msat balance is zero");
    }

    /// When a channel opening coincides with fee-credit consumption and a
    /// BTCPay shortfall payment, both credits are real: the grossed-up
    /// receive shows what the customer paid for the first invoice, and the
    /// shortfall payment is genuine additional income.
    #[test]
    fn split_payment_channel_opening() {
        // P1: earlier payments earning fee credit = 13739 sat.
        // P2: channel-opening.  gross_fee = 1027*1000 + 21122000 = 22149000.
        //     credit consumed = 13739000.  net_fee = 8410000.
        //     receive = 3825000 + 8410000 = 12235000 → 12235 sat.
        //     fee = gross 22149000 → 22149 sat.
        // P3: BTCPay shortfall payment, plain receive 8410 sat.
        //     (BTCPay saw only 3825 credited, requested 8410 more.)
        // Accounting: 13739 + 12235 − 22149 + 8410 = 12235.  ✓
        let csv = "\
date,id,type,amount_msat,fee_credit_msat,mining_fee_sat,service_fee_msat,payment_hash,tx_id
2026-01-01T00:00:00.000Z,p1,lightning_received,0,13739000,0,0,hash_a,
2026-01-01T00:00:40.000Z,p2,lightning_received,3825000,-13739000,1027,21122000,hash_b,on_chain_tx
2026-01-01T00:01:20.000Z,p3,lightning_received,8410000,0,0,0,hash_c,\n";

        let source = PhoenixdCsv::from_str(csv).unwrap();
        let txs = source.list_transactions().unwrap();

        // P1: credit income 13739 sat.
        assert_eq!(txs[0].amount_sats, 13739);
        assert_eq!(txs[0].category, TxCategory::Receive);

        // P2 receive: grossed up 3825 + 8410 = 12235 sat.
        assert_eq!(txs[1].amount_sats, 12235);
        assert_eq!(txs[1].category, TxCategory::Receive);

        // P2 fee: gross 22149 sat.
        assert_eq!(txs[2].amount_sats, -22149);
        assert_eq!(txs[2].category, TxCategory::Send);
        assert!(matches!(txs[2].kind, crate::import::TxKind::LiquidityPurchase { .. }));

        // P3: BTCPay shortfall payment 8410 sat.
        assert_eq!(txs[3].amount_sats, 8410);
        assert_eq!(txs[3].category, TxCategory::Receive);

        // Total: 13739 + 12235 − 22149 + 8410 = 12235.
        assert_eq!(source.wallet_balance_sats(), 12235);
        let balance: i64 = txs.iter().map(|tx| {
            let fee = tx.fee_sats.unwrap_or(0).unsigned_abs() as i64;
            tx.amount_sats - if tx.category == TxCategory::Send { fee } else { 0 }
        }).sum();
        assert_eq!(balance, 12235, "accounting must match wallet balance");
    }
}
