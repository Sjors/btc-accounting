use std::path::Path;

use anyhow::{Context, Result};
use btc_fiat_value::accounting::{AccountingConfig, build_statement};
use btc_fiat_value::export::camt053::Camt053Exporter;
use btc_fiat_value::exchange_rate::ExchangeRateProvider;
use btc_fiat_value::iban::iban_from_node_id;
use btc_fiat_value::import::TransactionSource;
use btc_fiat_value::import::phoenixd_csv::PhoenixdCsv;

/// ACINQ's public node ID — a real, publicly known Lightning node key used as
/// a stable test input for deterministic IBAN and XML generation.
const ACINQ_NODE_ID: &str =
    "03864ef025fde8fb587d989186ce6a4a186895ee44a926bfc370e2c366597a3f8f";

/// Daily real EUR/BTC rates for 2024 from tests/fixtures/rates_2024.json,
/// indexed by day-of-year.  Extracted from Kraken XXBTZEUR 1440-minute candles.
struct RateProvider2024 {
    rates: Vec<f64>,
}

impl RateProvider2024 {
    fn load() -> Result<Self> {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/rates_2024.json");
        let data = std::fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        let rates: Vec<f64> = serde_json::from_str(&data)?;
        Ok(Self { rates })
    }
}

impl ExchangeRateProvider for RateProvider2024 {
    fn get_vwap(&self, timestamp: i64, _interval_minutes: u32) -> Result<f64> {
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp, 0)
            .context("invalid timestamp")?;
        let start_of_2024 = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();
        let day_index = (dt.signed_duration_since(start_of_2024).num_days()).max(0) as usize;
        let rate = self.rates.get(day_index).copied()
            .with_context(|| format!("no rate for day index {day_index}"))?;
        Ok(rate)
    }
}

/// Export `phoenixd_sample.csv` to CAMT.053, persist the result as
/// `tests/fixtures/phoenixd_sample_camt053.xml`, then assert structural
/// invariants.  CI validates the committed file against the official XSD.
#[test]
fn phoenixd_csv_export_produces_valid_camt053() -> Result<()> {
    let csv_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/phoenixd_sample.csv");
    let csv = std::fs::read_to_string(&csv_path)?;

    let source = PhoenixdCsv::from_str(&csv)?;
    let balance_sats = source.wallet_balance_sats();
    let mut transactions = source.list_transactions()?;

    // Sort by block_time so the statement is in chronological order.
    transactions.sort_by_key(|tx| tx.block_time);

    let iban = iban_from_node_id(ACINQ_NODE_ID, "FR")?;

    let config = AccountingConfig {
        fiat_mode: true,
        mark_to_market: false,
        fifo: false,
        currency: "EUR".to_owned(),
        account_iban: iban,
        candle_interval_minutes: 1440,
        start_date: None,
        opening_balance_cents: 0,
        bank_name: Some("Phoenixd".to_owned()),
        wallet_balance_sats: Some(balance_sats),
        ignore_balance_mismatch: false,
        fee_threshold_cents: 1,
    };

    let statement = build_statement(&transactions, &RateProvider2024::load()?, &config)?;

    let mut output = Vec::new();
    btc_fiat_value::export::AccountingExporter::write(&Camt053Exporter, &statement, &mut output)?;
    let xml = String::from_utf8(output)?;

    // Persist so CI can run xmllint over it.
    let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/phoenixd_sample_camt053.xml");
    std::fs::write(&fixture_path, &xml)?;

    // Structural assertions.
    assert!(xml.contains("urn:iso:std:iso:20022:tech:xsd:camt.053.001.02"));
    assert!(xml.contains("<IBAN>"), "expected IBAN element");
    assert!(xml.contains("LNBT"), "expected LNBT bank code in IBAN");
    assert!(xml.contains("<Ccy>EUR</Ccy>"), "expected EUR currency");

    let entry_count = xml.matches("<Ntry>").count();
    assert!(entry_count > 0, "expected at least one entry");
    eprintln!("Generated {entry_count} entries; saved to {}", fixture_path.display());

    Ok(())
}
