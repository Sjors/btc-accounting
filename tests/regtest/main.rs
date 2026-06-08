mod ipc_mining;
mod node;
mod wallet;

use anyhow::{Context, Result};
use btc_fiat_value::import::{TransactionSource, TxCategory, TxKind, WalletTransaction};
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use std::path::Path;
use std::process::Command;

use ipc_mining::{load_coinbase_cache, save_coinbase_cache};
use node::RegtestNode;
use wallet::RpcWallet;

/// Fixed seed for reproducible test scenarios.
const RNG_SEED: u64 = 20250101;

/// Number of months in the salary scenario.
const MONTHS: usize = 12;

/// Salary in EUR cents (€5,000 = 500_000 cents).
const SALARY_CENTS: i64 = 500_000;

/// Fixed tprv keys for deterministic wallets (regtest/testnet).
const MINING_TPRV: &str = "tprv8ZgxMBicQKsPfHCsTwkiM1KT56RXbGGTqvc2hgqzycpwbHqqpcajQeMRZoBD35kW4RtyCemu6j34Ku5DEspmgjKdt2qe4SvRch5Kk8B8A2v";
const ACCOUNTING_TPRV: &str = "tprv8ZgxMBicQKsPd7Uf69XL1XwhmjHopUGep8GuEiJDZmbQz6o58LninorQAfcKZWARbtRtfnLcJ5MQ2AtHcQJCCRUcMRvmDUjyEmNUWwx8UbK";

#[test]
fn salary_scenario_2025() {
    let result = run_salary_scenario();
    if let Err(e) = result {
        panic!("salary scenario failed: {e:#}");
    }
}

fn run_salary_scenario() -> Result<()> {
    // Find pre-built bitcoin wrapper (multiprocess with IPC)
    let bitcoin_path = node::find_bitcoin()?;
    eprintln!("Using bitcoin at: {}", bitcoin_path.display());

    // Load mock rates
    let mock_rates = load_mock_rates()?;
    eprintln!("Loaded {} daily mock rates", mock_rates.len());

    // Load coinbase cache for deterministic blocks
    let cache_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("coinbase_cache.json");
    let mut coinbase_cache = load_coinbase_cache(&cache_path)?;
    let initial_cache_size = coinbase_cache.len();
    eprintln!("Loaded {initial_cache_size} cached coinbase solutions");

    // Tokio runtime for IPC calls
    let rt = tokio::runtime::Runtime::new()?;

    // Start regtest node at 2024-12-30 00:00:00 UTC
    let initial_time = chrono::NaiveDate::from_ymd_opt(2024, 12, 30)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp();

    let mut node = RegtestNode::start(&bitcoin_path, initial_time)?;
    eprintln!("Started regtest node on port {}", node.rpc_port());

    // Create deterministic wallets with fixed tprv keys
    let mining = RpcWallet::create_deterministic(&node, "mining", MINING_TPRV)?;
    let accounting = RpcWallet::create_deterministic(&node, "accounting", ACCOUNTING_TPRV)?;
    eprintln!("Created deterministic wallets: mining, accounting");

    // Generate initial blocks for maturity (100-block coinbase maturity + 1)
    let mining_addr = mining.get_new_address()?;
    let all_cached =
        mining.mine_blocks_ipc(101, &mining_addr, "maturity-", &mut coinbase_cache, &rt)?;
    if !all_cached {
        eprintln!("⚠️  Cache miss during maturity blocks — output may not be deterministic");
    }
    eprintln!("Mined 101 blocks to mining wallet");

    // Send initial 0.001 BTC to accounting wallet as seed
    let accounting_addr = accounting.get_new_address()?;
    mining.send_to_address(&accounting_addr, 100_000)?; // 0.001 BTC = 100,000 sats
    let all_cached = mining.mine_blocks_ipc(1, &mining_addr, "seed-", &mut coinbase_cache, &rt)?;
    if !all_cached {
        eprintln!("⚠️  Cache miss during seed block — output may not be deterministic");
    }
    eprintln!("Sent initial 0.001 BTC to accounting wallet");

    // Monthly salary scenario with seeded RNG
    let mut rng = StdRng::seed_from_u64(RNG_SEED);

    for month in 1..=MONTHS {
        // Set mocktime to first workday of month, 12:00 CET (= 11:00 UTC)
        // January starts on the 2nd (New Year's Day is a holiday)
        let day = if month == 1 { 2 } else { 1 };
        let first_day = chrono::NaiveDate::from_ymd_opt(2025, month as u32, day).unwrap();
        let first_workday = skip_to_workday(first_day);
        let mock_time = first_workday
            .and_hms_opt(11, 0, 0) // 11:00 UTC = 12:00 CET
            .unwrap()
            .and_utc()
            .timestamp();

        node.set_mocktime(mock_time)?;

        // Get rate for this day
        let day_index = first_workday
            .signed_duration_since(chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap())
            .num_days() as usize;

        let rate = mock_rates.get(day_index).copied().unwrap_or(95_000.0);

        // Calculate BTC amount for €5,000 salary: sats = salary_cents * 100_000_000 / (rate * 100)
        let salary_sats = (SALARY_CENTS as f64 * 100_000_000.0 / (rate * 100.0)).round() as i64;

        let accounting_addr = accounting.get_new_address()?;
        let month_name = month_name(month);
        // Deliberately skip labels for months 4 (April) and 8 (August)
        if month != 4 && month != 8 {
            accounting.set_label(&accounting_addr, &format!("Salary {month_name}"))?;
        }
        mining.send_to_address(&accounting_addr, salary_sats)?;
        let label = format!("salary-{month}-");
        let all_cached =
            mining.mine_blocks_ipc(1, &mining_addr, &label, &mut coinbase_cache, &rt)?;
        if !all_cached {
            eprintln!(
                "⚠️  Cache miss at month {month} salary block — output may not be deterministic"
            );
        }
        eprintln!("Month {month}: Received {salary_sats} sats (€5,000 at rate {rate:.2})");

        // Random delay before spending (0-5 days), during daytime CET (14:00-20:00 CET = 13:00-19:00 UTC)
        let delay_days: i64 = rng.random_range(0..=5);
        let spend_hour: i64 = rng.random_range(13..=19); // 13:00-19:00 UTC = 14:00-20:00 CET
        let spend_time = mock_time + delay_days * 86_400 - 11 * 3600 + spend_hour * 3600;
        node.set_mocktime(spend_time)?;

        // Random spend percentage (60-90%)
        let spend_pct: f64 = rng.random_range(60..=90) as f64 / 100.0;

        // Get balance and spend
        let balance_sats = accounting.get_balance()?;
        let spend_sats = (balance_sats as f64 * spend_pct).round() as i64;

        if spend_sats > 546 {
            // above dust
            let mining_addr_spend = mining.get_new_address()?;
            // Label 10 of 12 send destinations "Exchange" (skip months 3 and 7)
            if month != 3 && month != 7 {
                accounting.set_label(&mining_addr_spend, "Exchange")?;
            }
            accounting.send_to_address(&mining_addr_spend, spend_sats)?;
            let label = format!("spend-{month}-");
            let all_cached =
                mining.mine_blocks_ipc(1, &mining_addr, &label, &mut coinbase_cache, &rt)?;
            if !all_cached {
                eprintln!(
                    "⚠️  Cache miss at month {month} spend block — output may not be deterministic"
                );
            }
            eprintln!(
                "  Spent {spend_sats} sats ({:.0}%) after {delay_days} day(s)",
                spend_pct * 100.0
            );
        }
    }

    // Save coinbase cache if it grew
    if coinbase_cache.len() > initial_cache_size {
        save_coinbase_cache(&cache_path, &coinbase_cache)?;
        eprintln!(
            "Saved {} coinbase solutions ({} new)",
            coinbase_cache.len(),
            coinbase_cache.len() - initial_cache_size
        );
    }

    // Get fingerprint for IBAN
    let fingerprint = accounting.get_fingerprint()?;
    eprintln!("Accounting wallet fingerprint: {fingerprint}");

    // Export with mock exchange rates
    let transactions = accounting.list_transactions()?;
    eprintln!("Listed {} transactions", transactions.len());

    // Collect receive addresses and fetch matching watch-only descriptors
    let receive_addresses: std::collections::HashSet<String> = transactions
        .iter()
        .filter(|tx| tx.category == btc_fiat_value::import::TxCategory::Receive)
        .map(|tx| tx.address.clone())
        .collect();
    let descriptors = accounting.get_receive_descriptors(&receive_addresses)?;
    eprintln!("Found {} receive descriptor(s)", descriptors.len());

    let mock_provider = MockRateProvider {
        rates: mock_rates.clone(),
    };
    let iban = btc_fiat_value::iban::iban_from_fingerprint(&fingerprint, "NL", "regtest")?;
    eprintln!("Generated IBAN: {iban}");

    let config = btc_fiat_value::accounting::AccountingConfig {
        fiat_mode: true,
        mark_to_market: true,
        fifo: true,
        currency: "EUR".to_owned(),
        account_iban: iban,
        candle_interval_minutes: 1440,
        start_date: Some(chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap()),
        opening_balance_cents: 0,
        bank_name: Some("Bitcoin Core - accounting".to_owned()),
        wallet_balance_sats: None,
        ignore_balance_mismatch: false,
        fee_threshold_cents: 1,
    };

    let mut statement =
        btc_fiat_value::accounting::build_statement(&transactions, &mock_provider, &config)?;
    statement.descriptors = descriptors;

    // Write CAMT.053 output
    let mut output = Vec::new();
    let exporter = btc_fiat_value::export::camt053::Camt053Exporter;
    btc_fiat_value::export::AccountingExporter::write(&exporter, &statement, &mut output)?;

    let xml = String::from_utf8(output)?;
    eprintln!("\nGenerated CAMT.053 ({} bytes):", xml.len());

    // Save to file for manual inspection
    let output_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("salary_2025_camt053.xml");
    std::fs::write(&output_path, &xml)
        .with_context(|| format!("failed to write {}", output_path.display()))?;
    eprintln!("Saved to {}", output_path.display());

    // Verify structure
    assert!(xml.contains("urn:iso:std:iso:20022:tech:xsd:camt.053.001.02"));
    assert!(xml.contains("<IBAN>"));
    assert!(xml.contains("<Ccy>EUR</Ccy>"));

    // Count entries
    let entry_count = xml.matches("<Ntry>").count();
    eprintln!("  {entry_count} entries (receives + sends + fees + mtm)");
    assert!(entry_count > 0, "expected at least one entry");

    // Verify mark-to-market entry exists
    assert!(
        xml.contains(":mtm:2025-12-31"),
        "expected mark-to-market entry"
    );

    // Verify watch-only descriptors are included as comments
    assert!(
        xml.contains("Watch-only descriptors"),
        "expected descriptor comments"
    );
    assert!(xml.contains("tpub"), "expected tpub in descriptor comments");

    // Verify BTC opening balance comment includes rate, and sanity-check the fiat value
    let balance_comment = xml
        .lines()
        .find(|l| l.contains("BTC opening balance"))
        .expect("expected BTC opening balance comment");
    assert!(
        balance_comment.contains(" @ "),
        "expected rate in BTC opening balance comment"
    );
    // Parse: "<!-- BTC opening balance: 0.00100000 BTC @ 95277.13 -->"
    let after_colon = balance_comment.split(':').last().unwrap().trim();
    let parts: Vec<&str> = after_colon.split(" @ ").collect();
    let btc: f64 = parts[0]
        .trim()
        .split_whitespace()
        .next()
        .unwrap()
        .parse()
        .unwrap();
    let rate: f64 = parts[1]
        .trim()
        .trim_end_matches("-->")
        .trim()
        .parse()
        .unwrap();
    let fiat_from_comment = btc * rate;
    let opening_cents = statement.opening_balance_cents;
    let opening_fiat = opening_cents as f64 / 100.0;
    assert!(
        (fiat_from_comment - opening_fiat).abs() < 0.02,
        "BTC opening balance comment fiat mismatch: {btc} BTC @ {rate} = {fiat_from_comment:.2}, \
         but opening balance is {opening_fiat:.2}"
    );

    // Verify reconstruction: check that non-virtual entry refs contain valid blockhash:txid:vout
    verify_reconstruction(&xml, &node)?;

    // Roundtrip: create watch-only wallet from embedded descriptors, verify all txs match
    verify_roundtrip(&xml, &node)?;

    let bitcoin_wallet_path = node::find_bitcoin_wallet(&bitcoin_path)?;
    let labels_path = node.datadir().join("accounting-bip329.jsonl");
    node.stop()?;

    export_bitcoin_core_bip329(
        &bitcoin_wallet_path,
        node.datadir(),
        "accounting",
        &labels_path,
    )?;
    let bip329_source =
        btc_fiat_value::import::bitcoin_core_bip329::BitcoinCoreBip329::from_path(&labels_path)?;
    eprintln!(
        "Exported Bitcoin Core BIP329 JSONL with {} descriptor(s)",
        bip329_source.descriptors().len()
    );

    assert_eq!(
        bip329_source.fingerprint(),
        fingerprint,
        "BIP329 export fingerprint should match RPC descriptor fingerprint"
    );

    let bip329_transactions = bip329_source.list_transactions()?;
    assert_wallet_transactions_equivalent(&transactions, &bip329_transactions);

    let bip329_provider = MockRateProvider {
        rates: mock_rates.clone(),
    };
    let mut bip329_statement = btc_fiat_value::accounting::build_statement(
        &bip329_transactions,
        &bip329_provider,
        &config,
    )?;
    bip329_statement.descriptors = bip329_source.descriptors().to_vec();
    assert!(
        !bip329_statement.descriptors.is_empty(),
        "BIP329 export should include descriptors"
    );
    assert_statement_accounting_equivalent(&statement, &bip329_statement);

    eprintln!("\n✅ Salary scenario passed");
    Ok(())
}

fn export_bitcoin_core_bip329(
    bitcoin_wallet_path: &Path,
    datadir: &Path,
    wallet_name: &str,
    labels_path: &Path,
) -> Result<()> {
    let output = Command::new(bitcoin_wallet_path)
        .arg(format!("-datadir={}", datadir.display()))
        .arg("-chain=regtest")
        .arg(format!("-wallet={wallet_name}"))
        .arg(format!("-labelsfile={}", labels_path.display()))
        .arg("exportlabels")
        .output()
        .with_context(|| {
            format!(
                "failed to run Bitcoin Core BIP329 export with {}",
                bitcoin_wallet_path.display()
            )
        })?;

    if !output.status.success() {
        anyhow::bail!(
            "bitcoin-wallet exportlabels failed with status {}\nstdout:\n{}\nstderr:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let exported = std::fs::read_to_string(labels_path)
        .with_context(|| format!("failed to read {}", labels_path.display()))?;
    assert!(
        exported
            .lines()
            .any(|line| line.contains("\"type\":\"tx\"")),
        "BIP329 export should include transaction records"
    );
    assert!(
        exported
            .lines()
            .any(|line| line.contains("\"type\":\"output\"")),
        "BIP329 export should include output records"
    );
    assert!(
        exported
            .lines()
            .any(|line| line.contains("\"type\":\"descriptor\"")),
        "BIP329 export should include descriptor records"
    );

    eprintln!(
        "  Exported {} BIP329 record(s) to {}",
        exported.lines().count(),
        labels_path.display()
    );
    Ok(())
}

fn verify_reconstruction(xml: &str, node: &RegtestNode) -> Result<()> {
    // Extract all <AddtlNtryInf> values from the XML (blockchain references)
    let mut count = 0;
    for line in xml.lines() {
        let line = line.trim();
        if let Some(content) = line
            .strip_prefix("<AddtlNtryInf>")
            .and_then(|s| s.strip_suffix("</AddtlNtryInf>"))
        {
            // Skip virtual entries (prefixed with :)
            if content.starts_with(':') {
                continue;
            }

            // Format: blockhash:txid:vout
            let parts: Vec<&str> = content.split(':').collect();
            assert_eq!(
                parts.len(),
                3,
                "expected blockhash:txid:vout, got: {content}"
            );

            let blockhash = parts[0];
            let txid = parts[1];

            // Verify we can look up the block and find the transaction
            let block = node.rpc_call::<serde_json::Value>(
                "getblock",
                &[serde_json::json!(blockhash), serde_json::json!(1)],
            )?;

            let block_txids = block["tx"].as_array().context("block has no tx array")?;

            let found = block_txids.iter().any(|t| t.as_str() == Some(txid));

            assert!(found, "txid {txid} not found in block {blockhash}");
            count += 1;
        }
    }

    eprintln!("  Verified {count} transaction reconstructions");
    assert!(count > 0, "expected at least one reconstructable entry");
    Ok(())
}

fn verify_roundtrip(xml: &str, node: &RegtestNode) -> Result<()> {
    // Parse descriptors from XML comments
    let parsed = btc_fiat_value::export::camt053::parse_camt053(xml)?;
    assert!(
        !parsed.descriptors.is_empty(),
        "expected descriptors in XML"
    );

    eprintln!(
        "\nRoundtrip: creating watch-only wallet from {} descriptor(s)...",
        parsed.descriptors.len()
    );

    // Create watch-only wallet and import descriptors
    let watch_only = RpcWallet::create_watch_only(node, "roundtrip", &parsed.descriptors)?;
    eprintln!("  Watch-only wallet created, listing transactions...");

    // List transactions from the reconstructed wallet
    let wallet_txs = watch_only.list_transactions()?;
    eprintln!(
        "  Found {} transactions in watch-only wallet",
        wallet_txs.len()
    );

    // Build lookup set of txid:vout from the wallet
    let wallet_tx_set: std::collections::HashSet<String> = wallet_txs
        .iter()
        .map(|tx| format!("{}:{}", tx.txid, tx.vout))
        .collect();

    // Verify each non-virtual entry in the XML is accounted for
    let mut verified = 0;
    let mut missing = Vec::new();

    for entry in &parsed.existing_entries {
        // Skip virtual entries (fee, mtm)
        if entry.full_ref.starts_with(':') {
            continue;
        }

        let parts: Vec<&str> = entry.full_ref.split(':').collect();
        if parts.len() != 3 {
            continue;
        }

        let txid = parts[1];
        let vout = parts[2];
        let key = format!("{txid}:{vout}");

        if wallet_tx_set.contains(&key) {
            verified += 1;
        } else {
            missing.push(entry.entry_ref.clone());
        }
    }

    if !missing.is_empty() {
        for ref_id in &missing {
            eprintln!("  Missing: {ref_id}");
        }
        anyhow::bail!(
            "{} transaction(s) from XML not found in watch-only wallet",
            missing.len()
        );
    }

    eprintln!("  ✅ Roundtrip verified: {verified} transactions match");
    Ok(())
}

fn assert_wallet_transactions_equivalent(
    rpc_transactions: &[WalletTransaction],
    bip329_transactions: &[WalletTransaction],
) {
    let mut rpc: Vec<_> = rpc_transactions
        .iter()
        .map(comparable_wallet_transaction)
        .collect();
    let mut bip329: Vec<_> = bip329_transactions
        .iter()
        .map(comparable_wallet_transaction)
        .collect();

    rpc.sort();
    bip329.sort();

    assert_eq!(
        rpc, bip329,
        "Bitcoin Core RPC and BIP329 export should describe the same wallet transactions"
    );
    eprintln!(
        "  BIP329 transaction equivalence verified: {} transaction(s)",
        bip329.len()
    );
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ComparableWalletTransaction {
    txid: String,
    vout: u32,
    amount_sats: i64,
    fee_sats_abs: Option<i64>,
    category: &'static str,
    booking_date: String,
    block_hash: String,
    address: String,
    label: String,
    kind: &'static str,
}

fn comparable_wallet_transaction(tx: &WalletTransaction) -> ComparableWalletTransaction {
    ComparableWalletTransaction {
        txid: tx.txid.clone(),
        vout: tx.vout,
        amount_sats: tx.amount_sats,
        fee_sats_abs: tx.fee_sats.map(|fee| fee.unsigned_abs() as i64),
        category: match &tx.category {
            TxCategory::Send => "send",
            TxCategory::Receive => "receive",
        },
        // The offline wallet export currently provides wallet transaction time
        // and may omit block height. For this daily accounting scenario, the
        // stable cross-source key is the booking date plus full blockhash ref.
        booking_date: timestamp_date(tx.block_time),
        block_hash: tx.block_hash.clone(),
        address: tx.address.clone(),
        label: tx.label.clone(),
        kind: match &tx.kind {
            TxKind::Default => "default",
            TxKind::LiquidityPurchase { .. } => "liquidity_purchase",
        },
    }
}

fn assert_statement_accounting_equivalent(
    rpc_statement: &btc_fiat_value::export::Statement,
    bip329_statement: &btc_fiat_value::export::Statement,
) {
    assert_eq!(
        comparable_statement(rpc_statement),
        comparable_statement(bip329_statement),
        "Bitcoin Core RPC and BIP329 export should produce equivalent accounting statements"
    );
    eprintln!(
        "  BIP329 accounting equivalence verified: {} statement entry/entries",
        bip329_statement.entries.len()
    );
}

#[derive(Debug, Eq, PartialEq)]
struct ComparableStatement {
    account_iban: String,
    currency: String,
    opening_balance_cents: i64,
    opening_balance_sats: i64,
    opening_rate_cents: Option<i64>,
    entries: Vec<ComparableEntry>,
    closing_balance_cents: i64,
    closing_balance_sats: i64,
    opening_date: String,
    statement_date: String,
    statement_id: String,
    bank_name: Option<String>,
}

#[derive(Debug, Eq, PartialEq)]
struct ComparableEntry {
    full_ref: String,
    booking_date: String,
    amount_cents: i64,
    is_credit: bool,
    description: String,
    is_fee: bool,
}

fn comparable_statement(statement: &btc_fiat_value::export::Statement) -> ComparableStatement {
    ComparableStatement {
        account_iban: statement.account_iban.clone(),
        currency: statement.currency.clone(),
        opening_balance_cents: statement.opening_balance_cents,
        opening_balance_sats: statement.opening_balance_sats,
        opening_rate_cents: statement
            .opening_rate
            .map(|rate| (rate * 100.0).round() as i64),
        entries: statement.entries.iter().map(comparable_entry).collect(),
        closing_balance_cents: statement.closing_balance_cents,
        closing_balance_sats: statement.closing_balance_sats,
        opening_date: statement.opening_date.clone(),
        statement_date: statement.statement_date.clone(),
        statement_id: statement.statement_id.clone(),
        bank_name: statement.bank_name.clone(),
    }
}

fn comparable_entry(entry: &btc_fiat_value::export::Entry) -> ComparableEntry {
    ComparableEntry {
        // Short refs and FIFO refs include block height; the current offline
        // export does not reliably provide it, while full on-chain refs do.
        full_ref: comparable_full_ref(&entry.full_ref),
        booking_date: entry.booking_date.clone(),
        amount_cents: entry.amount_cents,
        is_credit: entry.is_credit,
        description: entry.description.clone(),
        is_fee: entry.is_fee,
    }
}

fn comparable_full_ref(full_ref: &str) -> String {
    if let Some(rest) = full_ref.strip_prefix(":fifo:") {
        if let Some((_, suffix)) = rest.split_once(':') {
            return format!(":fifo:<height>:{suffix}");
        }
    }
    full_ref.to_owned()
}

fn timestamp_date(timestamp: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp, 0)
        .expect("valid timestamp")
        .date_naive()
        .format("%Y-%m-%d")
        .to_string()
}

fn skip_to_workday(date: chrono::NaiveDate) -> chrono::NaiveDate {
    use chrono::Datelike;
    match date.weekday() {
        chrono::Weekday::Sat => date + chrono::Duration::days(2),
        chrono::Weekday::Sun => date + chrono::Duration::days(1),
        _ => date,
    }
}

fn month_name(month: usize) -> &'static str {
    match month {
        1 => "January",
        2 => "February",
        3 => "March",
        4 => "April",
        5 => "May",
        6 => "June",
        7 => "July",
        8 => "August",
        9 => "September",
        10 => "October",
        11 => "November",
        12 => "December",
        _ => "Unknown",
    }
}

/// Load mock exchange rates from the fixture file.
fn load_mock_rates() -> Result<Vec<f64>> {
    let fixture_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("mock_rates_2025.json");

    if !fixture_path.exists() {
        // Generate mock rates: roughly realistic EUR/BTC rates for 2025
        // Starting around 95000, with some variation
        let mut rates = Vec::with_capacity(365);
        let mut rng = StdRng::seed_from_u64(42);
        let mut price = 95_000.0_f64;

        for _day in 0..365 {
            let change: f64 = rng.random_range(-2000.0..2000.0);
            price = (price + change).max(50_000.0).min(150_000.0);
            rates.push((price * 100.0).round() / 100.0);
        }

        let json = serde_json::to_string_pretty(&rates)?;
        std::fs::write(&fixture_path, &json)
            .with_context(|| format!("failed to write mock rates to {}", fixture_path.display()))?;

        eprintln!("Generated mock rates at {}", fixture_path.display());
        return Ok(rates);
    }

    let data = std::fs::read_to_string(&fixture_path)
        .with_context(|| format!("failed to read {}", fixture_path.display()))?;
    let rates: Vec<f64> = serde_json::from_str(&data)?;
    Ok(rates)
}

struct MockRateProvider {
    rates: Vec<f64>,
}

impl btc_fiat_value::exchange_rate::ExchangeRateProvider for MockRateProvider {
    fn get_vwap(&self, timestamp: i64, _interval_minutes: u32) -> Result<f64> {
        // Map timestamp to day index in 2025
        let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(timestamp, 0)
            .context("invalid timestamp")?;
        let start_of_2025 = chrono::NaiveDate::from_ymd_opt(2025, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc();

        let day_index = (dt.signed_duration_since(start_of_2025).num_days()).max(0) as usize;
        let rate = self.rates.get(day_index).copied().unwrap_or(95_000.0);
        Ok(rate)
    }
}
