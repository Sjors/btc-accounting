use std::env;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};
use chrono::NaiveDate;

use crate::accounting::{AccountingConfig, build_statement};
use crate::common::{AppConfig, default_bitcoin_datadir};
use crate::exchange_rate::KrakenProvider;
use crate::export::camt053::Camt053Exporter;
use crate::export::AccountingExporter;
use crate::iban::iban_from_fingerprint;
use crate::import::TransactionSource;
use crate::import::bitcoin_core_rpc::BitcoinCoreRpc;

pub const SUBCOMMAND_NAME: &str = "export";

pub const USAGE: &str = "\
usage: btc_fiat_value export --country <CC> --output <file> [options]

options:
  --output <file>       Output file path, e.g. my-wallet.xml (appends if file exists)
  --country <CC>        IBAN country code, e.g. NL (required; env: IBAN_COUNTRY)
  --wallet <name>       Bitcoin Core wallet name (auto-detect if only one loaded)
  --datadir <path>      Bitcoin Core data directory (default: BITCOIN_DATADIR)
  --chain <name>        Chain: main, testnet3, testnet4, signet, regtest (default: main)
  --format <fmt>        Output format: camt053 (default)
  --fiat-mode           Convert to fiat at spot rate
  --mark-to-market      Add year-end reconciliation entries (default: on in fiat mode)
  --fifo                Use FIFO lot tracking for realized gains (env: FIFO)
  --start-date <date>   Start date YYYY-MM-DD
  --bank-name <name>    Bank/institution name (default: Bitcoin Core - <wallet>)
  --candle <minutes>    Kraken candle interval (default: DEFAULT_CANDLE_MINUTES or 1440)
  --ignore-balance-mismatch  Warn instead of error on forward/backward balance mismatch";

#[derive(Debug, PartialEq, Eq)]
pub struct ExportArgs {
    pub wallet: Option<String>,
    pub country: String,
    pub datadir: PathBuf,
    pub chain: String,
    pub format: ExportFormat,
    pub fiat_mode: bool,
    pub mark_to_market: Option<bool>,
    pub fifo: bool,
    pub start_date: Option<NaiveDate>,
    pub output: PathBuf,
    pub candle_override_minutes: Option<u32>,
    pub bank_name: Option<String>,
    pub ignore_balance_mismatch: bool,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ExportFormat {
    Camt053,
}

pub fn run(args: ExportArgs) -> Result<()> {
    // Resolve wallet name: use provided, or auto-detect the single loaded wallet
    let wallet = match args.wallet {
        Some(w) => w,
        None => {
            let rpc_url = crate::import::bitcoin_core_rpc::rpc_url_for_chain(&args.chain)?;
            let cookie_path = crate::import::bitcoin_core_rpc::cookie_path(&args.datadir, &args.chain);
            let cookie = std::fs::read_to_string(&cookie_path)
                .with_context(|| format!("failed to read cookie file at {}", cookie_path.display()))?;
            let wallets = BitcoinCoreRpc::list_wallets(&rpc_url, &cookie)?;
            match wallets.len() {
                0 => bail!("no wallets loaded; specify --wallet"),
                1 => wallets.into_iter().next().unwrap(),
                n => bail!("{n} wallets loaded ({}) — specify --wallet", wallets.join(", ")),
            }
        }
    };

    let rpc = BitcoinCoreRpc::new(&wallet, &args.datadir, &args.chain)?;
    let fingerprint = rpc.get_fingerprint()?;
    let iban = iban_from_fingerprint(&fingerprint, &args.country, &args.chain)?;
    eprintln!("Virtual IBAN: {iban}");

    let transactions = rpc.list_transactions()?;
    let wallet_balance_sats = rpc.get_balance()?;

    // Collect receive addresses and fetch matching watch-only descriptors
    let receive_addresses: std::collections::HashSet<String> = transactions.iter()
        .filter(|tx| tx.category == crate::import::TxCategory::Receive)
        .map(|tx| tx.address.clone())
        .collect();
    let descriptors = rpc.get_receive_descriptors(&receive_addresses)?;

    let app_config = AppConfig::from_env()?;
    let candle_minutes = resolve_candle_minutes(
        args.candle_override_minutes,
        app_config.default_candle_minutes,
    );
    let provider = KrakenProvider::new(&app_config)?;

    let currency = if args.fiat_mode {
        quote_currency_from_pair(&app_config.kraken_pair)
    } else {
        "BTC".to_owned()
    };

    let mark_to_market = args.mark_to_market.unwrap_or(args.fiat_mode);

    // Auto-detect append mode: if output file exists, parse it for dedup and continuation
    let append = args.output.exists();

    let (opening_balance_cents, start_date, existing_entries, existing_entry_refs) = if append {
        let output_path = &args.output;

        let existing_xml = std::fs::read_to_string(output_path)
            .with_context(|| format!("failed to read existing file {}", output_path.display()))?;

        let parsed = crate::export::camt053::parse_camt053(&existing_xml)
            .context("failed to parse existing CAMT.053 file")?;

        // Consistency checks
        if parsed.account_iban != iban {
            bail!("IBAN mismatch: file has {} but wallet fingerprint gives {}", parsed.account_iban, iban);
        }
        if parsed.currency != currency {
            bail!("currency mismatch: file has {} but current config uses {}", parsed.currency, currency);
        }

        // Use last booking date + 1 day as implicit start date (unless explicitly set)
        let implicit_start = parsed.last_booking_date
            .and_then(|d| chrono::NaiveDate::parse_from_str(&d, "%Y-%m-%d").ok())
            .map(|d| d + chrono::Duration::days(1));

        let start = args.start_date.or(implicit_start);

        (parsed.closing_balance_cents, start, parsed.existing_entries, parsed.existing_entry_refs)
    } else {
        (0, args.start_date, Vec::new(), std::collections::HashSet::new())
    };

    let bank_name = Some(args.bank_name.unwrap_or_else(|| format!("Bitcoin Core - {wallet}")));

    let config = AccountingConfig {
        fiat_mode: args.fiat_mode,
        mark_to_market,
        fifo: args.fifo,
        currency: currency.clone(),
        account_iban: iban,
        candle_interval_minutes: candle_minutes,
        start_date,
        opening_balance_cents,
        bank_name,
        wallet_balance_sats: Some(wallet_balance_sats),
        ignore_balance_mismatch: args.ignore_balance_mismatch,
    };

    let mut statement = build_statement(&transactions, &provider, &config)?;
    statement.descriptors = descriptors;

    // Dedup: remove entries that already exist in the parsed file
    if !existing_entry_refs.is_empty() {
        statement.entries.retain(|e| !existing_entry_refs.contains(&e.entry_ref));
    }

    // Prepend existing entries
    if !existing_entries.is_empty() {
        let mut all_entries = existing_entries;
        all_entries.append(&mut statement.entries);
        statement.entries = all_entries;
    }

    // Count new entries (excluding those from the existing file)
    let new_entries: Vec<_> = statement.entries.iter()
        .filter(|e| !existing_entry_refs.contains(&e.entry_ref))
        .collect();
    let new_tx_count = new_entries.iter().filter(|e| !e.is_fee && !e.entry_ref.starts_with(":")).count();
    let new_mtm_count = new_entries.iter().filter(|e| e.entry_ref.starts_with(":mtm:")).count();
    let new_fifo_count = new_entries.iter().filter(|e| e.entry_ref.starts_with(":fifo:")).count();
    let first_date = new_entries.iter()
        .filter(|e| !e.is_fee && !e.entry_ref.starts_with(":"))
        .map(|e| e.booking_date.as_str())
        .next();
    let last_date = new_entries.iter()
        .filter(|e| !e.is_fee && !e.entry_ref.starts_with(":"))
        .map(|e| e.booking_date.as_str())
        .last();

    match args.format {
        ExportFormat::Camt053 => {
            let exporter = Camt053Exporter;
            let path = &args.output;
            let file = File::create(path)
                .with_context(|| format!("failed to create output file {}", path.display()))?;
            let mut writer = BufWriter::new(file);
            exporter.write(&statement, &mut writer)?;
        }
    }

    // Summary
    if new_tx_count > 0 {
        if let (Some(first), Some(last)) = (first_date, last_date) {
            let extras = match (new_fifo_count, new_mtm_count) {
                (0, 0) => String::new(),
                (f, 0) => format!(" ({f} FIFO gain/loss)"),
                (0, m) => format!(" ({m} mark-to-market)"),
                (f, m) => format!(" ({f} FIFO gain/loss, {m} mark-to-market)"),
            };
            if first == last {
                eprintln!("Exported {new_tx_count} transaction(s) from {first}.{extras}");
            } else {
                eprintln!("Exported {new_tx_count} transaction(s) from {first} to {last}.{extras}");
            }
        }
    } else if new_mtm_count > 0 {
        eprintln!("No new transactions; added {new_mtm_count} year-end mark-to-market entry/entries.");
    } else {
        eprintln!("No new transactions to export.");
    }

    if provider.cache_grew() {
        eprintln!("Note: exchange rates cached in .cache/rates.json — delete when no longer needed for privacy.");
    }

    Ok(())
}

fn quote_currency_from_pair(pair: &str) -> String {
    if pair.len() >= 3 {
        pair[pair.len() - 3..].to_uppercase()
    } else {
        "USD".to_owned()
    }
}

fn resolve_candle_minutes(candle_override_minutes: Option<u32>, default_candle_minutes: Option<u32>) -> u32 {
    candle_override_minutes
        .or(default_candle_minutes)
        .unwrap_or(1440)
}

pub fn parse_args_from<I>(args: I, usage: &str) -> Result<ExportArgs>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut wallet: Option<String> = None;
    let mut country: Option<String> = None;
    let mut datadir: Option<PathBuf> = None;
    let mut chain: Option<String> = None;
    let mut format: Option<ExportFormat> = None;
    let mut fiat_mode = false;
    let mut mark_to_market: Option<bool> = None;
    let mut fifo = false;
    let mut start_date: Option<NaiveDate> = None;
    let mut output: Option<PathBuf> = None;
    let mut candle_minutes: Option<u32> = None;
    let mut bank_name: Option<String> = None;
    let mut ignore_balance_mismatch = false;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--wallet" => {
                wallet = Some(args.next().ok_or_else(|| anyhow::anyhow!("--wallet requires a value\n\n{usage}"))?);
            }
            "--country" => {
                country = Some(args.next().ok_or_else(|| anyhow::anyhow!("--country requires a value\n\n{usage}"))?);
            }
            "--datadir" => {
                datadir = Some(PathBuf::from(args.next().ok_or_else(|| anyhow::anyhow!("--datadir requires a value\n\n{usage}"))?));
            }
            "--chain" => {
                chain = Some(args.next().ok_or_else(|| anyhow::anyhow!("--chain requires a value\n\n{usage}"))?);
            }
            "--format" => {
                let fmt = args.next().ok_or_else(|| anyhow::anyhow!("--format requires a value\n\n{usage}"))?;
                format = Some(match fmt.as_str() {
                    "camt053" => ExportFormat::Camt053,
                    _ => bail!("unsupported format: {fmt}\n\n{usage}"),
                });
            }
            "--fiat-mode" => fiat_mode = true,
            "--mark-to-market" => mark_to_market = Some(true),
            "--no-mark-to-market" => mark_to_market = Some(false),
            "--fifo" => fifo = true,
            "--start-date" => {
                let date_str = args.next().ok_or_else(|| anyhow::anyhow!("--start-date requires a value\n\n{usage}"))?;
                start_date = Some(NaiveDate::parse_from_str(&date_str, "%Y-%m-%d")
                    .with_context(|| format!("invalid date: {date_str}"))?);
            }
            "--output" => {
                output = Some(PathBuf::from(args.next().ok_or_else(|| anyhow::anyhow!("--output requires a value\n\n{usage}"))?));
            }
            "--candle" => {
                let val = args.next().ok_or_else(|| anyhow::anyhow!("--candle requires a value\n\n{usage}"))?;
                candle_minutes = Some(crate::common::parse_candle_interval_minutes(&val, "--candle")?);
            }
            "--bank-name" => {
                bank_name = Some(args.next().ok_or_else(|| anyhow::anyhow!("--bank-name requires a value\n\n{usage}"))?);
            }
            "--ignore-balance-mismatch" => ignore_balance_mismatch = true,
            "-h" | "--help" | "help" => bail!("{usage}"),
            _ => {
                // Handle --key=value form
                if let Some((key, value)) = arg.split_once('=') {
                    match key {
                        "--wallet" => wallet = Some(value.to_owned()),
                        "--country" => country = Some(value.to_owned()),
                        "--datadir" => datadir = Some(PathBuf::from(value)),
                        "--chain" => chain = Some(value.to_owned()),
                        "--output" => output = Some(PathBuf::from(value)),
                        "--candle" => candle_minutes = Some(crate::common::parse_candle_interval_minutes(value, "--candle")?),
                        "--bank-name" => bank_name = Some(value.to_owned()),
                        "--start-date" => {
                            start_date = Some(NaiveDate::parse_from_str(value, "%Y-%m-%d")
                                .with_context(|| format!("invalid date: {value}"))?);
                        }
                        "--format" => {
                            format = Some(match value {
                                "camt053" => ExportFormat::Camt053,
                                _ => bail!("unsupported format: {value}\n\n{usage}"),
                            });
                        }
                        _ => bail!("unknown option: {key}\n\n{usage}"),
                    }
                } else {
                    bail!("unknown argument: {arg}\n\n{usage}");
                }
            }
        }
    }

    let wallet = wallet
        .or_else(|| env::var("BITCOIN_WALLET").ok());

    let country = country
        .or_else(|| env::var("IBAN_COUNTRY").ok())
        .ok_or_else(|| anyhow::anyhow!("--country is required (or set IBAN_COUNTRY)\n\n{usage}"))?;

    let chain = chain
        .or_else(|| env::var("BITCOIN_CHAIN").ok())
        .unwrap_or_else(|| "main".to_owned());

    let datadir = datadir
        .or_else(|| env::var("BITCOIN_DATADIR").ok().map(PathBuf::from))
        .unwrap_or_else(default_bitcoin_datadir);

    let output = output
        .ok_or_else(|| anyhow::anyhow!("--output is required\n\n{usage}"))?;

    let fiat_mode_env = env::var("FIAT_MODE")
        .ok()
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    let fifo_env = env::var("FIFO")
        .ok()
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    Ok(ExportArgs {
        wallet,
        country,
        datadir,
        chain,
        format: format.unwrap_or(ExportFormat::Camt053),
        fiat_mode: fiat_mode || fiat_mode_env,
        mark_to_market,
        fifo: fifo || fifo_env,
        start_date,
        output,
        candle_override_minutes: candle_minutes,
        bank_name,
        ignore_balance_mismatch,
    })
}

#[cfg(test)]
mod tests {
    use super::{ExportFormat, ExportArgs, USAGE, parse_args_from, resolve_candle_minutes};

    #[test]
    fn parses_export_args_without_candle_override() {
        let args = parse_args_from(
            vec![
                "--country".to_owned(),
                "NL".to_owned(),
                "--wallet".to_owned(),
                "test_wallet".to_owned(),
                "--output".to_owned(),
                "my-wallet.xml".to_owned(),
            ],
            USAGE,
        )
        .expect("args");

        assert_eq!(
            args,
            ExportArgs {
                wallet: Some("test_wallet".to_owned()),
                country: "NL".to_owned(),
                datadir: crate::common::default_bitcoin_datadir(),
                chain: "main".to_owned(),
                format: ExportFormat::Camt053,
                fiat_mode: false,
                mark_to_market: None,
                fifo: false,
                start_date: None,
                output: "my-wallet.xml".into(),
                candle_override_minutes: None,
                bank_name: None,
                ignore_balance_mismatch: false,
            }
        );
    }

    #[test]
    fn parses_export_args_with_candle_override() {
        let args = parse_args_from(
            vec![
                "--country".to_owned(),
                "NL".to_owned(),
                "--wallet".to_owned(),
                "test_wallet".to_owned(),
                "--output".to_owned(),
                "my-wallet.xml".to_owned(),
                "--candle".to_owned(),
                "60".to_owned(),
            ],
            USAGE,
        )
        .expect("args");

        assert_eq!(args.candle_override_minutes, Some(60));
    }

    #[test]
    fn export_uses_default_candle_minutes_when_present() {
        assert_eq!(resolve_candle_minutes(None, Some(60)), 60);
    }

    #[test]
    fn export_candle_override_beats_default_candle_minutes() {
        assert_eq!(resolve_candle_minutes(Some(240), Some(60)), 240);
    }

    #[test]
    fn export_defaults_to_daily_when_no_override_or_env_default_exists() {
        assert_eq!(resolve_candle_minutes(None, None), 1440);
    }
}
