use std::env;

use anyhow::{Result, anyhow, bail};
use dotenvy::dotenv;

use btc_fiat_value::commands::cache_rates::{self as cache_rates_cmd, CacheRatesArgs};
use btc_fiat_value::commands::export::{self as export_cmd, ExportArgs};
use btc_fiat_value::commands::received_value::{self, ReceivedValueArgs};
use btc_fiat_value::commands::reconstruct::{self as reconstruct_cmd, ReconstructArgs};

const ROOT_USAGE: &str = "usage: btc_fiat_value <command> [options]\n\nsubcommands:\n  received-value  find the quote-currency value when BTC was received\n  cache-rates     populate .cache/rates.json for one year of rates\n  export          export wallet transactions to accounting format\n  reconstruct     verify an export by reconstructing the wallet";

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let _ = dotenv();
    match parse_command()? {
        Command::ReceivedValue(args) => received_value::run(args),
        Command::CacheRates(args) => cache_rates_cmd::run(args),
        Command::Export(args) => export_cmd::run(args),
        Command::Reconstruct(args) => reconstruct_cmd::run(args),
    }
}

#[derive(Debug)]
enum Command {
    ReceivedValue(ReceivedValueArgs),
    CacheRates(CacheRatesArgs),
    Export(ExportArgs),
    Reconstruct(ReconstructArgs),
}

fn parse_command() -> Result<Command> {
    parse_command_from(env::args().skip(1))
}

fn parse_command_from<I>(args: I) -> Result<Command>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let first = args.next().ok_or_else(|| anyhow!(ROOT_USAGE))?;

    match first.as_str() {
        received_value::SUBCOMMAND_NAME => Ok(Command::ReceivedValue(
            received_value::parse_args_from(args, received_value::USAGE)?,
        )),
        cache_rates_cmd::SUBCOMMAND_NAME => Ok(Command::CacheRates(
            cache_rates_cmd::parse_args_from(args, cache_rates_cmd::USAGE)?,
        )),
        export_cmd::SUBCOMMAND_NAME => Ok(Command::Export(
            export_cmd::parse_args_from(args, export_cmd::USAGE)?,
        )),
        reconstruct_cmd::SUBCOMMAND_NAME => Ok(Command::Reconstruct(
            reconstruct_cmd::parse_args_from(args, reconstruct_cmd::USAGE)?,
        )),
        "-h" | "--help" | "help" => bail!(ROOT_USAGE),
        _ => bail!(ROOT_USAGE),
    }
}

#[cfg(test)]
mod tests {
    use super::{Command, parse_command_from};

    #[test]
    fn parses_received_value_subcommand() {
        let command = parse_command_from(vec![
            "received-value".to_owned(),
            "--candle".to_owned(),
            "60".to_owned(),
            "--locale".to_owned(),
            "nl-NL".to_owned(),
            "bc1qexample".to_owned(),
        ])
        .expect("command");

        match command {
            Command::ReceivedValue(args) => {
                assert_eq!(args.address, Some("bc1qexample".to_owned()));
                assert_eq!(args.candle_override_minutes, Some(60));
            }
            _ => panic!("expected ReceivedValue"),
        }
    }

    #[test]
    fn parses_export_subcommand() {
        let command = parse_command_from(vec![
            "export".to_owned(),
            "--country".to_owned(),
            "NL".to_owned(),
            "--wallet".to_owned(),
            "test_wallet".to_owned(),
            "--output".to_owned(),
            "my-wallet.xml".to_owned(),
            "--fiat-mode".to_owned(),
        ])
        .expect("command");

        match command {
            Command::Export(args) => {
                assert_eq!(args.wallet, Some("test_wallet".to_owned()));
                assert_eq!(args.country, "NL");
                assert!(args.fiat_mode);
            }
            _ => panic!("expected Export"),
        }
    }

    #[test]
    fn parses_cache_rates_subcommand() {
        let command = parse_command_from(vec!["cache-rates".to_owned(), "2024".to_owned()])
            .expect("command");

        match command {
            Command::CacheRates(args) => {
                assert_eq!(args.year, 2024);
            }
            _ => panic!("expected CacheRates"),
        }
    }

    #[test]
    fn parses_reconstruct_subcommand() {
        let command = parse_command_from(vec![
            "reconstruct".to_owned(),
            "--input".to_owned(),
            "statement.xml".to_owned(),
            "--chain".to_owned(),
            "regtest".to_owned(),
        ])
        .expect("command");

        match command {
            Command::Reconstruct(args) => {
                assert_eq!(args.input.to_str().unwrap(), "statement.xml");
                assert_eq!(args.chain, "regtest");
                assert!(args.wallet.is_none());
            }
            _ => panic!("expected Reconstruct"),
        }
    }
}
