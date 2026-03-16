use std::env;

use anyhow::{Result, anyhow, bail};
use dotenvy::dotenv;

use btc_fiat_value::commands::export::{self as export_cmd, ExportArgs};
use btc_fiat_value::commands::received_value::{self, ReceivedValueArgs};

const ROOT_USAGE: &str = "usage: btc_fiat_value <command> [options]\n\nsubcommands:\n  received-value  find the quote-currency value when BTC was received\n  export          export wallet transactions to accounting format";

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
        Command::Export(args) => export_cmd::run(args),
    }
}

#[derive(Debug)]
enum Command {
    ReceivedValue(ReceivedValueArgs),
    Export(ExportArgs),
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
        export_cmd::SUBCOMMAND_NAME => Ok(Command::Export(
            export_cmd::parse_args_from(args, export_cmd::USAGE)?,
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
}
