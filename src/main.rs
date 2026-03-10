use std::env;

use anyhow::{Result, anyhow, bail};

mod commands;
mod common;

use commands::received_value::{self, ReceivedValueArgs};

const ROOT_USAGE: &str = "usage: btc_eur_value received-value [--candle <minutes>] [--locale <tag>] <bitcoin-address>\n\nsubcommands:\n  received-value  find the quote-currency value when BTC was received";

fn main() {
    if let Err(err) = run() {
        eprintln!("error: {err:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    match parse_command()? {
        Command::ReceivedValue(args) => received_value::run(args),
    }
}

#[derive(Debug, Eq, PartialEq)]
enum Command {
    ReceivedValue(ReceivedValueArgs),
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
        "-h" | "--help" | "help" => bail!(ROOT_USAGE),
        _ => bail!(ROOT_USAGE),
    }
}

#[cfg(test)]
mod tests {
    use super::{Command, parse_command_from};
    use crate::common::parse_output_locale;
    use crate::commands::received_value::ReceivedValueArgs;

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

        assert_eq!(
            command,
            Command::ReceivedValue(ReceivedValueArgs {
                address: Some("bc1qexample".to_owned()),
                candle_override_minutes: Some(60),
                locale_override: Some(parse_output_locale("nl-NL", "--locale").expect("locale")),
            })
        );
    }
}
