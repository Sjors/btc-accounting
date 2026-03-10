use std::io::{self, IsTerminal, Write};

use anyhow::{Context, Result, anyhow, bail};
use dotenvy::dotenv;

use crate::common::{
    AppConfig, build_http_client, available_candle_intervals, choose_candle_interval,
    current_unix_timestamp, fetch_candle_for_timestamp, fetch_kraken_candle_with_fallback,
    find_unique_receive_transaction, format_local_timestamp, format_number,
    format_quote_value, parse_candle_interval_minutes, parse_output_locale, sats_to_btc,
    OutputLocale,
};

pub(crate) const SUBCOMMAND_NAME: &str = "received-value";
pub(crate) const USAGE: &str =
    "usage: btc_eur_value received-value [--candle <minutes>] [--locale <tag>] <bitcoin-address>";

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct ReceivedValueArgs {
    pub(crate) address: Option<String>,
    pub(crate) candle_override_minutes: Option<u32>,
    pub(crate) locale_override: Option<OutputLocale>,
}

pub(crate) fn run(args: ReceivedValueArgs) -> Result<()> {
    let _ = dotenv();
    let config = AppConfig::from_env()?;

    let address = match args.address {
        Some(addr) => addr,
        None => {
            if !io::stdin().is_terminal() {
                bail!("{USAGE}");
            }
            return run_interactive(config, args);
        }
    };

    let mempool_client = build_http_client("mempool", config.mempool_proxy_url())?;
    let tor_kraken_client = config
        .kraken_proxy_url()
        .map(|proxy_url| build_http_client("Kraken", Some(proxy_url)))
        .transpose()?;
    let clearnet_kraken_client = build_http_client("clearnet Kraken", None)?;

    let receive_tx = find_unique_receive_transaction(&mempool_client, &config, &address)?;
    let block_time = receive_tx
        .block_time
        .context("receive transaction is missing a confirmation time")?;
    let now = current_unix_timestamp()?;
    let interval_minutes = choose_candle_interval(
        args.candle_override_minutes,
        config.default_candle_minutes,
        block_time,
        now,
    )?;
    let locale = args.locale_override.unwrap_or_else(|| config.locale.clone());

    let candle = match tor_kraken_client.as_ref() {
        Some(kraken_client) => fetch_kraken_candle_with_fallback(
            kraken_client,
            &clearnet_kraken_client,
            &config,
            block_time,
            interval_minutes,
        )?,
        None => fetch_candle_for_timestamp(
            &clearnet_kraken_client,
            &config,
            block_time,
            interval_minutes,
        )?,
    };
    let amount_btc = sats_to_btc(receive_tx.received_sats);
    let quote_value = amount_btc * candle.vwap;

    println!("receive_txid: {}", receive_tx.txid);
    println!("received_btc: {}", format_number(amount_btc, 8, &locale)?);
    println!("confirmed_at: {}", format_local_timestamp(block_time));
    println!("candle_interval_minutes: {}", interval_minutes);
    println!(
        "candle_vwap: {}",
        format_quote_value(&config.kraken_pair, candle.vwap, &locale)?
    );
    println!("{}", format_quote_value(&config.kraken_pair, quote_value, &locale)?);

    Ok(())
}

fn run_interactive(config: AppConfig, args: ReceivedValueArgs) -> Result<()> {
    let address = prompt_required("Bitcoin address")?;

    eprintln!("Fetching transaction…");
    let mempool_client = build_http_client("mempool", config.mempool_proxy_url())?;
    let receive_tx = find_unique_receive_transaction(&mempool_client, &config, &address)?;
    let block_time = receive_tx
        .block_time
        .context("receive transaction is missing a confirmation time")?;
    let now = current_unix_timestamp()?;

    eprintln!("Confirmed at {}", format_local_timestamp(block_time));

    let available = available_candle_intervals(block_time, now);
    if available.is_empty() {
        bail!("transaction is too old for Kraken OHLC history with candles up to 1d");
    }

    let default_interval = args
        .candle_override_minutes
        .filter(|m| available.contains(m))
        .or(config.default_candle_minutes.filter(|m| available.contains(m)))
        .unwrap_or(available[0]);

    let available_str = available
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let interval_input = prompt_with_default(
        &format!("Candle interval in minutes ({available_str})"),
        &default_interval.to_string(),
    )?;
    let interval_minutes = parse_candle_interval_minutes(&interval_input, "candle interval")?;
    if !available.contains(&interval_minutes) {
        bail!(
            "candle interval {interval_minutes} is not available for this transaction; choose from: {available_str}"
        );
    }

    let default_locale = args
        .locale_override
        .map(|l| l.to_string())
        .unwrap_or_else(|| config.locale.to_string());
    let locale_input = prompt_with_default("Locale", &default_locale)?;
    let locale = parse_output_locale(&locale_input, "locale")?;

    let tor_kraken_client = config
        .kraken_proxy_url()
        .map(|proxy_url| build_http_client("Kraken", Some(proxy_url)))
        .transpose()?;
    let clearnet_kraken_client = build_http_client("clearnet Kraken", None)?;
    let candle = match tor_kraken_client.as_ref() {
        Some(kraken_client) => fetch_kraken_candle_with_fallback(
            kraken_client,
            &clearnet_kraken_client,
            &config,
            block_time,
            interval_minutes,
        )?,
        None => fetch_candle_for_timestamp(
            &clearnet_kraken_client,
            &config,
            block_time,
            interval_minutes,
        )?,
    };
    let amount_btc = sats_to_btc(receive_tx.received_sats);
    let quote_value = amount_btc * candle.vwap;

    println!("receive_txid: {}", receive_tx.txid);
    println!("received_btc: {}", format_number(amount_btc, 8, &locale)?);
    println!("confirmed_at: {}", format_local_timestamp(block_time));
    println!("candle_interval_minutes: {}", interval_minutes);
    println!(
        "candle_vwap: {}",
        format_quote_value(&config.kraken_pair, candle.vwap, &locale)?
    );
    println!(
        "{}",
        format_quote_value(&config.kraken_pair, quote_value, &locale)?
    );

    Ok(())
}

fn prompt_required(label: &str) -> Result<String> {
    let mut stderr = io::stderr().lock();
    let stdin = io::stdin();
    let mut line = String::new();

    loop {
        write!(stderr, "{label}: ").context("failed to write prompt")?;
        stderr.flush().context("failed to flush prompt")?;

        line.clear();
        stdin
            .read_line(&mut line)
            .context("failed to read input")?;

        let trimmed = line.trim();
        if !trimmed.is_empty() {
            return Ok(trimmed.to_owned());
        }
    }
}

fn prompt_with_default(label: &str, default: &str) -> Result<String> {
    let mut stderr = io::stderr().lock();
    let stdin = io::stdin();
    let mut line = String::new();

    write!(stderr, "{label} [{default}]: ").context("failed to write prompt")?;
    stderr.flush().context("failed to flush prompt")?;

    stdin
        .read_line(&mut line)
        .context("failed to read input")?;

    let trimmed = line.trim();
    if trimmed.is_empty() {
        Ok(default.to_owned())
    } else {
        Ok(trimmed.to_owned())
    }
}

pub(crate) fn parse_args_from<I>(args: I, usage: &str) -> Result<ReceivedValueArgs>
where
    I: IntoIterator<Item = String>,
{
    let mut address: Option<String> = None;
    let mut candle_override_minutes: Option<u32> = None;
    let mut locale_override: Option<OutputLocale> = None;
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        if arg == "--candle" {
            let value = args
                .next()
                .ok_or_else(|| anyhow!("missing value for --candle\n{usage}"))?;
            candle_override_minutes = Some(parse_candle_interval_minutes(&value, "--candle")?);
            continue;
        }

        if arg == "--locale" {
            let value = args
                .next()
                .ok_or_else(|| anyhow!("missing value for --locale\n{usage}"))?;
            locale_override = Some(parse_output_locale(&value, "--locale")?);
            continue;
        }

        if let Some(value) = arg.strip_prefix("--candle=") {
            candle_override_minutes = Some(parse_candle_interval_minutes(value, "--candle")?);
            continue;
        }

        if let Some(value) = arg.strip_prefix("--locale=") {
            locale_override = Some(parse_output_locale(value, "--locale")?);
            continue;
        }

        if arg.starts_with("--") {
            bail!("unknown option: {arg}\n{usage}");
        }

        if address.replace(arg).is_some() {
            bail!("{usage}");
        }
    }

    let address = address.filter(|a| !a.is_empty());

    Ok(ReceivedValueArgs {
        address,
        candle_override_minutes,
        locale_override,
    })
}

#[cfg(test)]
mod tests {
    use crate::common::parse_output_locale;

    use super::{ReceivedValueArgs, parse_args_from};

    #[test]
    fn parses_args_without_candle_override() {
        let args = parse_args_from(
            vec!["bc1qexample".to_owned()],
            "usage: btc_eur_value received-value [--candle <minutes>] [--locale <tag>] <bitcoin-address>",
        )
        .expect("args");

        assert_eq!(
            args,
            ReceivedValueArgs {
                address: Some("bc1qexample".to_owned()),
                candle_override_minutes: None,
                locale_override: None,
            }
        );
    }

    #[test]
    fn parses_args_with_candle_override() {
        let args = parse_args_from(
            vec![
                "--candle".to_owned(),
                "60".to_owned(),
                "bc1qexample".to_owned(),
            ],
            "usage: btc_eur_value received-value [--candle <minutes>] [--locale <tag>] <bitcoin-address>",
        )
        .expect("args");

        assert_eq!(
            args,
            ReceivedValueArgs {
                address: Some("bc1qexample".to_owned()),
                candle_override_minutes: Some(60),
                locale_override: None,
            }
        );
    }

    #[test]
    fn parses_args_with_equals_style_candle_override() {
        let args = parse_args_from(
            vec!["bc1qexample".to_owned(), "--candle=240".to_owned()],
            "usage: btc_eur_value received-value [--candle <minutes>] [--locale <tag>] <bitcoin-address>",
        )
        .expect("args");

        assert_eq!(
            args,
            ReceivedValueArgs {
                address: Some("bc1qexample".to_owned()),
                candle_override_minutes: Some(240),
                locale_override: None,
            }
        );
    }

    #[test]
    fn parses_args_with_locale_override() {
        let args = parse_args_from(
            vec![
                "--locale".to_owned(),
                "nl-NL".to_owned(),
                "bc1qexample".to_owned(),
            ],
            "usage: btc_eur_value received-value [--candle <minutes>] [--locale <tag>] <bitcoin-address>",
        )
        .expect("args");

        assert_eq!(
            args,
            ReceivedValueArgs {
                address: Some("bc1qexample".to_owned()),
                candle_override_minutes: None,
                locale_override: Some(parse_output_locale("nl-NL", "--locale").expect("locale")),
            }
        );
    }

    #[test]
    fn parses_args_with_equals_style_locale_override() {
        let args = parse_args_from(
            vec!["bc1qexample".to_owned(), "--locale=de-DE".to_owned()],
            "usage: btc_eur_value received-value [--candle <minutes>] [--locale <tag>] <bitcoin-address>",
        )
        .expect("args");

        assert_eq!(
            args,
            ReceivedValueArgs {
                address: Some("bc1qexample".to_owned()),
                candle_override_minutes: None,
                locale_override: Some(parse_output_locale("de-DE", "--locale").expect("locale")),
            }
        );
    }
}
