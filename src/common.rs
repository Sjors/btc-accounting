use std::env;
use std::str::FromStr;
use std::io::{self, IsTerminal, Write};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, Local, TimeZone, Utc};
use fixed_decimal::Decimal as FixedDecimal;
use icu_decimal::options::{DecimalFormatterOptions, GroupingStrategy};
use icu_decimal::DecimalFormatter;
use icu_locale::Locale;
use reqwest::StatusCode;
use reqwest::blocking::Client;
use serde::Deserialize;
use serde_json::Value;

const DEFAULT_MEMPOOL_BASE_URL: &str = "https://mempool.space";
const KRAKEN_BASE_URL: &str = "https://api.kraken.com";
const DEFAULT_KRAKEN_PAIR: &str = "XXBTZUSD";
const DEFAULT_LOCALE: &str = "en-US";
const MAX_KRAKEN_INTERVAL_MINUTES: u32 = 1_440;
pub(crate) const KRAKEN_INTERVALS_MINUTES: [u32; 7] = [1, 5, 15, 30, 60, 240, 1_440];

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct OutputLocale(Locale);

impl OutputLocale {
    fn decimal_formatter(&self) -> Result<DecimalFormatter> {
        let mut options = DecimalFormatterOptions::default();
        options.grouping_strategy = Some(GroupingStrategy::Never);

        DecimalFormatter::try_new(self.0.clone().into(), options).with_context(|| {
            format!("locale {} is not available for decimal formatting", self.0)
        })
    }
}

pub(crate) fn build_http_client(kind: &str, proxy_url: Option<&str>) -> Result<Client> {
    let mut builder = Client::builder().user_agent(concat!(
        env!("CARGO_PKG_NAME"),
        "/",
        env!("CARGO_PKG_VERSION")
    ));

    if let Some(proxy_url) = proxy_url {
        builder = builder.proxy(reqwest::Proxy::all(proxy_url)?);
    }

    builder
        .build()
        .with_context(|| format!("failed to build {kind} HTTP client"))
}

pub(crate) fn fetch_kraken_candle_with_fallback(
    tor_client: &Client,
    clearnet_client: &Client,
    config: &AppConfig,
    timestamp: i64,
    interval_minutes: u32,
) -> Result<Candle> {
    match fetch_candle_for_timestamp(tor_client, config, timestamp, interval_minutes) {
        Ok(candle) => Ok(candle),
        Err(initial_error) => handle_kraken_failure_with_prompt(
            tor_client,
            clearnet_client,
            config,
            timestamp,
            interval_minutes,
            initial_error,
        ),
    }
}

fn handle_kraken_failure_with_prompt(
    tor_client: &Client,
    clearnet_client: &Client,
    config: &AppConfig,
    timestamp: i64,
    interval_minutes: u32,
    initial_error: anyhow::Error,
) -> Result<Candle> {
    if !io::stdin().is_terminal() {
        return Err(initial_error.context(
            "Kraken request through Tor failed and no interactive terminal is available for fallback",
        ));
    }

    eprintln!("Kraken request through Tor failed:");
    eprintln!("{initial_error:#}");

    loop {
        match prompt_for_kraken_fallback_choice()? {
            FallbackChoice::RetryTor => {
                match fetch_candle_for_timestamp(tor_client, config, timestamp, interval_minutes) {
                    Ok(candle) => return Ok(candle),
                    Err(err) => {
                        eprintln!("Retry through Tor failed:");
                        eprintln!("{err:#}");
                    }
                }
            }
            FallbackChoice::UseClearnet => {
                let candle = fetch_candle_for_timestamp(
                    clearnet_client,
                    config,
                    timestamp,
                    interval_minutes,
                )
                .context("Kraken request through clearnet failed")?;
                return Ok(candle);
            }
            FallbackChoice::Abort => {
                bail!("aborted after Tor-backed Kraken request failed");
            }
        }
    }
}

fn prompt_for_kraken_fallback_choice() -> Result<FallbackChoice> {
    let mut stderr = io::stderr().lock();
    let stdin = io::stdin();
    let mut line = String::new();

    loop {
        write!(
            stderr,
            "Try Kraken again via Tor, fall back to clearnet, or abort? [r/c/a]: "
        )
        .context("failed to write prompt")?;
        stderr.flush().context("failed to flush prompt")?;

        line.clear();
        stdin
            .read_line(&mut line)
            .context("failed to read fallback choice")?;

        match parse_fallback_choice(&line) {
            Some(choice) => return Ok(choice),
            None => eprintln!("Please enter 'r', 'c', or 'a'."),
        }
    }
}

fn parse_fallback_choice(input: &str) -> Option<FallbackChoice> {
    match input.trim().to_ascii_lowercase().as_str() {
        "r" | "retry" => Some(FallbackChoice::RetryTor),
        "c" | "clearnet" => Some(FallbackChoice::UseClearnet),
        "a" | "abort" => Some(FallbackChoice::Abort),
        _ => None,
    }
}

pub(crate) fn parse_candle_interval_minutes(value: &str, name: &str) -> Result<u32> {
    let interval_minutes = value
        .parse::<u32>()
        .with_context(|| format!("invalid {name} value: {value}"))?;

    if KRAKEN_INTERVALS_MINUTES.contains(&interval_minutes) {
        Ok(interval_minutes)
    } else {
        bail!(
            "unsupported {name} value: {interval_minutes}; supported intervals: {}",
            supported_candle_intervals()
        );
    }
}

pub(crate) fn parse_output_locale(value: &str, name: &str) -> Result<OutputLocale> {
    let trimmed = value.trim();

    if trimmed.is_empty() {
        bail!("invalid {name} value: empty");
    }

    let locale = Locale::from_str(trimmed)
        .with_context(|| format!("invalid {name} value: {trimmed}"))?;

    DecimalFormatter::try_new(locale.clone().into(), Default::default())
        .with_context(|| format!("unsupported {name} value: {trimmed}"))?;

    Ok(OutputLocale(locale))
}

pub(crate) fn supported_candle_intervals() -> String {
    KRAKEN_INTERVALS_MINUTES
        .iter()
        .map(u32::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

fn parse_default_candle_minutes(value: Option<String>) -> Result<Option<u32>> {
    match value {
        Some(value) if !value.trim().is_empty() => Ok(Some(parse_candle_interval_minutes(
            value.trim(),
            "DEFAULT_CANDLE_MINUTES",
        )?)),
        _ => Ok(None),
    }
}

pub(crate) fn current_unix_timestamp() -> Result<i64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock is before the unix epoch")?;
    i64::try_from(now.as_secs()).context("current timestamp does not fit in i64")
}

pub(crate) fn choose_interval_minutes(block_time: i64, now: i64) -> Option<u32> {
    let age_seconds = now.saturating_sub(block_time);
    KRAKEN_INTERVALS_MINUTES.into_iter().find(|interval| {
        let retention_seconds = i64::from(*interval) * 60 * 720;
        age_seconds <= retention_seconds && *interval <= MAX_KRAKEN_INTERVAL_MINUTES
    })
}

pub(crate) fn choose_candle_interval(
    candle_override_minutes: Option<u32>,
    default_candle_minutes: Option<u32>,
    block_time: i64,
    now: i64,
) -> Result<u32> {
    if let Some(interval_minutes) = candle_override_minutes.or(default_candle_minutes) {
        return Ok(interval_minutes);
    }

    choose_interval_minutes(block_time, now).ok_or_else(|| {
        anyhow!(
            "transaction at {} is too old for Kraken OHLC history with candles up to 1d",
            format_local_timestamp(block_time)
        )
    })
}

pub(crate) fn find_unique_receive_transaction(
    client: &Client,
    config: &AppConfig,
    address: &str,
) -> Result<ReceiveTransaction> {
    let mut last_seen_txid: Option<String> = None;
    let mut receive_transactions = Vec::new();

    loop {
        let txs =
            fetch_address_transactions_page(client, config, address, last_seen_txid.as_deref())?;
        if txs.is_empty() {
            break;
        }

        last_seen_txid = txs.last().map(|tx| tx.txid.clone());

        for tx in txs {
            let received_sats = tx.received_sats(address);
            if received_sats > 0 {
                receive_transactions.push(ReceiveTransaction {
                    txid: tx.txid,
                    received_sats,
                    block_time: tx.status.block_time,
                });

                if receive_transactions.len() > 1 {
                    bail!(
                        "address {address} is ambiguous: found more than one receive transaction"
                    );
                }
            }
        }
    }

    match receive_transactions.pop() {
        Some(tx) => Ok(tx),
        None => bail!("address {address} has no receive transaction"),
    }
}

fn fetch_address_transactions_page(
    client: &Client,
    config: &AppConfig,
    address: &str,
    last_seen_txid: Option<&str>,
) -> Result<Vec<MempoolTransaction>> {
    let is_follow_up_page = last_seen_txid.is_some();
    let url = match last_seen_txid {
        Some(txid) => format!(
            "{}/api/address/{address}/txs/chain/{txid}",
            config.mempool_base_url
        ),
        None => format!("{}/api/address/{address}/txs", config.mempool_base_url),
    };

    let response = client
        .get(&url)
        .send()
        .with_context(|| format!("failed to query mempool.space at {url}"))?;

    if is_follow_up_page && response.status() == StatusCode::NOT_FOUND {
        return Ok(Vec::new());
    }

    let response = response
        .error_for_status()
        .with_context(|| format!("mempool.space returned an error for {url}"))?;

    response
        .json()
        .with_context(|| format!("failed to decode mempool.space response from {url}"))
}

pub(crate) fn fetch_candle_for_timestamp(
    client: &Client,
    config: &AppConfig,
    timestamp: i64,
    interval_minutes: u32,
) -> Result<Candle> {
    let interval_seconds = i64::from(interval_minutes) * 60;
    let candle_start = (timestamp / interval_seconds) * interval_seconds;
    let since = candle_start.saturating_sub(interval_seconds);
    let url = format!(
        "{}/0/public/OHLC?pair={}&interval={interval_minutes}&since={since}",
        KRAKEN_BASE_URL, config.kraken_pair
    );

    let response = client
        .get(&url)
        .send()
        .with_context(|| format!("failed to query Kraken at {url}"))?
        .error_for_status()
        .with_context(|| format!("Kraken returned an error for {url}"))?;

    let payload: KrakenOhlcResponse = response
        .json()
        .with_context(|| format!("failed to decode Kraken response from {url}"))?;

    if !payload.error.is_empty() {
        bail!("Kraken API returned errors: {}", payload.error.join(", "));
    }

    let result = payload
        .result
        .context("Kraken response is missing a result field")?;
    let candle_rows = result
        .get(&config.kraken_pair)
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow!("Kraken response does not include pair {}", config.kraken_pair))?;

    candle_rows
        .iter()
        .filter_map(parse_candle_row)
        .find(|candle| timestamp >= candle.time && timestamp < candle.time + interval_seconds)
        .ok_or_else(|| {
            anyhow!(
                "Kraken did not return the {} minute candle covering {}",
                interval_minutes,
                format_local_timestamp(timestamp)
            )
        })
}

fn parse_candle_row(row: &Value) -> Option<Candle> {
    let values = row.as_array()?;
    if values.len() < 6 {
        return None;
    }

    Some(Candle {
        time: values.first()?.as_i64()?,
        vwap: parse_json_number(values.get(5)?)?,
    })
}

fn parse_json_number(value: &Value) -> Option<f64> {
    value
        .as_str()
        .and_then(|text| text.parse::<f64>().ok())
        .or_else(|| value.as_f64())
}

pub(crate) fn sats_to_btc(sats: u64) -> f64 {
    sats as f64 / 100_000_000.0
}

pub(crate) fn format_number(value: f64, precision: usize, locale: &OutputLocale) -> Result<String> {
    let formatted = format!("{value:.precision$}");
    let decimal = FixedDecimal::from_str(&formatted)
        .with_context(|| format!("failed to parse formatted decimal value: {formatted}"))?;
    let formatter = locale.decimal_formatter()?;

    Ok(formatter.format(&decimal).to_string())
}

pub(crate) fn format_local_timestamp(timestamp: i64) -> String {
    format_timestamp_in_timezone(timestamp, &Local)
}

fn format_timestamp_in_timezone<Tz>(timestamp: i64, tz: &Tz) -> String
where
    Tz: TimeZone,
    Tz::Offset: std::fmt::Display,
{
    DateTime::<Utc>::from_timestamp(timestamp, 0)
        .map(|dt| dt.with_timezone(tz).to_rfc3339())
        .unwrap_or_else(|| format!("unix:{timestamp}"))
}

pub(crate) fn format_quote_value(
    kraken_pair: &str,
    value: f64,
    locale: &OutputLocale,
) -> Result<String> {
    Ok(format!(
        "{}{}",
        quote_value_prefix(kraken_pair),
        format_number(value, 2, locale)?
    ))
}

pub(crate) fn quote_value_prefix(kraken_pair: &str) -> String {
    match quote_currency_code(kraken_pair) {
        Some("EUR") => "€".to_owned(),
        Some("USD") | Some("CAD") | Some("AUD") | Some("NZD") => "$".to_owned(),
        Some("GBP") => "£".to_owned(),
        Some("JPY") | Some("CNY") => "¥".to_owned(),
        Some(code) => format!("{code} "),
        None => String::new(),
    }
}

fn quote_currency_code(kraken_pair: &str) -> Option<&str> {
    (kraken_pair.len() >= 3).then(|| &kraken_pair[kraken_pair.len() - 3..])
}

#[derive(Clone, Debug)]
pub(crate) struct AppConfig {
    pub(crate) mempool_base_url: String,
    pub(crate) kraken_pair: String,
    pub(crate) default_candle_minutes: Option<u32>,
    pub(crate) locale: OutputLocale,
    socks_proxy_url: Option<String>,
}

impl AppConfig {
    pub(crate) fn from_env() -> Result<Self> {
        Self::from_env_values(|name| env::var(name).ok())
    }

    pub(crate) fn kraken_proxy_url(&self) -> Option<&str> {
        self.socks_proxy_url.as_deref()
    }

    pub(crate) fn mempool_proxy_url(&self) -> Option<&str> {
        match (
            self.uses_default_mempool_base_url(),
            self.socks_proxy_url.as_deref(),
        ) {
            (true, Some(proxy_url)) => Some(proxy_url),
            _ => None,
        }
    }

    fn uses_default_mempool_base_url(&self) -> bool {
        self.mempool_base_url == DEFAULT_MEMPOOL_BASE_URL
    }

    pub(crate) fn from_env_values<F>(mut get_env: F) -> Result<Self>
    where
        F: FnMut(&str) -> Option<String>,
    {
        Ok(Self {
            mempool_base_url: env_or_default(get_env("MEMPOOL_BASE_URL"), DEFAULT_MEMPOOL_BASE_URL),
            kraken_pair: env_or_default(get_env("KRAKEN_PAIR"), DEFAULT_KRAKEN_PAIR),
            default_candle_minutes: parse_default_candle_minutes(get_env("DEFAULT_CANDLE_MINUTES"))?,
            locale: parse_output_locale(
                &env_or_default(get_env("LOCALE"), DEFAULT_LOCALE),
                "LOCALE",
            )?,
            socks_proxy_url: env_optional(get_env("SOCKS_PROXY_URL")),
        })
    }
}

fn env_or_default(value: Option<String>, default: &str) -> String {
    match value {
        Some(value) if !value.trim().is_empty() => value,
        _ => default.to_owned(),
    }
}

fn env_optional(value: Option<String>) -> Option<String> {
    match value {
        Some(value) if !value.trim().is_empty() => Some(value),
        _ => None,
    }
}

#[derive(Debug)]
pub(crate) struct ReceiveTransaction {
    pub(crate) txid: String,
    pub(crate) received_sats: u64,
    pub(crate) block_time: Option<i64>,
}

#[derive(Debug)]
pub(crate) struct Candle {
    pub(crate) time: i64,
    pub(crate) vwap: f64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FallbackChoice {
    RetryTor,
    UseClearnet,
    Abort,
}

#[derive(Debug, Deserialize)]
struct MempoolTransaction {
    txid: String,
    status: TransactionStatus,
    vout: Vec<TransactionOutput>,
}

impl MempoolTransaction {
    fn received_sats(&self, address: &str) -> u64 {
        self.vout
            .iter()
            .filter(|output| output.scriptpubkey_address.as_deref() == Some(address))
            .map(|output| output.value)
            .sum()
    }
}

#[derive(Debug, Deserialize)]
struct TransactionOutput {
    scriptpubkey_address: Option<String>,
    value: u64,
}

#[derive(Clone, Debug, Deserialize)]
struct TransactionStatus {
    block_time: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct KrakenOhlcResponse {
    error: Vec<String>,
    result: Option<Value>,
}

#[cfg(test)]
mod tests {
    use chrono::FixedOffset;
    use serde_json::json;

    use super::{
        AppConfig, Candle, FallbackChoice, choose_candle_interval, choose_interval_minutes,
        format_local_timestamp, format_number, format_quote_value, format_timestamp_in_timezone,
        parse_candle_interval_minutes, parse_candle_row, parse_fallback_choice,
        parse_output_locale, quote_value_prefix,
    };

    #[test]
    fn chooses_smallest_interval_that_covers_age() {
        assert_eq!(choose_interval_minutes(1_000, 1_000 + 60), Some(1));
        assert_eq!(
            choose_interval_minutes(1_000, 1_000 + 13 * 60 * 60),
            Some(5)
        );
        assert_eq!(
            choose_interval_minutes(1_000, 1_000 + 10 * 24 * 60 * 60),
            Some(30)
        );
        assert_eq!(
            choose_interval_minutes(1_000, 1_000 + 800 * 24 * 60 * 60),
            None
        );
    }

    #[test]
    fn env_default_candle_minutes_is_used_when_present() {
        let interval_minutes = choose_candle_interval(None, Some(60), 1_000, 1_000 + 60)
            .expect("interval");

        assert_eq!(interval_minutes, 60);
    }

    #[test]
    fn cli_candle_override_beats_env_default() {
        let interval_minutes = choose_candle_interval(Some(15), Some(60), 1_000, 1_000 + 60)
            .expect("interval");

        assert_eq!(interval_minutes, 15);
    }

    #[test]
    fn parses_kraken_candle_row() {
        let row = json!([
            1772536500, "57784.5", "57830.0", "57637.2", "57697.0", "57735.1"
        ]);
        let candle = parse_candle_row(&row).expect("candle");

        assert_candle(&candle, 1772536500, 57735.1);
    }

    #[test]
    fn parses_fallback_choices() {
        assert_eq!(parse_fallback_choice("r"), Some(FallbackChoice::RetryTor));
        assert_eq!(
            parse_fallback_choice("Retry"),
            Some(FallbackChoice::RetryTor)
        );
        assert_eq!(
            parse_fallback_choice("c"),
            Some(FallbackChoice::UseClearnet)
        );
        assert_eq!(
            parse_fallback_choice("clearnet"),
            Some(FallbackChoice::UseClearnet)
        );
        assert_eq!(parse_fallback_choice("a"), Some(FallbackChoice::Abort));
        assert_eq!(parse_fallback_choice("abort"), Some(FallbackChoice::Abort));
        assert_eq!(parse_fallback_choice("x"), None);
    }

    #[test]
    fn rejects_unsupported_candle_interval() {
        let err = parse_candle_interval_minutes("2", "--candle").expect_err("invalid interval");

        assert!(
            err.to_string()
                .contains("supported intervals: 1, 5, 15, 30, 60, 240, 1440")
        );
    }

    #[test]
    fn reads_default_candle_minutes_from_env() {
        let config = AppConfig::from_env_values(|name| match name {
            "DEFAULT_CANDLE_MINUTES" => Some("60".to_owned()),
            _ => None,
        })
        .expect("config");

        assert_eq!(config.default_candle_minutes, Some(60));
    }

    #[test]
    fn rejects_invalid_default_candle_minutes_from_env() {
        let err = AppConfig::from_env_values(|name| match name {
            "DEFAULT_CANDLE_MINUTES" => Some("2".to_owned()),
            _ => None,
        })
        .expect_err("invalid config");

        assert!(err.to_string().contains("unsupported DEFAULT_CANDLE_MINUTES value: 2"));
    }

    #[test]
    fn defaults_locale_to_en_us() {
        let config = AppConfig::from_env_values(|_| None).expect("config");

        assert_eq!(
            config.locale,
            parse_output_locale("en-US", "LOCALE").expect("locale")
        );
    }

    #[test]
    fn reads_decimal_comma_locale_from_env() {
        let config = AppConfig::from_env_values(|name| match name {
            "LOCALE" => Some("nl-NL".to_owned()),
            _ => None,
        })
        .expect("config");

        assert_eq!(
            config.locale,
            parse_output_locale("nl-NL", "LOCALE").expect("locale")
        );
    }

    #[test]
    fn ignores_empty_socks_proxy_url() {
        let config = AppConfig::from_env_values(|name| match name {
            "SOCKS_PROXY_URL" => Some("   ".to_owned()),
            _ => None,
        })
        .expect("config");

        assert_eq!(config.kraken_proxy_url(), None);
    }

    #[test]
    fn uses_tor_for_default_mempool_when_proxy_is_configured() {
        let config = AppConfig::from_env_values(|name| match name {
            "SOCKS_PROXY_URL" => Some("socks5h://127.0.0.1:9050".to_owned()),
            _ => None,
        })
        .expect("config");

        assert_eq!(config.mempool_proxy_url(), Some("socks5h://127.0.0.1:9050"));
    }

    #[test]
    fn skips_tor_for_overridden_mempool_base_url() {
        let config = AppConfig::from_env_values(|name| match name {
            "MEMPOOL_BASE_URL" => Some("https://mempool.custom.example".to_owned()),
            "SOCKS_PROXY_URL" => Some("socks5h://127.0.0.1:9050".to_owned()),
            _ => None,
        })
        .expect("config");

        assert_eq!(config.mempool_proxy_url(), None);
    }

    #[test]
    fn formats_timestamps_in_local_timezone() {
        let tz = FixedOffset::east_opt(3600).expect("offset");

        assert_eq!(
            format_timestamp_in_timezone(0, &tz),
            "1970-01-01T01:00:00+01:00"
        );
        let local_text = format_local_timestamp(0);
        assert!(local_text.ends_with(":00") || local_text.contains('+') || local_text.contains('-'));
    }

    #[test]
    fn uses_quote_currency_prefix() {
        assert_eq!(quote_value_prefix("XXBTZEUR"), "€");
        assert_eq!(quote_value_prefix("XXBTZUSD"), "$");
        assert_eq!(quote_value_prefix("XXBTZGBP"), "£");
        assert_eq!(quote_value_prefix("XXBTZCHF"), "CHF ");
    }

    #[test]
    fn parses_locale_tags() {
        assert_eq!(
            parse_output_locale("nl-NL", "--locale").expect("locale"),
            parse_output_locale("nl-NL", "LOCALE").expect("locale")
        );
        assert_eq!(
            parse_output_locale("en-US", "--locale").expect("locale"),
            parse_output_locale("en-US", "LOCALE").expect("locale")
        );
    }

    #[test]
    fn formats_numbers_with_decimal_comma_locale() {
        let locale = parse_output_locale("nl-NL", "--locale").expect("locale");

        assert_eq!(
            format_number(0.001106, 8, &locale).expect("formatted"),
            "0,00110600"
        );
    }

    #[test]
    fn formats_quote_values() {
        let en_us = parse_output_locale("en-US", "LOCALE").expect("locale");
        let decimal_comma = parse_output_locale("nl-NL", "LOCALE").expect("locale");

        assert_eq!(
            format_quote_value("XXBTZEUR", 57735.1, &en_us).expect("formatted"),
            "€57735.10"
        );
        assert_eq!(
            format_quote_value("XXBTZUSD", 63.86, &en_us).expect("formatted"),
            "$63.86"
        );
        assert_eq!(
            format_quote_value("XXBTZCHF", 12.0, &decimal_comma).expect("formatted"),
            "CHF 12,00"
        );
    }

    fn assert_candle(candle: &Candle, time: i64, vwap: f64) {
        assert_eq!(candle.time, time);
        assert_eq!(candle.vwap, vwap);
    }
}
