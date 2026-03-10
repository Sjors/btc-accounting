# btc-accounting

Rust CLI for Bitcoin accounting tasks.

Current command: `received-value`.

## Available commands

### `received-value`

Find the fiat price when the transaction you received got confirmed. It uses the volume-weighted average price on Kraken for the smallest candle available.

The command works as follows:

1. Takes a Bitcoin address.
2. Queries the `mempool.space` API (or a self-hosted replacement) to find the unique _receive_ transaction for that address.
3. Fetches Kraken price data.
4. Uses the smallest Kraken `OHLC` candle interval that can still cover the transaction confirmation time (override with `--candle <minutes>`).
5. Estimates the value of the received BTC in the quote currency of the chosen Kraken pair.

## Configuration

The current `received-value` command uses these defaults:

- `MEMPOOL_BASE_URL=https://mempool.space`
- `KRAKEN_PAIR=XXBTZUSD`
- `LOCALE=en-US`

You can override those values in a local `.env` file.
You can also set `DEFAULT_CANDLE_MINUTES` in `.env` to change the default candle interval; `--candle` takes precedence.
Likewise, `LOCALE` controls number formatting, and `--locale` takes precedence.

## Tor

Tor is disabled unless you set `SOCKS_PROXY_URL`, for example `socks5h://127.0.0.1:9050`.

When `SOCKS_PROXY_URL` is set:

- Kraken requests use the configured SOCKS proxy.
- The default `mempool.space` requests also use that proxy.
- A custom or self-hosted `MEMPOOL_BASE_URL` does **not** use the proxy automatically.

If a Tor-backed Kraken request fails, the tool prompts to retry through Tor, fall back to clearnet, or abort.

## Run

```bash
cargo run -- received-value bc1q8v4suzh0xvf86f2jqf47aer984qx7c5y3dkr60
```

Example output:

```text
receive_txid: 249cff...
received_btc: 0.00110600
confirmed_at: 2026-03-03T12:16:21+01:00
candle_interval_minutes: 15
candle_vwap: $57735.10
$63.86
```

`confirmed_at` is shown in your local timezone, so the offset in the output will depend on the machine running the tool.

For a decimal comma, set e.g. `LOCALE=nl-NL` or pass `--locale nl-NL`.

## Candle selection

Kraken `OHLC` only exposes up to the most recent 720 candles; see [Get OHLC Data](https://docs.kraken.com/api/docs/rest-api/get-ohlc-data/). The tool therefore chooses the smallest interval from this set unless you pass `--candle <minutes>`:

- `1` (available for 12 hours)
- `5` (available for 2.5 days)
- `15` (available for 7.5 days)
- `30` (available for 15 days)
- `60` (available for about 1 month)
- `240` (available for about 4 months)
- `1440` (available for about 24 months)

`--candle` and `DEFAULT_CANDLE_MINUTES` must use one of those exact interval values in minutes.

The chosen interval must satisfy:

```text
transaction_age <= 720 * interval_minutes * 60
```

If the transaction is too old to fit inside Kraken's `1d` candle retention window, the tool exits with an error instead of silently switching to a coarser interval.

## License

Licensed under the MIT License. See `LICENSE` for details.

## Project layout

- `src/main.rs` — top-level command dispatcher
- `src/commands/received_value.rs` — implementation of the `received-value` subcommand
- `src/common.rs` — shared config, mempool, Kraken, Tor, candle, and formatting logic
