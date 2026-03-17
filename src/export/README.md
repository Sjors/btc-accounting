# Export formats

## CAMT.053

The `export` command generates bank statements in [CAMT.053.001.02](https://www.iso20022.org/iso-20022-message-definitions) format (ISO 20022), the standard used by European banks for electronic account statements.

### Schema compliance

The generated XML validates against the official `camt.053.001.02` XSD schema.

You can validate a generated file manually:

```bash
xmllint --schema camt.053.001.02.xsd statement.xml --noout
```

### Mapping to Bitcoin transactions

Every entry in the CAMT.053 file contains enough information to trace back to the original Bitcoin transaction.

#### Entry references

Each `<Ntry>` contains two references:

- **`<NtryRef>`** — short reference (Max35Text), used for deduplication:
  `<block_height>:<txid_prefix>:<vout>` (e.g. `840000:a1b2c3d4e5f6a7b8c9d0:0`)

- **`<AddtlNtryInf>`** — full reference (Max500Text), sufficient to reconstruct the exact UTXO:
  `<block_hash>:<txid>:<vout>`

The block hash + txid + vout uniquely identify the transaction output. Given a Bitcoin Core node, you can look up the original transaction with:

```bash
bitcoin-cli getrawtransaction <txid> true <block_hash>
```

#### Descriptions

The **`<Ustrd>`** element (inside `<RmtInf>`) contains a human-readable description of the transaction, shown in accounting software:

- **Receive**: `<label> - Received <btc> BTC @ <rate>` (or the address if no label is set)
- **Send**: `<label> - Sent <btc> BTC @ <rate>` (or the address if no label is set)
- **Fee**: `Mining fee (<sats> sat)`
- **Mark-to-market**: `Year-end mark-to-market adjustment <year>`

Labels are taken from Bitcoin Core address labels (set via `setlabel` RPC or the `label` parameter on `getnewaddress`).

#### Fee entries

Mining fees are separate debit entries with `:fee` as the vout component:
- `<NtryRef>`: `:<height>:<txid_prefix>:fee`
- `<AddtlNtryInf>`: `:<block_hash>:<txid>:fee`
- Description: `Mining fee (<sats> sat)`

Only one fee entry is emitted per transaction, even if the transaction has multiple outputs in the wallet.

#### FIFO realized gain/loss entries

With `--fiat-mode --fifo`, the export emits additional virtual entries for realized gains or losses on sends:

- `<NtryRef>`: `:fifo:<height>:<txid_prefix>:<vout>`
- `<AddtlNtryInf>`: `:fifo:<height>:<txid_prefix>:<vout>`
- Description: `FIFO realized gain <CCY><amount>` or `FIFO realized loss <CCY><amount>`

These entries adjust the fiat balance from spot-value booking to FIFO cost basis. Fees also consume FIFO lots, but do not produce separate `:fifo:` entries.

#### Mark-to-market entries

Year-end reconciliation entries use the reference `:mtm:<year>-12-31` in both `<NtryRef>` and `<AddtlNtryInf>`.

#### Watch-only descriptors

The export embeds watch-only (public key) descriptors as XML comments inside the `<Stmt>` element. Only descriptors whose derived addresses actually received coins are included. These allow reconstructing a watch-only wallet for auditing purposes.

The descriptors contain `xpub`/`tpub` keys (never private keys), but revealing them has privacy implications. The main [README](../../README.md) documents the privacy warning alongside the `export` command.

### IBAN and BIC

The IBAN is generated deterministically from the wallet's master fingerprint:

```
<country><check_digits><bank_code><fingerprint_10d>
```

- **Country**: 2-letter code from `--country` (e.g. `NL`)
- **Check digits**: 2 digits per ISO 13616 Mod-97
- **Bank code**: `XBTC` (mainnet) or `TBTC` (regtest, testnet3, testnet4, signet)
- **Account number**: master fingerprint (u32) zero-padded to 10 digits

Example: `NL86XBTC3548947263`

The BIC in the `<Svcr>` element is derived from the IBAN: `<bank_code><country>2A` (e.g. `XBTCNL2A`).

### Bank name

An optional bank name can be provided via `--bank-name`. It is written as `<Nm>` inside `<FinInstnId>` (the servicer's financial institution identification). When not specified, it defaults to `Bitcoin Core - <wallet_name>`.

### Balance ordering

Per the CAMT.053 XSD, all `<Bal>` elements (opening and closing) precede the `<Ntry>` elements. Both `OPBD` (opening booked) and `CLBD` (closing booked) balances are written before the entries.

### Bank Transaction Codes

Each entry includes a `<BkTxCd>` element:

| Direction | Domain | Family | Sub-family |
|-----------|--------|--------|------------|
| Credit (receive) | `PMNT` | `RCDT` | `OTHR` |
| Debit (send/fee) | `PMNT` | `ICDT` | `OTHR` |

### Append mode

When `--output` points to an existing CAMT.053 file, the tool:

1. Parses the existing file to extract entries and closing balance
2. Deduplicates new entries by `<NtryRef>`
3. Writes a new file with all entries (old + new) and updated balances

### Example output

Run the regtest integration test to generate an example at `tests/fixtures/salary_2025_camt053.xml`:

```bash
cargo test
```

### Accounting software compatibility

The generated XML validates against the official CAMT.053.001.02 XSD schema and should be accepted by any accounting software that supports the format.

If your software rejects the file, its parser may impose additional constraints beyond the XSD. A sample CAMT.053 from a bank your software is known to accept can be useful for comparison:

```bash
xmllint --format bank_export.xml > /tmp/bank.xml
xmllint --format statement.xml > /tmp/ours.xml
diff /tmp/bank.xml /tmp/ours.xml
```
