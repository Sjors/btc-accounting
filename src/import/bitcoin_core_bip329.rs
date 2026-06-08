use std::collections::HashMap;
use std::fs;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use chrono::DateTime;
use serde::Deserialize;

use super::{TransactionSource, TxCategory, WalletTransaction};

/// Bitcoin Core rich BIP329 JSONL transaction source.
///
/// This parser targets the experimental `bitcoin-wallet exportlabels` format
/// from Sjors/bitcoin#115, where `output` records carry accounting context.
pub struct BitcoinCoreBip329 {
    transactions: Vec<WalletTransaction>,
    descriptors: Vec<String>,
    fingerprint: String,
}

impl BitcoinCoreBip329 {
    pub fn from_path(path: &Path) -> Result<Self> {
        let jsonl = fs::read_to_string(path)
            .with_context(|| format!("failed to read BIP329 file {}", path.display()))?;
        Self::from_str(&jsonl)
    }

    pub fn from_str(jsonl: &str) -> Result<Self> {
        let mut tx_fees: HashMap<String, i64> = HashMap::new();
        let mut descriptors = Vec::new();
        let mut output_records = Vec::new();
        let mut fingerprint: Option<String> = None;

        for (idx, line) in jsonl.lines().enumerate() {
            let line_number = idx + 1;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            let record: Bip329Record = serde_json::from_str(line).with_context(|| {
                format!("failed to parse BIP329 JSON object on line {line_number}")
            })?;

            match record.record_type.as_str() {
                "tx" => {
                    if let Some(fee) = record.fee {
                        tx_fees.insert(record.ref_id.clone(), fee);
                    }
                    if fingerprint.is_none() {
                        fingerprint = record
                            .origin
                            .as_deref()
                            .and_then(parse_fingerprint_from_origin);
                    }
                }
                "output" => {
                    output_records.push((line_number, record));
                }
                "descriptor" => {
                    descriptors.push(record.ref_id.clone());
                    if fingerprint.is_none() {
                        fingerprint = parse_fingerprint_from_descriptor(&record.ref_id);
                    }
                }
                _ => {}
            }
        }

        let mut transactions = Vec::new();
        for (line_number, record) in output_records {
            let Some(category) = record.category.as_deref() else {
                continue;
            };
            let category = match category {
                "send" => TxCategory::Send,
                "receive" => TxCategory::Receive,
                _ => continue,
            };

            if record.time.is_none() || record.blockhash.is_none() {
                eprintln!(
                    "Skipping unconfirmed BIP329 output {} on line {line_number}",
                    record.ref_id
                );
                continue;
            }

            let (txid, vout) = parse_output_ref(&record.ref_id)
                .with_context(|| format!("invalid output ref on line {line_number}"))?;
            let block_time = parse_bip329_time(record.time.as_deref().unwrap())
                .with_context(|| format!("invalid BIP329 time on line {line_number}"))?;
            let amount_sats = match (record.wallet_value, record.value) {
                (Some(wallet_value), _) => wallet_value,
                (None, Some(value)) if category == TxCategory::Send => -value,
                (None, Some(value)) => value,
                (None, None) => {
                    bail!(
                        "BIP329 output record on line {line_number} is missing wallet_value/value"
                    )
                }
            };

            let fee_sats = if category == TxCategory::Send {
                record.fee.or_else(|| tx_fees.get(&txid).copied())
            } else {
                None
            };

            transactions.push(WalletTransaction {
                txid,
                vout,
                amount_sats,
                fee_sats,
                category,
                block_time,
                block_height: record.height.unwrap_or(0),
                block_hash: record.blockhash.unwrap_or_default(),
                address: record.address.unwrap_or_default(),
                label: record.label.unwrap_or_default(),
                payment_hash: None,
                kind: super::TxKind::Default,
            });
        }

        transactions.sort_by(|a, b| {
            a.block_time
                .cmp(&b.block_time)
                .then(a.block_height.cmp(&b.block_height))
                .then(a.vout.cmp(&b.vout))
        });

        let fingerprint = fingerprint.ok_or_else(|| {
            anyhow!("BIP329 export did not contain a descriptor/origin fingerprint")
        })?;

        Ok(Self {
            transactions,
            descriptors,
            fingerprint,
        })
    }

    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn descriptors(&self) -> &[String] {
        &self.descriptors
    }
}

impl TransactionSource for BitcoinCoreBip329 {
    fn list_transactions(&self) -> Result<Vec<WalletTransaction>> {
        Ok(self.transactions.clone())
    }
}

#[derive(Debug, Deserialize)]
struct Bip329Record {
    #[serde(rename = "type")]
    record_type: String,
    #[serde(rename = "ref")]
    ref_id: String,
    label: Option<String>,
    origin: Option<String>,
    category: Option<String>,
    value: Option<i64>,
    wallet_value: Option<i64>,
    fee: Option<i64>,
    height: Option<u32>,
    time: Option<String>,
    blockhash: Option<String>,
    address: Option<String>,
}

fn parse_output_ref(ref_id: &str) -> Result<(String, u32)> {
    let (txid, vout) = ref_id
        .split_once(':')
        .ok_or_else(|| anyhow!("expected txid:vout"))?;
    if txid.len() != 64 || !txid.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("invalid txid in output ref {ref_id}");
    }
    if vout.contains(':') {
        bail!("invalid output ref {ref_id}");
    }
    let vout = vout
        .parse::<u32>()
        .with_context(|| format!("invalid vout in output ref {ref_id}"))?;
    Ok((txid.to_owned(), vout))
}

fn parse_bip329_time(time: &str) -> Result<i64> {
    Ok(DateTime::parse_from_rfc3339(time)?.timestamp())
}

fn parse_fingerprint_from_origin(origin: &str) -> Option<String> {
    let start = origin.find('[')? + 1;
    parse_fingerprint_at(&origin[start..])
}

fn parse_fingerprint_from_descriptor(desc: &str) -> Option<String> {
    let start = desc.find('[')? + 1;
    parse_fingerprint_at(&desc[start..])
}

fn parse_fingerprint_at(value: &str) -> Option<String> {
    let fp = value.get(..8)?;
    if fp.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(fp.to_lowercase())
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TXID_RECEIVE: &str = "1111111111111111111111111111111111111111111111111111111111111111";
    const TXID_SEND: &str = "2222222222222222222222222222222222222222222222222222222222222222";
    const BLOCKHASH: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    #[test]
    fn parses_rich_bip329_records_into_wallet_transactions() {
        let jsonl = format!(
            r#"{{"type":"tx","ref":"{TXID_RECEIVE}","value":5000,"origin":"wpkh([AABBCCDD/84'/1'/0'])","height":42,"time":"2025-01-02T11:00:00Z","blockhash":"{BLOCKHASH}"}}
{{"type":"output","ref":"{TXID_RECEIVE}:0","label":"Salary January","category":"receive","wallet_value":5000,"value":5000,"height":42,"time":"2025-01-02T11:00:00Z","blockhash":"{BLOCKHASH}","address":"bcrt1qreceive"}}
{{"type":"tx","ref":"{TXID_SEND}","value":-3000,"fee":141,"origin":"wpkh([AABBCCDD/84'/1'/0'])","height":43,"time":"2025-01-03T12:00:00Z","blockhash":"{BLOCKHASH}"}}
{{"type":"output","ref":"{TXID_SEND}:1","label":"Exchange","category":"send","wallet_value":-3000,"value":3000,"height":43,"time":"2025-01-03T12:00:00Z","blockhash":"{BLOCKHASH}","address":"bcrt1qsend"}}
{{"type":"descriptor","ref":"wpkh([aabbccdd/84'/1'/0']tpub.../0/*)#abc","label":"descriptor"}}
"#
        );

        let source = BitcoinCoreBip329::from_str(&jsonl).expect("parse BIP329");
        assert_eq!(source.fingerprint(), "aabbccdd");
        assert_eq!(source.descriptors().len(), 1);

        let txs = source.list_transactions().expect("transactions");
        assert_eq!(txs.len(), 2);
        assert_eq!(txs[0].txid, TXID_RECEIVE);
        assert_eq!(txs[0].vout, 0);
        assert_eq!(txs[0].amount_sats, 5000);
        assert_eq!(txs[0].fee_sats, None);
        assert_eq!(txs[0].category, TxCategory::Receive);
        assert_eq!(txs[0].block_time, 1_735_815_600);
        assert_eq!(txs[0].block_height, 42);
        assert_eq!(txs[0].block_hash, BLOCKHASH);
        assert_eq!(txs[0].label, "Salary January");

        assert_eq!(txs[1].txid, TXID_SEND);
        assert_eq!(txs[1].vout, 1);
        assert_eq!(txs[1].amount_sats, -3000);
        assert_eq!(txs[1].fee_sats, Some(141));
        assert_eq!(txs[1].category, TxCategory::Send);
    }

    #[test]
    fn skips_unconfirmed_outputs() {
        let jsonl = format!(
            r#"{{"type":"descriptor","ref":"wpkh([aabbccdd/84'/1'/0']tpub.../0/*)#abc","label":"descriptor"}}
{{"type":"output","ref":"{TXID_RECEIVE}:0","category":"receive","wallet_value":5000}}
"#
        );
        let source = BitcoinCoreBip329::from_str(&jsonl).expect("parse BIP329");
        assert!(source.list_transactions().unwrap().is_empty());
    }
}
