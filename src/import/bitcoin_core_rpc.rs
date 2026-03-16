use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::blocking::Client;
use serde::Deserialize;

use super::{TransactionSource, TxCategory, WalletTransaction};

/// Bitcoin Core JSON-RPC transaction source.
pub struct BitcoinCoreRpc {
    client: Client,
    rpc_url: String,
    wallet: String,
    cookie: String,
}

impl BitcoinCoreRpc {
    pub fn new(wallet: &str, datadir: &Path, chain: &str) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build RPC HTTP client")?;

        let cookie_path = cookie_path(datadir, chain);
        let cookie = fs::read_to_string(&cookie_path)
            .with_context(|| format!("failed to read cookie file at {}", cookie_path.display()))?;

        let rpc_url = rpc_url_for_chain(chain)?;

        Ok(Self {
            client,
            rpc_url,
            wallet: wallet.to_owned(),
            cookie,
        })
    }

    /// Create with an explicit RPC URL (for testing).
    pub fn with_url(rpc_url: &str, wallet: &str, datadir: &Path, chain: &str) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build RPC HTTP client")?;

        let cookie_path = cookie_path(datadir, chain);
        let cookie = fs::read_to_string(&cookie_path)
            .with_context(|| format!("failed to read cookie file at {}", cookie_path.display()))?;

        Ok(Self {
            client,
            rpc_url: rpc_url.to_owned(),
            wallet: wallet.to_owned(),
            cookie,
        })
    }

    /// Get the master fingerprint from the wallet's descriptors.
    pub fn get_fingerprint(&self) -> Result<String> {
        let result = self.list_descriptors()?;

        for desc_info in &result {
            if let Some(fp) = parse_fingerprint_from_descriptor(&desc_info.desc) {
                return Ok(fp);
            }
        }

        bail!("no fingerprint found in wallet descriptors")
    }

    /// Get watch-only (public) descriptors for addresses that received coins.
    /// Returns multipath descriptors (BIP-389) combining receive and change paths.
    pub fn get_receive_descriptors(&self, receive_addresses: &std::collections::HashSet<String>) -> Result<Vec<String>> {
        let descriptors = self.list_descriptors()?;

        let mut result = Vec::new();
        for desc_info in &descriptors {
            if desc_info.internal {
                continue;
            }

            // Derive addresses within the descriptor's range and check for overlap
            let range = desc_info.range.unwrap_or([0, 0]);
            let derived: Vec<String> = self.rpc_call(
                "deriveaddresses",
                &[
                    serde_json::json!(desc_info.desc),
                    serde_json::json!(range),
                ],
            )?;

            if derived.iter().any(|a| receive_addresses.contains(a)) {
                // Build multipath descriptor by finding the matching change descriptor
                let multipath = self.build_multipath_descriptor(&desc_info.desc, &descriptors);
                result.push(multipath.unwrap_or_else(|| desc_info.desc.clone()));
            }
        }

        Ok(result)
    }

    /// Combine a receive descriptor (`.../0/*`) with its matching change descriptor (`.../1/*`)
    /// into a single BIP-389 multipath descriptor (`.../&lt;0;1&gt;/*`).
    fn build_multipath_descriptor(&self, receive_desc: &str, all_descriptors: &[DescriptorInfo]) -> Option<String> {
        // Strip checksum from receive descriptor
        let recv_bare = receive_desc.split('#').next()?;

        // The receive descriptor should end with /0/*)
        if !recv_bare.ends_with("/0/*)") {
            return None;
        }

        // Build expected change descriptor (same prefix, /1/* instead of /0/*)
        let prefix = &recv_bare[..recv_bare.len() - "/0/*)".len()];
        let expected_change = format!("{prefix}/1/*)");

        // Find matching internal descriptor
        let found = all_descriptors.iter().any(|d| {
            d.internal && d.desc.split('#').next() == Some(&expected_change)
        });

        if found {
            Some(format!("{prefix}/<0;1>/*)"))
        } else {
            None
        }
    }

    fn list_descriptors(&self) -> Result<Vec<DescriptorInfo>> {
        #[derive(Deserialize)]
        struct ListDescriptorsResult {
            descriptors: Vec<DescriptorInfo>,
        }

        let result: ListDescriptorsResult = self.rpc_call("listdescriptors", &[])?;
        Ok(result.descriptors)
    }

    /// List currently loaded wallets (non-wallet-specific RPC call).
    pub fn list_wallets(rpc_url: &str, cookie: &str) -> Result<Vec<String>> {
        let client = Client::builder()
            .user_agent(concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build RPC HTTP client")?;

        let response: RpcResponse<Vec<String>> = rpc_request(&client, rpc_url, cookie, "listwallets", &[])?;

        response.result.ok_or_else(|| match response.error {
            Some(err) => anyhow!("listwallets failed: {} (code {})", err.message, err.code),
            None => anyhow!("listwallets returned no result"),
        })
    }

    /// Get the wallet's confirmed balance in satoshis.
    pub fn get_balance(&self) -> Result<i64> {
        // minconf=1 to exclude unconfirmed transactions (matching listtransactions filtering)
        let btc: f64 = self.rpc_call("getbalance", &[
            serde_json::json!("*"),   // dummy (deprecated first arg)
            serde_json::json!(1),     // minconf
        ])?;
        Ok(btc_to_sats(btc))
    }

    fn rpc_call<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: &[serde_json::Value],
    ) -> Result<T> {
        let url = format!("{}/wallet/{}", self.rpc_url, self.wallet);
        let response: RpcResponse<T> = rpc_request(&self.client, &url, &self.cookie, method, params)?;

        response.result.ok_or_else(|| match response.error {
            Some(err) => anyhow!("{method} failed: {} (code {})", err.message, err.code),
            None => anyhow!("{method} returned no result"),
        })
    }
}

pub fn cookie_path(datadir: &Path, chain: &str) -> PathBuf {
    match chain {
        "main" | "" => datadir.join(".cookie"),
        other => datadir.join(other).join(".cookie"),
    }
}

pub fn rpc_url_for_chain(chain: &str) -> Result<String> {
    let port = match chain {
        "main" => 8332,
        "testnet3" => 18332,
        "testnet4" => 48332,
        "signet" => 38332,
        "regtest" => 18443,
        _ => bail!("unknown chain: {chain}; expected main, testnet3, testnet4, signet, or regtest"),
    };
    Ok(format!("http://127.0.0.1:{port}"))
}

fn rpc_request<T: serde::de::DeserializeOwned>(
    client: &Client,
    url: &str,
    cookie: &str,
    method: &str,
    params: &[serde_json::Value],
) -> Result<RpcResponse<T>> {
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });

    let (user, pass) = cookie.split_once(':').unwrap_or((cookie, ""));

    client
        .post(url)
        .basic_auth(user, Some(pass))
        .json(&body)
        .send()
        .with_context(|| format!("RPC {method} request to {url} failed"))?
        .json()
        .with_context(|| format!("failed to decode RPC {method} response"))
}

/// Parse fingerprint from a descriptor string like `wpkh([d34db33f/84'/0'/0']xpub...)`.
fn parse_fingerprint_from_descriptor(desc: &str) -> Option<String> {
    let start = desc.find('[')? + 1;
    let end = desc[start..].find('/')? + start;
    let fp = &desc[start..end];
    if fp.len() == 8 && fp.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(fp.to_lowercase())
    } else {
        None
    }
}

impl TransactionSource for BitcoinCoreRpc {
    fn list_transactions(&self) -> Result<Vec<WalletTransaction>> {
        let mut all_txs = Vec::new();
        let mut skip: u64 = 0;
        let page_size = 100;

        loop {
            let raw_txs: Vec<RpcTransaction> = self.rpc_call(
                "listtransactions",
                &[
                    serde_json::json!("*"),
                    serde_json::json!(page_size),
                    serde_json::json!(skip),
                ],
            )?;

            if raw_txs.is_empty() {
                break;
            }

            for tx in &raw_txs {
                if tx.confirmations < 1 {
                    if tx.walletconflicts.is_empty() && tx.replaces_txid.is_none() {
                        eprintln!("⚠️  Skipping unconfirmed transaction {} (no block hash yet)", tx.txid);
                    }
                    continue;
                }

                let category = match tx.category.as_str() {
                    "send" => TxCategory::Send,
                    "receive" => TxCategory::Receive,
                    _ => continue, // skip immature, generate, etc.
                };

                let amount_sats = btc_to_sats(tx.amount);
                let fee_sats = tx.fee.map(btc_to_sats);

                all_txs.push(WalletTransaction {
                    txid: tx.txid.clone(),
                    vout: tx.vout,
                    amount_sats,
                    fee_sats,
                    category,
                    block_time: tx.blocktime.unwrap_or(0),
                    block_height: tx.blockheight.unwrap_or(0),
                    block_hash: tx.blockhash.clone().unwrap_or_default(),
                    address: tx.address.clone().unwrap_or_default(),
                    label: tx.label.clone().unwrap_or_default(),
                });
            }

            skip += page_size;

            if (raw_txs.len() as u64) < page_size {
                break;
            }
        }

        all_txs.sort_by(|a, b| {
            a.block_time
                .cmp(&b.block_time)
                .then(a.block_height.cmp(&b.block_height))
                .then(a.vout.cmp(&b.vout))
        });

        Ok(all_txs)
    }
}

fn btc_to_sats(btc: f64) -> i64 {
    (btc * 100_000_000.0).round() as i64
}

#[derive(Debug, Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i32,
    message: String,
}

#[derive(Debug, Deserialize)]
struct RpcTransaction {
    txid: String,
    vout: u32,
    category: String,
    amount: f64,
    fee: Option<f64>,
    confirmations: i64,
    blockhash: Option<String>,
    blockheight: Option<u32>,
    blocktime: Option<i64>,
    address: Option<String>,
    label: Option<String>,
    #[serde(default)]
    walletconflicts: Vec<String>,
    replaces_txid: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DescriptorInfo {
    desc: String,
    #[serde(default)]
    internal: bool,
    range: Option<[u32; 2]>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fingerprint_from_descriptor() {
        assert_eq!(
            parse_fingerprint_from_descriptor("wpkh([d34db33f/84'/0'/0']xpub6...)"),
            Some("d34db33f".to_owned())
        );
        assert_eq!(
            parse_fingerprint_from_descriptor("tr([AABBCCDD/86'/0'/0']xpub6...)"),
            Some("aabbccdd".to_owned())
        );
        assert_eq!(
            parse_fingerprint_from_descriptor("wpkh(xpub6...)"),
            None
        );
    }

    #[test]
    fn converts_btc_to_sats() {
        assert_eq!(btc_to_sats(1.0), 100_000_000);
        assert_eq!(btc_to_sats(0.001), 100_000);
        assert_eq!(btc_to_sats(-0.5), -50_000_000);
        assert_eq!(btc_to_sats(0.00000001), 1);
    }
}
