use anyhow::{Context, Result};
use serde::Deserialize;

use super::ipc_mining::{self, CoinbaseSolution};
use super::node::RegtestNode;
use btc_fiat_value::import::{TxCategory, WalletTransaction};

/// Wrapper around a Bitcoin Core wallet for test operations.
pub struct RpcWallet<'a> {
    node: &'a RegtestNode,
    name: String,
}

impl<'a> RpcWallet<'a> {
    /// Create a deterministic wallet by importing a known tprv key.
    pub fn create_deterministic(node: &'a RegtestNode, name: &str, tprv: &str) -> Result<Self> {
        // Create blank wallet
        node.rpc_call::<serde_json::Value>(
            "createwallet",
            &[
                serde_json::json!(name),
                serde_json::json!(false), // disable_private_keys
                serde_json::json!(true),  // blank
            ],
        )?;

        let wallet = Self {
            node,
            name: name.to_owned(),
        };

        // Get checksum for descriptors via getdescriptorinfo
        let receive_desc = format!("wpkh({tprv}/84h/1h/0h/0/*)");
        let change_desc = format!("wpkh({tprv}/84h/1h/0h/1/*)");

        let recv_info: serde_json::Value = node.rpc_call("getdescriptorinfo", &[serde_json::json!(receive_desc)])?;
        let change_info: serde_json::Value = node.rpc_call("getdescriptorinfo", &[serde_json::json!(change_desc)])?;

        let recv_checksum = recv_info["checksum"].as_str()
            .context("getdescriptorinfo missing checksum for receive")?;
        let change_checksum = change_info["checksum"].as_str()
            .context("getdescriptorinfo missing checksum for change")?;

        let recv_desc_with_checksum = format!("{receive_desc}#{recv_checksum}");
        let change_desc_with_checksum = format!("{change_desc}#{change_checksum}");

        wallet.wallet_call::<serde_json::Value>("importdescriptors", &[serde_json::json!([
            {
                "desc": recv_desc_with_checksum,
                "timestamp": 0,
                "active": true,
                "keypool": true,
                "internal": false,
            },
            {
                "desc": change_desc_with_checksum,
                "timestamp": 0,
                "active": true,
                "keypool": true,
                "internal": true,
            },
        ])])?;

        Ok(wallet)
    }

    /// Create a watch-only wallet from multipath descriptors (for roundtrip testing).
    pub fn create_watch_only(node: &'a RegtestNode, name: &str, descriptors: &[String]) -> Result<Self> {
        node.rpc_call::<serde_json::Value>(
            "createwallet",
            &[
                serde_json::json!(name),
                serde_json::json!(true),  // disable_private_keys
                serde_json::json!(true),  // blank
            ],
        )?;

        let wallet = Self {
            node,
            name: name.to_owned(),
        };

        let mut import_descs = Vec::new();
        for desc in descriptors {
            if desc.contains("<0;1>") {
                // BIP-389 multipath: split into receive/change
                let recv = desc.replace("<0;1>", "0");
                let change = desc.replace("<0;1>", "1");

                let recv_info: serde_json::Value = node.rpc_call("getdescriptorinfo", &[serde_json::json!(recv)])?;
                let change_info: serde_json::Value = node.rpc_call("getdescriptorinfo", &[serde_json::json!(change)])?;

                let recv_with_checksum = recv_info["descriptor"].as_str()
                    .context("getdescriptorinfo missing descriptor")?;
                let change_with_checksum = change_info["descriptor"].as_str()
                    .context("getdescriptorinfo missing descriptor")?;

                import_descs.push(serde_json::json!({
                    "desc": recv_with_checksum,
                    "timestamp": 0,
                    "watchonly": true,
                    "active": true,
                    "keypool": true,
                    "internal": false,
                }));
                import_descs.push(serde_json::json!({
                    "desc": change_with_checksum,
                    "timestamp": 0,
                    "watchonly": true,
                    "active": true,
                    "keypool": true,
                    "internal": true,
                }));
            } else {
                let info: serde_json::Value = node.rpc_call("getdescriptorinfo", &[serde_json::json!(desc)])?;
                let desc_with_checksum = info["descriptor"].as_str()
                    .context("getdescriptorinfo missing descriptor")?;

                import_descs.push(serde_json::json!({
                    "desc": desc_with_checksum,
                    "timestamp": 0,
                    "watchonly": true,
                    "active": true,
                    "keypool": true,
                    "internal": false,
                }));
            }
        }

        wallet.wallet_call::<serde_json::Value>("importdescriptors", &[serde_json::json!(import_descs)])?;

        Ok(wallet)
    }

    pub fn get_new_address(&self) -> Result<String> {
        self.wallet_call("getnewaddress", &[
            serde_json::json!(""),       // label
            serde_json::json!("bech32"), // address_type
        ])
    }

    fn get_change_address(&self) -> Result<String> {
        self.wallet_call("getrawchangeaddress", &[
            serde_json::json!("bech32"),
        ])
    }

    pub fn set_label(&self, address: &str, label: &str) -> Result<()> {
        self.wallet_call::<serde_json::Value>("setlabel", &[
            serde_json::json!(address),
            serde_json::json!(label),
        ])?;
        Ok(())
    }

    pub fn send_to_address(&self, address: &str, amount_sats: i64) -> Result<String> {
        let btc = amount_sats as f64 / 100_000_000.0;

        // Build the transaction manually to ensure deterministic input ordering.
        // Bitcoin Core's `send`/`fundrawtransaction` RPCs shuffle inputs for
        // privacy, which breaks determinism across runs.
        let mut utxos: Vec<serde_json::Value> = self.wallet_call("listunspent", &[])?;
        utxos.sort_by(|a, b| {
            let ta = a["txid"].as_str().unwrap_or("");
            let tb = b["txid"].as_str().unwrap_or("");
            ta.cmp(tb).then_with(|| {
                a["vout"].as_u64().unwrap_or(0).cmp(&b["vout"].as_u64().unwrap_or(0))
            })
        });

        // Select minimum UTXOs from sorted list
        let mut selected = Vec::new();
        let mut total_in: i64 = 0;
        // P2WPKH: ~68 vbytes per input, ~31 per output, ~10 overhead, 2 outputs
        let overhead: i64 = 10 + 31 * 2;
        let per_input: i64 = 68;
        for utxo in &utxos {
            let sats = (utxo["amount"].as_f64().unwrap_or(0.0) * 100_000_000.0).round() as i64;
            selected.push(utxo);
            total_in += sats;
            let est_fee = overhead + per_input * selected.len() as i64; // 1 sat/vB
            if total_in >= amount_sats + est_fee {
                break;
            }
        }

        let n_inputs = selected.len() as i64;
        let fee = overhead + per_input * n_inputs; // 1 sat/vB
        anyhow::ensure!(total_in >= amount_sats + fee, "insufficient funds");
        let change = total_in - amount_sats - fee;

        // Build inputs and outputs in deterministic order: change at position 0
        let inputs: Vec<serde_json::Value> = selected.iter().map(|u| {
            serde_json::json!({
                "txid": u["txid"],
                "vout": u["vout"],
            })
        }).collect();

        let change_addr = self.get_change_address()?;
        let outputs = if change > 0 {
            serde_json::json!([
                { change_addr.clone(): change as f64 / 100_000_000.0 },
                { address: btc },
            ])
        } else {
            serde_json::json!([{ address: btc }])
        };

        // createrawtransaction preserves input ordering
        let raw_hex: String = self.wallet_call("createrawtransaction", &[
            serde_json::json!(inputs),
            outputs,
        ])?;

        // Sign (ECDSA with RFC 6979 is deterministic)
        let signed: serde_json::Value = self.wallet_call("signrawtransactionwithwallet", &[
            serde_json::json!(raw_hex),
        ])?;
        let signed_hex = signed["hex"].as_str()
            .context("signrawtransactionwithwallet missing hex")?;
        anyhow::ensure!(
            signed["complete"].as_bool() == Some(true),
            "transaction signing incomplete"
        );

        // Send
        let txid: String = self.wallet_call("sendrawtransaction", &[
            serde_json::json!(signed_hex),
        ])?;
        Ok(txid)
    }

    /// Mine blocks via IPC with deterministic coinbase.
    ///
    /// For each block, looks up a cached coinbase solution by `label_prefix`
    /// + block index. On cache miss, brute-forces a valid nonce and stores it.
    /// Returns true if all blocks hit the cache (fully deterministic).
    pub fn mine_blocks_ipc(
        &self,
        nblocks: u32,
        address: &str,
        label_prefix: &str,
        cache: &mut std::collections::HashMap<String, CoinbaseSolution>,
        rt: &tokio::runtime::Runtime,
    ) -> Result<bool> {
        // Get scriptPubKey for the address
        let addr_info: serde_json::Value = self.wallet_call(
            "getaddressinfo",
            &[serde_json::json!(address)],
        )?;
        let script_hex = addr_info["scriptPubKey"]
            .as_str()
            .context("getaddressinfo missing scriptPubKey")?;
        let script_bytes = ipc_mining::hex::decode(script_hex)?;

        let socket_path = self.node.ipc_socket_path();
        let mut all_cached = true;

        for i in 0..nblocks {
            let label = format!("{label_prefix}{i}");
            let cached = cache.get(&label);

            if cached.is_none() {
                all_cached = false;
            }

            let solution = rt.block_on(async {
                let local = tokio::task::LocalSet::new();
                local.run_until(
                    ipc_mining::mine_block_ipc(&socket_path, &script_bytes, cached)
                ).await
            }).with_context(|| format!("IPC mining failed for block '{label}'"))?;

            if cached.is_none() {
                cache.insert(label, solution);
            }
        }

        Ok(all_cached)
    }

    pub fn get_balance(&self) -> Result<i64> {
        let btc: f64 = self.wallet_call("getbalance", &[])?;
        Ok((btc * 100_000_000.0).round() as i64)
    }

    pub fn get_fingerprint(&self) -> Result<String> {
        #[derive(Deserialize)]
        struct ListDescriptorsResult {
            descriptors: Vec<DescriptorInfo>,
        }
        #[derive(Deserialize)]
        struct DescriptorInfo {
            desc: String,
        }

        let result: ListDescriptorsResult = self.wallet_call("listdescriptors", &[])?;

        for desc_info in &result.descriptors {
            if let Some(start) = desc_info.desc.find('[') {
                let rest = &desc_info.desc[start + 1..];
                if let Some(end) = rest.find('/') {
                    let fp = &rest[..end];
                    if fp.len() == 8 && fp.chars().all(|c| c.is_ascii_hexdigit()) {
                        return Ok(fp.to_lowercase());
                    }
                }
            }
        }

        anyhow::bail!("no fingerprint found in descriptors")
    }

    pub fn list_transactions(&self) -> Result<Vec<WalletTransaction>> {
        let mut all_txs = Vec::new();
        let mut skip: u64 = 0;
        let page_size = 100;

        loop {
            let raw_txs: Vec<RpcTx> = self.wallet_call(
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
                    eprintln!("⚠️  Skipping unconfirmed transaction {} (no block hash yet)", tx.txid);
                    continue;
                }

                let category = match tx.category.as_str() {
                    "send" => TxCategory::Send,
                    "receive" => TxCategory::Receive,
                    _ => continue,
                };

                let amount_sats = (tx.amount * 100_000_000.0).round() as i64;
                let fee_sats = tx.fee.map(|f| (f * 100_000_000.0).round() as i64);

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
            a.block_time.cmp(&b.block_time)
                .then(a.block_height.cmp(&b.block_height))
                .then(a.vout.cmp(&b.vout))
        });

        Ok(all_txs)
    }

    pub fn get_receive_descriptors(&self, receive_addresses: &std::collections::HashSet<String>) -> Result<Vec<String>> {
        #[derive(Deserialize)]
        struct ListDescriptorsResult {
            descriptors: Vec<DescriptorInfo>,
        }
        #[derive(Deserialize)]
        struct DescriptorInfo {
            desc: String,
            #[serde(default)]
            internal: bool,
            range: Option<[u32; 2]>,
        }

        let result: ListDescriptorsResult = self.wallet_call("listdescriptors", &[])?;

        let mut descriptors = Vec::new();
        for desc_info in &result.descriptors {
            if desc_info.internal {
                continue;
            }
            let range = desc_info.range.unwrap_or([0, 0]);
            let derived: Vec<String> = self.wallet_call("deriveaddresses", &[
                serde_json::json!(desc_info.desc),
                serde_json::json!(range),
            ])?;
            if derived.iter().any(|a| receive_addresses.contains(a)) {
                // Build multipath descriptor by combining with matched change descriptor
                let recv_bare = desc_info.desc.split('#').next().unwrap_or(&desc_info.desc);
                if recv_bare.ends_with("/0/*)") {
                    let prefix = &recv_bare[..recv_bare.len() - "/0/*)".len()];
                    let expected_change = format!("{prefix}/1/*)");
                    let found = result.descriptors.iter().any(|d| {
                        d.internal && d.desc.split('#').next() == Some(expected_change.as_str())
                    });
                    if found {
                        descriptors.push(format!("{prefix}/<0;1>/*)"));
                        continue;
                    }
                }
                descriptors.push(desc_info.desc.clone());
            }
        }

        Ok(descriptors)
    }

    fn wallet_call<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: &[serde_json::Value],
    ) -> Result<T> {
        self.node
            .rpc_call_wallet(Some(&self.name), method, params)
    }
}

#[derive(Debug, Deserialize)]
struct RpcTx {
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
}
