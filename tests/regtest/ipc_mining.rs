use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result, bail};
use bitcoin_capnp_types::{
    init_capnp::init,
    mining_capnp::{block_template, mining},
    proxy_capnp::thread,
};
use capnp_rpc::{RpcSystem, rpc_twoparty_capnp::Side, twoparty::VatNetwork};
use futures::io::BufReader;
use serde::{Deserialize, Serialize};
use tokio::net::UnixStream;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

/// Cached coinbase value for a single block, keyed by block label.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoinbaseSolution {
    pub coinbase_hex: String,
    pub version: u32,
    pub timestamp: u32,
    pub nonce: u32,
}

/// Load coinbase solutions from the cache file.
pub fn load_coinbase_cache(path: &Path) -> Result<HashMap<String, CoinbaseSolution>> {
    if !path.exists() {
        return Ok(HashMap::new());
    }
    let data = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read coinbase cache {}", path.display()))?;
    let cache: HashMap<String, CoinbaseSolution> = serde_json::from_str(&data)?;
    Ok(cache)
}

/// Save coinbase solutions to the cache file, sorted by block height.
pub fn save_coinbase_cache(
    path: &Path,
    cache: &HashMap<String, CoinbaseSolution>,
) -> Result<()> {
    // Sort keys by height order: compare text/numeric segments naturally
    let mut entries: Vec<(&String, &CoinbaseSolution)> = cache.iter().collect();
    entries.sort_by(|(a, _), (b, _)| {
        /// Split a string into alternating text and numeric segments for natural sort.
        fn segments(s: &str) -> Vec<Result<u32, &str>> {
            let mut result = Vec::new();
            let mut i = 0;
            let bytes = s.as_bytes();
            while i < bytes.len() {
                if bytes[i].is_ascii_digit() {
                    let start = i;
                    while i < bytes.len() && bytes[i].is_ascii_digit() { i += 1; }
                    result.push(Ok(s[start..i].parse::<u32>().unwrap_or(0)));
                } else {
                    let start = i;
                    while i < bytes.len() && !bytes[i].is_ascii_digit() { i += 1; }
                    result.push(Err(&s[start..i]));
                }
            }
            result
        }
        let sa = segments(a);
        let sb = segments(b);
        for (a_seg, b_seg) in sa.iter().zip(sb.iter()) {
            let ord = match (a_seg, b_seg) {
                (Ok(na), Ok(nb)) => na.cmp(nb),
                (Err(ta), Err(tb)) => ta.cmp(tb),
                (Ok(_), Err(_)) => std::cmp::Ordering::Less,
                (Err(_), Ok(_)) => std::cmp::Ordering::Greater,
            };
            if ord != std::cmp::Ordering::Equal { return ord; }
        }
        sa.len().cmp(&sb.len())
    });

    // Write JSON manually to preserve our sort order
    use std::io::Write;
    let mut buf = Vec::new();
    writeln!(buf, "{{")?;
    for (i, (key, val)) in entries.iter().enumerate() {
        let val_json = serde_json::to_string(val)?;
        let comma = if i + 1 < entries.len() { "," } else { "" };
        writeln!(buf, "  {}: {}{}", serde_json::to_string(key)?, val_json, comma)?;
    }
    writeln!(buf, "}}")?;
    std::fs::write(path, &buf)
        .with_context(|| format!("failed to write coinbase cache {}", path.display()))?;
    Ok(())
}

/// Mine a single block via IPC using createNewBlock + submitSolution.
///
/// If `cached` is Some, uses the cached coinbase/nonce/timestamp/version.
/// Otherwise brute-forces a valid nonce and returns the solution.
///
/// The `coinbase_output_script` is the scriptPubKey for the coinbase reward.
pub async fn mine_block_ipc(
    socket_path: &Path,
    coinbase_output_script: &[u8],
    cached: Option<&CoinbaseSolution>,
) -> Result<CoinbaseSolution> {
    let stream = UnixStream::connect(socket_path)
        .await
        .with_context(|| {
            format!(
                "IPC connection to {} failed. Is bitcoin running with -ipcbind=unix?",
                socket_path.display()
            )
        })?;

    let (reader, writer) = stream.into_split();
    let buf_reader = BufReader::new(reader.compat());
    let buf_writer = futures::io::BufWriter::new(writer.compat_write());
    let network = VatNetwork::new(buf_reader, buf_writer, Side::Client, Default::default());
    let mut rpc_system = RpcSystem::new(Box::new(network), None);

    let init_client: init::Client = rpc_system.bootstrap(Side::Server);
    tokio::task::spawn_local(rpc_system);

    // Construct / get thread handle
    let construct_resp = init_client
        .construct_request()
        .send()
        .promise
        .await
        .context("IPC construct failed")?;
    let thread_map = construct_resp
        .get()
        .unwrap()
        .get_thread_map()
        .unwrap();
    let thread_resp = thread_map
        .make_thread_request()
        .send()
        .promise
        .await
        .unwrap();
    let thread: thread::Client = thread_resp.get().unwrap().get_result().unwrap();

    // Get mining interface
    let mut mining_req = init_client.make_mining_request();
    mining_req
        .get()
        .get_context()
        .unwrap()
        .set_thread(thread.clone());
    let mining_resp = mining_req.send().promise.await.unwrap();
    let mining_client: mining::Client = mining_resp.get().unwrap().get_result().unwrap();

    // Create block template
    let mut tmpl_req = mining_client.create_new_block_request();
    tmpl_req
        .get()
        .get_context()
        .unwrap()
        .set_thread(thread.clone());
    tmpl_req.get().set_cooldown(false);
    let tmpl_resp = tmpl_req
        .send()
        .promise
        .await
        .context("createNewBlock failed")?;
    let template: block_template::Client = tmpl_resp.get().unwrap().get_result().unwrap();

    // Get the coinbase tx info to build our own coinbase
    let mut cb_req = template.get_coinbase_tx_request();
    cb_req
        .get()
        .get_context()
        .unwrap()
        .set_thread(thread.clone());
    let cb_resp = cb_req.send().promise.await.unwrap();
    let coinbase_info = cb_resp.get().unwrap().get_result().unwrap();

    let cb_version = coinbase_info.get_version();
    let cb_sequence = coinbase_info.get_sequence();
    let script_sig_prefix = coinbase_info.get_script_sig_prefix().unwrap().to_vec();
    let witness = coinbase_info.get_witness().unwrap().to_vec();
    let reward = coinbase_info.get_block_reward_remaining();
    let required_outputs = coinbase_info.get_required_outputs().unwrap();
    let lock_time = coinbase_info.get_lock_time();

    // Get the block header to extract timestamp
    let mut hdr_req = template.get_block_header_request();
    hdr_req
        .get()
        .get_context()
        .unwrap()
        .set_thread(thread.clone());
    let hdr_resp = hdr_req.send().promise.await.unwrap();
    let header_bytes = hdr_resp.get().unwrap().get_result().unwrap();
    // Block header: version(4) + prev_hash(32) + merkle_root(32) + time(4) + bits(4) + nonce(4)
    let header_version = u32::from_le_bytes(header_bytes[0..4].try_into().unwrap());
    let header_time = u32::from_le_bytes(header_bytes[68..72].try_into().unwrap());

    // Build coinbase transaction
    let coinbase_tx = if let Some(cached) = cached {
        hex::decode(&cached.coinbase_hex)
            .context("failed to decode cached coinbase hex")?
    } else {
        build_coinbase_tx(
            cb_version,
            &script_sig_prefix,
            cb_sequence,
            &witness,
            reward,
            coinbase_output_script,
            &required_outputs,
            lock_time,
        )
    };

    let version = cached.map_or(header_version, |c| c.version);
    let timestamp = cached.map_or(header_time, |c| c.timestamp);

    if let Some(cached) = cached {
        // Use cached solution directly
        let mut req = template.submit_solution_request();
        req.get()
            .get_context()
            .unwrap()
            .set_thread(thread.clone());
        req.get().set_version(cached.version);
        req.get().set_timestamp(cached.timestamp);
        req.get().set_nonce(cached.nonce);
        req.get().set_coinbase(&coinbase_tx);
        let resp = req.send().promise.await.context("submitSolution failed")?;
        let accepted = resp.get().unwrap().get_result();
        if !accepted {
            bail!("cached block solution was rejected — cache may be stale");
        }

        // Destroy template
        let mut destroy_req = template.destroy_request();
        destroy_req
            .get()
            .get_context()
            .unwrap()
            .set_thread(thread.clone());
        destroy_req.send().promise.await.ok();

        return Ok(cached.clone());
    }

    // Brute-force nonce
    for nonce in 0..u32::MAX {
        let mut req = template.submit_solution_request();
        req.get()
            .get_context()
            .unwrap()
            .set_thread(thread.clone());
        req.get().set_version(version);
        req.get().set_timestamp(timestamp);
        req.get().set_nonce(nonce);
        req.get().set_coinbase(&coinbase_tx);
        let resp = req.send().promise.await.context("submitSolution failed")?;
        let accepted = resp.get().unwrap().get_result();
        if accepted {
            return Ok(CoinbaseSolution {
                coinbase_hex: hex::encode(&coinbase_tx),
                version,
                timestamp,
                nonce,
            });
        }
    }

    bail!("failed to find valid nonce for block");
}

/// Build a coinbase transaction with the given parameters.
fn build_coinbase_tx(
    version: u32,
    script_sig_prefix: &[u8],
    sequence: u32,
    witness: &[u8],
    reward: i64,
    output_script: &[u8],
    required_outputs: &capnp::data_list::Reader<'_>,
    lock_time: u32,
) -> Vec<u8> {
    let mut tx = Vec::new();

    // Version
    tx.extend_from_slice(&version.to_le_bytes());

    // Segwit marker + flag: only if we have witness data and required outputs
    // (no witness commitment needed for coinbase-only blocks)
    let has_witness = !witness.is_empty() && required_outputs.len() > 0;
    if has_witness {
        tx.push(0x00); // marker
        tx.push(0x01); // flag
    }

    // Input count: always 1 (coinbase)
    tx.push(1);

    // Coinbase input:
    // prev_hash: 32 zero bytes
    tx.extend_from_slice(&[0u8; 32]);
    // prev_index: 0xFFFFFFFF
    tx.extend_from_slice(&0xFFFFFFFFu32.to_le_bytes());
    // scriptSig: use the prefix from getCoinbaseTx as-is
    // (include_dummy_extranonce ensures it's at least 2 bytes)
    let script_sig = script_sig_prefix.to_vec();
    push_varint(&mut tx, script_sig.len() as u64);
    tx.extend_from_slice(&script_sig);
    // sequence
    tx.extend_from_slice(&sequence.to_le_bytes());

    // Output count
    let required_count = required_outputs.len();
    let output_count = 1 + required_count; // reward output + required outputs
    push_varint(&mut tx, output_count as u64);

    // Reward output (our address)
    tx.extend_from_slice(&reward.to_le_bytes());
    push_varint(&mut tx, output_script.len() as u64);
    tx.extend_from_slice(output_script);

    // Required outputs (e.g. witness commitment)
    for i in 0..required_count {
        let output_data = required_outputs.get(i).unwrap();
        tx.extend_from_slice(output_data);
    }

    // Witness data (for the single coinbase input)
    if has_witness {
        tx.push(0x01); // 1 stack item
        push_varint(&mut tx, witness.len() as u64); // item length
        tx.extend_from_slice(witness); // item data (witness nonce)
    }

    // Lock time
    tx.extend_from_slice(&lock_time.to_le_bytes());

    tx
}

fn push_varint(buf: &mut Vec<u8>, val: u64) {
    if val < 0xFD {
        buf.push(val as u8);
    } else if val <= 0xFFFF {
        buf.push(0xFD);
        buf.extend_from_slice(&(val as u16).to_le_bytes());
    } else if val <= 0xFFFFFFFF {
        buf.push(0xFE);
        buf.extend_from_slice(&(val as u32).to_le_bytes());
    } else {
        buf.push(0xFF);
        buf.extend_from_slice(&val.to_le_bytes());
    }
}

/// Hex encode/decode helpers (no external dep needed).
pub mod hex {
    pub fn encode(data: &[u8]) -> String {
        data.iter().map(|b| format!("{b:02x}")).collect()
    }

    pub fn decode(s: &str) -> Result<Vec<u8>, anyhow::Error> {
        if s.len() % 2 != 0 {
            anyhow::bail!("odd hex length");
        }
        (0..s.len())
            .step_by(2)
            .map(|i| {
                u8::from_str_radix(&s[i..i + 2], 16)
                    .map_err(|e| anyhow::anyhow!("invalid hex: {e}"))
            })
            .collect()
    }
}
