use std::env;
use std::path::PathBuf;

use anyhow::{Context, Result, bail};

use crate::common::default_bitcoin_datadir;
use crate::export::camt053::parse_camt053;
use crate::import::bitcoin_core_rpc::BitcoinCoreRpc;
use crate::import::TransactionSource;

pub const SUBCOMMAND_NAME: &str = "reconstruct";

pub const USAGE: &str = "\
usage: btc_fiat_value reconstruct --input <file> [options]

Verify a CAMT.053 export by reconstructing the wallet from its embedded
watch-only descriptors and checking that every transaction is accounted for.

options:
  --input <file>        CAMT.053 XML file to verify (required)
  --wallet <name>       Use existing wallet (default: create new watch-only wallet)
  --datadir <path>      Bitcoin Core data directory (default: BITCOIN_DATADIR)
  --chain <name>        Chain: main, testnet3, testnet4, signet, regtest (default: main)";

#[derive(Debug)]
pub struct ReconstructArgs {
    pub input: PathBuf,
    pub wallet: Option<String>,
    pub datadir: PathBuf,
    pub chain: String,
}

pub fn run(args: ReconstructArgs) -> Result<()> {
    // Parse the CAMT.053 file
    let xml = std::fs::read_to_string(&args.input)
        .with_context(|| format!("failed to read {}", args.input.display()))?;
    let parsed = parse_camt053(&xml)
        .context("failed to parse CAMT.053 file")?;

    if parsed.descriptors.is_empty() {
        bail!("no watch-only descriptors found in XML comments");
    }

    eprintln!("Parsed {} entries, {} descriptor(s) from {}",
        parsed.existing_entries.len(),
        parsed.descriptors.len(),
        args.input.display());

    // Determine wallet name
    let wallet_name = args.wallet.unwrap_or_else(|| {
        format!("reconstruct-{}", parsed.account_iban)
    });

    let rpc_url = crate::import::bitcoin_core_rpc::rpc_url_for_chain(&args.chain)?;
    let cookie_path = crate::import::bitcoin_core_rpc::cookie_path(&args.datadir, &args.chain);
    let cookie = std::fs::read_to_string(&cookie_path)
        .with_context(|| format!("failed to read cookie file at {}", cookie_path.display()))?;

    // Check if wallet already exists
    let wallets = BitcoinCoreRpc::list_wallets(&rpc_url, &cookie)?;
    let wallet_exists = wallets.iter().any(|w| w == &wallet_name);

    if !wallet_exists {
        eprintln!("Creating watch-only wallet '{wallet_name}'...");
        create_watch_only_wallet(&rpc_url, &cookie, &wallet_name, &parsed.descriptors)?;
    } else {
        eprintln!("Using existing wallet '{wallet_name}'");
    }

    // Connect to the wallet
    let rpc = BitcoinCoreRpc::with_url(&rpc_url, &wallet_name, &args.datadir, &args.chain)?;

    // List transactions from the reconstructed wallet
    let wallet_txs = rpc.list_transactions()?;
    eprintln!("Found {} transactions in wallet", wallet_txs.len());

    // Build a set of txid:vout from the wallet for lookup
    let mut wallet_tx_set = std::collections::HashSet::new();
    for tx in &wallet_txs {
        wallet_tx_set.insert(format!("{}:{}", tx.txid, tx.vout));
    }

    // Verify each non-virtual entry in the XML is accounted for
    let mut verified = 0;
    let mut missing = Vec::new();

    for entry in &parsed.existing_entries {
        // Skip virtual entries (fee, mtm) — their full_ref starts with ':'
        if entry.full_ref.starts_with(':') {
            continue;
        }

        // full_ref format: blockhash:txid:vout
        let parts: Vec<&str> = entry.full_ref.split(':').collect();
        if parts.len() != 3 {
            eprintln!("  Warning: unexpected full_ref format: {}", entry.full_ref);
            continue;
        }

        let txid = parts[1];
        let vout = parts[2];
        let key = format!("{txid}:{vout}");

        if wallet_tx_set.contains(&key) {
            verified += 1;
        } else {
            missing.push(entry.entry_ref.clone());
        }
    }

    eprintln!("Verified {verified} transaction(s)");

    if !missing.is_empty() {
        for ref_id in &missing {
            eprintln!("  Missing: {ref_id}");
        }
        bail!("{} transaction(s) from XML not found in wallet", missing.len());
    }

    eprintln!("✅ All transactions verified");
    Ok(())
}

/// Create a watch-only wallet and import descriptors.
fn create_watch_only_wallet(
    rpc_url: &str,
    cookie: &str,
    wallet_name: &str,
    descriptors: &[String],
) -> Result<()> {
    let client = reqwest::blocking::Client::builder()
        .user_agent(concat!(env!("CARGO_PKG_NAME"), "/", env!("CARGO_PKG_VERSION")))
        .build()
        .context("failed to build HTTP client")?;

    // Create blank, watch-only wallet (disable_private_keys=true, blank=true)
    let create_result = rpc_call_raw(
        &client, rpc_url, cookie,
        "createwallet",
        &[
            serde_json::json!(wallet_name),
            serde_json::json!(true),  // disable_private_keys
            serde_json::json!(true),  // blank
        ],
    )?;
    if let Some(err) = create_result.get("error").filter(|e| !e.is_null()) {
        bail!("createwallet failed: {}", err);
    }

    // Expand multipath descriptors into separate receive/change descriptors
    let wallet_url = format!("{rpc_url}/wallet/{wallet_name}");
    let mut import_descs = Vec::new();

    for desc in descriptors {
        if desc.contains("<0;1>") {
            // BIP-389 multipath: split into receive (0) and change (1)
            let recv = desc.replace("<0;1>", "0");
            let change = desc.replace("<0;1>", "1");

            let recv_checksum = get_descriptor_checksum(&client, &wallet_url, cookie, &recv)?;
            let change_checksum = get_descriptor_checksum(&client, &wallet_url, cookie, &change)?;

            import_descs.push(serde_json::json!({
                "desc": format!("{recv}#{recv_checksum}"),
                "timestamp": 0,
                "watchonly": true,
                "active": true,
                "keypool": true,
                "internal": false,
            }));
            import_descs.push(serde_json::json!({
                "desc": format!("{change}#{change_checksum}"),
                "timestamp": 0,
                "watchonly": true,
                "active": true,
                "keypool": true,
                "internal": true,
            }));
        } else {
            // Single descriptor — get checksum if missing
            let desc_with_checksum = if desc.contains('#') {
                desc.clone()
            } else {
                let checksum = get_descriptor_checksum(&client, &wallet_url, cookie, desc)?;
                format!("{desc}#{checksum}")
            };

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

    eprintln!("Importing {} descriptor(s)...", import_descs.len());

    let import_result = rpc_call_raw(
        &client, &wallet_url, cookie,
        "importdescriptors",
        &[serde_json::json!(import_descs)],
    )?;

    // Check for import errors
    if let Some(results) = import_result.get("result").and_then(|r| r.as_array()) {
        for (i, r) in results.iter().enumerate() {
            if r.get("success").and_then(|s| s.as_bool()) != Some(true) {
                let err = r.get("error").map(|e| e.to_string()).unwrap_or_default();
                bail!("importdescriptors[{i}] failed: {err}");
            }
        }
    }

    eprintln!("Descriptors imported, wallet is scanning...");
    Ok(())
}

fn get_descriptor_checksum(
    client: &reqwest::blocking::Client,
    url: &str,
    cookie: &str,
    desc: &str,
) -> Result<String> {
    let result = rpc_call_raw(client, url, cookie, "getdescriptorinfo", &[serde_json::json!(desc)])?;
    let checksum = result
        .get("result")
        .and_then(|r| r.get("checksum"))
        .and_then(|c| c.as_str())
        .context("getdescriptorinfo missing checksum")?;
    Ok(checksum.to_owned())
}

fn rpc_call_raw(
    client: &reqwest::blocking::Client,
    url: &str,
    cookie: &str,
    method: &str,
    params: &[serde_json::Value],
) -> Result<serde_json::Value> {
    let (user, pass) = cookie.split_once(':').unwrap_or((cookie, ""));
    let body = serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params,
    });

    client
        .post(url)
        .basic_auth(user, Some(pass))
        .json(&body)
        .send()
        .with_context(|| format!("RPC {method} request failed"))?
        .json()
        .with_context(|| format!("failed to decode RPC {method} response"))
}

pub fn parse_args_from<I>(args: I, usage: &str) -> Result<ReconstructArgs>
where
    I: IntoIterator<Item = String>,
{
    let mut args = args.into_iter();
    let mut input: Option<PathBuf> = None;
    let mut wallet: Option<String> = None;
    let mut datadir: Option<PathBuf> = None;
    let mut chain: Option<String> = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => {
                input = Some(PathBuf::from(
                    args.next().ok_or_else(|| anyhow::anyhow!("--input requires a value\n\n{usage}"))?,
                ));
            }
            "--wallet" => {
                wallet = Some(args.next().ok_or_else(|| anyhow::anyhow!("--wallet requires a value\n\n{usage}"))?);
            }
            "--datadir" => {
                datadir = Some(PathBuf::from(
                    args.next().ok_or_else(|| anyhow::anyhow!("--datadir requires a value\n\n{usage}"))?,
                ));
            }
            "--chain" => {
                chain = Some(args.next().ok_or_else(|| anyhow::anyhow!("--chain requires a value\n\n{usage}"))?);
            }
            "-h" | "--help" | "help" => bail!("{usage}"),
            _ => {
                if let Some((key, value)) = arg.split_once('=') {
                    match key {
                        "--input" => input = Some(PathBuf::from(value)),
                        "--wallet" => wallet = Some(value.to_owned()),
                        "--datadir" => datadir = Some(PathBuf::from(value)),
                        "--chain" => chain = Some(value.to_owned()),
                        _ => bail!("unknown option: {key}\n\n{usage}"),
                    }
                } else {
                    bail!("unknown argument: {arg}\n\n{usage}");
                }
            }
        }
    }

    let input = input
        .ok_or_else(|| anyhow::anyhow!("--input is required\n\n{usage}"))?;

    let chain = chain
        .or_else(|| env::var("BITCOIN_CHAIN").ok())
        .unwrap_or_else(|| "main".to_owned());

    let datadir = datadir
        .or_else(|| env::var("BITCOIN_DATADIR").ok().map(PathBuf::from))
        .unwrap_or_else(default_bitcoin_datadir);

    Ok(ReconstructArgs {
        input,
        wallet,
        datadir,
        chain,
    })
}
