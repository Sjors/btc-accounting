use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::blocking::Client;

/// A running Bitcoin Core regtest node with a temporary data directory.
#[allow(dead_code)]
pub struct RegtestNode {
    process: Option<Child>,
    datadir: tempfile::TempDir,
    rpc_port: u16,
    rpc_url: String,
    client: Client,
    cookie: String,
}

impl RegtestNode {
    pub fn start(bitcoin_path: &Path, mocktime: i64) -> Result<Self> {
        let datadir = tempfile::tempdir().context("failed to create temp datadir")?;
        let rpc_port = find_available_port()?;
        let rpc_url = format!("http://127.0.0.1:{rpc_port}");

        let mut child = Command::new(bitcoin_path)
            .args([
                "-m",
                "node",
                "-regtest",
                "-server",
                "-ipcbind=unix",
                &format!("-datadir={}", datadir.path().display()),
                &format!("-rpcport={rpc_port}"),
                &format!("-mocktime={mocktime}"),
                "-fallbackfee=0.00001",
                "-txindex=0",
                "-listen=0",
                "-listenonion=0",
            ])
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .context("failed to start bitcoin node")?;

        // Wait for RPC to become available
        let client = Client::builder()
            .timeout(Duration::from_secs(120))
            .build()?;

        let cookie_path = datadir.path().join("regtest").join(".cookie");
        let mut cookie = String::new();

        for attempt in 0..60 {
            std::thread::sleep(Duration::from_millis(500));

            // Check if process is still running
            if let Some(status) = child.try_wait()? {
                bail!("bitcoin node exited with status {status} before RPC was ready");
            }

            if let Ok(c) = std::fs::read_to_string(&cookie_path) {
                cookie = c;
                // Try an RPC call
                let (user, pass) = cookie.split_once(':').unwrap_or((&cookie, ""));
                let body = serde_json::json!({
                    "jsonrpc": "2.0", "id": 1,
                    "method": "getblockchaininfo", "params": []
                });

                if client
                    .post(&rpc_url)
                    .basic_auth(user, Some(pass))
                    .json(&body)
                    .send()
                    .is_ok()
                {
                    eprintln!("bitcoin node ready after {attempt} attempts");
                    break;
                }
            }

            if attempt == 59 {
                child.kill().ok();
                bail!("bitcoin node RPC did not become available within 30 seconds");
            }
        }

        Ok(Self {
            process: Some(child),
            datadir,
            rpc_port,
            rpc_url,
            client,
            cookie,
        })
    }

    pub fn rpc_port(&self) -> u16 {
        self.rpc_port
    }

    #[allow(dead_code)]
    pub fn rpc_url(&self) -> &str {
        &self.rpc_url
    }

    #[allow(dead_code)]
    pub fn datadir(&self) -> &Path {
        self.datadir.path()
    }

    /// Path to the IPC Unix socket (created by -ipcbind=unix).
    pub fn ipc_socket_path(&self) -> PathBuf {
        self.datadir.path().join("regtest").join("node.sock")
    }

    /// Mine blocks via RPC `generatetoaddress` (used for early heights where
    /// IPC `createNewBlock` would fail without the extranonce patch).
    pub fn generate_to_address(&self, nblocks: u32, address: &str) -> Result<Vec<String>> {
        self.rpc_call("generatetoaddress", &[
            serde_json::json!(nblocks),
            serde_json::json!(address),
        ])
    }

    pub fn set_mocktime(&self, timestamp: i64) -> Result<()> {
        self.rpc_call::<serde_json::Value>("setmocktime", &[serde_json::json!(timestamp)])?;
        Ok(())
    }

    pub fn rpc_call<T: serde::de::DeserializeOwned>(
        &self,
        method: &str,
        params: &[serde_json::Value],
    ) -> Result<T> {
        self.rpc_call_wallet(None, method, params)
    }

    pub fn rpc_call_wallet<T: serde::de::DeserializeOwned>(
        &self,
        wallet: Option<&str>,
        method: &str,
        params: &[serde_json::Value],
    ) -> Result<T> {
        let url = match wallet {
            Some(w) => format!("{}/wallet/{w}", self.rpc_url),
            None => self.rpc_url.clone(),
        };

        let (user, pass) = self.cookie.split_once(':').unwrap_or((&self.cookie, ""));
        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params,
        });

        let resp_text = self
            .client
            .post(&url)
            .basic_auth(user, Some(pass))
            .json(&body)
            .send()
            .with_context(|| format!("RPC {method} to {url} failed"))?
            .text()
            .with_context(|| format!("failed to read {method} response"))?;

        let raw: serde_json::Value = serde_json::from_str(&resp_text)
            .with_context(|| format!("failed to parse {method} response"))?;

        if let Some(err) = raw.get("error").filter(|e| !e.is_null()) {
            let code = err.get("code").and_then(|c| c.as_i64()).unwrap_or(-1);
            let message = err.get("message").and_then(|m| m.as_str()).unwrap_or("unknown error");
            bail!("{method}: {message} (code {code})");
        }

        let result_val = raw.get("result").cloned().unwrap_or(serde_json::Value::Null);
        serde_json::from_value(result_val)
            .with_context(|| format!("failed to decode {method} result"))
    }
}

impl Drop for RegtestNode {
    fn drop(&mut self) {
        if let Some(mut child) = self.process.take() {
            // Try graceful shutdown via RPC stop
            let _ = self.rpc_call::<serde_json::Value>("stop", &[]);
            // Wait briefly for clean exit
            let _ = child.wait();
        }
    }
}

fn find_available_port() -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    Ok(listener.local_addr()?.port())
}

/// Find a pre-built `bitcoin` wrapper binary.
///
/// Checks two layouts:
///   1. bitcoin-core/bin/bitcoin   — downloaded release tarball
///   2. bitcoin-core/build/bin/bitcoin — cmake build from source
///
/// See DEVELOP.md for setup instructions.
pub fn find_bitcoin() -> Result<PathBuf> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source_dir = manifest_dir.join("bitcoin-core");

    // Downloaded binary layout: bitcoin-core/bin/bitcoin
    let downloaded = source_dir.join("bin").join("bitcoin");
    if downloaded.exists() {
        return Ok(downloaded);
    }

    // Built-from-source layout: bitcoin-core/build/bin/bitcoin
    let built = source_dir.join("build").join("bin").join("bitcoin");
    if built.exists() {
        return Ok(built);
    }

    bail!(
        "bitcoin not found.\n\
         Download binaries or build from source — see DEVELOP.md.\n\
         Checked:\n  \
         {}\n  \
         {}",
        downloaded.display(),
        built.display()
    );
}
