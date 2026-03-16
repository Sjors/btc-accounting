pub mod bitcoin_core_rpc;

use anyhow::Result;

/// Category of a wallet transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TxCategory {
    Send,
    Receive,
}

/// A wallet transaction with all data needed for accounting.
#[derive(Clone, Debug)]
pub struct WalletTransaction {
    pub txid: String,
    pub vout: u32,
    pub amount_sats: i64,
    /// Fee in satoshis (only available for sends).
    pub fee_sats: Option<i64>,
    pub category: TxCategory,
    pub block_time: i64,
    pub block_height: u32,
    pub block_hash: String,
    pub address: String,
    /// Address label or transaction comment (from Bitcoin Core).
    pub label: String,
}

/// Source of wallet transactions.
pub trait TransactionSource {
    fn list_transactions(&self) -> Result<Vec<WalletTransaction>>;
}
