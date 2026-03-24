pub mod camt053;

use std::io::Write;

use anyhow::Result;

/// A single entry in an accounting statement.
#[derive(Clone, Debug)]
pub struct Entry {
    /// Short reference for dedup (Max35Text): `<height>:<txid_prefix>:<vout>`
    pub entry_ref: String,
    /// Full reference for reconstruction (Max140Text): `<blockhash>:<txid>:<vout>`
    pub full_ref: String,
    /// ISO date-time: YYYY-MM-DDTHH:MM:SS (or date-only YYYY-MM-DD for virtual entries)
    pub booking_date: String,
    /// Amount in cents (fiat mode) or satoshis (BTC mode). Always positive.
    pub amount_cents: i64,
    /// Whether this is a credit (incoming) or debit (outgoing).
    pub is_credit: bool,
    /// Human-readable description.
    pub description: String,
    /// Whether this is a fee entry (virtual).
    pub is_fee: bool,
}

/// Complete statement ready for export.
#[derive(Clone, Debug)]
pub struct Statement {
    pub account_iban: String,
    pub currency: String,
    pub opening_balance_cents: i64,
    /// Opening BTC balance in satoshis.
    pub opening_balance_sats: i64,
    /// Exchange rate used to value the opening BTC balance (fiat per BTC).
    pub opening_rate: Option<f64>,
    pub entries: Vec<Entry>,
    pub closing_balance_cents: i64,
    /// Closing BTC balance in satoshis.
    pub closing_balance_sats: i64,
    /// ISO date of the opening balance (= start_date or first entry date).
    pub opening_date: String,
    /// ISO date: YYYY-MM-DD
    pub statement_date: String,
    /// Unique statement ID
    pub statement_id: String,
    /// Optional bank/institution name (shown in CAMT.053 as FinInstnId/Nm)
    pub bank_name: Option<String>,
    /// Watch-only descriptors for addresses that received coins.
    pub descriptors: Vec<String>,
}

/// Writes a Statement to an accounting format.
pub trait AccountingExporter {
    fn write(&self, statement: &Statement, out: &mut dyn Write) -> Result<()>;
}

/// Extract the date portion from a booking_date ("YYYY-MM-DD" or "YYYY-MM-DDTHH:MM:SS").
pub fn booking_date_to_date(booking_date: &str) -> &str {
    &booking_date[..10]
}
