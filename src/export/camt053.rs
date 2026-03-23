use std::collections::HashSet;
use std::io::Write;

use anyhow::{Context, Result, bail};

use super::{AccountingExporter, Entry, Statement, booking_date_to_date};

/// CAMT.053.001.02 XML exporter.
pub struct Camt053Exporter;

impl AccountingExporter for Camt053Exporter {
    fn write(&self, stmt: &Statement, out: &mut dyn Write) -> Result<()> {
        write_xml(stmt, out)
    }
}

fn write_xml(stmt: &Statement, out: &mut dyn Write) -> Result<()> {
    writeln!(out, r#"<?xml version="1.0" encoding="UTF-8"?>"#)?;
    writeln!(
        out,
        r#"<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.053.001.02" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">"#
    )?;
    writeln!(out, "  <BkToCstmrStmt>")?;

    // Group Header (mandatory in CAMT.053)
    writeln!(out, "    <GrpHdr>")?;
    writeln!(out, "      <MsgId>{}</MsgId>", xml_escape(&stmt.statement_id))?;
    writeln!(out, "      <CreDtTm>{}T00:00:00</CreDtTm>", stmt.statement_date)?;
    writeln!(out, "      <MsgPgntn>")?;
    writeln!(out, "        <PgNb>1</PgNb>")?;
    writeln!(out, "        <LastPgInd>true</LastPgInd>")?;
    writeln!(out, "      </MsgPgntn>")?;
    writeln!(out, "    </GrpHdr>")?;

    writeln!(out, "    <Stmt>")?;
    writeln!(out, "      <Id>{}</Id>", xml_escape(&stmt.statement_id))?;
    writeln!(out, "      <ElctrncSeqNb>1</ElctrncSeqNb>")?;
    writeln!(out, "      <CreDtTm>{}T00:00:00</CreDtTm>", stmt.statement_date)?;

    // Account
    let bic = bic_from_iban(&stmt.account_iban);
    writeln!(out, "      <Acct>")?;
    writeln!(out, "        <Id>")?;
    writeln!(out, "          <IBAN>{}</IBAN>", stmt.account_iban)?;
    writeln!(out, "        </Id>")?;
    writeln!(out, "        <Ccy>{}</Ccy>", stmt.currency)?;
    writeln!(out, "        <Svcr>")?;
    writeln!(out, "          <FinInstnId>")?;
    writeln!(out, "            <BIC>{bic}</BIC>")?;
    if let Some(ref name) = stmt.bank_name {
        writeln!(out, "            <Nm>{}</Nm>", xml_escape(name))?;
    }
    writeln!(out, "          </FinInstnId>")?;
    writeln!(out, "        </Svcr>")?;
    writeln!(out, "      </Acct>")?;

    // Opening balance (fiat)
    write_balance(out, "OPBD", stmt.opening_balance_cents, &stmt.currency, &opening_date(stmt))?;

    // Closing balance
    write_balance(out, "CLBD", stmt.closing_balance_cents, &stmt.currency, &stmt.statement_date)?;

    // Entries
    for entry in &stmt.entries {
        write_entry(out, entry, &stmt.currency)?;
    }

    // Watch-only descriptors as XML comments (for wallet reconstruction)
    if !stmt.descriptors.is_empty() {
        writeln!(out, "    <!-- Watch-only descriptors (privacy-sensitive — see README) -->")?;
        for desc in &stmt.descriptors {
            writeln!(out, "    <!-- {} -->", xml_escape_comment(desc))?;
        }
    }

    // BTC opening balance as a comment (informational; not part of the fiat statement)
    if stmt.opening_balance_sats != 0 {
        let abs_sats = stmt.opening_balance_sats.unsigned_abs();
        let whole = abs_sats / 100_000_000;
        let frac = abs_sats % 100_000_000;
        let sign = if stmt.opening_balance_sats < 0 { "-" } else { "" };
        if let Some(rate) = stmt.opening_rate {
            writeln!(out, "    <!-- BTC opening balance: {sign}{whole}.{frac:08} BTC @ {rate:.2} -->")?;
        } else {
            writeln!(out, "    <!-- BTC opening balance: {sign}{whole}.{frac:08} BTC -->")?;
        }
    }

    writeln!(out, "    </Stmt>")?;
    writeln!(out, "  </BkToCstmrStmt>")?;
    writeln!(out, "</Document>")?;

    Ok(())
}

fn opening_date(stmt: &Statement) -> String {
    stmt.opening_date.clone()
}

fn write_balance(out: &mut dyn Write, code: &str, amount_cents: i64, currency: &str, date: &str) -> Result<()> {
    let (cd_indicator, abs_cents) = if amount_cents >= 0 {
        ("CRDT", amount_cents)
    } else {
        ("DBIT", -amount_cents)
    };

    let (whole, frac) = cents_to_parts(abs_cents);

    writeln!(out, "      <Bal>")?;
    writeln!(out, "        <Tp>")?;
    writeln!(out, "          <CdOrPrtry>")?;
    writeln!(out, "            <Cd>{code}</Cd>")?;
    writeln!(out, "          </CdOrPrtry>")?;
    writeln!(out, "        </Tp>")?;
    writeln!(out, "        <Amt Ccy=\"{currency}\">{whole}.{frac:02}</Amt>")?;
    writeln!(out, "        <CdtDbtInd>{cd_indicator}</CdtDbtInd>")?;
    writeln!(out, "        <Dt>")?;
    writeln!(out, "          <Dt>{date}</Dt>")?;
    writeln!(out, "        </Dt>")?;
    writeln!(out, "      </Bal>")?;

    Ok(())
}

fn write_entry(out: &mut dyn Write, entry: &Entry, currency: &str) -> Result<()> {
    let cd_indicator = if entry.is_credit { "CRDT" } else { "DBIT" };
    let (whole, frac) = cents_to_parts(entry.amount_cents);

    writeln!(out, "      <Ntry>")?;
    writeln!(out, "        <NtryRef>{}</NtryRef>", xml_escape(&entry.entry_ref))?;
    writeln!(out, "        <Amt Ccy=\"{currency}\">{whole}.{frac:02}</Amt>")?;
    writeln!(out, "        <CdtDbtInd>{cd_indicator}</CdtDbtInd>")?;
    writeln!(out, "        <Sts>BOOK</Sts>")?;
    writeln!(out, "        <BookgDt>")?;
    if entry.booking_date.len() > 10 {
        writeln!(out, "          <DtTm>{}</DtTm>", entry.booking_date)?;
    } else {
        writeln!(out, "          <Dt>{}</Dt>", entry.booking_date)?;
    }
    writeln!(out, "        </BookgDt>")?;
    writeln!(out, "        <ValDt>")?;
    if entry.booking_date.len() > 10 {
        writeln!(out, "          <DtTm>{}</DtTm>", entry.booking_date)?;
    } else {
        writeln!(out, "          <Dt>{}</Dt>", entry.booking_date)?;
    }
    writeln!(out, "        </ValDt>")?;

    // Bank Transaction Code
    let family = if entry.is_credit { "RCDT" } else { "ICDT" };
    writeln!(out, "        <BkTxCd>")?;
    writeln!(out, "          <Domn>")?;
    writeln!(out, "            <Cd>PMNT</Cd>")?;
    writeln!(out, "            <Fmly>")?;
    writeln!(out, "              <Cd>{family}</Cd>")?;
    writeln!(out, "              <SubFmlyCd>OTHR</SubFmlyCd>")?;
    writeln!(out, "            </Fmly>")?;
    writeln!(out, "          </Domn>")?;
    writeln!(out, "        </BkTxCd>")?;

    // Entry details with description (shown to user in accounting software)
    writeln!(out, "        <NtryDtls>")?;
    writeln!(out, "          <TxDtls>")?;
    writeln!(out, "            <RmtInf>")?;
    writeln!(
        out,
        "              <Ustrd>{}</Ustrd>",
        xml_escape(&entry.description)
    )?;
    writeln!(out, "            </RmtInf>")?;
    writeln!(out, "          </TxDtls>")?;
    writeln!(out, "        </NtryDtls>")?;

    // Full blockchain reference for transaction reconstruction
    writeln!(
        out,
        "        <AddtlNtryInf>{}</AddtlNtryInf>",
        xml_escape(&entry.full_ref)
    )?;

    writeln!(out, "      </Ntry>")?;

    Ok(())
}

fn cents_to_parts(cents: i64) -> (i64, i64) {
    let abs = cents.unsigned_abs() as i64;
    (abs / 100, abs % 100)
}

/// Derive a BIC-like identifier from an IBAN.
///
/// Takes the bank code (chars 4..8) and country (chars 0..2) from the IBAN
/// and appends "2A" as location code.
fn bic_from_iban(iban: &str) -> String {
    let country = &iban[..2];
    let bank = &iban[4..8];
    format!("{bank}{country}2A")
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

/// Escape a string for use inside an XML comment (no `--` allowed).
fn xml_escape_comment(s: &str) -> String {
    s.replace("--", "- -")
}

/// Data extracted from an existing CAMT.053 file for append mode.
pub struct Camt053ParseResult {
    /// Opening balance of the existing file.
    pub opening_balance_cents: i64,
    /// Opening balance date of the existing file.
    pub opening_date: Option<String>,
    /// Set of NtryRef values already in the file (for dedup).
    pub existing_entry_refs: HashSet<String>,
    /// All existing entries (preserved in output).
    pub existing_entries: Vec<Entry>,
    /// Closing balance of the existing file (becomes new opening balance).
    pub closing_balance_cents: i64,
    /// IBAN from the existing file (consistency check).
    pub account_iban: String,
    /// Currency from the existing file (consistency check).
    pub currency: String,
    /// Date of the last entry (used for implicit start date).
    pub last_booking_date: Option<String>,
    /// Watch-only descriptors extracted from XML comments.
    pub descriptors: Vec<String>,
}

/// Parse an existing CAMT.053 XML file to extract data for append mode.
pub fn parse_camt053(xml: &str) -> Result<Camt053ParseResult> {
    use quick_xml::events::Event;
    use quick_xml::Reader;

    let mut reader = Reader::from_str(xml);

    let mut account_iban = String::new();
    let mut currency = String::new();
    let mut opening_balance_cents: i64 = 0;
    let mut opening_date: Option<String> = None;
    let mut closing_balance_cents: i64 = 0;
    let mut existing_entry_refs = HashSet::new();
    let mut existing_entries = Vec::new();
    let mut descriptors = Vec::new();

    // State tracking for nested elements
    let mut path: Vec<String> = Vec::new();

    // Temporary state for parsing entries
    let mut in_entry = false;
    let mut cur_entry_ref = String::new();
    let mut cur_amount_cents: i64 = 0;
    let mut cur_is_credit = true;
    let mut cur_booking_date = String::new();
    let mut cur_full_ref = String::new();
    let mut cur_description = String::new();
    let mut cur_is_fee = false;

    // Balance tracking
    let mut in_bal = false;
    let mut bal_code = String::new();
    let mut bal_amount_cents: i64 = 0;
    let mut bal_is_credit = true;
    let mut bal_is_btc = false;
    let mut bal_date = String::new();

    loop {
        match reader.read_event() {
            Ok(Event::Start(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();
                path.push(name.clone());

                match name.as_str() {
                    "Ntry" => {
                        in_entry = true;
                        cur_entry_ref.clear();
                        cur_amount_cents = 0;
                        cur_is_credit = true;
                        cur_booking_date.clear();
                        cur_full_ref.clear();
                        cur_description.clear();
                        cur_is_fee = false;
                    }
                    "Bal" => {
                        in_bal = true;
                        bal_code.clear();
                        bal_amount_cents = 0;
                        bal_is_credit = true;
                        bal_is_btc = false;
                        bal_date.clear();
                    }
                    "Amt" if in_bal => {
                        // Skip BTC-denominated balances
                        bal_is_btc = e.attributes().flatten().any(|a| {
                            a.key.as_ref() == b"Ccy"
                                && a.unescape_value().map(|v| v == "BTC").unwrap_or(false)
                        });
                    }
                    _ => {}
                }
            }
            Ok(Event::End(e)) => {
                let name = String::from_utf8_lossy(e.name().as_ref()).to_string();

                if name == "Ntry" && in_entry {
                    in_entry = false;
                    existing_entry_refs.insert(cur_entry_ref.clone());
                    existing_entries.push(Entry {
                        entry_ref: cur_entry_ref.clone(),
                        full_ref: cur_full_ref.clone(),
                        booking_date: cur_booking_date.clone(),
                        amount_cents: cur_amount_cents,
                        is_credit: cur_is_credit,
                        description: cur_description.clone(),
                        is_fee: cur_is_fee,
                    });
                }

                if name == "Bal" && in_bal {
                    in_bal = false;
                    let signed_balance = if bal_is_credit {
                        bal_amount_cents
                    } else {
                        -bal_amount_cents
                    };
                    if bal_code == "OPBD" {
                        opening_balance_cents = signed_balance;
                        if !bal_date.is_empty() {
                            opening_date = Some(bal_date.clone());
                        }
                    }
                    if bal_code == "CLBD" {
                        closing_balance_cents = signed_balance;
                    }
                }

                path.pop();
            }
            Ok(Event::Text(e)) => {
                let text = e.unescape().unwrap_or_default().to_string();
                let current = path.last().map(|s| s.as_str()).unwrap_or("");

                if in_entry {
                    match current {
                        "NtryRef" => cur_entry_ref = text.clone(),
                        "Amt" => {
                            cur_amount_cents = parse_amount_cents(&text)?;
                        }
                        "CdtDbtInd" => {
                            // Distinguish entry-level vs balance-level
                            if !in_bal {
                                cur_is_credit = text == "CRDT";
                            }
                        }
                        "Dt" | "DtTm" => {
                            // Only capture the first Dt/DtTm (BookgDt) for entries
                            if cur_booking_date.is_empty() && path_contains(&path, "BookgDt") {
                                cur_booking_date = text.clone();
                            }
                        }
                        "Ustrd" => cur_description = xml_unescape(&text),
                        "AddtlNtryInf" => cur_full_ref = xml_unescape(&text),
                        _ => {}
                    }
                    // Detect fee entries by entry_ref prefix
                    if current == "NtryRef" && text.contains(":fee") {
                        cur_is_fee = true;
                    }
                } else if in_bal {
                    match current {
                        "Cd" => bal_code = text.clone(),
                        "Amt" if !bal_is_btc => {
                            bal_amount_cents = parse_amount_cents(&text)?;
                        }
                        "CdtDbtInd" => bal_is_credit = text == "CRDT",
                        "Dt" | "DtTm" if path_contains(&path, "Bal") && path_contains(&path, "Dt") => {
                            let trimmed = text.trim();
                            if bal_date.is_empty() && !trimmed.is_empty() {
                                bal_date = trimmed.to_owned();
                            }
                        }
                        _ => {}
                    }
                } else {
                    // Top-level elements
                    match current {
                        "IBAN" => account_iban = text.clone(),
                        "Ccy" => {
                            if currency.is_empty() {
                                currency = text.clone();
                            }
                        }
                        _ => {}
                    }
                }
            }
            Ok(Event::Eof) => break,
            Ok(Event::Comment(e)) => {
                let comment = String::from_utf8_lossy(e.as_ref()).trim().to_string();
                // Descriptor comments contain descriptor expressions like wpkh(...), tr(...), etc.
                if comment.contains("(") && comment.contains("/*)") {
                    // Unescape comment (reverse of xml_escape_comment: "- -" → "--")
                    descriptors.push(comment.replace("- -", "--"));
                }
            }
            Err(e) => bail!("error parsing CAMT.053 XML: {e}"),
            _ => {}
        }
    }

    if account_iban.is_empty() {
        bail!("no IBAN found in CAMT.053 file");
    }
    if currency.is_empty() {
        bail!("no currency found in CAMT.053 file");
    }

    let last_booking_date = existing_entries
        .last()
        .map(|e| booking_date_to_date(&e.booking_date).to_owned());

    Ok(Camt053ParseResult {
        opening_balance_cents,
        opening_date,
        existing_entry_refs,
        existing_entries,
        closing_balance_cents,
        account_iban,
        currency,
        last_booking_date,
        descriptors,
    })
}

fn parse_amount_cents(s: &str) -> Result<i64> {
    let parts: Vec<&str> = s.split('.').collect();
    let whole: i64 = parts[0].parse().context("invalid amount whole part")?;
    let frac: i64 = if parts.len() > 1 {
        let frac_str = parts[1];
        match frac_str.len() {
            1 => frac_str.parse::<i64>().context("invalid amount fractional part")? * 10,
            2 => frac_str.parse().context("invalid amount fractional part")?,
            _ => bail!("unexpected decimal places in amount: {s}"),
        }
    } else {
        0
    };
    Ok(whole * 100 + frac)
}

fn path_contains(path: &[String], element: &str) -> bool {
    path.iter().any(|s| s == element)
}

fn xml_unescape(s: &str) -> String {
    s.replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_statement() -> Statement {
        Statement {
            account_iban: "NL00XBTC0000000000".to_owned(),
            currency: "EUR".to_owned(),
            opening_balance_cents: 0,
            opening_balance_sats: 0,
            entries: vec![
                Entry {
                    entry_ref: "100:abcdef01234567890abc:0".to_owned(),
                    full_ref: "00000000000000000000000000000000:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:0".to_owned(),
                    booking_date: "2025-01-02T12:00:00".to_owned(),
                    amount_cents: 500_000,
                    is_credit: true,
                    description: "bc1qtest - Received 0.05263158 BTC @ 95000.00".to_owned(),
                    is_fee: false,
                },
                Entry {
                    entry_ref: ":100:abcdef01234567890abc:fee".to_owned(),
                    full_ref: ":00000000000000000000000000000000:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:fee".to_owned(),
                    booking_date: "2025-01-02T12:00:00".to_owned(),
                    amount_cents: 15,
                    is_credit: false,
                    description: "Mining fee (1000 sat)".to_owned(),
                    is_fee: true,
                },
            ],
            closing_balance_cents: 499_985,
            opening_date: "2025-01-02".to_owned(),
            statement_date: "2025-01-02".to_owned(),
            statement_id: "STMT-2025-01-02".to_owned(),
            bank_name: None,
            opening_rate: None,
            descriptors: Vec::new(),
        }
    }

    #[test]
    fn generates_valid_camt053_xml() {
        let stmt = sample_statement();
        let mut buf = Vec::new();
        Camt053Exporter.write(&stmt, &mut buf).unwrap();
        let xml = String::from_utf8(buf).unwrap();

        assert!(xml.contains("urn:iso:std:iso:20022:tech:xsd:camt.053.001.02"));
        // Group header
        assert!(xml.contains("<GrpHdr>"));
        assert!(xml.contains("<MsgId>STMT-2025-01-02</MsgId>"));
        assert!(xml.contains("<PgNb>1</PgNb>"));
        assert!(xml.contains("<LastPgInd>true</LastPgInd>"));
        // Statement
        assert!(xml.contains("<ElctrncSeqNb>1</ElctrncSeqNb>"));
        assert!(xml.contains("<IBAN>NL00XBTC0000000000</IBAN>"));
        assert!(xml.contains("<Ccy>EUR</Ccy>"));
        // Servicer BIC
        assert!(xml.contains("<BIC>XBTCNL2A</BIC>"));
        assert!(xml.contains("<Cd>OPBD</Cd>"));
        assert!(xml.contains("<Cd>CLBD</Cd>"));
        assert!(xml.contains("<Amt Ccy=\"EUR\">5000.00</Amt>"));
        assert!(xml.contains("<CdtDbtInd>CRDT</CdtDbtInd>"));
        assert!(xml.contains("<Sts>BOOK</Sts>"));
        assert!(xml.contains("<NtryRef>100:abcdef01234567890abc:0</NtryRef>"));
        assert!(xml.contains("bc1qtest - Received 0.05263158 BTC @ 95000.00"));
        // Bank transaction codes
        assert!(xml.contains("<Cd>PMNT</Cd>"));
        assert!(xml.contains("<Cd>RCDT</Cd>"));  // credit entry
        assert!(xml.contains("<Cd>ICDT</Cd>"));  // debit entry (fee)
        assert!(xml.contains("<SubFmlyCd>OTHR</SubFmlyCd>"));
    }

    #[test]
    fn formats_cents_correctly() {
        assert_eq!(cents_to_parts(500_000), (5000, 0));
        assert_eq!(cents_to_parts(12345), (123, 45));
        assert_eq!(cents_to_parts(1), (0, 1));
        assert_eq!(cents_to_parts(0), (0, 0));
    }

    #[test]
    fn escapes_xml_special_chars() {
        assert_eq!(xml_escape("a<b>c&d\"e'f"), "a&lt;b&gt;c&amp;d&quot;e&apos;f");
    }

    #[test]
    fn bic_derived_from_iban() {
        assert_eq!(bic_from_iban("NL00XBTC0000000000"), "XBTCNL2A");
        assert_eq!(bic_from_iban("DE00TBTC0000000000"), "TBTCDE2A");
    }

    #[test]
    fn closing_balance_in_xml() {
        let stmt = sample_statement();
        let mut buf = Vec::new();
        Camt053Exporter.write(&stmt, &mut buf).unwrap();
        let xml = String::from_utf8(buf).unwrap();

        // Closing balance: 499985 cents = 4999.85
        assert!(xml.contains("<Amt Ccy=\"EUR\">4999.85</Amt>"));
    }

    #[test]
    fn round_trip_parse() {
        let stmt = sample_statement();
        let mut buf = Vec::new();
        Camt053Exporter.write(&stmt, &mut buf).unwrap();
        let xml = String::from_utf8(buf).unwrap();

        let parsed = parse_camt053(&xml).unwrap();

        assert_eq!(parsed.account_iban, "NL00XBTC0000000000");
        assert_eq!(parsed.currency, "EUR");
        assert_eq!(parsed.opening_balance_cents, 0);
        assert_eq!(parsed.opening_date, Some("2025-01-02".to_owned()));
        assert_eq!(parsed.closing_balance_cents, 499_985);
        assert_eq!(parsed.existing_entry_refs.len(), 2);
        assert!(parsed.existing_entry_refs.contains("100:abcdef01234567890abc:0"));
        assert!(parsed.existing_entry_refs.contains(":100:abcdef01234567890abc:fee"));
        assert_eq!(parsed.last_booking_date, Some("2025-01-02".to_owned()));

        // Verify entries were fully parsed
        assert_eq!(parsed.existing_entries.len(), 2);
        let e0 = &parsed.existing_entries[0];
        assert_eq!(e0.amount_cents, 500_000);
        assert!(e0.is_credit);
        assert_eq!(e0.booking_date, "2025-01-02T12:00:00");

        let e1 = &parsed.existing_entries[1];
        assert_eq!(e1.amount_cents, 15);
        assert!(!e1.is_credit);
        assert!(e1.is_fee);
    }

    #[test]
    fn parse_amount_cents_variants() {
        assert_eq!(parse_amount_cents("5000.00").unwrap(), 500_000);
        assert_eq!(parse_amount_cents("0.15").unwrap(), 15);
        assert_eq!(parse_amount_cents("100.5").unwrap(), 10_050);
        assert_eq!(parse_amount_cents("0.01").unwrap(), 1);
    }
}
