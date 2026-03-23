use anyhow::{Result, bail};

/// Bank code for mainnet IBANs.
const BANK_CODE_MAIN: &str = "XBTC";
/// Bank code for test-network IBANs (regtest, testnet3, testnet4, signet).
const BANK_CODE_TEST: &str = "TBTC";
/// Bank code for Lightning Network IBANs (all networks).
const BANK_CODE_LIGHTNING: &str = "LNBT";

/// Generate a deterministic IBAN from a wallet fingerprint.
///
/// Format: `<country>` + 2 check digits + bank code + 10 numeric digits.
/// Bank code is `XBTC` for mainnet, `TBTC` for test networks.
/// The 10 numeric digits = fingerprint as zero-padded decimal (max u32 = 4,294,967,295 = 10 digits).
pub fn iban_from_fingerprint(fingerprint_hex: &str, country: &str, chain: &str) -> Result<String> {
    let fp = u32::from_str_radix(fingerprint_hex, 16)
        .map_err(|_| anyhow::anyhow!("invalid fingerprint hex: {fingerprint_hex}"))?;

    let bank_code = match chain {
        "main" => BANK_CODE_MAIN,
        _ => BANK_CODE_TEST,
    };

    let fp_decimal = format!("{fp:010}");
    let bban = format!("{bank_code}{fp_decimal}");

    let iban_check = iban_mod97_check(country, &bban)?;
    Ok(format!("{country}{iban_check:02}{bban}"))
}

/// Generate a deterministic IBAN from a phoenixd node ID.
///
/// Uses the first 4 bytes (8 hex chars) of the 33-byte compressed pubkey as the
/// fingerprint and always uses the `LNBT` bank code to distinguish Lightning
/// Network accounts from on-chain Bitcoin accounts.
pub fn iban_from_node_id(node_id: &str, country: &str) -> Result<String> {
    if node_id.len() < 8 {
        bail!("node ID too short: {node_id}");
    }
    let fp = u32::from_str_radix(&node_id[..8], 16)
        .map_err(|_| anyhow::anyhow!("invalid node ID (expected hex): {node_id}"))?;
    let fp_decimal = format!("{fp:010}");
    let bban = format!("{BANK_CODE_LIGHTNING}{fp_decimal}");
    let iban_check = iban_mod97_check(country, &bban)?;
    Ok(format!("{country}{iban_check:02}{bban}"))
}

/// Compute IBAN check digits using ISO 13616 Mod-97 algorithm.
fn iban_mod97_check(country: &str, bban: &str) -> Result<u32> {
    // Move country code + "00" to end, convert letters to numbers (A=10..Z=35)
    let rearranged = format!("{bban}{country}00");
    let numeric_str = alpha_to_numeric(&rearranged);

    let remainder = mod97(&numeric_str);
    Ok(98 - remainder)
}

fn alpha_to_numeric(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphabetic() {
                // A=10, B=11, ..., Z=35
                let val = c.to_ascii_uppercase() as u32 - 'A' as u32 + 10;
                val.to_string()
            } else {
                c.to_string()
            }
        })
        .collect()
}

fn mod97(numeric_str: &str) -> u32 {
    // Process in chunks to avoid overflow
    let mut remainder: u32 = 0;
    for chunk in numeric_str.as_bytes().chunks(7) {
        let chunk_str = std::str::from_utf8(chunk).unwrap_or("0");
        let combined = format!("{remainder}{chunk_str}");
        remainder = combined.parse::<u64>().unwrap_or(0) as u32 % 97;
    }
    remainder
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_valid_iban_from_fingerprint() {
        let iban = iban_from_fingerprint("d34db33f", "NL", "main").unwrap();
        assert!(iban.starts_with("NL"));
        assert!(iban.contains("XBTC"));
        assert_eq!(iban.len(), 18); // NL(2) + check(2) + XBTC(4) + account(10) = 18
        // Verify mod-97
        let numeric = alpha_to_numeric(&format!("{}{}", &iban[4..], &iban[..4]));
        assert_eq!(mod97(&numeric), 1, "IBAN {iban} should pass mod-97 check");
    }

    #[test]
    fn testnet_uses_tbtc_bank_code() {
        let iban = iban_from_fingerprint("d34db33f", "NL", "regtest").unwrap();
        assert!(iban.contains("TBTC"));
        let numeric = alpha_to_numeric(&format!("{}{}", &iban[4..], &iban[..4]));
        assert_eq!(mod97(&numeric), 1, "IBAN {iban} should pass mod-97 check");
    }

    #[test]
    fn iban_check_digits_are_valid() {
        let iban = iban_from_fingerprint("00000001", "NL", "main").unwrap();
        let numeric = alpha_to_numeric(&format!("{}{}", &iban[4..], &iban[..4]));
        assert_eq!(mod97(&numeric), 1, "IBAN {iban} should pass mod-97 check");
    }

    #[test]
    fn different_fingerprints_produce_different_ibans() {
        let iban1 = iban_from_fingerprint("00000001", "NL", "main").unwrap();
        let iban2 = iban_from_fingerprint("00000002", "NL", "main").unwrap();
        assert_ne!(iban1, iban2);
    }

    #[test]
    fn zero_fingerprint() {
        let iban = iban_from_fingerprint("00000000", "NL", "main").unwrap();
        assert!(iban.starts_with("NL"));
        assert_eq!(iban.len(), 18);
        let numeric = alpha_to_numeric(&format!("{}{}", &iban[4..], &iban[..4]));
        assert_eq!(mod97(&numeric), 1);
    }

    #[test]
    fn rejects_invalid_hex() {
        assert!(iban_from_fingerprint("zzzzzzzz", "NL", "main").is_err());
    }

    #[test]
    fn max_fingerprint() {
        // 0xFFFFFFFF = 4_294_967_295 = exactly 10 digits
        let iban = iban_from_fingerprint("FFFFFFFF", "NL", "main").unwrap();
        assert_eq!(iban.len(), 18);
        let numeric = alpha_to_numeric(&format!("{}{}", &iban[4..], &iban[..4]));
        assert_eq!(mod97(&numeric), 1);
    }

    #[test]
    fn node_id_uses_lnbt_bank_code() {
        // ACINQ's well-known public node ID (03864ef0...)
        let node_id = "03864ef025fde8fb587d989186ce6a4a186895ee44a926bfc370e2c366597a3f8f";
        let iban = iban_from_node_id(node_id, "FR").unwrap();
        assert!(iban.starts_with("FR"));
        assert!(iban.contains("LNBT"), "IBAN should use LNBT bank code: {iban}");
        assert_eq!(iban.len(), 18);
        let numeric = alpha_to_numeric(&format!("{}{}", &iban[4..], &iban[..4]));
        assert_eq!(mod97(&numeric), 1, "IBAN {iban} should pass mod-97 check");
    }

    #[test]
    fn node_id_rejects_short_input() {
        assert!(iban_from_node_id("02eec7", "NL").is_err());
    }

    #[test]
    fn node_id_rejects_invalid_hex() {
        assert!(iban_from_node_id("zzzzzzzzzzzzzzzz", "NL").is_err());
    }
}
