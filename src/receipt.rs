//! Tamper-evident reproducibility receipts. A receipt binds a run's inputs (image digest,
//! source, input, secrets generation) to its output (result hash, exit) under a keyed HMAC,
//! so anyone holding `DOKAN_RECEIPT_KEY` can DETECT tampering and verify that a recalled run
//! is sound — not a stale or altered cache hit. The HMAC is symmetric: it is tamper-evidence
//! for key-holders, NOT a third-party-verifiable signature (that needs an asymmetric scheme —
//! on the roadmap). Only meaningful for network-disabled (deterministic) scripts, where the
//! output really is a pure function of the inputs.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

#[derive(Clone)]
pub struct Signer {
    key: Vec<u8>,
}

impl Signer {
    /// Key from `DOKAN_RECEIPT_KEY`; if unset, a clearly-flagged dev key (don't trust those
    /// receipts across hosts).
    pub fn from_env() -> Self {
        let key = std::env::var("DOKAN_RECEIPT_KEY").unwrap_or_else(|_| {
            tracing::warn!("DOKAN_RECEIPT_KEY unset — receipts HMAC'd with a non-secret dev key; tamper-evidence is void");
            "dokan-dev-receipt-key".to_string()
        });
        Self { key: key.into_bytes() }
    }

    pub fn sign(&self, payload: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).expect("hmac accepts any key len");
        mac.update(payload.as_bytes());
        hex(&mac.finalize().into_bytes())
    }
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(bytes);
    hex(&h.finalize())
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_is_deterministic_and_key_sensitive() {
        let a = Signer { key: b"k1".to_vec() };
        let b = Signer { key: b"k2".to_vec() };
        assert_eq!(a.sign("x"), a.sign("x"), "same key+payload -> same sig");
        assert_ne!(a.sign("x"), a.sign("y"), "payload matters");
        assert_ne!(a.sign("x"), b.sign("x"), "key matters");
        assert_eq!(sha256_hex(b""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }
}
