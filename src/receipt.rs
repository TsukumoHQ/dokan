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
    /// Key from `DOKAN_RECEIPT_KEY`; if unset, a clearly-flagged PUBLIC dev key (don't trust
    /// those receipts — they are forgeable). Reaching the fallback in the daemon implies the
    /// `DOKAN_DEV_INSECURE` escape hatch is set; `preflight_security` refuses to boot otherwise.
    pub fn from_env() -> Self {
        let key = match std::env::var("DOKAN_RECEIPT_KEY") {
            Ok(k) if !k.is_empty() => k,
            _ => {
                if crate::crypto::dev_insecure() {
                    tracing::warn!(
                        "DOKAN_RECEIPT_KEY unset and DOKAN_DEV_INSECURE set — receipts HMAC'd with \
                         a PUBLIC dev key; they are NOT tamper-evident (anyone can forge them). \
                         Dev/test only; never run this in production."
                    );
                } else {
                    tracing::error!(
                        "DOKAN_RECEIPT_KEY unset — receipts would use a public dev key and be \
                         forgeable. Set DOKAN_RECEIPT_KEY, or DOKAN_DEV_INSECURE=1 for local dev."
                    );
                }
                "dokan-dev-receipt-key".to_string()
            }
        };
        Self { key: key.into_bytes() }
    }

    /// Whether a non-empty `DOKAN_RECEIPT_KEY` is configured (vs. the public dev fallback).
    /// Used by the boot-time fail-closed preflight.
    pub fn key_configured() -> bool {
        std::env::var("DOKAN_RECEIPT_KEY").map(|k| !k.is_empty()).unwrap_or(false)
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
