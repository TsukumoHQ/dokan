//! Secrets-at-rest encryption (T2). Secret values are sealed with ChaCha20-Poly1305 under a
//! key derived from DOKAN_SECRET_KEY before they touch Postgres, so a DB dump alone never
//! yields plaintext keys. Back-compatible: an unprefixed value is treated as legacy
//! plaintext, and with no key configured we store plaintext (loudly warned).

use base64::Engine;
use chacha20poly1305::aead::rand_core::RngCore;
use chacha20poly1305::aead::{Aead, AeadCore, KeyInit, OsRng};
use chacha20poly1305::ChaCha20Poly1305;
use sha2::{Digest, Sha256};

const PREFIX: &str = "enc:v1:";
const NONCE_LEN: usize = 12;

/// Unguessable 128-bit token (hex) — the capability in a webhook URL `/hook/<token>`.
/// Uses the same OS CSPRNG as nonce generation; no extra dependency.
pub fn random_token() -> String {
    let mut b = [0u8; 16];
    OsRng.fill_bytes(&mut b);
    b.iter().map(|x| format!("{x:02x}")).collect()
}

#[derive(Clone)]
pub struct SecretCrypto {
    cipher: Option<ChaCha20Poly1305>,
}

impl SecretCrypto {
    pub fn from_env() -> Self {
        match std::env::var("DOKAN_SECRET_KEY") {
            Ok(k) if !k.is_empty() => {
                // Derive a 32-byte key from the passphrase (SHA-256 → 32-byte ChaCha key).
                let digest = Sha256::digest(k.as_bytes());
                Self { cipher: Some(ChaCha20Poly1305::new(&digest)) }
            }
            _ => {
                tracing::warn!("DOKAN_SECRET_KEY unset — secrets stored in plaintext at rest");
                Self { cipher: None }
            }
        }
    }

    /// Seal a value for storage. No-op (returns plaintext) when no key is configured.
    pub fn encrypt(&self, plain: &str) -> String {
        let Some(cipher) = &self.cipher else {
            return plain.to_string();
        };
        let nonce = ChaCha20Poly1305::generate_nonce(&mut OsRng);
        let ct = match cipher.encrypt(&nonce, plain.as_bytes()) {
            Ok(c) => c,
            Err(_) => return plain.to_string(),
        };
        let mut blob = nonce.to_vec();
        blob.extend_from_slice(&ct);
        format!("{PREFIX}{}", base64::engine::general_purpose::STANDARD.encode(blob))
    }

    /// Open a stored value. An unprefixed value is legacy plaintext. Returns "" if it's
    /// sealed but we can't open it (wrong/missing key) — a job sees an empty secret rather
    /// than a panic or a leak.
    pub fn decrypt(&self, stored: &str) -> String {
        let Some(rest) = stored.strip_prefix(PREFIX) else {
            return stored.to_string(); // legacy plaintext
        };
        let Some(cipher) = &self.cipher else {
            return String::new();
        };
        let Ok(blob) = base64::engine::general_purpose::STANDARD.decode(rest) else {
            return String::new();
        };
        if blob.len() < NONCE_LEN {
            return String::new();
        }
        let (nonce, ct) = blob.split_at(NONCE_LEN);
        cipher
            .decrypt(nonce.into(), ct)
            .ok()
            .and_then(|b| String::from_utf8(b).ok())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keyed() -> SecretCrypto {
        let digest = Sha256::digest(b"test-pass");
        SecretCrypto { cipher: Some(ChaCha20Poly1305::new(&digest)) }
    }

    #[test]
    fn round_trips_and_seals() {
        let c = keyed();
        let sealed = c.encrypt("sk-secret");
        assert!(sealed.starts_with(PREFIX), "sealed + prefixed");
        assert!(!sealed.contains("sk-secret"), "plaintext not visible");
        assert_eq!(c.decrypt(&sealed), "sk-secret", "round-trips");
        assert_eq!(c.decrypt("legacy-plain"), "legacy-plain", "unprefixed = legacy plaintext");
    }

    #[test]
    fn no_key_is_plaintext_passthrough() {
        let c = SecretCrypto { cipher: None };
        assert_eq!(c.encrypt("x"), "x");
        assert_eq!(c.decrypt("x"), "x");
    }
}
