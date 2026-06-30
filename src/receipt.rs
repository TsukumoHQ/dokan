//! Reproducibility receipts. A receipt binds a run's inputs (image digest, source, input,
//! secrets generation) to its output (result hash, exit) so a recalled run can be shown sound —
//! not a stale or altered cache hit. Two signatures, complementary:
//!
//! - **HMAC-SHA256** (symmetric) — tamper-evidence for holders of `DOKAN_RECEIPT_KEY`. It is NOT
//!   a third-party-verifiable signature (Turborepo's model; they disclaim it as "not a security
//!   feature"). Kept for back-compat + cheap key-holder checks.
//! - **Ed25519** (asymmetric) over an in-toto Statement in a DSSE envelope — the real public-verify
//!   story: anyone with the PUBLIC key (`/api/receipt/pubkey`, `dokan pubkey`) can verify, no shared
//!   secret. This is what licenses calling a receipt "verifiable" to a third party.
//!
//! Only meaningful for network-disabled (deterministic) scripts, where the output really is a pure
//! function of the inputs.

use ed25519_dalek::{Signer as _, SigningKey, VerifyingKey};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

/// DSSE payloadType for a dokan run statement (the value bound into the PAE preamble).
pub const DSSE_PAYLOAD_TYPE: &str = "application/vnd.in-toto+json";
/// in-toto predicateType for a dokan run.
pub const PREDICATE_TYPE: &str = "https://dokan.dev/Run/v1";

#[derive(Clone)]
pub struct Signer {
    key: Vec<u8>,
    ed: SigningKey,
}

impl Signer {
    /// HMAC key from `DOKAN_RECEIPT_KEY` + Ed25519 secret from `DOKAN_RECEIPT_ED25519_SECRET`
    /// (base64 32-byte seed). Either unset → a clearly-flagged PUBLIC dev key (don't trust those
    /// receipts — they are forgeable). Reaching a fallback in the daemon implies the
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
        Self { key: key.into_bytes(), ed: ed_from_env() }
    }

    /// Whether a non-empty `DOKAN_RECEIPT_KEY` is configured (vs. the public dev fallback).
    /// Used by the boot-time fail-closed preflight.
    pub fn key_configured() -> bool {
        std::env::var("DOKAN_RECEIPT_KEY").map(|k| !k.is_empty()).unwrap_or(false)
    }

    /// Whether a usable (base64, 32-byte) `DOKAN_RECEIPT_ED25519_SECRET` is configured (vs. the
    /// public dev fallback). Used by the boot-time fail-closed preflight.
    pub fn ed_key_configured() -> bool {
        std::env::var("DOKAN_RECEIPT_ED25519_SECRET")
            .ok()
            .and_then(|s| ed_seed_from_b64(&s))
            .is_some()
    }

    /// HMAC-SHA256 over the canonical binding payload (hex). Key-holder tamper-evidence.
    pub fn sign(&self, payload: &str) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).expect("hmac accepts any key len");
        mac.update(payload.as_bytes());
        hex(&mac.finalize().into_bytes())
    }

    /// Ed25519 signature (hex of the 64-byte sig) over an arbitrary message. Used to sign the
    /// DSSE pre-authentication encoding of an in-toto Statement.
    pub fn ed_sign(&self, msg: &[u8]) -> String {
        hex(&self.ed.sign(msg).to_bytes())
    }

    /// The Ed25519 PUBLIC key (base64, 32 bytes) — safe to publish; third parties verify with it.
    pub fn ed_public_b64(&self) -> String {
        use base64::Engine;
        base64::engine::general_purpose::STANDARD.encode(self.ed.verifying_key().to_bytes())
    }

    /// Short stable key id (first 16 hex of SHA-256 over the public key) for the DSSE `keyid`.
    pub fn ed_keyid(&self) -> String {
        let h = sha256_hex(&self.ed.verifying_key().to_bytes());
        h[..16].to_string()
    }
}

/// DSSE Pre-Authentication Encoding (PAE):
/// `"DSSEv1" SP len(type) SP type SP len(body) SP body`, where lengths are ASCII decimal of the
/// UTF-8 byte length and SP is a single 0x20. This is the exact byte string that gets signed, so a
/// verifier reconstructs it from (payloadType, payload) before checking the Ed25519 signature.
pub fn dsse_pae(payload_type: &str, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + payload_type.len() + 32);
    out.extend_from_slice(b"DSSEv1 ");
    out.extend_from_slice(payload_type.len().to_string().as_bytes());
    out.push(b' ');
    out.extend_from_slice(payload_type.as_bytes());
    out.push(b' ');
    out.extend_from_slice(payload.len().to_string().as_bytes());
    out.push(b' ');
    out.extend_from_slice(payload);
    out
}

/// Verify a DSSE/Ed25519 signature with a PUBLIC key only — the third-party verification path.
/// `public_b64` = base64 32-byte verifying key, `sig_hex` = hex 64-byte signature over
/// `dsse_pae(payload_type, payload)`. Returns false on any malformed input (never panics).
pub fn ed_verify(public_b64: &str, payload_type: &str, payload: &[u8], sig_hex: &str) -> bool {
    use base64::Engine;
    let Ok(pk_bytes) = base64::engine::general_purpose::STANDARD.decode(public_b64) else {
        return false;
    };
    let Ok(pk_arr): Result<[u8; 32], _> = pk_bytes.try_into() else {
        return false;
    };
    let Ok(vk) = VerifyingKey::from_bytes(&pk_arr) else {
        return false;
    };
    let Some(sig_bytes) = unhex(sig_hex) else {
        return false;
    };
    let Ok(sig_arr): Result<[u8; 64], _> = sig_bytes.try_into() else {
        return false;
    };
    let sig = ed25519_dalek::Signature::from_bytes(&sig_arr);
    vk.verify_strict(&dsse_pae(payload_type, payload), &sig).is_ok()
}

impl Signer {
    /// Recompute the HMAC binding from a receipt's stored fields and compare it to the stored
    /// `sig`. True iff this signer's key reproduces it — the key-holder tamper-evidence check.
    /// Returns false if any field is missing/malformed.
    pub fn verify_hmac(&self, receipt: &serde_json::Value) -> bool {
        let s = |k: &str| receipt.get(k).and_then(|v| v.as_str()).map(str::to_string);
        let n = |k: &str| receipt.get(k).and_then(|v| v.as_i64());
        let b = |k: &str| receipt.get(k).and_then(|v| v.as_bool());
        let (Some(digest), Some(source_sha), Some(input_sha), Some(output_hash), Some(sig)) =
            (s("image_digest"), s("source_sha256"), s("input_sha256"), s("output_sha256"), s("sig"))
        else {
            return false;
        };
        let (Some(secrets_gen), Some(exit_code), Some(network)) =
            (n("secrets_generation"), n("exit"), b("network"))
        else {
            return false;
        };
        let blobs_canon = crate::mcp::canonical_input_blobs(receipt.get("input_blobs"));
        let output_blobs_canon = crate::mcp::canonical_input_blobs(receipt.get("output_blobs"));
        let payload = format!(
            "v1|{digest}|{source_sha}|{input_sha}|{secrets_gen}|{output_hash}|{exit_code}|{network}|{blobs_canon}|{output_blobs_canon}",
        );
        // Constant-ish compare is unnecessary here (both sides are public hex), but keep it simple.
        self.sign(&payload) == sig
    }
}

/// Result of public (key-free) receipt verification — the offline `verify` path.
#[derive(Debug, Clone)]
pub struct VerifyReport {
    /// The Ed25519/DSSE signature verifies against the receipt's embedded public key.
    pub ed25519_valid: bool,
    /// The signed in-toto Statement's `output_sha256` matches the receipt's top-level claim
    /// (the envelope attests THIS receipt's output, not some other run's).
    pub binding_consistent: bool,
    /// Signed hermetic claim (network was disabled) → output is a pure function of inputs.
    pub hermetic: bool,
    /// DSSE keyid the receipt was signed under.
    pub keyid: String,
    /// A trust anchor is configured (DOKAN_TRUSTED_RECEIPT_KEYS set) — authenticity is enforced.
    pub trust_enforced: bool,
    /// The receipt's embedded public key is in the trusted set. Meaningful only when
    /// `trust_enforced`; without a trust anchor the embedded key is taken on faith
    /// (tamper-EVIDENT, not authenticated). (TSU — Ed25519 trust-anchor + rotation)
    pub pinned: bool,
}

impl VerifyReport {
    /// A receipt is sound iff its Ed25519 signature verifies AND the signed statement is bound to
    /// the receipt's claimed output AND — when a trust anchor is configured — the signing key is
    /// trusted. Without a trust anchor (no DOKAN_TRUSTED_RECEIPT_KEYS) it stays tamper-evident:
    /// valid+bound passes, since there's no authenticity policy to enforce (back-compat).
    pub fn ok(&self) -> bool {
        self.ed25519_valid && self.binding_consistent && (!self.trust_enforced || self.pinned)
    }
}

/// The configured trust anchor: base64 Ed25519 public keys allowed to sign receipts, from
/// `DOKAN_TRUSTED_RECEIPT_KEYS` (comma-separated). None when unset/empty → no authenticity
/// policy (verify stays tamper-evident). Rotation = list the current PLUS retired-but-trusted
/// public keys here; the receipt's keyid identifies which signed it. (TSU)
pub fn trusted_receipt_keys() -> Option<std::collections::HashSet<String>> {
    let raw = std::env::var("DOKAN_TRUSTED_RECEIPT_KEYS").ok()?;
    let set: std::collections::HashSet<String> =
        raw.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect();
    if set.is_empty() { None } else { Some(set) }
}

/// Verify a receipt with NO key material — the third-party path. Checks the DSSE/Ed25519
/// signature against the receipt's embedded public key, then that the signed statement attests
/// the same output the receipt claims at top level. Never panics on a malformed receipt.
pub fn verify_receipt(receipt: &serde_json::Value) -> VerifyReport {
    verify_receipt_with(receipt, trusted_receipt_keys().as_ref())
}

/// Core of `verify_receipt` with the trust anchor passed explicitly (so it's unit-testable
/// without touching the process env). `trusted` = the allowed signing public keys, or None for
/// no authenticity policy (tamper-evident only).
pub fn verify_receipt_with(
    receipt: &serde_json::Value,
    trusted: Option<&std::collections::HashSet<String>>,
) -> VerifyReport {
    use base64::Engine;
    let dsse = receipt.get("dsse");
    let public_b64 = receipt
        .get("ed25519")
        .and_then(|e| e.get("public_key"))
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let payload_type = dsse
        .and_then(|d| d.get("payloadType"))
        .and_then(|v| v.as_str())
        .unwrap_or(DSSE_PAYLOAD_TYPE);
    let payload_b64 = dsse
        .and_then(|d| d.get("payload"))
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let sig = dsse
        .and_then(|d| d.get("signatures"))
        .and_then(|s| s.get(0))
        .and_then(|s| s.get("sig"))
        .and_then(|v| v.as_str())
        .unwrap_or_default();
    let keyid = dsse
        .and_then(|d| d.get("signatures"))
        .and_then(|s| s.get(0))
        .and_then(|s| s.get("keyid"))
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_string();

    let payload_bytes = base64::engine::general_purpose::STANDARD
        .decode(payload_b64)
        .unwrap_or_default();
    let ed25519_valid =
        !payload_bytes.is_empty() && ed_verify(public_b64, payload_type, &payload_bytes, sig);

    // Binding coherence: the signed statement must attest the receipt's own claimed output AND the
    // served artifact handles. Binding `output_sha256` alone is NOT enough — the bytes a holder
    // actually downloads come from the top-level `input_blobs` / `output_blobs` handles, so a relay
    // could substitute artifacts (swap those handles) while `output_sha256` stays intact and verify
    // still passed. Require the top-level blob sets to equal the SIGNED predicate's (canonical,
    // order-independent). The HMAC path already binds these; this closes the public-verify gap.
    let claimed_out = receipt.get("output_sha256").and_then(|v| v.as_str());
    let signed_pred = serde_json::from_slice::<serde_json::Value>(&payload_bytes)
        .ok()
        .and_then(|st| st.get("predicate").cloned());
    let binding_consistent = match (claimed_out, signed_pred.as_ref()) {
        (Some(out), Some(pred)) => {
            use crate::mcp::canonical_input_blobs;
            pred.get("output_sha256").and_then(|v| v.as_str()) == Some(out)
                && canonical_input_blobs(receipt.get("input_blobs"))
                    == canonical_input_blobs(pred.get("input_blobs"))
                && canonical_input_blobs(receipt.get("output_blobs"))
                    == canonical_input_blobs(pred.get("output_blobs"))
        }
        _ => false,
    };
    let hermetic = receipt.get("hermetic").and_then(|v| v.as_bool()).unwrap_or(false);

    // Trust-anchor pinning: when a trust set is configured, the embedded signing key must be one
    // we trust (authenticity). None → no policy, pinned=false but ok() ignores it.
    let trust_enforced = trusted.is_some();
    let pinned = trusted.map(|set| !public_b64.is_empty() && set.contains(public_b64)).unwrap_or(false);

    VerifyReport { ed25519_valid, binding_consistent, hermetic, keyid, trust_enforced, pinned }
}

/// Offline verification verdict — the human-facing outcome of `dokan verify`. Distinct from a
/// bare bool so the CLI can say WHY: a missing signature ("can't prove this offline") is not
/// the same as a failed one ("this was altered").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Ed25519 signature valid AND bound to the receipt's claimed output.
    Verified,
    /// A signature is present but does not verify, or attests a different output → altered.
    Tampered,
    /// The signature is VALID and output-bound, but the signing key is NOT in the configured
    /// trust anchor (DOKAN_TRUSTED_RECEIPT_KEYS) — authentic-looking but not from a key we trust
    /// (a forger can self-sign). Fail-closed. Only possible when a trust anchor is set. (TSU)
    Untrusted,
    /// No Ed25519/DSSE signature material to check offline (e.g. an HMAC-only/legacy receipt).
    /// Not a failure — just unprovable by the key-free path; use `reproduce` or a HMAC-key check.
    Inconclusive,
}

impl Verdict {
    pub fn label(self) -> &'static str {
        match self {
            Verdict::Verified => "VERIFIED",
            Verdict::Tampered => "TAMPERED",
            Verdict::Untrusted => "UNTRUSTED",
            Verdict::Inconclusive => "INCONCLUSIVE",
        }
    }
    /// Process exit code for the CLI: 0 verified, 1 tampered, 2 inconclusive, 3 untrusted-key.
    pub fn exit_code(self) -> i32 {
        match self {
            Verdict::Verified => 0,
            Verdict::Tampered => 1,
            Verdict::Inconclusive => 2,
            Verdict::Untrusted => 3,
        }
    }
}

/// True iff the receipt carries the Ed25519/DSSE material the offline path needs: a non-empty
/// signature, embedded public key, and signed payload. False → nothing to verify key-free.
pub fn has_ed25519_signature(receipt: &serde_json::Value) -> bool {
    let nonempty = |v: Option<&serde_json::Value>| {
        v.and_then(|x| x.as_str()).map(|s| !s.is_empty()).unwrap_or(false)
    };
    let dsse = receipt.get("dsse");
    let sig = dsse
        .and_then(|d| d.get("signatures"))
        .and_then(|s| s.get(0))
        .and_then(|s| s.get("sig"));
    let pk = receipt.get("ed25519").and_then(|e| e.get("public_key"));
    let payload = dsse.and_then(|d| d.get("payload"));
    nonempty(sig) && nonempty(pk) && nonempty(payload)
}

/// Classify a receipt offline (no key material): no signature → INCONCLUSIVE; signature valid
/// and output-bound → VERIFIED; otherwise → TAMPERED. Returns the report too for detail output.
pub fn classify(receipt: &serde_json::Value) -> (Verdict, VerifyReport) {
    classify_with(receipt, trusted_receipt_keys().as_ref())
}

/// Core of `classify` with the trust anchor passed explicitly (unit-testable without env).
pub fn classify_with(
    receipt: &serde_json::Value,
    trusted: Option<&std::collections::HashSet<String>>,
) -> (Verdict, VerifyReport) {
    let rep = verify_receipt_with(receipt, trusted);
    let verdict = if !has_ed25519_signature(receipt) {
        Verdict::Inconclusive
    } else if rep.ok() {
        Verdict::Verified
    } else if rep.ed25519_valid && rep.binding_consistent && rep.trust_enforced && !rep.pinned {
        // Valid + output-bound, but the signer isn't in the trust anchor → authentic-looking
        // but not trusted (distinct from an altered receipt).
        Verdict::Untrusted
    } else {
        Verdict::Tampered
    };
    (verdict, rep)
}

/// Build the Ed25519 signing key: from `DOKAN_RECEIPT_ED25519_SECRET` (base64 32-byte seed), else
/// a deterministic, clearly-flagged PUBLIC dev key (forgeable — dev/test only).
fn ed_from_env() -> SigningKey {
    if let Some(seed) = std::env::var("DOKAN_RECEIPT_ED25519_SECRET")
        .ok()
        .and_then(|s| ed_seed_from_b64(&s))
    {
        return SigningKey::from_bytes(&seed);
    }
    if crate::crypto::dev_insecure() {
        tracing::warn!(
            "DOKAN_RECEIPT_ED25519_SECRET unset/invalid and DOKAN_DEV_INSECURE set — receipts \
             Ed25519-signed with a PUBLIC dev key; the public-verify story is void (anyone can \
             forge). Dev/test only; never run this in production."
        );
    } else {
        tracing::error!(
            "DOKAN_RECEIPT_ED25519_SECRET unset/invalid — receipts would be signed with a public \
             dev key and be forgeable. Set it (base64 32-byte seed), or DOKAN_DEV_INSECURE=1 for \
             local dev."
        );
    }
    // Deterministic dev seed so dev receipts verify consistently against the dev pubkey.
    let mut seed = [0u8; 32];
    let d = Sha256::digest(b"dokan-dev-receipt-ed25519");
    seed.copy_from_slice(&d);
    SigningKey::from_bytes(&seed)
}

/// Decode a base64 string to a 32-byte Ed25519 seed, or None if malformed / wrong length.
fn ed_seed_from_b64(s: &str) -> Option<[u8; 32]> {
    use base64::Engine;
    let bytes = base64::engine::general_purpose::STANDARD.decode(s.trim()).ok()?;
    bytes.try_into().ok()
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

/// Decode a hex string to bytes, or None if it has an odd length or a non-hex digit.
fn unhex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dev_signer() -> Signer {
        Signer { key: b"k1".to_vec(), ed: ed_from_env() }
    }

    #[test]
    fn sign_is_deterministic_and_key_sensitive() {
        let a = Signer { key: b"k1".to_vec(), ed: ed_from_env() };
        let b = Signer { key: b"k2".to_vec(), ed: ed_from_env() };
        assert_eq!(a.sign("x"), a.sign("x"), "same key+payload -> same sig");
        assert_ne!(a.sign("x"), a.sign("y"), "payload matters");
        assert_ne!(a.sign("x"), b.sign("x"), "key matters");
        assert_eq!(sha256_hex(b""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    #[test]
    fn dsse_pae_is_canonical() {
        // "DSSEv1 SP 4 SP test SP 5 SP hello"
        assert_eq!(dsse_pae("test", b"hello"), b"DSSEv1 4 test 5 hello");
    }

    #[test]
    fn ed25519_round_trips_with_public_key_only() {
        let s = dev_signer();
        let payload = br#"{"_type":"https://in-toto.io/Statement/v1"}"#;
        let sig = s.ed_sign(&dsse_pae(DSSE_PAYLOAD_TYPE, payload));
        let pubkey = s.ed_public_b64();
        // Third party with ONLY the public key verifies.
        assert!(ed_verify(&pubkey, DSSE_PAYLOAD_TYPE, payload, &sig), "valid sig verifies");
        // A flipped payload byte is rejected.
        assert!(
            !ed_verify(&pubkey, DSSE_PAYLOAD_TYPE, br#"{"_type":"https://in-toto.io/Statement/v2"}"#, &sig),
            "tampered payload rejected"
        );
        // A flipped signature is rejected.
        let mut bad = sig.clone();
        bad.replace_range(0..2, if &sig[0..2] == "00" { "ff" } else { "00" });
        assert!(!ed_verify(&pubkey, DSSE_PAYLOAD_TYPE, payload, &bad), "tampered sig rejected");
    }

    // Build a fully-signed receipt (mirrors exec.rs) attesting `output`.
    fn signed_receipt(output: &str) -> serde_json::Value {
        use base64::Engine;
        let s = dev_signer();
        let statement = serde_json::json!({
            "_type": "https://in-toto.io/Statement/v1",
            "predicate": { "output_sha256": output }
        });
        let payload_bytes = serde_json::to_vec(&statement).unwrap();
        let payload_b64 = base64::engine::general_purpose::STANDARD.encode(&payload_bytes);
        let sig = s.ed_sign(&dsse_pae(DSSE_PAYLOAD_TYPE, &payload_bytes));
        serde_json::json!({
            "output_sha256": output,
            "hermetic": true,
            "dsse": {
                "payloadType": DSSE_PAYLOAD_TYPE,
                "payload": payload_b64,
                "signatures": [ { "keyid": s.ed_keyid(), "sig": sig } ]
            },
            "ed25519": { "public_key": s.ed_public_b64(), "keyid": s.ed_keyid() }
        })
    }

    #[test]
    fn classify_valid_receipt_is_verified() {
        let r = signed_receipt("abc123");
        let (v, rep) = classify(&r);
        assert_eq!(v, Verdict::Verified, "valid signed+bound receipt");
        assert_eq!(v.exit_code(), 0);
        assert!(rep.ed25519_valid && rep.binding_consistent);
    }

    #[test]
    fn classify_output_mismatch_is_tampered() {
        let mut r = signed_receipt("abc123");
        // Alter the claimed output so the signed statement no longer binds it.
        r["output_sha256"] = serde_json::json!("deadbeef");
        let (v, _) = classify(&r);
        assert_eq!(v, Verdict::Tampered, "binding mismatch → tampered");
        assert_eq!(v.exit_code(), 1);
    }

    #[test]
    fn classify_flipped_signature_is_tampered() {
        let mut r = signed_receipt("abc123");
        let sig = r["dsse"]["signatures"][0]["sig"].as_str().unwrap().to_string();
        let flipped = format!("{}{}", if &sig[0..2] == "00" { "ff" } else { "00" }, &sig[2..]);
        r["dsse"]["signatures"][0]["sig"] = serde_json::json!(flipped);
        let (v, _) = classify(&r);
        assert_eq!(v, Verdict::Tampered, "bad signature → tampered");
    }

    // A signed receipt that also attests an output_blobs handle (both top-level + signed predicate).
    fn signed_receipt_with_output_blob(output: &str, name: &str, sha: &str) -> serde_json::Value {
        use base64::Engine;
        let s = dev_signer();
        let blobs = serde_json::json!({ name: sha });
        let statement = serde_json::json!({
            "_type": "https://in-toto.io/Statement/v1",
            "predicate": { "output_sha256": output, "input_blobs": serde_json::Value::Null, "output_blobs": blobs }
        });
        let payload_bytes = serde_json::to_vec(&statement).unwrap();
        let payload_b64 = base64::engine::general_purpose::STANDARD.encode(&payload_bytes);
        let sig = s.ed_sign(&dsse_pae(DSSE_PAYLOAD_TYPE, &payload_bytes));
        serde_json::json!({
            "output_sha256": output,
            "hermetic": true,
            "input_blobs": serde_json::Value::Null,
            "output_blobs": blobs,
            "dsse": { "payloadType": DSSE_PAYLOAD_TYPE, "payload": payload_b64,
                      "signatures": [ { "keyid": s.ed_keyid(), "sig": sig } ] },
            "ed25519": { "public_key": s.ed_public_b64(), "keyid": s.ed_keyid() }
        })
    }

    #[test]
    fn classify_swapped_served_blob_is_tampered() {
        // A legit receipt with an output artifact verifies.
        let r = signed_receipt_with_output_blob("abc123", "report.csv", "sha-good");
        assert_eq!(classify(&r).0, Verdict::Verified, "legit artifact receipt verifies");
        // Swapping the SERVED top-level handle (artifact substitution) while the signed statement is
        // untouched must FAIL — binding output_sha256 alone would NOT catch this.
        let mut swapped = r.clone();
        swapped["output_blobs"]["report.csv"] = serde_json::json!("sha-EVIL");
        let (v, rep) = classify(&swapped);
        assert!(!rep.binding_consistent, "swapped served blob breaks the binding");
        assert_eq!(v, Verdict::Tampered, "substituted artifact → tampered");
    }

    #[test]
    fn classify_no_signature_is_inconclusive() {
        // Legacy / HMAC-only receipt: nothing for the key-free path to check.
        let r = serde_json::json!({ "output_sha256": "abc123", "sig": "hmac-only", "alg": "hmac-sha256" });
        let (v, _) = classify(&r);
        assert_eq!(v, Verdict::Inconclusive, "no ed25519 material → inconclusive");
        assert_eq!(v.exit_code(), 2);
        assert!(!has_ed25519_signature(&r));
    }

    // ---- Trust anchor + rotation (env-free via *_with so parallel tests don't race on env) ----

    fn keyset(keys: &[&str]) -> std::collections::HashSet<String> {
        keys.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn trust_anchor_unset_stays_tamper_evident() {
        let r = signed_receipt("abc123");
        let (v, rep) = classify_with(&r, None);
        assert_eq!(v, Verdict::Verified, "no trust anchor → valid+bound passes (tamper-evident)");
        assert!(!rep.trust_enforced, "no policy enforced");
        assert!(!rep.pinned);
    }

    #[test]
    fn trust_anchor_pins_the_trusted_signer() {
        let r = signed_receipt("abc123");
        let pubkey = dev_signer().ed_public_b64();
        let (v, rep) = classify_with(&r, Some(&keyset(&[&pubkey])));
        assert_eq!(v, Verdict::Verified, "signer in the trust anchor → verified");
        assert!(rep.trust_enforced && rep.pinned);
        assert_eq!(v.exit_code(), 0);
    }

    #[test]
    fn trust_anchor_rejects_untrusted_signer() {
        // Valid signature + output-bound, but the signing key is NOT in the trust anchor:
        // authentic-looking but not ours → UNTRUSTED (fail-closed), distinct from TAMPERED.
        let r = signed_receipt("abc123");
        let (v, rep) = classify_with(&r, Some(&keyset(&["some-other-pubkey-b64"])));
        assert_eq!(v, Verdict::Untrusted, "valid sig, untrusted key → untrusted");
        assert!(rep.trust_enforced && !rep.pinned);
        assert!(rep.ed25519_valid && rep.binding_consistent, "the sig itself is valid");
        assert_eq!(v.exit_code(), 3);
        assert!(!rep.ok(), "fail-closed: not sound under an enforced anchor");
    }

    #[test]
    fn trust_anchor_rotation_keeps_old_keys_verifying() {
        // Rotation = list the current PLUS retired public keys. A receipt signed by the (now
        // retired) key still verifies because its pubkey is still trusted + carried in-receipt.
        let r = signed_receipt("abc123");
        let retired = dev_signer().ed_public_b64();
        let (v, _) = classify_with(&r, Some(&keyset(&["new-primary-pubkey", &retired])));
        assert_eq!(v, Verdict::Verified, "retired-but-trusted signer still verifies post-rotation");
    }
}
