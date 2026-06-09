//! Small shared helpers: segment naming, random IDs, access-key hashing.

use sha2::{Digest, Sha256};

/// Well-known control segment name (socket-free bootstrap rendezvous point).
pub const CONTROL_SEGMENT: &str = "/impulse-ring.ctl.v1";

/// Per-client reply segment, addressed by the client's bootstrap nonce.
pub fn client_segment(nonce: u64) -> String {
    format!("/impulse-ring.cli.{nonce}.v1")
}

/// Per-channel data arena.
pub fn channel_arena(channel_id: u64) -> String {
    format!("/impulse-ring.arena.{channel_id}.v1")
}

/// Per-function request arena.
pub fn function_arena(fn_id: u64) -> String {
    format!("/impulse-ring.fn.{fn_id}.v1")
}

/// A cryptographically-random `u64`, used for nonces and correlation IDs.
pub fn random_u64() -> u64 {
    let mut b = [0u8; 8];
    getrandom::getrandom(&mut b).expect("getrandom failed");
    u64::from_le_bytes(b)
}

/// Hash an access key for storage/comparison. The broker stores only the hash.
///
/// Milestone 1 uses a salted SHA-256; this is a deliberate, documented
/// placeholder for a memory-hard KDF (argon2) in a later milestone.
pub fn key_hash(key: &str) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b"impulse-ring/key/v1:");
    h.update(key.as_bytes());
    let out = h.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out);
    arr
}

/// Constant-time-ish comparison of a presented key against a stored hash.
pub fn key_matches(presented: &str, stored_hash: &[u8; 32]) -> bool {
    let h = key_hash(presented);
    // Non-short-circuiting compare.
    let mut diff = 0u8;
    for i in 0..32 {
        diff |= h[i] ^ stored_hash[i];
    }
    diff == 0
}
