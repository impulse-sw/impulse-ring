//! Layout of the well-known control segment — the socket-free bootstrap
//! rendezvous point. Both the broker and every connector must agree on this
//! layout exactly, so it lives in the shared core.
//!
//! ```text
//! offset  size  field
//! 0       8     magic        = "IMPRING\0"
//! 8       4     version      = 1
//! 12      4     broker_pid
//! 16      8     epoch        (broker start nanos; bumps each broker run)
//! 64      ...   submission ring (clients -> broker, MPSC)
//! ```

use crate::ring::{Ring, ring_bytes};
use crate::shm::Segment;
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;

/// "IMPRING\0" interpreted as a little-endian u64.
pub const CTL_MAGIC: u64 = u64::from_le_bytes(*b"IMPRING\0");
pub const CTL_VERSION: u32 = 1;

const OFF_MAGIC: usize = 0;
const OFF_VERSION: usize = 8;
const OFF_PID: usize = 12;
const OFF_EPOCH: usize = 16;

/// 64-aligned base of the submission ring within the control segment.
pub const SUBMISSION_BASE: usize = 64;
/// Submission ring data-region capacity (bytes).
pub const SUBMISSION_CAP: usize = 1 << 16;

/// Default reply-ring capacity for a per-client reply segment.
///
/// A reply (control reply or unary RPC response) is delivered as a single ring
/// record, so this is the hard ceiling on an RPC response size. 512 KiB keeps
/// typical HTTP responses (assets, JSON pages) inline; larger bodies are chunked
/// over a channel by the HTTP layer (`impulse-ring-http`).
pub const REPLY_CAP: usize = 1 << 19;
/// Default data-arena capacity for channels and function request rings.
///
/// 512 KiB by default, matching [`REPLY_CAP`]. A service may request a larger (or
/// smaller) request arena per function when exposing it; the broker clamps the
/// request with [`clamp_arena_cap`] to `[MIN_ARENA_CAP, MAX_ARENA_CAP]`.
pub const ARENA_CAP: usize = 1 << 19;

/// Smallest per-service request arena the broker will allocate.
///
/// Kept at 256 KiB so it always exceeds the HTTP layer's inline-request ceiling
/// (`impulse_ring_http::MAX_INLINE_REQUEST_BODY`, 192 KiB) plus framing — i.e. an
/// inline request body is safe even against the smallest configurable arena.
pub const MIN_ARENA_CAP: usize = 1 << 18;
/// Largest per-service request arena the broker will allocate.
///
/// 128 MiB. The ring header stores its capacity in a `u32`, and an oversized
/// shared segment is RAM in `/dev/shm`, so this is a deliberately conservative
/// ceiling on what one (mis)configured service can reserve.
pub const MAX_ARENA_CAP: usize = 1 << 27;

/// Resolve a requested arena capacity (in bytes) to a legal ring capacity.
///
/// `0` means "use the default" ([`ARENA_CAP`]). Any other value is clamped to
/// `[MIN_ARENA_CAP, MAX_ARENA_CAP]` and rounded **up** to a power of two, because
/// a ring's capacity must be a power of two ([`crate::ring::Ring::format`]).
pub fn clamp_arena_cap(requested: usize) -> usize {
  if requested == 0 {
    return ARENA_CAP;
  }
  requested.clamp(MIN_ARENA_CAP, MAX_ARENA_CAP).next_power_of_two()
}

/// Total bytes required for the control segment.
pub fn control_segment_bytes() -> usize {
  SUBMISSION_BASE + ring_bytes(SUBMISSION_CAP)
}

/// Initialize the control segment superblock and submission ring (broker side).
pub fn format_control(seg: Arc<Segment>, broker_pid: i32, epoch: u64) -> io::Result<Ring> {
  let ring = Ring::format(seg.clone(), SUBMISSION_BASE, SUBMISSION_CAP)?;
  unsafe { seg.atomic_u32_at(OFF_VERSION) }.store(CTL_VERSION, Ordering::Relaxed);
  unsafe { seg.atomic_u32_at(OFF_PID) }.store(broker_pid as u32, Ordering::Relaxed);
  unsafe { seg.atomic_u64_at(OFF_EPOCH) }.store(epoch, Ordering::Relaxed);
  // Publish magic last so a racing connector only proceeds on a ready segment.
  unsafe { seg.atomic_u64_at(OFF_MAGIC) }.store(CTL_MAGIC, Ordering::Release);
  Ok(ring)
}

/// Attach to an existing control segment, validating the superblock (client side).
pub fn attach_control(seg: Arc<Segment>) -> io::Result<Ring> {
  let magic = unsafe { seg.atomic_u64_at(OFF_MAGIC) }.load(Ordering::Acquire);
  if magic != CTL_MAGIC {
    return Err(io::Error::other("control magic mismatch (broker not running?)"));
  }
  let version = unsafe { seg.atomic_u32_at(OFF_VERSION) }.load(Ordering::Relaxed);
  if version != CTL_VERSION {
    return Err(io::Error::other(format!("control version {version} unsupported")));
  }
  Ring::attach(seg, SUBMISSION_BASE)
}

/// Read the broker PID recorded in the control superblock.
pub fn broker_pid(seg: &Segment) -> i32 {
  unsafe { seg.atomic_u32_at(OFF_PID) }.load(Ordering::Relaxed) as i32
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn clamp_arena_cap_defaults_and_bounds() {
    // 0 → default.
    assert_eq!(clamp_arena_cap(0), ARENA_CAP);
    // Below the floor is raised to the minimum.
    assert_eq!(clamp_arena_cap(1), MIN_ARENA_CAP);
    assert_eq!(clamp_arena_cap(MIN_ARENA_CAP - 1), MIN_ARENA_CAP);
    // Above the ceiling is capped at the maximum.
    assert_eq!(clamp_arena_cap(usize::MAX), MAX_ARENA_CAP);
    // In-range non-power-of-two is rounded up, never past the ceiling.
    assert_eq!(clamp_arena_cap(MIN_ARENA_CAP + 1), MIN_ARENA_CAP * 2);
    assert!(clamp_arena_cap(MAX_ARENA_CAP - 1) <= MAX_ARENA_CAP);
    // Every result is a legal ring capacity (power of two).
    for req in [0, 1, 300 * 1024, 4 * 1024 * 1024, usize::MAX] {
      assert!(clamp_arena_cap(req).is_power_of_two());
    }
  }
}
