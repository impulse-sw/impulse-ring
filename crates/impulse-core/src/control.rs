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

use crate::ring::{ring_bytes, Ring};
use crate::shm::Segment;
use std::io;
use std::sync::atomic::Ordering;
use std::sync::Arc;

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
pub const REPLY_CAP: usize = 1 << 16;
/// Default data-arena capacity for channels and function request rings.
pub const ARENA_CAP: usize = 1 << 18;

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
        return Err(io::Error::other(
            "control magic mismatch (broker not running?)",
        ));
    }
    let version = unsafe { seg.atomic_u32_at(OFF_VERSION) }.load(Ordering::Relaxed);
    if version != CTL_VERSION {
        return Err(io::Error::other(format!(
            "control version {version} unsupported"
        )));
    }
    Ring::attach(seg, SUBMISSION_BASE)
}

/// Read the broker PID recorded in the control superblock.
pub fn broker_pid(seg: &Segment) -> i32 {
    unsafe { seg.atomic_u32_at(OFF_PID) }.load(Ordering::Relaxed) as i32
}
