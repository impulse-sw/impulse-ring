//! Shared-memory ring buffer carrying length-prefixed, variable-size records.
//!
//! One ring instance lives inside a [`Segment`] at a 64-byte-aligned `base`.
//! Records are framed as `[u32 len][len bytes]` and may wrap the data region;
//! reads/writes handle the wrap with a two-part copy.
//!
//! Concurrency model (Milestone 1):
//! * **Multiple producers** are serialized by a futex mutex in the header, so
//!   the ring is MPSC-safe (control submission, per-client reply, RPC request).
//!   A lock-free SPSC fast path is a deliberate follow-up optimization.
//! * **Single consumer**, lock-free on the read side.
//!
//! Wakeup uses two futex words: producers park on `space_seq` (signalled by the
//! consumer after it frees space) and the consumer parks on `data_seq`
//! (signalled by a producer after it publishes a record). Both are preceded by
//! an adaptive spin to keep latency low under load.
//!
//! Memory ordering: a producer publishes payload bytes with a `Release` store
//! to `head`; the consumer reads `head` with `Acquire` before touching the
//! bytes. Symmetrically the consumer frees space with a `Release` store to
//! `tail` that the producer observes with `Acquire`. This is what makes the
//! ring correct on weakly-ordered targets such as arm64.

use crate::futex;
use crate::shm::Segment;
use std::io;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

const MAGIC: u32 = 0x474e_4952; // "RING" LE

// Field offsets relative to the ring `base`, laid out across cache lines to
// avoid producer/consumer false sharing.
const OFF_MAGIC: usize = 0;
const OFF_CAP: usize = 4;
const OFF_HEAD: usize = 64; // producer line
const OFF_PRODLOCK: usize = 72;
const OFF_SPACE_SEQ: usize = 76;
const OFF_TAIL: usize = 128; // consumer line
const OFF_DATA_SEQ: usize = 136;
/// Size of the ring header before the data region begins.
pub const RING_HEADER: usize = 192;

/// Spin iterations before falling back to a blocking futex wait.
const SPIN_LIMIT: u32 = 256;

/// Compute the total segment bytes required to host a ring with `capacity`
/// bytes of data region. `capacity` must be a power of two.
pub fn ring_bytes(capacity: usize) -> usize {
    RING_HEADER + capacity
}

/// A producer or consumer view over a ring embedded in a shared segment.
#[derive(Clone)]
pub struct Ring {
    seg: Arc<Segment>,
    base: usize,
    capacity: usize,
    mask: u64,
}

impl Ring {
    /// Initialize a fresh ring header at `base` (creator side).
    pub fn format(seg: Arc<Segment>, base: usize, capacity: usize) -> io::Result<Ring> {
        assert!(capacity.is_power_of_two(), "capacity must be power of two");
        assert!(base.is_multiple_of(64), "ring base must be 64-byte aligned");
        if base + ring_bytes(capacity) > seg.len() {
            return Err(io::Error::other("segment too small for ring"));
        }
        let r = Ring {
            seg,
            base,
            capacity,
            mask: (capacity as u64) - 1,
        };
        r.head().store(0, Ordering::Relaxed);
        r.tail().store(0, Ordering::Relaxed);
        r.prod_lock().store(0, Ordering::Relaxed);
        r.space_seq().store(0, Ordering::Relaxed);
        r.data_seq().store(0, Ordering::Relaxed);
        r.cap_word().store(capacity as u32, Ordering::Relaxed);
        // Publish magic last so attachers only succeed on a fully-formed header.
        r.magic_word().store(MAGIC, Ordering::Release);
        Ok(r)
    }

    /// Attach to an already-formatted ring at `base`.
    pub fn attach(seg: Arc<Segment>, base: usize) -> io::Result<Ring> {
        let magic = unsafe { seg.atomic_u32_at(base + OFF_MAGIC) }.load(Ordering::Acquire);
        if magic != MAGIC {
            return Err(io::Error::other("ring magic mismatch / not formatted"));
        }
        let capacity =
            unsafe { seg.atomic_u32_at(base + OFF_CAP) }.load(Ordering::Relaxed) as usize;
        if !capacity.is_power_of_two() || base + ring_bytes(capacity) > seg.len() {
            return Err(io::Error::other("ring capacity invalid"));
        }
        Ok(Ring {
            seg,
            base,
            capacity,
            mask: (capacity as u64) - 1,
        })
    }

    fn magic_word(&self) -> &AtomicU32 {
        unsafe { self.seg.atomic_u32_at(self.base + OFF_MAGIC) }
    }
    fn cap_word(&self) -> &AtomicU32 {
        unsafe { self.seg.atomic_u32_at(self.base + OFF_CAP) }
    }
    fn head(&self) -> &AtomicU64 {
        unsafe { self.seg.atomic_u64_at(self.base + OFF_HEAD) }
    }
    fn tail(&self) -> &AtomicU64 {
        unsafe { self.seg.atomic_u64_at(self.base + OFF_TAIL) }
    }
    fn prod_lock(&self) -> &AtomicU32 {
        unsafe { self.seg.atomic_u32_at(self.base + OFF_PRODLOCK) }
    }
    fn space_seq(&self) -> &AtomicU32 {
        unsafe { self.seg.atomic_u32_at(self.base + OFF_SPACE_SEQ) }
    }
    fn data_seq(&self) -> &AtomicU32 {
        unsafe { self.seg.atomic_u32_at(self.base + OFF_DATA_SEQ) }
    }

    #[inline]
    fn data_ptr(&self) -> *mut u8 {
        unsafe { self.seg.as_mut_ptr().add(self.base + RING_HEADER) }
    }

    fn write_wrapped(&self, pos: u64, src: &[u8]) {
        let cap = self.capacity;
        let idx = (pos & self.mask) as usize;
        let first = std::cmp::min(src.len(), cap - idx);
        unsafe {
            std::ptr::copy_nonoverlapping(src.as_ptr(), self.data_ptr().add(idx), first);
            if first < src.len() {
                std::ptr::copy_nonoverlapping(
                    src.as_ptr().add(first),
                    self.data_ptr(),
                    src.len() - first,
                );
            }
        }
    }

    fn read_wrapped(&self, pos: u64, len: usize) -> Vec<u8> {
        let cap = self.capacity;
        let idx = (pos & self.mask) as usize;
        let first = std::cmp::min(len, cap - idx);
        let mut out = vec![0u8; len];
        unsafe {
            std::ptr::copy_nonoverlapping(self.data_ptr().add(idx), out.as_mut_ptr(), first);
            if first < len {
                std::ptr::copy_nonoverlapping(
                    self.data_ptr(),
                    out.as_mut_ptr().add(first),
                    len - first,
                );
            }
        }
        out
    }

    // ---- futex mutex (3-state) guarding the producer side -----------------

    fn lock_producer(&self) {
        let lock = self.prod_lock();
        // Fast path: 0 -> 1.
        if lock
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
        {
            return;
        }
        loop {
            // Mark contended (2) and sleep until released.
            if lock.swap(2, Ordering::Acquire) == 0 {
                return;
            }
            futex::wait(lock, 2, Some(Duration::from_millis(50)));
        }
    }

    fn unlock_producer(&self) {
        let lock = self.prod_lock();
        if lock.swap(0, Ordering::Release) == 2 {
            futex::wake(lock, 1);
        }
    }

    /// Non-blocking push. Returns `false` if the ring lacks room (backpressure).
    pub fn try_push(&self, data: &[u8]) -> bool {
        self.lock_producer();
        let ok = self.push_locked(data);
        self.unlock_producer();
        if ok {
            // Publish data availability and wake the consumer.
            self.data_seq().fetch_add(1, Ordering::Release);
            futex::wake(self.data_seq(), 1);
        }
        ok
    }

    /// Blocking push with optional timeout. Returns `false` only on timeout.
    pub fn push_blocking(&self, data: &[u8], timeout: Option<Duration>) -> bool {
        let deadline = timeout.map(|d| std::time::Instant::now() + d);
        loop {
            self.lock_producer();
            if self.push_locked(data) {
                self.unlock_producer();
                self.data_seq().fetch_add(1, Ordering::Release);
                futex::wake(self.data_seq(), 1);
                return true;
            }
            // No room: capture the space sequence, then release the lock and park.
            let seen = self.space_seq().load(Ordering::Acquire);
            self.unlock_producer();
            if let Some(dl) = deadline {
                let now = std::time::Instant::now();
                if now >= dl {
                    return false;
                }
                futex::wait(self.space_seq(), seen, Some(dl - now));
            } else {
                futex::wait(self.space_seq(), seen, Some(Duration::from_millis(50)));
            }
        }
    }

    /// Caller must hold the producer lock.
    fn push_locked(&self, data: &[u8]) -> bool {
        let need = 4 + data.len();
        if need > self.capacity {
            return false; // record can never fit
        }
        let head = self.head().load(Ordering::Relaxed);
        let tail = self.tail().load(Ordering::Acquire);
        let used = (head - tail) as usize;
        if self.capacity - used < need {
            return false;
        }
        let len = data.len() as u32;
        self.write_wrapped(head, &len.to_le_bytes());
        self.write_wrapped(head + 4, data);
        // Release: makes the payload visible before the consumer sees head move.
        self.head().store(head + need as u64, Ordering::Release);
        true
    }

    /// Non-blocking pop. Returns `None` if the ring is empty.
    pub fn try_pop(&self) -> Option<Vec<u8>> {
        let tail = self.tail().load(Ordering::Relaxed);
        let head = self.head().load(Ordering::Acquire);
        if head == tail {
            return None;
        }
        let len_bytes = self.read_wrapped(tail, 4);
        let len =
            u32::from_le_bytes([len_bytes[0], len_bytes[1], len_bytes[2], len_bytes[3]]) as usize;
        let payload = self.read_wrapped(tail + 4, len);
        // Release: frees the bytes for producers, who observe tail with Acquire.
        self.tail().store(tail + 4 + len as u64, Ordering::Release);
        // Signal producers waiting for space.
        self.space_seq().fetch_add(1, Ordering::Release);
        futex::wake(self.space_seq(), i32::MAX);
        Some(payload)
    }

    /// Blocking pop with optional timeout: adaptive spin, then futex park.
    pub fn pop_blocking(&self, timeout: Option<Duration>) -> Option<Vec<u8>> {
        let deadline = timeout.map(|d| std::time::Instant::now() + d);
        loop {
            for _ in 0..SPIN_LIMIT {
                if let Some(v) = self.try_pop() {
                    return Some(v);
                }
                std::hint::spin_loop();
            }
            let seen = self.data_seq().load(Ordering::Acquire);
            if let Some(v) = self.try_pop() {
                return Some(v);
            }
            if let Some(dl) = deadline {
                let now = std::time::Instant::now();
                if now >= dl {
                    return None;
                }
                futex::wait(self.data_seq(), seen, Some(dl - now));
            } else {
                futex::wait(self.data_seq(), seen, Some(Duration::from_millis(100)));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shm::Segment;

    fn temp_ring(cap: usize) -> Ring {
        use std::sync::atomic::AtomicU64;
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let uniq = SEQ.fetch_add(1, Ordering::Relaxed);
        let name = format!(
            "/impulse-ring.test.{}.{}.v1",
            std::process::id() as u64,
            uniq
        );
        let seg = Arc::new(Segment::create(&name, ring_bytes(cap)).unwrap());
        Ring::format(seg, 0, cap).unwrap()
    }

    #[test]
    fn roundtrip_and_wrap() {
        let r = temp_ring(64);
        // Push records that force a wrap of the 64-byte data region.
        for i in 0..50u8 {
            let msg = vec![i; 10];
            assert!(r.try_push(&msg), "push {i}");
            let got = r.try_pop().expect("pop");
            assert_eq!(got, msg);
        }
        assert!(r.try_pop().is_none());
    }

    #[test]
    fn backpressure_when_full() {
        let r = temp_ring(64);
        // 64 cap; record of 20 bytes => 24 with prefix. Two fit (48), third fails.
        assert!(r.try_push(&[1u8; 20]));
        assert!(r.try_push(&[2u8; 20]));
        assert!(!r.try_push(&[3u8; 20]), "ring should be full");
        assert_eq!(r.try_pop().unwrap(), vec![1u8; 20]);
        assert!(r.try_push(&[3u8; 20]), "space freed after pop");
    }
}
