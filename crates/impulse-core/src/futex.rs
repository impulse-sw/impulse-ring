//! Thin wrapper over the Linux `futex` syscall.
//!
//! Tier 0 is Linux-only, so we lean on `futex` directly for cross-process
//! blocking/wakeup on a shared-memory word. We use the *shared* (non-private)
//! variant because the word lives in a segment mapped by multiple processes.
//!
//! Robustness note: we deliberately operate on a plain `AtomicU32` word rather
//! than a PI/robust mutex. A waiter that is left parked by a crashed peer is
//! recovered by the broker's heartbeat reaper, which issues a wake. This keeps
//! the hot path free of robust-mutex complexity for Milestone 1.

use std::sync::atomic::AtomicU32;
use std::time::Duration;

const FUTEX_WAIT: libc::c_int = 0;
const FUTEX_WAKE: libc::c_int = 1;

/// Block while `*word == expected`, up to an optional timeout.
///
/// Returns immediately if the value already differs (classic futex race-free
/// check performed kernel-side). A spurious wakeup is possible; callers must
/// re-check their condition in a loop.
pub fn wait(word: &AtomicU32, expected: u32, timeout: Option<Duration>) {
    let uaddr = word as *const AtomicU32 as *const libc::c_void;
    let ts = timeout.map(|d| libc::timespec {
        tv_sec: d.as_secs() as libc::time_t,
        tv_nsec: d.subsec_nanos() as libc::c_long,
    });
    let ts_ptr = ts
        .as_ref()
        .map(|t| t as *const libc::timespec)
        .unwrap_or(std::ptr::null());
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            uaddr,
            FUTEX_WAIT,
            expected as libc::c_int,
            ts_ptr,
            std::ptr::null::<libc::c_void>(),
            0,
        );
    }
}

/// Wake up to `n` waiters parked on `word`. Use `i32::MAX` to wake all.
pub fn wake(word: &AtomicU32, n: i32) {
    let uaddr = word as *const AtomicU32 as *const libc::c_void;
    unsafe {
        libc::syscall(
            libc::SYS_futex,
            uaddr,
            FUTEX_WAKE,
            n,
            std::ptr::null::<libc::c_void>(),
            std::ptr::null::<libc::c_void>(),
            0,
        );
    }
}
