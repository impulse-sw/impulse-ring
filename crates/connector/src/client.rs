//! Connector internals: the per-connection shared state, the oneshot reply
//! "slot" used to await correlated replies, and the dispatcher thread that
//! drains the client's reply ring and routes frames to waiters.

use impulse_ring_core::frame::Frame;
use impulse_ring_core::proto;
use impulse_ring_core::ring::Ring;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

/// A oneshot rendezvous for a single correlated reply. Supports both blocking
/// waits (control calls) and `Future` polling (async RPC).
#[derive(Default)]
pub struct Slot {
  state: Mutex<SlotState>,
  cv: Condvar,
}

#[derive(Default)]
struct SlotState {
  frame: Option<Frame>,
  waker: Option<Waker>,
}

impl Slot {
  pub fn new() -> Arc<Slot> {
    Arc::new(Slot::default())
  }

  /// Deliver the reply, waking any blocking or async waiter.
  pub fn complete(&self, frame: Frame) {
    let mut g = self.state.lock().unwrap();
    g.frame = Some(frame);
    if let Some(w) = g.waker.take() {
      w.wake();
    }
    self.cv.notify_all();
  }

  /// Block until the reply arrives or `timeout` elapses.
  pub fn wait(&self, timeout: Duration) -> Option<Frame> {
    let mut g = self.state.lock().unwrap();
    loop {
      if let Some(f) = g.frame.take() {
        return Some(f);
      }
      let (ng, to) = self.cv.wait_timeout(g, timeout).unwrap();
      g = ng;
      if to.timed_out() {
        return g.frame.take();
      }
    }
  }

  /// Poll for the reply (async path).
  pub fn poll(&self, cx: &mut Context<'_>) -> Poll<Frame> {
    let mut g = self.state.lock().unwrap();
    if let Some(f) = g.frame.take() {
      Poll::Ready(f)
    } else {
      g.waker = Some(cx.waker().clone());
      Poll::Pending
    }
  }
}

/// Shared per-connection state.
pub struct Inner {
  pub submission: Ring,
  pub pending: Mutex<HashMap<i64, Arc<Slot>>>,
  pub client_id: AtomicI64,
  pub reply_segment: String,
}

impl Inner {
  /// Send a control request and block for its correlated reply frame.
  pub fn call_control(&self, corr: i64, frame: Frame, timeout: Duration) -> io::Result<Frame> {
    let slot = Slot::new();
    self.pending.lock().unwrap().insert(corr, slot.clone());
    if !self
      .submission
      .push_blocking(&frame.encode(), Some(Duration::from_secs(2)))
    {
      self.pending.lock().unwrap().remove(&corr);
      return Err(io::Error::other("submission ring full (broker stuck?)"));
    }
    match slot.wait(timeout) {
      Some(f) => Ok(f),
      None => {
        self.pending.lock().unwrap().remove(&corr);
        Err(io::Error::new(
          io::ErrorKind::TimedOut,
          "timed out waiting for broker reply",
        ))
      }
    }
  }

  /// Register a pending slot for an async correlation (RPC).
  pub fn register(&self, corr: i64) -> Arc<Slot> {
    let slot = Slot::new();
    self.pending.lock().unwrap().insert(corr, slot.clone());
    slot
  }

  pub fn drop_pending(&self, corr: i64) {
    self.pending.lock().unwrap().remove(&corr);
  }
}

/// Drain the reply ring, routing each frame to the slot registered under its
/// correlation id. Exits when `running` is cleared (connection shutdown).
pub fn run_dispatcher(inner: Arc<Inner>, reply_ring: Ring, running: Arc<AtomicBool>) {
  while running.load(Ordering::Relaxed) {
    let Some(bytes) = reply_ring.pop_blocking(Some(Duration::from_millis(100))) else {
      continue;
    };
    let frame = match Frame::decode(&bytes) {
      Ok(f) => f,
      Err(e) => {
        log::warn!("dispatcher: bad frame: {e}");
        continue;
      }
    };
    match proto::correlation_id(&frame) {
      Ok(corr) => {
        let slot = inner.pending.lock().unwrap().remove(&corr);
        if let Some(slot) = slot {
          slot.complete(frame);
        } else {
          log::warn!("dispatcher: no waiter for correlation {corr}");
        }
      }
      Err(e) => log::warn!("dispatcher: unroutable frame: {e}"),
    }
  }
}
