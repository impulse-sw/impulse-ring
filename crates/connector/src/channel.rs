//! Channel publish/subscribe handles over a data arena. The publisher is the
//! sole producer and the subscriber the sole consumer of the arena's ring
//! (Milestone 1: one subscriber per channel; fan-out is a later milestone).
//!
//! Both handles read their `Ring` through a [`RingCell`] so a broker-restart
//! reconnect can rebind them to a fresh arena without invalidating the handle the
//! caller is holding.

use crate::client::{Inner, RingCell};
use apache_avro::Schema;
use impulse_ring_core::avro::{self, Fingerprint};
use impulse_ring_core::frame::Frame;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::io;
use std::sync::Arc;
use std::time::Duration;

/// Producer end of a channel.
pub struct Publisher {
  ring: RingCell,
  schema: Arc<Schema>,
  schema_fp: Fingerprint,
  // Held so the channel can be replayed onto a fresh broker after a restart, and
  // deregistered when the publisher is dropped.
  inner: Arc<Inner>,
  reg_id: u64,
}

impl Publisher {
  pub(crate) fn new(
    ring: RingCell,
    schema: Arc<Schema>,
    schema_fp: Fingerprint,
    inner: Arc<Inner>,
    reg_id: u64,
  ) -> Self {
    Publisher {
      ring,
      schema,
      schema_fp,
      inner,
      reg_id,
    }
  }

  /// The channel's Avro schema fingerprint.
  pub fn schema_fp(&self) -> Fingerprint {
    self.schema_fp
  }

  /// Publish a message (Avro-encoded), blocking briefly if the ring is full.
  pub fn publish<T: Serialize>(&self, msg: &T) -> io::Result<()> {
    let body = avro::encode(&self.schema, msg)?;
    let frame = Frame::new(self.schema_fp, body);
    let ring = self.ring.read().unwrap().clone();
    if !ring.push_blocking(&frame.encode(), Some(Duration::from_secs(1))) {
      return Err(io::Error::other("channel full (slow subscriber)"));
    }
    Ok(())
  }

  /// Non-blocking publish; returns `false` on backpressure.
  pub fn try_publish<T: Serialize>(&self, msg: &T) -> io::Result<bool> {
    let body = avro::encode(&self.schema, msg)?;
    let frame = Frame::new(self.schema_fp, body);
    let ring = self.ring.read().unwrap().clone();
    Ok(ring.try_push(&frame.encode()))
  }
}

impl Drop for Publisher {
  fn drop(&mut self) {
    self.inner.deregister(self.reg_id);
  }
}

/// Consumer end of a channel.
pub struct Subscriber {
  ring: RingCell,
  schema: Arc<Schema>,
  schema_fp: Fingerprint,
}

impl Subscriber {
  pub(crate) fn new(ring: RingCell, schema: Arc<Schema>, schema_fp: Fingerprint) -> Self {
    Subscriber {
      ring,
      schema,
      schema_fp,
    }
  }

  pub fn schema_fp(&self) -> Fingerprint {
    self.schema_fp
  }

  /// Receive the next message, waiting up to `timeout`. `Ok(None)` on timeout.
  pub fn recv<T: DeserializeOwned>(&self, timeout: Duration) -> io::Result<Option<T>> {
    let ring = self.ring.read().unwrap().clone();
    let Some(bytes) = ring.pop_blocking(Some(timeout)) else {
      return Ok(None);
    };
    let frame = Frame::decode(&bytes)?;
    if frame.schema_fp != self.schema_fp {
      return Err(io::Error::other(format!(
        "message schema mismatch: expected {:#x}, got {:#x}",
        self.schema_fp, frame.schema_fp
      )));
    }
    let value: T = avro::decode(&self.schema, &frame.body)?;
    Ok(Some(value))
  }
}
