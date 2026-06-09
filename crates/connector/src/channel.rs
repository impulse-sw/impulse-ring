//! Channel publish/subscribe handles over a data arena. The publisher is the
//! sole producer and the subscriber the sole consumer of the arena's ring
//! (Milestone 1: one subscriber per channel; fan-out is a later milestone).

use apache_avro::Schema;
use impulse_ring_core::avro::{self, Fingerprint};
use impulse_ring_core::frame::Frame;
use impulse_ring_core::ring::Ring;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::io;
use std::sync::Arc;
use std::time::Duration;

/// Producer end of a channel.
pub struct Publisher {
  ring: Ring,
  schema: Arc<Schema>,
  schema_fp: Fingerprint,
}

impl Publisher {
  pub(crate) fn new(ring: Ring, schema: Arc<Schema>, schema_fp: Fingerprint) -> Self {
    Publisher {
      ring,
      schema,
      schema_fp,
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
    if !self.ring.push_blocking(&frame.encode(), Some(Duration::from_secs(1))) {
      return Err(io::Error::other("channel full (slow subscriber)"));
    }
    Ok(())
  }

  /// Non-blocking publish; returns `false` on backpressure.
  pub fn try_publish<T: Serialize>(&self, msg: &T) -> io::Result<bool> {
    let body = avro::encode(&self.schema, msg)?;
    let frame = Frame::new(self.schema_fp, body);
    Ok(self.ring.try_push(&frame.encode()))
  }
}

/// Consumer end of a channel.
pub struct Subscriber {
  ring: Ring,
  schema: Arc<Schema>,
  schema_fp: Fingerprint,
}

impl Subscriber {
  pub(crate) fn new(ring: Ring, schema: Arc<Schema>, schema_fp: Fingerprint) -> Self {
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
    let Some(bytes) = self.ring.pop_blocking(Some(timeout)) else {
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
