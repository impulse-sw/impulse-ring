//! `impulse-ring-connector` — the native Rust connector for **Ring**.
//!
//! A connector registers an application on the bus, publishes and subscribes to
//! channels (key-gated), exposes functions, and calls remote functions with an
//! async result — all over shared memory, no sockets. Every payload is Apache
//! Avro and every cross-service boundary is fingerprint-checked.
//!
//! ```no_run
//! use impulse_ring_connector::Connection;
//! let conn = Connection::connect("my-service").unwrap();
//! let chans = conn.list_channels().unwrap();
//! # let _ = chans;
//! ```
//!
//! ## Surviving a broker restart
//!
//! `impulsed` recreates its shared memory (with a fresh `epoch`) every time it
//! starts, which invalidates an existing connection's submission ring, reply
//! segment and `client_id`. A connection detects this — either proactively (a
//! background watcher polls the control-segment epoch) or lazily (a control/RPC
//! call that stops getting answered) — and **transparently reconnects**:
//! re-attaches the control segment, re-registers under the same name, and
//! **replays** the channels it had published and the functions it had exposed so
//! live [`Publisher`]s and RPC services keep working. This is on by default; turn
//! it off with [`Connection::set_auto_reconnect`].
//!
//! Subscribers are *not* auto-replayed (a channel's id changes across a restart
//! and its publisher lives in another process); re-acquire a [`Subscriber`] after
//! a restart by resolving the channel by name again.

#![deny(warnings, clippy::todo, clippy::unimplemented)]

mod channel;
mod client;
mod rpc;

pub use channel::{Publisher, Subscriber};
pub use client::live_broker_epoch;
pub use rpc::{CallFuture, block_on};

use client::{Inner, Reg, RingCell};
use impulse_ring_core::avro::{self, Fingerprint};
use impulse_ring_core::proto::{self, Kind};
use impulse_ring_core::ring::Ring;
use impulse_ring_core::shm::Segment;
use impulse_ring_core::util;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::io;
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::Duration;

/// A live connection to the Ring broker.
pub struct Connection {
  inner: Arc<Inner>,
  threads: Mutex<Vec<JoinHandle<()>>>,
}

impl Connection {
  /// Connect to the broker and register `app_name` on the bus.
  pub fn connect(app_name: &str) -> io::Result<Connection> {
    let (inner, threads) = Inner::connect(app_name)?;
    Ok(Connection {
      inner,
      threads: Mutex::new(threads),
    })
  }

  fn next_corr() -> i64 {
    util::random_u64() as i64
  }

  /// Publish a channel with the given Avro schema. `key` gates subscribers.
  pub fn publish_channel(&self, name: &str, schema_json: &str, key: Option<&str>) -> io::Result<Publisher> {
    let (schema, _fp) = avro::parse(schema_json)?;
    let (ring, fp) = self
      .inner
      .with_reconnect(|| self.inner.do_publish(name, schema_json, key))?;
    let cell: RingCell = Arc::new(RwLock::new(ring));
    let id = self.inner.register_entry(Reg::Channel {
      name: name.to_string(),
      schema_json: schema_json.to_string(),
      key: key.map(str::to_string),
      ring: cell.clone(),
    });
    Ok(Publisher::new(cell, Arc::new(schema), fp, self.inner.clone(), id))
  }

  /// List all channels currently on the bus.
  pub fn list_channels(&self) -> io::Result<Vec<proto::ChannelInfo>> {
    self.inner.with_reconnect(|| self.inner.list_channels())
  }

  /// Subscribe to a channel by id. `expected_schema_json` is fingerprinted and
  /// checked against the publisher's schema by the broker.
  ///
  /// A subscriber is not auto-replayed across a broker restart (see the crate
  /// docs); re-subscribe by resolving the channel by name again.
  pub fn subscribe(&self, channel_id: i64, key: Option<&str>, expected_schema_json: &str) -> io::Result<Subscriber> {
    let (schema, expected_fp) = avro::parse(expected_schema_json)?;
    let (ring, fp) = self
      .inner
      .with_reconnect(|| self.inner.do_subscribe(channel_id, key, expected_fp))?;
    Ok(Subscriber::new(Arc::new(RwLock::new(ring)), Arc::new(schema), fp))
  }

  /// Expose a function. `handler` runs on a dedicated service thread, decoding
  /// `Req` and encoding `Resp` with the supplied Avro schemas.
  ///
  /// The function's request arena uses the broker's default capacity; use
  /// [`Connection::expose_function_with_arena`] to size it explicitly.
  pub fn expose_function<Req, Resp, F>(
    &self,
    name: &str,
    req_schema_json: &str,
    resp_schema_json: &str,
    key: Option<&str>,
    handler: F,
  ) -> io::Result<()>
  where
    Req: DeserializeOwned,
    Resp: Serialize,
    F: Fn(Req) -> Resp + Send + 'static,
  {
    self.expose_function_with_arena(name, req_schema_json, resp_schema_json, key, 0, handler)
  }

  /// Expose a function, requesting a request-arena capacity of `req_arena_cap`
  /// bytes (`0` = broker default).
  ///
  /// The broker clamps the request to `[MIN_ARENA_CAP, MAX_ARENA_CAP]` and rounds
  /// it up to a power of two (see `impulse_ring_core::control::clamp_arena_cap`).
  /// A larger arena lets a high-throughput service buffer more in-flight requests
  /// before producers hit backpressure.
  pub fn expose_function_with_arena<Req, Resp, F>(
    &self,
    name: &str,
    req_schema_json: &str,
    resp_schema_json: &str,
    key: Option<&str>,
    req_arena_cap: usize,
    handler: F,
  ) -> io::Result<()>
  where
    Req: DeserializeOwned,
    Resp: Serialize,
    F: Fn(Req) -> Resp + Send + 'static,
  {
    let (req_schema, _req_fp) = avro::parse(req_schema_json)?;
    let (resp_schema, _resp_fp) = avro::parse(resp_schema_json)?;
    let arena_cap = req_arena_cap as i64;
    let (req_ring, req_fp, resp_fp) = self.inner.with_reconnect(|| {
      self
        .inner
        .do_expose(name, req_schema_json, resp_schema_json, key, arena_cap)
    })?;
    let cell: RingCell = Arc::new(RwLock::new(req_ring));
    // A function is owned for the connection's lifetime, so it has no caller
    // handle to deregister it — it is replayed until the connection drops.
    self.inner.register_entry(Reg::Function {
      name: name.to_string(),
      req_schema_json: req_schema_json.to_string(),
      resp_schema_json: resp_schema_json.to_string(),
      key: key.map(str::to_string),
      arena_cap,
      ring: cell.clone(),
    });
    let handle = rpc::spawn_service::<Req, Resp, F>(
      cell,
      Arc::new(req_schema),
      Arc::new(resp_schema),
      req_fp,
      resp_fp,
      handler,
      self.inner.running(),
    );
    self.threads.lock().unwrap().push(handle);
    Ok(())
  }

  /// Call a remote function, returning a future for the response. Schema
  /// fingerprints are validated against the callee before sending.
  ///
  /// Note: the returned future is *not* auto-retried across a broker restart that
  /// happens after the request was placed; use [`Connection::call_blocking`] (or
  /// retry yourself) if you need that.
  pub fn call<Req, Resp>(
    &self,
    fn_name: &str,
    key: Option<&str>,
    req: &Req,
    req_schema_json: &str,
    resp_schema_json: &str,
  ) -> io::Result<CallFuture<Resp>>
  where
    Req: Serialize,
    Resp: DeserializeOwned,
  {
    let (req_schema, arg_fp) = avro::parse(req_schema_json)?;
    let (resp_schema, expected_resp_fp) = avro::parse(resp_schema_json)?;

    // Look the function up and check interface compatibility.
    let lr = self.inner.do_lookup(fn_name, key)?;
    if proto::i64_to_fp(lr.req_fp) != arg_fp {
      return Err(io::Error::other("call rejected: request schema mismatch"));
    }
    if proto::i64_to_fp(lr.resp_fp) != expected_resp_fp {
      return Err(io::Error::other("call rejected: response schema mismatch"));
    }

    // Encode args and place the request on the function's arena.
    let args = avro::encode(&req_schema, req)?;
    let fn_seg = Arc::new(Segment::open(&lr.req_arena)?);
    let fn_ring = Ring::attach(fn_seg, 0)?;

    let rpc_corr = Self::next_corr();
    let slot = self.inner.register(rpc_corr);
    let rpc_req = proto::RpcRequest {
      correlation_id: rpc_corr,
      caller_id: self.inner.client_id(),
      reply_segment: self.inner.reply_segment(),
      arg_fp: proto::fp_to_i64(arg_fp),
      args,
    };
    let req_frame = proto::to_frame(Kind::RpcRequest, &rpc_req)?;
    if !fn_ring.push_blocking(&req_frame.encode(), Some(Duration::from_secs(2))) {
      self.inner.drop_pending(rpc_corr);
      return Err(io::Error::other("function request ring full"));
    }
    Ok(CallFuture::new(slot, Arc::new(resp_schema), expected_resp_fp))
  }

  /// Blocking convenience wrapper around [`Connection::call`].
  ///
  /// The whole lookup → send → await round-trip is retried once if `impulsed` is
  /// restarted underneath it (when auto-reconnect is enabled).
  pub fn call_blocking<Req, Resp>(
    &self,
    fn_name: &str,
    key: Option<&str>,
    req: &Req,
    req_schema_json: &str,
    resp_schema_json: &str,
    timeout: Duration,
  ) -> io::Result<Resp>
  where
    Req: Serialize,
    Resp: DeserializeOwned,
  {
    self.inner.with_reconnect(|| {
      let fut = self.call::<Req, Resp>(fn_name, key, req, req_schema_json, resp_schema_json)?;
      match rpc::block_on(fut, timeout) {
        Some(res) => res,
        None => Err(io::Error::new(io::ErrorKind::TimedOut, "rpc call timed out")),
      }
    })
  }

  /// Our broker-assigned client id (0 until registered).
  pub fn id(&self) -> Fingerprint {
    self.inner.client_id() as Fingerprint
  }

  /// The broker epoch this connection is currently attached under. It changes
  /// when `impulsed` restarts (and after a reconnect tracks the new broker).
  pub fn broker_epoch(&self) -> u64 {
    self.inner.epoch()
  }

  /// Enable or disable transparent reconnect on a detected broker restart
  /// (default: enabled).
  pub fn set_auto_reconnect(&self, on: bool) {
    self.inner.set_auto_reconnect(on);
  }

  /// Whether transparent reconnect is enabled.
  pub fn auto_reconnect(&self) -> bool {
    self.inner.auto_reconnect()
  }
}

impl Drop for Connection {
  fn drop(&mut self) {
    // Best-effort unregister, then stop background threads.
    self.inner.send_unregister();
    self.inner.running.store(false, std::sync::atomic::Ordering::Relaxed);
    for h in self.threads.lock().unwrap().drain(..) {
      let _ = h.join();
    }
  }
}
