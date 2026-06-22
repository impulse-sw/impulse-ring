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

#![deny(warnings, clippy::todo, clippy::unimplemented)]

mod channel;
mod client;
mod rpc;

pub use channel::{Publisher, Subscriber};
pub use rpc::{CallFuture, block_on};

use client::{Inner, Slot};
use impulse_ring_core::avro::{self, Fingerprint};
use impulse_ring_core::control;
use impulse_ring_core::proto::{self, Kind, status};
use impulse_ring_core::ring::{Ring, ring_bytes};
use impulse_ring_core::shm::Segment;
use impulse_ring_core::util;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

const CONTROL_TIMEOUT: Duration = Duration::from_secs(5);

/// A live connection to the Ring broker.
pub struct Connection {
  inner: Arc<Inner>,
  running: Arc<AtomicBool>,
  threads: Mutex<Vec<JoinHandle<()>>>,
  // Keep the bootstrap segments mapped (and the reply segment unlinked on drop).
  _control_seg: Arc<Segment>,
  _reply_seg: Arc<Segment>,
}

impl Connection {
  /// Connect to the broker and register `app_name` on the bus.
  pub fn connect(app_name: &str) -> io::Result<Connection> {
    // 1. Attach the well-known control segment (socket-free rendezvous).
    let control_seg = Arc::new(Segment::open(util::CONTROL_SEGMENT).map_err(|e| {
      io::Error::new(
        e.kind(),
        format!("cannot open control segment (is impulsed running?): {e}"),
      )
    })?);
    let submission = control::attach_control(control_seg.clone())?;

    // 2. Create our own reply segment, named by a random bootstrap nonce.
    let nonce = util::random_u64();
    let reply_name = util::client_segment(nonce);
    let reply_seg = Arc::new(Segment::create(&reply_name, ring_bytes(control::REPLY_CAP))?);
    let reply_ring = Ring::format(reply_seg.clone(), 0, control::REPLY_CAP)?;

    let inner = Arc::new(Inner {
      submission,
      pending: Mutex::new(HashMap::new()),
      client_id: AtomicI64::new(0),
      reply_segment: reply_name.clone(),
    });
    let running = Arc::new(AtomicBool::new(true));

    // 3. Start the dispatcher that drains our reply ring.
    let d_inner = inner.clone();
    let d_run = running.clone();
    let dispatcher = std::thread::spawn(move || client::run_dispatcher(d_inner, reply_ring, d_run));

    let conn = Connection {
      inner,
      running,
      threads: Mutex::new(vec![dispatcher]),
      _control_seg: control_seg,
      _reply_seg: reply_seg,
    };

    // 4. Register and learn our client id.
    let corr = util::random_u64() as i64;
    let frame = proto::to_frame(
      Kind::Register,
      &proto::Register {
        correlation_id: corr,
        app_name: app_name.to_string(),
        nonce: nonce as i64,
        reply_segment: reply_name,
        heartbeat_ms: 1000,
        // Report our pid so the broker can reclaim our names if we die without
        // unregistering (crash / SIGKILL) and later restart.
        pid: std::process::id() as i64,
      },
    )?;
    let reply = conn.inner.call_control(corr, frame, CONTROL_TIMEOUT)?;
    let rr: proto::RegisterReply = proto::from_frame(Kind::RegisterReply, &reply)?;
    if rr.status != status::OK {
      return Err(io::Error::other(format!("register rejected: {}", rr.message)));
    }
    conn.inner.client_id.store(rr.client_id, Ordering::Relaxed);
    Ok(conn)
  }

  fn client_id(&self) -> i64 {
    self.inner.client_id.load(Ordering::Relaxed)
  }

  fn next_corr() -> i64 {
    util::random_u64() as i64
  }

  /// Publish a channel with the given Avro schema. `key` gates subscribers.
  pub fn publish_channel(&self, name: &str, schema_json: &str, key: Option<&str>) -> io::Result<Publisher> {
    let (schema, _fp) = avro::parse(schema_json)?;
    let corr = Self::next_corr();
    let frame = proto::to_frame(
      Kind::PublishChannel,
      &proto::PublishChannel {
        correlation_id: corr,
        client_id: self.client_id(),
        channel: name.to_string(),
        schema_json: schema_json.to_string(),
        access_key: key.unwrap_or("").to_string(),
      },
    )?;
    let reply = self.inner.call_control(corr, frame, CONTROL_TIMEOUT)?;
    let pr: proto::PublishReply = proto::from_frame(Kind::PublishReply, &reply)?;
    if pr.status != status::OK {
      return Err(io::Error::other(format!("publish failed: {}", pr.message)));
    }
    let arena = Arc::new(Segment::open(&pr.arena)?);
    let ring = Ring::attach(arena, 0)?;
    Ok(Publisher::new(ring, Arc::new(schema), proto::i64_to_fp(pr.schema_fp)))
  }

  /// List all channels currently on the bus.
  pub fn list_channels(&self) -> io::Result<Vec<proto::ChannelInfo>> {
    let corr = Self::next_corr();
    let frame = proto::to_frame(
      Kind::ListChannels,
      &proto::ListChannels {
        correlation_id: corr,
        client_id: self.client_id(),
      },
    )?;
    let reply = self.inner.call_control(corr, frame, CONTROL_TIMEOUT)?;
    let list: proto::ChannelList = proto::from_frame(Kind::ChannelList, &reply)?;
    Ok(list.channels)
  }

  /// Subscribe to a channel by id. `expected_schema_json` is fingerprinted and
  /// checked against the publisher's schema by the broker.
  pub fn subscribe(&self, channel_id: i64, key: Option<&str>, expected_schema_json: &str) -> io::Result<Subscriber> {
    let (schema, expected_fp) = avro::parse(expected_schema_json)?;
    let corr = Self::next_corr();
    let frame = proto::to_frame(
      Kind::Subscribe,
      &proto::Subscribe {
        correlation_id: corr,
        client_id: self.client_id(),
        channel_id,
        access_key: key.unwrap_or("").to_string(),
        expected_fp: proto::fp_to_i64(expected_fp),
      },
    )?;
    let reply = self.inner.call_control(corr, frame, CONTROL_TIMEOUT)?;
    let sr: proto::SubscribeReply = proto::from_frame(Kind::SubscribeReply, &reply)?;
    if sr.status != status::OK {
      return Err(io::Error::other(format!("subscribe failed: {}", sr.message)));
    }
    let arena = Arc::new(Segment::open(&sr.arena)?);
    let ring = Ring::attach(arena, 0)?;
    Ok(Subscriber::new(ring, Arc::new(schema), proto::i64_to_fp(sr.schema_fp)))
  }

  /// Expose a function. `handler` runs on a dedicated service thread, decoding
  /// `Req` and encoding `Resp` with the supplied Avro schemas.
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
    let (req_schema, _req_fp) = avro::parse(req_schema_json)?;
    let (resp_schema, _resp_fp) = avro::parse(resp_schema_json)?;
    let corr = Self::next_corr();
    let frame = proto::to_frame(
      Kind::ExposeFunction,
      &proto::ExposeFunction {
        correlation_id: corr,
        client_id: self.client_id(),
        fn_name: name.to_string(),
        req_schema_json: req_schema_json.to_string(),
        resp_schema_json: resp_schema_json.to_string(),
        access_key: key.unwrap_or("").to_string(),
      },
    )?;
    let reply = self.inner.call_control(corr, frame, CONTROL_TIMEOUT)?;
    let er: proto::ExposeReply = proto::from_frame(Kind::ExposeReply, &reply)?;
    if er.status != status::OK {
      return Err(io::Error::other(format!("expose failed: {}", er.message)));
    }
    let arena = Arc::new(Segment::open(&er.req_arena)?);
    let req_ring = Ring::attach(arena, 0)?;
    let handle = rpc::spawn_service::<Req, Resp, F>(
      req_ring,
      Arc::new(req_schema),
      Arc::new(resp_schema),
      proto::i64_to_fp(er.req_fp),
      proto::i64_to_fp(er.resp_fp),
      handler,
      self.running.clone(),
    );
    self.threads.lock().unwrap().push(handle);
    Ok(())
  }

  /// Call a remote function, returning a future for the response. Schema
  /// fingerprints are validated against the callee before sending.
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
    let corr = Self::next_corr();
    let frame = proto::to_frame(
      Kind::LookupFunction,
      &proto::LookupFunction {
        correlation_id: corr,
        client_id: self.client_id(),
        fn_name: fn_name.to_string(),
        access_key: key.unwrap_or("").to_string(),
      },
    )?;
    let reply = self.inner.call_control(corr, frame, CONTROL_TIMEOUT)?;
    let lr: proto::LookupReply = proto::from_frame(Kind::LookupReply, &reply)?;
    if lr.status != status::OK {
      return Err(io::Error::other(format!("lookup failed: {}", lr.message)));
    }
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
    let slot: Arc<Slot> = self.inner.register(rpc_corr);
    let rpc_req = proto::RpcRequest {
      correlation_id: rpc_corr,
      caller_id: self.client_id(),
      reply_segment: self.inner.reply_segment.clone(),
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
    let fut = self.call::<Req, Resp>(fn_name, key, req, req_schema_json, resp_schema_json)?;
    match rpc::block_on(fut, timeout) {
      Some(res) => res,
      None => Err(io::Error::new(io::ErrorKind::TimedOut, "rpc call timed out")),
    }
  }

  /// Our broker-assigned client id (0 until registered).
  pub fn id(&self) -> Fingerprint {
    self.client_id() as Fingerprint
  }
}

impl Drop for Connection {
  fn drop(&mut self) {
    // Best-effort unregister, then stop background threads.
    let corr = Self::next_corr();
    if let Ok(frame) = proto::to_frame(
      Kind::Unregister,
      &proto::Unregister {
        correlation_id: corr,
        client_id: self.client_id(),
      },
    ) {
      let _ = self
        .inner
        .submission
        .push_blocking(&frame.encode(), Some(Duration::from_millis(200)));
    }
    self.running.store(false, Ordering::Relaxed);
    for h in self.threads.lock().unwrap().drain(..) {
      let _ = h.join();
    }
  }
}
