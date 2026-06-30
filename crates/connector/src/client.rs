//! Connector internals: the per-connection shared state, the oneshot reply
//! "slot" used to await correlated replies, the dispatcher thread that drains
//! the client's reply ring, and the broker-restart **watcher** that transparently
//! re-bootstraps the connection when `impulsed` is restarted.
//!
//! All control-plane round-trips live here (on [`Inner`]) so that both the public
//! [`Connection`](crate::Connection) API and the reconnect/replay machinery can
//! drive them.

use impulse_ring_core::avro::Fingerprint;
use impulse_ring_core::control;
use impulse_ring_core::frame::Frame;
use impulse_ring_core::proto::{self, Kind, status};
use impulse_ring_core::ring::{Ring, ring_bytes};
use impulse_ring_core::shm::Segment;
use impulse_ring_core::util;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::task::{Context, Poll, Waker};
use std::thread::JoinHandle;
use std::time::Duration;

const CONTROL_TIMEOUT: Duration = Duration::from_secs(5);
/// How often the watcher polls the control segment for a changed broker epoch.
const WATCH_INTERVAL: Duration = Duration::from_millis(250);

/// A swappable ring handle. Publishers, subscribers and service loops read the
/// `Ring` through this cell so that a reconnect can rebind them to a fresh arena
/// (the old arena is gone after a broker restart) without invalidating handles
/// the caller is still holding.
pub type RingCell = Arc<RwLock<Ring>>;

/// Snapshot of a published channel to replay after a reconnect:
/// `(name, schema_json, key, ring-cell to rebind)`.
type ChannelReplay = (String, String, Option<String>, RingCell);
/// Snapshot of an exposed function to replay after a reconnect:
/// `(name, req_schema, resp_schema, key, arena_cap, ring-cell to rebind)`.
type FunctionReplay = (String, String, String, Option<String>, i64, RingCell);

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

/// One per-generation transport: everything that is invalidated when the broker
/// restarts (the submission ring into the *current* broker, our reply ring, our
/// broker-assigned `client_id`, and the epoch we attached under). A reconnect
/// swaps the whole struct atomically behind [`Inner::transport`].
struct Transport {
  submission: Ring,
  reply_ring: Ring,
  reply_segment: String,
  nonce: u64,
  client_id: i64,
  epoch: u64,
  // Keep the bootstrap segments mapped; the reply segment is unlinked on drop.
  _control_seg: Arc<Segment>,
  _reply_seg: Arc<Segment>,
}

/// A registration this connection owns and must replay onto a fresh broker after
/// a restart so the caller's live handle keeps working.
pub enum Reg {
  /// A published channel; `ring` is the publisher's arena cell.
  Channel {
    name: String,
    schema_json: String,
    key: Option<String>,
    ring: RingCell,
  },
  /// An exposed function; `ring` is the service loop's request-arena cell.
  Function {
    name: String,
    req_schema_json: String,
    resp_schema_json: String,
    key: Option<String>,
    arena_cap: i64,
    ring: RingCell,
  },
}

/// Shared per-connection state.
pub struct Inner {
  transport: RwLock<Transport>,
  pub pending: Mutex<HashMap<i64, Arc<Slot>>>,
  /// The local name we registered under (replayed verbatim on reconnect).
  app_name: String,
  pub running: Arc<AtomicBool>,
  /// Whether a detected broker restart triggers a transparent reconnect.
  auto_reconnect: AtomicBool,
  /// Serializes reconnects so concurrent callers re-bootstrap exactly once.
  reconnect_mu: Mutex<()>,
  /// Registrations to replay after a reconnect, keyed by a local id.
  registry: Mutex<HashMap<u64, Reg>>,
  reg_seq: AtomicU64,
}

/// Open the control segment fresh, create a new reply segment, and assemble a
/// [`Transport`] (not yet registered: `client_id` is 0). Used by both the initial
/// connect and every reconnect.
fn bootstrap() -> io::Result<Transport> {
  let control_seg = Arc::new(Segment::open(util::CONTROL_SEGMENT).map_err(|e| {
    io::Error::new(
      e.kind(),
      format!("cannot open control segment (is impulsed running?): {e}"),
    )
  })?);
  let submission = control::attach_control(control_seg.clone())?;
  let epoch = control::broker_epoch(&control_seg);

  let nonce = util::random_u64();
  let reply_name = util::client_segment(nonce);
  let reply_seg = Arc::new(Segment::create(&reply_name, ring_bytes(control::REPLY_CAP))?);
  let reply_ring = Ring::format(reply_seg.clone(), 0, control::REPLY_CAP)?;

  Ok(Transport {
    submission,
    reply_ring,
    reply_segment: reply_name,
    nonce,
    client_id: 0,
    epoch,
    _control_seg: control_seg,
    _reply_seg: reply_seg,
  })
}

/// Read the live broker epoch by opening the control segment *freshly* (the cached
/// mapping still points at the unlinked pre-restart segment). `Err` means the
/// broker is currently unreachable.
pub fn live_broker_epoch() -> io::Result<u64> {
  let seg = Arc::new(Segment::open(util::CONTROL_SEGMENT)?);
  control::attach_control(seg.clone())?; // validate magic/version
  Ok(control::broker_epoch(&seg))
}

/// Does this error mean the broker stopped answering (and a reconnect is worth
/// attempting)? A plain remote error or schema mismatch is *not* such a case.
fn broker_unreachable(e: &io::Error) -> bool {
  matches!(e.kind(), io::ErrorKind::TimedOut | io::ErrorKind::NotFound)
    || e.to_string().contains("submission ring full")
}

impl Inner {
  /// Connect to the broker: bootstrap a transport, start the background threads,
  /// and register `app_name`. On a registration failure the threads are stopped
  /// before returning the error.
  pub fn connect(app_name: &str) -> io::Result<(Arc<Inner>, Vec<JoinHandle<()>>)> {
    let transport = bootstrap()?;
    let (inner, threads) = Inner::start(app_name, transport);
    if let Err(e) = inner.do_register() {
      inner.running.store(false, Ordering::Relaxed);
      for h in threads {
        let _ = h.join();
      }
      return Err(e);
    }
    Ok((inner, threads))
  }

  /// Build the shared state from an initial transport and start the background
  /// dispatcher + restart watcher. Returns the `Arc<Inner>` and the thread
  /// handles (owned by the [`Connection`](crate::Connection)).
  fn start(app_name: &str, transport: Transport) -> (Arc<Inner>, Vec<JoinHandle<()>>) {
    let inner = Arc::new(Inner {
      transport: RwLock::new(transport),
      pending: Mutex::new(HashMap::new()),
      app_name: app_name.to_string(),
      running: Arc::new(AtomicBool::new(true)),
      auto_reconnect: AtomicBool::new(true),
      reconnect_mu: Mutex::new(()),
      registry: Mutex::new(HashMap::new()),
      reg_seq: AtomicU64::new(1),
    });
    let d = inner.clone();
    let dispatcher = std::thread::spawn(move || run_dispatcher(d));
    let w = inner.clone();
    let watcher = std::thread::spawn(move || run_watcher(w));
    (inner, vec![dispatcher, watcher])
  }

  pub fn client_id(&self) -> i64 {
    self.transport.read().unwrap().client_id
  }

  pub fn reply_segment(&self) -> String {
    self.transport.read().unwrap().reply_segment.clone()
  }

  pub fn epoch(&self) -> u64 {
    self.transport.read().unwrap().epoch
  }

  pub fn running(&self) -> Arc<AtomicBool> {
    self.running.clone()
  }

  pub fn set_auto_reconnect(&self, on: bool) {
    self.auto_reconnect.store(on, Ordering::Relaxed);
  }

  pub fn auto_reconnect(&self) -> bool {
    self.auto_reconnect.load(Ordering::Relaxed)
  }

  fn next_corr() -> i64 {
    util::random_u64() as i64
  }

  /// Send a control request and block for its correlated reply frame. Low level:
  /// it does *not* reconnect (so the reconnect/replay path can reuse it).
  pub fn call_control(&self, corr: i64, frame: Frame, timeout: Duration) -> io::Result<Frame> {
    let slot = Slot::new();
    self.pending.lock().unwrap().insert(corr, slot.clone());
    let submission = self.transport.read().unwrap().submission.clone();
    if !submission.push_blocking(&frame.encode(), Some(Duration::from_secs(2))) {
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

  /// Push a frame onto the current submission ring (used by the unary RPC path).
  pub fn submit(&self, frame: &Frame, timeout: Duration) -> bool {
    let submission = self.transport.read().unwrap().submission.clone();
    submission.push_blocking(&frame.encode(), Some(timeout))
  }

  // ---- control-plane round-trips (also used by replay) ----

  /// Send the `Register` record and adopt the broker-assigned `client_id`.
  pub fn do_register(&self) -> io::Result<i64> {
    let (nonce, reply_segment) = {
      let t = self.transport.read().unwrap();
      (t.nonce, t.reply_segment.clone())
    };
    let corr = Self::next_corr();
    let frame = proto::to_frame(
      Kind::Register,
      &proto::Register {
        correlation_id: corr,
        app_name: self.app_name.clone(),
        nonce: nonce as i64,
        reply_segment,
        heartbeat_ms: 1000,
        // Report our pid so the broker can reclaim our names if we die without
        // unregistering (crash / SIGKILL) and later restart.
        pid: std::process::id() as i64,
      },
    )?;
    let reply = self.call_control(corr, frame, CONTROL_TIMEOUT)?;
    let rr: proto::RegisterReply = proto::from_frame(Kind::RegisterReply, &reply)?;
    if rr.status != status::OK {
      return Err(io::Error::other(format!("register rejected: {}", rr.message)));
    }
    self.transport.write().unwrap().client_id = rr.client_id;
    Ok(rr.client_id)
  }

  /// Publish a channel and attach its arena ring.
  pub fn do_publish(&self, name: &str, schema_json: &str, key: Option<&str>) -> io::Result<(Ring, Fingerprint)> {
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
    let reply = self.call_control(corr, frame, CONTROL_TIMEOUT)?;
    let pr: proto::PublishReply = proto::from_frame(Kind::PublishReply, &reply)?;
    if pr.status != status::OK {
      return Err(io::Error::other(format!("publish failed: {}", pr.message)));
    }
    let arena = Arc::new(Segment::open(&pr.arena)?);
    let ring = Ring::attach(arena, 0)?;
    Ok((ring, proto::i64_to_fp(pr.schema_fp)))
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
    let reply = self.call_control(corr, frame, CONTROL_TIMEOUT)?;
    let list: proto::ChannelList = proto::from_frame(Kind::ChannelList, &reply)?;
    Ok(list.channels)
  }

  /// Subscribe to a channel by id and attach its arena ring.
  pub fn do_subscribe(
    &self,
    channel_id: i64,
    key: Option<&str>,
    expected_fp: Fingerprint,
  ) -> io::Result<(Ring, Fingerprint)> {
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
    let reply = self.call_control(corr, frame, CONTROL_TIMEOUT)?;
    let sr: proto::SubscribeReply = proto::from_frame(Kind::SubscribeReply, &reply)?;
    if sr.status != status::OK {
      return Err(io::Error::other(format!("subscribe failed: {}", sr.message)));
    }
    let arena = Arc::new(Segment::open(&sr.arena)?);
    let ring = Ring::attach(arena, 0)?;
    Ok((ring, proto::i64_to_fp(sr.schema_fp)))
  }

  /// Expose a function and attach its request-arena ring.
  pub fn do_expose(
    &self,
    name: &str,
    req_schema_json: &str,
    resp_schema_json: &str,
    key: Option<&str>,
    arena_cap: i64,
  ) -> io::Result<(Ring, Fingerprint, Fingerprint)> {
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
        req_arena_cap: arena_cap,
      },
    )?;
    let reply = self.call_control(corr, frame, CONTROL_TIMEOUT)?;
    let er: proto::ExposeReply = proto::from_frame(Kind::ExposeReply, &reply)?;
    if er.status != status::OK {
      return Err(io::Error::other(format!("expose failed: {}", er.message)));
    }
    let arena = Arc::new(Segment::open(&er.req_arena)?);
    let req_ring = Ring::attach(arena, 0)?;
    Ok((req_ring, proto::i64_to_fp(er.req_fp), proto::i64_to_fp(er.resp_fp)))
  }

  /// Look up a function (returns the broker's arena name + fingerprints).
  pub fn do_lookup(&self, fn_name: &str, key: Option<&str>) -> io::Result<proto::LookupReply> {
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
    let reply = self.call_control(corr, frame, CONTROL_TIMEOUT)?;
    let lr: proto::LookupReply = proto::from_frame(Kind::LookupReply, &reply)?;
    if lr.status != status::OK {
      return Err(io::Error::other(format!("lookup failed: {}", lr.message)));
    }
    Ok(lr)
  }

  // ---- registry (replay bookkeeping) ----

  pub fn register_entry(&self, reg: Reg) -> u64 {
    let id = self.reg_seq.fetch_add(1, Ordering::Relaxed);
    self.registry.lock().unwrap().insert(id, reg);
    id
  }

  pub fn deregister(&self, id: u64) {
    self.registry.lock().unwrap().remove(&id);
  }

  // ---- reconnect ----

  /// Run `op`; if it fails because the broker stopped answering *and* the live
  /// broker epoch has actually changed, reconnect and retry once. A slow server
  /// (same epoch) surfaces the original error unchanged.
  pub fn with_reconnect<T>(&self, mut op: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    match op() {
      Ok(v) => Ok(v),
      Err(e) => {
        if !self.auto_reconnect() || !broker_unreachable(&e) {
          return Err(e);
        }
        let observed = self.epoch();
        match live_broker_epoch() {
          Ok(live) if live != observed => {
            self.reconnect(observed)?;
            op()
          }
          _ => Err(e),
        }
      }
    }
  }

  /// Re-bootstrap onto the (restarted) broker: new control attach, new reply
  /// segment, re-register, and replay every owned channel/function so live
  /// handles keep working. Single-flight: a concurrent caller that lost the race
  /// observes the bumped epoch and returns early.
  pub fn reconnect(&self, failed_epoch: u64) -> io::Result<()> {
    let _g = self.reconnect_mu.lock().unwrap();
    if self.epoch() != failed_epoch {
      return Ok(()); // another thread already reconnected
    }
    log::warn!(
      "impulsed restart detected (epoch was {failed_epoch}); reconnecting '{}'",
      self.app_name
    );
    let transport = bootstrap()?;
    let new_epoch = transport.epoch;
    // In-flight calls bound to the dead broker will never be answered.
    self.pending.lock().unwrap().clear();
    *self.transport.write().unwrap() = transport;
    let client_id = self.do_register()?;
    self.replay()?;
    log::info!("reconnected to impulsed (epoch {new_epoch}) as client {client_id}");
    Ok(())
  }

  /// Re-establish every owned registration on the fresh broker and rebind its
  /// live ring cell. Best-effort per entry: a single failure is logged and does
  /// not abort the rest (the watcher will retry on the next tick).
  fn replay(&self) -> io::Result<()> {
    // Snapshot the specs so we don't hold the registry lock across control
    // round-trips.
    let (channels, functions): (Vec<ChannelReplay>, Vec<FunctionReplay>) = {
      let reg = self.registry.lock().unwrap();
      let mut channels = Vec::new();
      let mut functions = Vec::new();
      for r in reg.values() {
        match r {
          Reg::Channel {
            name,
            schema_json,
            key,
            ring,
          } => channels.push((name.clone(), schema_json.clone(), key.clone(), ring.clone())),
          Reg::Function {
            name,
            req_schema_json,
            resp_schema_json,
            key,
            arena_cap,
            ring,
          } => functions.push((
            name.clone(),
            req_schema_json.clone(),
            resp_schema_json.clone(),
            key.clone(),
            *arena_cap,
            ring.clone(),
          )),
        }
      }
      (channels, functions)
    };
    // Best-effort per entry: a single failure is logged and does not abort the
    // rest (the watcher retries on the next tick).
    for (name, schema_json, key, cell) in channels {
      match self.do_publish(&name, &schema_json, key.as_deref()) {
        Ok((ring, _fp)) => *cell.write().unwrap() = ring,
        Err(e) => log::warn!("replay: re-publish '{name}' failed: {e}"),
      }
    }
    for (name, req, resp, key, cap, cell) in functions {
      match self.do_expose(&name, &req, &resp, key.as_deref(), cap) {
        Ok((ring, _a, _b)) => *cell.write().unwrap() = ring,
        Err(e) => log::warn!("replay: re-expose '{name}' failed: {e}"),
      }
    }
    Ok(())
  }

  /// Best-effort `Unregister` on shutdown (the connection is going away).
  pub fn send_unregister(&self) {
    let corr = Self::next_corr();
    if let Ok(frame) = proto::to_frame(
      Kind::Unregister,
      &proto::Unregister {
        correlation_id: corr,
        client_id: self.client_id(),
      },
    ) {
      let _ = self.submit(&frame, Duration::from_millis(200));
    }
  }
}

/// Drain the current reply ring, routing each frame to the slot registered under
/// its correlation id. Re-reads the reply ring from the transport every tick so a
/// reconnect's swap is picked up. Exits when `running` is cleared.
fn run_dispatcher(inner: Arc<Inner>) {
  while inner.running.load(Ordering::Relaxed) {
    let reply_ring = inner.transport.read().unwrap().reply_ring.clone();
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

/// Proactively detect an `impulsed` restart by polling the live control-segment
/// epoch, so an *idle* connection (notably an RPC server that only serves and
/// never calls) re-establishes itself without waiting for a failed operation.
fn run_watcher(inner: Arc<Inner>) {
  while inner.running.load(Ordering::Relaxed) {
    std::thread::sleep(WATCH_INTERVAL);
    if !inner.running.load(Ordering::Relaxed) || !inner.auto_reconnect() {
      continue;
    }
    let observed = inner.epoch();
    if let Ok(live) = live_broker_epoch()
      && live != observed
      && let Err(e) = inner.reconnect(observed)
    {
      log::warn!("watcher: reconnect failed: {e}");
    }
  }
}
