//! The Ring broker (`impulsed`): owns the control segment and data arenas,
//! registers applications, and routes the control plane. The data plane
//! (channel messages and RPC payloads) flows peer-to-peer through arenas the
//! broker hands out — the broker is never on the hot copy path.

use crate::registry::{ChannelMeta, FunctionMeta, Registry};
use impulse_ring_core::control;
use impulse_ring_core::frame::Frame;
use impulse_ring_core::proto::{self, Kind, status};
use impulse_ring_core::ring::{Ring, ring_bytes};
use impulse_ring_core::shm::{self, Segment};
use impulse_ring_core::util;
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub struct Broker {
  _control_seg: Arc<Segment>,
  submission: Ring,
  reg: Registry,
  reply_rings: HashMap<i64, Ring>,
  _reply_segs: HashMap<i64, Arc<Segment>>,
  /// Arenas created by the broker, keyed by segment name; kept mapped while in
  /// use and unlinked when their owning channel/function is reclaimed or the
  /// bus shuts down.
  arenas: HashMap<String, Arc<Segment>>,
  /// Names of client-owned reply segments, unlinked when the bus shuts down so
  /// a client that died ungracefully does not leak shared memory.
  reply_names: Vec<String>,
}

/// Why [`Broker::start`] could not bring the broker up.
#[derive(Debug)]
pub enum StartError {
  /// Another `impulsed` is already running and owns the control segment.
  ///
  /// Starting a second broker would clobber the live one's shared memory, so
  /// `start` refuses instead. Callers that merely want *a* broker running can
  /// treat this as success and carry on.
  AlreadyRunning,
  /// An OS error occurred while bringing the broker up.
  Io(io::Error),
}

impl std::fmt::Display for StartError {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    match self {
      StartError::AlreadyRunning => write!(f, "another impulsed broker is already running"),
      StartError::Io(e) => write!(f, "{e}"),
    }
  }
}

impl std::error::Error for StartError {}

impl From<io::Error> for StartError {
  fn from(e: io::Error) -> Self {
    StartError::Io(e)
  }
}

impl Drop for Broker {
  fn drop(&mut self) {
    // The bus is going down; reclaim client reply segments (the broker only
    // opened them, so they are not unlinked by their `Segment` drop).
    for name in &self.reply_names {
      let _ = shm::unlink(name);
    }
  }
}

impl Broker {
  /// Bring up the broker: refuse if a live broker already owns the bus, then
  /// garbage-collect stale segments and create and format the control segment.
  ///
  /// Returns [`StartError::AlreadyRunning`] if another `impulsed` is already
  /// running — in that case nothing is touched (the live broker's control
  /// segment is left intact).
  pub fn start() -> Result<Broker, StartError> {
    // The singleton guard MUST come before `cleanup_stale`: that step unlinks
    // every Ring segment, which would tear the shared memory out from under a
    // broker that is already serving clients.
    Self::guard_singleton()?;

    Self::cleanup_stale();
    let bytes = control::control_segment_bytes();
    let seg = Arc::new(Segment::create(util::CONTROL_SEGMENT, bytes)?);
    let pid = std::process::id() as i32;
    let epoch = SystemTime::now()
      .duration_since(UNIX_EPOCH)
      .map(|d| d.as_nanos() as u64)
      .unwrap_or(0);
    let submission = control::format_control(seg.clone(), pid, epoch)?;
    log::info!("impulsed up: control={} pid={pid}", util::CONTROL_SEGMENT);
    Ok(Broker {
      _control_seg: seg,
      submission,
      reg: Registry::new(),
      reply_rings: HashMap::new(),
      _reply_segs: HashMap::new(),
      arenas: HashMap::new(),
      reply_names: Vec::new(),
    })
  }

  /// Ensure at most one broker owns the bus, using the control segment itself
  /// as the singleton token (no separate lock file — that would be owned by
  /// whoever created it first and break across `sudo`/non-`sudo` runs).
  ///
  /// Inspects the existing control segment, if any:
  /// - a valid segment whose recorded broker PID is still alive ⇒
  ///   [`StartError::AlreadyRunning`] (leave it untouched);
  /// - a stale segment (dead PID or not a valid control segment) ⇒ unlink it
  ///   and let the caller recreate it;
  /// - no segment ⇒ proceed;
  /// - a segment we can't even open (`EACCES`, i.e. owned by another user) ⇒
  ///   assume a foreign live broker and report [`StartError::AlreadyRunning`]
  ///   rather than failing hard.
  fn guard_singleton() -> Result<(), StartError> {
    match Segment::open(util::CONTROL_SEGMENT) {
      Ok(seg) => {
        let seg = Arc::new(seg);
        // A valid, ready control segment with a live PID means a broker is up.
        if control::attach_control(seg.clone()).is_ok() {
          let pid = control::broker_pid(&seg);
          if shm::pid_alive(pid) {
            return Err(StartError::AlreadyRunning);
          }
          log::warn!("found stale control segment from dead broker pid={pid}; reclaiming");
        } else {
          log::warn!("found invalid control segment; reclaiming");
        }
        drop(seg);
        let _ = shm::unlink(util::CONTROL_SEGMENT);
        Ok(())
      }
      Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
      Err(e) if e.raw_os_error() == Some(libc::EACCES) => {
        // The control segment exists but belongs to another user; don't clobber
        // it and don't crash — treat it as a foreign broker already running.
        log::warn!("control segment owned by another user; assuming a broker is already running");
        Err(StartError::AlreadyRunning)
      }
      Err(e) => Err(StartError::Io(e)),
    }
  }

  /// Remove any Ring segments left behind by a previous (possibly crashed)
  /// broker run. Safe in the Milestone 1 single-broker model.
  fn cleanup_stale() {
    if let Ok(names) = shm::list_segments() {
      for n in names {
        let _ = shm::unlink(&n);
      }
    }
  }

  /// Process control submissions until `should_run` returns false.
  pub fn run(&mut self, should_run: impl Fn() -> bool) {
    while should_run() {
      if let Some(bytes) = self.submission.pop_blocking(Some(Duration::from_millis(200)))
        && let Err(e) = self.handle(&bytes)
      {
        log::warn!("control message error: {e}");
      }
    }
  }

  fn handle(&mut self, bytes: &[u8]) -> io::Result<()> {
    let frame = Frame::decode(bytes)?;
    let kind = proto::catalog()
      .kind_of(frame.schema_fp)
      .ok_or_else(|| io::Error::other("unknown control schema fingerprint"))?;
    match kind {
      Kind::Register => self.on_register(&frame),
      Kind::Unregister => self.on_unregister(&frame),
      Kind::PublishChannel => self.on_publish(&frame),
      Kind::ListChannels => self.on_list(&frame),
      Kind::Subscribe => self.on_subscribe(&frame),
      Kind::ExposeFunction => self.on_expose(&frame),
      Kind::LookupFunction => self.on_lookup(&frame),
      Kind::Heartbeat => Ok(()), // liveness only; reaping is lenient in M1
      other => Err(io::Error::other(format!("unexpected control kind {other:?}"))),
    }
  }

  /// Push a reply frame onto a client's reply ring.
  fn reply(&self, client_id: i64, kind: Kind, msg: &impl serde::Serialize) -> io::Result<()> {
    let ring = self
      .reply_rings
      .get(&client_id)
      .ok_or_else(|| io::Error::other("reply ring for client not found"))?;
    let frame = proto::to_frame(kind, msg)?;
    if !ring.push_blocking(&frame.encode(), Some(Duration::from_secs(2))) {
      return Err(io::Error::other("reply ring full (client stuck?)"));
    }
    Ok(())
  }

  fn on_register(&mut self, frame: &Frame) -> io::Result<()> {
    // `from_frame_compat` lets a newer broker still decode an older connector's
    // Register (the pre-`pid` schema); `pid` then defaults to 0 ("unknown").
    let m: proto::Register = proto::from_frame_compat(frame)?;
    // Attach to the reply segment the client created during bootstrap.
    let seg = Arc::new(Segment::open(&m.reply_segment)?);
    let ring = Ring::attach(seg.clone(), 0)?;
    let client_id = self.reg.add_client(m.app_name.clone(), m.pid as i32);
    self.reply_rings.insert(client_id, ring);
    self._reply_segs.insert(client_id, seg);
    self.reply_names.push(m.reply_segment.clone());
    log::info!("registered '{}' as client {client_id} pid={}", m.app_name, m.pid);
    self.reply(
      client_id,
      Kind::RegisterReply,
      &proto::RegisterReply {
        correlation_id: m.correlation_id,
        client_id,
        status: status::OK,
        message: String::new(),
      },
    )
  }

  fn on_unregister(&mut self, frame: &Frame) -> io::Result<()> {
    let m: proto::Unregister = proto::from_frame(Kind::Unregister, frame)?;
    self.release_client(m.client_id);
    Ok(())
  }

  /// Liveness policy for a name's owner, by its registered pid.
  ///
  /// Pid `0` means "unknown" (an older connector that does not report its pid);
  /// we conservatively treat it as alive so a name is never reclaimed unless we
  /// positively observe the owning process is gone. Real pids are probed with
  /// `kill(pid, 0)` via [`shm::pid_alive`].
  fn owner_alive(pid: i32) -> bool {
    pid == 0 || shm::pid_alive(pid)
  }

  /// Reclaim everything a departing client owned: its channels and functions
  /// (so their names become free again) and the backing arenas, plus its reply
  /// ring. This is what lets an app restart and re-publish the same channel.
  fn release_client(&mut self, client_id: i64) {
    for arena in self.reg.remove_client_channels(client_id) {
      self.reclaim_arena(&arena);
    }
    for arena in self.reg.remove_client_functions(client_id) {
      self.reclaim_arena(&arena);
    }
    self.reg.remove_client(client_id);
    self.reply_rings.remove(&client_id);
    self._reply_segs.remove(&client_id);
  }

  /// Create + format a data arena, keep it mapped, and return its name.
  fn make_arena(&mut self, name: String, cap: usize) -> io::Result<String> {
    let seg = Arc::new(Segment::create(&name, ring_bytes(cap))?);
    Ring::format(seg.clone(), 0, cap)?;
    self.arenas.insert(name.clone(), seg);
    Ok(name)
  }

  /// Drop the broker's mapping for an arena, which unlinks it from /dev/shm.
  fn reclaim_arena(&mut self, name: &str) {
    // Dropping the last `Arc<Segment>` unlinks the segment. Subscribers in other
    // processes keep their own mappings until they close (POSIX semantics).
    self.arenas.remove(name);
  }

  fn on_publish(&mut self, frame: &Frame) -> io::Result<()> {
    let m: proto::PublishChannel = proto::from_frame(Kind::PublishChannel, frame)?;
    if self.reg.channel_name_taken(&m.channel) {
      // As with functions: reclaim the channel name if its owner has died.
      match self.reg.dead_channel_owner(&m.channel, Self::owner_alive) {
        Some(dead) => {
          log::warn!(
            "reclaiming channel '{}' from dead owner client={dead}; re-publishing for client {}",
            m.channel,
            m.client_id
          );
          self.release_client(dead);
        }
        None => {
          return self.reply(
            m.client_id,
            Kind::PublishReply,
            &proto::PublishReply {
              correlation_id: m.correlation_id,
              channel_id: 0,
              schema_fp: 0,
              arena: String::new(),
              status: status::ERR_EXISTS,
              message: "channel already exists".into(),
            },
          );
        }
      }
    }
    let (_schema, fp) = match impulse_ring_core::avro::parse(&m.schema_json) {
      Ok(v) => v,
      Err(e) => {
        return self.reply(
          m.client_id,
          Kind::PublishReply,
          &proto::PublishReply {
            correlation_id: m.correlation_id,
            channel_id: 0,
            schema_fp: 0,
            arena: String::new(),
            status: status::ERR_INTERNAL,
            message: format!("bad schema: {e}"),
          },
        );
      }
    };
    let channel_id = self.reg.alloc_channel_id();
    let arena = self.make_arena(util::channel_arena(channel_id as u64), control::ARENA_CAP)?;
    let key = (!m.access_key.is_empty()).then(|| util::key_hash(&m.access_key));
    self.reg.insert_channel(ChannelMeta {
      id: channel_id,
      name: m.channel.clone(),
      owner: m.client_id,
      schema_fp: fp,
      key,
      arena: arena.clone(),
    });
    log::info!("channel '{}' id={channel_id} fp={fp:#x}", m.channel);
    self.reply(
      m.client_id,
      Kind::PublishReply,
      &proto::PublishReply {
        correlation_id: m.correlation_id,
        channel_id,
        schema_fp: proto::fp_to_i64(fp),
        arena,
        status: status::OK,
        message: String::new(),
      },
    )
  }

  fn on_list(&mut self, frame: &Frame) -> io::Result<()> {
    let m: proto::ListChannels = proto::from_frame(Kind::ListChannels, frame)?;
    let channels = self.reg.list_channels();
    self.reply(
      m.client_id,
      Kind::ChannelList,
      &proto::ChannelList {
        correlation_id: m.correlation_id,
        channels,
      },
    )
  }

  fn on_subscribe(&mut self, frame: &Frame) -> io::Result<()> {
    let m: proto::Subscribe = proto::from_frame(Kind::Subscribe, frame)?;
    let reply = match self
      .reg
      .resolve_subscribe(m.channel_id, &m.access_key, proto::i64_to_fp(m.expected_fp))
    {
      Ok((arena, fp)) => proto::SubscribeReply {
        correlation_id: m.correlation_id,
        arena,
        schema_fp: proto::fp_to_i64(fp),
        status: status::OK,
        message: String::new(),
      },
      Err((code, msg)) => proto::SubscribeReply {
        correlation_id: m.correlation_id,
        arena: String::new(),
        schema_fp: 0,
        status: code,
        message: msg,
      },
    };
    self.reply(m.client_id, Kind::SubscribeReply, &reply)
  }

  fn on_expose(&mut self, frame: &Frame) -> io::Result<()> {
    // `from_frame_compat` lets a newer broker still decode an older connector's
    // ExposeFunction (one without `req_arena_cap`), which serde defaults to 0.
    let m: proto::ExposeFunction = proto::from_frame_compat(frame)?;
    if self.reg.function_name_taken(&m.fn_name) {
      // The name is taken — but if its owner has died (e.g. a previous instance
      // was SIGKILLed or crashed before it could unregister), reclaim it so this
      // restart can re-expose. Only reclaim when we positively know the owner is
      // gone: an unknown pid (0, legacy connector) is treated as alive.
      match self.reg.dead_function_owner(&m.fn_name, Self::owner_alive) {
        Some(dead) => {
          log::warn!(
            "reclaiming function '{}' from dead owner client={dead}; re-exposing for client {}",
            m.fn_name,
            m.client_id
          );
          self.release_client(dead);
        }
        None => {
          return self.reply(
            m.client_id,
            Kind::ExposeReply,
            &err_expose(m.correlation_id, status::ERR_EXISTS, "function exists"),
          );
        }
      }
    }
    let req = impulse_ring_core::avro::parse(&m.req_schema_json);
    let resp = impulse_ring_core::avro::parse(&m.resp_schema_json);
    let (req_fp, resp_fp) = match (req, resp) {
      (Ok((_, a)), Ok((_, b))) => (a, b),
      _ => {
        return self.reply(
          m.client_id,
          Kind::ExposeReply,
          &err_expose(m.correlation_id, status::ERR_INTERNAL, "bad schema"),
        );
      }
    };
    let fn_id = self.reg.alloc_fn_id();
    let arena_cap = control::clamp_arena_cap(m.req_arena_cap.max(0) as usize);
    let req_arena = self.make_arena(util::function_arena(fn_id as u64), arena_cap)?;
    if arena_cap != control::ARENA_CAP {
      log::info!("function '{}' uses a {arena_cap}-byte request arena", m.fn_name);
    }
    let key = (!m.access_key.is_empty()).then(|| util::key_hash(&m.access_key));
    self.reg.insert_function(FunctionMeta {
      id: fn_id,
      name: m.fn_name.clone(),
      owner: m.client_id,
      req_fp,
      resp_fp,
      key,
      req_arena: req_arena.clone(),
    });
    log::info!("function '{}' id={fn_id} req_fp={req_fp:#x}", m.fn_name);
    self.reply(
      m.client_id,
      Kind::ExposeReply,
      &proto::ExposeReply {
        correlation_id: m.correlation_id,
        fn_id,
        req_fp: proto::fp_to_i64(req_fp),
        resp_fp: proto::fp_to_i64(resp_fp),
        req_arena,
        status: status::OK,
        message: String::new(),
      },
    )
  }

  fn on_lookup(&mut self, frame: &Frame) -> io::Result<()> {
    let m: proto::LookupFunction = proto::from_frame(Kind::LookupFunction, frame)?;
    let reply = match self.reg.resolve_lookup(&m.fn_name, &m.access_key) {
      Ok((fn_id, req_fp, resp_fp, req_arena)) => proto::LookupReply {
        correlation_id: m.correlation_id,
        fn_id,
        req_fp: proto::fp_to_i64(req_fp),
        resp_fp: proto::fp_to_i64(resp_fp),
        req_arena,
        status: status::OK,
        message: String::new(),
      },
      Err((code, msg)) => proto::LookupReply {
        correlation_id: m.correlation_id,
        fn_id: 0,
        req_fp: 0,
        resp_fp: 0,
        req_arena: String::new(),
        status: code,
        message: msg,
      },
    };
    self.reply(m.client_id, Kind::LookupReply, &reply)
  }
}

fn err_expose(correlation_id: i64, code: i32, msg: &str) -> proto::ExposeReply {
  proto::ExposeReply {
    correlation_id,
    fn_id: 0,
    req_fp: 0,
    resp_fp: 0,
    req_arena: String::new(),
    status: code,
    message: msg.into(),
  }
}
