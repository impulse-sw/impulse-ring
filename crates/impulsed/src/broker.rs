//! The Ring broker (`impulsed`): owns the control segment and data arenas,
//! registers applications, and routes the control plane. The data plane
//! (channel messages and RPC payloads) flows peer-to-peer through arenas the
//! broker hands out — the broker is never on the hot copy path.

use crate::registry::{ChannelMeta, FunctionMeta, Registry};
use impulse_core::control;
use impulse_core::frame::Frame;
use impulse_core::proto::{self, status, Kind};
use impulse_core::ring::{ring_bytes, Ring};
use impulse_core::shm::{self, Segment};
use impulse_core::util;
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
    /// Arenas created by the broker; kept mapped and unlinked on shutdown.
    arenas: Vec<Arc<Segment>>,
    /// Names of client-owned reply segments, unlinked when the bus shuts down so
    /// a client that died ungracefully does not leak shared memory.
    reply_names: Vec<String>,
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
    /// Bring up the broker: garbage-collect stale segments from a prior run,
    /// then create and format the control segment.
    pub fn start() -> io::Result<Broker> {
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
            arenas: Vec::new(),
            reply_names: Vec::new(),
        })
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
            if let Some(bytes) = self
                .submission
                .pop_blocking(Some(Duration::from_millis(200)))
            {
                if let Err(e) = self.handle(&bytes) {
                    log::warn!("control message error: {e}");
                }
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
            other => Err(io::Error::other(format!(
                "unexpected control kind {other:?}"
            ))),
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
        let m: proto::Register = proto::from_frame(Kind::Register, frame)?;
        // Attach to the reply segment the client created during bootstrap.
        let seg = Arc::new(Segment::open(&m.reply_segment)?);
        let ring = Ring::attach(seg.clone(), 0)?;
        let client_id = self.reg.add_client(m.app_name.clone());
        self.reply_rings.insert(client_id, ring);
        self._reply_segs.insert(client_id, seg);
        self.reply_names.push(m.reply_segment.clone());
        log::info!("registered '{}' as client {client_id}", m.app_name);
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
        self.reg.remove_client(m.client_id);
        self.reply_rings.remove(&m.client_id);
        self._reply_segs.remove(&m.client_id);
        Ok(())
    }

    /// Create + format a data arena, keep it mapped, and return its name.
    fn make_arena(&mut self, name: String, cap: usize) -> io::Result<String> {
        let seg = Arc::new(Segment::create(&name, ring_bytes(cap))?);
        Ring::format(seg.clone(), 0, cap)?;
        self.arenas.push(seg);
        Ok(name)
    }

    fn on_publish(&mut self, frame: &Frame) -> io::Result<()> {
        let m: proto::PublishChannel = proto::from_frame(Kind::PublishChannel, frame)?;
        if self.reg.channel_name_taken(&m.channel) {
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
        let (_schema, fp) = match impulse_core::avro::parse(&m.schema_json) {
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
                )
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
        let reply = match self.reg.resolve_subscribe(
            m.channel_id,
            &m.access_key,
            proto::i64_to_fp(m.expected_fp),
        ) {
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
        let m: proto::ExposeFunction = proto::from_frame(Kind::ExposeFunction, frame)?;
        if self.reg.function_name_taken(&m.fn_name) {
            return self.reply(
                m.client_id,
                Kind::ExposeReply,
                &err_expose(m.correlation_id, status::ERR_EXISTS, "function exists"),
            );
        }
        let req = impulse_core::avro::parse(&m.req_schema_json);
        let resp = impulse_core::avro::parse(&m.resp_schema_json);
        let (req_fp, resp_fp) = match (req, resp) {
            (Ok((_, a)), Ok((_, b))) => (a, b),
            _ => {
                return self.reply(
                    m.client_id,
                    Kind::ExposeReply,
                    &err_expose(m.correlation_id, status::ERR_INTERNAL, "bad schema"),
                )
            }
        };
        let fn_id = self.reg.alloc_fn_id();
        let req_arena = self.make_arena(util::function_arena(fn_id as u64), control::ARENA_CAP)?;
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
