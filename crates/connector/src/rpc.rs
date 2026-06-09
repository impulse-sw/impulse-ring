//! RPC plumbing: the async [`CallFuture`], a dependency-free `block_on`, and the
//! service loop that executes an exposed function and ships the Avro result back
//! to the caller's reply segment.

use crate::client::Slot;
use apache_avro::Schema;
use impulse_ring_core::avro;
use impulse_ring_core::frame::Frame;
use impulse_ring_core::proto::{self, Kind, RpcRequest, RpcResponse, status};
use impulse_ring_core::ring::Ring;
use impulse_ring_core::shm::Segment;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// A pending RPC call. Resolves to the decoded response or an error.
pub struct CallFuture<Resp> {
  slot: Arc<Slot>,
  resp_schema: Arc<Schema>,
  expected_resp_fp: u64,
  _pd: std::marker::PhantomData<Resp>,
}

impl<Resp: DeserializeOwned> CallFuture<Resp> {
  pub(crate) fn new(slot: Arc<Slot>, resp_schema: Arc<Schema>, expected_resp_fp: u64) -> Self {
    CallFuture {
      slot,
      resp_schema,
      expected_resp_fp,
      _pd: std::marker::PhantomData,
    }
  }

  fn decode_response(&self, frame: Frame) -> io::Result<Resp> {
    let resp: RpcResponse = proto::from_frame(Kind::RpcResponse, &frame)?;
    if resp.status != status::OK {
      return Err(io::Error::other(format!(
        "remote function error ({}): {}",
        resp.status, resp.message
      )));
    }
    let got = proto::i64_to_fp(resp.result_fp);
    if got != self.expected_resp_fp {
      return Err(io::Error::other(format!(
        "response schema mismatch: expected {:#x}, got {:#x}",
        self.expected_resp_fp, got
      )));
    }
    avro::decode(&self.resp_schema, &resp.result)
  }
}

impl<Resp: DeserializeOwned> Future for CallFuture<Resp> {
  type Output = io::Result<Resp>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    match self.slot.poll(cx) {
      Poll::Ready(frame) => Poll::Ready(self.decode_response(frame)),
      Poll::Pending => Poll::Pending,
    }
  }
}

/// Minimal executor: drive `fut` on the current thread until it resolves or
/// `timeout` elapses. Returns `None` on timeout. No async runtime required.
pub fn block_on<F: Future>(fut: F, timeout: Duration) -> Option<F::Output> {
  use std::sync::atomic::AtomicBool as Flag;
  use std::task::Wake;

  struct ThreadWaker {
    thread: std::thread::Thread,
    notified: Flag,
  }
  impl Wake for ThreadWaker {
    fn wake(self: Arc<Self>) {
      self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
      self.notified.store(true, Ordering::Release);
      self.thread.unpark();
    }
  }

  let waker_state = Arc::new(ThreadWaker {
    thread: std::thread::current(),
    notified: Flag::new(false),
  });
  let waker = waker_state.clone().into();
  let mut cx = Context::from_waker(&waker);
  let mut fut = std::pin::pin!(fut);

  let deadline = Instant::now() + timeout;
  loop {
    if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
      return Some(v);
    }
    let now = Instant::now();
    if now >= deadline {
      return None;
    }
    if !waker_state.notified.swap(false, Ordering::Acquire) {
      std::thread::park_timeout(deadline - now);
    }
  }
}

/// Run an exposed function: pop requests, decode args, execute the handler, and
/// push the encoded response to each caller's reply segment.
#[allow(clippy::too_many_arguments)]
pub fn spawn_service<Req, Resp, F>(
  req_ring: Ring,
  req_schema: Arc<Schema>,
  resp_schema: Arc<Schema>,
  req_fp: u64,
  resp_fp: u64,
  handler: F,
  running: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()>
where
  Req: DeserializeOwned,
  Resp: Serialize,
  F: Fn(Req) -> Resp + Send + 'static,
{
  std::thread::spawn(move || {
    // Cache opened caller reply segments so we don't reopen per call.
    let mut reply_rings: HashMap<String, Ring> = HashMap::new();
    while running.load(Ordering::Relaxed) {
      let Some(bytes) = req_ring.pop_blocking(Some(Duration::from_millis(100))) else {
        continue;
      };
      let frame = match Frame::decode(&bytes) {
        Ok(f) => f,
        Err(e) => {
          log::warn!("service: bad request frame: {e}");
          continue;
        }
      };
      let req: RpcRequest = match proto::from_frame(Kind::RpcRequest, &frame) {
        Ok(r) => r,
        Err(e) => {
          log::warn!("service: cannot decode RpcRequest: {e}");
          continue;
        }
      };
      let response = handle_one::<Req, Resp, F>(&req, &req_schema, &resp_schema, req_fp, resp_fp, &handler);
      // Route the response to the caller's reply segment.
      if let Err(e) = send_response(&mut reply_rings, &req.reply_segment, &response) {
        log::warn!("service: cannot deliver response: {e}");
      }
    }
  })
}

fn handle_one<Req, Resp, F>(
  req: &RpcRequest,
  req_schema: &Schema,
  resp_schema: &Schema,
  req_fp: u64,
  resp_fp: u64,
  handler: &F,
) -> RpcResponse
where
  Req: DeserializeOwned,
  Resp: Serialize,
  F: Fn(Req) -> Resp,
{
  // Validate the caller's argument schema against ours.
  if proto::i64_to_fp(req.arg_fp) != req_fp {
    return err_response(
      req.correlation_id,
      status::ERR_SCHEMA_MISMATCH,
      "request schema mismatch",
    );
  }
  let decoded: Req = match avro::decode(req_schema, &req.args) {
    Ok(v) => v,
    Err(e) => return err_response(req.correlation_id, status::ERR_INTERNAL, &format!("bad args: {e}")),
  };
  let result = handler(decoded);
  match avro::encode(resp_schema, &result) {
    Ok(body) => RpcResponse {
      correlation_id: req.correlation_id,
      status: status::OK,
      message: String::new(),
      result_fp: proto::fp_to_i64(resp_fp),
      result: body,
    },
    Err(e) => err_response(req.correlation_id, status::ERR_INTERNAL, &format!("encode failed: {e}")),
  }
}

fn send_response(cache: &mut HashMap<String, Ring>, reply_segment: &str, response: &RpcResponse) -> io::Result<()> {
  if !cache.contains_key(reply_segment) {
    let seg = Arc::new(Segment::open(reply_segment)?);
    let ring = Ring::attach(seg, 0)?;
    cache.insert(reply_segment.to_string(), ring);
  }
  let ring = cache.get(reply_segment).unwrap();
  let frame = proto::to_frame(Kind::RpcResponse, response)?;
  if !ring.push_blocking(&frame.encode(), Some(Duration::from_secs(2))) {
    return Err(io::Error::other("caller reply ring full"));
  }
  Ok(())
}

fn err_response(correlation_id: i64, code: i32, msg: &str) -> RpcResponse {
  RpcResponse {
    correlation_id,
    status: code,
    message: msg.into(),
    result_fp: 0,
    result: Vec::new(),
  }
}
