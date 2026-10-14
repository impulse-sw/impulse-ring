//! RPC plumbing: the async [`CallFuture`], a dependency-free `block_on`, and the
//! service loop that executes an exposed function and ships the Avro result back
//! to the caller's reply segment.

use crate::client::{RingCell, Slot};
use apache_avro::Schema;
use impulse_ring_core::avro;
use impulse_ring_core::frame::Frame;
use impulse_ring_core::proto::{self, Kind, RpcRequest, RpcResponse, status};
use impulse_ring_core::ring::Ring;
use impulse_ring_core::shm::Segment;
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
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
///
/// The request ring is read through a [`RingCell`] so that a broker-restart
/// reconnect (which re-exposes the function on a fresh arena) is picked up on the
/// next poll without restarting this thread.
/// How many requests one exposed function may have in flight at once, unless the
/// caller says otherwise ([`Connection::set_service_workers`]).
///
/// The number is about held requests, not cores: a handler that waits — a long
/// poll, a slow database, an upstream of its own — occupies a worker without
/// using a CPU, and that is the case this bound exists for. Threads are spawned
/// lazily, so an application answering one request at a time still runs one.
pub const DEFAULT_SERVICE_WORKERS: usize = 64;

/// One request handed to a worker, with everything needed to answer it.
struct Job {
  req: RpcRequest,
}

/// Turns a decoded request into a response. Generics are erased here so the pool
/// below is one type regardless of what the function signature was.
type Serve = Arc<dyn Fn(&RpcRequest) -> RpcResponse + Send + Sync + 'static>;

/// The worker pool behind one exposed function.
///
/// Why a pool at all: the service used to run the handler on the thread that
/// read the request, so an exposed function answered strictly one request at a
/// time. Any handler that *waits* — and a long poll waits on purpose — stopped
/// every other caller of that application; their requests piled up in the
/// request arena until it was full, and then callers got `function request ring
/// full` while the application's own log showed nothing, because it had not read
/// those requests yet.
struct Pool {
  queue: Mutex<VecDeque<Job>>,
  idle: Condvar,
  /// Jobs waiting for a worker, and workers not currently serving one. Counted
  /// rather than derived, because a worker is "busy" from the moment it takes a
  /// job off the queue.
  state: Mutex<Counts>,
  max_workers: usize,
  running: Arc<AtomicBool>,
  serve: Serve,
}

#[derive(Default)]
struct Counts {
  workers: usize,
  idle_workers: usize,
}

impl Pool {
  /// Hand a request to an idle worker, spawning one if there is none and the
  /// bound allows it.
  fn submit(self: &Arc<Self>, job: Job) {
    self.queue.lock().unwrap().push_back(job);
    let spawn = {
      let mut counts = self.state.lock().unwrap();
      if counts.idle_workers == 0 && counts.workers < self.max_workers {
        counts.workers += 1;
        true
      } else {
        false
      }
    };
    if spawn {
      let pool = Arc::clone(self);
      std::thread::spawn(move || pool.work());
    }
    self.idle.notify_one();
  }

  /// A worker: take a job, answer it, repeat. Exits when the connection stops.
  fn work(self: Arc<Self>) {
    // Each worker keeps its own cache of caller reply rings. Writing into one
    // from several workers at once is safe — a ring serialises its producers —
    // so no coordination is needed beyond that.
    let mut reply_rings: HashMap<String, Ring> = HashMap::new();
    loop {
      let job = {
        let mut queue = self.queue.lock().unwrap();
        loop {
          if let Some(job) = queue.pop_front() {
            break Some(job);
          }
          if !self.running.load(Ordering::Relaxed) {
            break None;
          }
          self.state.lock().unwrap().idle_workers += 1;
          let (q, _) = self
            .idle
            .wait_timeout(queue, Duration::from_millis(200))
            .expect("service queue mutex");
          queue = q;
          self.state.lock().unwrap().idle_workers -= 1;
        }
      };
      let Some(job) = job else { break };

      // A panicking handler used to take the service thread with it: the
      // application stayed registered on the bus and never answered again. It
      // answers an error instead, and keeps serving.
      let response = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.serve)(&job.req))) {
        Ok(response) => response,
        Err(_) => {
          log::error!("service: handler panicked; answering an error instead of dying");
          err_response(job.req.correlation_id, status::ERR_INTERNAL, "handler panicked")
        }
      };
      if let Err(e) = send_response(&mut reply_rings, &job.req.reply_segment, &response) {
        log::warn!("service: cannot deliver response: {e}");
      }
    }
    self.state.lock().unwrap().workers -= 1;
  }
}

/// Read requests off the function's arena and hand them to the pool.
///
/// The returned handle is the reader thread; workers are spawned by the pool as
/// concurrency demands and exit when `running` is cleared.
#[allow(clippy::too_many_arguments)]
pub fn spawn_service<Req, Resp, F>(
  req_ring: RingCell,
  req_schema: Arc<Schema>,
  resp_schema: Arc<Schema>,
  req_fp: u64,
  resp_fp: u64,
  handler: F,
  running: Arc<AtomicBool>,
  max_workers: usize,
) -> std::thread::JoinHandle<()>
where
  Req: DeserializeOwned,
  Resp: Serialize,
  F: Fn(Req) -> Resp + Send + Sync + 'static,
{
  let handler = Arc::new(handler);
  let serve: Serve = Arc::new(move |req: &RpcRequest| {
    handle_one::<Req, Resp, F>(req, &req_schema, &resp_schema, req_fp, resp_fp, &handler)
  });
  let pool = Arc::new(Pool {
    queue: Mutex::new(VecDeque::new()),
    idle: Condvar::new(),
    state: Mutex::new(Counts::default()),
    max_workers: max_workers.max(1),
    running: running.clone(),
    serve,
  });

  std::thread::spawn(move || {
    while running.load(Ordering::Relaxed) {
      let ring = req_ring.read().unwrap().clone();
      let Some(bytes) = ring.pop_blocking(Some(Duration::from_millis(100))) else {
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
      // Reading stays on this one thread: the arena is single-consumer. Only the
      // answering is spread out.
      pool.submit(Job { req });
    }
    // Wake every worker so they see the cleared flag and finish.
    pool.idle.notify_all();
  })
}

fn handle_one<Req, Resp, F>(
  req: &RpcRequest,
  req_schema: &Schema,
  resp_schema: &Schema,
  req_fp: u64,
  resp_fp: u64,
  handler: &Arc<F>,
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
