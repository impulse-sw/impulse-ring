//! End-to-end test: spawn the real `impulsed` broker as a child process and
//! drive a full register -> publish/subscribe -> RPC flow through shared memory,
//! including the negative access-control and schema-mismatch paths.

use impulse_ring_connector::Connection;
use serde::{Deserialize, Serialize};
use std::io::{BufRead, BufReader};
use std::process::{Child, Command, Stdio};
use std::time::Duration;

/// Env flag that turns a re-exec of this test binary into the "victim": it
/// registers, exposes a function, prints a readiness marker, then idles forever
/// so the parent can SIGKILL it (no graceful `Unregister`) to simulate a crash.
const VICTIM_ENV: &str = "RING_E2E_VICTIM";
const VICTIM_READY: &str = "VICTIM_READY";

const METRIC_SCHEMA: &str = r#"{
  "type":"record","name":"Metric","namespace":"ring.test",
  "fields":[{"name":"name","type":"string"},{"name":"value","type":"double"}]
}"#;

const ADD_REQ_SCHEMA: &str = r#"{
  "type":"record","name":"AddReq","namespace":"ring.test",
  "fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]
}"#;

const ADD_RESP_SCHEMA: &str = r#"{
  "type":"record","name":"AddResp","namespace":"ring.test",
  "fields":[{"name":"sum","type":"long"}]
}"#;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Metric {
  name: String,
  value: f64,
}

#[derive(Serialize, Deserialize)]
struct AddReq {
  a: i64,
  b: i64,
}

#[derive(Serialize, Deserialize)]
struct AddResp {
  sum: i64,
}

/// RAII guard that SIGTERMs the broker and reaps it.
struct BrokerGuard(Child);
impl Drop for BrokerGuard {
  fn drop(&mut self) {
    let pid = self.0.id() as i32;
    unsafe { libc::kill(pid, libc::SIGTERM) };
    for _ in 0..60 {
      if let Ok(Some(_)) = self.0.try_wait() {
        return;
      }
      std::thread::sleep(Duration::from_millis(50));
    }
    let _ = self.0.kill();
    let _ = self.0.wait();
  }
}

/// Snapshot the Ring segment names currently present in `/dev/shm`.
fn scan_ring_segments() -> std::collections::HashSet<String> {
  std::fs::read_dir("/dev/shm")
    .map(|rd| {
      rd.filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("impulse-ring."))
        .collect()
    })
    .unwrap_or_default()
}

fn start_broker() -> BrokerGuard {
  let child = Command::new(env!("CARGO_BIN_EXE_impulsed"))
    .spawn()
    .expect("spawn impulsed");
  let guard = BrokerGuard(child);
  // Wait for the broker to publish the control segment.
  for _ in 0..100 {
    if std::path::Path::new("/dev/shm/impulse-ring.ctl.v1").exists() {
      // Give it a beat to finish formatting the ring header.
      std::thread::sleep(Duration::from_millis(50));
      return guard;
    }
    std::thread::sleep(Duration::from_millis(20));
  }
  panic!("broker did not come up");
}

/// The victim role (a separate process): expose `crash.add` against the broker
/// the parent already started, announce readiness, then block until killed.
/// Crucially it never returns, so `Connection::drop` (and thus `Unregister`)
/// never runs — exactly what a `SIGKILL`/crash looks like to the broker.
fn run_victim() -> ! {
  let conn = Connection::connect("crash-victim").expect("victim connect");
  conn
    .expose_function::<AddReq, AddResp, _>("crash.add", ADD_REQ_SCHEMA, ADD_RESP_SCHEMA, None, |req| AddResp {
      sum: req.a + req.b,
    })
    .expect("victim expose");
  // Flush a readiness marker the parent waits for before killing us.
  println!("{VICTIM_READY}");
  use std::io::Write;
  let _ = std::io::stdout().flush();
  loop {
    std::thread::sleep(Duration::from_secs(3600));
  }
}

#[test]
fn full_flow_pubsub_and_rpc() {
  // If re-exec'd as the victim, take that role and never come back (no broker,
  // no Unregister) — see `crash_reclaim` section below.
  if std::env::var(VICTIM_ENV).is_ok() {
    run_victim();
  }

  // Segments already present belong to a concurrent test binary or a live Ring
  // deployment on this host, not to us. Snapshot them so the leak check below
  // judges only what *this* test created, instead of asserting a clean global
  // `/dev/shm` (which fails whenever anything else on the machine uses Ring).
  let preexisting = scan_ring_segments();

  let _broker = start_broker();

  {
    // --- Service application: publishes a channel and exposes a function.
    let svc = Connection::connect("svc-a").expect("connect svc");
    let publisher = svc
      .publish_channel("metrics", METRIC_SCHEMA, Some("chan-key"))
      .expect("publish channel");
    publisher
      .publish(&Metric {
        name: "cpu".into(),
        value: 0.75,
      })
      .expect("publish message");

    svc
      .expose_function::<AddReq, AddResp, _>("add", ADD_REQ_SCHEMA, ADD_RESP_SCHEMA, Some("fn-key"), |req| AddResp {
        sum: req.a + req.b,
      })
      .expect("expose add");

    // --- Client application: lists, subscribes, receives, and calls.
    let client = Connection::connect("client-b").expect("connect client");

    let channels = client.list_channels().expect("list channels");
    let metrics = channels
      .iter()
      .find(|c| c.name == "metrics")
      .expect("metrics channel listed");
    assert!(metrics.requires_key, "metrics must require a key");
    let channel_id = metrics.channel_id;

    // Wrong key is denied.
    let denied = client.subscribe(channel_id, Some("wrong"), METRIC_SCHEMA);
    assert!(denied.is_err(), "subscribe with wrong key must fail");

    // Wrong schema is a fingerprint mismatch.
    let mismatch = client.subscribe(
      channel_id,
      Some("chan-key"),
      r#"{"type":"record","name":"Other","fields":[{"name":"x","type":"int"}]}"#,
    );
    assert!(mismatch.is_err(), "subscribe with wrong schema must fail");

    // Correct key + schema succeeds and receives the published message.
    let sub = client
      .subscribe(channel_id, Some("chan-key"), METRIC_SCHEMA)
      .expect("subscribe ok");
    let got: Metric = sub.recv(Duration::from_secs(2)).expect("recv ok").expect("a message");
    assert_eq!(
      got,
      Metric {
        name: "cpu".into(),
        value: 0.75
      }
    );

    // RPC call returns the async result.
    let resp: AddResp = client
      .call_blocking(
        "add",
        Some("fn-key"),
        &AddReq { a: 7, b: 35 },
        ADD_REQ_SCHEMA,
        ADD_RESP_SCHEMA,
        Duration::from_secs(5),
      )
      .expect("rpc ok");
    assert_eq!(resp.sum, 42);

    // Calling with the wrong function key is denied.
    let denied_call = client.call_blocking::<AddReq, AddResp>(
      "add",
      Some("nope"),
      &AddReq { a: 1, b: 1 },
      ADD_REQ_SCHEMA,
      ADD_RESP_SCHEMA,
      Duration::from_secs(2),
    );
    assert!(denied_call.is_err(), "call with wrong key must fail");
  } // connections dropped here -> reply segments unlinked, threads joined

  // Regression: once the owner of "metrics"/"add" has left (unregistered above),
  // a fresh app must be able to re-publish the same channel and re-expose the
  // same function instead of getting "already exists".
  std::thread::sleep(Duration::from_millis(200)); // let the broker process Unregister
  {
    let reborn = Connection::connect("svc-a-restarted").expect("reconnect");
    reborn
      .publish_channel("metrics", METRIC_SCHEMA, Some("chan-key"))
      .expect("re-publish after the previous owner left");
    reborn
      .expose_function::<AddReq, AddResp, _>("add", ADD_REQ_SCHEMA, ADD_RESP_SCHEMA, None, |req| AddResp {
        sum: req.a + req.b,
      })
      .expect("re-expose after the previous owner left");
  }
  std::thread::sleep(Duration::from_millis(100));

  // --- Crash reclaim: a function whose owner *died without unregistering* must
  // be reclaimable on restart (the SIGKILL/crash case, distinct from the
  // graceful path above). Spawn a victim child that exposes `crash.add`, kill it
  // with SIGKILL so no Unregister is sent, then re-expose `crash.add` from this
  // (live, different-pid) process. The broker must notice the dead owner via its
  // pid and reclaim the name.
  {
    let mut victim = Command::new(std::env::current_exe().expect("current exe"))
      .args(["full_flow_pubsub_and_rpc", "--exact", "--nocapture"])
      .env(VICTIM_ENV, "1")
      .stdout(Stdio::piped())
      .spawn()
      .expect("spawn victim");

    // Wait until the victim has exposed `crash.add`.
    let stdout = victim.stdout.take().expect("victim stdout");
    let mut reader = BufReader::new(stdout);
    let mut ready = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
      let mut line = String::new();
      if reader.read_line(&mut line).unwrap_or(0) == 0 {
        break; // victim exited unexpectedly
      }
      if line.contains(VICTIM_READY) {
        ready = true;
        break;
      }
    }
    assert!(ready, "victim never became ready");

    // Before reclaim: the name is owned by a *live* process, so re-exposing it
    // is refused — this proves we only reclaim once the owner is actually gone.
    {
      let contender = Connection::connect("crash-contender").expect("contender connect");
      let refused =
        contender.expose_function::<AddReq, AddResp, _>("crash.add", ADD_REQ_SCHEMA, ADD_RESP_SCHEMA, None, |req| {
          AddResp { sum: req.a + req.b }
        });
      assert!(refused.is_err(), "must not steal a name from a live owner");
    }

    // Now crash the victim: SIGKILL skips destructors, so the broker never sees
    // an Unregister and `crash.add` stays registered under the dead pid.
    let vpid = victim.id() as i32;
    unsafe { libc::kill(vpid, libc::SIGKILL) };
    victim.wait().expect("reap victim");
    std::thread::sleep(Duration::from_millis(100));

    // The restart: a fresh process re-exposes the same function. The broker
    // detects the previous owner's pid is dead and reclaims the name.
    let reborn = Connection::connect("crash-reborn").expect("reborn connect");
    reborn
      .expose_function::<AddReq, AddResp, _>("crash.add", ADD_REQ_SCHEMA, ADD_RESP_SCHEMA, None, |req| AddResp {
        sum: req.a + req.b,
      })
      .expect("re-expose after the previous owner crashed (pid reclaim)");
  }
  std::thread::sleep(Duration::from_millis(100));

  // Shut the broker down and confirm it cleaned up its segments. Our own
  // segments unlink synchronously as the broker and connections drop; poll a few
  // times so any concurrent foreign churn settles before we judge, and ignore
  // anything that was already there when we started.
  drop(_broker);
  let mut leftovers: Vec<String> = Vec::new();
  for _ in 0..20 {
    std::thread::sleep(Duration::from_millis(50));
    leftovers = scan_ring_segments().difference(&preexisting).cloned().collect();
    if leftovers.is_empty() {
      break;
    }
  }
  assert!(
    leftovers.is_empty(),
    "shared memory leaked after shutdown: {leftovers:?}"
  );
}
