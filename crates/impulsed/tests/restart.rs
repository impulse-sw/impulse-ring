//! End-to-end test for **broker-restart recovery**: a service and a client
//! connect, the `impulsed` broker is killed and a fresh one started (a new
//! shared-memory generation with a new epoch), and we assert that the *existing*
//! connections transparently reconnect — the service's exposed function and
//! published channel are replayed, and the client's RPC + subscribe keep working
//! without rebuilding any handle.

use impulse_ring_connector::Connection;
use serde::{Deserialize, Serialize};
use std::process::{Child, Command};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Both tests drive the single, well-known broker on the global control segment,
/// so they must not run concurrently. Serialize them with a process-global lock
/// (recovering from poisoning so one test's panic doesn't wedge the other).
static BROKER_LOCK: Mutex<()> = Mutex::new(());

fn broker_guard() -> std::sync::MutexGuard<'static, ()> {
  BROKER_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

const ADD_REQ_SCHEMA: &str = r#"{
  "type":"record","name":"AddReq","namespace":"ring.test",
  "fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]
}"#;

const ADD_RESP_SCHEMA: &str = r#"{
  "type":"record","name":"AddResp","namespace":"ring.test",
  "fields":[{"name":"sum","type":"long"}]
}"#;

const METRIC_SCHEMA: &str = r#"{
  "type":"record","name":"Metric","namespace":"ring.test",
  "fields":[{"name":"name","type":"string"},{"name":"value","type":"double"}]
}"#;

#[derive(Serialize, Deserialize)]
struct AddReq {
  a: i64,
  b: i64,
}

#[derive(Serialize, Deserialize)]
struct AddResp {
  sum: i64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct Metric {
  name: String,
  value: f64,
}

/// SIGTERM + reap on drop (graceful shutdown of the broker under test).
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

fn start_broker() -> BrokerGuard {
  let child = Command::new(env!("CARGO_BIN_EXE_impulsed"))
    .spawn()
    .expect("spawn impulsed");
  let guard = BrokerGuard(child);
  for _ in 0..100 {
    if std::path::Path::new("/dev/shm/impulse-ring.ctl.v1").exists() {
      std::thread::sleep(Duration::from_millis(50));
      return guard;
    }
    std::thread::sleep(Duration::from_millis(20));
  }
  panic!("broker did not come up");
}

/// SIGKILL the broker (no graceful unlink) and **reap it** so its pid is free
/// before the replacement starts — otherwise the new broker's singleton guard
/// would see the zombie as still alive. Models an abrupt `impulsed` crash/restart.
fn kill_broker(mut guard: BrokerGuard) {
  let pid = guard.0.id() as i32;
  unsafe { libc::kill(pid, libc::SIGKILL) };
  let _ = guard.0.wait();
  // Defuse the guard's Drop (the child is already reaped).
  std::mem::forget(guard);
  let _ = pid;
}

fn call_add(client: &Connection, a: i64, b: i64) -> std::io::Result<AddResp> {
  client.call_blocking::<AddReq, AddResp>(
    "add",
    Some("fn-key"),
    &AddReq { a, b },
    ADD_REQ_SCHEMA,
    ADD_RESP_SCHEMA,
    Duration::from_secs(3),
  )
}

#[test]
fn connections_recover_after_broker_restart() {
  let _serial = broker_guard();
  let broker = start_broker();

  let svc = Connection::connect("svc-restart").expect("connect svc");
  svc
    .expose_function::<AddReq, AddResp, _>("add", ADD_REQ_SCHEMA, ADD_RESP_SCHEMA, Some("fn-key"), |req| AddResp {
      sum: req.a + req.b,
    })
    .expect("expose add");
  let publisher = svc
    .publish_channel("metrics", METRIC_SCHEMA, Some("chan-key"))
    .expect("publish channel");

  let client = Connection::connect("client-restart").expect("connect client");
  let epoch_before = client.broker_epoch();

  // Sanity: everything works against the first broker.
  assert_eq!(call_add(&client, 7, 35).expect("rpc before restart").sum, 42);

  // --- Restart the broker (new shared-memory generation + epoch). ---
  kill_broker(broker);
  let _broker2 = start_broker();

  // The client's watcher (or this very call) must reconnect; retry the RPC until
  // it succeeds against the fresh broker, within a generous window.
  let deadline = Instant::now() + Duration::from_secs(10);
  let resp = loop {
    match call_add(&client, 20, 22) {
      Ok(r) => break r,
      Err(_) if Instant::now() < deadline => std::thread::sleep(Duration::from_millis(100)),
      Err(e) => panic!("client never recovered after restart: {e}"),
    }
  };
  assert_eq!(resp.sum, 42, "RPC must work again after the broker restart");

  // The connection now tracks the *new* broker generation.
  assert_ne!(client.broker_epoch(), epoch_before, "epoch must advance after restart");

  // The service's published channel was replayed: the same publisher handle still
  // works, and a fresh subscriber on the client receives the message.
  publisher
    .publish(&Metric {
      name: "cpu".into(),
      value: 0.5,
    })
    .expect("publish after restart");

  // Resolve the (new) channel id by name and subscribe.
  let deadline = Instant::now() + Duration::from_secs(5);
  let sub = loop {
    let chans = client.list_channels().expect("list channels");
    if let Some(c) = chans.iter().find(|c| c.name == "metrics") {
      break client
        .subscribe(c.channel_id, Some("chan-key"), METRIC_SCHEMA)
        .expect("subscribe after restart");
    }
    if Instant::now() >= deadline {
      panic!("metrics channel was not replayed after restart");
    }
    std::thread::sleep(Duration::from_millis(50));
  };
  // Publish again now that we have a subscriber, then receive it.
  publisher
    .publish(&Metric {
      name: "cpu".into(),
      value: 0.9,
    })
    .expect("publish for subscriber");
  let got: Metric = loop {
    if let Some(m) = sub.recv::<Metric>(Duration::from_secs(2)).expect("recv") {
      break m;
    }
    if Instant::now() >= deadline {
      panic!("no message received after restart");
    }
  };
  assert_eq!(got.name, "cpu");
}

#[test]
fn auto_reconnect_can_be_disabled() {
  let _serial = broker_guard();
  let broker = start_broker();
  let client = Connection::connect("client-noreconnect").expect("connect");
  client.set_auto_reconnect(false);
  assert!(!client.auto_reconnect());

  // Expose a function from a service so a call has a target before the restart.
  let svc = Connection::connect("svc-noreconnect").expect("connect svc");
  svc
    .expose_function::<AddReq, AddResp, _>("add", ADD_REQ_SCHEMA, ADD_RESP_SCHEMA, Some("fn-key"), |req| AddResp {
      sum: req.a + req.b,
    })
    .expect("expose");
  assert_eq!(call_add(&client, 1, 2).expect("before").sum, 3);

  kill_broker(broker);
  let _broker2 = start_broker();

  // With auto-reconnect off, the stale connection does not recover on its own.
  std::thread::sleep(Duration::from_secs(1));
  assert!(
    call_add(&client, 1, 2).is_err(),
    "a non-reconnecting connection must stay broken after a restart"
  );
}
