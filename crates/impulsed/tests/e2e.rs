//! End-to-end test: spawn the real `impulsed` broker as a child process and
//! drive a full register -> publish/subscribe -> RPC flow through shared memory,
//! including the negative access-control and schema-mismatch paths.

use impulse_ring_connector::Connection;
use serde::{Deserialize, Serialize};
use std::process::{Child, Command};
use std::time::Duration;

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

#[test]
fn full_flow_pubsub_and_rpc() {
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

  // Shut the broker down and confirm it cleaned up its segments.
  drop(_broker);
  std::thread::sleep(Duration::from_millis(200));
  let leftovers: Vec<_> = std::fs::read_dir("/dev/shm")
    .unwrap()
    .filter_map(|e| e.ok())
    .map(|e| e.file_name().to_string_lossy().into_owned())
    .filter(|n| n.starts_with("impulse-ring."))
    .collect();
  assert!(
    leftovers.is_empty(),
    "shared memory leaked after shutdown: {leftovers:?}"
  );
}
