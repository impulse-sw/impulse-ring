//! End-to-end demo of the Rust connector. Start the broker first:
//!
//! ```text
//! cargo run -p impulsed              # terminal 1
//! cargo run -p impulse-ring-connector --example demo   # terminal 2
//! ```

use impulse_ring_connector::Connection;
use serde::{Deserialize, Serialize};
use std::time::Duration;

const METRIC: &str = r#"{"type":"record","name":"Metric","namespace":"ring.examples",
  "fields":[{"name":"name","type":"string"},{"name":"value","type":"double"}]}"#;
const ADD_REQ: &str = r#"{"type":"record","name":"AddReq","namespace":"ring.examples",
  "fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]}"#;
const ADD_RESP: &str = r#"{"type":"record","name":"AddResp","namespace":"ring.examples",
  "fields":[{"name":"sum","type":"long"}]}"#;

#[derive(Serialize, Deserialize, Debug)]
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

fn main() -> std::io::Result<()> {
  let svc = Connection::connect("demo-service")?;
  let pubr = svc.publish_channel("metrics", METRIC, Some("k1"))?;
  pubr.publish(&Metric {
    name: "cpu".into(),
    value: 0.5,
  })?;
  svc.expose_function::<AddReq, AddResp, _>("add", ADD_REQ, ADD_RESP, Some("k2"), |r| AddResp { sum: r.a + r.b })?;

  let cli = Connection::connect("demo-client")?;
  for c in cli.list_channels()? {
    println!("channel: {} (requires_key={})", c.name, c.requires_key);
  }
  let id = cli
    .list_channels()?
    .into_iter()
    .find(|c| c.name == "metrics")
    .unwrap()
    .channel_id;
  let sub = cli.subscribe(id, Some("k1"), METRIC)?;
  if let Some(m) = sub.recv::<Metric>(Duration::from_secs(2))? {
    println!("received metric: {m:?}");
  }
  let r: AddResp = cli.call_blocking(
    "add",
    Some("k2"),
    &AddReq { a: 20, b: 22 },
    ADD_REQ,
    ADD_RESP,
    Duration::from_secs(5),
  )?;
  println!("add(20, 22) = {}", r.sum);
  Ok(())
}
