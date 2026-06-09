//! A long-running Rust peer for cross-language tests. It publishes a channel and
//! exposes a function, then stays alive until killed. Other-language connectors
//! subscribe to the channel and call the function to prove data-plane Avro
//! interop. Run via the C connector's `run_tests.sh`.

use impulse_ring_connector::Connection;
use serde::{Deserialize, Serialize};
use std::time::Duration;

const RMETRIC: &str = r#"{"type":"record","name":"RMetric","namespace":"ring.xlang",
  "fields":[{"name":"name","type":"string"},{"name":"value","type":"double"}]}"#;
const MUL_REQ: &str = r#"{"type":"record","name":"MulReq","namespace":"ring.xlang",
  "fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]}"#;
const MUL_RESP: &str = r#"{"type":"record","name":"MulResp","namespace":"ring.xlang",
  "fields":[{"name":"product","type":"long"}]}"#;

#[derive(Serialize, Deserialize)]
struct RMetric {
  name: String,
  value: f64,
}
#[derive(Serialize, Deserialize)]
struct MulReq {
  a: i64,
  b: i64,
}
#[derive(Serialize, Deserialize)]
struct MulResp {
  product: i64,
}

fn main() -> std::io::Result<()> {
  let conn = Connection::connect("rust-peer")?;
  let pubr = conn.publish_channel("rmetrics", RMETRIC, None)?;
  conn.expose_function::<MulReq, MulResp, _>("rmul", MUL_REQ, MUL_RESP, None, |r| MulResp { product: r.a * r.b })?;
  eprintln!("rust-peer: ready (publishing rmetrics, exposing rmul)");

  // Keep publishing so a late subscriber reliably receives a message.
  loop {
    let _ = pubr.publish(&RMetric {
      name: "temp".into(),
      value: 21.5,
    });
    std::thread::sleep(Duration::from_millis(150));
  }
}
