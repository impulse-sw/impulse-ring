//! Ring relay benchmark — one node of a cross-language ring.
//!
//! N services form a ring `0 -> 1 -> ... -> N-1 -> 0`. Each node subscribes to
//! its predecessor's channel and publishes its own (all channels gated by a
//! hard-coded key). A token struct circulates; one full circuit is a "lap".
//!
//! Node 0 is the coordinator: it waits until every node's channel exists (all
//! services started), starts the clock, injects the token, counts laps, and on
//! the final lap sets `stop` so every node propagates it and exits. It then
//! prints how long the requested number of laps took.
//!
//! Usage: `ring-bench <index> <num_services> <laps>`

use impulse_ring_connector::Connection;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const KEY: &str = "ring-bench-key";
const SCHEMA: &str = r#"{"type":"record","name":"BenchToken","namespace":"ring.bench","fields":[{"name":"lap","type":"long"},{"name":"start_nanos","type":"long"},{"name":"elapsed_ns","type":"long"},{"name":"stop","type":"boolean"}]}"#;

#[derive(Serialize, Deserialize, Clone)]
struct BenchToken {
  lap: i64,
  start_nanos: i64,
  elapsed_ns: i64,
  stop: bool,
}

fn main() {
  let args: Vec<String> = std::env::args().collect();
  if args.len() < 4 {
    eprintln!("usage: ring-bench <index> <num_services> <laps>");
    std::process::exit(2);
  }
  let index: usize = args[1].parse().unwrap();
  let n: usize = args[2].parse().unwrap();
  let laps: i64 = args[3].parse().unwrap();

  let conn = Connection::connect(&format!("bench-{index}")).expect("connect");
  let publisher = conn
    .publish_channel(&format!("bench-{index}"), SCHEMA, Some(KEY))
    .expect("publish_channel");

  // Subscribe to the predecessor (retry until its channel is published).
  let prev = format!("bench-{}", (index + n - 1) % n);
  let sub = loop {
    let chans = conn.list_channels().expect("list");
    if let Some(ci) = chans.iter().find(|c| c.name == prev) {
      break conn.subscribe(ci.channel_id, Some(KEY), SCHEMA).expect("subscribe");
    }
    std::thread::sleep(Duration::from_millis(20));
  };

  let recv_timeout = Duration::from_secs(30);

  if index == 0 {
    // Wait until every node's channel exists, then a short grace for subs.
    loop {
      let chans = conn.list_channels().expect("list");
      let up = (0..n)
        .filter(|i| chans.iter().any(|c| c.name == format!("bench-{i}")))
        .count();
      if up >= n {
        break;
      }
      std::thread::sleep(Duration::from_millis(20));
    }
    std::thread::sleep(Duration::from_millis(300));

    let t0 = Instant::now();
    let start_nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64;
    publisher
      .publish(&BenchToken {
        lap: 0,
        start_nanos,
        elapsed_ns: 0,
        stop: false,
      })
      .expect("inject");

    loop {
      let tok: BenchToken = sub.recv(recv_timeout).expect("recv").expect("token");
      let lap = tok.lap + 1;
      if lap >= laps {
        let elapsed = t0.elapsed();
        publisher
          .publish(&BenchToken {
            lap,
            start_nanos,
            elapsed_ns: elapsed.as_nanos() as i64,
            stop: true,
          })
          .expect("stop");
        report(laps, n, elapsed);
        std::thread::sleep(Duration::from_millis(300)); // let stop propagate
        break;
      }
      publisher
        .publish(&BenchToken {
          lap,
          start_nanos,
          elapsed_ns: 0,
          stop: false,
        })
        .expect("relay");
    }
  } else {
    // Relay: forward each token; exit once the stop token passes through.
    while let Some(tok) = sub.recv::<BenchToken>(recv_timeout).expect("recv") {
      let stop = tok.stop;
      publisher.publish(&tok).expect("relay");
      if stop {
        break;
      }
    }
  }
}

fn report(laps: i64, n: usize, elapsed: Duration) {
  let secs = elapsed.as_secs_f64();
  let hops = laps * n as i64;
  println!(
    "ring-bench: {laps} laps across {n} services in {secs:.3}s | {:.0} laps/s | {} ns/lap | {} ns/hop",
    laps as f64 / secs,
    elapsed.as_nanos() as i64 / laps,
    elapsed.as_nanos() as i64 / hops,
  );
}
