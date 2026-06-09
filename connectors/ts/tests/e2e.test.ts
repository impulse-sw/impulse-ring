// Cross-language test (run with `bun test`): a TS client subscribes to a channel
// published by the Rust `peer` and calls a function it exposes, proving TS<->Rust
// Avro interop over the FFI-bound C transport.
//
// Requires the broker and peer binaries to be built:
//   cargo build -p impulsed --example peer -p impulse-ring-connector
import { afterAll, beforeAll, expect, test } from "bun:test";
import { existsSync } from "node:fs";

import { Connection, Decoder, Encoder } from "../src/index";

const ROOT = `${import.meta.dir}/../../..`;
let broker: Bun.Subprocess;
let peer: Bun.Subprocess;

async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

beforeAll(async () => {
  broker = Bun.spawn([`${ROOT}/target/debug/impulsed`], { stdout: "ignore", stderr: "ignore" });
  for (let i = 0; i < 100 && !existsSync("/dev/shm/impulse-ring.ctl.v1"); i++) await sleep(20);
  await sleep(100);
  peer = Bun.spawn([`${ROOT}/target/debug/examples/peer`], { stdout: "ignore", stderr: "ignore" });
  await sleep(600);
});

afterAll(() => {
  peer?.kill();
  broker?.kill("SIGTERM");
});

test("TS subscribes to and calls the Rust peer", () => {
  const conn = new Connection("ts-xlang");

  // Find the Rust-published channel.
  let channelId: bigint | null = null;
  for (let i = 0; i < 50 && channelId === null; i++) {
    for (const c of conn.listChannels()) {
      if (c.name === "rmetrics") channelId = c.channelId;
    }
  }
  expect(channelId).not.toBeNull();

  // Subscribe and receive a Rust-published message.
  const sub = conn.subscribe(channelId!);
  const body = sub.recv(3000);
  expect(body).not.toBeNull();
  const d = new Decoder(body!);
  expect(d.string()).toBe("temp");
  expect(Math.abs(d.double() - 21.5)).toBeLessThan(1e-9);
  sub.close();

  // Call the Rust-exposed function rmul(6, 7) == 42.
  const req = new Encoder().putLong(6).putLong(7).bytes_();
  const resp = conn.call("rmul", req);
  expect(Number(new Decoder(resp).long())).toBe(42);

  conn.close();
});
