// Cross-language tests (run with `bun test`): a TS client subscribes to a channel
// published by the Rust `peer` and calls a function it exposes (proving TS<->Rust
// Avro interop over the FFI-bound C transport), then the broker is restarted and
// the client must recover transparently.
//
// Requires the broker and peer binaries to be built:
//   cargo build -p impulsed --example peer -p impulse-ring-connector
//
// Both tests share one broker lifecycle and run in order (the restart test is
// last, since it tears the broker down and brings a fresh one up).
import { afterAll, beforeAll, expect, test } from "bun:test";
import { existsSync } from "node:fs";

import { Connection, Decoder, Encoder } from "../src/index";

const ROOT = `${import.meta.dir}/../../..`;
let broker: Bun.Subprocess;
let peer: Bun.Subprocess;

async function sleep(ms: number) {
  await new Promise((r) => setTimeout(r, ms));
}

async function spawnBroker(): Promise<Bun.Subprocess> {
  const b = Bun.spawn([`${ROOT}/target/debug/impulsed`], { stdout: "ignore", stderr: "ignore" });
  for (let i = 0; i < 200 && !existsSync("/dev/shm/impulse-ring.ctl.v1"); i++) await sleep(20);
  await sleep(100);
  return b;
}

beforeAll(async () => {
  broker = await spawnBroker();
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

function callRmul(conn: Connection): number | null {
  try {
    const req = new Encoder().putLong(6).putLong(7).bytes_();
    return Number(new Decoder(conn.call("rmul", req, "", 3000)).long());
  } catch {
    return null;
  }
}

test(
  "TS client recovers after a broker restart",
  async () => {
    const conn = new Connection("ts-restart");
    expect(callRmul(conn)).toBe(42);
    const epochBefore = conn.brokerEpoch();

    // Restart the broker (new shared-memory generation + epoch). Wait for the old
    // one to exit so the replacement's singleton guard does not see it alive.
    broker.kill("SIGKILL");
    await broker.exited;
    broker = await spawnBroker();

    // The TS client (via the C connection) and the peer both reconnect; retry the
    // call until it succeeds against the fresh broker.
    let ok = false;
    for (let i = 0; i < 100; i++) {
      if (callRmul(conn) === 42) {
        ok = true;
        break;
      }
      await sleep(100);
    }
    expect(ok).toBe(true);
    expect(conn.brokerEpoch()).not.toBe(epochBefore);

    conn.close();
  },
  30000,
);
