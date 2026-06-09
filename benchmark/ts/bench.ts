// Ring relay benchmark — TypeScript node (run with Bun).
// Usage: bun run bench.ts <index> <num_services> <laps>
import { Connection, Decoder, Encoder } from "../../connectors/ts/src/index";

const KEY = "ring-bench-key";
const SCHEMA =
  '{"type":"record","name":"BenchToken","namespace":"ring.bench","fields":[' +
  '{"name":"lap","type":"long"},{"name":"start_nanos","type":"long"},' +
  '{"name":"elapsed_ns","type":"long"},{"name":"stop","type":"boolean"}]}';

function encode(lap: number, start: number, elapsed: number, stop: boolean): Uint8Array {
  return new Encoder().putLong(lap).putLong(start).putLong(elapsed).putBool(stop).bytes_();
}

function present(conn: Connection, n: number): number {
  const names = new Set(conn.listChannels().map((c) => c.name));
  let count = 0;
  for (let i = 0; i < n; i++) if (names.has(`bench-${i}`)) count++;
  return count;
}

const index = Number(process.argv[2]);
const n = Number(process.argv[3]);
const laps = Number(process.argv[4]);
const self = `bench-${index}`;

const conn = new Connection(self);
const pub = conn.publishChannel(self, SCHEMA, KEY);

const prev = `bench-${(index + n - 1) % n}`;
let cid: bigint | null = null;
while (cid === null) {
  for (const c of conn.listChannels()) if (c.name === prev) cid = c.channelId;
  if (cid === null) Bun.sleepSync(20);
}
const sub = conn.subscribe(cid, KEY);

if (index === 0) {
  while (present(conn, n) < n) Bun.sleepSync(20);
  Bun.sleepSync(300);

  const t0 = Bun.nanoseconds();
  pub.publish(encode(0, t0, 0, false));
  for (;;) {
    const body = sub.recv(30000);
    if (body === null) break;
    const lap = Number(new Decoder(body).long()) + 1;
    if (lap >= laps) {
      const elapsed = Bun.nanoseconds() - t0;
      pub.publish(encode(lap, t0, elapsed, true));
      const secs = elapsed / 1e9;
      console.log(
        `ring-bench(ts): ${laps} laps across ${n} services in ${secs.toFixed(3)}s | ` +
          `${(laps / secs).toFixed(0)} laps/s | ${Math.round(elapsed / laps)} ns/lap | ` +
          `${Math.round(elapsed / (laps * n))} ns/hop`,
      );
      Bun.sleepSync(300);
      break;
    }
    pub.publish(encode(lap, t0, 0, false));
  }
} else {
  for (;;) {
    const body = sub.recv(30000);
    if (body === null) break;
    const d = new Decoder(body);
    d.long();
    d.long();
    d.long();
    const stop = d.bool();
    pub.publish(body); // forward the same bytes
    if (stop) break;
  }
}

sub.close();
pub.close();
conn.close();
