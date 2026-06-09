// Demo of the TS connector (run with Bun). Start the broker first:
//   cargo run -p impulsed
//   bun run connectors/ts/examples/demo.ts
import { Connection, Encoder } from "../src/index";

const METRIC =
  '{"type":"record","name":"Metric","namespace":"ring.examples",' +
  '"fields":[{"name":"name","type":"string"},{"name":"value","type":"double"}]}';

const conn = new Connection("ts-demo");

// Publish a channel and one message.
const pub = conn.publishChannel("ts-metrics", METRIC);
pub.publish(new Encoder().putString("cpu").putDouble(0.5).bytes_());

// List what's on the bus.
for (const c of conn.listChannels()) {
  console.log(`channel: ${c.name} (requires_key=${c.requiresKey})`);
}

pub.close();
conn.close();
