//! Emit the control-plane Avro schemas and their CRC-64-AVRO fingerprints.
//! Used to regenerate `SPEC/schemas/control.avsc` and `SPEC/schemas/FINGERPRINTS.md`.

use impulse_ring_core::proto::{self, catalog};

fn main() {
  let mode = std::env::args().nth(1).unwrap_or_default();
  let cat = catalog();
  if mode == "fps" {
    println!("# Control schema fingerprints (CRC-64-AVRO Rabin)\n");
    println!("| Message | Fingerprint (u64, hex) |");
    println!("|---------|------------------------|");
    for &k in proto::all_kinds() {
      println!("| `{k:?}` | `{:#018x}` |", cat.fp(k));
    }
  } else {
    // Emit a JSON array of all record schemas.
    let parts: Vec<String> = proto::all_kinds()
      .iter()
      .map(|&k| proto::schema_json(k).to_string())
      .collect();
    println!("[\n{}\n]", parts.join(",\n"));
  }
}
