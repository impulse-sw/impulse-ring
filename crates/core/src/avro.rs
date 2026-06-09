//! Apache Avro integration: schema fingerprinting and datum (de)serialization.
//!
//! Every payload on the bus — control messages, channel data, RPC arguments and
//! results — is Avro binary. Interface compatibility is enforced by comparing
//! the **CRC-64-AVRO Rabin fingerprint** of the writer's and reader's schemas
//! (the canonical Avro schema fingerprint). A mismatch is a hard error, so two
//! services can never silently exchange incompatible structures.

use apache_avro::rabin::Rabin;
use apache_avro::{Schema, from_avro_datum, from_value, to_avro_datum, to_value};
use serde::Serialize;
use serde::de::DeserializeOwned;
use std::collections::HashMap;
use std::io;

/// A schema's CRC-64-AVRO Rabin fingerprint as a `u64` (8-byte LE encoding).
pub type Fingerprint = u64;

/// Compute the CRC-64-AVRO Rabin fingerprint of `schema`.
pub fn fingerprint(schema: &Schema) -> Fingerprint {
  let fp = schema.fingerprint::<Rabin>();
  // The Rabin digest is the 8-byte little-endian Rabin hash.
  let mut b = [0u8; 8];
  let n = fp.bytes.len().min(8);
  b[..n].copy_from_slice(&fp.bytes[..n]);
  u64::from_le_bytes(b)
}

/// Parse a JSON Avro schema string, returning the schema and its fingerprint.
pub fn parse(json: &str) -> io::Result<(Schema, Fingerprint)> {
  let schema = Schema::parse_str(json).map_err(|e| io::Error::other(e.to_string()))?;
  let fp = fingerprint(&schema);
  Ok((schema, fp))
}

/// Encode a serializable value to an Avro datum (binary, no container header).
pub fn encode<T: Serialize>(schema: &Schema, value: &T) -> io::Result<Vec<u8>> {
  let v = to_value(value).map_err(|e| io::Error::other(e.to_string()))?;
  let v = v.resolve(schema).map_err(|e| io::Error::other(e.to_string()))?;
  to_avro_datum(schema, v).map_err(|e| io::Error::other(e.to_string()))
}

/// Decode an Avro datum into `T` using `schema` as the writer schema.
pub fn decode<T: DeserializeOwned>(schema: &Schema, bytes: &[u8]) -> io::Result<T> {
  let mut cursor = io::Cursor::new(bytes);
  let value = from_avro_datum(schema, &mut cursor, None).map_err(|e| io::Error::other(e.to_string()))?;
  from_value::<T>(&value).map_err(|e| io::Error::other(e.to_string()))
}

/// In-broker registry mapping fingerprints to parsed schemas.
///
/// Used to validate that a subscriber/caller's expected schema fingerprint
/// matches the publisher/function's registered fingerprint.
#[derive(Default)]
pub struct SchemaRegistry {
  by_fp: HashMap<Fingerprint, Schema>,
}

impl SchemaRegistry {
  pub fn new() -> Self {
    Self::default()
  }

  /// Register a schema (idempotent), returning its fingerprint.
  pub fn register(&mut self, schema: Schema) -> Fingerprint {
    let fp = fingerprint(&schema);
    self.by_fp.entry(fp).or_insert(schema);
    fp
  }

  /// Register from a JSON string.
  pub fn register_json(&mut self, json: &str) -> io::Result<Fingerprint> {
    let (schema, _) = parse(json)?;
    Ok(self.register(schema))
  }

  pub fn get(&self, fp: Fingerprint) -> Option<&Schema> {
    self.by_fp.get(&fp)
  }

  /// True if `expected` matches `actual`, i.e. the interfaces are compatible.
  pub fn compatible(expected: Fingerprint, actual: Fingerprint) -> bool {
    expected == actual
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use serde::{Deserialize, Serialize};

  const ADD_REQ: &str = r#"{
        "type":"record","name":"AddReq",
        "fields":[{"name":"a","type":"long"},{"name":"b","type":"long"}]
    }"#;

  #[derive(Serialize, Deserialize, PartialEq, Debug)]
  struct AddReq {
    a: i64,
    b: i64,
  }

  #[test]
  fn fingerprint_is_stable_and_nonzero() {
    let (s1, fp1) = parse(ADD_REQ).unwrap();
    let (_s2, fp2) = parse(ADD_REQ).unwrap();
    assert_eq!(fp1, fp2, "same schema -> same fingerprint");
    assert_ne!(fp1, 0);
    // Whitespace differences must not change the canonical fingerprint.
    let (_s3, fp3) = parse(&ADD_REQ.replace('\n', " ")).unwrap();
    assert_eq!(fp1, fp3);
    let _ = s1;
  }

  #[test]
  fn encode_decode_roundtrip() {
    let (schema, _) = parse(ADD_REQ).unwrap();
    let msg = AddReq { a: 7, b: 35 };
    let bytes = encode(&schema, &msg).unwrap();
    let back: AddReq = decode(&schema, &bytes).unwrap();
    assert_eq!(msg, back);
  }

  #[test]
  fn different_schema_different_fingerprint() {
    let (_s, fp_a) = parse(ADD_REQ).unwrap();
    let (_s2, fp_b) = parse(r#"{"type":"record","name":"AddReq","fields":[{"name":"a","type":"string"}]}"#).unwrap();
    assert!(!SchemaRegistry::compatible(fp_a, fp_b));
  }
}
