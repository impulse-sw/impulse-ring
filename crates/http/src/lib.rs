//! `impulse-ring-http` — the HTTP-over-Ring wire protocol.
//!
//! Ring is a shared-memory RPC bus (see [`impulse-ring-connector`]). This crate
//! layers a tiny request/response convention on top of it so that an HTTP
//! server can be reached over shared memory instead of TCP/Unix sockets:
//!
//! - A **server** registers on the bus and exposes a single function whose name
//!   is derived from the application name via [`http_fn_name`]. The function's
//!   argument is a [`RingHttpRequest`] and its result is a [`RingHttpResponse`].
//! - A **client** ([`impulse-client-ring`]) looks that function up by the same
//!   application name and calls it, shipping an HTTP request and receiving an
//!   HTTP response — all Avro-encoded, all in shared memory.
//!
//! Both ends share the exact Avro schemas in [`REQUEST_SCHEMA`] /
//! [`RESPONSE_SCHEMA`], so the broker's fingerprint check guarantees the two
//! sides agree on the wire shape.
//!
//! [`impulse-ring-connector`]: https://docs.rs/impulse-ring-connector
//! [`impulse-client-ring`]: https://docs.rs/impulse-client-ring

#![deny(warnings, clippy::todo, clippy::unimplemented)]
#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};

/// Wire protocol revision. Bumped on any incompatible change to the records or
/// the function-naming convention.
pub const PROTOCOL_VERSION: u32 = 1;

/// Prefix applied to the application name to derive the bus function name.
///
/// Namespacing the function under this prefix keeps the HTTP entry point from
/// colliding with ordinary user RPC functions exposed on the same bus.
pub const HTTP_FN_PREFIX: &str = "impulse-ring-http/v1/";

/// Derive the bus function name an HTTP server exposes for `app_name`.
///
/// The client addresses a server purely by its application name; both ends run
/// this function to agree on the registered name.
pub fn http_fn_name(app_name: &str) -> String {
  format!("{HTTP_FN_PREFIX}{app_name}")
}

/// A single HTTP header as an Avro record (`bytes`-free so it stays text).
#[derive(Serialize, Deserialize, Clone, Debug, Default, PartialEq, Eq)]
pub struct RingHeader {
  /// Header name (case-insensitive on the HTTP side).
  pub name: String,
  /// Header value.
  pub value: String,
}

impl RingHeader {
  /// Build a header from any pair of string-likes.
  pub fn new(name: impl Into<String>, value: impl Into<String>) -> Self {
    RingHeader {
      name: name.into(),
      value: value.into(),
    }
  }
}

/// An HTTP request carried over the Ring bus.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RingHttpRequest {
  /// Request method, e.g. `GET`, `POST` (uppercase, as on the wire).
  pub method: String,
  /// Request target: path plus optional `?query`, e.g. `/api/items?page=2`.
  pub uri: String,
  /// Request headers in arrival order.
  pub headers: Vec<RingHeader>,
  /// Raw request body (may be empty).
  #[serde(with = "serde_bytes")]
  pub body: Vec<u8>,
}

/// An HTTP response carried over the Ring bus.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RingHttpResponse {
  /// HTTP status code, e.g. `200`, `404`.
  pub status: i32,
  /// Response headers in emission order.
  pub headers: Vec<RingHeader>,
  /// Raw response body (may be empty).
  #[serde(with = "serde_bytes")]
  pub body: Vec<u8>,
}

/// Avro schema (JSON) for [`RingHttpRequest`]. Must match the struct field-for-field.
pub const REQUEST_SCHEMA: &str = r#"{
  "type": "record",
  "name": "RingHttpRequest",
  "fields": [
    {"name": "method", "type": "string"},
    {"name": "uri", "type": "string"},
    {"name": "headers", "type": {"type": "array", "items": {
      "type": "record",
      "name": "RingHeader",
      "fields": [
        {"name": "name", "type": "string"},
        {"name": "value", "type": "string"}
      ]
    }}},
    {"name": "body", "type": "bytes"}
  ]
}"#;

/// Avro schema (JSON) for [`RingHttpResponse`]. Must match the struct field-for-field.
pub const RESPONSE_SCHEMA: &str = r#"{
  "type": "record",
  "name": "RingHttpResponse",
  "fields": [
    {"name": "status", "type": "int"},
    {"name": "headers", "type": {"type": "array", "items": {
      "type": "record",
      "name": "RingHeader",
      "fields": [
        {"name": "name", "type": "string"},
        {"name": "value", "type": "string"}
      ]
    }}},
    {"name": "body", "type": "bytes"}
  ]
}"#;

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn fn_name_is_prefixed() {
    assert_eq!(http_fn_name("my-svc"), "impulse-ring-http/v1/my-svc");
  }

  #[test]
  fn header_ctor() {
    let h = RingHeader::new("Content-Type", "application/json");
    assert_eq!(h.name, "Content-Type");
    assert_eq!(h.value, "application/json");
  }
}
