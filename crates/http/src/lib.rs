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
///
/// v2 adds the streaming/upgrade convention ([`RingUpgradeKind`],
/// [`RingStreamFrame`], the `X-Impulse-Ring-*` handshake headers and the
/// channel-naming helpers) on top of the v1 request/response RPC, which is
/// unchanged and remains wire-compatible.
pub const PROTOCOL_VERSION: u32 = 2;

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

// ===========================================================================
// Streaming / upgrade convention (protocol v2)
// ===========================================================================
//
// Plain HTTP keeps using the single-shot [`RingHttpRequest`]/[`RingHttpResponse`]
// RPC above. Streaming and connection-upgrade protocols (SSE, WebSocket,
// WebTransport) are layered on top *without* touching the base Ring IPC: the
// initial request/response is still an ordinary RPC and merely *negotiates* the
// streaming, while the bytes themselves flow over Ring **channels**
// (`publish_channel`/`subscribe`).
//
// The handshake works like this:
//
// 1. The client issues a normal [`RingHttpRequest`] carrying the usual upgrade
//    intent (`Accept: text/event-stream`, `Upgrade: websocket`, …). For the
//    directions it will *produce* (WebSocket up-channel, WebTransport), it
//    publishes its channels first and names them via the request headers below.
// 2. The server runs its pipeline, publishes the channels it will *produce*
//    (the SSE/WebSocket down-channel, WebTransport session channels) and answers
//    with a normal [`RingHttpResponse`] whose headers carry
//    [`HEADER_UPGRADE`] plus the channel names ([`HEADER_CHAN_DOWN`], …).
// 3. Both ends `subscribe` to the peer's channels (resolved by name through
//    `Connection::list_channels`) and exchange [`RingStreamFrame`]s.

/// The negotiated streaming/upgrade protocol, as carried by [`HEADER_UPGRADE`].
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq)]
pub enum RingUpgradeKind {
  /// Server-Sent Events: a single server→client stream of bytes.
  Sse,
  /// WebSocket: a bidirectional byte stream (a channel per direction).
  WebSocket,
  /// WebTransport: a session multiplexing bidi/uni streams and datagrams.
  WebTransport,
}

impl RingUpgradeKind {
  /// The lowercase token used on the wire (in [`HEADER_UPGRADE`]).
  pub fn as_str(&self) -> &'static str {
    match self {
      RingUpgradeKind::Sse => "sse",
      RingUpgradeKind::WebSocket => "websocket",
      RingUpgradeKind::WebTransport => "webtransport",
    }
  }

  /// Parse the wire token (case-insensitive). Returns `None` if unrecognized.
  pub fn parse(s: &str) -> Option<Self> {
    match s.trim().to_ascii_lowercase().as_str() {
      "sse" => Some(RingUpgradeKind::Sse),
      "websocket" | "ws" => Some(RingUpgradeKind::WebSocket),
      "webtransport" | "wt" => Some(RingUpgradeKind::WebTransport),
      _ => None,
    }
  }
}

/// Response header naming the negotiated [`RingUpgradeKind`] (its `as_str`).
///
/// Its presence on a [`RingHttpResponse`] is what tells the client the call was
/// upgraded to a streaming session rather than a one-shot response.
pub const HEADER_UPGRADE: &str = "x-impulse-ring-upgrade";
/// Header carrying the server→client channel name (SSE/WebSocket down-channel).
pub const HEADER_CHAN_DOWN: &str = "x-impulse-ring-chan-down";
/// Header carrying the client→server channel name (WebSocket up-channel).
pub const HEADER_CHAN_UP: &str = "x-impulse-ring-chan-up";
/// Header carrying the WebTransport datagram channel name.
pub const HEADER_CHAN_DATAGRAMS: &str = "x-impulse-ring-chan-datagrams";
/// Header carrying the opaque session id chosen for this upgrade.
pub const HEADER_SESSION: &str = "x-impulse-ring-session";

/// Response header naming a channel that carries a **chunked response body**.
///
/// A unary RPC response travels as a single reply-ring record, so a body that
/// would exceed the reply ring cannot be returned inline. When the server
/// produces such a body it publishes a Ring channel, streams the body onto it as
/// [`RingStreamFrame`] [`opcode::DATA`] chunks terminated by [`opcode::CLOSE`],
/// and sets this header on an otherwise normal [`RingHttpResponse`] (with an
/// empty inline `body`). The client subscribes to the named channel, reassembles
/// the chunks into the full body and strips this header — so the chunking is
/// transparent to HTTP consumers (including the LBRP `impring://` connector).
///
/// This is orthogonal to [`HEADER_UPGRADE`]: it carries a *finite* body, not a
/// live SSE/WebSocket stream.
pub const HEADER_BODY_CHANNEL: &str = "x-impulse-ring-body-chan";

/// Largest response body the server returns inline through the reply ring.
///
/// Kept comfortably under `impulse_ring_core::control::REPLY_CAP` (512 KiB) to
/// leave room for the status, headers and Avro framing in the same record.
/// Bodies above this are streamed over a channel named by [`HEADER_BODY_CHANNEL`].
pub const MAX_INLINE_RESPONSE_BODY: usize = 448 * 1024;

/// Chunk size used when streaming a large body over a body channel.
///
/// Kept under `impulse_ring_core::control::ARENA_CAP` (256 KiB) so each frame
/// fits the channel's data arena with room for framing.
pub const RESPONSE_BODY_CHUNK: usize = 192 * 1024;

/// Request header naming a channel that carries a **streamed request body**.
///
/// A unary RPC argument travels as a single record on the function's request
/// ring, so a request body that would exceed that ring cannot be shipped inline
/// (the connector reports `function request ring full`). When the client has such
/// a body it publishes a Ring channel, streams the body onto it as
/// [`RingStreamFrame`] [`opcode::DATA`] chunks terminated by [`opcode::CLOSE`],
/// and sets this header on an otherwise normal [`RingHttpRequest`] (with an empty
/// inline `body`). The listener subscribes to the named channel, reassembles the
/// chunks into the full body and strips this header — so the chunking is
/// transparent to the HTTP pipeline (and to the LBRP `impring://` connector,
/// which just forwards the request as-is).
///
/// This is the request-side mirror of [`HEADER_BODY_CHANNEL`].
pub const HEADER_REQUEST_BODY_CHANNEL: &str = "x-impulse-ring-req-body-chan";

/// Largest request body the client ships inline through the function request ring.
///
/// Kept comfortably under `impulse_ring_core::control::ARENA_CAP` (256 KiB) — the
/// capacity of a function's request ring — to leave room for the method, uri,
/// headers and Avro/RPC framing in the same record. Bodies above this are streamed
/// over a channel named by [`HEADER_REQUEST_BODY_CHANNEL`]. Mirrors
/// [`MAX_INLINE_RESPONSE_BODY`] on the request side.
pub const MAX_INLINE_REQUEST_BODY: usize = 192 * 1024;

/// Chunk size used when streaming a large request body over a body channel.
///
/// Kept under `impulse_ring_core::control::ARENA_CAP` (256 KiB) so each frame fits
/// the channel's data arena with room for framing. Mirrors [`RESPONSE_BODY_CHUNK`].
pub const REQUEST_BODY_CHUNK: usize = 192 * 1024;

// The inline ceiling and the streaming chunk must both fit a 256 KiB arena with
// framing headroom (the smallest configurable request arena, `MIN_ARENA_CAP`).
const _: () = assert!(MAX_INLINE_REQUEST_BODY <= 256 * 1024);
const _: () = assert!(REQUEST_BODY_CHUNK <= 256 * 1024);

/// Prefix for channels carrying HTTP-over-Ring stream data.
pub const STREAM_CHAN_PREFIX: &str = "impulse-ring-http/v1/stream/";

/// Derive a stream channel name from the application, session id and a role.
///
/// `role` distinguishes the channels of one session, e.g. `"down"`, `"up"`,
/// `"dgram"`, or `"s{stream_id}"` for an individual WebTransport stream. Keeping
/// the app and session in the name makes channels easy to find with
/// `Connection::list_channels` and avoids collisions across sessions.
pub fn stream_channel_name(app: &str, session_id: u64, role: &str) -> String {
  format!("{STREAM_CHAN_PREFIX}{app}/{session_id:016x}/{role}")
}

/// Stream-frame opcodes carried in [`RingStreamFrame::opcode`].
pub mod opcode {
  /// Raw byte payload for the (single) stream — SSE body / WebSocket bytes.
  pub const DATA: i32 = 0;
  /// Orderly end of the stream/direction; `payload` is empty.
  pub const CLOSE: i32 = 1;
  /// A WebTransport datagram; `payload` is the datagram bytes.
  pub const DATAGRAM: i32 = 2;
  /// A new WebTransport stream was opened (`stream_id` set; `payload` empty).
  pub const STREAM_OPEN: i32 = 3;
  /// Bytes for the WebTransport stream identified by `stream_id`.
  pub const STREAM_DATA: i32 = 4;
  /// The WebTransport stream identified by `stream_id` was closed.
  pub const STREAM_CLOSE: i32 = 5;
}

/// One message on a streaming channel.
///
/// For SSE/WebSocket only [`opcode::DATA`]/[`opcode::CLOSE`] are used and
/// `stream_id` is `0`. For WebTransport, `stream_id` multiplexes several logical
/// streams over one Ring channel and the `STREAM_*`/`DATAGRAM` opcodes apply.
#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct RingStreamFrame {
  /// One of the [`opcode`] constants.
  pub opcode: i32,
  /// WebTransport stream id, or `0` for SSE/WebSocket and datagrams.
  pub stream_id: i64,
  /// Frame payload (raw bytes; may be empty for control opcodes).
  #[serde(with = "serde_bytes")]
  pub payload: Vec<u8>,
}

impl RingStreamFrame {
  /// A [`opcode::DATA`] frame carrying `payload`.
  pub fn data(payload: impl Into<Vec<u8>>) -> Self {
    RingStreamFrame {
      opcode: opcode::DATA,
      stream_id: 0,
      payload: payload.into(),
    }
  }

  /// A [`opcode::CLOSE`] frame.
  pub fn close() -> Self {
    RingStreamFrame {
      opcode: opcode::CLOSE,
      stream_id: 0,
      payload: Vec::new(),
    }
  }

  /// A [`opcode::DATAGRAM`] frame carrying `payload`.
  pub fn datagram(payload: impl Into<Vec<u8>>) -> Self {
    RingStreamFrame {
      opcode: opcode::DATAGRAM,
      stream_id: 0,
      payload: payload.into(),
    }
  }
}

/// Avro schema (JSON) for [`RingStreamFrame`]. Used as the channel schema for
/// every streaming channel, so both ends agree via the broker fingerprint check.
pub const STREAM_SCHEMA: &str = r#"{
  "type": "record",
  "name": "RingStreamFrame",
  "fields": [
    {"name": "opcode", "type": "int"},
    {"name": "stream_id", "type": "long"},
    {"name": "payload", "type": "bytes"}
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

  #[test]
  fn upgrade_kind_round_trips() {
    for k in [
      RingUpgradeKind::Sse,
      RingUpgradeKind::WebSocket,
      RingUpgradeKind::WebTransport,
    ] {
      assert_eq!(RingUpgradeKind::parse(k.as_str()), Some(k));
    }
    assert_eq!(RingUpgradeKind::parse("WebSocket"), Some(RingUpgradeKind::WebSocket));
    assert_eq!(RingUpgradeKind::parse("ws"), Some(RingUpgradeKind::WebSocket));
    assert_eq!(RingUpgradeKind::parse("nope"), None);
  }

  #[test]
  fn stream_channel_names_are_unique_per_session_and_role() {
    let a = stream_channel_name("svc", 1, "down");
    let b = stream_channel_name("svc", 1, "up");
    let c = stream_channel_name("svc", 2, "down");
    assert_ne!(a, b);
    assert_ne!(a, c);
    assert!(a.starts_with(STREAM_CHAN_PREFIX));
  }

  #[test]
  fn stream_frame_ctors() {
    assert_eq!(RingStreamFrame::data(vec![1, 2, 3]).opcode, opcode::DATA);
    assert_eq!(RingStreamFrame::close().opcode, opcode::CLOSE);
    assert!(RingStreamFrame::close().payload.is_empty());
    assert_eq!(RingStreamFrame::datagram(vec![9]).opcode, opcode::DATAGRAM);
  }
}
