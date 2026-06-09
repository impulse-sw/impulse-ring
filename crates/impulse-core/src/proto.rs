//! Control-plane protocol: the Avro records exchanged between connectors and the
//! broker, plus the data-plane RPC envelope records.
//!
//! Every message is an Avro record with a fixed schema. A message is placed on a
//! ring inside a [`Frame`](crate::frame::Frame) whose `schema_fp` is the record
//! schema's fingerprint — so the receiver identifies the message type purely
//! from the fingerprint, reusing the same compatibility mechanism used for user
//! payloads. These schemas are fixed and embedded here; the broker and every
//! connector compute identical fingerprints offline, so control schemas never
//! need to be exchanged at runtime.
//!
//! Note on fingerprints: Avro `long` is signed `i64`, but schema fingerprints
//! are `u64`. We transport them as `i64` via a lossless bit reinterpret
//! ([`fp_to_i64`] / [`i64_to_fp`]).

use crate::avro::{self, Fingerprint};
use crate::frame::Frame;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io;
use std::sync::OnceLock;

/// Status codes carried in reply records.
pub mod status {
    pub const OK: i32 = 0;
    pub const ERR_NOT_FOUND: i32 = 1;
    pub const ERR_DENIED: i32 = 2;
    pub const ERR_SCHEMA_MISMATCH: i32 = 3;
    pub const ERR_INTERNAL: i32 = 4;
    pub const ERR_EXISTS: i32 = 5;
}

#[inline]
pub fn fp_to_i64(fp: Fingerprint) -> i64 {
    fp as i64
}
#[inline]
pub fn i64_to_fp(v: i64) -> Fingerprint {
    v as u64
}

/// Every control/RPC message kind.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Kind {
    Register,
    RegisterReply,
    Unregister,
    PublishChannel,
    PublishReply,
    ListChannels,
    ChannelList,
    Subscribe,
    SubscribeReply,
    ExposeFunction,
    ExposeReply,
    LookupFunction,
    LookupReply,
    Heartbeat,
    RpcRequest,
    RpcResponse,
}

// ---- message structs -------------------------------------------------------

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Register {
    pub correlation_id: i64,
    pub app_name: String,
    pub nonce: i64,
    pub reply_segment: String,
    pub heartbeat_ms: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RegisterReply {
    pub correlation_id: i64,
    pub client_id: i64,
    pub status: i32,
    pub message: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Unregister {
    pub correlation_id: i64,
    pub client_id: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PublishChannel {
    pub correlation_id: i64,
    pub client_id: i64,
    pub channel: String,
    pub schema_json: String,
    pub access_key: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PublishReply {
    pub correlation_id: i64,
    pub channel_id: i64,
    pub schema_fp: i64,
    pub arena: String,
    pub status: i32,
    pub message: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ListChannels {
    pub correlation_id: i64,
    pub client_id: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ChannelInfo {
    pub channel_id: i64,
    pub name: String,
    pub owner_app: String,
    pub schema_fp: i64,
    pub requires_key: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ChannelList {
    pub correlation_id: i64,
    pub channels: Vec<ChannelInfo>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Subscribe {
    pub correlation_id: i64,
    pub client_id: i64,
    pub channel_id: i64,
    pub access_key: String,
    pub expected_fp: i64,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SubscribeReply {
    pub correlation_id: i64,
    pub arena: String,
    pub schema_fp: i64,
    pub status: i32,
    pub message: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ExposeFunction {
    pub correlation_id: i64,
    pub client_id: i64,
    pub fn_name: String,
    pub req_schema_json: String,
    pub resp_schema_json: String,
    pub access_key: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ExposeReply {
    pub correlation_id: i64,
    pub fn_id: i64,
    pub req_fp: i64,
    pub resp_fp: i64,
    pub req_arena: String,
    pub status: i32,
    pub message: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LookupFunction {
    pub correlation_id: i64,
    pub client_id: i64,
    pub fn_name: String,
    pub access_key: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct LookupReply {
    pub correlation_id: i64,
    pub fn_id: i64,
    pub req_fp: i64,
    pub resp_fp: i64,
    pub req_arena: String,
    pub status: i32,
    pub message: String,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Heartbeat {
    pub correlation_id: i64,
    pub client_id: i64,
    pub seq: i64,
}

/// Data-plane RPC request, placed on a function's request arena.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RpcRequest {
    pub correlation_id: i64,
    pub caller_id: i64,
    pub reply_segment: String,
    pub arg_fp: i64,
    #[serde(with = "serde_bytes")]
    pub args: Vec<u8>,
}

/// Data-plane RPC response, placed on the caller's reply segment.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct RpcResponse {
    pub correlation_id: i64,
    pub status: i32,
    pub message: String,
    pub result_fp: i64,
    #[serde(with = "serde_bytes")]
    pub result: Vec<u8>,
}

// ---- schemas ---------------------------------------------------------------

macro_rules! schema_json {
    ($name:literal, $fields:literal) => {
        concat!(
            r#"{"type":"record","name":""#,
            $name,
            r#"","namespace":"ring.proto","fields":["#,
            $fields,
            "]}"
        )
    };
}

/// JSON Avro schema for a message kind (the canonical source of the contract).
pub fn schema_json(kind: Kind) -> &'static str {
    schema_for(kind)
}

/// Every message kind, in a stable order.
pub fn all_kinds() -> &'static [Kind] {
    &ALL_KINDS
}

fn schema_for(kind: Kind) -> &'static str {
    match kind {
        Kind::Register => schema_json!(
            "Register",
            r#"{"name":"correlation_id","type":"long"},{"name":"app_name","type":"string"},{"name":"nonce","type":"long"},{"name":"reply_segment","type":"string"},{"name":"heartbeat_ms","type":"long"}"#
        ),
        Kind::RegisterReply => schema_json!(
            "RegisterReply",
            r#"{"name":"correlation_id","type":"long"},{"name":"client_id","type":"long"},{"name":"status","type":"int"},{"name":"message","type":"string"}"#
        ),
        Kind::Unregister => schema_json!(
            "Unregister",
            r#"{"name":"correlation_id","type":"long"},{"name":"client_id","type":"long"}"#
        ),
        Kind::PublishChannel => schema_json!(
            "PublishChannel",
            r#"{"name":"correlation_id","type":"long"},{"name":"client_id","type":"long"},{"name":"channel","type":"string"},{"name":"schema_json","type":"string"},{"name":"access_key","type":"string"}"#
        ),
        Kind::PublishReply => schema_json!(
            "PublishReply",
            r#"{"name":"correlation_id","type":"long"},{"name":"channel_id","type":"long"},{"name":"schema_fp","type":"long"},{"name":"arena","type":"string"},{"name":"status","type":"int"},{"name":"message","type":"string"}"#
        ),
        Kind::ListChannels => schema_json!(
            "ListChannels",
            r#"{"name":"correlation_id","type":"long"},{"name":"client_id","type":"long"}"#
        ),
        Kind::ChannelList => concat!(
            r#"{"type":"record","name":"ChannelList","namespace":"ring.proto","fields":["#,
            r#"{"name":"correlation_id","type":"long"},"#,
            r#"{"name":"channels","type":{"type":"array","items":{"type":"record","name":"ChannelInfo","fields":["#,
            r#"{"name":"channel_id","type":"long"},{"name":"name","type":"string"},{"name":"owner_app","type":"string"},{"name":"schema_fp","type":"long"},{"name":"requires_key","type":"boolean"}"#,
            r#"]}}}]}"#
        ),
        Kind::Subscribe => schema_json!(
            "Subscribe",
            r#"{"name":"correlation_id","type":"long"},{"name":"client_id","type":"long"},{"name":"channel_id","type":"long"},{"name":"access_key","type":"string"},{"name":"expected_fp","type":"long"}"#
        ),
        Kind::SubscribeReply => schema_json!(
            "SubscribeReply",
            r#"{"name":"correlation_id","type":"long"},{"name":"arena","type":"string"},{"name":"schema_fp","type":"long"},{"name":"status","type":"int"},{"name":"message","type":"string"}"#
        ),
        Kind::ExposeFunction => schema_json!(
            "ExposeFunction",
            r#"{"name":"correlation_id","type":"long"},{"name":"client_id","type":"long"},{"name":"fn_name","type":"string"},{"name":"req_schema_json","type":"string"},{"name":"resp_schema_json","type":"string"},{"name":"access_key","type":"string"}"#
        ),
        Kind::ExposeReply => schema_json!(
            "ExposeReply",
            r#"{"name":"correlation_id","type":"long"},{"name":"fn_id","type":"long"},{"name":"req_fp","type":"long"},{"name":"resp_fp","type":"long"},{"name":"req_arena","type":"string"},{"name":"status","type":"int"},{"name":"message","type":"string"}"#
        ),
        Kind::LookupFunction => schema_json!(
            "LookupFunction",
            r#"{"name":"correlation_id","type":"long"},{"name":"client_id","type":"long"},{"name":"fn_name","type":"string"},{"name":"access_key","type":"string"}"#
        ),
        Kind::LookupReply => schema_json!(
            "LookupReply",
            r#"{"name":"correlation_id","type":"long"},{"name":"fn_id","type":"long"},{"name":"req_fp","type":"long"},{"name":"resp_fp","type":"long"},{"name":"req_arena","type":"string"},{"name":"status","type":"int"},{"name":"message","type":"string"}"#
        ),
        Kind::Heartbeat => schema_json!(
            "Heartbeat",
            r#"{"name":"correlation_id","type":"long"},{"name":"client_id","type":"long"},{"name":"seq","type":"long"}"#
        ),
        Kind::RpcRequest => schema_json!(
            "RpcRequest",
            r#"{"name":"correlation_id","type":"long"},{"name":"caller_id","type":"long"},{"name":"reply_segment","type":"string"},{"name":"arg_fp","type":"long"},{"name":"args","type":"bytes"}"#
        ),
        Kind::RpcResponse => schema_json!(
            "RpcResponse",
            r#"{"name":"correlation_id","type":"long"},{"name":"status","type":"int"},{"name":"message","type":"string"},{"name":"result_fp","type":"long"},{"name":"result","type":"bytes"}"#
        ),
    }
}

const ALL_KINDS: [Kind; 16] = [
    Kind::Register,
    Kind::RegisterReply,
    Kind::Unregister,
    Kind::PublishChannel,
    Kind::PublishReply,
    Kind::ListChannels,
    Kind::ChannelList,
    Kind::Subscribe,
    Kind::SubscribeReply,
    Kind::ExposeFunction,
    Kind::ExposeReply,
    Kind::LookupFunction,
    Kind::LookupReply,
    Kind::Heartbeat,
    Kind::RpcRequest,
    Kind::RpcResponse,
];

/// Parsed schemas + fingerprints for every control/RPC message kind.
pub struct Catalog {
    schemas: HashMap<Kind, apache_avro::Schema>,
    fps: HashMap<Kind, Fingerprint>,
    by_fp: HashMap<Fingerprint, Kind>,
}

impl Catalog {
    fn build() -> Catalog {
        let mut schemas = HashMap::new();
        let mut fps = HashMap::new();
        let mut by_fp = HashMap::new();
        for k in ALL_KINDS {
            let (schema, fp) = avro::parse(schema_for(k)).expect("control schema must parse");
            schemas.insert(k, schema);
            fps.insert(k, fp);
            by_fp.insert(fp, k);
        }
        Catalog {
            schemas,
            fps,
            by_fp,
        }
    }

    pub fn schema(&self, k: Kind) -> &apache_avro::Schema {
        &self.schemas[&k]
    }
    pub fn fp(&self, k: Kind) -> Fingerprint {
        self.fps[&k]
    }
    pub fn kind_of(&self, fp: Fingerprint) -> Option<Kind> {
        self.by_fp.get(&fp).copied()
    }
}

/// Process-wide catalog of control schemas (built once).
pub fn catalog() -> &'static Catalog {
    static C: OnceLock<Catalog> = OnceLock::new();
    C.get_or_init(Catalog::build)
}

/// Encode a typed message into a self-describing frame.
pub fn to_frame<T: Serialize>(kind: Kind, msg: &T) -> io::Result<Frame> {
    let c = catalog();
    let body = avro::encode(c.schema(kind), msg)?;
    Ok(Frame::new(c.fp(kind), body))
}

/// Decode a frame's body as kind `T`'s schema.
pub fn from_frame<T: DeserializeOwned>(kind: Kind, frame: &Frame) -> io::Result<T> {
    let c = catalog();
    avro::decode(c.schema(kind), &frame.body)
}

/// Extract the `correlation_id` from any reply/response frame, used by a
/// connector's dispatcher to route a frame to the waiting caller without
/// knowing its concrete type ahead of time.
pub fn correlation_id(frame: &Frame) -> io::Result<i64> {
    let kind = catalog()
        .kind_of(frame.schema_fp)
        .ok_or_else(|| io::Error::other("unknown frame fingerprint"))?;
    let corr = match kind {
        Kind::RegisterReply => from_frame::<RegisterReply>(kind, frame)?.correlation_id,
        Kind::PublishReply => from_frame::<PublishReply>(kind, frame)?.correlation_id,
        Kind::ChannelList => from_frame::<ChannelList>(kind, frame)?.correlation_id,
        Kind::SubscribeReply => from_frame::<SubscribeReply>(kind, frame)?.correlation_id,
        Kind::ExposeReply => from_frame::<ExposeReply>(kind, frame)?.correlation_id,
        Kind::LookupReply => from_frame::<LookupReply>(kind, frame)?.correlation_id,
        Kind::RpcResponse => from_frame::<RpcResponse>(kind, frame)?.correlation_id,
        other => {
            return Err(io::Error::other(format!(
                "frame kind {other:?} is not a routable reply"
            )))
        }
    };
    Ok(corr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn all_schemas_parse_and_have_unique_fps() {
        let c = catalog();
        let mut seen = std::collections::HashSet::new();
        for k in ALL_KINDS {
            let fp = c.fp(k);
            assert!(seen.insert(fp), "duplicate fingerprint for {k:?}");
            assert_eq!(c.kind_of(fp), Some(k));
        }
    }

    #[test]
    fn register_frame_roundtrip() {
        let msg = Register {
            correlation_id: 42,
            app_name: "svc-a".into(),
            nonce: 999,
            reply_segment: "/impulse-ring.cli.999.v1".into(),
            heartbeat_ms: 1000,
        };
        let frame = to_frame(Kind::Register, &msg).unwrap();
        assert_eq!(catalog().kind_of(frame.schema_fp), Some(Kind::Register));
        let back: Register = from_frame(Kind::Register, &frame).unwrap();
        assert_eq!(back.app_name, "svc-a");
        assert_eq!(back.nonce, 999);
    }

    #[test]
    fn rpc_bytes_roundtrip() {
        let msg = RpcResponse {
            correlation_id: 1,
            status: status::OK,
            message: String::new(),
            result_fp: -5,
            result: vec![9, 8, 7, 6],
        };
        let frame = to_frame(Kind::RpcResponse, &msg).unwrap();
        let back: RpcResponse = from_frame(Kind::RpcResponse, &frame).unwrap();
        assert_eq!(back.result, vec![9, 8, 7, 6]);
        assert_eq!(back.result_fp, -5);
    }

    #[test]
    fn channel_list_array_roundtrip() {
        let msg = ChannelList {
            correlation_id: 7,
            channels: vec![
                ChannelInfo {
                    channel_id: 1,
                    name: "metrics".into(),
                    owner_app: "a".into(),
                    schema_fp: 123,
                    requires_key: true,
                },
                ChannelInfo {
                    channel_id: 2,
                    name: "logs".into(),
                    owner_app: "b".into(),
                    schema_fp: 456,
                    requires_key: false,
                },
            ],
        };
        let frame = to_frame(Kind::ChannelList, &msg).unwrap();
        let back: ChannelList = from_frame(Kind::ChannelList, &frame).unwrap();
        assert_eq!(back.channels.len(), 2);
        assert_eq!(back.channels[0].name, "metrics");
        assert!(back.channels[0].requires_key);
        assert!(!back.channels[1].requires_key);
    }
}
