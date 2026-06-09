//! `impulse-ring-core` — platform-independent mechanics for **Ring**, the
//! shared-memory IPC bus by Impulse.
//!
//! This crate is the single Rust home of the wire mechanics: POSIX
//! shared-memory segments, ring buffers with futex wakeup, Avro framing and
//! fingerprinting, and the control-plane protocol records. The broker
//! (`impulsed`) and the Rust connector (`impulse-ring-connector`) both build on it.
//!
//! Tier 0 is Linux-only (arm64/amd64); see `SPEC/` for the byte-for-byte wire
//! contract that native connectors in other languages implement against.

#![deny(warnings, clippy::todo, clippy::unimplemented)]

pub mod avro;
pub mod control;
pub mod frame;
pub mod futex;
pub mod proto;
pub mod ring;
pub mod shm;
pub mod util;

pub use avro::Fingerprint;
pub use frame::Frame;
pub use ring::Ring;
pub use shm::Segment;
