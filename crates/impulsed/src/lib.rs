//! `impulsed` — the Ring broker daemon, exposed as a library.
//!
//! Pure shared-memory IPC broker: no HTTP, no sockets. It owns the well-known
//! control segment under `/dev/shm` and the data arenas it hands out to
//! connectors. Linux-only (Tier 0: arm64/amd64).
//!
//! The daemon is also embeddable: another binary (e.g. an orchestrator) can
//! own the broker's lifecycle directly via [`Broker::start`] and
//! [`Broker::run`] instead of spawning the `impulsed` executable. A
//! process-wide singleton guard (see [`StartError::AlreadyRunning`]) makes
//! that safe — only one broker can own the control segment at a time.

#![deny(warnings, clippy::todo, clippy::unimplemented)]

pub mod broker;
pub mod registry;

pub use broker::{Broker, StartError};
