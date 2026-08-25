//! Production QUIC adapter package for RunenNet.
//!
//! Normative QUIC wire and transport semantics live in the repository
//! `spec/transport/quic.md`. This crate is the downstream implementation
//! boundary for that profile and does not own transport-independent RunenNet
//! semantics.

// RN5C1 establishes pure internal primitives before RN5C2 binds them to Quinn.
// Keep them crate-private until the standalone public surface is justified.
#[allow(
    dead_code,
    reason = "RN5C1 intentionally lands pure adapter primitives before RN5C2 runtime wiring"
)]
mod reliable;
#[allow(
    dead_code,
    reason = "RN5C1 intentionally lands pure adapter primitives before RN5C2 runtime wiring"
)]
mod wire;
