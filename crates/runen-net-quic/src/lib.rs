//! Production QUIC adapter package for RunenNet.
//!
//! Normative QUIC wire and transport semantics live in the repository
//! `spec/transport/quic.md`. This crate is the downstream implementation
//! boundary for that profile and does not own transport-independent RunenNet
//! semantics.

// RN5C2B wires the accepted RN5C1 primitives to Quinn while the connection
// bootstrap/control owner remains deferred to RN5E. Keep the realization
// crate-private until RN6 justifies a public standalone facade.
#[allow(
    dead_code,
    reason = "RN5C2B lands crate-private reliable realization before RN5E/RN6 wiring"
)]
mod quinn_binding;
#[allow(
    dead_code,
    reason = "RN5C1 primitives remain internal implementation authority for RN5C2B"
)]
mod reliable;
#[allow(
    dead_code,
    reason = "RN5C1 wire primitives remain internal implementation authority for RN5C2B"
)]
mod wire;
