//! Production QUIC adapter package for RunenNet.
//!
//! Normative QUIC wire and transport semantics live in the repository
//! `spec/transport/quic.md`. This crate is the downstream implementation
//! boundary for that profile and does not own transport-independent RunenNet
//! semantics.

#[allow(
    dead_code,
    reason = "RN5E3 lands crate-private profile control before RN5E4/RN5E5 wiring"
)]
mod control;
#[allow(
    dead_code,
    reason = "RN5D3 lands crate-private DATAGRAM realization before RN5E/RN6 wiring"
)]
mod datagram;
#[allow(
    dead_code,
    reason = "RN5E2 lands crate-private endpoint configuration before later RN5E control/lifecycle wiring"
)]
mod endpoint;
#[allow(
    dead_code,
    reason = "RN5E4B1 lands crate-private delivery-flow control before RN5E5 wiring"
)]
mod flow_control;
#[allow(
    dead_code,
    reason = "RN5E5A4D lands crate-private phase-separated flow-control sends before the later RN5E5 lifecycle driver"
)]
mod flow_driver;
#[allow(
    dead_code,
    reason = "RN5E5A2B lands crate-private admitted ProfileReady lifecycle before later RN5E5 wiring"
)]
mod lifecycle;
#[allow(
    dead_code,
    reason = "RN5E4A2B lands crate-private compatibility negotiation control before RN5E4B/RN5E5 wiring"
)]
mod negotiation;
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
