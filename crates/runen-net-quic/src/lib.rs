//! Production QUIC adapter package for RunenNet.
//!
//! Normative QUIC wire and transport semantics live in the repository
//! `spec/transport/quic.md`. This crate is the downstream implementation boundary for that profile
//! and does not own transport-independent RunenNet semantics.
//!
//! # Ordinary lifecycle
//!
//! A normal application follows this public path:
//!
//! ```text
//! EndpointConfig / ProfileConfig
//!     -> ClientEndpoint::connect / ServerEndpoint::accept
//!     -> ProfileReadyConnection
//!     -> Connection::activate
//!     -> Connection::poll + explicit host negotiation decisions
//!     -> ConnectionEvent::Established
//!     -> Core-keyed flow operations
//!     -> consuming Connection::teardown
//! ```
//!
//! [`EndpointConfig::baseline`] and [`ProfileConfig::baseline`] are the named finite first-use
//! configuration path. Full limit structures remain available for expert transport tuning.
//! `ClientEndpoint::connect` and `ServerEndpoint::accept` perform async transport/ProfileReady
//! bootstrap without retaining mutable Core owners across `.await`.
//!
//! [`ProfileReadyConnection::activate`] consumes ProfileReady ownership and returns one durable
//! [`Connection`]. The application keeps its `runen_net::protocol::NegotiationManager` and
//! `runen_net::delivery::DeliveryEndpoint`; [`Connection::poll`] borrows them synchronously while
//! progressing negotiation and established transport work. There is no hidden connection task,
//! command queue, or second delivery-state authority.
//!
//! # Two different established stages
//!
//! Core compatibility establishment and QUIC I/O readiness are adjacent but intentionally distinct:
//!
//! 1. During polling, the Core `runen_net::protocol::NegotiationManager` reaches established
//!    compatibility. Its `established(connection)` method returns
//!    `runen_net::protocol::EstablishedNegotiation`, which is the proof consumed by Core session
//!    admission/replacement.
//! 2. The same public [`Connection`] then activates the negotiated state into its established QUIC
//!    flow/delivery driver and emits [`ConnectionEvent::Established`]. That event means ordinary
//!    QUIC flow I/O is ready; it is not the session-admission proof.
//!
//! A host that uses `runen_net::session::Session` may therefore obtain/use the Core established
//! negotiation before or alongside observing the later QUIC established event, according to its
//! lifecycle policy. The two values must not be substituted for one another.
//!
//! # Authority and identity
//!
//! QUIC client/server side does not choose RunenNet semantic Authority. [`SemanticRole`] declares
//! the profile role independently; an Authority may be on either transport side.
//!
//! Public flow and connection operations remain keyed by Core
//! `runen_net::identity::ConnectionHandle` and `runen_net::delivery::DeliveryFlowKey`. Quinn
//! connection/stream/DATAGRAM identifiers remain private transport mechanics.
//!
//! # Events and payload custody
//!
//! [`ConnectionEvent`] reports durable host-visible progress or decisions. In particular,
//! [`ConnectionEvent::DataReady`] identifies the Core flow with observable data; it does not copy or
//! own the payload. The application reads the message from its
//! `runen_net::delivery::DeliveryEndpoint`, preserving one delivery custody authority for QUIC and
//! custom transports alike.
//!
//! Incoming flow requests are move-only host decisions, and retryable submission/decision errors
//! preserve the owned request or payload where retry is legal. Teardown is consuming and returns
//! [`ConnectionTeardown`] evidence; higher-level session retention/replacement policy remains in
//! Core/the host.
//!
//! # Advanced transport boundary
//!
//! Ordinary applications using this crate do not need `runen_net::delivery::adapter`. That advanced
//! sealed extension boundary is for custom transport realizations and is also what this QUIC crate
//! uses internally. It operates on the same application-owned `DeliveryEndpoint` and cannot replace
//! Core delivery acceptance, ordering, pressure, exposure, or termination semantics.

mod facade;
pub use facade::{
    CertificateDer, ClientEndpoint, ClientTrust, EndpointBindError, EndpointConfig,
    EndpointResourceError, EndpointResourceLimits, PrivateKeyDer, ProfileBootstrapFailure,
    ProfileConfig, ProfileConfigError, ProfileConnectionError, ProfileConnectionErrorKind,
    ProfileLimits, ProfileReadyConnection, ReliableReceiveLimits, SemanticRole, ServerEndpoint,
    ServerIdentity, TlsMaterialError,
};
mod public_connection;
pub use public_connection::{
    Connection, ConnectionCleanupError, ConnectionError, ConnectionErrorKind, ConnectionEvent,
    ConnectionStateError, ConnectionTeardown, NegotiationFailure, NegotiationReportStatus,
    UnreliableReceiveDropReason,
};
mod public_flow;
pub use public_flow::{
    FlowCommandError, FlowRejectionReason, FlowTerminationCause, FlowTerminationOrigin,
    InboundFlowConfig, IncomingFlowDecisionError, IncomingFlowRequest, OutboundFlowConfig,
    SubmissionError, SubmitOutcome,
};

#[cfg(test)]
mod conformance_tests;
#[cfg(test)]
mod peer_close_conformance_tests;
#[allow(
    dead_code,
    reason = "RN5E5A5L lands the crate-private established connection orchestrator before RN5E5 loopback conformance"
)]
mod connection_driver;
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
    reason = "RN5E5A5F lands crate-private DATAGRAM scheduling/read ownership before the unified RN5E5 connection poll loop"
)]
mod datagram_driver;
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
// RN5C2B wires the accepted reliable primitives to Quinn. RN6 keeps this
// low-level realization private behind the deliberate public facade rather
// than making transport mechanics part of the standalone application API.
#[allow(
    dead_code,
    reason = "low-level reliable realization remains crate-private behind the RN6 facade"
)]
mod quinn_binding;
#[allow(
    dead_code,
    reason = "RN5C1 primitives remain internal implementation authority for RN5C2B"
)]
mod reliable;
#[allow(
    dead_code,
    reason = "RN5E5A5C lands crate-private reliable stream acquisition/binding ownership before the unified RN5E5 connection poll loop"
)]
mod reliable_driver;
#[allow(
    dead_code,
    reason = "RN5C1 wire primitives remain internal implementation authority for RN5C2B"
)]
mod wire;
