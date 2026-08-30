//! Standalone RunenNet semantic Core.
//!
//! RunenNet Core owns reusable networking semantics and state. It does not own an ECS, gameplay
//! simulation, world/spatial policy, rendering, a scheduler, or an async executor. A host keeps
//! the Core owners it needs and calls them from its own runtime.
//!
//! The normative RunenNet specification lives in the repository `spec/` tree. Rust
//! representations and the navigation guidance here are implementation/public-API choices unless
//! a specification revision explicitly standardizes them.
//!
//! # Mental model
//!
//! The public modules compose in this direction:
//!
//! ```text
//! identity + protocol negotiation
//!         -> session membership / authorization
//!         -> delivery flows
//!         -> replication
//!         -> input / prediction
//! ```
//!
//! This is an authority and navigation map, not a mandatory stack. A byte-channel application can
//! use [`protocol`] and [`delivery`] without adopting replication or prediction. Higher-level
//! modules reuse identities and evidence from lower layers rather than creating parallel truth.
//!
//! Keep the main lifetime domains distinct:
//!
//! - [`identity::SessionId`] identifies one session;
//! - [`identity::ParticipantId`] identifies one participant incarnation under session policy;
//! - [`identity::ConnectionHandle`] identifies one local transport-connection lifetime and has no
//!   wire meaning;
//! - [`identity::SimulationTick`] is host simulation logical time, not session recovery time.
//!
//! # Negotiation and session admission
//!
//! [`protocol::NegotiationManager`] owns compatibility negotiation for Core connection handles.
//! Once negotiation is established, [`protocol::NegotiationManager::established`] returns an
//! [`protocol::EstablishedNegotiation`] view containing the established contract and connection.
//! That value is the compatibility proof consumed by [`session::Session::admit_new`] and
//! [`session::Session::bind_replacement`].
//!
//! This Core notion of "established negotiation" is intentionally narrower than a transport
//! adapter saying that all of its established I/O machinery is ready. In `runen-net-quic`, the
//! later `ConnectionEvent::Established` event marks that adapter-ready stage; it is not the value
//! passed to session admission.
//!
//! # Session recovery
//!
//! [`session::Session`] owns participant membership and connection binding. A typical retained
//! replacement path is:
//!
//! 1. admit a participant with [`protocol::EstablishedNegotiation`];
//! 2. report connection loss with [`session::Session::connection_lost`] and an explicit
//!    [`session::RetentionPolicy`];
//! 3. advance the host-supplied [`session::RecoveryTime`] timeline when appropriate;
//! 4. establish compatibility for a new [`identity::ConnectionHandle`] and pass that
//!    [`protocol::EstablishedNegotiation`] to [`session::Session::bind_replacement`] before expiry;
//! 5. remove/expire membership or close the session when host policy requires it.
//!
//! Recovery time is a host-selected session-retention clock. It is not wall time,
//! [`identity::SimulationTick`], transport time, or reconnect scheduling.
//!
//! # Delivery, replication, and prediction
//!
//! [`delivery::DeliveryEndpoint`] is the Core authority for flow acceptance, buffering, exposure,
//! pressure, and termination. Ordinary applications submit and expose messages through its normal
//! API. The advanced [`delivery::adapter`] module exists only for transport realizations; importing
//! it does not create another delivery-state owner.
//!
//! [`replication`] builds authoritative snapshot/ACK/recovery state on top of application-selected
//! delivery. A [`replication::ReplicationLineageKey`] identifies the replication lineage for one
//! session/participant pair. Within that lineage, [`replication::ReplicationCursor`] orders
//! authoritative committed snapshot progression; [`identity::SimulationTick`] separately records
//! the host simulation tick represented by a committed snapshot. An authority prepares a snapshot,
//! records the shared [`DeliveryAcceptance`] fact after submitting the complete message, and later
//! classifies acknowledgements. A client commits authoritative state through
//! [`replication::ClientReplicationSet`].
//!
//! [`input`] owns authoritative input windows and prediction/reconciliation contracts.
//! [`input::PredictionLineage`] uses the same [`replication::ReplicationLineageKey`] and observes
//! the live [`replication::ClientReplicationSet`]; replication remains the sole authority for the
//! current committed cursor, tick, and recovery state. Prediction's frontier is the latest
//! authoritative [`identity::SimulationTick`] against which local pending inputs are admitted and
//! reconciled. An invalidated prediction lineage cannot admit normal predicted input until the
//! accepted authoritative/recovery conditions reactivate it. The host supplies replay behavior and
//! owns the actual gameplay/simulation state being replayed.
//!
//! # Examples
//!
//! The repository's `runen-net-quic` standalone example is the ordinary production-QUIC starting
//! point. The `runen-net` `authoritative_counter` example is intentionally lower level: it directly
//! realizes delivery through [`delivery::adapter`] to prove transport-independent Core semantics,
//! then demonstrates authoritative replication.

/// Stable host/Core identity domains used by the other subsystems.
///
/// Start here when deciding which value identifies a session, participant, connection lifetime,
/// network entity, or simulation tick. These domains are deliberately distinct and are not
/// process-global allocator services.
pub mod identity;

/// Compatibility declarations and per-connection negotiation authority.
///
/// A host establishes a [`protocol::NegotiatedContract`] for an
/// [`identity::ConnectionHandle`]. The resulting [`protocol::EstablishedNegotiation`] is the Core
/// proof used by [`session`] admission; transport adapters may have additional later readiness
/// stages without replacing this contract authority.
pub mod protocol;

/// Participant membership, connection authorization, retention, and replacement lifecycle.
///
/// [`session::Session`] consumes established negotiation evidence when binding a connection. It
/// owns membership/recovery state, but not reconnect attempts, transport bootstrap, gameplay, or
/// scheduler policy.
pub mod session;

/// Core-keyed message flows, delivery modes, pressure policy, buffering, and exposure.
///
/// [`delivery::DeliveryEndpoint`] is the sole delivery-state authority. A
/// [`delivery::DeliveryFlowKey`] combines one connection lifetime, flow direction, and host flow
/// handle into the Core identity used by both ordinary applications and transport realizations.
/// Ordinary applications use the endpoint's inherent API; custom transports explicitly opt into
/// the advanced [`delivery::adapter`] extension boundary against that same endpoint.
pub mod delivery;

mod delivery_acceptance;
pub use delivery_acceptance::DeliveryAcceptance;
mod error_behavior;

/// Authoritative snapshot emission/ACK/recovery and client reconstruction state.
///
/// [`replication::ReplicationLineageKey`] selects one session/participant lineage and
/// [`replication::ReplicationCursor`] orders its authoritative snapshot progression. Replication
/// consumes delivery acceptance evidence but does not own transport realization. On the client,
/// [`replication::ClientReplicationSet`] remains the authoritative source observed by
/// [`input::PredictionLineage`] during reconciliation.
pub mod replication;

/// Authoritative input admission plus client prediction/reconciliation contracts.
///
/// Prediction is layered on live client replication state rather than duplicating its cursor/tick
/// authority. Its frontier is the latest authoritative simulation tick used to decide which local
/// predicted inputs are still future/pending. RunenNet owns eligibility, ordering, invalidation,
/// and replay contracts; the host owns gameplay meaning and replay execution.
pub mod input;

mod protocol_declaration;
pub use protocol_declaration::{
    CompatibilityOfferBuilder, SchemaContractOfferBuilder, SchemaOfferBuilder,
};
