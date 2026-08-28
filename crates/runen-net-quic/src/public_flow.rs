use std::num::NonZeroUsize;

use runen_net::{
    delivery::{
        DeliveryFlowKey, DeliveryMode, DeliveryScopeLimits, FlowResourcePolicy,
    },
    identity::ConnectionHandle,
};

use crate::flow_control::InboundOpenRequest;

/// Explicit application-owned declaration for one outbound RunenNet delivery flow.
///
/// The Core `DeliveryFlowKey` is the public flow identity. Delivery mode and resource
/// policy are fixed explicitly; the QUIC adapter never rewrites them based on transport
/// availability, payload size, or pressure.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct OutboundFlowConfig {
    pub key: DeliveryFlowKey,
    pub mode: DeliveryMode,
    pub policy: FlowResourcePolicy,
    pub connection_limits: DeliveryScopeLimits,
    pub stable_max_message_bytes: NonZeroUsize,
}

/// Move-only capability for one pending peer `OPEN_FLOW` decision.
///
/// The private wire flow identity never crosses this boundary. The host-visible
/// connection identity is retained so a request from one connection cannot be
/// accidentally applied to another connection with coincident transport-local state.
#[derive(Debug, PartialEq, Eq)]
pub struct IncomingFlowRequest {
    pub(super) connection: ConnectionHandle,
    pub(super) inner: InboundOpenRequest,
}

impl IncomingFlowRequest {
    pub const fn connection(&self) -> ConnectionHandle {
        self.connection
    }

    pub const fn mode(&self) -> DeliveryMode {
        self.inner.mode()
    }

    pub const fn max_message_bytes(&self) -> u64 {
        self.inner.max_message_bytes()
    }
}

/// Public rejection vocabulary for an incoming or outbound flow establishment attempt.
///
/// These are the two accepted revision-1 profile rejection classes. Wire codes remain private.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FlowRejectionReason {
    ResourceLimit,
    MessageLimit,
}

/// Which endpoint caused an observable delivery-flow termination.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FlowTerminationOrigin {
    Local,
    Remote,
}

/// Application-facing profile cause for an observable delivery-flow termination.
///
/// This mirrors the accepted revision-1 termination classes without exposing wire enums.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FlowTerminationCause {
    Normal,
    ResourceFailure,
    ProtocolFailure,
    ReliableDeliveryFailure,
}

/// Result of one public Core-keyed message submission.
///
/// Reliable submissions use the Core acceptance result directly. Unreliable submissions
/// additionally preserve the two accepted pre-accept QUIC DATAGRAM rejection classes.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SubmitOutcome {
    Accepted {
        accepted_index: u64,
        local_pressure_drops: usize,
    },
    RejectedTooLarge,
    RejectedPressure,
    RejectedCounterExhausted,
    RejectedTransportUnavailable,
    RejectedCurrentDatagramSize,
}
