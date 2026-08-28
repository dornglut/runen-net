use std::num::NonZeroUsize;

use runen_net::delivery::{
    DeliveryFlowKey, DeliveryMode, DeliveryScopeLimits, FlowResourcePolicy,
};

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
