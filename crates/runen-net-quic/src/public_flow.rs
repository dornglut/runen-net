use std::num::NonZeroUsize;

use runen_net::{
    delivery::{
        DeliveryFlowKey, DeliveryMode, DeliveryScopeLimits, FlowResourcePolicy, SubmissionOutcome,
    },
    identity::ConnectionHandle,
};

use crate::{
    datagram::DatagramSubmissionOutcome,
    flow_control::InboundOpenRequest,
    wire::{FlowRejectReason, FlowTerminateReason},
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

impl From<FlowRejectReason> for FlowRejectionReason {
    fn from(reason: FlowRejectReason) -> Self {
        match reason {
            FlowRejectReason::ResourceLimit => Self::ResourceLimit,
            FlowRejectReason::MessageLimit => Self::MessageLimit,
        }
    }
}

impl From<FlowRejectionReason> for FlowRejectReason {
    fn from(reason: FlowRejectionReason) -> Self {
        match reason {
            FlowRejectionReason::ResourceLimit => Self::ResourceLimit,
            FlowRejectionReason::MessageLimit => Self::MessageLimit,
        }
    }
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

impl From<FlowTerminateReason> for FlowTerminationCause {
    fn from(reason: FlowTerminateReason) -> Self {
        match reason {
            FlowTerminateReason::Normal => Self::Normal,
            FlowTerminateReason::ResourceFailure => Self::ResourceFailure,
            FlowTerminateReason::ProtocolFailure => Self::ProtocolFailure,
            FlowTerminateReason::ReliableDeliveryFailure => Self::ReliableDeliveryFailure,
        }
    }
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

impl From<SubmissionOutcome> for SubmitOutcome {
    fn from(outcome: SubmissionOutcome) -> Self {
        match outcome {
            SubmissionOutcome::Accepted {
                accepted_index,
                local_pressure_drops,
            } => Self::Accepted {
                accepted_index,
                local_pressure_drops,
            },
            SubmissionOutcome::RejectedTooLarge => Self::RejectedTooLarge,
            SubmissionOutcome::RejectedPressure => Self::RejectedPressure,
            SubmissionOutcome::RejectedCounterExhausted => Self::RejectedCounterExhausted,
        }
    }
}

impl From<DatagramSubmissionOutcome> for SubmitOutcome {
    fn from(outcome: DatagramSubmissionOutcome) -> Self {
        match outcome {
            DatagramSubmissionOutcome::Accepted {
                accepted_index,
                local_pressure_drops,
            } => Self::Accepted {
                accepted_index,
                local_pressure_drops,
            },
            DatagramSubmissionOutcome::RejectedTooLarge => Self::RejectedTooLarge,
            DatagramSubmissionOutcome::RejectedPressure => Self::RejectedPressure,
            DatagramSubmissionOutcome::RejectedCounterExhausted => Self::RejectedCounterExhausted,
            DatagramSubmissionOutcome::RejectedTransportUnavailable => {
                Self::RejectedTransportUnavailable
            }
            DatagramSubmissionOutcome::RejectedCurrentDatagramSize => {
                Self::RejectedCurrentDatagramSize
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_rejection_and_termination_vocabularies_map_exactly() {
        assert_eq!(
            FlowRejectionReason::from(FlowRejectReason::ResourceLimit),
            FlowRejectionReason::ResourceLimit
        );
        assert_eq!(
            FlowRejectionReason::from(FlowRejectReason::MessageLimit),
            FlowRejectionReason::MessageLimit
        );
        assert_eq!(
            FlowTerminationCause::from(FlowTerminateReason::Normal),
            FlowTerminationCause::Normal
        );
        assert_eq!(
            FlowTerminationCause::from(FlowTerminateReason::ResourceFailure),
            FlowTerminationCause::ResourceFailure
        );
        assert_eq!(
            FlowTerminationCause::from(FlowTerminateReason::ProtocolFailure),
            FlowTerminationCause::ProtocolFailure
        );
        assert_eq!(
            FlowTerminationCause::from(FlowTerminateReason::ReliableDeliveryFailure),
            FlowTerminationCause::ReliableDeliveryFailure
        );
    }

    #[test]
    fn submit_outcomes_preserve_core_and_datagram_rejections() {
        assert_eq!(
            SubmitOutcome::from(SubmissionOutcome::Accepted {
                accepted_index: 7,
                local_pressure_drops: 2,
            }),
            SubmitOutcome::Accepted {
                accepted_index: 7,
                local_pressure_drops: 2,
            }
        );
        assert_eq!(
            SubmitOutcome::from(DatagramSubmissionOutcome::RejectedTransportUnavailable),
            SubmitOutcome::RejectedTransportUnavailable
        );
        assert_eq!(
            SubmitOutcome::from(DatagramSubmissionOutcome::RejectedCurrentDatagramSize),
            SubmitOutcome::RejectedCurrentDatagramSize
        );
    }
}
