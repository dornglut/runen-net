use std::{fmt, num::NonZeroUsize};

use runen_net::{
    delivery::{
        DeliveryFlowKey, DeliveryMode, DeliveryScopeLimits, FlowResourcePolicy, SubmissionOutcome,
    },
    identity::ConnectionHandle,
};

use crate::{
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

/// Explicit host-owned Core admission configuration for one incoming flow request.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct InboundFlowConfig {
    pub key: DeliveryFlowKey,
    pub policy: FlowResourcePolicy,
    pub connection_limits: DeliveryScopeLimits,
}

/// Move-only capability for one pending peer `OPEN_FLOW` decision.
///
/// The private wire flow identity never crosses this boundary. The host-visible
/// connection identity is retained so a request from one connection cannot be
/// accidentally applied to another connection with coincident transport-local state.
pub struct IncomingFlowRequest {
    pub(super) connection: ConnectionHandle,
    pub(super) inner: InboundOpenRequest,
}

impl fmt::Debug for IncomingFlowRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IncomingFlowRequest")
            .field("connection", &self.connection)
            .field("mode", &self.mode())
            .field("max_message_bytes", &self.max_message_bytes())
            .finish()
    }
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

/// Stable application-facing failure categories for established flow commands.
///
/// Private driver, wire, and transport error topology deliberately remains behind this boundary.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FlowCommandError {
    NotEstablished,
    Busy,
    Terminal,
    WrongConnection,
    WrongDirection,
    InvalidConfiguration,
    AlreadyExists,
    Pending,
    UnknownFlow,
    StaleRequest,
    MessageLimit,
    ResourceLimit,
    DatagramTooSmall,
    FlowTerminated,
    ProtocolFailure,
    ConnectionFailure,
}

/// Incoming decision failure that preserves the move-only request whenever retry is legal.
#[derive(Debug)]
pub enum IncomingFlowDecisionError {
    Retryable {
        request: IncomingFlowRequest,
        reason: FlowCommandError,
    },
    Failed(FlowCommandError),
}

impl IncomingFlowDecisionError {
    pub const fn reason(&self) -> FlowCommandError {
        match self {
            Self::Retryable { reason, .. } | Self::Failed(reason) => *reason,
        }
    }

    pub fn into_request(self) -> Option<IncomingFlowRequest> {
        match self {
            Self::Retryable { request, .. } => Some(request),
            Self::Failed(_) => None,
        }
    }
}

/// Message submission failure.
///
/// Retryable state or identity failures return the original owned payload so the host can
/// retry explicitly. RunenNet never installs a hidden retry queue.
#[derive(Debug)]
pub enum SubmissionError {
    Retryable {
        key: DeliveryFlowKey,
        payload: Vec<u8>,
        reason: FlowCommandError,
    },
    Failed(FlowCommandError),
}

impl SubmissionError {
    pub const fn reason(&self) -> FlowCommandError {
        match self {
            Self::Retryable { reason, .. } | Self::Failed(reason) => *reason,
        }
    }

    pub const fn key(&self) -> Option<DeliveryFlowKey> {
        match self {
            Self::Retryable { key, .. } => Some(*key),
            Self::Failed(_) => None,
        }
    }

    pub fn payload(&self) -> Option<&[u8]> {
        match self {
            Self::Retryable { payload, .. } => Some(payload.as_slice()),
            Self::Failed(_) => None,
        }
    }

    pub fn into_payload(self) -> Option<Vec<u8>> {
        match self {
            Self::Retryable { payload, .. } => Some(payload),
            Self::Failed(_) => None,
        }
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
/// Core acceptance/rejection is preserved directly. Unreliable submission may additionally
/// reject before Core acceptance when the current negotiated DATAGRAM size cannot carry the
/// message. Loss of negotiated DATAGRAM capability is connection-terminal and is therefore
/// reported through the connection error boundary, not as a submission outcome.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SubmitOutcome {
    Accepted {
        accepted_index: u64,
        local_pressure_drops: usize,
    },
    RejectedTooLarge,
    RejectedPressure,
    RejectedCounterExhausted,
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
    fn submit_outcomes_preserve_core_rejections() {
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
            SubmitOutcome::from(SubmissionOutcome::RejectedPressure),
            SubmitOutcome::RejectedPressure
        );
    }

    #[test]
    fn retryable_submission_preserves_owned_payload() {
        let key = DeliveryFlowKey::new(
            ConnectionHandle::new(1),
            runen_net::delivery::FlowDirection::Outbound,
            runen_net::delivery::DeliveryFlowHandle::new(2),
        );
        let error = SubmissionError::Retryable {
            key,
            payload: b"retry".to_vec(),
            reason: FlowCommandError::Busy,
        };
        assert_eq!(error.reason(), FlowCommandError::Busy);
        assert_eq!(error.key(), Some(key));
        assert_eq!(error.payload(), Some(b"retry".as_slice()));
        assert_eq!(error.into_payload(), Some(b"retry".to_vec()));
    }
}
