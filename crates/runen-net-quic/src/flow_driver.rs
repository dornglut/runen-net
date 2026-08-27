use std::{future::Future, num::NonZeroUsize, pin::Pin};

use quinn::Connection;
use runen_net::delivery::{
    DeliveryEndpoint, DeliveryFlowKey, DeliveryMode, FlowTermination, ReceiveOutcome,
};

use crate::{
    control::{
        ControlFrame, ControlFrameType, ControlReceiver, ControlSender, ProfileBootstrapError,
    },
    datagram::{
        DatagramReceiveError, DatagramReceiveFailure, DatagramSendError, DatagramSubmissionError,
    },
    flow_control::{
        EstablishedFlow, FlowControl, FlowControlError, FlowControlProgress, InboundAdmission,
        InboundAdmissionError, InboundOpenRequest, InboundResolution, LocalTermination,
        OutboundOpenError, OutboundOpenRequest, PreparedFlow, PreparedOutboundOpen,
    },
    quinn_binding::{ReceiveError, SendError},
    reliable::ReliableFrameError,
    reliable_driver::ReliableFailureContext,
    wire::{
        ApplicationErrorCode, FlowId, FlowRejectReason, FlowTerminate, FlowTerminateReason,
    },
};

pub(super) type OwnedControlReceiveFuture =
    Pin<Box<dyn Future<Output = ControlReceiveCompletion> + Send + 'static>>;
pub(super) type OwnedFlowControlSendFuture =
    Pin<Box<dyn Future<Output = FlowControlSendCompletion> + Send + 'static>>;

#[must_use = "completed control receive returns the sole receiver direction"]
#[derive(Debug)]
pub(super) struct ControlReceiveCompletion {
    pub(super) receiver: ControlReceiver,
    pub(super) result: Result<ControlFrame, ProfileBootstrapError>,
}

#[must_use = "completed flow-control send returns the sole sender direction"]
#[derive(Debug)]
pub(super) struct FlowControlSendCompletion {
    pub(super) sender: ControlSender,
    pub(super) result: Result<FlowControlSendEffect, Box<FlowControlSendError>>,
}

#[must_use = "pending flow-control reporting must be sent or the connection torn down"]
#[derive(Debug, PartialEq, Eq)]
pub(super) struct PendingFlowControlSend {
    frame: ControlFrame,
    effect: FlowControlSendEffect,
}

#[must_use = "flow-control send effect records already-committed local state"]
#[derive(Debug, PartialEq, Eq)]
pub(super) enum FlowControlSendEffect {
    OutboundOpenPrepared(PreparedFlow),
    InboundRejected {
        flow_id: FlowId,
        reason: FlowRejectReason,
    },
    InboundAccepted(EstablishedFlow),
    OutboundFailedAfterAccept {
        flow_id: FlowId,
        key: DeliveryFlowKey,
        reason: FlowTerminateReason,
    },
    LocalTerminated {
        flow: EstablishedFlow,
        reason: FlowTerminateReason,
        termination: FlowTermination,
    },
    ReportOnlyTermination {
        flow_id: FlowId,
        reason: FlowTerminateReason,
    },
}

#[derive(Debug)]
pub(super) struct FlowControlSendError {
    pub(super) error: ProfileBootstrapError,
    pub(super) effect: FlowControlSendEffect,
}

#[must_use = "flow-control progress may require host admission or a control send"]
#[derive(Debug, PartialEq, Eq)]
pub(super) enum FlowControlDriverProgress {
    InboundOpen(InboundOpenRequest),
    PendingSend(PendingFlowControlSend),
    OutboundEstablished(EstablishedFlow),
    OutboundRejected {
        flow_id: FlowId,
        key: DeliveryFlowKey,
        reason: FlowRejectReason,
    },
    RemoteTerminated {
        flow: EstablishedFlow,
        reason: FlowTerminateReason,
        termination: FlowTermination,
    },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum EstablishedDataFailureDisposition {
    TerminateAndReport {
        flow_id: FlowId,
        reason: FlowTerminateReason,
    },
    ReportOnly {
        flow_id: FlowId,
        reason: FlowTerminateReason,
    },
    CleanupOnly {
        flow_id: FlowId,
    },
    ConnectionTerminal {
        code: Option<ApplicationErrorCode>,
    },
}

#[must_use = "failure disposition may require one control send or connection teardown"]
#[derive(Debug, PartialEq, Eq)]
pub(super) enum EstablishedDataFailureProgress {
    PendingSend(PendingFlowControlSend),
    CleanupOnly {
        flow_id: FlowId,
    },
    ConnectionTerminal {
        code: Option<ApplicationErrorCode>,
    },
}

impl PendingFlowControlSend {
    fn new(frame: ControlFrame, effect: FlowControlSendEffect) -> Self {
        Self { frame, effect }
    }

    pub(super) async fn send(
        self,
        sender: &mut ControlSender,
    ) -> Result<FlowControlSendEffect, Box<FlowControlSendError>> {
        let Self { frame, effect } = self;
        match sender.send_frame(frame.frame_type, &frame.body).await {
            Ok(()) => Ok(effect),
            Err(error) => Err(Box::new(FlowControlSendError { error, effect })),
        }
    }

    pub(super) fn into_owned_send(self, mut sender: ControlSender) -> OwnedFlowControlSendFuture {
        Box::pin(async move {
            let result = self.send(&mut sender).await;
            FlowControlSendCompletion { sender, result }
        })
    }
}

pub(super) fn receive_control_owned(mut receiver: ControlReceiver) -> OwnedControlReceiveFuture {
    Box::pin(async move {
        let result = receiver.receive_frame().await;
        ControlReceiveCompletion { receiver, result }
    })
}

pub(super) fn prepare_outbound_open(
    connection: &Connection,
    flow_control: &mut FlowControl,
    endpoint: &DeliveryEndpoint,
    request: OutboundOpenRequest,
) -> Result<PendingFlowControlSend, OutboundOpenError> {
    let PreparedOutboundOpen { frame, flow } =
        flow_control.prepare_outbound_open(endpoint, request, connection.max_datagram_size())?;
    Ok(PendingFlowControlSend::new(
        frame,
        FlowControlSendEffect::OutboundOpenPrepared(flow),
    ))
}

pub(super) fn process_received(
    flow_control: &mut FlowControl,
    endpoint: &mut DeliveryEndpoint,
    frame: ControlFrame,
) -> Result<FlowControlDriverProgress, FlowControlError> {
    let progress = flow_control.receive(endpoint, frame)?;
    Ok(match progress {
        FlowControlProgress::InboundOpen(request) => {
            FlowControlDriverProgress::InboundOpen(request)
        }
        FlowControlProgress::InboundRejected {
            flow_id,
            reason,
            frame,
        } => FlowControlDriverProgress::PendingSend(PendingFlowControlSend::new(
            frame,
            FlowControlSendEffect::InboundRejected { flow_id, reason },
        )),
        FlowControlProgress::OutboundEstablished(flow) => {
            FlowControlDriverProgress::OutboundEstablished(flow)
        }
        FlowControlProgress::OutboundRejected {
            flow_id,
            key,
            reason,
        } => FlowControlDriverProgress::OutboundRejected {
            flow_id,
            key,
            reason,
        },
        FlowControlProgress::OutboundFailedAfterAccept {
            flow_id,
            key,
            reason,
            frame,
        } => FlowControlDriverProgress::PendingSend(PendingFlowControlSend::new(
            frame,
            FlowControlSendEffect::OutboundFailedAfterAccept {
                flow_id,
                key,
                reason,
            },
        )),
        FlowControlProgress::RemoteTerminated {
            flow,
            reason,
            termination,
        } => FlowControlDriverProgress::RemoteTerminated {
            flow,
            reason,
            termination,
        },
    })
}

pub(super) fn accept_inbound(
    flow_control: &mut FlowControl,
    endpoint: &mut DeliveryEndpoint,
    request: InboundOpenRequest,
    admission: InboundAdmission,
    reliable_max_staging_bytes: NonZeroUsize,
) -> Result<PendingFlowControlSend, InboundAdmissionError> {
    if let Some(reason) = reliable_staging_rejection(
        request.mode(),
        request.max_message_bytes(),
        reliable_max_staging_bytes,
    ) {
        return reject_inbound(flow_control, request, reason);
    }
    let resolution = flow_control.accept_inbound(endpoint, request, admission)?;
    Ok(pending_inbound_resolution(resolution))
}

pub(super) fn reject_inbound(
    flow_control: &mut FlowControl,
    request: InboundOpenRequest,
    reason: FlowRejectReason,
) -> Result<PendingFlowControlSend, InboundAdmissionError> {
    let resolution = flow_control.reject_inbound(request, reason)?;
    Ok(pending_inbound_resolution(resolution))
}

pub(super) fn terminate_local(
    flow_control: &mut FlowControl,
    endpoint: &mut DeliveryEndpoint,
    flow_id: FlowId,
    reason: FlowTerminateReason,
) -> Result<PendingFlowControlSend, FlowControlError> {
    let LocalTermination {
        flow,
        reason,
        termination,
        frame,
    } = flow_control.terminate_local(endpoint, flow_id, reason)?;
    Ok(PendingFlowControlSend::new(
        frame,
        FlowControlSendEffect::LocalTerminated {
            flow,
            reason,
            termination,
        },
    ))
}

pub(super) fn prepare_established_data_failure(
    flow_control: &mut FlowControl,
    endpoint: &mut DeliveryEndpoint,
    disposition: EstablishedDataFailureDisposition,
) -> Result<EstablishedDataFailureProgress, FlowControlError> {
    match disposition {
        EstablishedDataFailureDisposition::TerminateAndReport { flow_id, reason } => {
            terminate_local(flow_control, endpoint, flow_id, reason)
                .map(EstablishedDataFailureProgress::PendingSend)
        }
        EstablishedDataFailureDisposition::ReportOnly { flow_id, reason } => {
            if flow_control.registry().registered_flow(flow_id).is_some() {
                return terminate_local(flow_control, endpoint, flow_id, reason)
                    .map(EstablishedDataFailureProgress::PendingSend);
            }
            pending_report_only_termination(flow_id, reason)
                .map(EstablishedDataFailureProgress::PendingSend)
        }
        EstablishedDataFailureDisposition::CleanupOnly { flow_id } => {
            Ok(EstablishedDataFailureProgress::CleanupOnly { flow_id })
        }
        EstablishedDataFailureDisposition::ConnectionTerminal { code } => {
            Ok(EstablishedDataFailureProgress::ConnectionTerminal { code })
        }
    }
}

pub(super) fn classify_outbound_reliable_active_failure(
    flow_control: &FlowControl,
    context: ReliableFailureContext,
    error: &SendError,
) -> EstablishedDataFailureDisposition {
    let reason = reliable_send_failure_reason(error);
    classify_reliable_active_context(flow_control, context, reason, None)
}

pub(super) fn classify_inbound_reliable_active_failure(
    flow_control: &FlowControl,
    context: ReliableFailureContext,
    error: &ReceiveError,
) -> EstablishedDataFailureDisposition {
    if context == ReliableFailureContext::Unresolved {
        return classify_unresolved_reliable_receive(error);
    }
    let reason = reliable_receive_failure_reason(error);
    classify_reliable_active_context(flow_control, context, reason, None)
}

pub(super) fn classify_inbound_reliable_construction_failure(
    error: &ReceiveError,
) -> EstablishedDataFailureDisposition {
    let code = match error {
        ReceiveError::ZeroRtt => Some(ApplicationErrorCode::ProfileProtocolError),
        ReceiveError::AllocationFailed => Some(ApplicationErrorCode::ResourceLimitError),
        _ => None,
    };
    EstablishedDataFailureDisposition::ConnectionTerminal { code }
}

pub(super) fn classify_outbound_reliable_acquisition_failure(
    flow_control: &FlowControl,
    flow_id: FlowId,
    error: &SendError,
) -> EstablishedDataFailureDisposition {
    known_flow_disposition(
        flow_id,
        reliable_send_failure_reason(error),
        flow_control.registry().registered_flow(flow_id).is_some(),
        false,
    )
}

pub(super) fn classify_known_flow_resource_failure(
    flow_control: &FlowControl,
    flow_id: FlowId,
) -> EstablishedDataFailureDisposition {
    known_flow_disposition(
        flow_id,
        FlowTerminateReason::ResourceFailure,
        flow_control.registry().registered_flow(flow_id).is_some(),
        false,
    )
}

pub(super) fn classify_datagram_receive_failure(
    flow_control: &FlowControl,
    failure: &DatagramReceiveFailure,
) -> EstablishedDataFailureDisposition {
    let Some(flow_id) = failure.flow_id else {
        let code = match failure.error {
            DatagramReceiveError::VarInt(_) | DatagramReceiveError::FlowId(_) => {
                Some(ApplicationErrorCode::ProfileProtocolError)
            }
            DatagramReceiveError::AllocationFailed => {
                Some(ApplicationErrorCode::ResourceLimitError)
            }
            DatagramReceiveError::WrongDirection
            | DatagramReceiveError::ReliableFlow
            | DatagramReceiveError::PayloadExceedsProfile
            | DatagramReceiveError::Core(_) => None,
        };
        return EstablishedDataFailureDisposition::ConnectionTerminal { code };
    };

    let reason = match failure.error {
        DatagramReceiveError::AllocationFailed => FlowTerminateReason::ResourceFailure,
        DatagramReceiveError::VarInt(_)
        | DatagramReceiveError::FlowId(_)
        | DatagramReceiveError::WrongDirection
        | DatagramReceiveError::ReliableFlow
        | DatagramReceiveError::PayloadExceedsProfile
        | DatagramReceiveError::Core(_) => FlowTerminateReason::ProtocolFailure,
    };
    known_flow_disposition(
        flow_id,
        reason,
        flow_control.registry().registered_flow(flow_id).is_some(),
        false,
    )
}

pub(super) fn classify_datagram_send_failure(
    flow_control: &FlowControl,
    flow_id: FlowId,
    error: &DatagramSendError,
) -> EstablishedDataFailureDisposition {
    match error {
        DatagramSendError::ProfileUnavailable | DatagramSendError::ConnectionLost => {
            EstablishedDataFailureDisposition::ConnectionTerminal { code: None }
        }
        DatagramSendError::AllocationFailed => known_flow_disposition(
            flow_id,
            FlowTerminateReason::ResourceFailure,
            flow_control.registry().registered_flow(flow_id).is_some(),
            false,
        ),
        DatagramSendError::SequenceExhausted => classify_datagram_sequence_exhaustion(
            flow_control,
            flow_id,
        ),
        DatagramSendError::UnknownFlowId
        | DatagramSendError::WrongDirection
        | DatagramSendError::ReliableFlow
        | DatagramSendError::Core(_)
        | DatagramSendError::Custody(_)
        | DatagramSendError::Wire(_)
        | DatagramSendError::LengthOverflow
        | DatagramSendError::ModeMismatch
        | DatagramSendError::PayloadExceedsProfile => known_flow_disposition(
            flow_id,
            FlowTerminateReason::ProtocolFailure,
            flow_control.registry().registered_flow(flow_id).is_some(),
            false,
        ),
    }
}

pub(super) fn classify_datagram_submission_error(
    flow_control: &FlowControl,
    flow_id: FlowId,
    error: &DatagramSubmissionError,
) -> Option<EstablishedDataFailureDisposition> {
    match error {
        DatagramSubmissionError::SequenceExhausted => {
            Some(classify_datagram_sequence_exhaustion(flow_control, flow_id))
        }
        DatagramSubmissionError::UnknownFlowId
        | DatagramSubmissionError::WrongDirection
        | DatagramSubmissionError::ReliableFlow
        | DatagramSubmissionError::Core(_)
        | DatagramSubmissionError::Wire(_)
        | DatagramSubmissionError::LengthOverflow
        | DatagramSubmissionError::AcceptedIndexMismatch { .. } => None,
    }
}

fn classify_datagram_sequence_exhaustion(
    flow_control: &FlowControl,
    flow_id: FlowId,
) -> EstablishedDataFailureDisposition {
    let Some(flow) = flow_control.registry().registered_flow(flow_id) else {
        return EstablishedDataFailureDisposition::CleanupOnly { flow_id };
    };
    let reason = if flow.mode() == DeliveryMode::UnreliableSequenced {
        FlowTerminateReason::Normal
    } else {
        FlowTerminateReason::ProtocolFailure
    };
    EstablishedDataFailureDisposition::TerminateAndReport { flow_id, reason }
}

fn classify_reliable_active_context(
    flow_control: &FlowControl,
    context: ReliableFailureContext,
    reason: FlowTerminateReason,
    unresolved_code: Option<ApplicationErrorCode>,
) -> EstablishedDataFailureDisposition {
    match context {
        ReliableFailureContext::Unresolved => {
            EstablishedDataFailureDisposition::ConnectionTerminal {
                code: unresolved_code,
            }
        }
        ReliableFailureContext::ResolvedDetached { flow_id } => {
            EstablishedDataFailureDisposition::CleanupOnly { flow_id }
        }
        ReliableFailureContext::ResolvedLive { flow_id } => known_flow_disposition(
            flow_id,
            reason,
            flow_control.registry().registered_flow(flow_id).is_some(),
            true,
        ),
    }
}

const fn known_flow_disposition(
    flow_id: FlowId,
    reason: FlowTerminateReason,
    live_now: bool,
    report_if_detached: bool,
) -> EstablishedDataFailureDisposition {
    if live_now {
        EstablishedDataFailureDisposition::TerminateAndReport { flow_id, reason }
    } else if report_if_detached {
        EstablishedDataFailureDisposition::ReportOnly { flow_id, reason }
    } else {
        EstablishedDataFailureDisposition::CleanupOnly { flow_id }
    }
}

fn classify_unresolved_reliable_receive(
    error: &ReceiveError,
) -> EstablishedDataFailureDisposition {
    let code = match error {
        ReceiveError::Registry(_)
        | ReceiveError::Prefix(_)
        | ReceiveError::ZeroRtt
        | ReceiveError::TruncatedAssociation => Some(ApplicationErrorCode::ProfileProtocolError),
        ReceiveError::AllocationFailed => Some(ApplicationErrorCode::ResourceLimitError),
        ReceiveError::Framing(_)
        | ReceiveError::Core(_)
        | ReceiveError::Io(_)
        | ReceiveError::AdapterStagingBelowFlowMaximum { .. }
        | ReceiveError::AcceptedIndexExhausted
        | ReceiveError::UnexpectedCoreOutcome(_)
        | ReceiveError::Terminal => None,
    };
    EstablishedDataFailureDisposition::ConnectionTerminal { code }
}

const fn reliable_send_failure_reason(error: &SendError) -> FlowTerminateReason {
    match error {
        SendError::Registry(_)
        | SendError::Framing(_)
        | SendError::UnexpectedAcceptedIndex { .. }
        | SendError::AcceptedIndexExhausted => FlowTerminateReason::ProtocolFailure,
        SendError::Core(_)
        | SendError::Custody(_)
        | SendError::InvalidWriteCount
        | SendError::WriteZero
        | SendError::Io(_)
        | SendError::PendingData
        | SendError::AlreadyFinishing
        | SendError::Terminal => FlowTerminateReason::ReliableDeliveryFailure,
    }
}

const fn reliable_receive_failure_reason(error: &ReceiveError) -> FlowTerminateReason {
    match error {
        ReceiveError::Framing(
            ReliableFrameError::StagingLimitExceeded { .. } | ReliableFrameError::AllocationFailed,
        )
        | ReceiveError::AllocationFailed
        | ReceiveError::AdapterStagingBelowFlowMaximum { .. } => {
            FlowTerminateReason::ResourceFailure
        }
        ReceiveError::Io(_)
        | ReceiveError::UnexpectedCoreOutcome(ReceiveOutcome::TerminalReliableFailure) => {
            FlowTerminateReason::ReliableDeliveryFailure
        }
        ReceiveError::Registry(_)
        | ReceiveError::Prefix(_)
        | ReceiveError::Framing(_)
        | ReceiveError::Core(_)
        | ReceiveError::ZeroRtt
        | ReceiveError::TruncatedAssociation
        | ReceiveError::AcceptedIndexExhausted
        | ReceiveError::UnexpectedCoreOutcome(_)
        | ReceiveError::Terminal => FlowTerminateReason::ProtocolFailure,
    }
}

fn pending_report_only_termination(
    flow_id: FlowId,
    reason: FlowTerminateReason,
) -> Result<PendingFlowControlSend, FlowControlError> {
    let encoded = FlowTerminate { flow_id, reason }.encode();
    let encoded = encoded.as_slice();
    let mut body = Vec::new();
    body.try_reserve_exact(encoded.len())
        .map_err(FlowControlError::Allocation)?;
    body.extend_from_slice(encoded);
    Ok(PendingFlowControlSend::new(
        ControlFrame {
            frame_type: ControlFrameType::FlowTerminate,
            body,
        },
        FlowControlSendEffect::ReportOnlyTermination { flow_id, reason },
    ))
}

fn reliable_staging_rejection(
    mode: DeliveryMode,
    max_message_bytes: u64,
    max_staging_bytes: NonZeroUsize,
) -> Option<FlowRejectReason> {
    if mode != DeliveryMode::ReliableOrdered {
        return None;
    }
    let max_staging_bytes = u64::try_from(max_staging_bytes.get()).unwrap_or(u64::MAX);
    (max_message_bytes > max_staging_bytes).then_some(FlowRejectReason::ResourceLimit)
}

fn pending_inbound_resolution(resolution: InboundResolution) -> PendingFlowControlSend {
    match resolution {
        InboundResolution::Accepted { flow, frame } => {
            PendingFlowControlSend::new(frame, FlowControlSendEffect::InboundAccepted(flow))
        }
        InboundResolution::Rejected {
            flow_id,
            reason,
            frame,
        } => PendingFlowControlSend::new(
            frame,
            FlowControlSendEffect::InboundRejected { flow_id, reason },
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        quinn_binding::{IoFailure, PrefixError, RegistryError},
        wire::{VarIntDecodeError, WireSide},
    };

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    fn flow(sequence: u64) -> FlowId {
        FlowId::new(WireSide::Client, sequence).unwrap()
    }

    fn assert_owned_send<T: Send + 'static>() {}

    #[test]
    fn owned_control_operation_futures_are_send_and_static() {
        assert_owned_send::<OwnedControlReceiveFuture>();
        assert_owned_send::<OwnedFlowControlSendFuture>();
    }

    #[test]
    fn reliable_staging_gate_only_rejects_oversized_reliable_requests() {
        assert_eq!(
            reliable_staging_rejection(DeliveryMode::ReliableOrdered, 63, nz(64)),
            None
        );
        assert_eq!(
            reliable_staging_rejection(DeliveryMode::ReliableOrdered, 64, nz(64)),
            None
        );
        assert_eq!(
            reliable_staging_rejection(DeliveryMode::ReliableOrdered, 65, nz(64)),
            Some(FlowRejectReason::ResourceLimit)
        );
        assert_eq!(
            reliable_staging_rejection(DeliveryMode::UnreliableUnordered, u64::MAX, nz(1)),
            None
        );
        assert_eq!(
            reliable_staging_rejection(DeliveryMode::UnreliableSequenced, u64::MAX, nz(1)),
            None
        );
        assert_eq!(
            reliable_staging_rejection(DeliveryMode::ReliableOrdered, u64::MAX, nz(1)),
            Some(FlowRejectReason::ResourceLimit)
        );
    }

    #[test]
    fn known_failure_disposition_distinguishes_cleanup_and_reporting() {
        let flow_id = flow(7);
        assert_eq!(
            known_flow_disposition(
                flow_id,
                FlowTerminateReason::ProtocolFailure,
                true,
                true,
            ),
            EstablishedDataFailureDisposition::TerminateAndReport {
                flow_id,
                reason: FlowTerminateReason::ProtocolFailure,
            }
        );
        assert_eq!(
            known_flow_disposition(
                flow_id,
                FlowTerminateReason::ReliableDeliveryFailure,
                false,
                true,
            ),
            EstablishedDataFailureDisposition::ReportOnly {
                flow_id,
                reason: FlowTerminateReason::ReliableDeliveryFailure,
            }
        );
        assert_eq!(
            known_flow_disposition(
                flow_id,
                FlowTerminateReason::ProtocolFailure,
                false,
                false,
            ),
            EstablishedDataFailureDisposition::CleanupOnly { flow_id }
        );
    }

    #[test]
    fn detached_reliable_failure_is_cleanup_only() {
        let flow_id = flow(8);
        assert_eq!(
            classify_reliable_active_context(
                unsafe_flow_control_placeholder(),
                ReliableFailureContext::ResolvedDetached { flow_id },
                FlowTerminateReason::ReliableDeliveryFailure,
                None,
            ),
            EstablishedDataFailureDisposition::CleanupOnly { flow_id }
        );
    }

    #[test]
    fn reliable_error_classes_preserve_protocol_resource_and_delivery_failure() {
        assert_eq!(
            reliable_send_failure_reason(&SendError::AcceptedIndexExhausted),
            FlowTerminateReason::ProtocolFailure
        );
        assert_eq!(
            reliable_send_failure_reason(&SendError::Io(IoFailure::Write)),
            FlowTerminateReason::ReliableDeliveryFailure
        );
        assert_eq!(
            reliable_receive_failure_reason(&ReceiveError::Framing(
                ReliableFrameError::AllocationFailed,
            )),
            FlowTerminateReason::ResourceFailure
        );
        assert_eq!(
            reliable_receive_failure_reason(&ReceiveError::Framing(
                ReliableFrameError::TruncatedFrame,
            )),
            FlowTerminateReason::ProtocolFailure
        );
        assert_eq!(
            reliable_receive_failure_reason(&ReceiveError::UnexpectedCoreOutcome(
                ReceiveOutcome::TerminalReliableFailure,
            )),
            FlowTerminateReason::ReliableDeliveryFailure
        );
    }

    #[test]
    fn unresolved_reliable_association_distinguishes_peer_fault_from_transport_loss() {
        assert_eq!(
            classify_unresolved_reliable_receive(&ReceiveError::Prefix(PrefixError::VarInt(
                VarIntDecodeError::NonMinimal,
            ))),
            EstablishedDataFailureDisposition::ConnectionTerminal {
                code: Some(ApplicationErrorCode::ProfileProtocolError),
            }
        );
        assert_eq!(
            classify_unresolved_reliable_receive(&ReceiveError::Registry(
                RegistryError::UnknownFlowId,
            )),
            EstablishedDataFailureDisposition::ConnectionTerminal {
                code: Some(ApplicationErrorCode::ProfileProtocolError),
            }
        );
        assert_eq!(
            classify_unresolved_reliable_receive(&ReceiveError::AllocationFailed),
            EstablishedDataFailureDisposition::ConnectionTerminal {
                code: Some(ApplicationErrorCode::ResourceLimitError),
            }
        );
        assert_eq!(
            classify_unresolved_reliable_receive(&ReceiveError::Io(IoFailure::Read)),
            EstablishedDataFailureDisposition::ConnectionTerminal { code: None }
        );
    }

    #[test]
    fn report_only_termination_uses_canonical_wire_encoder_without_core_state() {
        let flow_id = flow(9);
        let pending = pending_report_only_termination(
            flow_id,
            FlowTerminateReason::ReliableDeliveryFailure,
        )
        .unwrap();
        assert_eq!(pending.frame.frame_type, ControlFrameType::FlowTerminate);
        assert_eq!(
            FlowTerminate::decode(&pending.frame.body),
            Ok(FlowTerminate {
                flow_id,
                reason: FlowTerminateReason::ReliableDeliveryFailure,
            })
        );
        assert_eq!(
            pending.effect,
            FlowControlSendEffect::ReportOnlyTermination {
                flow_id,
                reason: FlowTerminateReason::ReliableDeliveryFailure,
            }
        );
    }

    #[test]
    fn datagram_receive_reason_is_protocol_or_resource_only_after_identity() {
        let flow_id = flow(10);
        let protocol = DatagramReceiveFailure {
            flow_id: Some(flow_id),
            error: DatagramReceiveError::WrongDirection,
        };
        let allocation = DatagramReceiveFailure {
            flow_id: Some(flow_id),
            error: DatagramReceiveError::AllocationFailed,
        };
        assert_eq!(
            datagram_receive_reason(&protocol),
            Some(FlowTerminateReason::ProtocolFailure)
        );
        assert_eq!(
            datagram_receive_reason(&allocation),
            Some(FlowTerminateReason::ResourceFailure)
        );
        let unresolved = DatagramReceiveFailure {
            flow_id: None,
            error: DatagramReceiveError::VarInt(VarIntDecodeError::NonMinimal),
        };
        assert_eq!(datagram_receive_reason(&unresolved), None);
    }

    #[test]
    fn only_datagram_sequence_error_authorizes_normal_termination() {
        assert!(matches!(
            DatagramSubmissionError::SequenceExhausted,
            DatagramSubmissionError::SequenceExhausted
        ));
        assert!(!matches!(
            DatagramSubmissionError::LengthOverflow,
            DatagramSubmissionError::SequenceExhausted
        ));
    }

    fn unsafe_flow_control_placeholder() -> &'static FlowControl {
        panic!("detached context must not inspect FlowControl")
    }

    fn datagram_receive_reason(
        failure: &DatagramReceiveFailure,
    ) -> Option<FlowTerminateReason> {
        failure.flow_id.map(|_| match failure.error {
            DatagramReceiveError::AllocationFailed => FlowTerminateReason::ResourceFailure,
            DatagramReceiveError::VarInt(_)
            | DatagramReceiveError::FlowId(_)
            | DatagramReceiveError::WrongDirection
            | DatagramReceiveError::ReliableFlow
            | DatagramReceiveError::PayloadExceedsProfile
            | DatagramReceiveError::Core(_) => FlowTerminateReason::ProtocolFailure,
        })
    }
}
