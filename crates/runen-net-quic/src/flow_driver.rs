use std::num::NonZeroUsize;

use quinn::Connection;
use runen_net::delivery::{DeliveryEndpoint, DeliveryFlowKey, DeliveryMode, FlowTermination};

use crate::{
    control::{ControlFrame, ControlSender, ProfileBootstrapError},
    flow_control::{
        EstablishedFlow, FlowControl, FlowControlError, FlowControlProgress, InboundAdmission,
        InboundAdmissionError, InboundOpenRequest, InboundResolution, LocalTermination,
        OutboundOpenError, OutboundOpenRequest, PreparedFlow, PreparedOutboundOpen,
    },
    wire::{FlowId, FlowRejectReason, FlowTerminateReason},
};

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
    reliable_staging_ceiling: NonZeroUsize,
) -> Result<PendingFlowControlSend, InboundAdmissionError> {
    if let Some(reason) = reliable_staging_rejection(
        request.mode(),
        request.max_message_bytes(),
        reliable_staging_ceiling,
    ) {
        let resolution = flow_control.reject_inbound(request, reason)?;
        return Ok(pending_inbound_resolution(resolution));
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

fn reliable_staging_rejection(
    mode: DeliveryMode,
    max_message_bytes: u64,
    reliable_staging_ceiling: NonZeroUsize,
) -> Option<FlowRejectReason> {
    if mode != DeliveryMode::ReliableOrdered {
        return None;
    }

    match usize::try_from(max_message_bytes) {
        Ok(max_message_bytes) if max_message_bytes <= reliable_staging_ceiling.get() => None,
        Ok(_) | Err(_) => Some(FlowRejectReason::ResourceLimit),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    #[test]
    fn reliable_staging_gate_is_mode_scoped_and_non_wrapping() {
        let ceiling = nz(64);

        assert_eq!(
            reliable_staging_rejection(DeliveryMode::ReliableOrdered, 63, ceiling),
            None
        );
        assert_eq!(
            reliable_staging_rejection(DeliveryMode::ReliableOrdered, 64, ceiling),
            None
        );
        assert_eq!(
            reliable_staging_rejection(DeliveryMode::ReliableOrdered, 65, ceiling),
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

        let platform_ceiling = nz(usize::MAX);
        let expected = if usize::try_from(u64::MAX).is_ok() {
            None
        } else {
            Some(FlowRejectReason::ResourceLimit)
        };
        assert_eq!(
            reliable_staging_rejection(
                DeliveryMode::ReliableOrdered,
                u64::MAX,
                platform_ceiling,
            ),
            expected
        );
    }
}
