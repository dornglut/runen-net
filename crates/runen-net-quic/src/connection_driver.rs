use std::{
    fmt,
    task::{Context, Poll},
};

use quinn::Connection;
use runen_net::{
    delivery::{DeliveryEndpoint, DeliveryFlowKey, DeliveryMode, FlowTermination},
    protocol::NegotiationManager,
};

use crate::{
    control::{
        ControlFrame, ControlFrameError, ControlFrameType, ControlReceiver, ControlSender,
        ProfileBootstrapError,
    },
    datagram::{DatagramReceiveOutcome, DatagramSubmissionError, DatagramSubmissionOutcome},
    datagram_driver::{DatagramConnectionIo, DatagramIoError, DatagramOutboundProgress},
    flow_control::{
        EstablishedFlow, FlowControl, FlowControlError, InboundAdmission, InboundAdmissionError,
        InboundOpenRequest, OutboundOpenError, OutboundOpenRequest,
    },
    flow_driver::{
        self, EstablishedDataFailureDisposition, EstablishedDataFailureProgress,
        FlowControlDriverProgress, FlowControlSendEffect, FlowControlSendError,
        OwnedControlReceiveFuture, OwnedFlowControlSendFuture, PendingFlowControlSend,
    },
    lifecycle::{
        ConnectionTeardown, EstablishedIoParts, EstablishedTeardown,
        close_for_post_profile_control_error, close_for_received_flow_control_error,
    },
    quinn_binding::{IoFailure, ReceiveError, SendError},
    reliable_driver::{
        ActiveReliableProgress, OutboundAcquisitionProgress, ReliableConnectionIo,
        ReliableEstablishedIoParts, ReliableIoError, ReliableIoStateError,
    },
    wire::{ApplicationErrorCode, FlowId, FlowRejectReason, FlowTerminateReason},
};

const DRIVER_CATEGORY_COUNT: usize = 8;
const ESTABLISHED_DATA_CLOSE_REASON: &[u8] = b"established data failed";

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum DriverPhase {
    Active,
    Terminal,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ControlSendPhase {
    Ready,
    Pending,
    Terminal,
}

enum ControlSendState {
    Ready(ControlSender),
    Pending(OwnedFlowControlSendFuture),
    Terminal,
}

impl ControlSendState {
    const fn phase(&self) -> ControlSendPhase {
        match self {
            Self::Ready(_) => ControlSendPhase::Ready,
            Self::Pending(_) => ControlSendPhase::Pending,
            Self::Terminal => ControlSendPhase::Terminal,
        }
    }
}

impl fmt::Debug for ControlSendState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Ready(_) => "Ready",
            Self::Pending(_) => "Pending",
            Self::Terminal => "Terminal",
        })
    }
}

enum ControlReceiveState {
    Pending(OwnedControlReceiveFuture),
    Deferred {
        receiver: ControlReceiver,
        frame: ControlFrame,
    },
    Terminal,
}

impl fmt::Debug for ControlReceiveState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending(_) => formatter.write_str("Pending"),
            Self::Deferred { frame, .. } => formatter
                .debug_struct("Deferred")
                .field("frame_type", &frame.frame_type)
                .finish(),
            Self::Terminal => formatter.write_str("Terminal"),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum ConnectionDriverStateError {
    Terminal,
    ControlSendBusy,
}

#[derive(Debug)]
pub(super) enum ConnectionDriverError {
    State(ConnectionDriverStateError),
    OutboundOpen(OutboundOpenError),
    InboundAdmission(InboundAdmissionError),
    DatagramSubmission(DatagramSubmissionError),
    DatagramProfileUnavailable,
    ControlReceive(ProfileBootstrapError),
    ControlSend(Box<FlowControlSendError>),
    ReceivedFlowControl(FlowControlError),
    Reliable(ReliableIoError),
    Datagram(DatagramIoError),
    FailurePreparation(FlowControlError),
}

#[derive(Debug)]
pub(super) enum InboundDecisionDriverError {
    Unavailable {
        request: InboundOpenRequest,
        error: ConnectionDriverStateError,
    },
    Driver(ConnectionDriverError),
}

#[derive(Debug)]
pub(super) enum DatagramSubmitDriverError {
    Unavailable {
        flow_id: FlowId,
        payload: Vec<u8>,
        error: ConnectionDriverStateError,
    },
    Driver(ConnectionDriverError),
}

#[derive(Debug)]
pub(super) enum KeyedDatagramSubmitError {
    Unavailable {
        payload: Vec<u8>,
        error: ConnectionDriverStateError,
    },
    UnknownFlow {
        payload: Vec<u8>,
    },
    Driver(ConnectionDriverError),
}

#[derive(Debug)]
pub(super) enum KeyedFinishError {
    State(ConnectionDriverStateError),
    UnknownFlow,
    Driver(ConnectionDriverError),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum OutboundFinishOutcome {
    Started,
    FlowFailureHandled { flow_id: FlowId },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum DatagramSubmitOutcome {
    Submitted(DatagramSubmissionOutcome),
    FlowFailureHandled { flow_id: FlowId },
}

#[derive(Debug)]
pub(super) enum EstablishedConnectionProgress {
    ControlSendStarted,
    ControlSendCompleted(FlowControlSendEffect),
    InboundOpen(InboundOpenRequest),
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
    ReliableOutboundAcquisition(OutboundAcquisitionProgress),
    ReliableInboundAcquired,
    Reliable(ActiveReliableProgress),
    DatagramOutbound(DatagramOutboundProgress),
    DatagramInbound(DatagramReceiveOutcome),
    FlowFailureHandled {
        flow_id: FlowId,
    },
}

enum DriverStep {
    None,
    Progress(EstablishedConnectionProgress),
    Error(ConnectionDriverError),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum OutboundTransportOwner {
    Reliable,
    Datagram,
}

#[must_use = "established connection driver must be polled or synchronously torn down"]
#[derive(Debug)]
pub(super) struct EstablishedConnectionDriver {
    connection: Connection,
    flow_control: FlowControl,
    reliable: ReliableConnectionIo,
    datagram: DatagramConnectionIo,
    teardown: EstablishedTeardown,
    sender: ControlSendState,
    receiver: ControlReceiveState,
    phase: DriverPhase,
    poll_cursor: usize,
}

impl ReliableEstablishedIoParts {
    pub(super) fn into_connection_driver(self) -> EstablishedConnectionDriver {
        let ReliableEstablishedIoParts {
            established,
            reliable,
        } = self;
        let EstablishedIoParts {
            connection,
            sender,
            receiver,
            flow_control,
            teardown,
        } = established;
        let datagram = DatagramConnectionIo::new(connection.clone());
        EstablishedConnectionDriver {
            connection,
            flow_control,
            reliable,
            datagram,
            teardown,
            sender: ControlSendState::Ready(sender),
            receiver: ControlReceiveState::Pending(flow_driver::receive_control_owned(receiver)),
            phase: DriverPhase::Active,
            poll_cursor: 0,
        }
    }
}

impl EstablishedConnectionDriver {
    #[cfg(test)]
    pub(super) fn send_raw_datagram_for_test(
        &self,
        datagram: Vec<u8>,
    ) -> Result<(), quinn::SendDatagramError> {
        self.connection.send_datagram(datagram.into())
    }

    pub(super) fn peer_no_error_close_observed(&self) -> bool {
        matches!(
            self.connection.close_reason(),
            Some(quinn::ConnectionError::ApplicationClosed(close))
                if close.error_code == ApplicationErrorCode::NoError.quinn()
        )
    }

    pub(super) fn poll_step(
        &mut self,
        cx: &mut Context<'_>,
        endpoint: &mut DeliveryEndpoint,
    ) -> Poll<Result<EstablishedConnectionProgress, ConnectionDriverError>> {
        if self.phase == DriverPhase::Terminal {
            return Poll::Ready(Err(ConnectionDriverError::State(
                ConnectionDriverStateError::Terminal,
            )));
        }

        let start = self.poll_cursor % DRIVER_CATEGORY_COUNT;
        for offset in 0..DRIVER_CATEGORY_COUNT {
            let index = (start + offset) % DRIVER_CATEGORY_COUNT;
            let step = match index {
                0 => self.poll_control_send(cx),
                1 => self.poll_control_receive(cx, endpoint),
                2 if self.data_poll_allowed() => {
                    self.poll_reliable_outbound_acquisition(cx, endpoint)
                }
                3 if self.data_poll_allowed() => {
                    self.poll_reliable_inbound_acquisition(cx, endpoint)
                }
                4 if self.data_poll_allowed() => self.poll_reliable_outbound(cx, endpoint),
                5 if self.data_poll_allowed() => self.poll_reliable_inbound(cx, endpoint),
                6 if self.data_poll_allowed() => self.drive_datagram_outbound(endpoint),
                7 if self.data_poll_allowed() => self.poll_datagram_inbound(cx, endpoint),
                2..=7 => DriverStep::None,
                _ => unreachable!("established driver category is bounded"),
            };
            match step {
                DriverStep::None => {}
                DriverStep::Progress(progress) => {
                    self.poll_cursor = next_poll_cursor(index);
                    return Poll::Ready(Ok(progress));
                }
                DriverStep::Error(error) => {
                    self.poll_cursor = next_poll_cursor(index);
                    return Poll::Ready(Err(error));
                }
            }
        }

        self.poll_cursor = next_poll_cursor(start);
        Poll::Pending
    }

    pub(super) fn open_outbound(
        &mut self,
        endpoint: &DeliveryEndpoint,
        request: OutboundOpenRequest,
    ) -> Result<(), ConnectionDriverError> {
        self.require_control_send_ready()
            .map_err(ConnectionDriverError::State)?;
        let pending = match flow_driver::prepare_outbound_open(
            &self.connection,
            &mut self.flow_control,
            endpoint,
            request,
        ) {
            Ok(pending) => pending,
            Err(error) => {
                if outbound_open_error_is_connection_terminal(&error) {
                    self.enter_terminal(None);
                }
                return Err(ConnectionDriverError::OutboundOpen(error));
            }
        };
        self.start_prepared_send(pending)
    }

    pub(super) fn accept_inbound(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        request: InboundOpenRequest,
        admission: InboundAdmission,
    ) -> Result<(), InboundDecisionDriverError> {
        if let Err(error) = self.require_control_send_ready() {
            return Err(InboundDecisionDriverError::Unavailable { request, error });
        }
        let pending = match flow_driver::accept_inbound(
            &mut self.flow_control,
            endpoint,
            request,
            admission,
            self.reliable.max_staging_bytes(),
        ) {
            Ok(pending) => pending,
            Err(error) => {
                if inbound_admission_error_is_connection_terminal(&error) {
                    self.enter_terminal(None);
                }
                return Err(InboundDecisionDriverError::Driver(
                    ConnectionDriverError::InboundAdmission(error),
                ));
            }
        };
        self.start_prepared_send(pending)
            .map_err(InboundDecisionDriverError::Driver)
    }

    pub(super) fn reject_inbound(
        &mut self,
        request: InboundOpenRequest,
        reason: FlowRejectReason,
    ) -> Result<(), InboundDecisionDriverError> {
        if let Err(error) = self.require_control_send_ready() {
            return Err(InboundDecisionDriverError::Unavailable { request, error });
        }
        let pending = match flow_driver::reject_inbound(&mut self.flow_control, request, reason) {
            Ok(pending) => pending,
            Err(error) => {
                if inbound_admission_error_is_connection_terminal(&error) {
                    self.enter_terminal(None);
                }
                return Err(InboundDecisionDriverError::Driver(
                    ConnectionDriverError::InboundAdmission(error),
                ));
            }
        };
        self.start_prepared_send(pending)
            .map_err(InboundDecisionDriverError::Driver)
    }

    pub(super) fn has_reliable_outbound_flow(&self, key: DeliveryFlowKey) -> bool {
        self.reliable
            .outbound_flow_id(&self.flow_control, key)
            .is_some()
    }

    pub(super) fn request_outbound_finish_normal_by_key(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        key: DeliveryFlowKey,
        mode: DeliveryMode,
    ) -> Result<OutboundFinishOutcome, KeyedFinishError> {
        self.require_control_send_ready()
            .map_err(KeyedFinishError::State)?;
        match mode {
            DeliveryMode::ReliableOrdered => {
                let flow_id = self
                    .reliable
                    .outbound_flow_id(&self.flow_control, key)
                    .ok_or(KeyedFinishError::UnknownFlow)?;
                self.request_outbound_finish_normal(endpoint, flow_id)
                    .map_err(KeyedFinishError::Driver)
            }
            DeliveryMode::UnreliableUnordered | DeliveryMode::UnreliableSequenced => {
                let flow_id = self
                    .datagram
                    .outbound_flow_id(&self.flow_control, key)
                    .ok_or(KeyedFinishError::UnknownFlow)?;
                let pending = flow_driver::terminate_local(
                    &mut self.flow_control,
                    endpoint,
                    flow_id,
                    FlowTerminateReason::Normal,
                )
                .map_err(ConnectionDriverError::FailurePreparation)
                .map_err(KeyedFinishError::Driver)?;
                self.start_prepared_send(pending)
                    .map_err(KeyedFinishError::Driver)?;
                Ok(OutboundFinishOutcome::Started)
            }
        }
    }

    pub(super) fn request_outbound_finish_normal(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        flow_id: FlowId,
    ) -> Result<OutboundFinishOutcome, ConnectionDriverError> {
        self.require_control_send_ready()
            .map_err(ConnectionDriverError::State)?;
        let error = match self
            .reliable
            .request_outbound_finish_normal(endpoint, flow_id)
        {
            Ok(()) => return Ok(OutboundFinishOutcome::Started),
            Err(error) => error,
        };

        match &error {
            ReliableIoError::State(
                ReliableIoStateError::OutboundAcquisitionPending
                | ReliableIoStateError::UnknownOutboundFlow,
            ) => Err(ConnectionDriverError::Reliable(error)),
            ReliableIoError::OutboundFinish {
                flow_id,
                error: send_error,
            } => {
                let Some(disposition) = flow_driver::classify_outbound_reliable_finish_failure(
                    &self.flow_control,
                    *flow_id,
                    send_error,
                ) else {
                    return Err(ConnectionDriverError::Reliable(error));
                };
                let flow_id = self.apply_data_failure(
                    endpoint,
                    disposition,
                    ConnectionDriverError::Reliable(error),
                )?;
                Ok(OutboundFinishOutcome::FlowFailureHandled { flow_id })
            }
            _ => {
                self.enter_terminal(None);
                Err(ConnectionDriverError::Reliable(error))
            }
        }
    }

    pub(super) fn submit_unreliable_by_key(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        key: DeliveryFlowKey,
        payload: Vec<u8>,
    ) -> Result<DatagramSubmitOutcome, KeyedDatagramSubmitError> {
        let Some(flow_id) = self.datagram.outbound_flow_id(&self.flow_control, key) else {
            return Err(KeyedDatagramSubmitError::UnknownFlow { payload });
        };
        match self.submit_unreliable(endpoint, flow_id, payload) {
            Ok(outcome) => Ok(outcome),
            Err(DatagramSubmitDriverError::Unavailable { payload, error, .. }) => {
                Err(KeyedDatagramSubmitError::Unavailable { payload, error })
            }
            Err(DatagramSubmitDriverError::Driver(error)) => {
                Err(KeyedDatagramSubmitError::Driver(error))
            }
        }
    }

    pub(super) fn submit_unreliable(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        flow_id: FlowId,
        payload: Vec<u8>,
    ) -> Result<DatagramSubmitOutcome, DatagramSubmitDriverError> {
        if let Err(error) = self.require_control_send_ready() {
            return Err(DatagramSubmitDriverError::Unavailable {
                flow_id,
                payload,
                error,
            });
        }
        match self
            .datagram
            .submit(endpoint, &self.flow_control, flow_id, payload)
        {
            Ok(outcome) if datagram_submission_outcome_is_connection_terminal(outcome) => {
                self.enter_terminal(None);
                Err(DatagramSubmitDriverError::Driver(
                    ConnectionDriverError::DatagramProfileUnavailable,
                ))
            }
            Ok(outcome) => Ok(DatagramSubmitOutcome::Submitted(outcome)),
            Err(error) => {
                let disposition = flow_driver::classify_datagram_submission_error(
                    &self.flow_control,
                    flow_id,
                    &error,
                );
                if let Some(disposition) = disposition {
                    let flow_id = self
                        .apply_data_failure(
                            endpoint,
                            disposition,
                            ConnectionDriverError::DatagramSubmission(error),
                        )
                        .map_err(DatagramSubmitDriverError::Driver)?;
                    return Ok(DatagramSubmitOutcome::FlowFailureHandled { flow_id });
                }
                if datagram_submission_error_is_connection_terminal(&error) {
                    self.enter_terminal(None);
                }
                Err(DatagramSubmitDriverError::Driver(
                    ConnectionDriverError::DatagramSubmission(error),
                ))
            }
        }
    }

    pub(super) fn teardown(
        self,
        manager: &mut NegotiationManager,
        endpoint: &mut DeliveryEndpoint,
    ) -> ConnectionTeardown {
        let Self {
            connection,
            flow_control,
            reliable,
            datagram,
            teardown,
            sender,
            receiver,
            phase: _,
            poll_cursor: _,
        } = self;
        drop((
            connection,
            flow_control,
            reliable,
            datagram,
            sender,
            receiver,
        ));
        teardown.teardown(manager, endpoint)
    }

    fn poll_control_send(&mut self, cx: &mut Context<'_>) -> DriverStep {
        let polled = match &mut self.sender {
            ControlSendState::Pending(future) => Some(future.as_mut().poll(cx)),
            ControlSendState::Ready(_) | ControlSendState::Terminal => None,
        };
        let Some(polled) = polled else {
            return DriverStep::None;
        };
        let Poll::Ready(completion) = polled else {
            return DriverStep::None;
        };
        let flow_driver::FlowControlSendCompletion { sender, result } = completion;
        match result {
            Ok(effect) => {
                self.sender = ControlSendState::Ready(sender);
                DriverStep::Progress(EstablishedConnectionProgress::ControlSendCompleted(effect))
            }
            Err(error) => {
                close_for_post_profile_control_error(&self.connection, &error.error);
                self.enter_terminal(None);
                DriverStep::Error(ConnectionDriverError::ControlSend(error))
            }
        }
    }

    fn poll_control_receive(
        &mut self,
        cx: &mut Context<'_>,
        endpoint: &mut DeliveryEndpoint,
    ) -> DriverStep {
        let deferred_ready = match &self.receiver {
            ControlReceiveState::Deferred { frame, .. } => !should_defer_received_frame(
                frame.frame_type,
                self.flow_control.has_pending_inbound(),
                self.sender.phase(),
            ),
            ControlReceiveState::Pending(_) | ControlReceiveState::Terminal => false,
        };
        if deferred_ready {
            let state = std::mem::replace(&mut self.receiver, ControlReceiveState::Terminal);
            if let ControlReceiveState::Deferred { receiver, frame } = state {
                return self.process_received_frame(receiver, frame, endpoint);
            }
            unreachable!("deferred readiness was checked before moving receive state");
        }

        let polled = match &mut self.receiver {
            ControlReceiveState::Pending(future) => Some(future.as_mut().poll(cx)),
            ControlReceiveState::Deferred { .. } | ControlReceiveState::Terminal => None,
        };
        let Some(polled) = polled else {
            return DriverStep::None;
        };
        let Poll::Ready(completion) = polled else {
            return DriverStep::None;
        };
        let flow_driver::ControlReceiveCompletion { receiver, result } = completion;
        match result {
            Err(error) => {
                let error = self.normalize_peer_no_error_control_end(error);
                close_for_post_profile_control_error(&self.connection, &error);
                self.receiver = ControlReceiveState::Terminal;
                self.enter_terminal(None);
                DriverStep::Error(ConnectionDriverError::ControlReceive(error))
            }
            Ok(frame) => {
                if should_defer_received_frame(
                    frame.frame_type,
                    self.flow_control.has_pending_inbound(),
                    self.sender.phase(),
                ) {
                    self.receiver = ControlReceiveState::Deferred { receiver, frame };
                    DriverStep::None
                } else {
                    self.process_received_frame(receiver, frame, endpoint)
                }
            }
        }
    }

    fn normalize_peer_no_error_control_end(
        &self,
        error: ProfileBootstrapError,
    ) -> ProfileBootstrapError {
        if !matches!(
            &error,
            ProfileBootstrapError::Frame(ControlFrameError::EndOfStream)
        ) {
            return error;
        }
        let Some(close_reason) = self.connection.close_reason() else {
            return error;
        };
        if matches!(
            &close_reason,
            quinn::ConnectionError::ApplicationClosed(close)
                if close.error_code == ApplicationErrorCode::NoError.quinn()
        ) {
            ProfileBootstrapError::Connection(close_reason)
        } else {
            error
        }
    }

    fn process_received_frame(
        &mut self,
        receiver: ControlReceiver,
        frame: ControlFrame,
        endpoint: &mut DeliveryEndpoint,
    ) -> DriverStep {
        let progress = match flow_driver::process_received(&mut self.flow_control, endpoint, frame)
        {
            Ok(progress) => progress,
            Err(error) => {
                close_for_received_flow_control_error(&self.connection, &error);
                self.receiver = ControlReceiveState::Terminal;
                self.enter_terminal(None);
                return DriverStep::Error(ConnectionDriverError::ReceivedFlowControl(error));
            }
        };
        self.receiver = ControlReceiveState::Pending(flow_driver::receive_control_owned(receiver));
        self.route_flow_control_progress(progress, endpoint)
    }

    fn route_flow_control_progress(
        &mut self,
        progress: FlowControlDriverProgress,
        endpoint: &mut DeliveryEndpoint,
    ) -> DriverStep {
        match progress {
            FlowControlDriverProgress::InboundOpen(request) => {
                DriverStep::Progress(EstablishedConnectionProgress::InboundOpen(request))
            }
            FlowControlDriverProgress::PendingSend(pending) => match self
                .start_prepared_send(pending)
            {
                Ok(()) => DriverStep::Progress(EstablishedConnectionProgress::ControlSendStarted),
                Err(error) => DriverStep::Error(error),
            },
            FlowControlDriverProgress::OutboundEstablished(flow) => {
                self.route_outbound_established(flow, endpoint)
            }
            FlowControlDriverProgress::OutboundRejected {
                flow_id,
                key,
                reason,
            } => DriverStep::Progress(EstablishedConnectionProgress::OutboundRejected {
                flow_id,
                key,
                reason,
            }),
            FlowControlDriverProgress::RemoteTerminated {
                flow,
                reason,
                termination,
            } => DriverStep::Progress(EstablishedConnectionProgress::RemoteTerminated {
                flow,
                reason,
                termination,
            }),
        }
    }

    fn route_outbound_established(
        &mut self,
        flow: EstablishedFlow,
        endpoint: &mut DeliveryEndpoint,
    ) -> DriverStep {
        match outbound_transport_owner(flow.mode()) {
            OutboundTransportOwner::Reliable => {
                match self
                    .reliable
                    .schedule_outbound(&self.connection, &self.flow_control, flow)
                {
                    Ok(()) => DriverStep::Progress(
                        EstablishedConnectionProgress::OutboundEstablished(flow),
                    ),
                    Err(error @ ReliableIoError::Allocation(_)) => {
                        let disposition = flow_driver::classify_known_flow_resource_failure(
                            &self.flow_control,
                            flow.flow_id(),
                        );
                        self.finish_data_failure(
                            endpoint,
                            disposition,
                            ConnectionDriverError::Reliable(error),
                        )
                    }
                    Err(error) => {
                        self.enter_terminal(None);
                        DriverStep::Error(ConnectionDriverError::Reliable(error))
                    }
                }
            }
            OutboundTransportOwner::Datagram => {
                match self.datagram.register_outbound(&self.flow_control, flow) {
                    Ok(()) => DriverStep::Progress(
                        EstablishedConnectionProgress::OutboundEstablished(flow),
                    ),
                    Err(error @ DatagramIoError::Allocation(_)) => {
                        let disposition = flow_driver::classify_known_flow_resource_failure(
                            &self.flow_control,
                            flow.flow_id(),
                        );
                        self.finish_data_failure(
                            endpoint,
                            disposition,
                            ConnectionDriverError::Datagram(error),
                        )
                    }
                    Err(error) => {
                        self.enter_terminal(None);
                        DriverStep::Error(ConnectionDriverError::Datagram(error))
                    }
                }
            }
        }
    }

    fn poll_reliable_outbound_acquisition(
        &mut self,
        cx: &mut Context<'_>,
        endpoint: &mut DeliveryEndpoint,
    ) -> DriverStep {
        match self
            .reliable
            .poll_outbound_acquisition(cx, &mut self.flow_control)
        {
            Poll::Pending => DriverStep::None,
            Poll::Ready(Ok(progress)) => DriverStep::Progress(
                EstablishedConnectionProgress::ReliableOutboundAcquisition(progress),
            ),
            Poll::Ready(Err(error)) => match &error {
                ReliableIoError::OutboundAcquisitionBinding {
                    flow_id,
                    error: send_error,
                } => self.finish_data_failure(
                    endpoint,
                    flow_driver::classify_outbound_reliable_acquisition_failure(
                        &self.flow_control,
                        *flow_id,
                        send_error,
                    ),
                    ConnectionDriverError::Reliable(error),
                ),
                _ => {
                    self.enter_terminal(None);
                    DriverStep::Error(ConnectionDriverError::Reliable(error))
                }
            },
        }
    }

    fn poll_reliable_inbound_acquisition(
        &mut self,
        cx: &mut Context<'_>,
        endpoint: &mut DeliveryEndpoint,
    ) -> DriverStep {
        match self.reliable.poll_inbound_acquisition(cx, &self.connection) {
            Poll::Pending => DriverStep::None,
            Poll::Ready(Ok(())) => {
                DriverStep::Progress(EstablishedConnectionProgress::ReliableInboundAcquired)
            }
            Poll::Ready(Err(error)) => {
                let disposition = match &error {
                    ReliableIoError::InboundConstruction(receive_error) => Some(
                        flow_driver::classify_inbound_reliable_construction_failure(receive_error),
                    ),
                    ReliableIoError::Allocation(_) => {
                        Some(EstablishedDataFailureDisposition::ConnectionTerminal {
                            code: Some(ApplicationErrorCode::ResourceLimitError),
                        })
                    }
                    ReliableIoError::Connection(_) => {
                        Some(EstablishedDataFailureDisposition::ConnectionTerminal { code: None })
                    }
                    _ => None,
                };
                let Some(disposition) = disposition else {
                    self.enter_terminal(None);
                    return DriverStep::Error(ConnectionDriverError::Reliable(error));
                };
                self.finish_data_failure(
                    endpoint,
                    disposition,
                    ConnectionDriverError::Reliable(error),
                )
            }
        }
    }

    fn poll_reliable_outbound(
        &mut self,
        cx: &mut Context<'_>,
        endpoint: &mut DeliveryEndpoint,
    ) -> DriverStep {
        match self
            .reliable
            .poll_outbound_binding(cx, endpoint, &mut self.flow_control)
        {
            Poll::Pending => DriverStep::None,
            Poll::Ready(Ok(progress)) => {
                DriverStep::Progress(EstablishedConnectionProgress::Reliable(progress))
            }
            Poll::Ready(Err(error)) => {
                if reliable_active_error_is_connection_loss(&error) {
                    self.enter_terminal(None);
                    return DriverStep::Error(ConnectionDriverError::Reliable(error));
                }
                let disposition = match &error {
                    ReliableIoError::OutboundActiveBinding {
                        context,
                        error: send_error,
                    } => Some(flow_driver::classify_outbound_reliable_active_failure(
                        &self.flow_control,
                        *context,
                        send_error,
                    )),
                    _ => None,
                };
                let Some(disposition) = disposition else {
                    self.enter_terminal(None);
                    return DriverStep::Error(ConnectionDriverError::Reliable(error));
                };
                self.finish_data_failure(
                    endpoint,
                    disposition,
                    ConnectionDriverError::Reliable(error),
                )
            }
        }
    }

    fn poll_reliable_inbound(
        &mut self,
        cx: &mut Context<'_>,
        endpoint: &mut DeliveryEndpoint,
    ) -> DriverStep {
        match self
            .reliable
            .poll_inbound_binding(cx, endpoint, &mut self.flow_control)
        {
            Poll::Pending => DriverStep::None,
            Poll::Ready(Ok(progress)) => {
                DriverStep::Progress(EstablishedConnectionProgress::Reliable(progress))
            }
            Poll::Ready(Err(error)) => {
                if reliable_active_error_is_connection_loss(&error) {
                    self.enter_terminal(None);
                    return DriverStep::Error(ConnectionDriverError::Reliable(error));
                }
                let disposition = match &error {
                    ReliableIoError::InboundActiveBinding {
                        context,
                        error: receive_error,
                    } => Some(flow_driver::classify_inbound_reliable_active_failure(
                        &self.flow_control,
                        *context,
                        receive_error,
                    )),
                    _ => None,
                };
                let Some(disposition) = disposition else {
                    self.enter_terminal(None);
                    return DriverStep::Error(ConnectionDriverError::Reliable(error));
                };
                self.finish_data_failure(
                    endpoint,
                    disposition,
                    ConnectionDriverError::Reliable(error),
                )
            }
        }
    }

    fn drive_datagram_outbound(&mut self, endpoint: &mut DeliveryEndpoint) -> DriverStep {
        match self.datagram.drive_outbound(endpoint, &self.flow_control) {
            Ok(None) => DriverStep::None,
            Ok(Some(progress)) => {
                DriverStep::Progress(EstablishedConnectionProgress::DatagramOutbound(progress))
            }
            Err(error) => match &error {
                DatagramIoError::Send {
                    flow_id,
                    error: send_error,
                } => self.finish_data_failure(
                    endpoint,
                    flow_driver::classify_datagram_send_failure(
                        &self.flow_control,
                        *flow_id,
                        send_error,
                    ),
                    ConnectionDriverError::Datagram(error),
                ),
                _ => {
                    self.enter_terminal(None);
                    DriverStep::Error(ConnectionDriverError::Datagram(error))
                }
            },
        }
    }

    fn poll_datagram_inbound(
        &mut self,
        cx: &mut Context<'_>,
        endpoint: &mut DeliveryEndpoint,
    ) -> DriverStep {
        match self
            .datagram
            .poll_inbound(cx, &self.connection, endpoint, &self.flow_control)
        {
            Poll::Pending => DriverStep::None,
            Poll::Ready(Ok(outcome)) => {
                DriverStep::Progress(EstablishedConnectionProgress::DatagramInbound(outcome))
            }
            Poll::Ready(Err(error)) => match &error {
                DatagramIoError::Receive(failure) => self.finish_data_failure(
                    endpoint,
                    flow_driver::classify_datagram_receive_failure(&self.flow_control, failure),
                    ConnectionDriverError::Datagram(error),
                ),
                _ => {
                    self.enter_terminal(None);
                    DriverStep::Error(ConnectionDriverError::Datagram(error))
                }
            },
        }
    }

    fn finish_data_failure(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        disposition: EstablishedDataFailureDisposition,
        terminal_error: ConnectionDriverError,
    ) -> DriverStep {
        match self.apply_data_failure(endpoint, disposition, terminal_error) {
            Ok(flow_id) => {
                DriverStep::Progress(EstablishedConnectionProgress::FlowFailureHandled { flow_id })
            }
            Err(error) => DriverStep::Error(error),
        }
    }

    fn apply_data_failure(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        disposition: EstablishedDataFailureDisposition,
        terminal_error: ConnectionDriverError,
    ) -> Result<FlowId, ConnectionDriverError> {
        let flow_id = match disposition {
            EstablishedDataFailureDisposition::TerminateAndReport { flow_id, .. }
            | EstablishedDataFailureDisposition::ReportOnly { flow_id, .. }
            | EstablishedDataFailureDisposition::CleanupOnly { flow_id } => flow_id,
            EstablishedDataFailureDisposition::ConnectionTerminal { code } => {
                self.enter_terminal(code);
                return Err(terminal_error);
            }
        };

        let progress = match flow_driver::prepare_established_data_failure(
            &mut self.flow_control,
            endpoint,
            disposition,
        ) {
            Ok(progress) => progress,
            Err(error) => {
                self.enter_terminal(None);
                return Err(ConnectionDriverError::FailurePreparation(error));
            }
        };
        match progress {
            EstablishedDataFailureProgress::PendingSend(pending) => {
                self.start_prepared_send(pending)?;
            }
            EstablishedDataFailureProgress::CleanupOnly {
                flow_id: cleaned_flow_id,
            } => {
                debug_assert_eq!(cleaned_flow_id, flow_id);
            }
            EstablishedDataFailureProgress::ConnectionTerminal { code } => {
                self.enter_terminal(code);
                return Err(terminal_error);
            }
        }
        Ok(flow_id)
    }

    fn start_prepared_send(
        &mut self,
        pending: PendingFlowControlSend,
    ) -> Result<(), ConnectionDriverError> {
        if let Err(error) = self.start_control_send(pending) {
            self.enter_terminal(None);
            return Err(ConnectionDriverError::State(error));
        }
        Ok(())
    }

    fn start_control_send(
        &mut self,
        pending: PendingFlowControlSend,
    ) -> Result<(), ConnectionDriverStateError> {
        if self.phase == DriverPhase::Terminal {
            return Err(ConnectionDriverStateError::Terminal);
        }
        let state = std::mem::replace(&mut self.sender, ControlSendState::Terminal);
        match state {
            ControlSendState::Ready(sender) => {
                self.sender = ControlSendState::Pending(pending.into_owned_send(sender));
                Ok(())
            }
            ControlSendState::Pending(future) => {
                self.sender = ControlSendState::Pending(future);
                Err(ConnectionDriverStateError::ControlSendBusy)
            }
            ControlSendState::Terminal => {
                self.sender = ControlSendState::Terminal;
                Err(ConnectionDriverStateError::Terminal)
            }
        }
    }

    fn require_control_send_ready(&self) -> Result<(), ConnectionDriverStateError> {
        control_operation_error(self.phase, self.sender.phase()).map_or(Ok(()), Err)
    }

    fn data_poll_allowed(&self) -> bool {
        self.phase == DriverPhase::Active && self.sender.phase() == ControlSendPhase::Ready
    }

    fn enter_terminal(&mut self, code: Option<ApplicationErrorCode>) {
        if self.phase == DriverPhase::Terminal {
            return;
        }
        if let Some(code) = code {
            self.connection
                .close(code.quinn(), ESTABLISHED_DATA_CLOSE_REASON);
        }
        self.phase = DriverPhase::Terminal;
        self.sender = ControlSendState::Terminal;
        self.receiver = ControlReceiveState::Terminal;
    }
}

fn reliable_active_error_is_connection_loss(error: &ReliableIoError) -> bool {
    matches!(
        error,
        ReliableIoError::OutboundActiveBinding {
            error: SendError::Io(IoFailure::ConnectionLost),
            ..
        } | ReliableIoError::InboundActiveBinding {
            error: ReceiveError::Io(IoFailure::ConnectionLost),
            ..
        }
    )
}

const fn control_operation_error(
    phase: DriverPhase,
    sender: ControlSendPhase,
) -> Option<ConnectionDriverStateError> {
    match (phase, sender) {
        (DriverPhase::Terminal, _) | (_, ControlSendPhase::Terminal) => {
            Some(ConnectionDriverStateError::Terminal)
        }
        (DriverPhase::Active, ControlSendPhase::Pending) => {
            Some(ConnectionDriverStateError::ControlSendBusy)
        }
        (DriverPhase::Active, ControlSendPhase::Ready) => None,
    }
}

const fn should_defer_received_frame(
    frame_type: ControlFrameType,
    pending_inbound: bool,
    sender: ControlSendPhase,
) -> bool {
    !matches!(sender, ControlSendPhase::Ready)
        || (pending_inbound && matches!(frame_type, ControlFrameType::OpenFlow))
}

const fn outbound_transport_owner(mode: DeliveryMode) -> OutboundTransportOwner {
    match mode {
        DeliveryMode::ReliableOrdered => OutboundTransportOwner::Reliable,
        DeliveryMode::UnreliableUnordered | DeliveryMode::UnreliableSequenced => {
            OutboundTransportOwner::Datagram
        }
    }
}

fn outbound_open_error_is_connection_terminal(error: &OutboundOpenError) -> bool {
    matches!(error, OutboundOpenError::DatagramUnavailable)
}

fn inbound_admission_error_is_connection_terminal(error: &InboundAdmissionError) -> bool {
    !matches!(error, InboundAdmissionError::RequestNotPending(_))
}

const fn datagram_submission_outcome_is_connection_terminal(
    outcome: DatagramSubmissionOutcome,
) -> bool {
    matches!(
        outcome,
        DatagramSubmissionOutcome::RejectedTransportUnavailable
    )
}

fn datagram_submission_error_is_connection_terminal(error: &DatagramSubmissionError) -> bool {
    matches!(
        error,
        DatagramSubmissionError::Core(_)
            | DatagramSubmissionError::Wire(_)
            | DatagramSubmissionError::LengthOverflow
            | DatagramSubmissionError::AcceptedIndexMismatch { .. }
    )
}

const fn next_poll_cursor(current: usize) -> usize {
    (current + 1) % DRIVER_CATEGORY_COUNT
}

#[cfg(test)]
mod tests {
    use runen_net::delivery::{DeliveryOperationError, FlowDirection};

    use super::*;
    use crate::{reliable_driver::ReliableFailureContext, wire::WireSide};

    fn assert_static<T: 'static>() {}

    fn flow(sequence: u64) -> FlowId {
        FlowId::new(WireSide::Client, sequence).unwrap()
    }

    #[test]
    fn driver_and_control_states_are_move_owned() {
        assert_static::<EstablishedConnectionDriver>();
        assert_static::<ControlSendState>();
        assert_static::<ControlReceiveState>();
    }

    #[test]
    fn reliable_connection_loss_is_connection_terminal_before_flow_failure_routing() {
        let flow_id = flow(1);
        assert!(reliable_active_error_is_connection_loss(
            &ReliableIoError::OutboundActiveBinding {
                context: ReliableFailureContext::Unresolved,
                error: SendError::Io(IoFailure::ConnectionLost),
            }
        ));
        assert!(reliable_active_error_is_connection_loss(
            &ReliableIoError::InboundActiveBinding {
                context: ReliableFailureContext::Unresolved,
                error: ReceiveError::Io(IoFailure::ConnectionLost),
            }
        ));
        assert!(!reliable_active_error_is_connection_loss(
            &ReliableIoError::OutboundActiveBinding {
                context: ReliableFailureContext::ResolvedDetached {
                    flow_id,
                    key: DeliveryFlowKey::new(
                        runen_net::identity::ConnectionHandle::new(1),
                        FlowDirection::Outbound,
                        runen_net::delivery::DeliveryFlowHandle::new(1),
                    ),
                },
                error: SendError::Io(IoFailure::Write),
            }
        ));
    }

    #[test]
    fn report_capable_operations_require_the_sole_ready_sender() {
        assert_eq!(
            control_operation_error(DriverPhase::Active, ControlSendPhase::Ready),
            None
        );
        assert_eq!(
            control_operation_error(DriverPhase::Active, ControlSendPhase::Pending),
            Some(ConnectionDriverStateError::ControlSendBusy)
        );
        assert_eq!(
            control_operation_error(DriverPhase::Terminal, ControlSendPhase::Ready),
            Some(ConnectionDriverStateError::Terminal)
        );
        assert_eq!(
            control_operation_error(DriverPhase::Active, ControlSendPhase::Terminal),
            Some(ConnectionDriverStateError::Terminal)
        );
    }

    #[test]
    fn control_receive_defers_only_for_sender_backpressure_or_second_open() {
        assert!(should_defer_received_frame(
            ControlFrameType::FlowAccept,
            false,
            ControlSendPhase::Pending,
        ));
        assert!(should_defer_received_frame(
            ControlFrameType::OpenFlow,
            true,
            ControlSendPhase::Ready,
        ));
        assert!(!should_defer_received_frame(
            ControlFrameType::FlowTerminate,
            true,
            ControlSendPhase::Ready,
        ));
        assert!(!should_defer_received_frame(
            ControlFrameType::OpenFlow,
            false,
            ControlSendPhase::Ready,
        ));
    }

    #[test]
    fn outbound_mode_routes_to_exactly_one_transport_owner() {
        assert_eq!(
            outbound_transport_owner(DeliveryMode::ReliableOrdered),
            OutboundTransportOwner::Reliable
        );
        assert_eq!(
            outbound_transport_owner(DeliveryMode::UnreliableUnordered),
            OutboundTransportOwner::Datagram
        );
        assert_eq!(
            outbound_transport_owner(DeliveryMode::UnreliableSequenced),
            OutboundTransportOwner::Datagram
        );
    }

    #[test]
    fn post_profile_datagram_capability_loss_is_connection_terminal() {
        assert!(outbound_open_error_is_connection_terminal(
            &OutboundOpenError::DatagramUnavailable
        ));
        assert!(datagram_submission_outcome_is_connection_terminal(
            DatagramSubmissionOutcome::RejectedTransportUnavailable
        ));
        assert!(!datagram_submission_outcome_is_connection_terminal(
            DatagramSubmissionOutcome::RejectedCurrentDatagramSize
        ));
    }

    #[test]
    fn consumed_inbound_decision_failure_is_terminal_but_stale_request_is_not() {
        let flow_id = flow(0);
        assert!(!inbound_admission_error_is_connection_terminal(
            &InboundAdmissionError::RequestNotPending(flow_id)
        ));
        assert!(inbound_admission_error_is_connection_terminal(
            &InboundAdmissionError::WrongDirection(FlowDirection::Inbound)
        ));
    }

    #[test]
    fn unreliable_submission_distinguishes_host_misuse_from_driver_invariants() {
        assert!(!datagram_submission_error_is_connection_terminal(
            &DatagramSubmissionError::UnknownFlowId
        ));
        assert!(!datagram_submission_error_is_connection_terminal(
            &DatagramSubmissionError::WrongDirection
        ));
        assert!(!datagram_submission_error_is_connection_terminal(
            &DatagramSubmissionError::ReliableFlow
        ));
        assert!(datagram_submission_error_is_connection_terminal(
            &DatagramSubmissionError::Core(DeliveryOperationError::UnknownFlow)
        ));
        assert!(datagram_submission_error_is_connection_terminal(
            &DatagramSubmissionError::LengthOverflow
        ));
        assert!(datagram_submission_error_is_connection_terminal(
            &DatagramSubmissionError::AcceptedIndexMismatch {
                expected: 1,
                accepted: 2,
            }
        ));
    }

    #[test]
    fn cross_direction_cursor_is_finite_and_bounded() {
        for current in 0..DRIVER_CATEGORY_COUNT {
            assert!(next_poll_cursor(current) < DRIVER_CATEGORY_COUNT);
        }
        assert_eq!(next_poll_cursor(DRIVER_CATEGORY_COUNT - 1), 0);
    }
}
