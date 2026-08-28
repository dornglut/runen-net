use std::{
    fmt,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};

use quinn::Connection as QuinnConnection;
use runen_net::{
    delivery::{
        DeliveryEndpoint, DeliveryFlowKey, DeliveryMode, DeliveryOperationError, FlowDirection,
        FlowTermination,
    },
    identity::ConnectionHandle,
    protocol::{
        CompatibilityOffer, NegotiatedContract, NegotiationManager, NegotiationRequirements,
    },
};

use crate::{
    connection_driver::{
        ConnectionDriverError, ConnectionDriverStateError, DatagramSubmitOutcome,
        EstablishedConnectionDriver, InboundDecisionDriverError, KeyedDatagramSubmitError,
        KeyedFinishError, OutboundFinishOutcome,
    },
    control::{
        ControlFrame, ControlFrameType, ControlReceiver, ControlSender, ProfileBootstrapError,
        ProfileReadyParts, Settings, ValidatedControlProfile,
    },
    datagram::DatagramSubmissionOutcome,
    endpoint::ConnectionSlotPermit,
    facade::{ProfileBootstrapFailure, ProfileReadyConnection},
    flow_control::{
        FlowControlError, InboundAdmission, InboundAdmissionError, OutboundOpenError,
        OutboundOpenRequest,
    },
    lifecycle::{
        AdmittedProfileReadyConnection, EstablishedNegotiatedConnection,
        close_for_post_profile_control_error, close_negotiation_failed,
        close_negotiation_protocol_error, teardown_connection,
    },
    negotiation::{
        NegotiationControlError, NegotiationExchange, NegotiationOutcome, NegotiationProgress,
    },
    public_flow::{
        FlowCommandError, FlowRejectionReason, InboundFlowConfig, IncomingFlowDecisionError,
        IncomingFlowRequest, OutboundFlowConfig, SubmissionError, SubmitOutcome,
    },
    quinn_binding::SendError,
    reliable_driver::{ReliableIoError, ReliableIoStateError},
    wire::WireSide,
};

const MAX_INTERNAL_TRANSITIONS_PER_POLL: usize = 16;

/// Explicit finite reliable receive resources retained for established delivery activation.
///
/// RN6 provides no implicit defaults. These values are retained through negotiation and
/// consumed unchanged when the established reliable transport owner is activated.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ReliableReceiveLimits {
    pub scratch_bytes: NonZeroUsize,
    pub max_staging_bytes: NonZeroUsize,
}

/// Stable semantic compatibility-failure categories exposed by the public connection API.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum NegotiationFailure {
    MalformedOffer,
    ProtocolIncompatible,
    RequiredCapabilityUnavailable,
    RequiredSchemaUnavailable,
    ResourceLimitExceeded,
    InvalidSelection,
}

impl From<NegotiationOutcome> for NegotiationFailure {
    fn from(outcome: NegotiationOutcome) -> Self {
        match outcome {
            NegotiationOutcome::MalformedOffer => Self::MalformedOffer,
            NegotiationOutcome::ProtocolIncompatible => Self::ProtocolIncompatible,
            NegotiationOutcome::RequiredCapabilityUnavailable => {
                Self::RequiredCapabilityUnavailable
            }
            NegotiationOutcome::RequiredSchemaUnavailable => Self::RequiredSchemaUnavailable,
            NegotiationOutcome::ResourceLimitExceeded => Self::ResourceLimitExceeded,
            NegotiationOutcome::InvalidSelection => Self::InvalidSelection,
        }
    }
}

/// Status of the best-effort semantic failure report for a local negotiation failure.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum NegotiationReportStatus {
    Unavailable,
    Sent,
    Failed(ProfileBootstrapFailure),
}

/// Invalid host operation for the current public connection state.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ConnectionStateError {
    AuthoritySelectionNotRequired,
    Terminal,
}

/// Public post-ProfileReady connection failures.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ConnectionError {
    LocalNegotiation {
        outcome: NegotiationFailure,
        report: NegotiationReportStatus,
    },
    RemoteNegotiation(NegotiationFailure),
    NegotiationProtocol,
    Control {
        failure: ProfileBootstrapFailure,
        cleanup_failed: bool,
    },
    ManagerState,
    UnexpectedCoreState,
    EstablishedActivation,
    State(ConnectionStateError),
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "RunenNet connection progression failed: {self:?}"
        )
    }
}

impl std::error::Error for ConnectionError {}

/// Public connection progress that requires host observation or action.
#[non_exhaustive]
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ConnectionEvent {
    AuthoritySelectionRequired { connection: ConnectionHandle },
    Established { connection: ConnectionHandle },
}

/// Categorized cleanup failure returned by consuming connection teardown.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ConnectionCleanupError {
    NegotiationManagerState,
}

/// Host-relevant evidence returned by consuming connection teardown.
#[derive(Debug)]
pub struct ConnectionTeardown {
    connection: ConnectionHandle,
    flow_terminations: Vec<FlowTermination>,
    cleanup_error: Option<ConnectionCleanupError>,
}

impl ConnectionTeardown {
    pub const fn connection(&self) -> ConnectionHandle {
        self.connection
    }

    pub fn flow_terminations(&self) -> &[FlowTermination] {
        &self.flow_terminations
    }

    pub const fn cleanup_error(&self) -> Option<ConnectionCleanupError> {
        self.cleanup_error
    }
}

impl From<crate::lifecycle::ConnectionTeardown> for ConnectionTeardown {
    fn from(teardown: crate::lifecycle::ConnectionTeardown) -> Self {
        Self {
            connection: teardown.connection,
            flow_terminations: teardown.flow_terminations,
            cleanup_error: teardown
                .negotiation_cleanup_error
                .map(|_| ConnectionCleanupError::NegotiationManagerState),
        }
    }
}

type OwnedNegotiationSendFuture =
    Pin<Box<dyn Future<Output = NegotiationSendCompletion> + Send + 'static>>;
type OwnedNegotiationReceiveFuture =
    Pin<Box<dyn Future<Output = NegotiationReceiveCompletion> + Send + 'static>>;

struct NegotiationSendCompletion {
    sender: ControlSender,
    result: Result<(), ProfileBootstrapError>,
}

struct NegotiationReceiveCompletion {
    receiver: ControlReceiver,
    result: Result<ControlFrame, ProfileBootstrapError>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum PendingSendDisposition {
    Continue,
    Establish,
    TerminalLocalFailure(NegotiationOutcome),
}

struct ProfileMetadata {
    connection: QuinnConnection,
    side: WireSide,
    profile: ValidatedControlProfile,
    peer_settings: Settings,
}

struct NegotiationCore {
    connection: ConnectionHandle,
    profile: ProfileMetadata,
    connection_permit: ConnectionSlotPermit,
    exchange: NegotiationExchange,
    requirements: NegotiationRequirements,
}

impl NegotiationCore {
    fn into_established(
        self,
        sender: ControlSender,
        receiver: ControlReceiver,
    ) -> EstablishedNegotiatedConnection {
        let Self {
            connection,
            profile,
            connection_permit,
            exchange,
            requirements: _,
        } = self;
        let ProfileMetadata {
            connection: transport_connection,
            side,
            profile,
            peer_settings,
        } = profile;
        EstablishedNegotiatedConnection::from_parts(
            connection,
            ProfileReadyParts {
                connection: transport_connection,
                side,
                profile,
                peer_settings,
                sender,
                receiver,
            },
            connection_permit,
            exchange,
        )
    }

    fn teardown(
        self,
        manager: &mut NegotiationManager,
        delivery: &mut DeliveryEndpoint,
        close_unfinished: bool,
    ) -> ConnectionTeardown {
        let Self {
            connection,
            profile,
            connection_permit,
            exchange,
            requirements: _,
        } = self;
        if close_unfinished {
            close_negotiation_failed(&profile.connection);
        }
        let teardown = teardown_connection(connection, exchange, manager, delivery);
        drop((profile, connection_permit));
        teardown.into()
    }
}

enum ConnectionState {
    Sending {
        core: NegotiationCore,
        receiver: ControlReceiver,
        future: OwnedNegotiationSendFuture,
        disposition: PendingSendDisposition,
    },
    Receiving {
        core: NegotiationCore,
        sender: ControlSender,
        future: OwnedNegotiationReceiveFuture,
    },
    AuthoritySelection {
        core: NegotiationCore,
        sender: ControlSender,
        receiver: ControlReceiver,
    },
    NegotiatedEstablished {
        established: EstablishedNegotiatedConnection,
    },
    Established {
        driver: Box<EstablishedConnectionDriver>,
    },
    EstablishedActivationFailed {
        established: EstablishedNegotiatedConnection,
    },
    Failed {
        core: NegotiationCore,
    },
    Transitioning,
}

impl fmt::Debug for ConnectionState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Sending { .. } => "Sending",
            Self::Receiving { .. } => "Receiving",
            Self::AuthoritySelection { .. } => "AuthoritySelection",
            Self::NegotiatedEstablished { .. } => "NegotiatedEstablished",
            Self::Established { .. } => "Established",
            Self::EstablishedActivationFailed { .. } => "EstablishedActivationFailed",
            Self::Failed { .. } => "Failed",
            Self::Transitioning => "Transitioning",
        })
    }
}

/// One move-owned post-ProfileReady RunenNet connection.
///
/// This owner uses explicit polling after ProfileReady. It never retains
/// `NegotiationManager` or `DeliveryEndpoint` borrows across transport I/O.
#[must_use = "connection must be driven or synchronously torn down"]
pub struct Connection {
    connection: ConnectionHandle,
    reliable_receive: ReliableReceiveLimits,
    state: ConnectionState,
}

impl fmt::Debug for Connection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Connection")
            .field("connection", &self.connection)
            .field("reliable_receive", &self.reliable_receive)
            .field("state", &self.state)
            .finish()
    }
}

impl ProfileReadyConnection {
    /// Consume ProfileReady ownership and begin explicit-poll compatibility negotiation.
    pub fn activate(
        self,
        connection: ConnectionHandle,
        offer: CompatibilityOffer,
        requirements: NegotiationRequirements,
        reliable_receive: ReliableReceiveLimits,
        manager: &mut NegotiationManager,
    ) -> Result<Connection, ConnectionError> {
        Connection::activate(
            self.into_inner(),
            connection,
            offer,
            requirements,
            reliable_receive,
            manager,
        )
    }
}

impl Connection {
    fn activate(
        admitted: AdmittedProfileReadyConnection,
        connection: ConnectionHandle,
        offer: CompatibilityOffer,
        requirements: NegotiationRequirements,
        reliable_receive: ReliableReceiveLimits,
        manager: &mut NegotiationManager,
    ) -> Result<Self, ConnectionError> {
        let (profile_ready, connection_permit) = admitted.into_parts();
        let mut exchange = NegotiationExchange::from_profile(connection, &profile_ready);
        let result = exchange.prepare_offer(manager, offer);
        let ProfileReadyParts {
            connection: transport_connection,
            side,
            profile,
            peer_settings,
            sender,
            receiver,
        } = profile_ready.into_parts();
        let core = NegotiationCore {
            connection,
            profile: ProfileMetadata {
                connection: transport_connection,
                side,
                profile,
                peer_settings,
            },
            connection_permit,
            exchange,
            requirements,
        };

        let state = match transition_from_local_operation(core, sender, receiver, result) {
            LocalOperationTransition::State(state) => state,
            LocalOperationTransition::Error(_, error) => return Err(error),
        };

        Ok(Self {
            connection,
            reliable_receive,
            state,
        })
    }

    pub const fn connection_handle(&self) -> ConnectionHandle {
        self.connection
    }

    pub const fn reliable_receive_limits(&self) -> ReliableReceiveLimits {
        self.reliable_receive
    }

    /// Begin one outbound flow establishment using only the host Core flow identity.
    pub fn open_outbound_flow(
        &mut self,
        delivery: &DeliveryEndpoint,
        config: OutboundFlowConfig,
    ) -> Result<(), FlowCommandError> {
        let driver = match &mut self.state {
            ConnectionState::Established { driver } => driver,
            ConnectionState::EstablishedActivationFailed { .. }
            | ConnectionState::Failed { .. }
            | ConnectionState::Transitioning => return Err(FlowCommandError::Terminal),
            ConnectionState::Sending { .. }
            | ConnectionState::Receiving { .. }
            | ConnectionState::AuthoritySelection { .. }
            | ConnectionState::NegotiatedEstablished { .. } => {
                return Err(FlowCommandError::NotEstablished);
            }
        };
        driver
            .open_outbound(
                delivery,
                OutboundOpenRequest {
                    key: config.key,
                    mode: config.mode,
                    policy: config.policy,
                    stable_max_message_bytes: config.stable_max_message_bytes,
                    connection_limits: config.connection_limits,
                },
            )
            .map_err(map_flow_command_driver_error)
    }

    /// Accept one move-only incoming request with an explicit host Core flow identity.
    pub fn accept_incoming_flow(
        &mut self,
        delivery: &mut DeliveryEndpoint,
        request: IncomingFlowRequest,
        config: InboundFlowConfig,
    ) -> Result<(), IncomingFlowDecisionError> {
        if request.connection != self.connection || config.key.connection() != self.connection {
            return Err(IncomingFlowDecisionError::Retryable {
                request,
                reason: FlowCommandError::WrongConnection,
            });
        }
        if config.key.direction() != FlowDirection::Inbound {
            return Err(IncomingFlowDecisionError::Retryable {
                request,
                reason: FlowCommandError::WrongDirection,
            });
        }
        if config.policy.validate_for_mode(request.mode()).is_err() {
            return Err(IncomingFlowDecisionError::Retryable {
                request,
                reason: FlowCommandError::InvalidConfiguration,
            });
        }

        let driver = match &mut self.state {
            ConnectionState::Established { driver } => driver,
            ConnectionState::EstablishedActivationFailed { .. }
            | ConnectionState::Failed { .. }
            | ConnectionState::Transitioning => {
                return Err(IncomingFlowDecisionError::Failed(
                    FlowCommandError::Terminal,
                ));
            }
            ConnectionState::Sending { .. }
            | ConnectionState::Receiving { .. }
            | ConnectionState::AuthoritySelection { .. }
            | ConnectionState::NegotiatedEstablished { .. } => {
                return Err(IncomingFlowDecisionError::Retryable {
                    request,
                    reason: FlowCommandError::NotEstablished,
                });
            }
        };

        match driver.accept_inbound(
            delivery,
            request.inner,
            InboundAdmission {
                key: config.key,
                policy: config.policy,
                connection_limits: config.connection_limits,
            },
        ) {
            Ok(()) => Ok(()),
            Err(InboundDecisionDriverError::Unavailable { request, error }) => {
                let reason = map_driver_state_error(error);
                if reason == FlowCommandError::Busy {
                    Err(IncomingFlowDecisionError::Retryable {
                        request: IncomingFlowRequest {
                            connection: self.connection,
                            inner: request,
                        },
                        reason,
                    })
                } else {
                    Err(IncomingFlowDecisionError::Failed(reason))
                }
            }
            Err(InboundDecisionDriverError::Driver(error)) => Err(
                IncomingFlowDecisionError::Failed(map_inbound_decision_driver_error(&error)),
            ),
        }
    }

    /// Reject one move-only incoming request with an accepted profile rejection reason.
    pub fn reject_incoming_flow(
        &mut self,
        request: IncomingFlowRequest,
        reason: FlowRejectionReason,
    ) -> Result<(), IncomingFlowDecisionError> {
        if request.connection != self.connection {
            return Err(IncomingFlowDecisionError::Retryable {
                request,
                reason: FlowCommandError::WrongConnection,
            });
        }

        let driver = match &mut self.state {
            ConnectionState::Established { driver } => driver,
            ConnectionState::EstablishedActivationFailed { .. }
            | ConnectionState::Failed { .. }
            | ConnectionState::Transitioning => {
                return Err(IncomingFlowDecisionError::Failed(
                    FlowCommandError::Terminal,
                ));
            }
            ConnectionState::Sending { .. }
            | ConnectionState::Receiving { .. }
            | ConnectionState::AuthoritySelection { .. }
            | ConnectionState::NegotiatedEstablished { .. } => {
                return Err(IncomingFlowDecisionError::Retryable {
                    request,
                    reason: FlowCommandError::NotEstablished,
                });
            }
        };

        match driver.reject_inbound(request.inner, reason.into()) {
            Ok(()) => Ok(()),
            Err(InboundDecisionDriverError::Unavailable { request, error }) => {
                let reason = map_driver_state_error(error);
                if reason == FlowCommandError::Busy {
                    Err(IncomingFlowDecisionError::Retryable {
                        request: IncomingFlowRequest {
                            connection: self.connection,
                            inner: request,
                        },
                        reason,
                    })
                } else {
                    Err(IncomingFlowDecisionError::Failed(reason))
                }
            }
            Err(InboundDecisionDriverError::Driver(error)) => Err(
                IncomingFlowDecisionError::Failed(map_inbound_decision_driver_error(&error)),
            ),
        }
    }

    /// Submit one owned payload to an established outbound flow using only its Core key.
    pub fn submit(
        &mut self,
        delivery: &mut DeliveryEndpoint,
        key: DeliveryFlowKey,
        payload: Vec<u8>,
    ) -> Result<SubmitOutcome, SubmissionError> {
        if key.connection() != self.connection {
            return Err(SubmissionError::Retryable {
                key,
                payload,
                reason: FlowCommandError::WrongConnection,
            });
        }
        if key.direction() != FlowDirection::Outbound {
            return Err(SubmissionError::Retryable {
                key,
                payload,
                reason: FlowCommandError::WrongDirection,
            });
        }

        let driver = match &mut self.state {
            ConnectionState::Established { driver } => driver,
            ConnectionState::EstablishedActivationFailed { .. }
            | ConnectionState::Failed { .. }
            | ConnectionState::Transitioning => {
                return Err(SubmissionError::Failed(FlowCommandError::Terminal));
            }
            ConnectionState::Sending { .. }
            | ConnectionState::Receiving { .. }
            | ConnectionState::AuthoritySelection { .. }
            | ConnectionState::NegotiatedEstablished { .. } => {
                return Err(SubmissionError::Retryable {
                    key,
                    payload,
                    reason: FlowCommandError::NotEstablished,
                });
            }
        };

        let Some((mode, _)) = delivery.flow_contract(key) else {
            return Err(SubmissionError::Failed(FlowCommandError::UnknownFlow));
        };
        match mode {
            DeliveryMode::ReliableOrdered => {
                if !driver.has_reliable_outbound_flow(key) {
                    return Err(SubmissionError::Failed(FlowCommandError::UnknownFlow));
                }
                delivery
                    .submit(key, payload)
                    .map(SubmitOutcome::from)
                    .map_err(|error| SubmissionError::Failed(map_core_submission_error(error)))
            }
            DeliveryMode::UnreliableUnordered | DeliveryMode::UnreliableSequenced => {
                match driver.submit_unreliable_by_key(delivery, key, payload) {
                    Ok(outcome) => {
                        map_datagram_submit_outcome(outcome).map_err(SubmissionError::Failed)
                    }
                    Err(KeyedDatagramSubmitError::Unavailable { payload, error }) => {
                        let reason = map_driver_state_error(error);
                        if reason == FlowCommandError::Busy {
                            Err(SubmissionError::Retryable {
                                key,
                                payload,
                                reason,
                            })
                        } else {
                            Err(SubmissionError::Failed(reason))
                        }
                    }
                    Err(KeyedDatagramSubmitError::UnknownFlow { .. }) => {
                        Err(SubmissionError::Failed(FlowCommandError::UnknownFlow))
                    }
                    Err(KeyedDatagramSubmitError::Driver(error)) => Err(SubmissionError::Failed(
                        map_flow_command_driver_error(error),
                    )),
                }
            }
        }
    }

    /// Request the accepted normal sender finish for one established outbound flow.
    pub fn finish_outbound_flow_normal(
        &mut self,
        delivery: &mut DeliveryEndpoint,
        key: DeliveryFlowKey,
    ) -> Result<(), FlowCommandError> {
        if key.connection() != self.connection {
            return Err(FlowCommandError::WrongConnection);
        }
        if key.direction() != FlowDirection::Outbound {
            return Err(FlowCommandError::WrongDirection);
        }

        let driver = match &mut self.state {
            ConnectionState::Established { driver } => driver,
            ConnectionState::EstablishedActivationFailed { .. }
            | ConnectionState::Failed { .. }
            | ConnectionState::Transitioning => return Err(FlowCommandError::Terminal),
            ConnectionState::Sending { .. }
            | ConnectionState::Receiving { .. }
            | ConnectionState::AuthoritySelection { .. }
            | ConnectionState::NegotiatedEstablished { .. } => {
                return Err(FlowCommandError::NotEstablished);
            }
        };

        let Some((mode, _)) = delivery.flow_contract(key) else {
            return Err(FlowCommandError::UnknownFlow);
        };
        match driver.request_outbound_finish_normal_by_key(delivery, key, mode) {
            Ok(OutboundFinishOutcome::Started) => Ok(()),
            Ok(OutboundFinishOutcome::FlowFailureHandled { .. }) => {
                Err(FlowCommandError::FlowTerminated)
            }
            Err(KeyedFinishError::State(error)) => Err(map_driver_state_error(error)),
            Err(KeyedFinishError::UnknownFlow) => Err(FlowCommandError::UnknownFlow),
            Err(KeyedFinishError::Driver(error)) => Err(map_finish_driver_error(error)),
        }
    }

    /// Drive at most a finite amount of post-ProfileReady connection work.
    ///
    /// Aggregate Core authorities are borrowed only for this synchronous call.
    pub fn poll(
        &mut self,
        cx: &mut Context<'_>,
        manager: &mut NegotiationManager,
        _delivery: &mut DeliveryEndpoint,
    ) -> Poll<Result<ConnectionEvent, ConnectionError>> {
        for _ in 0..MAX_INTERNAL_TRANSITIONS_PER_POLL {
            let state = std::mem::replace(&mut self.state, ConnectionState::Transitioning);
            match state {
                ConnectionState::Sending {
                    mut core,
                    receiver,
                    mut future,
                    disposition,
                } => match future.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.state = ConnectionState::Sending {
                            core,
                            receiver,
                            future,
                            disposition,
                        };
                        return Poll::Pending;
                    }
                    Poll::Ready(completion) => match completion.result {
                        Ok(()) => match disposition {
                            PendingSendDisposition::Continue => {
                                self.state = receiving_state(core, completion.sender, receiver);
                            }
                            PendingSendDisposition::Establish => {
                                self.state = ConnectionState::NegotiatedEstablished {
                                    established: core.into_established(completion.sender, receiver),
                                };
                            }
                            PendingSendDisposition::TerminalLocalFailure(outcome) => {
                                close_negotiation_failed(&core.profile.connection);
                                self.state = ConnectionState::Failed { core };
                                return Poll::Ready(Err(ConnectionError::LocalNegotiation {
                                    outcome: outcome.into(),
                                    report: NegotiationReportStatus::Sent,
                                }));
                            }
                        },
                        Err(error) => {
                            if let PendingSendDisposition::TerminalLocalFailure(outcome) =
                                disposition
                            {
                                close_negotiation_failed(&core.profile.connection);
                                self.state = ConnectionState::Failed { core };
                                return Poll::Ready(Err(ConnectionError::LocalNegotiation {
                                    outcome: outcome.into(),
                                    report: NegotiationReportStatus::Failed(
                                        ProfileBootstrapFailure::from(&error),
                                    ),
                                }));
                            }
                            close_for_post_profile_control_error(&core.profile.connection, &error);
                            let cleanup_failed = core.exchange.abort(manager).is_err();
                            self.state = ConnectionState::Failed { core };
                            return Poll::Ready(Err(ConnectionError::Control {
                                failure: ProfileBootstrapFailure::from(&error),
                                cleanup_failed,
                            }));
                        }
                    },
                },
                ConnectionState::Receiving {
                    mut core,
                    sender,
                    mut future,
                } => match future.as_mut().poll(cx) {
                    Poll::Pending => {
                        self.state = ConnectionState::Receiving {
                            core,
                            sender,
                            future,
                        };
                        return Poll::Pending;
                    }
                    Poll::Ready(completion) => match completion.result {
                        Ok(frame) => {
                            let result = core.exchange.receive(manager, &core.requirements, frame);
                            match transition_from_controller(
                                core,
                                sender,
                                completion.receiver,
                                result,
                            ) {
                                DriverTransition::State(state) => self.state = state,
                                DriverTransition::Event(state, event) => {
                                    self.state = state;
                                    return Poll::Ready(Ok(event));
                                }
                                DriverTransition::Error(state, error) => {
                                    self.state = state;
                                    return Poll::Ready(Err(error));
                                }
                            }
                        }
                        Err(error) => {
                            close_for_post_profile_control_error(&core.profile.connection, &error);
                            let cleanup_failed = core.exchange.abort(manager).is_err();
                            self.state = ConnectionState::Failed { core };
                            return Poll::Ready(Err(ConnectionError::Control {
                                failure: ProfileBootstrapFailure::from(&error),
                                cleanup_failed,
                            }));
                        }
                    },
                },
                ConnectionState::AuthoritySelection {
                    core,
                    sender,
                    receiver,
                } => {
                    self.state = ConnectionState::AuthoritySelection {
                        core,
                        sender,
                        receiver,
                    };
                    return Poll::Pending;
                }
                ConnectionState::NegotiatedEstablished { established } => {
                    match activate_established_driver(established, self.reliable_receive) {
                        Ok(driver) => {
                            self.state = ConnectionState::Established {
                                driver: Box::new(driver),
                            };
                            return Poll::Ready(Ok(ConnectionEvent::Established {
                                connection: self.connection,
                            }));
                        }
                        Err(established) => {
                            self.state = ConnectionState::EstablishedActivationFailed {
                                established: *established,
                            };
                            return Poll::Ready(Err(ConnectionError::EstablishedActivation));
                        }
                    }
                }
                ConnectionState::Established { driver } => {
                    self.state = ConnectionState::Established { driver };
                    return Poll::Pending;
                }
                ConnectionState::EstablishedActivationFailed { established } => {
                    self.state = ConnectionState::EstablishedActivationFailed { established };
                    return Poll::Ready(Err(ConnectionError::State(
                        ConnectionStateError::Terminal,
                    )));
                }
                ConnectionState::Failed { core } => {
                    self.state = ConnectionState::Failed { core };
                    return Poll::Ready(Err(ConnectionError::State(
                        ConnectionStateError::Terminal,
                    )));
                }
                ConnectionState::Transitioning => {
                    self.state = ConnectionState::Transitioning;
                    return Poll::Ready(Err(ConnectionError::State(
                        ConnectionStateError::Terminal,
                    )));
                }
            }
        }

        cx.waker().wake_by_ref();
        Poll::Pending
    }

    /// Resume negotiation after an `AuthoritySelectionRequired` event.
    pub fn select_authority(
        &mut self,
        manager: &mut NegotiationManager,
        contract: NegotiatedContract,
    ) -> Result<(), ConnectionError> {
        let state = std::mem::replace(&mut self.state, ConnectionState::Transitioning);
        let (mut core, sender, receiver) = match state {
            ConnectionState::AuthoritySelection {
                core,
                sender,
                receiver,
            } => (core, sender, receiver),
            ConnectionState::Failed { core } => {
                self.state = ConnectionState::Failed { core };
                return Err(ConnectionError::State(ConnectionStateError::Terminal));
            }
            state => {
                self.state = state;
                return Err(ConnectionError::State(
                    ConnectionStateError::AuthoritySelectionNotRequired,
                ));
            }
        };

        let result = core
            .exchange
            .propose_authority(manager, contract, &core.requirements);
        match transition_from_local_operation(core, sender, receiver, result) {
            LocalOperationTransition::State(state) => {
                self.state = state;
                Ok(())
            }
            LocalOperationTransition::Error(state, error) => {
                self.state = state;
                Err(error)
            }
        }
    }

    /// Consume all post-ProfileReady ownership and release Core connection state.
    pub fn teardown(
        self,
        manager: &mut NegotiationManager,
        delivery: &mut DeliveryEndpoint,
    ) -> ConnectionTeardown {
        match self.state {
            ConnectionState::Sending {
                core,
                receiver,
                future,
                ..
            } => {
                drop((receiver, future));
                core.teardown(manager, delivery, true)
            }
            ConnectionState::Receiving {
                core,
                sender,
                future,
            } => {
                drop((sender, future));
                core.teardown(manager, delivery, true)
            }
            ConnectionState::AuthoritySelection {
                core,
                sender,
                receiver,
            } => {
                drop((sender, receiver));
                core.teardown(manager, delivery, true)
            }
            ConnectionState::NegotiatedEstablished { established }
            | ConnectionState::EstablishedActivationFailed { established } => {
                established.teardown(manager, delivery).into()
            }
            ConnectionState::Established { driver } => (*driver).teardown(manager, delivery).into(),
            ConnectionState::Failed { core } => core.teardown(manager, delivery, false),
            ConnectionState::Transitioning => unreachable!("transition state never escapes a call"),
        }
    }

    #[cfg(test)]
    pub(super) fn into_established_internal(
        self,
    ) -> Result<(EstablishedConnectionDriver, ReliableReceiveLimits), Box<Self>> {
        let Self {
            connection,
            reliable_receive,
            state,
        } = self;
        match state {
            ConnectionState::Established { driver } => Ok((*driver, reliable_receive)),
            state => Err(Box::new(Self {
                connection,
                reliable_receive,
                state,
            })),
        }
    }
}

fn activate_established_driver(
    established: EstablishedNegotiatedConnection,
    reliable_receive: ReliableReceiveLimits,
) -> Result<EstablishedConnectionDriver, Box<EstablishedNegotiatedConnection>> {
    let flow_controlled = match established.into_flow_control() {
        Ok(flow_controlled) => flow_controlled,
        Err(error) => return Err(error.established),
    };
    Ok(flow_controlled
        .into_reliable_io(
            reliable_receive.scratch_bytes,
            reliable_receive.max_staging_bytes,
        )
        .into_established_io()
        .into_connection_driver())
}

fn map_driver_state_error(error: ConnectionDriverStateError) -> FlowCommandError {
    match error {
        ConnectionDriverStateError::ControlSendBusy => FlowCommandError::Busy,
        ConnectionDriverStateError::Terminal => FlowCommandError::Terminal,
    }
}

fn map_outbound_open_error(error: &OutboundOpenError) -> FlowCommandError {
    match error {
        OutboundOpenError::WrongConnection { .. } => FlowCommandError::WrongConnection,
        OutboundOpenError::WrongDirection(_) => FlowCommandError::WrongDirection,
        OutboundOpenError::InvalidPolicy(_)
        | OutboundOpenError::StableMessageLimitMismatch { .. }
        | OutboundOpenError::StableMessageLimitOutOfRange => FlowCommandError::InvalidConfiguration,
        OutboundOpenError::CoreFlowAlreadyExists(_) => FlowCommandError::AlreadyExists,
        OutboundOpenError::PendingCoreFlow(_) => FlowCommandError::Pending,
        OutboundOpenError::PeerMessageLimit { .. } => FlowCommandError::MessageLimit,
        OutboundOpenError::PeerActiveFlowLimit { .. }
        | OutboundOpenError::FlowId(_)
        | OutboundOpenError::Allocation(_) => FlowCommandError::ResourceLimit,
        OutboundOpenError::DatagramTooSmall { .. } => FlowCommandError::DatagramTooSmall,
        OutboundOpenError::DatagramUnavailable => FlowCommandError::ConnectionFailure,
        OutboundOpenError::DatagramEnvelope(_) | OutboundOpenError::Body(_) => {
            FlowCommandError::ProtocolFailure
        }
    }
}

fn map_flow_command_driver_error(error: ConnectionDriverError) -> FlowCommandError {
    match error {
        ConnectionDriverError::State(error) => map_driver_state_error(error),
        ConnectionDriverError::OutboundOpen(error) => map_outbound_open_error(&error),
        _ => FlowCommandError::ConnectionFailure,
    }
}

fn map_inbound_decision_driver_error(error: &ConnectionDriverError) -> FlowCommandError {
    match error {
        ConnectionDriverError::State(error) => map_driver_state_error(*error),
        ConnectionDriverError::InboundAdmission(InboundAdmissionError::RequestNotPending(_)) => {
            FlowCommandError::StaleRequest
        }
        ConnectionDriverError::InboundAdmission(_) => FlowCommandError::ConnectionFailure,
        _ => FlowCommandError::ConnectionFailure,
    }
}

fn map_core_submission_error(error: DeliveryOperationError) -> FlowCommandError {
    match error {
        DeliveryOperationError::UnknownFlow => FlowCommandError::UnknownFlow,
        DeliveryOperationError::WrongDirection => FlowCommandError::WrongDirection,
        DeliveryOperationError::NotReliable => FlowCommandError::ProtocolFailure,
    }
}

fn map_finish_driver_error(error: ConnectionDriverError) -> FlowCommandError {
    match error {
        ConnectionDriverError::State(error) => map_driver_state_error(error),
        ConnectionDriverError::Reliable(ReliableIoError::State(
            ReliableIoStateError::OutboundAcquisitionPending,
        )) => FlowCommandError::Pending,
        ConnectionDriverError::Reliable(ReliableIoError::State(
            ReliableIoStateError::UnknownOutboundFlow,
        )) => FlowCommandError::UnknownFlow,
        ConnectionDriverError::Reliable(ReliableIoError::OutboundFinish {
            error: SendError::PendingData | SendError::AlreadyFinishing,
            ..
        }) => FlowCommandError::Pending,
        ConnectionDriverError::Reliable(ReliableIoError::OutboundFinish {
            error: SendError::Terminal | SendError::Core(DeliveryOperationError::UnknownFlow),
            ..
        }) => FlowCommandError::FlowTerminated,
        ConnectionDriverError::FailurePreparation(FlowControlError::Allocation(_)) => {
            FlowCommandError::ResourceLimit
        }
        ConnectionDriverError::FailurePreparation(
            FlowControlError::UnknownActiveFlow(_)
            | FlowControlError::CoreState(DeliveryOperationError::UnknownFlow),
        ) => FlowCommandError::FlowTerminated,
        ConnectionDriverError::FailurePreparation(_) => FlowCommandError::ProtocolFailure,
        _ => FlowCommandError::ConnectionFailure,
    }
}

fn map_datagram_submit_outcome(
    outcome: DatagramSubmitOutcome,
) -> Result<SubmitOutcome, FlowCommandError> {
    match outcome {
        DatagramSubmitOutcome::Submitted(outcome) => match outcome {
            DatagramSubmissionOutcome::Accepted {
                accepted_index,
                local_pressure_drops,
            } => Ok(SubmitOutcome::Accepted {
                accepted_index,
                local_pressure_drops,
            }),
            DatagramSubmissionOutcome::RejectedTooLarge => Ok(SubmitOutcome::RejectedTooLarge),
            DatagramSubmissionOutcome::RejectedPressure => Ok(SubmitOutcome::RejectedPressure),
            DatagramSubmissionOutcome::RejectedCounterExhausted => {
                Ok(SubmitOutcome::RejectedCounterExhausted)
            }
            DatagramSubmissionOutcome::RejectedCurrentDatagramSize => {
                Ok(SubmitOutcome::RejectedCurrentDatagramSize)
            }
            DatagramSubmissionOutcome::RejectedTransportUnavailable => {
                Err(FlowCommandError::ConnectionFailure)
            }
        },
        DatagramSubmitOutcome::FlowFailureHandled { .. } => Err(FlowCommandError::FlowTerminated),
    }
}

enum LocalOperationTransition {
    State(ConnectionState),
    Error(ConnectionState, ConnectionError),
}

fn transition_from_local_operation(
    core: NegotiationCore,
    sender: ControlSender,
    receiver: ControlReceiver,
    result: Result<ControlFrame, NegotiationControlError>,
) -> LocalOperationTransition {
    match result {
        Ok(frame) => LocalOperationTransition::State(sending_state(
            core,
            sender,
            receiver,
            frame,
            PendingSendDisposition::Continue,
        )),
        Err(NegotiationControlError::LocalFailure {
            outcome,
            report: Some(frame),
        }) => LocalOperationTransition::State(sending_state(
            core,
            sender,
            receiver,
            frame,
            PendingSendDisposition::TerminalLocalFailure(outcome),
        )),
        Err(NegotiationControlError::LocalFailure {
            outcome,
            report: None,
        }) => {
            close_negotiation_failed(&core.profile.connection);
            LocalOperationTransition::Error(
                ConnectionState::Failed { core },
                ConnectionError::LocalNegotiation {
                    outcome: outcome.into(),
                    report: NegotiationReportStatus::Unavailable,
                },
            )
        }
        Err(error) => {
            let public = terminal_controller_error(&core, error);
            LocalOperationTransition::Error(ConnectionState::Failed { core }, public)
        }
    }
}

enum DriverTransition {
    State(ConnectionState),
    Event(ConnectionState, ConnectionEvent),
    Error(ConnectionState, ConnectionError),
}

fn transition_from_controller(
    core: NegotiationCore,
    sender: ControlSender,
    receiver: ControlReceiver,
    result: Result<NegotiationProgress, NegotiationControlError>,
) -> DriverTransition {
    match result {
        Ok(NegotiationProgress::Waiting) => {
            DriverTransition::State(receiving_state(core, sender, receiver))
        }
        Ok(NegotiationProgress::AuthoritySelectionRequired) => {
            let connection = core.connection;
            DriverTransition::Event(
                ConnectionState::AuthoritySelection {
                    core,
                    sender,
                    receiver,
                },
                ConnectionEvent::AuthoritySelectionRequired { connection },
            )
        }
        Ok(NegotiationProgress::Send(frame)) => {
            let disposition = controller_send_disposition(frame.frame_type);
            DriverTransition::State(sending_state(core, sender, receiver, frame, disposition))
        }
        Ok(NegotiationProgress::Established) => {
            DriverTransition::State(ConnectionState::NegotiatedEstablished {
                established: core.into_established(sender, receiver),
            })
        }
        Ok(NegotiationProgress::RemoteFailed(outcome)) => {
            close_negotiation_failed(&core.profile.connection);
            DriverTransition::Error(
                ConnectionState::Failed { core },
                ConnectionError::RemoteNegotiation(outcome.into()),
            )
        }
        Err(NegotiationControlError::LocalFailure {
            outcome,
            report: Some(frame),
        }) => DriverTransition::State(sending_state(
            core,
            sender,
            receiver,
            frame,
            PendingSendDisposition::TerminalLocalFailure(outcome),
        )),
        Err(NegotiationControlError::LocalFailure {
            outcome,
            report: None,
        }) => {
            close_negotiation_failed(&core.profile.connection);
            DriverTransition::Error(
                ConnectionState::Failed { core },
                ConnectionError::LocalNegotiation {
                    outcome: outcome.into(),
                    report: NegotiationReportStatus::Unavailable,
                },
            )
        }
        Err(error) => {
            let public = terminal_controller_error(&core, error);
            DriverTransition::Error(ConnectionState::Failed { core }, public)
        }
    }
}

fn terminal_controller_error(
    core: &NegotiationCore,
    error: NegotiationControlError,
) -> ConnectionError {
    match error {
        NegotiationControlError::LocalFailure { outcome, .. } => {
            close_negotiation_failed(&core.profile.connection);
            ConnectionError::LocalNegotiation {
                outcome: outcome.into(),
                report: NegotiationReportStatus::Unavailable,
            }
        }
        NegotiationControlError::ProfileProtocol(_) => {
            close_negotiation_protocol_error(&core.profile.connection);
            ConnectionError::NegotiationProtocol
        }
        NegotiationControlError::ManagerState(_) => {
            close_negotiation_failed(&core.profile.connection);
            ConnectionError::ManagerState
        }
        NegotiationControlError::UnexpectedCoreStatus(_) => {
            close_negotiation_failed(&core.profile.connection);
            ConnectionError::UnexpectedCoreState
        }
    }
}

fn sending_state(
    core: NegotiationCore,
    sender: ControlSender,
    receiver: ControlReceiver,
    frame: ControlFrame,
    disposition: PendingSendDisposition,
) -> ConnectionState {
    ConnectionState::Sending {
        core,
        receiver,
        future: send_control_owned(sender, frame, disposition),
        disposition,
    }
}

fn receiving_state(
    core: NegotiationCore,
    sender: ControlSender,
    receiver: ControlReceiver,
) -> ConnectionState {
    ConnectionState::Receiving {
        core,
        sender,
        future: receive_control_owned(receiver),
    }
}

fn send_control_owned(
    mut sender: ControlSender,
    frame: ControlFrame,
    disposition: PendingSendDisposition,
) -> OwnedNegotiationSendFuture {
    Box::pin(async move {
        let result = match disposition {
            PendingSendDisposition::TerminalLocalFailure(_) => {
                debug_assert_eq!(frame.frame_type, ControlFrameType::NegotiationFailed);
                sender.send_terminal_negotiation_failure(&frame.body).await
            }
            PendingSendDisposition::Continue | PendingSendDisposition::Establish => {
                sender.send_frame(frame.frame_type, &frame.body).await
            }
        };
        NegotiationSendCompletion { sender, result }
    })
}

fn receive_control_owned(mut receiver: ControlReceiver) -> OwnedNegotiationReceiveFuture {
    Box::pin(async move {
        let result = receiver.receive_frame().await;
        NegotiationReceiveCompletion { receiver, result }
    })
}

const fn controller_send_disposition(frame_type: ControlFrameType) -> PendingSendDisposition {
    match frame_type {
        ControlFrameType::NegotiationEstablished => PendingSendDisposition::Establish,
        _ => PendingSendDisposition::Continue,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_owned_send<T: Send + 'static>() {}

    #[test]
    fn negotiation_transport_futures_are_owned_and_send() {
        assert_owned_send::<OwnedNegotiationSendFuture>();
        assert_owned_send::<OwnedNegotiationReceiveFuture>();
    }

    #[test]
    fn only_final_established_send_finishes_authority_negotiation() {
        for frame_type in [
            ControlFrameType::NegotiationOffer,
            ControlFrameType::NegotiationProposal,
            ControlFrameType::NegotiationValidated,
            ControlFrameType::NegotiationFailed,
        ] {
            assert_eq!(
                controller_send_disposition(frame_type),
                PendingSendDisposition::Continue
            );
        }
        assert_eq!(
            controller_send_disposition(ControlFrameType::NegotiationEstablished),
            PendingSendDisposition::Establish
        );
    }

    #[test]
    fn datagram_submission_mapping_preserves_preaccept_rejection_and_flow_failure() {
        assert_eq!(
            map_datagram_submit_outcome(DatagramSubmitOutcome::Submitted(
                DatagramSubmissionOutcome::RejectedCurrentDatagramSize,
            )),
            Ok(SubmitOutcome::RejectedCurrentDatagramSize)
        );
        assert_eq!(
            map_datagram_submit_outcome(DatagramSubmitOutcome::FlowFailureHandled {
                flow_id: crate::wire::FlowId::new(WireSide::Client, 0).unwrap(),
            }),
            Err(FlowCommandError::FlowTerminated)
        );
    }

    #[test]
    fn finish_mapping_preserves_pending_and_terminal_flow_categories() {
        let flow_id = crate::wire::FlowId::new(WireSide::Client, 0).unwrap();
        assert_eq!(
            map_finish_driver_error(ConnectionDriverError::Reliable(ReliableIoError::State(
                ReliableIoStateError::OutboundAcquisitionPending,
            ))),
            FlowCommandError::Pending
        );
        assert_eq!(
            map_finish_driver_error(ConnectionDriverError::Reliable(
                ReliableIoError::OutboundFinish {
                    flow_id,
                    error: SendError::PendingData,
                },
            )),
            FlowCommandError::Pending
        );
        assert_eq!(
            map_finish_driver_error(ConnectionDriverError::Reliable(
                ReliableIoError::OutboundFinish {
                    flow_id,
                    error: SendError::Terminal,
                },
            )),
            FlowCommandError::FlowTerminated
        );
    }
}
