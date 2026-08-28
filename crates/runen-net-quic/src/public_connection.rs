use std::{
    fmt,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};

use quinn::Connection as QuinnConnection;
use runen_net::{
    delivery::{DeliveryEndpoint, FlowTermination},
    identity::ConnectionHandle,
    protocol::{
        CompatibilityOffer, NegotiatedContract, NegotiationManager, NegotiationRequirements,
    },
};

use crate::{
    control::{
        ControlFrame, ControlFrameType, ControlReceiver, ControlSender, ProfileBootstrapError,
        ProfileReadyParts, Settings, ValidatedControlProfile,
    },
    endpoint::ConnectionSlotPermit,
    facade::{ProfileBootstrapFailure, ProfileReadyConnection},
    lifecycle::{
        AdmittedProfileReadyConnection, EstablishedNegotiatedConnection,
        close_for_post_profile_control_error, close_negotiation_failed,
        close_negotiation_protocol_error, teardown_connection,
    },
    negotiation::{
        NegotiationControlError, NegotiationExchange, NegotiationOutcome, NegotiationProgress,
    },
    wire::WireSide,
};

const MAX_INTERNAL_TRANSITIONS_PER_POLL: usize = 16;

/// Explicit finite reliable receive resources retained for established delivery activation.
///
/// RN6 provides no implicit defaults. These values are retained through negotiation and
/// consumed unchanged by the established reliable transport owner in the next RN6 slice.
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
    Established {
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
            Self::Established { .. } => "Established",
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
                                self.state = ConnectionState::Established {
                                    established: core.into_established(completion.sender, receiver),
                                };
                                return Poll::Ready(Ok(ConnectionEvent::Established {
                                    connection: self.connection,
                                }));
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
                ConnectionState::Established { established } => {
                    self.state = ConnectionState::Established { established };
                    return Poll::Pending;
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
            ConnectionState::Established { established } => {
                established.teardown(manager, delivery).into()
            }
            ConnectionState::Failed { core } => core.teardown(manager, delivery, false),
            ConnectionState::Transitioning => unreachable!("transition state never escapes a call"),
        }
    }

    #[cfg(test)]
    pub(super) fn into_established_internal(
        self,
    ) -> Result<(EstablishedNegotiatedConnection, ReliableReceiveLimits), Box<Self>> {
        let Self {
            connection,
            reliable_receive,
            state,
        } = self;
        match state {
            ConnectionState::Established { established } => Ok((established, reliable_receive)),
            state => Err(Box::new(Self {
                connection,
                reliable_receive,
                state,
            })),
        }
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
            let connection = core.connection;
            DriverTransition::Event(
                ConnectionState::Established {
                    established: core.into_established(sender, receiver),
                },
                ConnectionEvent::Established { connection },
            )
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
}
