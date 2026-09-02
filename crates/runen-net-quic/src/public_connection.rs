use std::{
    fmt,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};

use quinn::{Connection as QuinnConnection, ReadError, ReadExactError, WriteError};
use runen_net::{
    delivery::{
        DeliveryEndpoint, DeliveryFlowKey, DeliveryMode, DeliveryOperationError, FlowDirection,
        FlowTermination, ReceiveOutcome,
    },
    identity::ConnectionHandle,
    protocol::{
        CompatibilityOffer, NegotiatedContract, NegotiationManager, NegotiationManagerError,
        NegotiationRequirements,
    },
};

use crate::{
    connection_driver::{
        ConnectionDriverError, ConnectionDriverStateError, DatagramSubmitOutcome,
        EstablishedConnectionDriver, EstablishedConnectionProgress, InboundDecisionDriverError,
        KeyedDatagramSubmitError, KeyedFinishError, OutboundFinishOutcome,
    },
    control::{
        ControlFrame, ControlFrameError, ControlFrameType, ControlReceiver, ControlSender,
        ProfileBootstrapError, ProfileReadyParts, Settings, ValidatedControlProfile,
    },
    datagram::{
        DatagramReceiveError, DatagramReceiveOutcome, DatagramSendError, DatagramSendProgress,
        DatagramSubmissionOutcome,
    },
    datagram_driver::{DatagramIoError, DatagramOutboundProgress},
    endpoint::ConnectionSlotPermit,
    facade::{ProfileBootstrapFailure, ProfileReadyConnection, ReliableReceiveLimits},
    flow_control::{
        FlowControlConfigError, FlowControlError, InboundAdmission, InboundAdmissionError,
        OutboundOpenError, OutboundOpenRequest,
    },
    flow_driver::FlowControlSendEffect,
    lifecycle::{
        AdmittedProfileReadyConnection, EstablishedNegotiatedConnection,
        FlowControlActivationError, close_for_post_profile_control_error, close_negotiation_failed,
        close_negotiation_protocol_error, teardown_connection,
    },
    negotiation::{
        NegotiationControlError, NegotiationExchange, NegotiationOutcome, NegotiationProgress,
    },
    public_flow::{
        FlowCommandError, FlowRejectionReason, FlowTerminationCause, FlowTerminationOrigin,
        InboundFlowConfig, IncomingFlowDecisionError, IncomingFlowRequest, OutboundFlowConfig,
        SubmissionError, SubmitOutcome,
    },
    quinn_binding::{ReceiveError, ReceiveProgress, RegistryError, SendError, SendProgress},
    reliable_driver::{
        ActiveReliableProgress, ReliableFailureContext, ReliableIoError, ReliableIoStateError,
    },
    wire::WireSide,
};

const MAX_INTERNAL_TRANSITIONS_PER_POLL: usize = 16;

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

impl fmt::Display for ConnectionStateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid RunenNet connection state: {self:?}")
    }
}

impl std::error::Error for ConnectionStateError {}

/// Stable application-facing classification for a post-ProfileReady connection failure.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ConnectionErrorKind {
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
    EstablishedResource,
    EstablishedProtocol,
    EstablishedTransport,
    EstablishedControl(ProfileBootstrapFailure),
    State(ConnectionStateError),
}

/// Public post-ProfileReady connection failure.
///
/// [`Self::kind`] is the stable application classification. Private transport/controller detail
/// may be retained as an opaque diagnostic source for the first observable failure without making
/// Quinn, wire, or driver types part of the public API.
#[derive(Debug)]
pub struct ConnectionError {
    kind: ConnectionErrorKind,
    diagnostic: Option<ConnectionDiagnostic>,
}

impl ConnectionError {
    pub const fn kind(&self) -> ConnectionErrorKind {
        self.kind
    }

    const fn classified(kind: ConnectionErrorKind) -> Self {
        Self {
            kind,
            diagnostic: None,
        }
    }

    fn diagnosed(kind: ConnectionErrorKind, diagnostic: ConnectionDiagnostic) -> Self {
        Self {
            kind,
            diagnostic: Some(diagnostic),
        }
    }

    const fn state(error: ConnectionStateError) -> Self {
        Self::classified(ConnectionErrorKind::State(error))
    }
}

impl PartialEq for ConnectionError {
    fn eq(&self, other: &Self) -> bool {
        self.kind == other.kind
    }
}

impl Eq for ConnectionError {}

impl fmt::Display for ConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.diagnostic {
            Some(diagnostic) => write!(
                formatter,
                "RunenNet connection progression failed: {:?}: {diagnostic}",
                self.kind
            ),
            None => write!(
                formatter,
                "RunenNet connection progression failed: {:?}",
                self.kind
            ),
        }
    }
}

impl std::error::Error for ConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.kind {
            ConnectionErrorKind::State(ref error) => Some(error),
            _ => self
                .diagnostic
                .as_ref()
                .map(|diagnostic| diagnostic as &(dyn std::error::Error + 'static)),
        }
    }
}

#[derive(Debug)]
enum ConnectionDiagnostic {
    Resource(ConnectionResourceDiagnostic),
    Opaque(Box<OpaqueConnectionDiagnostic>),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ConnectionResourceDiagnostic {
    ProfileControlAllocation { cleanup_failed: bool },
    Established(EstablishedResourceDiagnostic),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum EstablishedResourceDiagnostic {
    Allocation,
    CapacityOverflow,
}

#[derive(Debug)]
enum OpaqueConnectionDiagnostic {
    ProfileControl(ProfileBootstrapError),
    Control {
        error: ProfileBootstrapError,
        cleanup_error: Option<NegotiationControlError>,
    },
    Negotiation(NegotiationControlError),
    EstablishedActivation(FlowControlConfigError),
    EstablishedDriver(ConnectionDriverError),
    UnexpectedCoreReceive(ReceiveOutcome),
}

impl ConnectionDiagnostic {
    fn profile_control(error: ProfileBootstrapError) -> Self {
        if profile_control_is_allocation(&error) {
            Self::Resource(ConnectionResourceDiagnostic::ProfileControlAllocation {
                cleanup_failed: false,
            })
        } else {
            Self::Opaque(Box::new(OpaqueConnectionDiagnostic::ProfileControl(error)))
        }
    }

    fn control(
        error: ProfileBootstrapError,
        cleanup_error: Option<NegotiationControlError>,
    ) -> Self {
        if profile_control_is_allocation(&error) {
            Self::Resource(ConnectionResourceDiagnostic::ProfileControlAllocation {
                cleanup_failed: cleanup_error.is_some(),
            })
        } else {
            Self::Opaque(Box::new(OpaqueConnectionDiagnostic::Control {
                error,
                cleanup_error,
            }))
        }
    }

    fn negotiation(error: NegotiationControlError) -> Self {
        Self::Opaque(Box::new(OpaqueConnectionDiagnostic::Negotiation(error)))
    }

    fn established_activation(error: FlowControlConfigError) -> Self {
        Self::Opaque(Box::new(OpaqueConnectionDiagnostic::EstablishedActivation(
            error,
        )))
    }

    const fn established_resource(error: EstablishedResourceDiagnostic) -> Self {
        Self::Resource(ConnectionResourceDiagnostic::Established(error))
    }

    fn established_driver(error: ConnectionDriverError) -> Self {
        Self::Opaque(Box::new(OpaqueConnectionDiagnostic::EstablishedDriver(
            error,
        )))
    }

    fn unexpected_core_receive(outcome: ReceiveOutcome) -> Self {
        Self::Opaque(Box::new(OpaqueConnectionDiagnostic::UnexpectedCoreReceive(
            outcome,
        )))
    }
}

impl fmt::Display for ConnectionDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resource(ConnectionResourceDiagnostic::ProfileControlAllocation {
                cleanup_failed,
            }) => write!(
                formatter,
                "profile control allocation failure; cleanup_failed={cleanup_failed}"
            ),
            Self::Resource(ConnectionResourceDiagnostic::Established(error)) => {
                write!(formatter, "established resource failure: {error:?}")
            }
            Self::Opaque(diagnostic) => diagnostic.fmt(formatter),
        }
    }
}

impl std::error::Error for ConnectionDiagnostic {}

impl fmt::Display for OpaqueConnectionDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ProfileControl(error) => {
                write!(formatter, "profile control failure: {error:?}")
            }
            Self::Control {
                error,
                cleanup_error,
            } => {
                write!(formatter, "profile control failure: {error:?}")?;
                if let Some(cleanup_error) = cleanup_error {
                    write!(formatter, "; cleanup failure: {cleanup_error:?}")?;
                }
                Ok(())
            }
            Self::Negotiation(error) => {
                write!(formatter, "negotiation controller failure: {error:?}")
            }
            Self::EstablishedActivation(error) => {
                write!(formatter, "established activation failure: {error:?}")
            }
            Self::EstablishedDriver(error) => {
                write!(formatter, "established driver failure: {error:?}")
            }
            Self::UnexpectedCoreReceive(outcome) => {
                write!(formatter, "unexpected Core receive outcome: {outcome:?}")
            }
        }
    }
}

fn profile_control_is_allocation(error: &ProfileBootstrapError) -> bool {
    matches!(
        error,
        ProfileBootstrapError::Frame(ControlFrameError::Allocation(_))
    )
}

/// Why an unreliable inbound payload was observably dropped before application exposure.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum UnreliableReceiveDropReason {
    Pressure { local_pressure_drops: usize },
    TooLarge,
    StaleSequenced,
}

/// Public connection progress that requires host observation or action.
///
/// This event is intentionally move-only because incoming admission owns a one-shot request
/// capability. Transport-local flow identifiers and progress never cross this boundary.
#[non_exhaustive]
#[derive(Debug)]
pub enum ConnectionEvent {
    AuthoritySelectionRequired {
        connection: ConnectionHandle,
    },
    Established {
        connection: ConnectionHandle,
    },
    /// The established peer closed the `runennet/1` QUIC connection with `NO_ERROR`.
    ///
    /// This is connection-lifecycle information only. It does not imply application-protocol
    /// success, complete message exposure, or normal delivery-flow termination.
    PeerClosed {
        connection: ConnectionHandle,
    },
    IncomingFlowRequested {
        request: IncomingFlowRequest,
    },
    OutboundFlowEstablished {
        key: DeliveryFlowKey,
    },
    OutboundFlowRejected {
        key: DeliveryFlowKey,
        reason: FlowRejectionReason,
    },
    DataReady {
        key: DeliveryFlowKey,
        buffered_messages: usize,
        local_pressure_drops: usize,
    },
    UnreliableReceiveDropped {
        key: DeliveryFlowKey,
        reason: UnreliableReceiveDropReason,
    },
    UnreliableTransportDropped {
        key: DeliveryFlowKey,
        accepted_index: u64,
    },
    FlowTerminated {
        key: DeliveryFlowKey,
        origin: FlowTerminationOrigin,
        cause: FlowTerminationCause,
        termination: Option<FlowTermination>,
    },
}

/// Categorized cleanup failure returned by consuming connection teardown.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ConnectionCleanupError {
    NegotiationManagerState(NegotiationManagerError),
}

impl fmt::Display for ConnectionCleanupError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NegotiationManagerState(error) => {
                write!(formatter, "RunenNet connection cleanup failed: {error}")
            }
        }
    }
}

impl std::error::Error for ConnectionCleanupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::NegotiationManagerState(error) => Some(error),
        }
    }
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
            cleanup_error: teardown.negotiation_cleanup_error.map(|error| match error {
                NegotiationControlError::ManagerState(error) => {
                    ConnectionCleanupError::NegotiationManagerState(error)
                }
                _ => unreachable!(
                    "NegotiationExchange::abort only returns negotiation-manager state failures"
                ),
            }),
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
    EstablishedPeerClosed {
        driver: Box<EstablishedConnectionDriver>,
        close_pending: bool,
    },
    EstablishedFailed {
        driver: Box<EstablishedConnectionDriver>,
        kind: ConnectionErrorKind,
        pending_error: Option<ConnectionError>,
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
            Self::EstablishedPeerClosed { .. } => "EstablishedPeerClosed",
            Self::EstablishedFailed { .. } => "EstablishedFailed",
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
        manager: &mut NegotiationManager,
    ) -> Result<Connection, ConnectionError> {
        let (admitted, reliable_receive) = self.into_parts();
        Connection::activate(
            admitted,
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
            ConnectionState::EstablishedPeerClosed { .. }
            | ConnectionState::EstablishedFailed { .. }
            | ConnectionState::EstablishedActivationFailed { .. }
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
                    stable_max_message_bytes: NonZeroUsize::new(config.policy.max_message_bytes())
                        .expect("Core flow policy maximum is non-zero"),
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
            ConnectionState::EstablishedPeerClosed { .. }
            | ConnectionState::EstablishedFailed { .. }
            | ConnectionState::EstablishedActivationFailed { .. }
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
            ConnectionState::EstablishedPeerClosed { .. }
            | ConnectionState::EstablishedFailed { .. }
            | ConnectionState::EstablishedActivationFailed { .. }
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
            ConnectionState::EstablishedPeerClosed { .. }
            | ConnectionState::EstablishedFailed { .. }
            | ConnectionState::EstablishedActivationFailed { .. }
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
            ConnectionState::EstablishedPeerClosed { .. }
            | ConnectionState::EstablishedFailed { .. }
            | ConnectionState::EstablishedActivationFailed { .. }
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
    /// Aggregate Core authorities are borrowed only for this synchronous call. Private transport
    /// progress is consumed internally; at most one durable application event or error is returned.
    pub fn poll(
        &mut self,
        cx: &mut Context<'_>,
        manager: &mut NegotiationManager,
        delivery: &mut DeliveryEndpoint,
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
                                return Poll::Ready(Err(ConnectionError::classified(
                                    ConnectionErrorKind::LocalNegotiation {
                                        outcome: outcome.into(),
                                        report: NegotiationReportStatus::Sent,
                                    },
                                )));
                            }
                        },
                        Err(error) => {
                            if let PendingSendDisposition::TerminalLocalFailure(outcome) =
                                disposition
                            {
                                close_negotiation_failed(&core.profile.connection);
                                let kind = ConnectionErrorKind::LocalNegotiation {
                                    outcome: outcome.into(),
                                    report: NegotiationReportStatus::Failed(
                                        ProfileBootstrapFailure::from(&error),
                                    ),
                                };
                                self.state = ConnectionState::Failed { core };
                                return Poll::Ready(Err(ConnectionError::diagnosed(
                                    kind,
                                    ConnectionDiagnostic::profile_control(error),
                                )));
                            }
                            close_for_post_profile_control_error(&core.profile.connection, &error);
                            let cleanup_error = core.exchange.abort(manager).err();
                            let public_error = control_connection_error(error, cleanup_error);
                            self.state = ConnectionState::Failed { core };
                            return Poll::Ready(Err(public_error));
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
                            let cleanup_error = core.exchange.abort(manager).err();
                            let public_error = control_connection_error(error, cleanup_error);
                            self.state = ConnectionState::Failed { core };
                            return Poll::Ready(Err(public_error));
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
                        Err(error) => {
                            let FlowControlActivationError { error, established } = error;
                            self.state = ConnectionState::EstablishedActivationFailed {
                                established: *established,
                            };
                            return Poll::Ready(Err(ConnectionError::diagnosed(
                                ConnectionErrorKind::EstablishedActivation,
                                ConnectionDiagnostic::established_activation(error),
                            )));
                        }
                    }
                }
                ConnectionState::Established { mut driver } => {
                    match driver.poll_step(cx, delivery) {
                        Poll::Pending => {
                            self.state = ConnectionState::Established { driver };
                            return Poll::Pending;
                        }
                        Poll::Ready(Ok(progress)) => {
                            match map_established_progress(self.connection, progress) {
                                Ok(Some(event)) => {
                                    self.state = ConnectionState::Established { driver };
                                    return Poll::Ready(Ok(event));
                                }
                                Ok(None) => {
                                    self.state = ConnectionState::Established { driver };
                                    continue;
                                }
                                Err(error) => {
                                    let kind = error.kind();
                                    self.state = ConnectionState::EstablishedFailed {
                                        driver,
                                        kind,
                                        pending_error: None,
                                    };
                                    return Poll::Ready(Err(error));
                                }
                            }
                        }
                        Poll::Ready(Err(error)) => {
                            if driver.peer_no_error_close_observed()
                                && established_driver_error_is_connection_close_observation(&error)
                            {
                                let preceding_event = peer_close_preceding_event(error);
                                let close_pending = preceding_event.is_some();
                                self.state = ConnectionState::EstablishedPeerClosed {
                                    driver,
                                    close_pending,
                                };
                                if let Some(event) = preceding_event {
                                    return Poll::Ready(Ok(event));
                                }
                                return Poll::Ready(Ok(ConnectionEvent::PeerClosed {
                                    connection: self.connection,
                                }));
                            }

                            let (event, public_error) = map_established_driver_error(error);
                            let kind = public_error.kind();
                            if let Some(event) = event {
                                self.state = ConnectionState::EstablishedFailed {
                                    driver,
                                    kind,
                                    pending_error: Some(public_error),
                                };
                                return Poll::Ready(Ok(event));
                            }
                            self.state = ConnectionState::EstablishedFailed {
                                driver,
                                kind,
                                pending_error: None,
                            };
                            return Poll::Ready(Err(public_error));
                        }
                    }
                }
                ConnectionState::EstablishedPeerClosed {
                    driver,
                    close_pending,
                } => {
                    if close_pending {
                        self.state = ConnectionState::EstablishedPeerClosed {
                            driver,
                            close_pending: false,
                        };
                        return Poll::Ready(Ok(ConnectionEvent::PeerClosed {
                            connection: self.connection,
                        }));
                    }
                    self.state = ConnectionState::EstablishedPeerClosed {
                        driver,
                        close_pending: false,
                    };
                    return Poll::Ready(Err(ConnectionError::state(
                        ConnectionStateError::Terminal,
                    )));
                }
                ConnectionState::EstablishedFailed {
                    driver,
                    kind,
                    pending_error,
                } => {
                    if let Some(error) = pending_error {
                        self.state = ConnectionState::EstablishedFailed {
                            driver,
                            kind,
                            pending_error: None,
                        };
                        return Poll::Ready(Err(error));
                    }
                    self.state = ConnectionState::EstablishedFailed {
                        driver,
                        kind,
                        pending_error: None,
                    };
                    return Poll::Ready(Err(ConnectionError::classified(kind)));
                }
                ConnectionState::EstablishedActivationFailed { established } => {
                    self.state = ConnectionState::EstablishedActivationFailed { established };
                    return Poll::Ready(Err(ConnectionError::state(
                        ConnectionStateError::Terminal,
                    )));
                }
                ConnectionState::Failed { core } => {
                    self.state = ConnectionState::Failed { core };
                    return Poll::Ready(Err(ConnectionError::state(
                        ConnectionStateError::Terminal,
                    )));
                }
                ConnectionState::Transitioning => {
                    self.state = ConnectionState::Transitioning;
                    return Poll::Ready(Err(ConnectionError::state(
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
                return Err(ConnectionError::state(ConnectionStateError::Terminal));
            }
            state => {
                self.state = state;
                return Err(ConnectionError::state(
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
            ConnectionState::Established { driver }
            | ConnectionState::EstablishedPeerClosed { driver, .. }
            | ConnectionState::EstablishedFailed { driver, .. } => {
                (*driver).teardown(manager, delivery).into()
            }
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
) -> Result<EstablishedConnectionDriver, FlowControlActivationError> {
    let flow_controlled = established.into_flow_control()?;
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

fn map_established_progress(
    connection: ConnectionHandle,
    progress: EstablishedConnectionProgress,
) -> Result<Option<ConnectionEvent>, ConnectionError> {
    let event = match progress {
        EstablishedConnectionProgress::ControlSendStarted => None,
        EstablishedConnectionProgress::ControlSendCompleted(effect) => {
            map_control_send_effect(effect)
        }
        EstablishedConnectionProgress::InboundOpen(inner) => {
            Some(ConnectionEvent::IncomingFlowRequested {
                request: IncomingFlowRequest { connection, inner },
            })
        }
        EstablishedConnectionProgress::OutboundEstablished(flow) => {
            Some(ConnectionEvent::OutboundFlowEstablished { key: flow.key() })
        }
        EstablishedConnectionProgress::OutboundRejected { key, reason, .. } => {
            Some(ConnectionEvent::OutboundFlowRejected {
                key,
                reason: reason.into(),
            })
        }
        EstablishedConnectionProgress::RemoteTerminated {
            flow,
            reason,
            termination,
        } => Some(flow_terminated_event(
            flow.key(),
            FlowTerminationOrigin::Remote,
            reason.into(),
            Some(termination),
        )),
        EstablishedConnectionProgress::ReliableOutboundAcquisition(_)
        | EstablishedConnectionProgress::ReliableInboundAcquired => None,
        EstablishedConnectionProgress::Reliable(progress) => match progress {
            ActiveReliableProgress::Outbound {
                progress: SendProgress::Closed { termination },
                ..
            } => Some(flow_terminated_event(
                termination.key,
                FlowTerminationOrigin::Local,
                FlowTerminationCause::Normal,
                Some(termination),
            )),
            ActiveReliableProgress::Outbound { .. } => None,
            ActiveReliableProgress::Inbound(ReceiveProgress::MessagesBuffered { key, count }) => {
                Some(ConnectionEvent::DataReady {
                    key,
                    buffered_messages: count,
                    local_pressure_drops: 0,
                })
            }
            ActiveReliableProgress::Inbound(ReceiveProgress::Closed {
                key,
                termination: Some(termination),
            }) => Some(flow_terminated_event(
                key,
                FlowTerminationOrigin::Remote,
                FlowTerminationCause::Normal,
                Some(termination),
            )),
            ActiveReliableProgress::Inbound(
                ReceiveProgress::Progressed { .. }
                | ReceiveProgress::Associated { .. }
                | ReceiveProgress::Draining { .. }
                | ReceiveProgress::Closed {
                    termination: None, ..
                },
            ) => None,
        },
        EstablishedConnectionProgress::DatagramOutbound(progress) => match progress {
            DatagramOutboundProgress::Cancelled { .. } => None,
            DatagramOutboundProgress::Driven {
                key,
                progress: DatagramSendProgress::DroppedTransport { accepted_index },
                ..
            } => Some(ConnectionEvent::UnreliableTransportDropped {
                key,
                accepted_index,
            }),
            DatagramOutboundProgress::Driven { .. } => None,
        },
        EstablishedConnectionProgress::DatagramInbound(outcome) => match outcome {
            DatagramReceiveOutcome::DiscardedUnknownFlow => None,
            DatagramReceiveOutcome::Core { key, outcome } => match outcome {
                ReceiveOutcome::Buffered {
                    local_pressure_drops,
                } => Some(ConnectionEvent::DataReady {
                    key,
                    buffered_messages: 1,
                    local_pressure_drops,
                }),
                ReceiveOutcome::DroppedByPressure {
                    local_pressure_drops,
                } => Some(ConnectionEvent::UnreliableReceiveDropped {
                    key,
                    reason: UnreliableReceiveDropReason::Pressure {
                        local_pressure_drops,
                    },
                }),
                ReceiveOutcome::DroppedTooLarge => {
                    Some(ConnectionEvent::UnreliableReceiveDropped {
                        key,
                        reason: UnreliableReceiveDropReason::TooLarge,
                    })
                }
                ReceiveOutcome::StaleSequenced => Some(ConnectionEvent::UnreliableReceiveDropped {
                    key,
                    reason: UnreliableReceiveDropReason::StaleSequenced,
                }),
                unexpected @ (ReceiveOutcome::DuplicateReliable
                | ReceiveOutcome::RejectedModeMismatch
                | ReceiveOutcome::TerminalReliableFailure) => {
                    return Err(ConnectionError::diagnosed(
                        ConnectionErrorKind::UnexpectedCoreState,
                        ConnectionDiagnostic::unexpected_core_receive(unexpected),
                    ));
                }
            },
        },
        EstablishedConnectionProgress::FlowFailureHandled { .. } => None,
    };
    Ok(event)
}

fn map_control_send_effect(effect: FlowControlSendEffect) -> Option<ConnectionEvent> {
    match effect {
        FlowControlSendEffect::OutboundFailedAfterAccept {
            key,
            reason,
            termination,
            ..
        }
        | FlowControlSendEffect::ReportOnlyTermination {
            key,
            reason,
            termination,
            ..
        } => Some(flow_terminated_event(
            key,
            FlowTerminationOrigin::Local,
            reason.into(),
            termination,
        )),
        FlowControlSendEffect::LocalTerminated {
            flow,
            reason,
            termination,
        } => Some(flow_terminated_event(
            flow.key(),
            FlowTerminationOrigin::Local,
            reason.into(),
            Some(termination),
        )),
        FlowControlSendEffect::OutboundOpenPrepared(_)
        | FlowControlSendEffect::InboundRejected { .. }
        | FlowControlSendEffect::InboundAccepted(_) => None,
    }
}

fn established_driver_error_is_connection_close_observation(
    error: &ConnectionDriverError,
) -> bool {
    match error {
        ConnectionDriverError::ControlReceive(error) => {
            profile_control_is_connection_close_observation(error)
        }
        ConnectionDriverError::ControlSend(error) => {
            profile_control_is_connection_close_observation(&error.error)
        }
        ConnectionDriverError::Reliable(ReliableIoError::Connection(_)) => true,
        ConnectionDriverError::Reliable(ReliableIoError::InboundActiveBinding {
            context: ReliableFailureContext::Unresolved,
            error: ReceiveError::Io(_),
        }) => true,
        ConnectionDriverError::Datagram(DatagramIoError::Connection(_)) => true,
        ConnectionDriverError::Datagram(DatagramIoError::Send {
            error: DatagramSendError::ProfileUnavailable | DatagramSendError::ConnectionLost,
            ..
        }) => true,
        ConnectionDriverError::State(_)
        | ConnectionDriverError::OutboundOpen(_)
        | ConnectionDriverError::InboundAdmission(_)
        | ConnectionDriverError::DatagramSubmission(_)
        | ConnectionDriverError::DatagramProfileUnavailable
        | ConnectionDriverError::ReceivedFlowControl(_)
        | ConnectionDriverError::Reliable(_)
        | ConnectionDriverError::Datagram(_)
        | ConnectionDriverError::FailurePreparation(_) => false,
    }
}

fn profile_control_is_connection_close_observation(error: &ProfileBootstrapError) -> bool {
    matches!(
        error,
        ProfileBootstrapError::Connection(_)
            | ProfileBootstrapError::Frame(ControlFrameError::Read(
                ReadExactError::FinishedEarly(_)
            ))
            | ProfileBootstrapError::Frame(ControlFrameError::Read(ReadExactError::ReadError(
                ReadError::ConnectionLost(_)
            )))
            | ProfileBootstrapError::Frame(ControlFrameError::Write(WriteError::ConnectionLost(_)))
    )
}

fn peer_close_preceding_event(error: ConnectionDriverError) -> Option<ConnectionEvent> {
    match error {
        ConnectionDriverError::ControlSend(error) => map_control_send_effect(error.effect),
        _ => None,
    }
}

const fn flow_terminated_event(
    key: DeliveryFlowKey,
    origin: FlowTerminationOrigin,
    cause: FlowTerminationCause,
    termination: Option<FlowTermination>,
) -> ConnectionEvent {
    ConnectionEvent::FlowTerminated {
        key,
        origin,
        cause,
        termination,
    }
}

fn map_established_driver_error(
    error: ConnectionDriverError,
) -> (Option<ConnectionEvent>, ConnectionError) {
    match error {
        ConnectionDriverError::ControlSend(error) => {
            let error = *error;
            let kind = ConnectionErrorKind::EstablishedControl(ProfileBootstrapFailure::from(
                &error.error,
            ));
            let event = map_control_send_effect(error.effect);
            (
                event,
                ConnectionError::diagnosed(
                    kind,
                    ConnectionDiagnostic::profile_control(error.error),
                ),
            )
        }
        ConnectionDriverError::State(ConnectionDriverStateError::Terminal) => {
            (None, ConnectionError::state(ConnectionStateError::Terminal))
        }
        error => {
            let kind = established_driver_error_kind(&error);
            if let Some(diagnostic) = connection_driver_resource_diagnostic(&error) {
                return (
                    None,
                    ConnectionError::diagnosed(
                        kind,
                        ConnectionDiagnostic::established_resource(diagnostic),
                    ),
                );
            }
            (
                None,
                ConnectionError::diagnosed(kind, ConnectionDiagnostic::established_driver(error)),
            )
        }
    }
}

fn connection_driver_resource_diagnostic(
    error: &ConnectionDriverError,
) -> Option<EstablishedResourceDiagnostic> {
    if connection_driver_is_allocation(error) {
        return Some(EstablishedResourceDiagnostic::Allocation);
    }
    if matches!(
        error,
        ConnectionDriverError::Reliable(ReliableIoError::State(
            ReliableIoStateError::CapacityOverflow
        ))
    ) {
        return Some(EstablishedResourceDiagnostic::CapacityOverflow);
    }
    None
}

fn connection_driver_is_allocation(error: &ConnectionDriverError) -> bool {
    match error {
        ConnectionDriverError::OutboundOpen(OutboundOpenError::Allocation(_)) => true,
        ConnectionDriverError::InboundAdmission(error) => inbound_admission_is_allocation(error),
        ConnectionDriverError::ControlReceive(error) => profile_control_is_allocation(error),
        ConnectionDriverError::ControlSend(error) => profile_control_is_allocation(&error.error),
        ConnectionDriverError::ReceivedFlowControl(error)
        | ConnectionDriverError::FailurePreparation(error) => flow_control_is_allocation(error),
        ConnectionDriverError::Reliable(error) => reliable_io_is_allocation(error),
        ConnectionDriverError::Datagram(error) => datagram_io_is_allocation(error),
        ConnectionDriverError::State(_)
        | ConnectionDriverError::OutboundOpen(_)
        | ConnectionDriverError::DatagramSubmission(_)
        | ConnectionDriverError::DatagramProfileUnavailable => false,
    }
}

fn inbound_admission_is_allocation(error: &InboundAdmissionError) -> bool {
    matches!(
        error,
        InboundAdmissionError::Allocation(_)
            | InboundAdmissionError::Registry(RegistryError::AllocationFailed)
    )
}

fn flow_control_is_allocation(error: &FlowControlError) -> bool {
    matches!(
        error,
        FlowControlError::Allocation(_)
            | FlowControlError::Registry(RegistryError::AllocationFailed)
    )
}

fn reliable_io_is_allocation(error: &ReliableIoError) -> bool {
    match error {
        ReliableIoError::Allocation(_) => true,
        ReliableIoError::OutboundFinish { error, .. }
        | ReliableIoError::OutboundAcquisitionBinding { error, .. }
        | ReliableIoError::OutboundActiveBinding { error, .. } => send_error_is_allocation(error),
        ReliableIoError::InboundConstruction(error)
        | ReliableIoError::InboundActiveBinding { error, .. } => receive_error_is_allocation(error),
        ReliableIoError::State(_) | ReliableIoError::Connection(_) => false,
    }
}

fn send_error_is_allocation(error: &SendError) -> bool {
    matches!(error, SendError::Registry(RegistryError::AllocationFailed))
}

fn receive_error_is_allocation(error: &ReceiveError) -> bool {
    matches!(
        error,
        ReceiveError::AllocationFailed | ReceiveError::Registry(RegistryError::AllocationFailed)
    )
}

fn datagram_io_is_allocation(error: &DatagramIoError) -> bool {
    match error {
        DatagramIoError::Allocation(_) => true,
        DatagramIoError::Receive(failure) => {
            matches!(failure.error, DatagramReceiveError::AllocationFailed)
        }
        DatagramIoError::Send {
            error: DatagramSendError::AllocationFailed,
            ..
        } => true,
        DatagramIoError::State(_)
        | DatagramIoError::Connection(_)
        | DatagramIoError::Send { .. } => false,
    }
}

fn established_driver_error_kind(error: &ConnectionDriverError) -> ConnectionErrorKind {
    match error {
        ConnectionDriverError::ControlSend(error) => {
            ConnectionErrorKind::EstablishedControl(ProfileBootstrapFailure::from(&error.error))
        }
        ConnectionDriverError::ControlReceive(error) => {
            ConnectionErrorKind::EstablishedControl(ProfileBootstrapFailure::from(error))
        }
        ConnectionDriverError::State(ConnectionDriverStateError::Terminal) => {
            ConnectionErrorKind::State(ConnectionStateError::Terminal)
        }
        ConnectionDriverError::Reliable(ReliableIoError::Allocation(_))
        | ConnectionDriverError::Reliable(ReliableIoError::State(
            ReliableIoStateError::CapacityOverflow,
        ))
        | ConnectionDriverError::Reliable(ReliableIoError::InboundConstruction(
            ReceiveError::AllocationFailed,
        ))
        | ConnectionDriverError::Datagram(DatagramIoError::Allocation(_))
        | ConnectionDriverError::ReceivedFlowControl(FlowControlError::Allocation(_))
        | ConnectionDriverError::FailurePreparation(FlowControlError::Allocation(_)) => {
            ConnectionErrorKind::EstablishedResource
        }
        ConnectionDriverError::Reliable(ReliableIoError::Connection(_))
        | ConnectionDriverError::Datagram(DatagramIoError::Connection(_))
        | ConnectionDriverError::Datagram(DatagramIoError::Send {
            error: DatagramSendError::ProfileUnavailable | DatagramSendError::ConnectionLost,
            ..
        })
        | ConnectionDriverError::DatagramProfileUnavailable => {
            ConnectionErrorKind::EstablishedTransport
        }
        ConnectionDriverError::State(ConnectionDriverStateError::ControlSendBusy)
        | ConnectionDriverError::OutboundOpen(_)
        | ConnectionDriverError::InboundAdmission(_)
        | ConnectionDriverError::DatagramSubmission(_) => ConnectionErrorKind::UnexpectedCoreState,
        ConnectionDriverError::ReceivedFlowControl(_)
        | ConnectionDriverError::Reliable(_)
        | ConnectionDriverError::Datagram(_)
        | ConnectionDriverError::FailurePreparation(_) => ConnectionErrorKind::EstablishedProtocol,
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
                ConnectionError::classified(ConnectionErrorKind::LocalNegotiation {
                    outcome: outcome.into(),
                    report: NegotiationReportStatus::Unavailable,
                }),
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
                ConnectionError::classified(ConnectionErrorKind::RemoteNegotiation(outcome.into())),
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
                ConnectionError::classified(ConnectionErrorKind::LocalNegotiation {
                    outcome: outcome.into(),
                    report: NegotiationReportStatus::Unavailable,
                }),
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
            ConnectionError::classified(ConnectionErrorKind::LocalNegotiation {
                outcome: outcome.into(),
                report: NegotiationReportStatus::Unavailable,
            })
        }
        NegotiationControlError::ProfileProtocol(error) => {
            close_negotiation_protocol_error(&core.profile.connection);
            ConnectionError::diagnosed(
                ConnectionErrorKind::NegotiationProtocol,
                ConnectionDiagnostic::negotiation(NegotiationControlError::ProfileProtocol(error)),
            )
        }
        NegotiationControlError::ManagerState(error) => {
            close_negotiation_failed(&core.profile.connection);
            ConnectionError::diagnosed(
                ConnectionErrorKind::ManagerState,
                ConnectionDiagnostic::negotiation(NegotiationControlError::ManagerState(error)),
            )
        }
        NegotiationControlError::UnexpectedCoreStatus(status) => {
            close_negotiation_failed(&core.profile.connection);
            ConnectionError::diagnosed(
                ConnectionErrorKind::UnexpectedCoreState,
                ConnectionDiagnostic::negotiation(NegotiationControlError::UnexpectedCoreStatus(
                    status,
                )),
            )
        }
    }
}

fn control_connection_error(
    error: ProfileBootstrapError,
    cleanup_error: Option<NegotiationControlError>,
) -> ConnectionError {
    let kind = ConnectionErrorKind::Control {
        failure: ProfileBootstrapFailure::from(&error),
        cleanup_failed: cleanup_error.is_some(),
    };
    ConnectionError::diagnosed(kind, ConnectionDiagnostic::control(error, cleanup_error))
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
    use runen_net::delivery::{
        DeliveryFlowHandle, FlowTerminationReason as CoreFlowTerminationReason,
    };

    use super::*;

    fn assert_owned_send<T: Send + 'static>() {}

    fn key(direction: FlowDirection, handle: u64) -> DeliveryFlowKey {
        DeliveryFlowKey::new(
            ConnectionHandle::new(1),
            direction,
            DeliveryFlowHandle::new(handle),
        )
    }

    fn termination(key: DeliveryFlowKey) -> FlowTermination {
        FlowTermination {
            key,
            reason: CoreFlowTerminationReason::ReliableCustodyLost,
            pending_messages: 1,
            reliable_obligation_failed: true,
        }
    }

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

    #[test]
    fn established_progress_projects_core_keyed_data_and_drop_events() {
        let inbound = key(FlowDirection::Inbound, 9);
        let data = map_established_progress(
            ConnectionHandle::new(1),
            EstablishedConnectionProgress::Reliable(ActiveReliableProgress::Inbound(
                ReceiveProgress::MessagesBuffered {
                    key: inbound,
                    count: 3,
                },
            )),
        )
        .unwrap()
        .unwrap();
        match data {
            ConnectionEvent::DataReady {
                key,
                buffered_messages,
                local_pressure_drops,
            } => {
                assert_eq!(key, inbound);
                assert_eq!(buffered_messages, 3);
                assert_eq!(local_pressure_drops, 0);
            }
            event => panic!("unexpected event: {event:?}"),
        }

        let dropped = map_established_progress(
            ConnectionHandle::new(1),
            EstablishedConnectionProgress::DatagramInbound(DatagramReceiveOutcome::Core {
                key: inbound,
                outcome: ReceiveOutcome::DroppedByPressure {
                    local_pressure_drops: 2,
                },
            }),
        )
        .unwrap()
        .unwrap();
        match dropped {
            ConnectionEvent::UnreliableReceiveDropped {
                key,
                reason:
                    UnreliableReceiveDropReason::Pressure {
                        local_pressure_drops,
                    },
            } => {
                assert_eq!(key, inbound);
                assert_eq!(local_pressure_drops, 2);
            }
            event => panic!("unexpected event: {event:?}"),
        }
    }

    #[test]
    fn control_report_effect_preserves_local_termination_evidence() {
        let key = key(FlowDirection::Outbound, 10);
        let termination = termination(key);
        let event = map_control_send_effect(FlowControlSendEffect::ReportOnlyTermination {
            flow_id: crate::wire::FlowId::new(WireSide::Client, 0).unwrap(),
            key,
            reason: crate::wire::FlowTerminateReason::ReliableDeliveryFailure,
            termination: Some(termination),
        })
        .unwrap();

        match event {
            ConnectionEvent::FlowTerminated {
                key: event_key,
                origin,
                cause,
                termination: Some(event_termination),
            } => {
                assert_eq!(event_key, key);
                assert_eq!(origin, FlowTerminationOrigin::Local);
                assert_eq!(cause, FlowTerminationCause::ReliableDeliveryFailure);
                assert_eq!(event_termination, termination);
            }
            event => panic!("unexpected event: {event:?}"),
        }
    }
}
