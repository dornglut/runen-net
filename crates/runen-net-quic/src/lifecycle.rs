use std::net::SocketAddr;

use quinn::{ConnectError, Connection, ConnectionError, ReadError, ReadExactError, WriteError};
use runen_net::{
    delivery::{DeliveryEndpoint, FlowTermination},
    identity::ConnectionHandle,
    protocol::{
        CompatibilityOffer, NegotiatedContract, NegotiationManager, NegotiationManagerError,
        NegotiationRequirements, NegotiationStatus,
    },
};

use crate::{
    control::{
        ControlFrame, ControlFrameError, ControlFrameType, ProfileBootstrapError,
        ProfileReadyConnection, ProfileReadyParts, ValidatedControlProfile,
        bootstrap_client_control, bootstrap_server_control, confirm_profile_transport,
    },
    endpoint::{
        ConfiguredEndpoint, ConnectionAdmissionError, ConnectionSlotPermit,
        ValidatedEndpointResources,
    },
    flow_control::{FlowControl, FlowControlConfigError},
    negotiation::{
        NegotiationControlError, NegotiationExchange, NegotiationOutcome, NegotiationProgress,
        NegotiationProtocolError,
    },
    wire::{ApplicationErrorCode, WireSide},
};

const PROFILE_BOOTSTRAP_CLOSE_REASON: &[u8] = b"profile bootstrap failed";
const PROFILE_CONTROL_CLOSE_REASON: &[u8] = b"profile control failed";
const NEGOTIATION_CLOSE_REASON: &[u8] = b"negotiation failed";

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum ProfilePreflightError {
    WrongEndpointSide {
        expected: WireSide,
        actual: WireSide,
    },
    ResourceAuthorityMismatch,
}

#[derive(Debug)]
pub(super) enum ProfileConnectionError {
    Preflight(ProfilePreflightError),
    Admission(ConnectionAdmissionError),
    Connect(ConnectError),
    Handshake(ConnectionError),
    Bootstrap(ProfileBootstrapError),
}

#[derive(Debug)]
pub(super) struct AdmittedProfileReadyConnection {
    profile_ready: ProfileReadyConnection,
    connection_permit: ConnectionSlotPermit,
}

#[derive(Debug)]
struct NegotiationCore {
    connection: ConnectionHandle,
    profile: ProfileReadyParts,
    connection_permit: ConnectionSlotPermit,
    exchange: NegotiationExchange,
}

#[must_use = "negotiation state must be progressed or synchronously aborted"]
#[derive(Debug)]
pub(super) struct NegotiatingConnection {
    core: NegotiationCore,
    received: Option<ControlFrame>,
}

#[must_use = "received negotiation control must be synchronously processed or aborted"]
#[derive(Debug)]
pub(super) struct ReceivedNegotiationFrame {
    core: NegotiationCore,
    frame: ControlFrame,
}

#[must_use = "authority selection state must be selected or synchronously aborted"]
#[derive(Debug)]
pub(super) struct AuthoritySelectionRequired {
    core: NegotiationCore,
}

#[must_use = "pending negotiation control must be sent and completed or synchronously aborted"]
#[derive(Debug)]
pub(super) struct PendingNegotiationSend {
    core: NegotiationCore,
    pending: PendingControlSend,
    disposition: PendingSendDisposition,
}

#[must_use = "established negotiation state owns connection-scoped Core negotiation state"]
#[derive(Debug)]
pub(super) struct EstablishedNegotiatedConnection {
    core: NegotiationCore,
}

#[must_use = "flow-controlled connection owns the sole delivery-flow control state"]
#[derive(Debug)]
pub(super) struct FlowControlledConnection {
    core: NegotiationCore,
    flow_control: FlowControl,
}

#[derive(Debug)]
pub(super) struct FlowControlActivationError {
    pub(super) error: FlowControlConfigError,
    pub(super) established: Box<EstablishedNegotiatedConnection>,
}

#[must_use = "connection teardown result contains host identity and Core cleanup evidence"]
#[derive(Debug)]
pub(super) struct ConnectionTeardown {
    pub(super) connection: ConnectionHandle,
    pub(super) flow_terminations: Vec<FlowTermination>,
    pub(super) negotiation_cleanup_error: Option<NegotiationControlError>,
}

#[derive(Debug)]
pub(super) enum NegotiationTransition {
    Negotiating(NegotiatingConnection),
    AuthoritySelection(AuthoritySelectionRequired),
    PendingSend(PendingNegotiationSend),
    Established(EstablishedNegotiatedConnection),
}

#[derive(Debug)]
pub(super) enum NegotiationSendCompletion {
    Negotiating(NegotiatingConnection),
    Established(EstablishedNegotiatedConnection),
    LocalFailure(NegotiationOutcome),
}

#[derive(Debug)]
pub(super) enum NegotiationLifecycleError {
    LocalFailure {
        outcome: NegotiationOutcome,
        report_error: Option<ProfileBootstrapError>,
        cleanup_error: Option<NegotiationControlError>,
    },
    RemoteFailure(NegotiationOutcome),
    ProfileProtocol(NegotiationProtocolError),
    ManagerState(NegotiationManagerError),
    UnexpectedCoreStatus(NegotiationStatus),
    IoAbort {
        error: Option<ProfileBootstrapError>,
        cleanup_error: Option<NegotiationControlError>,
    },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum NegotiationReceiveStateError {
    FrameAlreadyReceived,
}

#[derive(Debug)]
pub(super) enum NegotiationReceiveError {
    State(NegotiationReceiveStateError),
    Control(ProfileBootstrapError),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum NegotiationSendStateError {
    FrameAlreadyConsumed,
}

#[derive(Debug)]
pub(super) enum NegotiationPendingSendError {
    State(NegotiationSendStateError),
    Control(ProfileBootstrapError),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum PendingSendDisposition {
    Continue,
    Establish,
    TerminalLocalFailure(NegotiationOutcome),
}

#[derive(Debug)]
struct PendingControlSend {
    frame: Option<ControlFrame>,
    sent: bool,
}

impl PendingControlSend {
    fn new(frame: ControlFrame) -> Self {
        Self {
            frame: Some(frame),
            sent: false,
        }
    }

    fn take(&mut self) -> Result<ControlFrame, NegotiationSendStateError> {
        self.frame
            .take()
            .ok_or(NegotiationSendStateError::FrameAlreadyConsumed)
    }

    fn mark_sent(&mut self) {
        debug_assert!(!self.sent);
        self.sent = true;
    }

    const fn is_complete(&self) -> bool {
        self.sent
    }
}

impl NegotiationCore {
    fn teardown(
        mut self,
        manager: &mut NegotiationManager,
        delivery: &mut DeliveryEndpoint,
    ) -> ConnectionTeardown {
        let connection = self.connection;
        let flow_terminations = delivery.terminate_connection(connection);
        let negotiation_cleanup_error = self.exchange.abort(manager).err();
        ConnectionTeardown {
            connection,
            flow_terminations,
            negotiation_cleanup_error,
        }
    }
}

pub(super) fn begin_negotiation(
    admitted: AdmittedProfileReadyConnection,
    connection: ConnectionHandle,
    manager: &mut NegotiationManager,
    offer: CompatibilityOffer,
) -> Result<PendingNegotiationSend, NegotiationLifecycleError> {
    let AdmittedProfileReadyConnection {
        profile_ready,
        connection_permit,
    } = admitted;
    let mut exchange = NegotiationExchange::from_profile(connection, &profile_ready);
    let profile = profile_ready.into_parts();
    let frame = match exchange.prepare_offer(manager, offer) {
        Ok(frame) => frame,
        Err(NegotiationControlError::LocalFailure { outcome, report }) => {
            let core = NegotiationCore {
                connection,
                profile,
                connection_permit,
                exchange,
            };
            return pending_local_failure(core, outcome, report);
        }
        Err(error) => {
            let core = NegotiationCore {
                connection,
                profile,
                connection_permit,
                exchange,
            };
            return Err(terminal_negotiation_error(core, error));
        }
    };

    Ok(PendingNegotiationSend::new(
        NegotiationCore {
            connection,
            profile,
            connection_permit,
            exchange,
        },
        frame,
        PendingSendDisposition::Continue,
    ))
}

impl NegotiatingConnection {
    pub(super) async fn receive(&mut self) -> Result<(), NegotiationReceiveError> {
        if self.received.is_some() {
            return Err(NegotiationReceiveError::State(
                NegotiationReceiveStateError::FrameAlreadyReceived,
            ));
        }
        let frame = self
            .core
            .profile
            .receiver
            .receive_frame()
            .await
            .map_err(NegotiationReceiveError::Control)?;
        self.received = Some(frame);
        Ok(())
    }

    pub(super) fn into_received(mut self) -> Result<ReceivedNegotiationFrame, Box<Self>> {
        let Some(frame) = self.received.take() else {
            return Err(Box::new(self));
        };
        Ok(ReceivedNegotiationFrame {
            core: self.core,
            frame,
        })
    }

    pub(super) fn abort_after_control_error(
        self,
        manager: &mut NegotiationManager,
        error: ProfileBootstrapError,
    ) -> NegotiationLifecycleError {
        abort_negotiation(self.core, manager, Some(error))
    }

    pub(super) fn abort_cancelled(
        self,
        manager: &mut NegotiationManager,
    ) -> NegotiationLifecycleError {
        abort_negotiation(self.core, manager, None)
    }
}

impl ReceivedNegotiationFrame {
    pub(super) fn process(
        mut self,
        manager: &mut NegotiationManager,
        requirements: &NegotiationRequirements,
    ) -> Result<NegotiationTransition, NegotiationLifecycleError> {
        let result = self
            .core
            .exchange
            .receive(manager, requirements, self.frame);
        transition_from_progress(self.core, result)
    }

    pub(super) fn abort_cancelled(
        self,
        manager: &mut NegotiationManager,
    ) -> NegotiationLifecycleError {
        abort_negotiation(self.core, manager, None)
    }
}

impl AuthoritySelectionRequired {
    pub(super) const fn connection(&self) -> ConnectionHandle {
        self.core.connection
    }

    pub(super) fn select(
        mut self,
        manager: &mut NegotiationManager,
        contract: NegotiatedContract,
        requirements: &NegotiationRequirements,
    ) -> Result<PendingNegotiationSend, NegotiationLifecycleError> {
        match self
            .core
            .exchange
            .propose_authority(manager, contract, requirements)
        {
            Ok(frame) => Ok(PendingNegotiationSend::new(
                self.core,
                frame,
                PendingSendDisposition::Continue,
            )),
            Err(NegotiationControlError::LocalFailure { outcome, report }) => {
                pending_local_failure(self.core, outcome, report)
            }
            Err(error) => Err(terminal_negotiation_error(self.core, error)),
        }
    }

    pub(super) fn abort_cancelled(
        self,
        manager: &mut NegotiationManager,
    ) -> NegotiationLifecycleError {
        abort_negotiation(self.core, manager, None)
    }
}

impl PendingNegotiationSend {
    fn new(
        core: NegotiationCore,
        frame: ControlFrame,
        disposition: PendingSendDisposition,
    ) -> Self {
        Self {
            core,
            pending: PendingControlSend::new(frame),
            disposition,
        }
    }

    pub(super) async fn send(&mut self) -> Result<(), NegotiationPendingSendError> {
        let frame = self
            .pending
            .take()
            .map_err(NegotiationPendingSendError::State)?;
        self.core
            .profile
            .sender
            .send_frame(frame.frame_type, &frame.body)
            .await
            .map_err(NegotiationPendingSendError::Control)?;
        self.pending.mark_sent();
        Ok(())
    }

    pub(super) fn complete(self) -> Result<NegotiationSendCompletion, Box<Self>> {
        if !self.pending.is_complete() {
            return Err(Box::new(self));
        }
        let Self {
            core, disposition, ..
        } = self;
        match disposition {
            PendingSendDisposition::Continue => Ok(NegotiationSendCompletion::Negotiating(
                NegotiatingConnection {
                    core,
                    received: None,
                },
            )),
            PendingSendDisposition::Establish => Ok(NegotiationSendCompletion::Established(
                EstablishedNegotiatedConnection { core },
            )),
            PendingSendDisposition::TerminalLocalFailure(outcome) => {
                close_negotiation_failed(&core.profile.connection);
                Ok(NegotiationSendCompletion::LocalFailure(outcome))
            }
        }
    }

    pub(super) fn abort_after_control_error(
        mut self,
        manager: &mut NegotiationManager,
        error: ProfileBootstrapError,
    ) -> NegotiationLifecycleError {
        if let PendingSendDisposition::TerminalLocalFailure(outcome) = self.disposition {
            let cleanup_error = self.core.exchange.abort(manager).err();
            close_negotiation_failed(&self.core.profile.connection);
            return NegotiationLifecycleError::LocalFailure {
                outcome,
                report_error: Some(error),
                cleanup_error,
            };
        }
        abort_negotiation(self.core, manager, Some(error))
    }

    pub(super) fn abort_cancelled(
        mut self,
        manager: &mut NegotiationManager,
    ) -> NegotiationLifecycleError {
        if let PendingSendDisposition::TerminalLocalFailure(outcome) = self.disposition {
            let cleanup_error = self.core.exchange.abort(manager).err();
            close_negotiation_failed(&self.core.profile.connection);
            return NegotiationLifecycleError::LocalFailure {
                outcome,
                report_error: None,
                cleanup_error,
            };
        }
        abort_negotiation(self.core, manager, None)
    }
}

impl EstablishedNegotiatedConnection {
    pub(super) fn into_flow_control(
        self,
    ) -> Result<FlowControlledConnection, FlowControlActivationError> {
        let flow_control =
            match FlowControl::from_profile_parts(self.core.connection, &self.core.profile) {
                Ok(flow_control) => flow_control,
                Err(error) => {
                    return Err(FlowControlActivationError {
                        error,
                        established: Box::new(self),
                    });
                }
            };
        Ok(FlowControlledConnection {
            core: self.core,
            flow_control,
        })
    }

    pub(super) fn teardown(
        self,
        manager: &mut NegotiationManager,
        delivery: &mut DeliveryEndpoint,
    ) -> ConnectionTeardown {
        self.core.teardown(manager, delivery)
    }
}

impl FlowControlledConnection {
    pub(super) fn teardown(
        self,
        manager: &mut NegotiationManager,
        delivery: &mut DeliveryEndpoint,
    ) -> ConnectionTeardown {
        let Self {
            core,
            flow_control: _flow_control,
        } = self;
        core.teardown(manager, delivery)
    }
}

fn transition_from_progress(
    core: NegotiationCore,
    result: Result<NegotiationProgress, NegotiationControlError>,
) -> Result<NegotiationTransition, NegotiationLifecycleError> {
    match result {
        Ok(NegotiationProgress::Waiting) => {
            Ok(NegotiationTransition::Negotiating(NegotiatingConnection {
                core,
                received: None,
            }))
        }
        Ok(NegotiationProgress::AuthoritySelectionRequired) => Ok(
            NegotiationTransition::AuthoritySelection(AuthoritySelectionRequired { core }),
        ),
        Ok(NegotiationProgress::Send(frame)) => {
            let disposition = controller_send_disposition(frame.frame_type);
            Ok(NegotiationTransition::PendingSend(
                PendingNegotiationSend::new(core, frame, disposition),
            ))
        }
        Ok(NegotiationProgress::Established) => Ok(NegotiationTransition::Established(
            EstablishedNegotiatedConnection { core },
        )),
        Ok(NegotiationProgress::RemoteFailed(outcome)) => {
            close_negotiation_failed(&core.profile.connection);
            Err(NegotiationLifecycleError::RemoteFailure(outcome))
        }
        Err(NegotiationControlError::LocalFailure { outcome, report }) => {
            pending_local_failure(core, outcome, report).map(NegotiationTransition::PendingSend)
        }
        Err(error) => Err(terminal_negotiation_error(core, error)),
    }
}

fn pending_local_failure(
    core: NegotiationCore,
    outcome: NegotiationOutcome,
    report: Option<ControlFrame>,
) -> Result<PendingNegotiationSend, NegotiationLifecycleError> {
    match report {
        Some(frame) => Ok(PendingNegotiationSend::new(
            core,
            frame,
            PendingSendDisposition::TerminalLocalFailure(outcome),
        )),
        None => {
            close_negotiation_failed(&core.profile.connection);
            Err(NegotiationLifecycleError::LocalFailure {
                outcome,
                report_error: None,
                cleanup_error: None,
            })
        }
    }
}

fn terminal_negotiation_error(
    core: NegotiationCore,
    error: NegotiationControlError,
) -> NegotiationLifecycleError {
    match error {
        NegotiationControlError::LocalFailure { outcome, .. } => {
            close_negotiation_failed(&core.profile.connection);
            NegotiationLifecycleError::LocalFailure {
                outcome,
                report_error: None,
                cleanup_error: None,
            }
        }
        NegotiationControlError::ProfileProtocol(error) => {
            core.profile.connection.close(
                ApplicationErrorCode::ProfileProtocolError.quinn(),
                NEGOTIATION_CLOSE_REASON,
            );
            NegotiationLifecycleError::ProfileProtocol(error)
        }
        NegotiationControlError::ManagerState(error) => {
            close_negotiation_failed(&core.profile.connection);
            NegotiationLifecycleError::ManagerState(error)
        }
        NegotiationControlError::UnexpectedCoreStatus(status) => {
            close_negotiation_failed(&core.profile.connection);
            NegotiationLifecycleError::UnexpectedCoreStatus(status)
        }
    }
}

fn abort_negotiation(
    mut core: NegotiationCore,
    manager: &mut NegotiationManager,
    error: Option<ProfileBootstrapError>,
) -> NegotiationLifecycleError {
    match error.as_ref() {
        Some(error) => close_for_post_profile_control_error(&core.profile.connection, error),
        None => close_negotiation_failed(&core.profile.connection),
    }
    let cleanup_error = core.exchange.abort(manager).err();
    NegotiationLifecycleError::IoAbort {
        error,
        cleanup_error,
    }
}

const fn controller_send_disposition(frame_type: ControlFrameType) -> PendingSendDisposition {
    match frame_type {
        ControlFrameType::NegotiationEstablished => PendingSendDisposition::Establish,
        _ => PendingSendDisposition::Continue,
    }
}

fn close_negotiation_failed(connection: &Connection) {
    connection.close(
        ApplicationErrorCode::NegotiationFailed.quinn(),
        NEGOTIATION_CLOSE_REASON,
    );
}

fn close_for_post_profile_control_error(connection: &Connection, error: &ProfileBootstrapError) {
    if let Some(code) = post_profile_close_code(error) {
        connection.close(code.quinn(), PROFILE_CONTROL_CLOSE_REASON);
    }
}

fn post_profile_close_code(error: &ProfileBootstrapError) -> Option<ApplicationErrorCode> {
    match error {
        ProfileBootstrapError::SettingsAfterReady => {
            Some(ApplicationErrorCode::ProfileProtocolError)
        }
        _ => bootstrap_close_code(error),
    }
}

pub(super) async fn connect_profile_ready(
    endpoint: &ConfiguredEndpoint,
    remote_address: SocketAddr,
    server_name: &str,
    profile: ValidatedControlProfile,
) -> Result<AdmittedProfileReadyConnection, ProfileConnectionError> {
    validate_preflight(
        endpoint.side(),
        endpoint.resources(),
        profile,
        WireSide::Client,
    )
    .map_err(ProfileConnectionError::Preflight)?;

    let permit = endpoint
        .try_acquire_connection_slot()
        .map_err(ProfileConnectionError::Admission)?;
    let connecting = endpoint
        .endpoint()
        .connect(remote_address, server_name)
        .map_err(ProfileConnectionError::Connect)?;
    let connection = connecting
        .await
        .map_err(ProfileConnectionError::Handshake)?;

    establish_profile_ready(connection, WireSide::Client, profile, permit).await
}

pub(super) async fn accept_profile_ready(
    endpoint: &ConfiguredEndpoint,
    profile: ValidatedControlProfile,
) -> Result<Option<AdmittedProfileReadyConnection>, ProfileConnectionError> {
    validate_preflight(
        endpoint.side(),
        endpoint.resources(),
        profile,
        WireSide::Server,
    )
    .map_err(ProfileConnectionError::Preflight)?;

    let Some(incoming) = endpoint.endpoint().accept().await else {
        return Ok(None);
    };
    let permit = match endpoint.try_acquire_connection_slot() {
        Ok(permit) => permit,
        Err(error) => {
            incoming.refuse();
            return Err(ProfileConnectionError::Admission(error));
        }
    };
    let connecting = incoming
        .accept()
        .map_err(ProfileConnectionError::Handshake)?;
    let connection = connecting
        .await
        .map_err(ProfileConnectionError::Handshake)?;

    establish_profile_ready(connection, WireSide::Server, profile, permit)
        .await
        .map(Some)
}

fn validate_preflight(
    actual_side: WireSide,
    endpoint_resources: ValidatedEndpointResources,
    profile: ValidatedControlProfile,
    expected_side: WireSide,
) -> Result<(), ProfilePreflightError> {
    if actual_side != expected_side {
        return Err(ProfilePreflightError::WrongEndpointSide {
            expected: expected_side,
            actual: actual_side,
        });
    }
    if endpoint_resources != profile.resources() {
        return Err(ProfilePreflightError::ResourceAuthorityMismatch);
    }
    Ok(())
}

async fn establish_profile_ready(
    connection: Connection,
    expected_side: WireSide,
    profile: ValidatedControlProfile,
    permit: ConnectionSlotPermit,
) -> Result<AdmittedProfileReadyConnection, ProfileConnectionError> {
    let close_connection = connection.clone();
    let transport = match confirm_profile_transport(connection) {
        Ok(transport) => transport,
        Err(error) => {
            close_for_bootstrap_error(&close_connection, &error);
            return Err(ProfileConnectionError::Bootstrap(error));
        }
    };

    let close_connection = transport.connection().clone();
    let profile_ready = match expected_side {
        WireSide::Client => bootstrap_client_control(transport, profile).await,
        WireSide::Server => bootstrap_server_control(transport, profile).await,
    };
    match profile_ready {
        Ok(profile_ready) => Ok(AdmittedProfileReadyConnection {
            profile_ready,
            connection_permit: permit,
        }),
        Err(error) => {
            close_for_bootstrap_error(&close_connection, &error);
            Err(ProfileConnectionError::Bootstrap(error))
        }
    }
}

fn close_for_bootstrap_error(connection: &Connection, error: &ProfileBootstrapError) {
    if let Some(code) = bootstrap_close_code(error) {
        connection.close(code.quinn(), PROFILE_BOOTSTRAP_CLOSE_REASON);
    }
}

fn bootstrap_close_code(error: &ProfileBootstrapError) -> Option<ApplicationErrorCode> {
    match error {
        ProfileBootstrapError::WrongAlpn
        | ProfileBootstrapError::DatagramUnsupported
        | ProfileBootstrapError::ZeroRttControlStream
        | ProfileBootstrapError::Settings(_)
        | ProfileBootstrapError::UnexpectedFrameBeforeReady(_)
        | ProfileBootstrapError::DuplicateSettings
        | ProfileBootstrapError::PeerRoleMismatch { .. } => {
            Some(ApplicationErrorCode::ProfileProtocolError)
        }
        ProfileBootstrapError::Frame(error) => control_frame_close_code(error),
        ProfileBootstrapError::Connection(_)
        | ProfileBootstrapError::MissingHandshakeData
        | ProfileBootstrapError::UnexpectedHandshakeDataType
        | ProfileBootstrapError::WrongQuicSide { .. }
        | ProfileBootstrapError::SettingsAfterReady
        | ProfileBootstrapError::SettingsOwnedByBootstrap
        | ProfileBootstrapError::ControlChannelPoisoned => None,
    }
}

fn control_frame_close_code(error: &ControlFrameError) -> Option<ApplicationErrorCode> {
    match error {
        ControlFrameError::VarInt(_)
        | ControlFrameError::UnknownFrameType(_)
        | ControlFrameError::BodyTooLarge { .. }
        | ControlFrameError::NegotiationBodyTooLarge { .. }
        | ControlFrameError::Read(ReadExactError::FinishedEarly(_))
        | ControlFrameError::Read(ReadExactError::ReadError(ReadError::Reset(_)))
        | ControlFrameError::Write(WriteError::Stopped(_)) => {
            Some(ApplicationErrorCode::ControlFrameError)
        }
        ControlFrameError::Allocation(_) => Some(ApplicationErrorCode::ResourceLimitError),
        ControlFrameError::Read(ReadExactError::ReadError(ReadError::ConnectionLost(_)))
        | ControlFrameError::Read(ReadExactError::ReadError(ReadError::ClosedStream))
        | ControlFrameError::Read(ReadExactError::ReadError(ReadError::IllegalOrderedRead))
        | ControlFrameError::Read(ReadExactError::ReadError(ReadError::ZeroRttRejected))
        | ControlFrameError::Write(WriteError::ConnectionLost(_))
        | ControlFrameError::Write(WriteError::ClosedStream)
        | ControlFrameError::Write(WriteError::ZeroRttRejected)
        | ControlFrameError::VarIntEncode(_)
        | ControlFrameError::BodyLengthOutOfRange
        | ControlFrameError::ZeroWriteProgress => None,
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use quinn::VarInt;

    use super::*;
    use crate::{
        control::{ControlFrameType, LocalControlLimits, SemanticRole, SettingsError},
        endpoint::EndpointResourceLimits,
        wire::VarIntDecodeError,
    };

    fn resources(max_connections: usize) -> ValidatedEndpointResources {
        EndpointResourceLimits {
            max_connections,
            max_active_incoming_flows: 16,
            udp_payload_ceiling: 1_452,
            stream_receive_window: 64 * 1024,
            connection_receive_window: 256 * 1024,
            send_window: 256 * 1024,
            crypto_buffer_bytes: 32 * 1024,
            datagram_receive_buffer_bytes: 64 * 1024,
            datagram_send_buffer_bytes: 64 * 1024,
            max_idle_timeout: Duration::from_secs(30),
        }
        .validate()
        .unwrap()
    }

    fn profile(resources: ValidatedEndpointResources) -> ValidatedControlProfile {
        LocalControlLimits {
            semantic_role: SemanticRole::Authority,
            max_control_frame_bytes: 64 * 1024,
            max_negotiation_frame_bytes: 32 * 1024,
            max_incoming_message_bytes: 128 * 1024,
        }
        .validate(resources)
        .unwrap()
    }

    #[test]
    fn preflight_requires_expected_side_and_same_resource_authority() {
        let endpoint_resources = resources(4);
        let matching_profile = profile(endpoint_resources);
        assert_eq!(
            validate_preflight(
                WireSide::Client,
                endpoint_resources,
                matching_profile,
                WireSide::Client,
            ),
            Ok(())
        );
        assert_eq!(
            validate_preflight(
                WireSide::Server,
                endpoint_resources,
                matching_profile,
                WireSide::Client,
            ),
            Err(ProfilePreflightError::WrongEndpointSide {
                expected: WireSide::Client,
                actual: WireSide::Server,
            })
        );

        let different_resources = resources(5);
        assert_eq!(
            validate_preflight(
                WireSide::Client,
                endpoint_resources,
                profile(different_resources),
                WireSide::Client,
            ),
            Err(ProfilePreflightError::ResourceAuthorityMismatch)
        );
    }

    #[test]
    fn bootstrap_profile_violations_map_to_profile_protocol_error() {
        let cases = [
            ProfileBootstrapError::WrongAlpn,
            ProfileBootstrapError::DatagramUnsupported,
            ProfileBootstrapError::ZeroRttControlStream,
            ProfileBootstrapError::Settings(SettingsError::EmptyBody),
            ProfileBootstrapError::UnexpectedFrameBeforeReady(ControlFrameType::OpenFlow),
            ProfileBootstrapError::DuplicateSettings,
            ProfileBootstrapError::PeerRoleMismatch {
                expected: SemanticRole::Authority,
                received: SemanticRole::NonAuthority,
            },
        ];
        for error in cases {
            assert_eq!(
                bootstrap_close_code(&error),
                Some(ApplicationErrorCode::ProfileProtocolError)
            );
        }
    }

    #[test]
    fn peer_control_failures_map_to_control_frame_error() {
        let cases = [
            ControlFrameError::VarInt(VarIntDecodeError::NonMinimal),
            ControlFrameError::UnknownFrameType(99),
            ControlFrameError::BodyTooLarge {
                received: 65,
                limit: 64,
            },
            ControlFrameError::Read(ReadExactError::FinishedEarly(0)),
            ControlFrameError::Read(ReadExactError::ReadError(ReadError::Reset(
                VarInt::from_u32(2),
            ))),
            ControlFrameError::Write(WriteError::Stopped(VarInt::from_u32(2))),
        ];
        for error in cases {
            assert_eq!(
                control_frame_close_code(&error),
                Some(ApplicationErrorCode::ControlFrameError)
            );
        }
    }

    #[test]
    fn bounded_inbound_allocation_failure_maps_to_resource_limit() {
        let allocation = Vec::<u8>::new().try_reserve(usize::MAX).unwrap_err();
        assert_eq!(
            control_frame_close_code(&ControlFrameError::Allocation(allocation)),
            Some(ApplicationErrorCode::ResourceLimitError)
        );
    }

    #[test]
    fn transport_loss_and_internal_state_are_not_reclassified() {
        assert_eq!(
            bootstrap_close_code(&ProfileBootstrapError::Connection(
                ConnectionError::TimedOut
            )),
            None
        );
        assert_eq!(
            bootstrap_close_code(&ProfileBootstrapError::WrongQuicSide {
                expected: WireSide::Client,
                actual: WireSide::Server,
            }),
            None
        );
        assert_eq!(
            bootstrap_close_code(&ProfileBootstrapError::MissingHandshakeData),
            None
        );
        assert_eq!(
            control_frame_close_code(&ControlFrameError::Read(ReadExactError::ReadError(
                ReadError::ConnectionLost(ConnectionError::TimedOut),
            ))),
            None
        );
        assert_eq!(
            control_frame_close_code(&ControlFrameError::Write(WriteError::ConnectionLost(
                ConnectionError::TimedOut,
            ))),
            None
        );
        assert_eq!(
            control_frame_close_code(&ControlFrameError::Write(WriteError::ZeroRttRejected)),
            None
        );
        assert_eq!(
            control_frame_close_code(&ControlFrameError::ZeroWriteProgress),
            None
        );
    }

    #[test]
    fn post_profile_settings_is_profile_protocol_error() {
        assert_eq!(
            post_profile_close_code(&ProfileBootstrapError::SettingsAfterReady),
            Some(ApplicationErrorCode::ProfileProtocolError)
        );
        assert_eq!(
            post_profile_close_code(&ProfileBootstrapError::Frame(ControlFrameError::VarInt(
                VarIntDecodeError::NonMinimal,
            ))),
            Some(ApplicationErrorCode::ControlFrameError)
        );
    }

    #[test]
    fn only_final_negotiation_established_send_has_establish_disposition() {
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
    fn pending_control_send_is_one_shot_and_completes_only_after_success_marker() {
        let mut pending = PendingControlSend::new(ControlFrame {
            frame_type: ControlFrameType::NegotiationOffer,
            body: Vec::new(),
        });
        assert!(!pending.is_complete());
        assert!(pending.take().is_ok());
        assert_eq!(
            pending.take().unwrap_err(),
            NegotiationSendStateError::FrameAlreadyConsumed
        );
        assert!(!pending.is_complete());
        pending.mark_sent();
        assert!(pending.is_complete());
    }

    #[test]
    fn terminal_local_failure_disposition_preserves_exact_outcome() {
        let disposition =
            PendingSendDisposition::TerminalLocalFailure(NegotiationOutcome::InvalidSelection);
        assert_eq!(
            disposition,
            PendingSendDisposition::TerminalLocalFailure(NegotiationOutcome::InvalidSelection)
        );
    }
}
