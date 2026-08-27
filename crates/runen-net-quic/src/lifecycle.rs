use std::net::SocketAddr;

use quinn::{ConnectError, Connection, ConnectionError, ReadError, ReadExactError, WriteError};
use runen_net::{
    delivery::{DeliveryEndpoint, FlowTermination},
    identity::ConnectionHandle,
    protocol::NegotiationManager,
};

use crate::{
    control::{
        ControlFrameError, ControlReceiver, ControlSender, ProfileBootstrapError,
        ProfileReadyConnection, ProfileReadyParts, ValidatedControlProfile,
        bootstrap_client_control, bootstrap_server_control, confirm_profile_transport,
    },
    endpoint::{
        ConfiguredEndpoint, ConnectionAdmissionError, ConnectionSlotPermit,
        ValidatedEndpointResources,
    },
    flow_control::{FlowControl, FlowControlConfigError, FlowControlError},
    negotiation::{NegotiationControlError, NegotiationExchange},
    wire::{ApplicationErrorCode, WireSide},
};

const PROFILE_BOOTSTRAP_CLOSE_REASON: &[u8] = b"profile bootstrap failed";
const PROFILE_CONTROL_CLOSE_REASON: &[u8] = b"profile control failed";
const NEGOTIATION_CLOSE_REASON: &[u8] = b"negotiation failed";
const FLOW_CONTROL_CLOSE_REASON: &[u8] = b"flow control failed";

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

impl AdmittedProfileReadyConnection {
    pub(super) fn into_parts(self) -> (ProfileReadyConnection, ConnectionSlotPermit) {
        (self.profile_ready, self.connection_permit)
    }
}

#[derive(Debug)]
struct ConnectionCore {
    connection: ConnectionHandle,
    profile: ProfileReadyParts,
    connection_permit: ConnectionSlotPermit,
    exchange: NegotiationExchange,
}

#[must_use = "established negotiation state owns connection-scoped Core negotiation state"]
#[derive(Debug)]
pub(super) struct EstablishedNegotiatedConnection {
    core: ConnectionCore,
}

#[must_use = "flow-controlled connection owns the sole delivery-flow control state"]
#[derive(Debug)]
pub(super) struct FlowControlledConnection {
    core: ConnectionCore,
    flow_control: FlowControl,
}

#[must_use = "established I/O ownership must be driven or synchronously torn down"]
#[derive(Debug)]
pub(super) struct EstablishedIoParts {
    pub(super) connection: Connection,
    pub(super) sender: ControlSender,
    pub(super) receiver: ControlReceiver,
    pub(super) flow_control: FlowControl,
    pub(super) teardown: EstablishedTeardown,
}

#[must_use = "established teardown ownership must be synchronously consumed"]
#[derive(Debug)]
pub(super) struct EstablishedTeardown {
    connection: ConnectionHandle,
    exchange: NegotiationExchange,
    connection_permit: ConnectionSlotPermit,
}

#[must_use = "driver parts borrow the live connection and independent control directions"]
#[derive(Debug)]
pub(super) struct FlowControlDriverParts<'a> {
    pub(super) connection: &'a Connection,
    pub(super) sender: &'a mut ControlSender,
    pub(super) receiver: &'a mut ControlReceiver,
    pub(super) flow_control: &'a mut FlowControl,
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

impl ConnectionCore {
    fn teardown(
        self,
        manager: &mut NegotiationManager,
        delivery: &mut DeliveryEndpoint,
    ) -> ConnectionTeardown {
        let Self {
            connection,
            profile: _profile,
            connection_permit: _connection_permit,
            exchange,
        } = self;
        teardown_connection(connection, exchange, manager, delivery)
    }

    fn into_established_io(self, flow_control: FlowControl) -> EstablishedIoParts {
        let Self {
            connection: core_connection,
            profile,
            connection_permit,
            exchange,
        } = self;
        let ProfileReadyParts {
            connection,
            side: _,
            profile: _,
            peer_settings: _,
            sender,
            receiver,
        } = profile;
        EstablishedIoParts {
            connection,
            sender,
            receiver,
            flow_control,
            teardown: EstablishedTeardown {
                connection: core_connection,
                exchange,
                connection_permit,
            },
        }
    }
}

impl EstablishedNegotiatedConnection {
    pub(super) fn from_parts(
        connection: ConnectionHandle,
        profile: ProfileReadyParts,
        connection_permit: ConnectionSlotPermit,
        exchange: NegotiationExchange,
    ) -> Self {
        Self {
            core: ConnectionCore {
                connection,
                profile,
                connection_permit,
                exchange,
            },
        }
    }

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

impl EstablishedTeardown {
    pub(super) fn teardown(
        self,
        manager: &mut NegotiationManager,
        delivery: &mut DeliveryEndpoint,
    ) -> ConnectionTeardown {
        let Self {
            connection,
            exchange,
            connection_permit: _connection_permit,
        } = self;
        teardown_connection(connection, exchange, manager, delivery)
    }
}

impl EstablishedIoParts {
    pub(super) fn teardown(
        self,
        manager: &mut NegotiationManager,
        delivery: &mut DeliveryEndpoint,
    ) -> ConnectionTeardown {
        let Self {
            connection: _connection,
            sender: _sender,
            receiver: _receiver,
            flow_control: _flow_control,
            teardown,
        } = self;
        teardown.teardown(manager, delivery)
    }
}

impl FlowControlledConnection {
    pub(super) fn driver_parts(&mut self) -> FlowControlDriverParts<'_> {
        let ProfileReadyParts {
            connection,
            sender,
            receiver,
            ..
        } = &mut self.core.profile;
        FlowControlDriverParts {
            connection,
            sender,
            receiver,
            flow_control: &mut self.flow_control,
        }
    }

    pub(super) fn into_established_io(self) -> EstablishedIoParts {
        let Self { core, flow_control } = self;
        core.into_established_io(flow_control)
    }

    pub(super) fn teardown(
        self,
        manager: &mut NegotiationManager,
        delivery: &mut DeliveryEndpoint,
    ) -> ConnectionTeardown {
        self.into_established_io().teardown(manager, delivery)
    }
}

pub(super) fn teardown_connection(
    connection: ConnectionHandle,
    mut exchange: NegotiationExchange,
    manager: &mut NegotiationManager,
    delivery: &mut DeliveryEndpoint,
) -> ConnectionTeardown {
    let flow_terminations = delivery.terminate_connection(connection);
    let negotiation_cleanup_error = exchange.abort(manager).err();
    ConnectionTeardown {
        connection,
        flow_terminations,
        negotiation_cleanup_error,
    }
}

pub(super) fn close_negotiation_failed(connection: &Connection) {
    connection.close(
        ApplicationErrorCode::NegotiationFailed.quinn(),
        NEGOTIATION_CLOSE_REASON,
    );
}

pub(super) fn close_negotiation_protocol_error(connection: &Connection) {
    connection.close(
        ApplicationErrorCode::ProfileProtocolError.quinn(),
        NEGOTIATION_CLOSE_REASON,
    );
}

pub(super) fn close_for_post_profile_control_error(
    connection: &Connection,
    error: &ProfileBootstrapError,
) {
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

pub(super) fn close_for_received_flow_control_error(
    connection: &Connection,
    error: &FlowControlError,
) {
    if let Some(code) = received_flow_control_close_code(error) {
        connection.close(code.quinn(), FLOW_CONTROL_CLOSE_REASON);
    }
}

fn received_flow_control_close_code(error: &FlowControlError) -> Option<ApplicationErrorCode> {
    match error {
        FlowControlError::UnexpectedFrame(_) => Some(ApplicationErrorCode::ProfileProtocolError),
        FlowControlError::Body(_)
        | FlowControlError::FlowId(_)
        | FlowControlError::WrongResponseSide { .. }
        | FlowControlError::UnknownPendingFlow(_)
        | FlowControlError::UnknownActiveFlow(_)
        | FlowControlError::ReliableNormalUsesFin(_) => {
            Some(ApplicationErrorCode::FlowProtocolError)
        }
        FlowControlError::Allocation(_) => Some(ApplicationErrorCode::ResourceLimitError),
        FlowControlError::InboundDecisionPending(_)
        | FlowControlError::CoreState(_)
        | FlowControlError::LocalEstablishment(_)
        | FlowControlError::Registry(_) => None,
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
        wire::{ControlBodyError, FlowId, FlowIdCursorError, VarIntDecodeError},
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

    fn assert_static<T: 'static>() {}

    #[test]
    fn established_io_ownership_is_move_owned() {
        assert_static::<EstablishedIoParts>();
        assert_static::<EstablishedTeardown>();
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
    fn received_non_flow_frame_maps_to_profile_protocol_error() {
        assert_eq!(
            received_flow_control_close_code(&FlowControlError::UnexpectedFrame(
                ControlFrameType::NegotiationOffer,
            )),
            Some(ApplicationErrorCode::ProfileProtocolError)
        );
    }

    #[test]
    fn received_flow_wire_and_state_errors_map_to_flow_protocol_error() {
        let flow_id = FlowId::new(WireSide::Client, 0).unwrap();
        let cases = [
            FlowControlError::Body(ControlBodyError::TrailingBytes),
            FlowControlError::FlowId(FlowIdCursorError::UnexpectedSequence {
                expected: 0,
                received: 1,
            }),
            FlowControlError::WrongResponseSide {
                expected: WireSide::Client,
                received: WireSide::Server,
            },
            FlowControlError::UnknownPendingFlow(flow_id),
            FlowControlError::UnknownActiveFlow(flow_id),
            FlowControlError::ReliableNormalUsesFin(flow_id),
        ];
        for error in cases {
            assert_eq!(
                received_flow_control_close_code(&error),
                Some(ApplicationErrorCode::FlowProtocolError)
            );
        }
    }

    #[test]
    fn received_flow_allocation_failure_maps_to_resource_limit() {
        let allocation = Vec::<u8>::new().try_reserve(usize::MAX).unwrap_err();
        assert_eq!(
            received_flow_control_close_code(&FlowControlError::Allocation(allocation)),
            Some(ApplicationErrorCode::ResourceLimitError)
        );
    }

    #[test]
    fn local_inbound_scheduling_state_is_not_reclassified_as_peer_fault() {
        let flow_id = FlowId::new(WireSide::Server, 0).unwrap();
        assert_eq!(
            received_flow_control_close_code(&FlowControlError::InboundDecisionPending(flow_id)),
            None
        );
    }
}
