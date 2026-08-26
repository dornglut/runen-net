use std::net::SocketAddr;

use quinn::{ConnectError, Connection, ConnectionError, ReadError, ReadExactError, WriteError};

use crate::{
    control::{
        ControlFrameError, ProfileBootstrapError, ProfileReadyConnection, ValidatedControlProfile,
        bootstrap_client_control, bootstrap_server_control, confirm_profile_transport,
    },
    endpoint::{
        ConfiguredEndpoint, ConnectionAdmissionError, ConnectionSlotPermit,
        ValidatedEndpointResources,
    },
    wire::{ApplicationErrorCode, WireSide},
};

const PROFILE_BOOTSTRAP_CLOSE_REASON: &[u8] = b"profile bootstrap failed";

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
}
