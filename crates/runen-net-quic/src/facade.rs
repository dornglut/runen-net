use std::{fmt, io, net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};

use quinn::rustls::RootCertStore;
pub use rustls_pki_types::{CertificateDer, PrivateKeyDer};

use crate::{
    control::{
        LocalControlLimitError as InternalProfileConfigError, LocalControlLimits,
        ProfileBootstrapError, SemanticRole as InternalSemanticRole, ValidatedControlProfile,
    },
    endpoint::{
        ConfiguredEndpoint, EndpointBuildError as InternalEndpointBuildError,
        EndpointResourceError as InternalEndpointResourceError,
        EndpointResourceLimits as InternalEndpointResourceLimits, ValidatedEndpointResources,
        bind_client_endpoint, bind_server_endpoint,
    },
    lifecycle::{
        AdmittedProfileReadyConnection, ProfileConnectionError as InternalProfileConnectionError,
        accept_profile_ready, connect_profile_ready,
    },
    wire::ApplicationErrorCode,
};

const ENDPOINT_SHUTDOWN_REASON: &[u8] = b"RunenNet endpoint shutdown";
const BASELINE_UDP_PAYLOAD_CEILING: u16 = 1_452;
const BASELINE_STREAM_RECEIVE_WINDOW: u64 = 64 * 1024;
const BASELINE_CONNECTION_RECEIVE_WINDOW: u64 = 256 * 1024;
const BASELINE_SEND_WINDOW: u64 = 256 * 1024;
const BASELINE_CRYPTO_BUFFER_BYTES: usize = 32 * 1024;
const BASELINE_DATAGRAM_RECEIVE_BUFFER_BYTES: usize = 64 * 1024;
const BASELINE_DATAGRAM_SEND_BUFFER_BYTES: usize = 64 * 1024;
const BASELINE_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
const BASELINE_MAX_CONTROL_FRAME_BYTES: usize = 64 * 1024;
const BASELINE_MAX_NEGOTIATION_FRAME_BYTES: usize = 32 * 1024;
const BASELINE_RELIABLE_SCRATCH_BYTES: usize = 4 * 1024;

/// Host-supplied semantic role advertised by the revision-1 RunenNet QUIC profile.
///
/// This role is independent of whether the endpoint is the QUIC client or server.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SemanticRole {
    Authority,
    NonAuthority,
}

impl From<SemanticRole> for InternalSemanticRole {
    fn from(role: SemanticRole) -> Self {
        match role {
            SemanticRole::Authority => Self::Authority,
            SemanticRole::NonAuthority => Self::NonAuthority,
        }
    }
}

/// Explicit finite reliable receive resources carried by one validated profile.
///
/// `max_staging_bytes` is validated against the profile's advertised incoming-message ceiling so
/// a reliable flow cannot be accepted when the local adapter is statically unable to reassemble it.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ReliableReceiveLimits {
    pub scratch_bytes: NonZeroUsize,
    pub max_staging_bytes: NonZeroUsize,
}

/// Explicit finite expert resource policy for one RunenNet QUIC endpoint.
///
/// Normal first-use code may use [`EndpointConfig::baseline`] and supply only the endpoint
/// capacities that remain application policy. This full structure remains available when a host
/// deliberately needs to tune transport windows, buffers, MTU discovery, or idle timeout.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct EndpointResourceLimits {
    pub max_connections: usize,
    pub max_active_incoming_flows: u64,
    pub udp_payload_ceiling: u16,
    pub stream_receive_window: u64,
    pub connection_receive_window: u64,
    pub send_window: u64,
    pub crypto_buffer_bytes: usize,
    pub datagram_receive_buffer_bytes: usize,
    pub datagram_send_buffer_bytes: usize,
    pub max_idle_timeout: Duration,
}

impl EndpointResourceLimits {
    pub fn validate(self) -> Result<EndpointConfig, EndpointResourceError> {
        let inner = InternalEndpointResourceLimits {
            max_connections: self.max_connections,
            max_active_incoming_flows: self.max_active_incoming_flows,
            udp_payload_ceiling: self.udp_payload_ceiling,
            stream_receive_window: self.stream_receive_window,
            connection_receive_window: self.connection_receive_window,
            send_window: self.send_window,
            crypto_buffer_bytes: self.crypto_buffer_bytes,
            datagram_receive_buffer_bytes: self.datagram_receive_buffer_bytes,
            datagram_send_buffer_bytes: self.datagram_send_buffer_bytes,
            max_idle_timeout: self.max_idle_timeout,
        }
        .validate()
        .map_err(EndpointResourceError::from)?;
        Ok(EndpointConfig {
            limits: self,
            inner,
        })
    }
}

/// Validated endpoint configuration accepted by the invariant-preserving QUIC builders.
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct EndpointConfig {
    limits: EndpointResourceLimits,
    inner: ValidatedEndpointResources,
}

impl EndpointConfig {
    /// Construct the named finite RunenNet revision-1 baseline for ordinary endpoint use.
    ///
    /// The caller still selects the endpoint connection and incoming-flow capacities. The remaining
    /// values are finite implementation tuning and remain inspectable through [`Self::limits`].
    pub fn baseline(
        max_connections: usize,
        max_active_incoming_flows: u64,
    ) -> Result<Self, EndpointResourceError> {
        EndpointResourceLimits {
            max_connections,
            max_active_incoming_flows,
            udp_payload_ceiling: BASELINE_UDP_PAYLOAD_CEILING,
            stream_receive_window: BASELINE_STREAM_RECEIVE_WINDOW,
            connection_receive_window: BASELINE_CONNECTION_RECEIVE_WINDOW,
            send_window: BASELINE_SEND_WINDOW,
            crypto_buffer_bytes: BASELINE_CRYPTO_BUFFER_BYTES,
            datagram_receive_buffer_bytes: BASELINE_DATAGRAM_RECEIVE_BUFFER_BYTES,
            datagram_send_buffer_bytes: BASELINE_DATAGRAM_SEND_BUFFER_BYTES,
            max_idle_timeout: BASELINE_MAX_IDLE_TIMEOUT,
        }
        .validate()
    }

    pub const fn limits(self) -> EndpointResourceLimits {
        self.limits
    }
}

impl fmt::Debug for EndpointConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EndpointConfig")
            .field("limits", &self.limits)
            .finish()
    }
}

/// Public endpoint-resource validation failures.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum EndpointResourceError {
    ZeroConnections,
    ZeroIncomingFlows,
    IncomingFlowsExceedQuicStreamLimit,
    UdpPayloadBelowMinimum,
    UdpPayloadAboveMaximum,
    ZeroStreamReceiveWindow,
    StreamReceiveWindowOutOfRange,
    ZeroConnectionReceiveWindow,
    ConnectionReceiveWindowOutOfRange,
    ConnectionReceiveWindowBelowStream,
    ZeroSendWindow,
    ZeroCryptoBuffer,
    ZeroDatagramReceiveBuffer,
    DatagramReceiveBufferBelowUdpCeiling,
    ZeroDatagramSendBuffer,
    DatagramSendBufferBelowUdpCeiling,
    ZeroIdleTimeout,
    IdleTimeoutOutOfRange,
}

impl From<InternalEndpointResourceError> for EndpointResourceError {
    fn from(error: InternalEndpointResourceError) -> Self {
        match error {
            InternalEndpointResourceError::ZeroConnections => Self::ZeroConnections,
            InternalEndpointResourceError::ZeroIncomingFlows => Self::ZeroIncomingFlows,
            InternalEndpointResourceError::IncomingFlowsExceedQuicStreamLimit => {
                Self::IncomingFlowsExceedQuicStreamLimit
            }
            InternalEndpointResourceError::UdpPayloadBelowMinimum => Self::UdpPayloadBelowMinimum,
            InternalEndpointResourceError::UdpPayloadAboveMaximum => Self::UdpPayloadAboveMaximum,
            InternalEndpointResourceError::ZeroStreamReceiveWindow => Self::ZeroStreamReceiveWindow,
            InternalEndpointResourceError::StreamReceiveWindowOutOfRange => {
                Self::StreamReceiveWindowOutOfRange
            }
            InternalEndpointResourceError::ZeroConnectionReceiveWindow => {
                Self::ZeroConnectionReceiveWindow
            }
            InternalEndpointResourceError::ConnectionReceiveWindowOutOfRange => {
                Self::ConnectionReceiveWindowOutOfRange
            }
            InternalEndpointResourceError::ConnectionReceiveWindowBelowStream => {
                Self::ConnectionReceiveWindowBelowStream
            }
            InternalEndpointResourceError::ZeroSendWindow => Self::ZeroSendWindow,
            InternalEndpointResourceError::ZeroCryptoBuffer => Self::ZeroCryptoBuffer,
            InternalEndpointResourceError::ZeroDatagramReceiveBuffer => {
                Self::ZeroDatagramReceiveBuffer
            }
            InternalEndpointResourceError::DatagramReceiveBufferBelowUdpCeiling => {
                Self::DatagramReceiveBufferBelowUdpCeiling
            }
            InternalEndpointResourceError::ZeroDatagramSendBuffer => Self::ZeroDatagramSendBuffer,
            InternalEndpointResourceError::DatagramSendBufferBelowUdpCeiling => {
                Self::DatagramSendBufferBelowUdpCeiling
            }
            InternalEndpointResourceError::ZeroIdleTimeout => Self::ZeroIdleTimeout,
            InternalEndpointResourceError::IdleTimeoutOutOfRange => Self::IdleTimeoutOutOfRange,
        }
    }
}

impl fmt::Display for EndpointResourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid RunenNet QUIC endpoint resources: {self:?}"
        )
    }
}

impl std::error::Error for EndpointResourceError {}

/// Explicit expert ProfileReady and reliable-receive limits for one endpoint.
///
/// Normal first-use code may use [`ProfileConfig::baseline`]. Expert configuration remains
/// explicit here, but all values are validated together before ProfileReady bootstrap begins.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ProfileLimits {
    pub semantic_role: SemanticRole,
    pub max_control_frame_bytes: usize,
    pub max_negotiation_frame_bytes: usize,
    pub max_incoming_message_bytes: u64,
    pub reliable_receive: ReliableReceiveLimits,
}

impl ProfileLimits {
    pub fn validate(self, endpoint: EndpointConfig) -> Result<ProfileConfig, ProfileConfigError> {
        let inner = LocalControlLimits {
            semantic_role: self.semantic_role.into(),
            max_control_frame_bytes: self.max_control_frame_bytes,
            max_negotiation_frame_bytes: self.max_negotiation_frame_bytes,
            max_incoming_message_bytes: self.max_incoming_message_bytes,
        }
        .validate(endpoint.inner)
        .map_err(ProfileConfigError::from)?;

        let incoming_message_bytes = usize::try_from(self.max_incoming_message_bytes)
            .map_err(|_| ProfileConfigError::IncomingMessageBytesDoNotFitPlatform)?;
        if self.reliable_receive.max_staging_bytes.get() < incoming_message_bytes {
            return Err(ProfileConfigError::ReliableStagingBelowIncomingMessageCeiling);
        }

        Ok(ProfileConfig {
            limits: self,
            inner,
        })
    }
}

/// Validated ProfileReady configuration bound to one validated endpoint resource policy.
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct ProfileConfig {
    limits: ProfileLimits,
    inner: ValidatedControlProfile,
}

impl ProfileConfig {
    /// Construct the named finite RunenNet revision-1 profile baseline.
    ///
    /// The caller selects semantic Authority role and the local incoming-message ceiling. Reliable
    /// staging is derived from that same ceiling so the public facade cannot advertise a reliable
    /// receive capability that its adapter is statically unable to realize.
    pub fn baseline(
        endpoint: EndpointConfig,
        semantic_role: SemanticRole,
        max_incoming_message_bytes: u64,
    ) -> Result<Self, ProfileConfigError> {
        if max_incoming_message_bytes == 0 {
            return Err(ProfileConfigError::ZeroIncomingMessageBytes);
        }
        let max_staging_bytes = usize::try_from(max_incoming_message_bytes)
            .map_err(|_| ProfileConfigError::IncomingMessageBytesDoNotFitPlatform)?;
        let max_staging_bytes = NonZeroUsize::new(max_staging_bytes)
            .ok_or(ProfileConfigError::ZeroIncomingMessageBytes)?;
        let scratch_bytes = NonZeroUsize::new(BASELINE_RELIABLE_SCRATCH_BYTES)
            .expect("RunenNet baseline reliable scratch is non-zero");

        ProfileLimits {
            semantic_role,
            max_control_frame_bytes: BASELINE_MAX_CONTROL_FRAME_BYTES,
            max_negotiation_frame_bytes: BASELINE_MAX_NEGOTIATION_FRAME_BYTES,
            max_incoming_message_bytes,
            reliable_receive: ReliableReceiveLimits {
                scratch_bytes,
                max_staging_bytes,
            },
        }
        .validate(endpoint)
    }

    pub const fn limits(self) -> ProfileLimits {
        self.limits
    }

    const fn reliable_receive_limits(self) -> ReliableReceiveLimits {
        self.limits.reliable_receive
    }
}

impl fmt::Debug for ProfileConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProfileConfig")
            .field("limits", &self.limits)
            .finish()
    }
}

/// Public ProfileReady configuration failures.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ProfileConfigError {
    ControlFrameTooSmall,
    ControlFrameOutOfRange,
    NegotiationFrameTooSmall,
    NegotiationFrameOutOfRange,
    NegotiationExceedsControl,
    ZeroIncomingMessageBytes,
    IncomingMessageBytesOutOfRange,
    IncomingMessageBytesDoNotFitPlatform,
    ReliableStagingBelowIncomingMessageCeiling,
}

impl From<InternalProfileConfigError> for ProfileConfigError {
    fn from(error: InternalProfileConfigError) -> Self {
        match error {
            InternalProfileConfigError::ControlFrameTooSmall => Self::ControlFrameTooSmall,
            InternalProfileConfigError::ControlFrameOutOfRange => Self::ControlFrameOutOfRange,
            InternalProfileConfigError::NegotiationFrameTooSmall => Self::NegotiationFrameTooSmall,
            InternalProfileConfigError::NegotiationFrameOutOfRange => {
                Self::NegotiationFrameOutOfRange
            }
            InternalProfileConfigError::NegotiationExceedsControl => {
                Self::NegotiationExceedsControl
            }
            InternalProfileConfigError::ZeroIncomingMessageBytes => Self::ZeroIncomingMessageBytes,
            InternalProfileConfigError::IncomingMessageBytesOutOfRange => {
                Self::IncomingMessageBytesOutOfRange
            }
        }
    }
}

impl fmt::Display for ProfileConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid RunenNet ProfileReady limits: {self:?}")
    }
}

impl std::error::Error for ProfileConfigError {}

/// Explicit client trust anchors for the RunenNet TLS profile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClientTrust {
    certificates: Vec<CertificateDer<'static>>,
}

impl ClientTrust {
    pub fn new(certificates: Vec<CertificateDer<'static>>) -> Result<Self, TlsMaterialError> {
        if certificates.is_empty() {
            return Err(TlsMaterialError::EmptyClientTrust);
        }
        Ok(Self { certificates })
    }

    fn into_root_store(self) -> Result<Arc<RootCertStore>, EndpointBindError> {
        let mut roots = RootCertStore::empty();
        for certificate in self.certificates {
            roots
                .add(certificate)
                .map_err(|_| EndpointBindError::TrustCertificateRejected)?;
        }
        Ok(Arc::new(roots))
    }
}

/// Explicit server certificate chain and private key for the RunenNet TLS profile.
#[derive(Debug)]
pub struct ServerIdentity {
    certificate_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
}

impl ServerIdentity {
    pub fn new(
        certificate_chain: Vec<CertificateDer<'static>>,
        private_key: PrivateKeyDer<'static>,
    ) -> Result<Self, TlsMaterialError> {
        if certificate_chain.is_empty() {
            return Err(TlsMaterialError::EmptyServerCertificateChain);
        }
        Ok(Self {
            certificate_chain,
            private_key,
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum TlsMaterialError {
    EmptyClientTrust,
    EmptyServerCertificateChain,
}

impl fmt::Display for TlsMaterialError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid RunenNet TLS material: {self:?}")
    }
}

impl std::error::Error for TlsMaterialError {}

/// Failure while constructing a public client or server endpoint.
#[derive(Debug)]
pub enum EndpointBindError {
    TrustCertificateRejected,
    TlsConfigurationRejected,
    TransportConfigurationRejected,
    Io(io::Error),
}

impl From<InternalEndpointBuildError> for EndpointBindError {
    fn from(error: InternalEndpointBuildError) -> Self {
        match error {
            InternalEndpointBuildError::EmptyTrustRoots => Self::TrustCertificateRejected,
            InternalEndpointBuildError::EmptyCertificateChain
            | InternalEndpointBuildError::Rustls(_)
            | InternalEndpointBuildError::MissingInitialCipherSuite(_) => {
                Self::TlsConfigurationRejected
            }
            InternalEndpointBuildError::EndpointConfigRejected => {
                Self::TransportConfigurationRejected
            }
            InternalEndpointBuildError::Io(error) => Self::Io(error),
        }
    }
}

impl fmt::Display for EndpointBindError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TrustCertificateRejected => {
                formatter.write_str("client trust certificate rejected")
            }
            Self::TlsConfigurationRejected => formatter.write_str("TLS configuration rejected"),
            Self::TransportConfigurationRejected => {
                formatter.write_str("QUIC transport configuration rejected")
            }
            Self::Io(error) => write!(formatter, "endpoint I/O failed: {error}"),
        }
    }
}

impl std::error::Error for EndpointBindError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

/// Application-facing ProfileReady bootstrap failure categories.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ProfileBootstrapFailure {
    Transport,
    HandshakeMetadata,
    Alpn,
    DatagramUnsupported,
    Control,
    Settings,
    RoleMismatch,
    ProtocolState,
}

impl From<&ProfileBootstrapError> for ProfileBootstrapFailure {
    fn from(error: &ProfileBootstrapError) -> Self {
        match error {
            ProfileBootstrapError::Connection(_) => Self::Transport,
            ProfileBootstrapError::MissingHandshakeData
            | ProfileBootstrapError::UnexpectedHandshakeDataType => Self::HandshakeMetadata,
            ProfileBootstrapError::WrongAlpn => Self::Alpn,
            ProfileBootstrapError::DatagramUnsupported => Self::DatagramUnsupported,
            ProfileBootstrapError::Frame(_) => Self::Control,
            ProfileBootstrapError::Settings(_) => Self::Settings,
            ProfileBootstrapError::PeerRoleMismatch { .. } => Self::RoleMismatch,
            ProfileBootstrapError::WrongQuicSide { .. }
            | ProfileBootstrapError::ZeroRttControlStream
            | ProfileBootstrapError::UnexpectedFrameBeforeReady(_)
            | ProfileBootstrapError::DuplicateSettings
            | ProfileBootstrapError::SettingsAfterReady
            | ProfileBootstrapError::SettingsOwnedByBootstrap
            | ProfileBootstrapError::ControlChannelPoisoned => Self::ProtocolState,
        }
    }
}

/// Stable application classification for one ProfileReady connection failure.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ProfileConnectionErrorKind {
    ConfigurationMismatch,
    AdmissionAtCapacity,
    ConnectSetup,
    Handshake,
    Bootstrap(ProfileBootstrapFailure),
}

#[derive(Debug)]
struct ProfileConnectionDiagnostic(InternalProfileConnectionError);

impl fmt::Display for ProfileConnectionDiagnostic {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0 {
            InternalProfileConnectionError::Preflight(error) => {
                write!(formatter, "ProfileReady preflight detail: {error:?}")
            }
            InternalProfileConnectionError::Admission(error) => {
                write!(formatter, "ProfileReady admission detail: {error:?}")
            }
            InternalProfileConnectionError::Connect(error) => {
                write!(formatter, "QUIC connect setup detail: {error}")
            }
            InternalProfileConnectionError::Handshake(error) => {
                write!(formatter, "QUIC handshake detail: {error}")
            }
            InternalProfileConnectionError::Bootstrap(error) => {
                write!(formatter, "ProfileReady bootstrap detail: {error:?}")
            }
        }
    }
}

impl std::error::Error for ProfileConnectionDiagnostic {}

/// Public connection failure up to the ProfileReady boundary.
///
/// [`Self::kind`] is the stable application classifier. Lower-level QUIC/bootstrap detail is kept
/// only as an opaque error source for diagnostics and is not a machine-readable public contract.
#[derive(Debug)]
pub struct ProfileConnectionError {
    kind: ProfileConnectionErrorKind,
    diagnostic: Option<ProfileConnectionDiagnostic>,
}

impl ProfileConnectionError {
    pub const fn kind(&self) -> ProfileConnectionErrorKind {
        self.kind
    }
}

impl From<InternalProfileConnectionError> for ProfileConnectionError {
    fn from(error: InternalProfileConnectionError) -> Self {
        match error {
            InternalProfileConnectionError::Admission(_) => Self {
                kind: ProfileConnectionErrorKind::AdmissionAtCapacity,
                diagnostic: None,
            },
            error @ InternalProfileConnectionError::Preflight(_) => Self {
                kind: ProfileConnectionErrorKind::ConfigurationMismatch,
                diagnostic: Some(ProfileConnectionDiagnostic(error)),
            },
            error @ InternalProfileConnectionError::Connect(_) => Self {
                kind: ProfileConnectionErrorKind::ConnectSetup,
                diagnostic: Some(ProfileConnectionDiagnostic(error)),
            },
            error @ InternalProfileConnectionError::Handshake(_) => Self {
                kind: ProfileConnectionErrorKind::Handshake,
                diagnostic: Some(ProfileConnectionDiagnostic(error)),
            },
            InternalProfileConnectionError::Bootstrap(error) => Self {
                kind: ProfileConnectionErrorKind::Bootstrap(ProfileBootstrapFailure::from(&error)),
                diagnostic: Some(ProfileConnectionDiagnostic(
                    InternalProfileConnectionError::Bootstrap(error),
                )),
            },
        }
    }
}

impl fmt::Display for ProfileConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "RunenNet ProfileReady connection failed: {:?}",
            self.kind
        )
    }
}

impl std::error::Error for ProfileConnectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.diagnostic
            .as_ref()
            .map(|diagnostic| diagnostic as &(dyn std::error::Error + 'static))
    }
}

/// Opaque ownership of one connection that completed the RunenNet QUIC ProfileReady gate.
///
/// The validated reliable receive resources are carried with this ownership and consumed by
/// `activate`; callers do not repeat them after the profile advertises its receive ceiling.
pub struct ProfileReadyConnection {
    _inner: AdmittedProfileReadyConnection,
    reliable_receive: ReliableReceiveLimits,
}

impl fmt::Debug for ProfileReadyConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProfileReadyConnection")
            .field("reliable_receive", &self.reliable_receive)
            .finish_non_exhaustive()
    }
}

impl ProfileReadyConnection {
    pub(crate) fn from_profile(
        inner: AdmittedProfileReadyConnection,
        reliable_receive: ReliableReceiveLimits,
    ) -> Self {
        Self {
            _inner: inner,
            reliable_receive,
        }
    }

    pub(super) fn into_parts(self) -> (AdmittedProfileReadyConnection, ReliableReceiveLimits) {
        (self._inner, self.reliable_receive)
    }
}

/// Public client-side owner for the fixed revision-1 RunenNet QUIC profile.
pub struct ClientEndpoint {
    inner: ConfiguredEndpoint,
}

impl fmt::Debug for ClientEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientEndpoint")
            .finish_non_exhaustive()
    }
}

impl ClientEndpoint {
    /// Bind inside an entered Tokio runtime using the validated RunenNet QUIC profile.
    pub fn bind(
        bind_address: SocketAddr,
        config: EndpointConfig,
        trust: ClientTrust,
    ) -> Result<Self, EndpointBindError> {
        let roots = trust.into_root_store()?;
        let inner = bind_client_endpoint(bind_address, config.inner, roots)?;
        Ok(Self { inner })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.endpoint().local_addr()
    }

    pub async fn connect(
        &self,
        remote_address: SocketAddr,
        server_name: &str,
        profile: ProfileConfig,
    ) -> Result<ProfileReadyConnection, ProfileConnectionError> {
        let reliable_receive = profile.reliable_receive_limits();
        connect_profile_ready(&self.inner, remote_address, server_name, profile.inner)
            .await
            .map(|inner| ProfileReadyConnection::from_profile(inner, reliable_receive))
            .map_err(ProfileConnectionError::from)
    }

    /// Stop accepting new work on this endpoint and close its active QUIC connections.
    pub fn close(&self) {
        self.inner.endpoint().close(
            ApplicationErrorCode::NoError.quinn(),
            ENDPOINT_SHUTDOWN_REASON,
        );
    }

    /// Wait until all endpoint connections have become idle after closure/drop.
    pub async fn wait_idle(&self) {
        self.inner.endpoint().wait_idle().await;
    }
}

/// Public server-side owner for the fixed revision-1 RunenNet QUIC profile.
pub struct ServerEndpoint {
    inner: ConfiguredEndpoint,
}

impl fmt::Debug for ServerEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerEndpoint")
            .finish_non_exhaustive()
    }
}

impl ServerEndpoint {
    /// Bind inside an entered Tokio runtime using the validated RunenNet QUIC profile.
    pub fn bind(
        bind_address: SocketAddr,
        config: EndpointConfig,
        identity: ServerIdentity,
    ) -> Result<Self, EndpointBindError> {
        let inner = bind_server_endpoint(
            bind_address,
            config.inner,
            identity.certificate_chain,
            identity.private_key,
        )?;
        Ok(Self { inner })
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.endpoint().local_addr()
    }

    pub async fn accept(
        &self,
        profile: ProfileConfig,
    ) -> Result<Option<ProfileReadyConnection>, ProfileConnectionError> {
        let reliable_receive = profile.reliable_receive_limits();
        accept_profile_ready(&self.inner, profile.inner)
            .await
            .map(|connection| {
                connection
                    .map(|inner| ProfileReadyConnection::from_profile(inner, reliable_receive))
            })
            .map_err(ProfileConnectionError::from)
    }

    /// Stop accepting new work on this endpoint and close its active QUIC connections.
    pub fn close(&self) {
        self.inner.endpoint().close(
            ApplicationErrorCode::NoError.quinn(),
            ENDPOINT_SHUTDOWN_REASON,
        );
    }

    /// Wait until all endpoint connections have become idle after closure/drop.
    pub async fn wait_idle(&self) {
        self.inner.endpoint().wait_idle().await;
    }
}
