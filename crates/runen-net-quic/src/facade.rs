use std::{fmt, io, net::SocketAddr, sync::Arc, time::Duration};

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

/// Explicit finite resource policy for one RunenNet QUIC endpoint.
///
/// RN6 intentionally provides no implicit or recommended defaults. Callers must
/// choose each finite resource authority and validate it before binding.
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

/// Explicit ProfileReady control limits for one endpoint.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ProfileLimits {
    pub semantic_role: SemanticRole,
    pub max_control_frame_bytes: usize,
    pub max_negotiation_frame_bytes: usize,
    pub max_incoming_message_bytes: u64,
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
    pub const fn limits(self) -> ProfileLimits {
        self.limits
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

/// Public connection errors up to the ProfileReady boundary.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ProfileConnectionError {
    ConfigurationMismatch,
    AdmissionAtCapacity,
    ConnectSetup,
    Handshake,
    Bootstrap(ProfileBootstrapFailure),
}

impl From<InternalProfileConnectionError> for ProfileConnectionError {
    fn from(error: InternalProfileConnectionError) -> Self {
        match error {
            InternalProfileConnectionError::Preflight(_) => Self::ConfigurationMismatch,
            InternalProfileConnectionError::Admission(_) => Self::AdmissionAtCapacity,
            InternalProfileConnectionError::Connect(_) => Self::ConnectSetup,
            InternalProfileConnectionError::Handshake(_) => Self::Handshake,
            InternalProfileConnectionError::Bootstrap(error) => {
                Self::Bootstrap(ProfileBootstrapFailure::from(&error))
            }
        }
    }
}

impl fmt::Display for ProfileConnectionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "RunenNet ProfileReady connection failed: {self:?}"
        )
    }
}

impl std::error::Error for ProfileConnectionError {}

/// Opaque ownership of one connection that completed the RunenNet QUIC ProfileReady gate.
///
/// RN6B intentionally exposes no control-stream, Quinn-connection, SETTINGS, or
/// negotiation access. A later RN6 slice consumes this value into the public
/// negotiation/established connection owner.
pub struct ProfileReadyConnection {
    _inner: AdmittedProfileReadyConnection,
}

impl fmt::Debug for ProfileReadyConnection {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProfileReadyConnection")
            .finish_non_exhaustive()
    }
}

impl From<AdmittedProfileReadyConnection> for ProfileReadyConnection {
    fn from(inner: AdmittedProfileReadyConnection) -> Self {
        Self { _inner: inner }
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
        connect_profile_ready(&self.inner, remote_address, server_name, profile.inner)
            .await
            .map(ProfileReadyConnection::from)
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
        accept_profile_ready(&self.inner, profile.inner)
            .await
            .map(|connection| connection.map(ProfileReadyConnection::from))
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
