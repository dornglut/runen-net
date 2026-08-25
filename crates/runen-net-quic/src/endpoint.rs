use std::{
    io,
    net::{SocketAddr, UdpSocket},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};

use quinn::{
    ClientConfig, Endpoint, EndpointConfig, IdleTimeout, MtuDiscoveryConfig, ServerConfig,
    TokioRuntime, TransportConfig, VarInt,
    crypto::rustls::{NoInitialCipherSuite, QuicClientConfig, QuicServerConfig},
    rustls::{
        self, RootCertStore,
        pki_types::{CertificateDer, PrivateKeyDer},
    },
};

use crate::wire::WireSide;

const RUNENNET_ALPN: &[u8] = b"runennet/1";
const QUIC_V1: u32 = 0x0000_0001;
const QUIC_MIN_UDP_PAYLOAD: u16 = 1_200;
const QUIC_MAX_UDP_PAYLOAD: u16 = 65_527;
const QUIC_MAX_STREAM_COUNT: u64 = 1 << 60;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) struct EndpointResourceLimits {
    pub(super) max_connections: usize,
    pub(super) max_active_incoming_flows: u64,
    pub(super) udp_payload_ceiling: u16,
    pub(super) stream_receive_window: u64,
    pub(super) connection_receive_window: u64,
    pub(super) send_window: u64,
    pub(super) crypto_buffer_bytes: usize,
    pub(super) datagram_receive_buffer_bytes: usize,
    pub(super) datagram_send_buffer_bytes: usize,
    pub(super) max_idle_timeout: Duration,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum EndpointResourceError {
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

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) struct ValidatedEndpointResources {
    max_connections: NonZeroUsize,
    max_active_incoming_flows: VarInt,
    udp_payload_ceiling: u16,
    stream_receive_window: VarInt,
    connection_receive_window: VarInt,
    send_window: u64,
    crypto_buffer_bytes: NonZeroUsize,
    datagram_receive_buffer_bytes: NonZeroUsize,
    datagram_send_buffer_bytes: NonZeroUsize,
    max_idle_timeout: IdleTimeout,
}

#[derive(Debug)]
pub(super) struct ConfiguredEndpoint {
    endpoint: Endpoint,
    resources: ValidatedEndpointResources,
    side: WireSide,
}

impl ConfiguredEndpoint {
    pub(super) const fn resources(&self) -> ValidatedEndpointResources {
        self.resources
    }

    pub(super) const fn side(&self) -> WireSide {
        self.side
    }

    pub(super) const fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct TransportProfile {
    incoming_bidi_streams: VarInt,
    incoming_uni_streams: VarInt,
    stream_receive_window: VarInt,
    connection_receive_window: VarInt,
    send_window: u64,
    crypto_buffer_bytes: usize,
    datagram_receive_buffer_bytes: usize,
    datagram_send_buffer_bytes: usize,
    max_idle_timeout: IdleTimeout,
    udp_payload_ceiling: u16,
}

impl EndpointResourceLimits {
    pub(super) fn validate(self) -> Result<ValidatedEndpointResources, EndpointResourceError> {
        let max_connections = NonZeroUsize::new(self.max_connections)
            .ok_or(EndpointResourceError::ZeroConnections)?;
        if self.max_active_incoming_flows == 0 {
            return Err(EndpointResourceError::ZeroIncomingFlows);
        }
        if self.max_active_incoming_flows > QUIC_MAX_STREAM_COUNT {
            return Err(EndpointResourceError::IncomingFlowsExceedQuicStreamLimit);
        }
        let max_active_incoming_flows = VarInt::from_u64(self.max_active_incoming_flows)
            .map_err(|_| EndpointResourceError::IncomingFlowsExceedQuicStreamLimit)?;

        if self.udp_payload_ceiling < QUIC_MIN_UDP_PAYLOAD {
            return Err(EndpointResourceError::UdpPayloadBelowMinimum);
        }
        if self.udp_payload_ceiling > QUIC_MAX_UDP_PAYLOAD {
            return Err(EndpointResourceError::UdpPayloadAboveMaximum);
        }

        if self.stream_receive_window == 0 {
            return Err(EndpointResourceError::ZeroStreamReceiveWindow);
        }
        let stream_receive_window = VarInt::from_u64(self.stream_receive_window)
            .map_err(|_| EndpointResourceError::StreamReceiveWindowOutOfRange)?;

        if self.connection_receive_window == 0 {
            return Err(EndpointResourceError::ZeroConnectionReceiveWindow);
        }
        if self.connection_receive_window < self.stream_receive_window {
            return Err(EndpointResourceError::ConnectionReceiveWindowBelowStream);
        }
        let connection_receive_window = VarInt::from_u64(self.connection_receive_window)
            .map_err(|_| EndpointResourceError::ConnectionReceiveWindowOutOfRange)?;

        if self.send_window == 0 {
            return Err(EndpointResourceError::ZeroSendWindow);
        }
        let crypto_buffer_bytes = NonZeroUsize::new(self.crypto_buffer_bytes)
            .ok_or(EndpointResourceError::ZeroCryptoBuffer)?;

        let datagram_receive_buffer_bytes =
            NonZeroUsize::new(self.datagram_receive_buffer_bytes)
                .ok_or(EndpointResourceError::ZeroDatagramReceiveBuffer)?;
        if datagram_receive_buffer_bytes.get() < usize::from(self.udp_payload_ceiling) {
            return Err(EndpointResourceError::DatagramReceiveBufferBelowUdpCeiling);
        }

        let datagram_send_buffer_bytes = NonZeroUsize::new(self.datagram_send_buffer_bytes)
            .ok_or(EndpointResourceError::ZeroDatagramSendBuffer)?;
        if datagram_send_buffer_bytes.get() < usize::from(self.udp_payload_ceiling) {
            return Err(EndpointResourceError::DatagramSendBufferBelowUdpCeiling);
        }

        if self.max_idle_timeout.is_zero() {
            return Err(EndpointResourceError::ZeroIdleTimeout);
        }
        let max_idle_timeout = self
            .max_idle_timeout
            .try_into()
            .map_err(|_| EndpointResourceError::IdleTimeoutOutOfRange)?;

        Ok(ValidatedEndpointResources {
            max_connections,
            max_active_incoming_flows,
            udp_payload_ceiling: self.udp_payload_ceiling,
            stream_receive_window,
            connection_receive_window,
            send_window: self.send_window,
            crypto_buffer_bytes,
            datagram_receive_buffer_bytes,
            datagram_send_buffer_bytes,
            max_idle_timeout,
        })
    }
}

impl ValidatedEndpointResources {
    pub(super) const fn max_connections(self) -> NonZeroUsize {
        self.max_connections
    }

    pub(super) const fn max_active_incoming_flows(self) -> u64 {
        self.max_active_incoming_flows.into_inner()
    }

    pub(super) const fn udp_payload_ceiling(self) -> u16 {
        self.udp_payload_ceiling
    }
}

#[derive(Debug)]
pub(super) enum EndpointBuildError {
    EmptyTrustRoots,
    EmptyCertificateChain,
    Rustls(rustls::Error),
    MissingInitialCipherSuite(NoInitialCipherSuite),
    EndpointConfigRejected,
    Io(io::Error),
}

impl From<io::Error> for EndpointBuildError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

fn build_client_config(
    resources: ValidatedEndpointResources,
    roots: Arc<RootCertStore>,
) -> Result<ClientConfig, EndpointBuildError> {
    let tls = build_client_tls(roots)?;
    let crypto =
        QuicClientConfig::try_from(tls).map_err(EndpointBuildError::MissingInitialCipherSuite)?;
    let mut config = ClientConfig::new(Arc::new(crypto));
    config.version(QUIC_V1);
    config.transport_config(Arc::new(build_transport_config(
        WireSide::Client,
        resources,
    )));
    Ok(config)
}

fn build_server_config(
    resources: ValidatedEndpointResources,
    certificate_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
) -> Result<ServerConfig, EndpointBuildError> {
    let tls = build_server_tls(certificate_chain, private_key)?;
    let crypto =
        QuicServerConfig::try_from(tls).map_err(EndpointBuildError::MissingInitialCipherSuite)?;
    let mut config = ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(Arc::new(build_transport_config(
        WireSide::Server,
        resources,
    )));
    Ok(config)
}

/// Bind a revision-1 RunenNet QUIC client endpoint inside an entered Tokio runtime.
///
/// The endpoint and its default client transport configuration are derived from
/// the same validated resource policy so the UDP/MTU/DATAGRAM relationship
/// cannot be split across independently configured objects.
pub(super) fn bind_client_endpoint(
    bind_addr: SocketAddr,
    resources: ValidatedEndpointResources,
    roots: Arc<RootCertStore>,
) -> Result<ConfiguredEndpoint, EndpointBuildError> {
    let client_config = build_client_config(resources, roots)?;
    let socket = bind_udp_socket(bind_addr)?;
    let mut endpoint = Endpoint::new(
        build_endpoint_config(resources)?,
        None,
        socket,
        Arc::new(TokioRuntime),
    )?;
    endpoint.set_default_client_config(client_config);
    Ok(ConfiguredEndpoint {
        endpoint,
        resources,
        side: WireSide::Client,
    })
}

/// Bind a revision-1 RunenNet QUIC server endpoint inside an entered Tokio runtime.
///
/// TLS identity, endpoint limits, and connection transport limits are composed
/// here so later RN5E slices cannot pair a server endpoint with a transport
/// configuration derived from a different resource authority.
pub(super) fn bind_server_endpoint(
    bind_addr: SocketAddr,
    resources: ValidatedEndpointResources,
    certificate_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
) -> Result<ConfiguredEndpoint, EndpointBuildError> {
    let server_config = build_server_config(resources, certificate_chain, private_key)?;
    let socket = bind_udp_socket(bind_addr)?;
    let endpoint = Endpoint::new(
        build_endpoint_config(resources)?,
        Some(server_config),
        socket,
        Arc::new(TokioRuntime),
    )?;
    Ok(ConfiguredEndpoint {
        endpoint,
        resources,
        side: WireSide::Server,
    })
}

fn bind_udp_socket(bind_addr: SocketAddr) -> io::Result<UdpSocket> {
    let socket = UdpSocket::bind(bind_addr)?;
    socket.set_nonblocking(true)?;
    Ok(socket)
}

fn build_endpoint_config(
    resources: ValidatedEndpointResources,
) -> Result<EndpointConfig, EndpointBuildError> {
    let mut config = EndpointConfig::default();
    config.supported_versions(vec![QUIC_V1]);
    config
        .max_udp_payload_size(resources.udp_payload_ceiling)
        .map_err(|_| EndpointBuildError::EndpointConfigRejected)?;
    Ok(config)
}

fn transport_profile(side: WireSide, resources: ValidatedEndpointResources) -> TransportProfile {
    TransportProfile {
        incoming_bidi_streams: match side {
            WireSide::Client => VarInt::from_u32(0),
            WireSide::Server => VarInt::from_u32(1),
        },
        incoming_uni_streams: resources.max_active_incoming_flows,
        stream_receive_window: resources.stream_receive_window,
        connection_receive_window: resources.connection_receive_window,
        send_window: resources.send_window,
        crypto_buffer_bytes: resources.crypto_buffer_bytes.get(),
        datagram_receive_buffer_bytes: resources.datagram_receive_buffer_bytes.get(),
        datagram_send_buffer_bytes: resources.datagram_send_buffer_bytes.get(),
        max_idle_timeout: resources.max_idle_timeout,
        udp_payload_ceiling: resources.udp_payload_ceiling,
    }
}

fn build_transport_config(
    side: WireSide,
    resources: ValidatedEndpointResources,
) -> TransportConfig {
    let profile = transport_profile(side, resources);
    let mut config = TransportConfig::default();
    config
        .max_concurrent_bidi_streams(profile.incoming_bidi_streams)
        .max_concurrent_uni_streams(profile.incoming_uni_streams)
        .stream_receive_window(profile.stream_receive_window)
        .receive_window(profile.connection_receive_window)
        .send_window(profile.send_window)
        .crypto_buffer_size(profile.crypto_buffer_bytes)
        .datagram_receive_buffer_size(Some(profile.datagram_receive_buffer_bytes))
        .datagram_send_buffer_size(profile.datagram_send_buffer_bytes)
        .max_idle_timeout(Some(profile.max_idle_timeout))
        .min_mtu(QUIC_MIN_UDP_PAYLOAD)
        .initial_mtu(QUIC_MIN_UDP_PAYLOAD);

    let mut mtu_discovery = MtuDiscoveryConfig::default();
    mtu_discovery.upper_bound(profile.udp_payload_ceiling);
    config.mtu_discovery_config(Some(mtu_discovery));
    config
}

fn build_client_tls(roots: Arc<RootCertStore>) -> Result<rustls::ClientConfig, EndpointBuildError> {
    if roots.is_empty() {
        return Err(EndpointBuildError::EmptyTrustRoots);
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(EndpointBuildError::Rustls)?;
    let mut config = builder.with_root_certificates(roots).with_no_client_auth();
    apply_client_profile(&mut config);
    Ok(config)
}

fn build_server_tls(
    certificate_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
) -> Result<rustls::ServerConfig, EndpointBuildError> {
    if certificate_chain.is_empty() {
        return Err(EndpointBuildError::EmptyCertificateChain);
    }

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder = rustls::ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(EndpointBuildError::Rustls)?;
    let mut config = builder
        .with_no_client_auth()
        .with_single_cert(certificate_chain, private_key)
        .map_err(EndpointBuildError::Rustls)?;
    apply_server_profile(&mut config);
    Ok(config)
}

fn apply_client_profile(config: &mut rustls::ClientConfig) {
    config.alpn_protocols = vec![RUNENNET_ALPN.to_vec()];
    config.enable_early_data = false;
}

fn apply_server_profile(config: &mut rustls::ServerConfig) {
    config.alpn_protocols = vec![RUNENNET_ALPN.to_vec()];
    config.max_early_data_size = 0;
}

#[cfg(test)]
mod tests {
    use quinn::rustls::{
        pki_types::{Der, TrustAnchor},
        server::ResolvesServerCertUsingSni,
    };

    use super::*;

    fn limits() -> EndpointResourceLimits {
        EndpointResourceLimits {
            max_connections: 8,
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
    }

    fn non_empty_roots() -> Arc<RootCertStore> {
        Arc::new(RootCertStore {
            roots: vec![TrustAnchor {
                subject: Der::from(vec![0x30, 0x00]),
                subject_public_key_info: Der::from(vec![0x30, 0x00]),
                name_constraints: None,
            }],
        })
    }

    fn profile_server_config_without_identity() -> rustls::ServerConfig {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let builder = rustls::ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap();
        let mut config = builder
            .with_no_client_auth()
            .with_cert_resolver(Arc::new(ResolvesServerCertUsingSni::new()));
        apply_server_profile(&mut config);
        config
    }

    #[test]
    fn resource_policy_accepts_finite_consistent_values() {
        let validated = limits().validate().unwrap();
        assert_eq!(validated.max_connections().get(), 8);
        assert_eq!(validated.max_active_incoming_flows(), 16);
        assert_eq!(validated.udp_payload_ceiling(), 1_452);
    }

    #[test]
    fn resource_policy_rejects_zero_and_out_of_range_values() {
        let mut input = limits();
        input.max_connections = 0;
        assert_eq!(
            input.validate(),
            Err(EndpointResourceError::ZeroConnections)
        );

        let mut input = limits();
        input.max_active_incoming_flows = 0;
        assert_eq!(
            input.validate(),
            Err(EndpointResourceError::ZeroIncomingFlows)
        );

        let mut input = limits();
        input.max_active_incoming_flows = QUIC_MAX_STREAM_COUNT;
        assert!(input.validate().is_ok());

        input.max_active_incoming_flows = QUIC_MAX_STREAM_COUNT + 1;
        assert_eq!(
            input.validate(),
            Err(EndpointResourceError::IncomingFlowsExceedQuicStreamLimit)
        );

        let mut input = limits();
        input.udp_payload_ceiling = QUIC_MIN_UDP_PAYLOAD - 1;
        assert_eq!(
            input.validate(),
            Err(EndpointResourceError::UdpPayloadBelowMinimum)
        );

        let mut input = limits();
        input.udp_payload_ceiling = QUIC_MAX_UDP_PAYLOAD + 1;
        assert_eq!(
            input.validate(),
            Err(EndpointResourceError::UdpPayloadAboveMaximum)
        );
    }

    #[test]
    fn resource_policy_rejects_invalid_window_and_buffer_relationships() {
        let mut input = limits();
        input.stream_receive_window = 0;
        assert_eq!(
            input.validate(),
            Err(EndpointResourceError::ZeroStreamReceiveWindow)
        );

        let mut input = limits();
        input.connection_receive_window = input.stream_receive_window - 1;
        assert_eq!(
            input.validate(),
            Err(EndpointResourceError::ConnectionReceiveWindowBelowStream)
        );

        let mut input = limits();
        input.send_window = 0;
        assert_eq!(input.validate(), Err(EndpointResourceError::ZeroSendWindow));

        let mut input = limits();
        input.crypto_buffer_bytes = 0;
        assert_eq!(
            input.validate(),
            Err(EndpointResourceError::ZeroCryptoBuffer)
        );

        let mut input = limits();
        input.datagram_receive_buffer_bytes = usize::from(input.udp_payload_ceiling) - 1;
        assert_eq!(
            input.validate(),
            Err(EndpointResourceError::DatagramReceiveBufferBelowUdpCeiling)
        );

        let mut input = limits();
        input.datagram_send_buffer_bytes = usize::from(input.udp_payload_ceiling) - 1;
        assert_eq!(
            input.validate(),
            Err(EndpointResourceError::DatagramSendBufferBelowUdpCeiling)
        );

        let mut input = limits();
        input.max_idle_timeout = Duration::ZERO;
        assert_eq!(
            input.validate(),
            Err(EndpointResourceError::ZeroIdleTimeout)
        );
    }

    #[test]
    fn udp_socket_binding_is_nonblocking_before_tokio_wrapping() {
        let socket = bind_udp_socket("127.0.0.1:0".parse().unwrap()).unwrap();
        let mut byte = [0u8; 1];
        let error = socket.recv_from(&mut byte).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn role_mapping_and_udp_ceiling_are_profile_owned() {
        let resources = limits().validate().unwrap();
        let client = transport_profile(WireSide::Client, resources);
        let server = transport_profile(WireSide::Server, resources);

        assert_eq!(client.incoming_bidi_streams, VarInt::from_u32(0));
        assert_eq!(server.incoming_bidi_streams, VarInt::from_u32(1));
        assert_eq!(client.incoming_uni_streams, VarInt::from_u32(16));
        assert_eq!(server.incoming_uni_streams, VarInt::from_u32(16));
        assert_eq!(client.udp_payload_ceiling, 1_452);
        assert_eq!(client.datagram_receive_buffer_bytes, 64 * 1024);
        assert_eq!(client.datagram_send_buffer_bytes, 64 * 1024);

        let endpoint = build_endpoint_config(resources).unwrap();
        assert_eq!(endpoint.get_max_udp_payload_size(), u64::from(1_452u16));
        assert_eq!(QUIC_V1, 0x0000_0001);
    }

    #[test]
    fn client_profile_rejects_empty_roots_and_fixes_alpn_and_early_data() {
        assert!(matches!(
            build_client_tls(Arc::new(RootCertStore::empty())),
            Err(EndpointBuildError::EmptyTrustRoots)
        ));

        let config = build_client_tls(non_empty_roots()).unwrap();
        assert_eq!(config.alpn_protocols, vec![RUNENNET_ALPN.to_vec()]);
        assert!(!config.enable_early_data);
    }

    #[test]
    fn server_profile_rejects_empty_chain_and_fixes_alpn_and_early_data() {
        let invalid_key: PrivateKeyDer<'static> =
            rustls::pki_types::PrivatePkcs8KeyDer::from(vec![0u8]).into();
        assert!(matches!(
            build_server_tls(Vec::new(), invalid_key),
            Err(EndpointBuildError::EmptyCertificateChain)
        ));

        let config = profile_server_config_without_identity();
        assert_eq!(config.alpn_protocols, vec![RUNENNET_ALPN.to_vec()]);
        assert_eq!(config.max_early_data_size, 0);
    }

    #[test]
    fn invalid_server_identity_fails_construction() {
        let certificate = CertificateDer::from(vec![0u8]);
        let private_key: PrivateKeyDer<'static> =
            rustls::pki_types::PrivatePkcs8KeyDer::from(vec![0u8]).into();
        assert!(matches!(
            build_server_tls(vec![certificate], private_key),
            Err(EndpointBuildError::Rustls(_))
        ));
    }
}
