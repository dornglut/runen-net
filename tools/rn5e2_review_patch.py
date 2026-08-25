from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


path = Path("crates/runen-net-quic/src/endpoint.rs")
text = path.read_text()

marker = """pub(super) struct ValidatedEndpointResources {
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

"""
insertion = marker + """#[derive(Debug)]
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

"""
text = replace_once(text, marker, insertion, "configured endpoint insertion")

text = replace_once(
    text,
    "pub(super) fn build_client_config(\n",
    "fn build_client_config(\n",
    "client config visibility",
)
text = replace_once(
    text,
    "pub(super) fn build_server_config(\n",
    "fn build_server_config(\n",
    "server config visibility",
)

old_bind = """pub(super) fn bind_client_endpoint(
    bind_addr: SocketAddr,
    resources: ValidatedEndpointResources,
    client_config: ClientConfig,
) -> Result<Endpoint, EndpointBuildError> {
    let socket = UdpSocket::bind(bind_addr)?;
    let mut endpoint = Endpoint::new(
        build_endpoint_config(resources)?,
        None,
        socket,
        Arc::new(TokioRuntime),
    )?;
    endpoint.set_default_client_config(client_config);
    Ok(endpoint)
}

pub(super) fn bind_server_endpoint(
    bind_addr: SocketAddr,
    resources: ValidatedEndpointResources,
    server_config: ServerConfig,
) -> Result<Endpoint, EndpointBuildError> {
    let socket = UdpSocket::bind(bind_addr)?;
    Ok(Endpoint::new(
        build_endpoint_config(resources)?,
        Some(server_config),
        socket,
        Arc::new(TokioRuntime),
    )?)
}
"""
new_bind = """/// Bind a revision-1 RunenNet QUIC client endpoint inside an entered Tokio runtime.
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
    let socket = UdpSocket::bind(bind_addr)?;
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
    let socket = UdpSocket::bind(bind_addr)?;
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
"""
text = replace_once(text, old_bind, new_bind, "bound endpoint composition")

old_transport = """fn build_transport_config(
    side: WireSide,
    resources: ValidatedEndpointResources,
) -> TransportConfig {
    let mut config = TransportConfig::default();
    let incoming_bidi = match side {
        WireSide::Client => VarInt::from_u32(0),
        WireSide::Server => VarInt::from_u32(1),
    };
    config
        .max_concurrent_bidi_streams(incoming_bidi)
        .max_concurrent_uni_streams(resources.max_active_incoming_flows)
        .stream_receive_window(resources.stream_receive_window)
        .receive_window(resources.connection_receive_window)
        .send_window(resources.send_window)
        .crypto_buffer_size(resources.crypto_buffer_bytes.get())
        .datagram_receive_buffer_size(Some(resources.datagram_receive_buffer_bytes.get()))
        .datagram_send_buffer_size(resources.datagram_send_buffer_bytes.get())
        .max_idle_timeout(Some(resources.max_idle_timeout))
        .min_mtu(QUIC_MIN_UDP_PAYLOAD)
        .initial_mtu(QUIC_MIN_UDP_PAYLOAD);

    let mut mtu_discovery = MtuDiscoveryConfig::default();
    mtu_discovery.upper_bound(resources.udp_payload_ceiling);
    config.mtu_discovery_config(Some(mtu_discovery));
    config
}
"""
new_transport = """fn transport_profile(
    side: WireSide,
    resources: ValidatedEndpointResources,
) -> TransportProfile {
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
"""
text = replace_once(text, old_transport, new_transport, "transport profile mapping")

old_test = """    #[test]
    fn role_mapping_and_udp_ceiling_are_profile_owned() {
        let resources = limits().validate().unwrap();
        assert_eq!(incoming_bidi_limit(WireSide::Client), VarInt::from_u32(0));
        assert_eq!(incoming_bidi_limit(WireSide::Server), VarInt::from_u32(1));
        assert_eq!(incoming_uni_limit(resources), VarInt::from_u32(16));

        let endpoint = build_endpoint_config(resources).unwrap();
        assert_eq!(endpoint.get_max_udp_payload_size(), u64::from(1_452u16));
        assert_eq!(QUIC_V1, 0x0000_0001);
    }
"""
new_test = """    #[test]
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
"""
text = replace_once(text, old_test, new_test, "role mapping test")

helpers = """
    fn incoming_bidi_limit(side: WireSide) -> VarInt {
        match side {
            WireSide::Client => VarInt::from_u32(0),
            WireSide::Server => VarInt::from_u32(1),
        }
    }

    fn incoming_uni_limit(resources: ValidatedEndpointResources) -> VarInt {
        resources.max_active_incoming_flows
    }
"""
text = replace_once(text, helpers, "\n", "duplicated test helpers")

path.write_text(text)
