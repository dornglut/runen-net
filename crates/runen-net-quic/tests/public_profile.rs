use std::{
    future::{Future, poll_fn},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    pin::pin,
    task::Poll,
    time::Duration,
};

use rcgen::{CertifiedKey, generate_simple_self_signed};
use runen_net_quic::{
    CertificateDer, ClientEndpoint, ClientTrust, EndpointConfig, EndpointResourceError,
    EndpointResourceLimits, PrivateKeyDer, ProfileBootstrapFailure, ProfileConfig,
    ProfileConfigError, ProfileConnectionError, ProfileLimits, ReliableReceiveLimits, SemanticRole,
    ServerEndpoint, ServerIdentity, TlsMaterialError,
};
use rustls_pki_types::PrivatePkcs8KeyDer;
use tokio::runtime::Builder;

const SCENARIO_TIMEOUT: Duration = Duration::from_secs(20);
const MAX_INCOMING_MESSAGE_BYTES: u64 = 128 * 1024;

#[test]
fn public_configuration_rejects_invalid_resources_and_tls_material() {
    let mut invalid_resources = resource_limits(1);
    invalid_resources.max_connections = 0;
    assert_eq!(
        invalid_resources.validate().unwrap_err(),
        EndpointResourceError::ZeroConnections
    );

    let endpoint = resource_limits(1).validate().unwrap();
    let mut invalid_profile = profile_limits(SemanticRole::Authority);
    invalid_profile.max_control_frame_bytes = 1;
    assert_eq!(
        invalid_profile.validate(endpoint).unwrap_err(),
        ProfileConfigError::ControlFrameTooSmall
    );

    let mut invalid_staging = profile_limits(SemanticRole::Authority);
    invalid_staging.reliable_receive.max_staging_bytes = nz(64 * 1024);
    assert_eq!(
        invalid_staging.validate(endpoint).unwrap_err(),
        ProfileConfigError::ReliableStagingBelowIncomingMessageCeiling
    );

    assert_eq!(
        ClientTrust::new(Vec::new()).unwrap_err(),
        TlsMaterialError::EmptyClientTrust
    );

    let (_, private_key) = ephemeral_identity();
    assert_eq!(
        ServerIdentity::new(Vec::new(), private_key).unwrap_err(),
        TlsMaterialError::EmptyServerCertificateChain
    );
}

#[test]
fn public_baseline_configuration_is_finite_and_inspectable() {
    let endpoint = EndpointConfig::baseline(3, 12).unwrap();
    let endpoint_limits = endpoint.limits();
    assert_eq!(endpoint_limits.max_connections, 3);
    assert_eq!(endpoint_limits.max_active_incoming_flows, 12);
    assert_eq!(endpoint_limits.udp_payload_ceiling, 1_452);
    assert_eq!(endpoint_limits.stream_receive_window, 64 * 1024);
    assert_eq!(endpoint_limits.connection_receive_window, 256 * 1024);
    assert_eq!(endpoint_limits.send_window, 256 * 1024);
    assert_eq!(endpoint_limits.crypto_buffer_bytes, 32 * 1024);
    assert_eq!(endpoint_limits.datagram_receive_buffer_bytes, 64 * 1024);
    assert_eq!(endpoint_limits.datagram_send_buffer_bytes, 64 * 1024);
    assert_eq!(endpoint_limits.max_idle_timeout, Duration::from_secs(30));

    let profile = ProfileConfig::baseline(
        endpoint,
        SemanticRole::Authority,
        MAX_INCOMING_MESSAGE_BYTES,
    )
    .unwrap();
    let profile_limits = profile.limits();
    assert_eq!(profile_limits.semantic_role, SemanticRole::Authority);
    assert_eq!(profile_limits.max_control_frame_bytes, 64 * 1024);
    assert_eq!(profile_limits.max_negotiation_frame_bytes, 32 * 1024);
    assert_eq!(
        profile_limits.max_incoming_message_bytes,
        MAX_INCOMING_MESSAGE_BYTES
    );
    assert_eq!(profile_limits.reliable_receive.scratch_bytes, nz(4 * 1024));
    assert_eq!(
        profile_limits.reliable_receive.max_staging_bytes,
        nz(MAX_INCOMING_MESSAGE_BYTES as usize)
    );
    assert_eq!(
        profile.reliable_receive_limits(),
        profile_limits.reliable_receive
    );
}

#[test]
fn public_endpoints_reach_profile_ready_and_shutdown_without_raw_quinn_access() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            let config = resource_limits(2).validate().unwrap();
            let (certificate, private_key) = ephemeral_identity();
            let client = ClientEndpoint::bind(
                loopback_ephemeral(),
                config,
                ClientTrust::new(vec![certificate.clone()]).unwrap(),
            )
            .unwrap();
            let server = ServerEndpoint::bind(
                loopback_ephemeral(),
                config,
                ServerIdentity::new(vec![certificate], private_key).unwrap(),
            )
            .unwrap();
            let server_address = server.local_addr().unwrap();
            assert!(client.local_addr().unwrap().port() != 0);
            assert!(server_address.port() != 0);

            let client_profile = profile(config, SemanticRole::Authority);
            let server_profile = profile(config, SemanticRole::NonAuthority);
            let client_receive = client_profile.reliable_receive_limits();
            let server_receive = server_profile.reliable_receive_limits();
            let (client_ready, server_ready) = join2(
                client.connect(server_address, "localhost", client_profile),
                server.accept(server_profile),
            )
            .await;
            let client_ready = client_ready.expect("public client failed ProfileReady");
            let server_ready = server_ready
                .expect("public server failed ProfileReady")
                .expect("public server endpoint closed before ProfileReady");
            assert_eq!(client_ready.reliable_receive_limits(), client_receive);
            assert_eq!(server_ready.reliable_receive_limits(), server_receive);

            drop(client_ready);
            drop(server_ready);
            client.close();
            server.close();
            join2(client.wait_idle(), server.wait_idle()).await;
        })
        .await
        .expect("public ProfileReady success scenario timed out");
    });
}

#[test]
fn public_same_role_profile_is_rejected_as_bootstrap_failure() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            let config = resource_limits(1).validate().unwrap();
            let (certificate, private_key) = ephemeral_identity();
            let client = ClientEndpoint::bind(
                loopback_ephemeral(),
                config,
                ClientTrust::new(vec![certificate.clone()]).unwrap(),
            )
            .unwrap();
            let server = ServerEndpoint::bind(
                loopback_ephemeral(),
                config,
                ServerIdentity::new(vec![certificate], private_key).unwrap(),
            )
            .unwrap();
            let server_address = server.local_addr().unwrap();
            let authority = profile(config, SemanticRole::Authority);

            let (client_result, server_result) = join2(
                client.connect(server_address, "localhost", authority),
                server.accept(authority),
            )
            .await;
            assert!(client_result.is_err());
            assert!(server_result.is_err());
            assert!(
                matches!(
                    client_result,
                    Err(ProfileConnectionError::Bootstrap(
                        ProfileBootstrapFailure::RoleMismatch
                    ))
                ) || matches!(
                    server_result,
                    Err(ProfileConnectionError::Bootstrap(
                        ProfileBootstrapFailure::RoleMismatch
                    ))
                ),
                "at least one endpoint must classify the exact peer-role mismatch"
            );

            client.close();
            server.close();
            join2(client.wait_idle(), server.wait_idle()).await;
        })
        .await
        .expect("public same-role rejection scenario timed out");
    });
}

#[test]
fn public_capacity_one_server_refuses_overlap_and_reopens_after_release() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            let config = resource_limits(1).validate().unwrap();
            let (certificate, private_key) = ephemeral_identity();
            let trust_a = ClientTrust::new(vec![certificate.clone()]).unwrap();
            let trust_b = ClientTrust::new(vec![certificate.clone()]).unwrap();
            let client_a = ClientEndpoint::bind(loopback_ephemeral(), config, trust_a).unwrap();
            let client_b = ClientEndpoint::bind(loopback_ephemeral(), config, trust_b).unwrap();
            let server = ServerEndpoint::bind(
                loopback_ephemeral(),
                config,
                ServerIdentity::new(vec![certificate], private_key).unwrap(),
            )
            .unwrap();
            let server_address = server.local_addr().unwrap();
            let client_profile = profile(config, SemanticRole::Authority);
            let server_profile = profile(config, SemanticRole::NonAuthority);

            let (first_client, first_server) = join2(
                client_a.connect(server_address, "localhost", client_profile),
                server.accept(server_profile),
            )
            .await;
            let first_client = first_client.expect("first public client failed ProfileReady");
            let first_server = first_server
                .expect("first public server failed ProfileReady")
                .expect("public server endpoint closed before first ProfileReady");

            let (overlap_client, overlap_server) = join2(
                client_b.connect(server_address, "localhost", client_profile),
                server.accept(server_profile),
            )
            .await;
            assert!(matches!(
                overlap_server,
                Err(ProfileConnectionError::AdmissionAtCapacity)
            ));
            assert!(overlap_client.is_err());

            drop(first_client);
            drop(first_server);

            let (retry_client, retry_server) = join2(
                client_b.connect(server_address, "localhost", client_profile),
                server.accept(server_profile),
            )
            .await;
            let retry_client = retry_client.expect("client admission was not reusable");
            let retry_server = retry_server
                .expect("server admission was not reusable")
                .expect("server endpoint closed before retry ProfileReady");
            drop(retry_client);
            drop(retry_server);

            client_a.close();
            client_b.close();
            server.close();
            client_a.wait_idle().await;
            client_b.wait_idle().await;
            server.wait_idle().await;
        })
        .await
        .expect("public capacity/reuse scenario timed out");
    });
}

fn resource_limits(max_connections: usize) -> EndpointResourceLimits {
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
        max_idle_timeout: Duration::from_secs(5),
    }
}

fn profile(config: EndpointConfig, role: SemanticRole) -> ProfileConfig {
    profile_limits(role).validate(config).unwrap()
}

fn profile_limits(role: SemanticRole) -> ProfileLimits {
    ProfileLimits {
        semantic_role: role,
        max_control_frame_bytes: 64 * 1024,
        max_negotiation_frame_bytes: 32 * 1024,
        max_incoming_message_bytes: MAX_INCOMING_MESSAGE_BYTES,
        reliable_receive: ReliableReceiveLimits {
            scratch_bytes: nz(4 * 1024),
            max_staging_bytes: nz(MAX_INCOMING_MESSAGE_BYTES as usize),
        },
    }
}

fn ephemeral_identity() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let certificate = cert.der().clone();
    let private_key = PrivatePkcs8KeyDer::from(signing_key.serialize_der()).into();
    (certificate, private_key)
}

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

const fn loopback_ephemeral() -> SocketAddr {
    SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
}

async fn join2<A, B>(first: A, second: B) -> (A::Output, B::Output)
where
    A: Future,
    B: Future,
{
    let mut first = pin!(first);
    let mut second = pin!(second);
    let mut first_output = None;
    let mut second_output = None;

    poll_fn(|cx| {
        if first_output.is_none()
            && let Poll::Ready(output) = first.as_mut().poll(cx)
        {
            first_output = Some(output);
        }
        if second_output.is_none()
            && let Poll::Ready(output) = second.as_mut().poll(cx)
        {
            second_output = Some(output);
        }
        match (first_output.take(), second_output.take()) {
            (Some(first), Some(second)) => Poll::Ready((first, second)),
            (first, second) => {
                first_output = first;
                second_output = second;
                Poll::Pending
            }
        }
    })
    .await
}
