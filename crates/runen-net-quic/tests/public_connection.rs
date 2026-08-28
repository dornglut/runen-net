use std::{
    future::{Future, poll_fn},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    pin::pin,
    task::{Context, Poll, Waker},
    time::Duration,
};

use rcgen::{CertifiedKey, generate_simple_self_signed};
use runen_net::{
    delivery::{DeliveryEndpoint, DeliveryScopeLimits},
    identity::ConnectionHandle,
    protocol::{
        CompatibilityOffer, NegotiatedContract, NegotiationManager, NegotiationManagerLimits,
        NegotiationRequirements, OfferLimits, ProtocolContract, ProtocolId, ProtocolRevision,
    },
};
use runen_net_quic::{
    CertificateDer, ClientEndpoint, ClientTrust, Connection, ConnectionError, ConnectionEvent,
    ConnectionStateError, EndpointConfig, EndpointResourceLimits, NegotiationFailure,
    NegotiationReportStatus, PrivateKeyDer, ProfileConfig, ProfileLimits, ProfileReadyConnection,
    ReliableReceiveLimits, SemanticRole, ServerEndpoint, ServerIdentity,
};
use rustls_pki_types::PrivatePkcs8KeyDer;
use tokio::runtime::Builder;

const SCENARIO_TIMEOUT: Duration = Duration::from_secs(20);
const CLIENT_CONNECTION: ConnectionHandle = ConnectionHandle::new(41);
const SERVER_CONNECTION: ConnectionHandle = ConnectionHandle::new(99);

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum AuthoritySide {
    Client,
    Server,
}

struct HostState {
    negotiation: NegotiationManager,
    delivery: DeliveryEndpoint,
}

#[test]
fn public_connection_negotiates_with_host_identity_and_explicit_authority_on_either_quic_side() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            for authority in [AuthoritySide::Client, AuthoritySide::Server] {
                run_public_success(authority).await;
            }
        })
        .await
        .expect("public RN6C success scenarios timed out");
    });
}

#[test]
fn pending_send_and_receive_teardown_release_core_and_endpoint_capacity() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            let config = resource_limits(1).validate().unwrap();
            let (client, server) = endpoints(config);
            let mut client_host = new_host();
            let mut server_host = new_host();

            let (client_ready, server_ready) =
                profile_ready_pair(&client, &server, config, AuthoritySide::Client).await;
            let (mut client_connection, mut server_connection) = activate_pair(
                client_ready,
                server_ready,
                &mut client_host,
                &mut server_host,
            );

            drive_until_authority_selection(
                &mut client_connection,
                &mut client_host,
                &mut server_connection,
                &mut server_host,
                AuthoritySide::Client,
            )
            .await;
            client_connection
                .select_authority(&mut client_host.negotiation, contract())
                .expect("valid Authority selection did not enter pending send");
            assert!(matches!(
                poll_once(
                    &mut server_connection,
                    &mut server_host.negotiation,
                    &mut server_host.delivery,
                ),
                Poll::Pending
            ));

            let client_teardown =
                client_connection.teardown(&mut client_host.negotiation, &mut client_host.delivery);
            let server_teardown =
                server_connection.teardown(&mut server_host.negotiation, &mut server_host.delivery);
            assert_clean_teardown(&client_teardown, CLIENT_CONNECTION);
            assert_clean_teardown(&server_teardown, SERVER_CONNECTION);

            let (client_ready, server_ready) =
                profile_ready_pair(&client, &server, config, AuthoritySide::Client).await;
            let (mut client_connection, mut server_connection) = activate_pair(
                client_ready,
                server_ready,
                &mut client_host,
                &mut server_host,
            );
            drive_until_authority_selection(
                &mut client_connection,
                &mut client_host,
                &mut server_connection,
                &mut server_host,
                AuthoritySide::Client,
            )
            .await;
            client_connection
                .select_authority(&mut client_host.negotiation, contract())
                .unwrap();
            drive_until_established(
                &mut client_connection,
                &mut client_host,
                &mut server_connection,
                &mut server_host,
            )
            .await;

            let client_teardown =
                client_connection.teardown(&mut client_host.negotiation, &mut client_host.delivery);
            let server_teardown =
                server_connection.teardown(&mut server_host.negotiation, &mut server_host.delivery);
            assert_clean_teardown(&client_teardown, CLIENT_CONNECTION);
            assert_clean_teardown(&server_teardown, SERVER_CONNECTION);

            client.close();
            server.close();
            join2(client.wait_idle(), server.wait_idle()).await;
        })
        .await
        .expect("pending RN6C teardown/reuse scenario timed out");
    });
}

#[test]
fn invalid_authority_selection_preserves_local_and_remote_semantic_failure_categories() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            let config = resource_limits(2).validate().unwrap();
            let (client, server) = endpoints(config);
            let mut client_host = new_host();
            let mut server_host = new_host();
            let (client_ready, server_ready) =
                profile_ready_pair(&client, &server, config, AuthoritySide::Client).await;
            let (mut client_connection, mut server_connection) = activate_pair(
                client_ready,
                server_ready,
                &mut client_host,
                &mut server_host,
            );

            drive_until_authority_selection(
                &mut client_connection,
                &mut client_host,
                &mut server_connection,
                &mut server_host,
                AuthoritySide::Client,
            )
            .await;

            client_connection
                .select_authority(&mut client_host.negotiation, invalid_contract())
                .expect("semantic failure report must remain asynchronously sendable");

            let mut client_error = None;
            let mut server_error = None;
            let (client_error, server_error) = poll_fn(|cx| {
                if client_error.is_none() {
                    client_error = poll_error(
                        &mut client_connection,
                        &mut client_host,
                        cx,
                        CLIENT_CONNECTION,
                    );
                }
                if server_error.is_none() {
                    server_error = poll_error(
                        &mut server_connection,
                        &mut server_host,
                        cx,
                        SERVER_CONNECTION,
                    );
                }
                match (client_error, server_error) {
                    (Some(client_error), Some(server_error)) => {
                        Poll::Ready((client_error, server_error))
                    }
                    _ => Poll::Pending,
                }
            })
            .await;

            assert_eq!(
                client_error,
                ConnectionError::LocalNegotiation {
                    outcome: NegotiationFailure::InvalidSelection,
                    report: NegotiationReportStatus::Sent,
                }
            );
            assert_eq!(
                server_error,
                ConnectionError::RemoteNegotiation(NegotiationFailure::InvalidSelection)
            );

            let client_teardown =
                client_connection.teardown(&mut client_host.negotiation, &mut client_host.delivery);
            let server_teardown =
                server_connection.teardown(&mut server_host.negotiation, &mut server_host.delivery);
            assert_clean_teardown(&client_teardown, CLIENT_CONNECTION);
            assert_clean_teardown(&server_teardown, SERVER_CONNECTION);

            client.close();
            server.close();
            join2(client.wait_idle(), server.wait_idle()).await;
        })
        .await
        .expect("public semantic-failure scenario timed out");
    });
}

async fn run_public_success(authority: AuthoritySide) {
    let config = resource_limits(2).validate().unwrap();
    let (client, server) = endpoints(config);
    let mut client_host = new_host();
    let mut server_host = new_host();
    let (client_ready, server_ready) =
        profile_ready_pair(&client, &server, config, authority).await;
    let (mut client_connection, mut server_connection) = activate_pair(
        client_ready,
        server_ready,
        &mut client_host,
        &mut server_host,
    );

    assert_eq!(client_connection.connection_handle(), CLIENT_CONNECTION);
    assert_eq!(server_connection.connection_handle(), SERVER_CONNECTION);
    assert_eq!(
        client_connection.reliable_receive_limits(),
        reliable_receive_limits()
    );
    assert_eq!(
        server_connection.reliable_receive_limits(),
        reliable_receive_limits()
    );

    drive_until_authority_selection(
        &mut client_connection,
        &mut client_host,
        &mut server_connection,
        &mut server_host,
        authority,
    )
    .await;

    let (authority_connection, authority_host, non_authority_connection, non_authority_host) =
        match authority {
            AuthoritySide::Client => (
                &mut client_connection,
                &mut client_host,
                &mut server_connection,
                &mut server_host,
            ),
            AuthoritySide::Server => (
                &mut server_connection,
                &mut server_host,
                &mut client_connection,
                &mut client_host,
            ),
        };

    assert!(matches!(
        poll_once(
            authority_connection,
            &mut authority_host.negotiation,
            &mut authority_host.delivery,
        ),
        Poll::Pending
    ));
    assert_eq!(
        non_authority_connection
            .select_authority(&mut non_authority_host.negotiation, contract())
            .unwrap_err(),
        ConnectionError::State(ConnectionStateError::AuthoritySelectionNotRequired)
    );
    authority_connection
        .select_authority(&mut authority_host.negotiation, contract())
        .unwrap();

    drive_until_established(
        &mut client_connection,
        &mut client_host,
        &mut server_connection,
        &mut server_host,
    )
    .await;

    assert_eq!(client_host.delivery.active_flows(), 0);
    assert_eq!(server_host.delivery.active_flows(), 0);
    assert!(matches!(
        poll_once(
            &mut client_connection,
            &mut client_host.negotiation,
            &mut client_host.delivery,
        ),
        Poll::Pending
    ));
    assert!(matches!(
        poll_once(
            &mut server_connection,
            &mut server_host.negotiation,
            &mut server_host.delivery,
        ),
        Poll::Pending
    ));

    let client_teardown =
        client_connection.teardown(&mut client_host.negotiation, &mut client_host.delivery);
    let server_teardown =
        server_connection.teardown(&mut server_host.negotiation, &mut server_host.delivery);
    assert_clean_teardown(&client_teardown, CLIENT_CONNECTION);
    assert_clean_teardown(&server_teardown, SERVER_CONNECTION);

    client.close();
    server.close();
    join2(client.wait_idle(), server.wait_idle()).await;
}

fn activate_pair(
    client_ready: ProfileReadyConnection,
    server_ready: ProfileReadyConnection,
    client_host: &mut HostState,
    server_host: &mut HostState,
) -> (Connection, Connection) {
    let client_connection = client_ready
        .activate(
            CLIENT_CONNECTION,
            offer(),
            NegotiationRequirements::default(),
            reliable_receive_limits(),
            &mut client_host.negotiation,
        )
        .unwrap();
    let server_connection = server_ready
        .activate(
            SERVER_CONNECTION,
            offer(),
            NegotiationRequirements::default(),
            reliable_receive_limits(),
            &mut server_host.negotiation,
        )
        .unwrap();
    (client_connection, server_connection)
}

async fn drive_until_authority_selection(
    client: &mut Connection,
    client_host: &mut HostState,
    server: &mut Connection,
    server_host: &mut HostState,
    authority: AuthoritySide,
) {
    poll_fn(|cx| {
        for (side, connection, host, expected_handle) in [
            (
                AuthoritySide::Client,
                &mut *client,
                &mut *client_host,
                CLIENT_CONNECTION,
            ),
            (
                AuthoritySide::Server,
                &mut *server,
                &mut *server_host,
                SERVER_CONNECTION,
            ),
        ] {
            match connection.poll(cx, &mut host.negotiation, &mut host.delivery) {
                Poll::Pending => {}
                Poll::Ready(Ok(ConnectionEvent::AuthoritySelectionRequired { connection })) => {
                    assert_eq!(side, authority);
                    assert_eq!(connection, expected_handle);
                    return Poll::Ready(());
                }
                Poll::Ready(Ok(ConnectionEvent::Established { .. })) => {
                    panic!("connection established before explicit Authority selection")
                }
                Poll::Ready(Ok(_)) => panic!("unexpected public connection event"),
                Poll::Ready(Err(error)) => panic!("public negotiation failed early: {error:?}"),
            }
        }
        Poll::Pending
    })
    .await;
}

async fn drive_until_established(
    client: &mut Connection,
    client_host: &mut HostState,
    server: &mut Connection,
    server_host: &mut HostState,
) {
    let mut client_established = false;
    let mut server_established = false;
    poll_fn(|cx| {
        if !client_established {
            match client.poll(cx, &mut client_host.negotiation, &mut client_host.delivery) {
                Poll::Pending => {}
                Poll::Ready(Ok(ConnectionEvent::Established { connection })) => {
                    assert_eq!(connection, CLIENT_CONNECTION);
                    client_established = true;
                }
                Poll::Ready(Ok(ConnectionEvent::AuthoritySelectionRequired { .. })) => {
                    panic!("Authority selection was surfaced more than once")
                }
                Poll::Ready(Ok(_)) => panic!("unexpected public connection event"),
                Poll::Ready(Err(error)) => panic!("client negotiation failed: {error:?}"),
            }
        }
        if !server_established {
            match server.poll(cx, &mut server_host.negotiation, &mut server_host.delivery) {
                Poll::Pending => {}
                Poll::Ready(Ok(ConnectionEvent::Established { connection })) => {
                    assert_eq!(connection, SERVER_CONNECTION);
                    server_established = true;
                }
                Poll::Ready(Ok(ConnectionEvent::AuthoritySelectionRequired { .. })) => {
                    panic!("Authority selection was surfaced more than once")
                }
                Poll::Ready(Ok(_)) => panic!("unexpected public connection event"),
                Poll::Ready(Err(error)) => panic!("server negotiation failed: {error:?}"),
            }
        }
        if client_established && server_established {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}

fn poll_error(
    connection: &mut Connection,
    host: &mut HostState,
    cx: &mut Context<'_>,
    expected_handle: ConnectionHandle,
) -> Option<ConnectionError> {
    match connection.poll(cx, &mut host.negotiation, &mut host.delivery) {
        Poll::Pending => None,
        Poll::Ready(Err(error)) => Some(error),
        Poll::Ready(Ok(ConnectionEvent::AuthoritySelectionRequired { .. })) => {
            panic!("semantic failure unexpectedly requested another Authority selection")
        }
        Poll::Ready(Ok(ConnectionEvent::Established { connection })) => {
            panic!(
                "semantic failure unexpectedly established {connection:?}; expected failure for {expected_handle:?}"
            )
        }
        Poll::Ready(Ok(_)) => panic!("unexpected public connection event"),
    }
}

fn poll_once(
    connection: &mut Connection,
    negotiation: &mut NegotiationManager,
    delivery: &mut DeliveryEndpoint,
) -> Poll<Result<ConnectionEvent, ConnectionError>> {
    let mut cx = Context::from_waker(Waker::noop());
    connection.poll(&mut cx, negotiation, delivery)
}

fn assert_clean_teardown(
    teardown: &runen_net_quic::ConnectionTeardown,
    expected: ConnectionHandle,
) {
    assert_eq!(teardown.connection(), expected);
    assert!(teardown.cleanup_error().is_none());
    assert!(teardown.flow_terminations().is_empty());
}

async fn profile_ready_pair(
    client: &ClientEndpoint,
    server: &ServerEndpoint,
    config: EndpointConfig,
    authority: AuthoritySide,
) -> (ProfileReadyConnection, ProfileReadyConnection) {
    let server_address = server.local_addr().unwrap();
    let client_role = if authority == AuthoritySide::Client {
        SemanticRole::Authority
    } else {
        SemanticRole::NonAuthority
    };
    let server_role = if authority == AuthoritySide::Server {
        SemanticRole::Authority
    } else {
        SemanticRole::NonAuthority
    };
    let (client_ready, server_ready) = join2(
        client.connect(server_address, "localhost", profile(config, client_role)),
        server.accept(profile(config, server_role)),
    )
    .await;
    (
        client_ready.expect("public client failed ProfileReady"),
        server_ready
            .expect("public server failed ProfileReady")
            .expect("public server endpoint closed before ProfileReady"),
    )
}

fn endpoints(config: EndpointConfig) -> (ClientEndpoint, ServerEndpoint) {
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
    (client, server)
}

fn new_host() -> HostState {
    HostState {
        negotiation: NegotiationManager::new(
            OfferLimits::default(),
            NegotiationManagerLimits::default(),
        )
        .unwrap(),
        delivery: DeliveryEndpoint::new(DeliveryScopeLimits::new(nz(64), nz(128), nz(1024 * 1024))),
    }
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
    ProfileLimits {
        semantic_role: role,
        max_control_frame_bytes: 64 * 1024,
        max_negotiation_frame_bytes: 32 * 1024,
        max_incoming_message_bytes: 128 * 1024,
    }
    .validate(config)
    .unwrap()
}

fn reliable_receive_limits() -> ReliableReceiveLimits {
    ReliableReceiveLimits {
        scratch_bytes: nz(4 * 1024),
        max_staging_bytes: nz(128 * 1024),
    }
}

fn offer() -> CompatibilityOffer {
    CompatibilityOffer::new(vec![protocol()], vec![], vec![], None)
}

const fn protocol() -> ProtocolContract {
    ProtocolContract::new(ProtocolId::new(1), ProtocolRevision::new(1))
}

fn contract() -> NegotiatedContract {
    NegotiatedContract::new(protocol())
}

fn invalid_contract() -> NegotiatedContract {
    NegotiatedContract::new(ProtocolContract::new(
        ProtocolId::new(2),
        ProtocolRevision::new(1),
    ))
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
