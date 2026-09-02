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
    delivery::{
        DeliveryEndpoint, DeliveryFlowHandle, DeliveryFlowKey, DeliveryMode, DeliveryScopeLimits,
        FlowDirection, FlowResourcePolicy, FlowTerminationReason, OutboundPressureBehavior,
        ReceiverPressureBehavior,
    },
    identity::ConnectionHandle,
    protocol::{
        CompatibilityOffer, NegotiatedContract, NegotiationManager, NegotiationManagerLimits,
        NegotiationRequirements, OfferLimits, ProtocolContract, ProtocolId, ProtocolRevision,
    },
};
use rustls_pki_types::PrivatePkcs8KeyDer;
use tokio::runtime::Builder;

use crate::{
    CertificateDer, ClientEndpoint, ClientTrust, Connection as PublicConnection, ConnectionError,
    ConnectionErrorKind, ConnectionEvent, ConnectionStateError, ConnectionTeardown, EndpointConfig,
    InboundFlowConfig, OutboundFlowConfig, PrivateKeyDer, ProfileBootstrapFailure, ProfileConfig,
    SemanticRole, ServerEndpoint, ServerIdentity, SubmitOutcome,
    wire::ApplicationErrorCode,
};

const SCENARIO_TIMEOUT: Duration = Duration::from_secs(20);
const CLIENT_CONNECTION: ConnectionHandle = ConnectionHandle::new(41);
const SERVER_CONNECTION: ConnectionHandle = ConnectionHandle::new(99);
const MAX_MESSAGE_BYTES: usize = 512;

struct HostState {
    negotiation: NegotiationManager,
    delivery: DeliveryEndpoint,
}

#[test]
fn consuming_established_teardown_repeatedly_surfaces_peer_close_and_preserves_flows() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            for iteration in 0..3 {
                run_teardown_peer_close_iteration(iteration).await;
            }
        })
        .await
        .expect("established teardown peer-close conformance timed out");
    });
}

#[test]
fn established_teardown_preserves_pending_reliable_obligation_evidence() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            let config = baseline_config();
            let (
                client_endpoint,
                server_endpoint,
                mut client_connection,
                mut client_host,
                mut server_connection,
                mut server_host,
            ) = establish_pair(config).await;

            let (outbound, inbound) = open_and_accept_flow(
                &mut client_connection,
                &mut client_host,
                &mut server_connection,
                &mut server_host,
                DeliveryMode::ReliableOrdered,
                30,
                130,
            )
            .await;

            assert_eq!(
                client_connection
                    .submit(
                        &mut client_host.delivery,
                        outbound,
                        b"still-obligated".to_vec(),
                    )
                    .unwrap(),
                SubmitOutcome::Accepted {
                    accepted_index: 0,
                    local_pressure_drops: 0,
                }
            );

            let client_teardown = client_connection
                .teardown(&mut client_host.negotiation, &mut client_host.delivery);
            let client_termination = assert_single_connection_end(&client_teardown, outbound);
            assert_eq!(client_termination.pending_messages, 1);
            assert!(client_termination.reliable_obligation_failed);

            assert_eq!(
                wait_peer_closed(&mut server_connection, &mut server_host).await,
                SERVER_CONNECTION
            );
            assert_terminal(&mut server_connection, &mut server_host);

            let server_teardown = server_connection
                .teardown(&mut server_host.negotiation, &mut server_host.delivery);
            let _ = assert_single_connection_end(&server_teardown, inbound);

            cleanup_endpoints(&client_endpoint, &server_endpoint).await;
        })
        .await
        .expect("pending reliable obligation conformance timed out");
    });
}

#[test]
fn non_zero_application_close_remains_a_public_failure() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            let config = baseline_config();
            let (
                client_endpoint,
                server_endpoint,
                mut client_connection,
                mut client_host,
                server_connection,
                mut server_host,
            ) = establish_pair(config).await;
            let (server_driver, _) = server_connection
                .into_established_internal()
                .expect("Established event did not retain the peer driver");

            server_driver.close_for_test(ApplicationErrorCode::ProfileProtocolError);
            let error = wait_public_error(&mut client_connection, &mut client_host).await;
            assert_existing_close_failure(error.kind());

            let client_teardown = client_connection
                .teardown(&mut client_host.negotiation, &mut client_host.delivery);
            assert!(client_teardown.flow_terminations().is_empty());
            let server_teardown = server_driver
                .teardown(&mut server_host.negotiation, &mut server_host.delivery);
            assert!(server_teardown.flow_terminations.is_empty());

            cleanup_endpoints(&client_endpoint, &server_endpoint).await;
        })
        .await
        .expect("non-zero application close conformance timed out");
    });
}

#[test]
fn partial_control_then_no_error_remains_a_control_failure() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            let config = baseline_config();
            let (
                client_endpoint,
                server_endpoint,
                mut client_connection,
                mut client_host,
                server_connection,
                mut server_host,
            ) = establish_pair(config).await;
            let (mut server_driver, _) = server_connection
                .into_established_internal()
                .expect("Established event did not retain the peer driver");

            server_driver
                .send_raw_control_bytes_for_test(&[0x40])
                .await
                .expect("partial control varint was not written");
            assert!(matches!(
                poll_once(&mut client_connection, &mut client_host),
                Poll::Pending
            ));

            server_driver.close_for_test(ApplicationErrorCode::NoError);
            let error = wait_public_error(&mut client_connection, &mut client_host).await;
            assert_eq!(
                error.kind(),
                ConnectionErrorKind::EstablishedControl(ProfileBootstrapFailure::Control)
            );

            let client_teardown = client_connection
                .teardown(&mut client_host.negotiation, &mut client_host.delivery);
            assert!(client_teardown.flow_terminations().is_empty());
            let server_teardown = server_driver
                .teardown(&mut server_host.negotiation, &mut server_host.delivery);
            assert!(server_teardown.flow_terminations.is_empty());

            cleanup_endpoints(&client_endpoint, &server_endpoint).await;
        })
        .await
        .expect("partial control plus NO_ERROR conformance timed out");
    });
}

#[test]
fn genuine_idle_timeout_remains_a_public_failure() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            let config = timeout_config();
            let (
                client_endpoint,
                server_endpoint,
                mut client_connection,
                mut client_host,
                server_connection,
                mut server_host,
            ) = establish_pair(config).await;

            let error = wait_public_error(&mut client_connection, &mut client_host).await;
            assert_existing_close_failure(error.kind());

            let client_teardown = client_connection
                .teardown(&mut client_host.negotiation, &mut client_host.delivery);
            assert!(client_teardown.flow_terminations().is_empty());
            let server_teardown = server_connection
                .teardown(&mut server_host.negotiation, &mut server_host.delivery);
            assert!(server_teardown.flow_terminations().is_empty());

            cleanup_endpoints(&client_endpoint, &server_endpoint).await;
        })
        .await
        .expect("genuine idle-timeout conformance timed out");
    });
}

async fn run_teardown_peer_close_iteration(iteration: u64) {
    let config = baseline_config();
    let (
        client_endpoint,
        server_endpoint,
        mut client_connection,
        mut client_host,
        server_connection,
        mut server_host,
    ) = establish_pair(config).await;

    let (outbound, inbound) = open_and_accept_flow(
        &mut client_connection,
        &mut client_host,
        &mut { server_connection },
        &mut server_host,
        DeliveryMode::UnreliableUnordered,
        10 + iteration,
        110 + iteration,
    )
    .await;

    // Recover ownership after the narrow mutable borrow used above.
    let mut server_connection = establish_unreachable!();

    let _ = (outbound, inbound, client_endpoint, server_endpoint, client_connection, client_host,
        server_connection, server_host);
}

fn establish_unreachable!() -> PublicConnection {
    unreachable!()
}

async fn establish_pair(
    config: EndpointConfig,
) -> (
    ClientEndpoint,
    ServerEndpoint,
    PublicConnection,
    HostState,
    PublicConnection,
    HostState,
) {
    let (client_endpoint, server_endpoint) = endpoints(config);
    let server_address = server_endpoint.local_addr().unwrap();
    let (client_ready, server_ready) = join2(
        client_endpoint.connect(
            server_address,
            "localhost",
            ProfileConfig::baseline(config, SemanticRole::Authority, 128 * 1024).unwrap(),
        ),
        server_endpoint.accept(
            ProfileConfig::baseline(config, SemanticRole::NonAuthority, 128 * 1024).unwrap(),
        ),
    )
    .await;
    let mut client_host = new_host();
    let mut server_host = new_host();
    let mut client_connection = client_ready
        .expect("client failed ProfileReady")
        .activate(
            CLIENT_CONNECTION,
            offer(),
            NegotiationRequirements::default(),
            &mut client_host.negotiation,
        )
        .unwrap();
    let mut server_connection = server_ready
        .expect("server endpoint closed before ProfileReady")
        .expect("server failed ProfileReady")
        .activate(
            SERVER_CONNECTION,
            offer(),
            NegotiationRequirements::default(),
            &mut server_host.negotiation,
        )
        .unwrap();

    let mut authority_selected = false;
    let mut client_established = false;
    let mut server_established = false;
    poll_fn(|cx| {
        if !client_established {
            match client_connection.poll(
                cx,
                &mut client_host.negotiation,
                &mut client_host.delivery,
            ) {
                Poll::Pending => {}
                Poll::Ready(Ok(ConnectionEvent::AuthoritySelectionRequired { connection })) => {
                    assert_eq!(connection, CLIENT_CONNECTION);
                    assert!(!authority_selected);
                    client_connection
                        .select_authority(&mut client_host.negotiation, contract())
                        .unwrap();
                    authority_selected = true;
                    cx.waker().wake_by_ref();
                }
                Poll::Ready(Ok(ConnectionEvent::Established { connection })) => {
                    assert_eq!(connection, CLIENT_CONNECTION);
                    assert!(authority_selected);
                    client_established = true;
                }
                Poll::Ready(Ok(event)) => {
                    panic!("unexpected client negotiation event: {event:?}")
                }
                Poll::Ready(Err(error)) => panic!("client negotiation failed: {error:?}"),
            }
        }
        if !server_established {
            match server_connection.poll(
                cx,
                &mut server_host.negotiation,
                &mut server_host.delivery,
            ) {
                Poll::Pending => {}
                Poll::Ready(Ok(ConnectionEvent::Established { connection })) => {
                    assert_eq!(connection, SERVER_CONNECTION);
                    server_established = true;
                }
                Poll::Ready(Ok(ConnectionEvent::AuthoritySelectionRequired { .. })) => {
                    panic!("NonAuthority requested Authority selection")
                }
                Poll::Ready(Ok(event)) => {
                    panic!("unexpected server negotiation event: {event:?}")
                }
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

    (
        client_endpoint,
        server_endpoint,
        client_connection,
        client_host,
        server_connection,
        server_host,
    )
}

async fn open_and_accept_flow(
    client: &mut PublicConnection,
    client_host: &mut HostState,
    server: &mut PublicConnection,
    server_host: &mut HostState,
    mode: DeliveryMode,
    outbound_handle: u64,
    inbound_handle: u64,
) -> (DeliveryFlowKey, DeliveryFlowKey) {
    let outbound = DeliveryFlowKey::new(
        CLIENT_CONNECTION,
        FlowDirection::Outbound,
        DeliveryFlowHandle::new(outbound_handle),
    );
    let inbound = DeliveryFlowKey::new(
        SERVER_CONNECTION,
        FlowDirection::Inbound,
        DeliveryFlowHandle::new(inbound_handle),
    );
    client
        .open_outbound_flow(
            &client_host.delivery,
            OutboundFlowConfig {
                key: outbound,
                mode,
                policy: flow_policy(mode),
                connection_limits: flow_connection_limits(),
            },
        )
        .unwrap();

    let mut accepted = false;
    let mut established = false;
    poll_fn(|cx| {
        match client.poll(
            cx,
            &mut client_host.negotiation,
            &mut client_host.delivery,
        ) {
            Poll::Pending => {}
            Poll::Ready(Ok(ConnectionEvent::OutboundFlowEstablished { key })) => {
                assert_eq!(key, outbound);
                established = true;
            }
            Poll::Ready(Ok(event)) => panic!("unexpected outbound-flow event: {event:?}"),
            Poll::Ready(Err(error)) => panic!("outbound-flow driver failed: {error:?}"),
        }

        match server.poll(
            cx,
            &mut server_host.negotiation,
            &mut server_host.delivery,
        ) {
            Poll::Pending => {}
            Poll::Ready(Ok(ConnectionEvent::IncomingFlowRequested { request })) => {
                assert!(!accepted);
                assert_eq!(request.connection(), SERVER_CONNECTION);
                assert_eq!(request.mode(), mode);
                server
                    .accept_incoming_flow(
                        &mut server_host.delivery,
                        request,
                        InboundFlowConfig {
                            key: inbound,
                            policy: flow_policy(mode),
                            connection_limits: flow_connection_limits(),
                        },
                    )
                    .unwrap();
                accepted = true;
                cx.waker().wake_by_ref();
            }
            Poll::Ready(Ok(event)) => panic!("unexpected inbound-flow event: {event:?}"),
            Poll::Ready(Err(error)) => panic!("inbound-flow driver failed: {error:?}"),
        }

        if accepted && established {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;

    (outbound, inbound)
}

async fn wait_peer_closed(
    connection: &mut PublicConnection,
    host: &mut HostState,
) -> ConnectionHandle {
    poll_fn(|cx| {
        match connection.poll(cx, &mut host.negotiation, &mut host.delivery) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(ConnectionEvent::PeerClosed { connection })) => {
                Poll::Ready(connection)
            }
            Poll::Ready(Ok(event)) => {
                panic!("peer close produced unexpected public event: {event:?}")
            }
            Poll::Ready(Err(error)) => {
                panic!("peer NO_ERROR was exposed as failure: {error:?}")
            }
        }
    })
    .await
}

async fn wait_public_error(
    connection: &mut PublicConnection,
    host: &mut HostState,
) -> ConnectionError {
    poll_fn(|cx| {
        match connection.poll(cx, &mut host.negotiation, &mut host.delivery) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(error),
            Poll::Ready(Ok(event)) => {
                panic!("failing peer unexpectedly produced public progress: {event:?}")
            }
        }
    })
    .await
}

fn poll_once(
    connection: &mut PublicConnection,
    host: &mut HostState,
) -> Poll<Result<ConnectionEvent, ConnectionError>> {
    let mut cx = Context::from_waker(Waker::noop());
    connection.poll(&mut cx, &mut host.negotiation, &mut host.delivery)
}

fn assert_terminal(connection: &mut PublicConnection, host: &mut HostState) {
    let error = match poll_once(connection, host) {
        Poll::Ready(Err(error)) => error,
        other => panic!("peer-closed connection did not become terminal: {other:?}"),
    };
    assert_eq!(
        error.kind(),
        ConnectionErrorKind::State(ConnectionStateError::Terminal)
    );
}

fn assert_single_connection_end<'a>(
    teardown: &'a ConnectionTeardown,
    key: DeliveryFlowKey,
) -> &'a runen_net::delivery::FlowTermination {
    assert!(teardown.cleanup_error().is_none());
    assert_eq!(teardown.flow_terminations().len(), 1);
    let termination = &teardown.flow_terminations()[0];
    assert_eq!(termination.key, key);
    assert_eq!(termination.reason, FlowTerminationReason::ConnectionEnded);
    termination
}

fn assert_existing_close_failure(kind: ConnectionErrorKind) {
    assert!(
        matches!(
            kind,
            ConnectionErrorKind::EstablishedTransport
                | ConnectionErrorKind::EstablishedControl(ProfileBootstrapFailure::Transport)
                | ConnectionErrorKind::EstablishedControl(ProfileBootstrapFailure::Control)
        ),
        "connection loss changed public failure category: {kind:?}"
    );
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

fn baseline_config() -> EndpointConfig {
    EndpointConfig::baseline(2, 16).unwrap()
}

fn timeout_config() -> EndpointConfig {
    let mut limits = baseline_config().limits();
    limits.max_idle_timeout = Duration::from_secs(1);
    limits.validate().unwrap()
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

fn flow_connection_limits() -> DeliveryScopeLimits {
    DeliveryScopeLimits::new(nz(32), nz(64), nz(512 * 1024))
}

fn flow_policy(mode: DeliveryMode) -> FlowResourcePolicy {
    FlowResourcePolicy::new(
        nz(MAX_MESSAGE_BYTES),
        nz(8),
        nz(8 * MAX_MESSAGE_BYTES),
        OutboundPressureBehavior::RejectNew,
        match mode {
            DeliveryMode::ReliableOrdered => ReceiverPressureBehavior::TerminateReliable,
            DeliveryMode::UnreliableUnordered | DeliveryMode::UnreliableSequenced => {
                ReceiverPressureBehavior::DropIncomingUnreliable
            }
        },
    )
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

async fn cleanup_endpoints(client: &ClientEndpoint, server: &ServerEndpoint) {
    client.close();
    server.close();
    join2(client.wait_idle(), server.wait_idle()).await;
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
