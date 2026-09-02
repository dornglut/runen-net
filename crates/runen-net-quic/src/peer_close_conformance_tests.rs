use std::{
    future::{Future, poll_fn},
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Waker},
    time::Duration,
};

use quinn::rustls::{
    RootCertStore,
    pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
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
use tokio::runtime::Builder;

use crate::{
    control::{LocalControlLimits, SemanticRole, ValidatedControlProfile},
    endpoint::{
        ConfiguredEndpoint, EndpointResourceLimits, ValidatedEndpointResources,
        bind_client_endpoint, bind_server_endpoint,
    },
    facade::{ProfileBootstrapFailure, ProfileReadyConnection, ReliableReceiveLimits},
    lifecycle::{AdmittedProfileReadyConnection, accept_profile_ready, connect_profile_ready},
    public_connection::{
        Connection as PublicConnection, ConnectionError, ConnectionErrorKind, ConnectionEvent,
        ConnectionStateError,
    },
    public_flow::{InboundFlowConfig, OutboundFlowConfig, SubmitOutcome},
    wire::ApplicationErrorCode,
};

const SCENARIO_TIMEOUT: Duration = Duration::from_secs(20);
const CONNECTION: ConnectionHandle = ConnectionHandle::new(701);
const MAX_MESSAGE_BYTES: usize = 512;

#[derive(Debug)]
struct HostState {
    negotiation: NegotiationManager,
    delivery: DeliveryEndpoint,
}

#[derive(Debug)]
struct PublicSide {
    connection: PublicConnection,
    host: HostState,
}

#[test]
fn peer_no_error_is_one_terminal_public_event_when_control_receive_is_polled_first() {
    run_test(async {
        let resources = resources(Duration::from_secs(5));
        let (client_endpoint, server_endpoint, mut client, server) =
            establish_public_pair(resources).await;

        server_endpoint.endpoint().close(
            ApplicationErrorCode::NoError.quinn(),
            b"peer no-error conformance",
        );

        let event = wait_public_result(&mut client)
            .await
            .expect("peer NO_ERROR was exposed as a public failure");
        match event {
            ConnectionEvent::PeerClosed { connection } => assert_eq!(connection, CONNECTION),
            other => panic!("peer NO_ERROR produced wrong public event: {other:?}"),
        }

        let terminal = match poll_public_once(&mut client) {
            Poll::Ready(Err(error)) => error,
            other => panic!("peer-closed public connection was not terminal: {other:?}"),
        };
        assert_eq!(
            terminal.kind(),
            ConnectionErrorKind::State(ConnectionStateError::Terminal)
        );

        assert_clean_teardown(teardown_public(client), 0);
        assert_clean_teardown(teardown_public(server), 0);
        close_endpoints(client_endpoint, server_endpoint).await;
    });
}

#[test]
fn peer_no_error_preserves_residual_reliable_obligation_for_teardown() {
    run_test(async {
        let resources = resources(Duration::from_secs(5));
        let (client_endpoint, server_endpoint, mut client, mut server) =
            establish_public_pair(resources).await;
        let (outbound, inbound) =
            establish_public_reliable_flow(&mut server, &mut client, 702, 703).await;

        assert_eq!(
            server
                .connection
                .submit(
                    &mut server.host.delivery,
                    outbound,
                    b"accepted-but-unexposed".to_vec(),
                )
                .unwrap(),
            SubmitOutcome::Accepted {
                accepted_index: 0,
                local_pressure_drops: 0,
            }
        );

        loop {
            let (server_event, client_event) = next_public_pair_event(&mut server, &mut client).await;
            assert!(server_event.is_none());
            match client_event {
                Some(ConnectionEvent::DataReady {
                    key,
                    buffered_messages,
                    local_pressure_drops,
                }) => {
                    assert_eq!(key, inbound);
                    assert_eq!(buffered_messages, 1);
                    assert_eq!(local_pressure_drops, 0);
                    break;
                }
                Some(other) => panic!("unexpected pre-close receiver event: {other:?}"),
                None => {}
            }
        }
        assert_eq!(client.host.delivery.pending_messages(), 1);

        server_endpoint.endpoint().close(
            ApplicationErrorCode::NoError.quinn(),
            b"peer no-error with reliable custody",
        );

        let event = wait_public_result(&mut client)
            .await
            .expect("peer NO_ERROR with residual reliable custody became a public failure");
        match event {
            ConnectionEvent::PeerClosed { connection } => assert_eq!(connection, CONNECTION),
            ConnectionEvent::FlowTerminated { .. } => {
                panic!("connection close fabricated a flow-termination event")
            }
            other => panic!("unexpected close event with residual reliable custody: {other:?}"),
        }

        let client_teardown = teardown_public(client);
        assert_eq!(client_teardown.connection(), CONNECTION);
        assert!(client_teardown.cleanup_error().is_none());
        assert_eq!(client_teardown.flow_terminations().len(), 1);
        let termination = client_teardown.flow_terminations()[0];
        assert_eq!(termination.key, inbound);
        assert_eq!(termination.reason, FlowTerminationReason::ConnectionEnded);
        assert_eq!(termination.pending_messages, 1);
        assert!(termination.reliable_obligation_failed);

        let server_teardown = teardown_public(server);
        assert_eq!(server_teardown.connection(), CONNECTION);
        assert!(server_teardown.cleanup_error().is_none());
        assert_eq!(server_teardown.flow_terminations().len(), 1);
        assert_eq!(server_teardown.flow_terminations()[0].key, outbound);
        assert_eq!(
            server_teardown.flow_terminations()[0].reason,
            FlowTerminationReason::ConnectionEnded
        );

        close_endpoints(client_endpoint, server_endpoint).await;
    });
}

#[test]
fn nonzero_peer_application_close_remains_public_failure() {
    run_test(async {
        let resources = resources(Duration::from_secs(5));
        let (client_endpoint, server_endpoint, mut client, server) =
            establish_public_pair(resources).await;

        server_endpoint.endpoint().close(
            ApplicationErrorCode::ProfileProtocolError.quinn(),
            b"nonzero peer close",
        );

        let error = wait_public_result(&mut client)
            .await
            .expect_err("nonzero peer application close was normalized as success");
        assert_transport_failure_kind(error.kind());

        assert_clean_teardown(teardown_public(client), 0);
        assert_clean_teardown(teardown_public(server), 0);
        close_endpoints(client_endpoint, server_endpoint).await;
    });
}

#[test]
fn genuine_idle_transport_loss_remains_public_failure() {
    run_test(async {
        let resources = resources(Duration::from_millis(250));
        let (client_endpoint, server_endpoint, mut client, server) =
            establish_public_pair(resources).await;

        let error = wait_public_result(&mut client)
            .await
            .expect_err("idle transport loss was normalized as peer NO_ERROR");
        assert_transport_failure_kind(error.kind());

        assert_clean_teardown(teardown_public(client), 0);
        assert_clean_teardown(teardown_public(server), 0);
        close_endpoints(client_endpoint, server_endpoint).await;
    });
}

#[test]
fn truncated_control_failure_wins_over_concurrent_peer_no_error() {
    run_test(async {
        let resources = resources(Duration::from_secs(5));
        let (client_endpoint, server_endpoint, mut client, server) =
            establish_public_pair(resources).await;
        let PublicSide {
            connection: server_connection,
            host: mut server_host,
        } = server;
        let (mut server_driver, reliable_receive) = server_connection
            .into_established_internal()
            .expect("Established event did not retain server driver");
        assert_eq!(reliable_receive, reliable_receive_limits());

        // 0x40 begins a two-byte QUIC varint. FIN acknowledgement proves the peer transport
        // received this partial control frame before the concurrent connection close is sent.
        server_driver
            .send_raw_control_bytes_for_test(&[0x40])
            .await
            .expect("partial control frame was not written");
        server_driver
            .finish_control_stream_for_test()
            .await
            .expect("partial control FIN was not acknowledged");
        server_driver.close_for_test(ApplicationErrorCode::NoError);

        let error = wait_public_result(&mut client)
            .await
            .expect_err("truncated control frame was masked by concurrent peer NO_ERROR");
        assert_eq!(
            error.kind(),
            ConnectionErrorKind::EstablishedControl(ProfileBootstrapFailure::Control)
        );

        assert_clean_teardown(teardown_public(client), 0);
        let server_teardown = server_driver.teardown(
            &mut server_host.negotiation,
            &mut server_host.delivery,
        );
        assert_eq!(server_teardown.connection, CONNECTION);
        assert!(server_teardown.negotiation_cleanup_error.is_none());
        assert!(server_teardown.flow_terminations.is_empty());

        close_endpoints(client_endpoint, server_endpoint).await;
    });
}

fn run_test(future: impl Future<Output = ()>) {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, future)
            .await
            .expect("peer-close conformance scenario timed out");
    });
}

async fn establish_public_pair(
    resources: ValidatedEndpointResources,
) -> (ConfiguredEndpoint, ConfiguredEndpoint, PublicSide, PublicSide) {
    let (client_endpoint, server_endpoint) = configured_endpoints(resources);
    let server_address = server_endpoint.endpoint().local_addr().unwrap();
    let (client_ready, server_ready) = join2(
        connect_profile_ready(
            &client_endpoint,
            server_address,
            "localhost",
            profile(resources, SemanticRole::Authority),
        ),
        accept_profile_ready(
            &server_endpoint,
            profile(resources, SemanticRole::NonAuthority),
        ),
    )
    .await;
    let client_ready = client_ready.expect("client failed ProfileReady");
    let server_ready = server_ready
        .expect("server failed ProfileReady")
        .expect("server endpoint closed before ProfileReady");

    let mut client_host = new_host();
    let mut server_host = new_host();
    let client_connection = activate_public_with_host(client_ready, &mut client_host);
    let server_connection = activate_public_with_host(server_ready, &mut server_host);
    let mut client = PublicSide {
        connection: client_connection,
        host: client_host,
    };
    let mut server = PublicSide {
        connection: server_connection,
        host: server_host,
    };

    let mut client_established = false;
    let mut server_established = false;
    let mut authority_selected = false;
    poll_fn(|cx| {
        if !client_established {
            match client.connection.poll(
                cx,
                &mut client.host.negotiation,
                &mut client.host.delivery,
            ) {
                Poll::Pending => {}
                Poll::Ready(Ok(ConnectionEvent::AuthoritySelectionRequired { connection })) => {
                    assert_eq!(connection, CONNECTION);
                    assert!(!authority_selected);
                    client
                        .connection
                        .select_authority(&mut client.host.negotiation, contract())
                        .expect("valid authority selection failed");
                    authority_selected = true;
                    cx.waker().wake_by_ref();
                }
                Poll::Ready(Ok(ConnectionEvent::Established { connection })) => {
                    assert_eq!(connection, CONNECTION);
                    client_established = true;
                }
                Poll::Ready(Ok(other)) => {
                    panic!("unexpected client event during negotiation: {other:?}")
                }
                Poll::Ready(Err(error)) => panic!("client negotiation failed: {error:?}"),
            }
        }

        if !server_established {
            match server.connection.poll(
                cx,
                &mut server.host.negotiation,
                &mut server.host.delivery,
            ) {
                Poll::Pending => {}
                Poll::Ready(Ok(ConnectionEvent::Established { connection })) => {
                    assert_eq!(connection, CONNECTION);
                    server_established = true;
                }
                Poll::Ready(Ok(ConnectionEvent::AuthoritySelectionRequired { .. })) => {
                    panic!("NonAuthority server requested authority selection")
                }
                Poll::Ready(Ok(other)) => {
                    panic!("unexpected server event during negotiation: {other:?}")
                }
                Poll::Ready(Err(error)) => panic!("server negotiation failed: {error:?}"),
            }
        }

        if client_established && server_established {
            assert!(authority_selected);
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;

    (client_endpoint, server_endpoint, client, server)
}

fn activate_public_with_host(
    admitted: AdmittedProfileReadyConnection,
    host: &mut HostState,
) -> PublicConnection {
    ProfileReadyConnection::from_profile(admitted, reliable_receive_limits())
        .activate(
            CONNECTION,
            offer(),
            NegotiationRequirements::default(),
            &mut host.negotiation,
        )
        .expect("valid public activation failed")
}

async fn establish_public_reliable_flow(
    sender: &mut PublicSide,
    receiver: &mut PublicSide,
    outbound_handle: u64,
    inbound_handle: u64,
) -> (DeliveryFlowKey, DeliveryFlowKey) {
    let mode = DeliveryMode::ReliableOrdered;
    let outbound = DeliveryFlowKey::new(
        CONNECTION,
        FlowDirection::Outbound,
        DeliveryFlowHandle::new(outbound_handle),
    );
    let inbound = DeliveryFlowKey::new(
        CONNECTION,
        FlowDirection::Inbound,
        DeliveryFlowHandle::new(inbound_handle),
    );
    sender
        .connection
        .open_outbound_flow(
            &sender.host.delivery,
            OutboundFlowConfig {
                key: outbound,
                mode,
                policy: policy(mode),
                connection_limits: connection_limits(),
            },
        )
        .unwrap();

    let mut accepted = false;
    loop {
        let (sender_event, receiver_event) = next_public_pair_event(sender, receiver).await;
        if let Some(event) = receiver_event {
            match event {
                ConnectionEvent::IncomingFlowRequested { request } => {
                    assert!(!accepted);
                    accepted = true;
                    receiver
                        .connection
                        .accept_incoming_flow(
                            &mut receiver.host.delivery,
                            request,
                            InboundFlowConfig {
                                key: inbound,
                                policy: policy(mode),
                                connection_limits: connection_limits(),
                            },
                        )
                        .unwrap();
                }
                other => panic!("unexpected receiver flow-establishment event: {other:?}"),
            }
        }
        if let Some(event) = sender_event {
            match event {
                ConnectionEvent::OutboundFlowEstablished { key } => {
                    assert!(accepted);
                    assert_eq!(key, outbound);
                    return (outbound, inbound);
                }
                other => panic!("unexpected sender flow-establishment event: {other:?}"),
            }
        }
    }
}

async fn next_public_pair_event(
    first: &mut PublicSide,
    second: &mut PublicSide,
) -> (Option<ConnectionEvent>, Option<ConnectionEvent>) {
    poll_fn(|cx| {
        let first_event = match first.connection.poll(
            cx,
            &mut first.host.negotiation,
            &mut first.host.delivery,
        ) {
            Poll::Pending => None,
            Poll::Ready(Ok(event)) => Some(event),
            Poll::Ready(Err(error)) => panic!("first public connection failed: {error:?}"),
        };
        let second_event = match second.connection.poll(
            cx,
            &mut second.host.negotiation,
            &mut second.host.delivery,
        ) {
            Poll::Pending => None,
            Poll::Ready(Ok(event)) => Some(event),
            Poll::Ready(Err(error)) => panic!("second public connection failed: {error:?}"),
        };
        if first_event.is_some() || second_event.is_some() {
            Poll::Ready((first_event, second_event))
        } else {
            Poll::Pending
        }
    })
    .await
}

async fn wait_public_result(side: &mut PublicSide) -> Result<ConnectionEvent, ConnectionError> {
    poll_fn(|cx| {
        side.connection
            .poll(cx, &mut side.host.negotiation, &mut side.host.delivery)
    })
    .await
}

fn poll_public_once(side: &mut PublicSide) -> Poll<Result<ConnectionEvent, ConnectionError>> {
    let mut cx = Context::from_waker(Waker::noop());
    side.connection
        .poll(&mut cx, &mut side.host.negotiation, &mut side.host.delivery)
}

fn teardown_public(mut side: PublicSide) -> crate::ConnectionTeardown {
    side.connection
        .teardown(&mut side.host.negotiation, &mut side.host.delivery)
}

fn assert_clean_teardown(teardown: crate::ConnectionTeardown, expected_flows: usize) {
    assert_eq!(teardown.connection(), CONNECTION);
    assert!(teardown.cleanup_error().is_none());
    assert_eq!(teardown.flow_terminations().len(), expected_flows);
}

fn assert_transport_failure_kind(kind: ConnectionErrorKind) {
    assert!(
        matches!(
            kind,
            ConnectionErrorKind::EstablishedTransport
                | ConnectionErrorKind::EstablishedControl(ProfileBootstrapFailure::Transport)
        ),
        "expected existing transport failure classification, got {kind:?}"
    );
}

async fn close_endpoints(client: ConfiguredEndpoint, server: ConfiguredEndpoint) {
    client
        .endpoint()
        .close(ApplicationErrorCode::NoError.quinn(), b"test complete");
    server
        .endpoint()
        .close(ApplicationErrorCode::NoError.quinn(), b"test complete");
    join2(client.endpoint().wait_idle(), server.endpoint().wait_idle()).await;
}

fn configured_endpoints(
    resources: ValidatedEndpointResources,
) -> (ConfiguredEndpoint, ConfiguredEndpoint) {
    let (certificate, private_key, roots) = ephemeral_identity();
    let server = bind_server_endpoint(
        loopback_ephemeral(),
        resources,
        vec![certificate],
        private_key,
    )
    .unwrap();
    let client = bind_client_endpoint(loopback_ephemeral(), resources, roots).unwrap();
    (client, server)
}

fn new_host() -> HostState {
    HostState {
        negotiation: NegotiationManager::new(
            OfferLimits::default(),
            NegotiationManagerLimits::default(),
        )
        .unwrap(),
        delivery: DeliveryEndpoint::new(aggregate_limits()),
    }
}

fn resources(max_idle_timeout: Duration) -> ValidatedEndpointResources {
    EndpointResourceLimits {
        max_connections: 4,
        max_active_incoming_flows: 16,
        udp_payload_ceiling: 1_452,
        stream_receive_window: 64 * 1024,
        connection_receive_window: 256 * 1024,
        send_window: 256 * 1024,
        crypto_buffer_bytes: 32 * 1024,
        datagram_receive_buffer_bytes: 64 * 1024,
        datagram_send_buffer_bytes: 64 * 1024,
        max_idle_timeout,
    }
    .validate()
    .unwrap()
}

fn profile(
    resources: ValidatedEndpointResources,
    semantic_role: SemanticRole,
) -> ValidatedControlProfile {
    LocalControlLimits {
        semantic_role,
        max_control_frame_bytes: 64 * 1024,
        max_negotiation_frame_bytes: 32 * 1024,
        max_incoming_message_bytes: 128 * 1024,
    }
    .validate(resources)
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

fn policy(mode: DeliveryMode) -> FlowResourcePolicy {
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

fn aggregate_limits() -> DeliveryScopeLimits {
    DeliveryScopeLimits::new(nz(64), nz(128), nz(1024 * 1024))
}

fn connection_limits() -> DeliveryScopeLimits {
    DeliveryScopeLimits::new(nz(32), nz(64), nz(512 * 1024))
}

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn ephemeral_identity() -> (
    quinn::rustls::pki_types::CertificateDer<'static>,
    PrivateKeyDer<'static>,
    Arc<RootCertStore>,
) {
    let CertifiedKey { cert, signing_key } =
        generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let certificate = cert.der().clone();
    let private_key = PrivatePkcs8KeyDer::from(signing_key.serialize_der()).into();
    let mut roots = RootCertStore::empty();
    roots.add(certificate.clone()).unwrap();
    (certificate, private_key, Arc::new(roots))
}

const fn loopback_ephemeral() -> SocketAddr {
    SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 0)
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
