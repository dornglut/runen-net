use std::{
    future::{Future, poll_fn},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    pin::pin,
    task::Poll,
    time::Duration,
};

use rcgen::{CertifiedKey, generate_simple_self_signed};
use runen_net::{
    delivery::{
        DeliveryEndpoint, DeliveryFlowHandle, DeliveryFlowKey, DeliveryMode, DeliveryScopeLimits,
        FlowDirection, FlowResourcePolicy, OutboundPressureBehavior, ReceiverPressureBehavior,
    },
    identity::{ConnectionHandle, ParticipantId, SessionId, SimulationTick},
    protocol::{
        CompatibilityOffer, NegotiatedContract, NegotiationManager, NegotiationManagerLimits,
        NegotiationRequirements, OfferLimits, ProtocolContract, ProtocolId, ProtocolRevision,
    },
    replication::{
        AccountedState, AuthorityAggregateLimits, AuthorityReplicationSession, FullSnapshot,
        ReplicationCursor, ReplicationRetentionLimits,
    },
};
use runen_net_quic::{
    CertificateDer, ClientEndpoint, ClientTrust, Connection, ConnectionEvent, EndpointConfig,
    EndpointResourceLimits, InboundFlowConfig, OutboundFlowConfig, PrivateKeyDer, ProfileConfig,
    ProfileLimits, ProfileReadyConnection, ReliableReceiveLimits, SemanticRole, ServerEndpoint,
    ServerIdentity, SubmitOutcome,
};
use rustls_pki_types::PrivatePkcs8KeyDer;
use tokio::runtime::Builder;

const SCENARIO_TIMEOUT: Duration = Duration::from_secs(20);
const CLIENT_CONNECTION: ConnectionHandle = ConnectionHandle::new(41);
const SERVER_CONNECTION: ConnectionHandle = ConnectionHandle::new(99);
const RELIABLE_MAX_MESSAGE_BYTES: usize = 512;
const UNRELIABLE_MAX_MESSAGE_BYTES: usize = 4 * 1024;

struct HostState {
    negotiation: NegotiationManager,
    delivery: DeliveryEndpoint,
}

#[test]
fn live_public_quic_submission_evidence_composes_directly_with_replication() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, run_live_composition())
            .await
            .expect("public QUIC + replication composition scenario timed out");
    });
}

async fn run_live_composition() {
    let config = resource_limits().validate().unwrap();
    let (client_endpoint, server_endpoint) = endpoints(config);
    let mut client_host = host_state();
    let mut server_host = host_state();
    let (mut client, mut server) = establish_pair(
        &client_endpoint,
        &server_endpoint,
        config,
        &mut client_host,
        &mut server_host,
    )
    .await;

    let participant = ParticipantId::new(1);
    let mut replication = authority();
    replication.add_lineage(participant, retention()).unwrap();

    let (reliable_outbound, reliable_inbound) = open_and_accept_flow(
        &mut client,
        &mut client_host,
        &mut server,
        &mut server_host,
        DeliveryMode::ReliableOrdered,
        1,
        101,
        RELIABLE_MAX_MESSAGE_BYTES,
    )
    .await;
    replication
        .prepare_full(
            participant,
            FullSnapshot::new(
                ReplicationCursor::new(1),
                SimulationTick::new(1),
                AccountedState::new(7, 4),
            ),
            true,
        )
        .unwrap();

    let accepted = client
        .submit(
            &mut client_host.delivery,
            reliable_outbound,
            b"snapshot-1".to_vec(),
        )
        .unwrap();
    assert!(matches!(accepted, SubmitOutcome::Accepted { .. }));
    let emitted = replication
        .record_delivery_acceptance(participant, accepted.acceptance())
        .unwrap()
        .expect("actual public QUIC acceptance must record replication emission");
    assert_eq!(emitted.target_cursor, ReplicationCursor::new(1));
    assert!(
        replication
            .lineage(participant)
            .unwrap()
            .pending_snapshot()
            .is_none()
    );
    receive_one(
        &mut client,
        &mut client_host,
        &mut server,
        &mut server_host,
        reliable_inbound,
        b"snapshot-1",
    )
    .await;

    let (unreliable_outbound, _unreliable_inbound) = open_and_accept_flow(
        &mut client,
        &mut client_host,
        &mut server,
        &mut server_host,
        DeliveryMode::UnreliableUnordered,
        2,
        102,
        UNRELIABLE_MAX_MESSAGE_BYTES,
    )
    .await;
    replication
        .prepare_full(
            participant,
            FullSnapshot::new(
                ReplicationCursor::new(2),
                SimulationTick::new(2),
                AccountedState::new(8, 4),
            ),
            false,
        )
        .unwrap();

    let adapter_rejected = client
        .submit(
            &mut client_host.delivery,
            unreliable_outbound,
            vec![0; 2 * 1024],
        )
        .unwrap();
    assert_eq!(adapter_rejected, SubmitOutcome::RejectedCurrentDatagramSize);
    assert_eq!(
        replication
            .record_delivery_acceptance(participant, adapter_rejected.acceptance())
            .unwrap(),
        None
    );
    assert!(
        replication
            .lineage(participant)
            .unwrap()
            .pending_snapshot()
            .is_some()
    );
    assert_eq!(
        replication
            .lineage(participant)
            .unwrap()
            .greatest_emitted_cursor(),
        Some(ReplicationCursor::new(1))
    );

    let client_teardown = client.teardown(&mut client_host.negotiation, &mut client_host.delivery);
    let server_teardown = server.teardown(&mut server_host.negotiation, &mut server_host.delivery);
    assert!(client_teardown.cleanup_error().is_none());
    assert!(server_teardown.cleanup_error().is_none());

    client_endpoint.close();
    server_endpoint.close();
    join2(client_endpoint.wait_idle(), server_endpoint.wait_idle()).await;
}

async fn establish_pair(
    client_endpoint: &ClientEndpoint,
    server_endpoint: &ServerEndpoint,
    config: EndpointConfig,
    client_host: &mut HostState,
    server_host: &mut HostState,
) -> (Connection, Connection) {
    let server_address = server_endpoint.local_addr().unwrap();
    let (client_ready, server_ready) = join2(
        client_endpoint.connect(
            server_address,
            "localhost",
            profile(config, SemanticRole::Authority),
        ),
        server_endpoint.accept(profile(config, SemanticRole::NonAuthority)),
    )
    .await;
    let client_ready = client_ready.expect("client failed to reach ProfileReady");
    let server_ready = server_ready
        .expect("server failed to accept ProfileReady")
        .expect("server endpoint closed before ProfileReady");
    let (mut client, mut server) = activate_pair(client_ready, server_ready, client_host, server_host);

    poll_fn(|cx| {
        match client.poll(cx, &mut client_host.negotiation, &mut client_host.delivery) {
            Poll::Pending => {}
            Poll::Ready(Ok(ConnectionEvent::AuthoritySelectionRequired { connection })) => {
                assert_eq!(connection, CLIENT_CONNECTION);
                return Poll::Ready(());
            }
            Poll::Ready(Ok(event)) => panic!("unexpected client event before Authority selection: {event:?}"),
            Poll::Ready(Err(error)) => panic!("client negotiation failed: {error:?}"),
        }
        match server.poll(cx, &mut server_host.negotiation, &mut server_host.delivery) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(event)) => {
                panic!("unexpected server event before Authority selection: {event:?}")
            }
            Poll::Ready(Err(error)) => panic!("server negotiation failed: {error:?}"),
        }
    })
    .await;

    client
        .select_authority(&mut client_host.negotiation, contract())
        .unwrap();
    drive_until_established(&mut client, client_host, &mut server, server_host).await;
    (client, server)
}

fn activate_pair(
    client_ready: ProfileReadyConnection,
    server_ready: ProfileReadyConnection,
    client_host: &mut HostState,
    server_host: &mut HostState,
) -> (Connection, Connection) {
    let client = client_ready
        .activate(
            CLIENT_CONNECTION,
            offer(),
            NegotiationRequirements::default(),
            reliable_receive_limits(),
            &mut client_host.negotiation,
        )
        .unwrap();
    let server = server_ready
        .activate(
            SERVER_CONNECTION,
            offer(),
            NegotiationRequirements::default(),
            reliable_receive_limits(),
            &mut server_host.negotiation,
        )
        .unwrap();
    (client, server)
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
                Poll::Ready(Ok(event)) => {
                    panic!("unexpected client event while establishing: {event:?}")
                }
                Poll::Ready(Err(error)) => panic!("client establishment failed: {error:?}"),
            }
        }
        if !server_established {
            match server.poll(cx, &mut server_host.negotiation, &mut server_host.delivery) {
                Poll::Pending => {}
                Poll::Ready(Ok(ConnectionEvent::Established { connection })) => {
                    assert_eq!(connection, SERVER_CONNECTION);
                    server_established = true;
                }
                Poll::Ready(Ok(event)) => {
                    panic!("unexpected server event while establishing: {event:?}")
                }
                Poll::Ready(Err(error)) => panic!("server establishment failed: {error:?}"),
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

async fn open_and_accept_flow(
    client: &mut Connection,
    client_host: &mut HostState,
    server: &mut Connection,
    server_host: &mut HostState,
    mode: DeliveryMode,
    outbound_handle: u64,
    inbound_handle: u64,
    max_message_bytes: usize,
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
                policy: flow_policy(mode, max_message_bytes),
                connection_limits: flow_connection_limits(),
                stable_max_message_bytes: nz(max_message_bytes),
            },
        )
        .unwrap();

    let request = loop {
        let (client_event, server_event) = next_pair_event(client, client_host, server, server_host).await;
        assert!(client_event.is_none());
        match server_event {
            Some(ConnectionEvent::IncomingFlowRequested { request }) => break request,
            Some(event) => panic!("unexpected server flow request event: {event:?}"),
            None => {}
        }
    };
    server
        .accept_incoming_flow(
            &mut server_host.delivery,
            request,
            InboundFlowConfig {
                key: inbound,
                policy: flow_policy(mode, max_message_bytes),
                connection_limits: flow_connection_limits(),
            },
        )
        .unwrap();

    loop {
        let (client_event, server_event) = next_pair_event(client, client_host, server, server_host).await;
        assert!(server_event.is_none());
        match client_event {
            Some(ConnectionEvent::OutboundFlowEstablished { key }) => {
                assert_eq!(key, outbound);
                break;
            }
            Some(event) => panic!("unexpected client flow-establishment event: {event:?}"),
            None => {}
        }
    }
    (outbound, inbound)
}

async fn receive_one(
    client: &mut Connection,
    client_host: &mut HostState,
    server: &mut Connection,
    server_host: &mut HostState,
    inbound: DeliveryFlowKey,
    expected: &[u8],
) {
    loop {
        let (client_event, server_event) = next_pair_event(client, client_host, server, server_host).await;
        assert!(client_event.is_none());
        match server_event {
            Some(ConnectionEvent::DataReady { key, .. }) => {
                assert_eq!(key, inbound);
                break;
            }
            Some(event) => panic!("unexpected server data event: {event:?}"),
            None => {}
        }
    }
    let exposed = server_host
        .delivery
        .poll_exposure(inbound)
        .unwrap()
        .expect("DataReady did not leave payload in Core custody");
    assert_eq!(exposed.payload(), expected);
}

async fn next_pair_event(
    client: &mut Connection,
    client_host: &mut HostState,
    server: &mut Connection,
    server_host: &mut HostState,
) -> (Option<ConnectionEvent>, Option<ConnectionEvent>) {
    poll_fn(|cx| {
        let client_event = match client.poll(cx, &mut client_host.negotiation, &mut client_host.delivery) {
            Poll::Pending => None,
            Poll::Ready(Ok(event)) => Some(event),
            Poll::Ready(Err(error)) => panic!("public client driver failed: {error:?}"),
        };
        let server_event = match server.poll(cx, &mut server_host.negotiation, &mut server_host.delivery) {
            Poll::Pending => None,
            Poll::Ready(Ok(event)) => Some(event),
            Poll::Ready(Err(error)) => panic!("public server driver failed: {error:?}"),
        };
        if client_event.is_some() || server_event.is_some() {
            Poll::Ready((client_event, server_event))
        } else {
            Poll::Pending
        }
    })
    .await
}

fn authority() -> AuthorityReplicationSession<u32, ()> {
    AuthorityReplicationSession::new(
        SessionId::new(1),
        AuthorityAggregateLimits::new(nz(2), nz(128), nz(4), nz(128), nz(4)),
    )
}

fn retention() -> ReplicationRetentionLimits {
    ReplicationRetentionLimits::new(nz(16), nz(2), nz(32), nz(16), nz(2)).unwrap()
}

fn host_state() -> HostState {
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

fn flow_policy(mode: DeliveryMode, max_message_bytes: usize) -> FlowResourcePolicy {
    FlowResourcePolicy::new(
        nz(max_message_bytes),
        nz(8),
        nz(8 * max_message_bytes),
        OutboundPressureBehavior::RejectNew,
        match mode {
            DeliveryMode::ReliableOrdered => ReceiverPressureBehavior::TerminateReliable,
            DeliveryMode::UnreliableUnordered | DeliveryMode::UnreliableSequenced => {
                ReceiverPressureBehavior::DropIncomingUnreliable
            }
        },
    )
}

fn resource_limits() -> EndpointResourceLimits {
    EndpointResourceLimits {
        max_connections: 1,
        max_active_incoming_flows: 4,
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
