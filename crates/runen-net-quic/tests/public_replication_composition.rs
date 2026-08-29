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
    CertificateDer, ClientEndpoint, ClientTrust, ConnectionEvent, EndpointConfig,
    EndpointResourceLimits, InboundFlowConfig, OutboundFlowConfig, PrivateKeyDer, ProfileConfig,
    ProfileLimits, ReliableReceiveLimits, SemanticRole, ServerEndpoint, ServerIdentity,
    SubmitOutcome,
};
use rustls_pki_types::PrivatePkcs8KeyDer;
use tokio::runtime::Builder;

const TIMEOUT: Duration = Duration::from_secs(20);
const CLIENT_CONNECTION: ConnectionHandle = ConnectionHandle::new(41);
const SERVER_CONNECTION: ConnectionHandle = ConnectionHandle::new(99);
const MAX_MESSAGE_BYTES: usize = 512;

#[test]
fn live_public_quic_submission_composes_directly_with_authority_replication() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(TIMEOUT, async {
            let config = endpoint_limits().validate().unwrap();
            let (certificate, private_key) = ephemeral_identity();
            let client_endpoint = ClientEndpoint::bind(
                loopback_ephemeral(),
                config,
                ClientTrust::new(vec![certificate.clone()]).unwrap(),
            )
            .unwrap();
            let server_endpoint = ServerEndpoint::bind(
                loopback_ephemeral(),
                config,
                ServerIdentity::new(vec![certificate], private_key).unwrap(),
            )
            .unwrap();

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

            let mut client_negotiation = negotiation_manager();
            let mut server_negotiation = negotiation_manager();
            let mut client_delivery = delivery_endpoint();
            let mut server_delivery = delivery_endpoint();
            let mut client = client_ready
                .activate(
                    CLIENT_CONNECTION,
                    offer(),
                    NegotiationRequirements::default(),
                    reliable_receive_limits(),
                    &mut client_negotiation,
                )
                .unwrap();
            let mut server = server_ready
                .activate(
                    SERVER_CONNECTION,
                    offer(),
                    NegotiationRequirements::default(),
                    reliable_receive_limits(),
                    &mut server_negotiation,
                )
                .unwrap();

            poll_fn(|cx| {
                match client.poll(cx, &mut client_negotiation, &mut client_delivery) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(ConnectionEvent::AuthoritySelectionRequired { connection })) => {
                        assert_eq!(connection, CLIENT_CONNECTION);
                        return Poll::Ready(());
                    }
                    Poll::Ready(Ok(event)) => {
                        panic!("unexpected client event before Authority selection: {event:?}")
                    }
                    Poll::Ready(Err(error)) => panic!("client negotiation failed: {error:?}"),
                }
                match server.poll(cx, &mut server_negotiation, &mut server_delivery) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Ok(event)) => {
                        panic!("unexpected server event before Authority selection: {event:?}")
                    }
                    Poll::Ready(Err(error)) => panic!("server negotiation failed: {error:?}"),
                }
            })
            .await;
            client
                .select_authority(&mut client_negotiation, contract())
                .unwrap();

            let mut client_established = false;
            let mut server_established = false;
            poll_fn(|cx| {
                if !client_established {
                    match client.poll(cx, &mut client_negotiation, &mut client_delivery) {
                        Poll::Pending => {}
                        Poll::Ready(Ok(ConnectionEvent::Established { connection })) => {
                            assert_eq!(connection, CLIENT_CONNECTION);
                            client_established = true;
                        }
                        Poll::Ready(Ok(event)) => {
                            panic!("unexpected client establishment event: {event:?}")
                        }
                        Poll::Ready(Err(error)) => {
                            panic!("client establishment failed: {error:?}")
                        }
                    }
                }
                if !server_established {
                    match server.poll(cx, &mut server_negotiation, &mut server_delivery) {
                        Poll::Pending => {}
                        Poll::Ready(Ok(ConnectionEvent::Established { connection })) => {
                            assert_eq!(connection, SERVER_CONNECTION);
                            server_established = true;
                        }
                        Poll::Ready(Ok(event)) => {
                            panic!("unexpected server establishment event: {event:?}")
                        }
                        Poll::Ready(Err(error)) => {
                            panic!("server establishment failed: {error:?}")
                        }
                    }
                }
                if client_established && server_established {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            })
            .await;

            let outbound = DeliveryFlowKey::new(
                CLIENT_CONNECTION,
                FlowDirection::Outbound,
                DeliveryFlowHandle::new(1),
            );
            let inbound = DeliveryFlowKey::new(
                SERVER_CONNECTION,
                FlowDirection::Inbound,
                DeliveryFlowHandle::new(101),
            );
            client
                .open_outbound_flow(
                    &client_delivery,
                    OutboundFlowConfig {
                        key: outbound,
                        mode: DeliveryMode::ReliableOrdered,
                        policy: reliable_policy(),
                        connection_limits: connection_limits(),
                    },
                )
                .unwrap();

            let request = poll_fn(|cx| {
                match client.poll(cx, &mut client_negotiation, &mut client_delivery) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(event)) => {
                        panic!("unexpected client event before flow admission: {event:?}")
                    }
                    Poll::Ready(Err(error)) => panic!("client flow driver failed: {error:?}"),
                }
                match server.poll(cx, &mut server_negotiation, &mut server_delivery) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Ok(ConnectionEvent::IncomingFlowRequested { request })) => {
                        Poll::Ready(request)
                    }
                    Poll::Ready(Ok(event)) => {
                        panic!("unexpected server event before flow admission: {event:?}")
                    }
                    Poll::Ready(Err(error)) => panic!("server flow driver failed: {error:?}"),
                }
            })
            .await;
            server
                .accept_incoming_flow(
                    &mut server_delivery,
                    request,
                    InboundFlowConfig {
                        key: inbound,
                        policy: reliable_policy(),
                        connection_limits: connection_limits(),
                    },
                )
                .unwrap();

            poll_fn(|cx| {
                match server.poll(cx, &mut server_negotiation, &mut server_delivery) {
                    Poll::Pending => {}
                    Poll::Ready(Ok(event)) => {
                        panic!("unexpected server flow-establishment event: {event:?}")
                    }
                    Poll::Ready(Err(error)) => panic!("server flow driver failed: {error:?}"),
                }
                match client.poll(cx, &mut client_negotiation, &mut client_delivery) {
                    Poll::Pending => Poll::Pending,
                    Poll::Ready(Ok(ConnectionEvent::OutboundFlowEstablished { key })) => {
                        assert_eq!(key, outbound);
                        Poll::Ready(())
                    }
                    Poll::Ready(Ok(event)) => {
                        panic!("unexpected client flow-establishment event: {event:?}")
                    }
                    Poll::Ready(Err(error)) => panic!("client flow driver failed: {error:?}"),
                }
            })
            .await;

            let participant = ParticipantId::new(1);
            let mut replication = AuthorityReplicationSession::<u32, ()>::new(
                SessionId::new(1),
                AuthorityAggregateLimits::new(nz(2), nz(128), nz(4), nz(128), nz(4)),
            );
            replication
                .add_lineage(
                    participant,
                    ReplicationRetentionLimits::new(nz(16), nz(2), nz(32), nz(16), nz(2)).unwrap(),
                )
                .unwrap();
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

            let submission = client
                .submit(&mut client_delivery, outbound, b"snapshot-1".to_vec())
                .unwrap();
            assert!(matches!(submission, SubmitOutcome::Accepted { .. }));
            let emitted = replication
                .record_delivery_acceptance(participant, submission.acceptance())
                .unwrap()
                .expect("actual public QUIC acceptance must record replication emission");
            assert_eq!(emitted.target_cursor, ReplicationCursor::new(1));

            let client_teardown = client.teardown(&mut client_negotiation, &mut client_delivery);
            let server_teardown = server.teardown(&mut server_negotiation, &mut server_delivery);
            assert!(client_teardown.cleanup_error().is_none());
            assert!(server_teardown.cleanup_error().is_none());
            client_endpoint.close();
            server_endpoint.close();
            join2(client_endpoint.wait_idle(), server_endpoint.wait_idle()).await;
        })
        .await
        .expect("public QUIC + replication composition scenario timed out");
    });
}

fn negotiation_manager() -> NegotiationManager {
    NegotiationManager::new(OfferLimits::default(), NegotiationManagerLimits::default()).unwrap()
}

fn delivery_endpoint() -> DeliveryEndpoint {
    DeliveryEndpoint::new(DeliveryScopeLimits::new(nz(64), nz(128), nz(1024 * 1024)))
}

fn reliable_policy() -> FlowResourcePolicy {
    FlowResourcePolicy::new(
        nz(MAX_MESSAGE_BYTES),
        nz(8),
        nz(8 * MAX_MESSAGE_BYTES),
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::TerminateReliable,
    )
}

fn connection_limits() -> DeliveryScopeLimits {
    DeliveryScopeLimits::new(nz(32), nz(64), nz(512 * 1024))
}

fn endpoint_limits() -> EndpointResourceLimits {
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
