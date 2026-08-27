use std::{
    future::{Future, poll_fn},
    net::{Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::Duration,
};

use quinn::{
    VarInt,
    rustls::{
        RootCertStore,
        pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer},
    },
};
use rcgen::{CertifiedKey, generate_simple_self_signed};
use runen_net::{
    delivery::{
        DeliveryEndpoint, DeliveryFlowHandle, DeliveryFlowKey, DeliveryMode, DeliveryScopeLimits,
        FlowDirection, FlowResourcePolicy, FlowTerminationReason, OutboundPressureBehavior,
        ReceiverPressureBehavior, SubmissionOutcome,
    },
    identity::ConnectionHandle,
    protocol::{
        CompatibilityOffer, NegotiatedContract, NegotiationManager, NegotiationManagerLimits,
        NegotiationRequirements, OfferLimits, ProtocolContract, ProtocolId, ProtocolRevision,
    },
};
use tokio::runtime::Builder;

use crate::{
    connection_driver::{
        ConnectionDriverError, ConnectionDriverStateError, DatagramSubmitOutcome,
        EstablishedConnectionDriver, EstablishedConnectionProgress, OutboundFinishOutcome,
    },
    control::{LocalControlLimits, SemanticRole, ValidatedControlProfile},
    datagram::DatagramSubmissionOutcome,
    endpoint::{
        ConfiguredEndpoint, EndpointResourceLimits, ValidatedEndpointResources,
        bind_client_endpoint, bind_server_endpoint,
    },
    flow_control::{InboundAdmission, OutboundOpenRequest},
    lifecycle::{
        AdmittedProfileReadyConnection, EstablishedNegotiatedConnection, NegotiationSendCompletion,
        NegotiationTransition, PendingNegotiationSend, accept_profile_ready, begin_negotiation,
        connect_profile_ready,
    },
    quinn_binding::{ReceiveProgress, SendProgress},
    wire::{FlowId, WireSide},
};

const SCENARIO_TIMEOUT: Duration = Duration::from_secs(20);
const CONNECTION: ConnectionHandle = ConnectionHandle::new(1);
const MAX_MESSAGE_BYTES: usize = 512;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum AuthoritySide {
    Client,
    Server,
}

#[derive(Debug)]
struct HostState {
    negotiation: NegotiationManager,
    delivery: DeliveryEndpoint,
}

#[derive(Debug)]
struct LiveSide {
    driver: EstablishedConnectionDriver,
    host: HostState,
}

#[derive(Debug, Copy, Clone)]
struct LiveFlow {
    flow_id: FlowId,
    outbound: DeliveryFlowKey,
    inbound: DeliveryFlowKey,
}

#[test]
fn successful_live_quic_path_operates_with_authority_on_either_quic_side() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            run_successful_scenario(AuthoritySide::Client).await;
            run_successful_scenario(AuthoritySide::Server).await;
        })
        .await
        .expect("successful live QUIC conformance scenario timed out");
    });
}

#[test]
fn live_quic_connection_close_is_terminal_and_teardown_cleans_connection_state() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, run_connection_loss_scenario())
            .await
            .expect("live QUIC connection-loss conformance scenario timed out");
    });
}

async fn run_successful_scenario(authority_side: AuthoritySide) {
    let (client, server, mut client_side, mut server_side) =
        establish_live_pair(authority_side).await;

    let reliable = establish_flow(
        &mut client_side,
        &mut server_side,
        DeliveryMode::ReliableOrdered,
        1,
        101,
    )
    .await;
    let unordered = establish_flow(
        &mut server_side,
        &mut client_side,
        DeliveryMode::UnreliableUnordered,
        2,
        102,
    )
    .await;
    let sequenced = establish_flow(
        &mut client_side,
        &mut server_side,
        DeliveryMode::UnreliableSequenced,
        3,
        103,
    )
    .await;

    assert_eq!(reliable.flow_id.side(), WireSide::Client);
    assert_eq!(unordered.flow_id.side(), WireSide::Server);
    assert_eq!(sequenced.flow_id.side(), WireSide::Client);

    send_reliable_and_expect(
        &mut client_side,
        &mut server_side,
        reliable,
        b"reliable-live".to_vec(),
    )
    .await;
    send_unreliable_and_expect(
        &mut server_side,
        &mut client_side,
        unordered,
        b"unordered-live".to_vec(),
    )
    .await;
    send_unreliable_and_expect(
        &mut client_side,
        &mut server_side,
        sequenced,
        b"sequenced-live".to_vec(),
    )
    .await;

    close_reliable_normally(&mut client_side, &mut server_side, reliable).await;

    assert!(
        client_side
            .host
            .delivery
            .flow_contract(reliable.outbound)
            .is_none()
    );
    assert!(
        server_side
            .host
            .delivery
            .flow_contract(reliable.inbound)
            .is_none()
    );
    assert!(
        client_side
            .host
            .delivery
            .flow_contract(sequenced.outbound)
            .is_some()
    );
    assert!(
        server_side
            .host
            .delivery
            .flow_contract(sequenced.inbound)
            .is_some()
    );
    assert!(
        server_side
            .host
            .delivery
            .flow_contract(unordered.outbound)
            .is_some()
    );
    assert!(
        client_side
            .host
            .delivery
            .flow_contract(unordered.inbound)
            .is_some()
    );

    let client_teardown = client_side.driver.teardown(
        &mut client_side.host.negotiation,
        &mut client_side.host.delivery,
    );
    let server_teardown = server_side.driver.teardown(
        &mut server_side.host.negotiation,
        &mut server_side.host.delivery,
    );
    assert_eq!(client_teardown.connection, CONNECTION);
    assert_eq!(server_teardown.connection, CONNECTION);
    assert!(client_teardown.negotiation_cleanup_error.is_none());
    assert!(server_teardown.negotiation_cleanup_error.is_none());
    assert_eq!(client_teardown.flow_terminations.len(), 2);
    assert_eq!(server_teardown.flow_terminations.len(), 2);
    assert!(
        client_teardown
            .flow_terminations
            .iter()
            .all(|termination| termination.reason == FlowTerminationReason::ConnectionEnded)
    );
    assert!(
        server_teardown
            .flow_terminations
            .iter()
            .all(|termination| termination.reason == FlowTerminationReason::ConnectionEnded)
    );
    assert_eq!(client_side.host.delivery.active_flows(), 0);
    assert_eq!(server_side.host.delivery.active_flows(), 0);

    client
        .endpoint()
        .close(VarInt::from_u32(0), b"test complete");
    server
        .endpoint()
        .close(VarInt::from_u32(0), b"test complete");
    join2(client.endpoint().wait_idle(), server.endpoint().wait_idle()).await;
}

async fn establish_live_pair(
    authority_side: AuthoritySide,
) -> (ConfiguredEndpoint, ConfiguredEndpoint, LiveSide, LiveSide) {
    let resources = resources();
    let (certificate, private_key, roots) = ephemeral_identity();
    let server = bind_server_endpoint(
        loopback_ephemeral(),
        resources,
        vec![certificate],
        private_key,
    )
    .unwrap();
    let client = bind_client_endpoint(loopback_ephemeral(), resources, roots).unwrap();
    let server_address = server.endpoint().local_addr().unwrap();

    let client_profile = profile(
        resources,
        if authority_side == AuthoritySide::Client {
            SemanticRole::Authority
        } else {
            SemanticRole::NonAuthority
        },
    );
    let server_profile = profile(
        resources,
        if authority_side == AuthoritySide::Server {
            SemanticRole::Authority
        } else {
            SemanticRole::NonAuthority
        },
    );

    let (client_ready, server_ready) = join2(
        connect_profile_ready(&client, server_address, "localhost", client_profile),
        accept_profile_ready(&server, server_profile),
    )
    .await;
    let client_ready = client_ready.unwrap();
    let server_ready = server_ready
        .unwrap()
        .expect("server endpoint remained open");

    let contract = contract();
    let client_authority = (authority_side == AuthoritySide::Client).then(|| contract.clone());
    let server_authority = (authority_side == AuthoritySide::Server).then(|| contract.clone());

    let (client_established, server_established) = join2(
        negotiate_side(client_ready, new_manager(), client_authority),
        negotiate_side(server_ready, new_manager(), server_authority),
    )
    .await;

    let (client_negotiated, client_manager) = client_established;
    let (server_negotiated, server_manager) = server_established;
    let client_side = activate(client_negotiated, client_manager);
    let server_side = activate(server_negotiated, server_manager);

    (client, server, client_side, server_side)
}

async fn run_connection_loss_scenario() {
    let (client, server, mut client_side, mut server_side) =
        establish_live_pair(AuthoritySide::Server).await;

    let reliable = establish_flow(
        &mut client_side,
        &mut server_side,
        DeliveryMode::ReliableOrdered,
        11,
        111,
    )
    .await;
    let datagram = establish_flow(
        &mut server_side,
        &mut client_side,
        DeliveryMode::UnreliableSequenced,
        12,
        112,
    )
    .await;

    assert!(
        client_side
            .host
            .delivery
            .flow_contract(reliable.outbound)
            .is_some()
    );
    assert!(
        server_side
            .host
            .delivery
            .flow_contract(reliable.inbound)
            .is_some()
    );
    assert!(
        server_side
            .host
            .delivery
            .flow_contract(datagram.outbound)
            .is_some()
    );
    assert!(
        client_side
            .host
            .delivery
            .flow_contract(datagram.inbound)
            .is_some()
    );

    server
        .endpoint()
        .close(VarInt::from_u32(0), b"forced connection-close conformance");

    let error = wait_for_connection_terminal_error(&mut client_side).await;
    assert_connection_loss_driver_error(&error);
    assert_driver_terminal(&mut client_side).await;

    let client_teardown = client_side.driver.teardown(
        &mut client_side.host.negotiation,
        &mut client_side.host.delivery,
    );
    let server_teardown = server_side.driver.teardown(
        &mut server_side.host.negotiation,
        &mut server_side.host.delivery,
    );

    for teardown in [&client_teardown, &server_teardown] {
        assert_eq!(teardown.connection, CONNECTION);
        assert!(teardown.negotiation_cleanup_error.is_none());
        assert_eq!(teardown.flow_terminations.len(), 2);
        assert!(
            teardown.flow_terminations.iter().all(|termination| {
                termination.reason == FlowTerminationReason::ConnectionEnded
            })
        );
    }
    assert_eq!(client_side.host.delivery.active_flows(), 0);
    assert_eq!(server_side.host.delivery.active_flows(), 0);

    client
        .endpoint()
        .close(VarInt::from_u32(0), b"connection-loss test complete");
    join2(client.endpoint().wait_idle(), server.endpoint().wait_idle()).await;
}

async fn wait_for_connection_terminal_error(side: &mut LiveSide) -> ConnectionDriverError {
    poll_fn(
        |cx| match side.driver.poll_step(cx, &mut side.host.delivery) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(error),
            Poll::Ready(Ok(progress)) => {
                assert_non_failure_progress(&progress);
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        },
    )
    .await
}

fn assert_connection_loss_driver_error(error: &ConnectionDriverError) {
    assert!(
        matches!(
            error,
            ConnectionDriverError::ControlReceive(_)
                | ConnectionDriverError::Reliable(_)
                | ConnectionDriverError::Datagram(_)
        ),
        "connection close surfaced non-transport driver error: {error:?}"
    );
}

async fn assert_driver_terminal(side: &mut LiveSide) {
    poll_fn(
        |cx| match side.driver.poll_step(cx, &mut side.host.delivery) {
            Poll::Ready(Err(ConnectionDriverError::State(
                ConnectionDriverStateError::Terminal,
            ))) => Poll::Ready(()),
            other => panic!("connection-loss driver did not remain terminal: {other:?}"),
        },
    )
    .await;
}

fn resources() -> ValidatedEndpointResources {
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
        max_idle_timeout: Duration::from_secs(5),
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

fn offer() -> CompatibilityOffer {
    CompatibilityOffer::new(vec![protocol()], vec![], vec![], None)
}

const fn protocol() -> ProtocolContract {
    ProtocolContract::new(ProtocolId::new(1), ProtocolRevision::new(1))
}

fn contract() -> NegotiatedContract {
    NegotiatedContract::new(protocol())
}

fn new_manager() -> NegotiationManager {
    NegotiationManager::new(OfferLimits::default(), NegotiationManagerLimits::default()).unwrap()
}

fn aggregate_limits() -> DeliveryScopeLimits {
    limits(64, 128, 1024 * 1024)
}

fn connection_limits() -> DeliveryScopeLimits {
    limits(32, 64, 512 * 1024)
}

fn limits(flows: usize, messages: usize, bytes: usize) -> DeliveryScopeLimits {
    DeliveryScopeLimits::new(nz(flows), nz(messages), nz(bytes))
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

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

async fn negotiate_side(
    admitted: AdmittedProfileReadyConnection,
    mut manager: NegotiationManager,
    authority_contract: Option<NegotiatedContract>,
) -> (EstablishedNegotiatedConnection, NegotiationManager) {
    let requirements = NegotiationRequirements::default();
    let pending = begin_negotiation(admitted, CONNECTION, &mut manager, offer()).unwrap();
    let mut transition = complete_negotiation_send(pending).await;

    loop {
        transition = match transition {
            NegotiationTransition::Negotiating(mut negotiating) => {
                negotiating.receive().await.unwrap();
                negotiating
                    .into_received()
                    .unwrap()
                    .process(&mut manager, &requirements)
                    .unwrap()
            }
            NegotiationTransition::AuthoritySelection(selection) => {
                let pending = selection
                    .select(
                        &mut manager,
                        authority_contract
                            .clone()
                            .expect("only the semantic Authority selects a contract"),
                        &requirements,
                    )
                    .unwrap();
                complete_negotiation_send(pending).await
            }
            NegotiationTransition::PendingSend(pending) => complete_negotiation_send(pending).await,
            NegotiationTransition::Established(established) => {
                return (established, manager);
            }
        };
    }
}

async fn complete_negotiation_send(mut pending: PendingNegotiationSend) -> NegotiationTransition {
    pending.send().await.unwrap();
    match pending.complete().unwrap() {
        NegotiationSendCompletion::Negotiating(negotiating) => {
            NegotiationTransition::Negotiating(negotiating)
        }
        NegotiationSendCompletion::Established(established) => {
            NegotiationTransition::Established(established)
        }
        NegotiationSendCompletion::LocalFailure(outcome) => {
            panic!("valid loopback negotiation failed locally: {outcome:?}")
        }
    }
}

fn activate(
    established: EstablishedNegotiatedConnection,
    negotiation: NegotiationManager,
) -> LiveSide {
    let driver = established
        .into_flow_control()
        .unwrap()
        .into_reliable_io(nz(4 * 1024), nz(128 * 1024))
        .into_established_io()
        .into_connection_driver();
    LiveSide {
        driver,
        host: HostState {
            negotiation,
            delivery: DeliveryEndpoint::new(aggregate_limits()),
        },
    }
}

async fn establish_flow(
    sender: &mut LiveSide,
    receiver: &mut LiveSide,
    mode: DeliveryMode,
    outbound_handle: u64,
    inbound_handle: u64,
) -> LiveFlow {
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
        .driver
        .open_outbound(
            &sender.host.delivery,
            OutboundOpenRequest {
                key: outbound,
                mode,
                policy: policy(mode),
                stable_max_message_bytes: nz(MAX_MESSAGE_BYTES),
                connection_limits: connection_limits(),
            },
        )
        .unwrap();

    let mut accepted_flow_id = None;
    let mut established_flow_id = None;
    let mut accept_send_completed = false;
    loop {
        let (sender_progress, receiver_progress) = next_pair_progress(sender, receiver).await;
        if let Some(progress) = receiver_progress {
            match progress {
                EstablishedConnectionProgress::InboundOpen(request) => {
                    assert_eq!(request.mode(), mode);
                    assert_eq!(request.max_message_bytes(), MAX_MESSAGE_BYTES as u64);
                    accepted_flow_id = Some(request.flow_id());
                    receiver
                        .driver
                        .accept_inbound(
                            &mut receiver.host.delivery,
                            request,
                            InboundAdmission {
                                key: inbound,
                                policy: policy(mode),
                                connection_limits: connection_limits(),
                            },
                        )
                        .unwrap();
                }
                EstablishedConnectionProgress::ControlSendCompleted(
                    crate::flow_driver::FlowControlSendEffect::InboundAccepted(flow),
                ) => {
                    assert_eq!(flow.key(), inbound);
                    assert_eq!(flow.mode(), mode);
                    assert_eq!(Some(flow.flow_id()), accepted_flow_id);
                    accept_send_completed = true;
                }
                other => assert_non_failure_progress(&other),
            }
        }
        if let Some(progress) = sender_progress {
            match progress {
                EstablishedConnectionProgress::OutboundEstablished(flow) => {
                    assert_eq!(flow.key(), outbound);
                    assert_eq!(flow.mode(), mode);
                    assert_eq!(Some(flow.flow_id()), accepted_flow_id);
                    established_flow_id = Some(flow.flow_id());
                }
                other => assert_non_failure_progress(&other),
            }
        }
        if accept_send_completed && let Some(flow_id) = established_flow_id {
            return LiveFlow {
                flow_id,
                outbound,
                inbound,
            };
        }
    }
}

async fn send_reliable_and_expect(
    sender: &mut LiveSide,
    receiver: &mut LiveSide,
    flow: LiveFlow,
    payload: Vec<u8>,
) {
    assert!(matches!(
        sender.host.delivery.submit(flow.outbound, payload.clone()),
        Ok(SubmissionOutcome::Accepted {
            accepted_index: 0,
            local_pressure_drops: 0,
        })
    ));
    drive_until_exposed(sender, receiver, flow.inbound, &payload).await;
}

async fn send_unreliable_and_expect(
    sender: &mut LiveSide,
    receiver: &mut LiveSide,
    flow: LiveFlow,
    payload: Vec<u8>,
) {
    assert!(matches!(
        sender
            .driver
            .submit_unreliable(&mut sender.host.delivery, flow.flow_id, payload.clone(),),
        Ok(DatagramSubmitOutcome::Submitted(
            DatagramSubmissionOutcome::Accepted {
                accepted_index: 0,
                local_pressure_drops: 0,
            }
        ))
    ));
    drive_until_exposed(sender, receiver, flow.inbound, &payload).await;
}

async fn drive_until_exposed(
    sender: &mut LiveSide,
    receiver: &mut LiveSide,
    inbound: DeliveryFlowKey,
    expected: &[u8],
) {
    loop {
        if let Some(message) = receiver.host.delivery.poll_exposure(inbound).unwrap() {
            assert_eq!(message.accepted_index(), 0);
            assert_eq!(message.payload(), expected);
            return;
        }
        let (sender_progress, receiver_progress) = next_pair_progress(sender, receiver).await;
        if let Some(progress) = sender_progress.as_ref() {
            assert_non_failure_progress(progress);
        }
        if let Some(progress) = receiver_progress.as_ref() {
            assert_non_failure_progress(progress);
        }
    }
}

async fn close_reliable_normally(sender: &mut LiveSide, receiver: &mut LiveSide, flow: LiveFlow) {
    assert!(matches!(
        sender
            .driver
            .request_outbound_finish_normal(&mut sender.host.delivery, flow.flow_id,),
        Ok(OutboundFinishOutcome::Started)
    ));

    let mut sender_closed = false;
    let mut receiver_closed = false;
    while !(sender_closed && receiver_closed) {
        let (sender_progress, receiver_progress) = next_pair_progress(sender, receiver).await;
        if let Some(progress) = sender_progress {
            match progress {
                EstablishedConnectionProgress::Reliable(
                    crate::reliable_driver::ActiveReliableProgress::Outbound {
                        flow_id,
                        progress: SendProgress::Closed,
                    },
                ) => {
                    assert_eq!(flow_id, flow.flow_id);
                    sender_closed = true;
                }
                EstablishedConnectionProgress::RemoteTerminated { .. }
                | EstablishedConnectionProgress::FlowFailureHandled { .. } => {
                    panic!("normal reliable FIN became exceptional flow termination: {progress:?}")
                }
                other => assert_non_failure_progress(&other),
            }
        }
        if let Some(progress) = receiver_progress {
            match progress {
                EstablishedConnectionProgress::Reliable(
                    crate::reliable_driver::ActiveReliableProgress::Inbound(
                        ReceiveProgress::Closed,
                    ),
                ) => receiver_closed = true,
                EstablishedConnectionProgress::RemoteTerminated { .. }
                | EstablishedConnectionProgress::FlowFailureHandled { .. } => {
                    panic!("normal reliable FIN became exceptional flow termination: {progress:?}")
                }
                other => assert_non_failure_progress(&other),
            }
        }
    }
}

async fn next_pair_progress(
    first: &mut LiveSide,
    second: &mut LiveSide,
) -> (
    Option<EstablishedConnectionProgress>,
    Option<EstablishedConnectionProgress>,
) {
    poll_fn(|cx| {
        let first_progress = match first.driver.poll_step(cx, &mut first.host.delivery) {
            Poll::Pending => None,
            Poll::Ready(Ok(progress)) => Some(progress),
            Poll::Ready(Err(error)) => panic!("first live driver failed: {error:?}"),
        };
        let second_progress = match second.driver.poll_step(cx, &mut second.host.delivery) {
            Poll::Pending => None,
            Poll::Ready(Ok(progress)) => Some(progress),
            Poll::Ready(Err(error)) => panic!("second live driver failed: {error:?}"),
        };
        if first_progress.is_some() || second_progress.is_some() {
            Poll::Ready((first_progress, second_progress))
        } else {
            Poll::Pending
        }
    })
    .await
}

fn assert_non_failure_progress(progress: &EstablishedConnectionProgress) {
    match progress {
        EstablishedConnectionProgress::OutboundRejected { .. }
        | EstablishedConnectionProgress::RemoteTerminated { .. }
        | EstablishedConnectionProgress::FlowFailureHandled { .. } => {
            panic!("successful live scenario observed failure progress: {progress:?}")
        }
        _ => {}
    }
}

async fn join2<A, B>(first: A, second: B) -> (A::Output, B::Output)
where
    A: Future,
    B: Future,
{
    let mut first: Pin<Box<A>> = Box::pin(first);
    let mut second: Pin<Box<B>> = Box::pin(second);
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
