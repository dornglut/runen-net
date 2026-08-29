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
    identity::ConnectionHandle,
    protocol::{
        CompatibilityOffer, NegotiatedContract, NegotiationManager, NegotiationManagerLimits,
        NegotiationRequirements, OfferLimits, ProtocolContract, ProtocolId, ProtocolRevision,
    },
};
use runen_net_quic::{
    CertificateDer, ClientEndpoint, ClientTrust, Connection, ConnectionEvent, EndpointConfig,
    FlowTerminationCause, FlowTerminationOrigin, InboundFlowConfig, OutboundFlowConfig,
    PrivateKeyDer, ProfileConfig, ProfileReadyConnection, SemanticRole, ServerEndpoint,
    ServerIdentity, SubmitOutcome,
};
use rustls_pki_types::PrivatePkcs8KeyDer;
use tokio::runtime::Builder;

const CLIENT_CONNECTION: ConnectionHandle = ConnectionHandle::new(1);
const SERVER_CONNECTION: ConnectionHandle = ConnectionHandle::new(2);
const MAX_MESSAGE_BYTES: usize = 512;
const MAX_INCOMING_MESSAGE_BYTES: u64 = 128 * 1024;
const SCENARIO_TIMEOUT: Duration = Duration::from_secs(20);

struct HostState {
    negotiation: NegotiationManager,
    delivery: DeliveryEndpoint,
}

struct FlowExample {
    mode: DeliveryMode,
    outbound_handle: u64,
    inbound_handle: u64,
    payload: &'static [u8],
}

fn main() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, run())
            .await
            .expect("standalone RunenNet example timed out");
    });
}

async fn run() {
    let endpoint_config = EndpointConfig::baseline(2, 16).unwrap();
    let (client_endpoint, server_endpoint) = endpoints(endpoint_config);
    let mut client_host = host_state();
    let mut server_host = host_state();

    // QUIC client/server side and semantic Authority are independent. This example deliberately
    // makes the QUIC server the RunenNet Authority.
    let (client_ready, server_ready) =
        profile_ready_pair(&client_endpoint, &server_endpoint, endpoint_config).await;
    let (mut client, mut server) = activate_pair(
        client_ready,
        server_ready,
        &mut client_host,
        &mut server_host,
    );

    drive_until_authority_selection(&mut client, &mut client_host, &mut server, &mut server_host)
        .await;
    server
        .select_authority(&mut server_host.negotiation, negotiated_contract())
        .unwrap();
    drive_until_established(&mut client, &mut client_host, &mut server, &mut server_host).await;

    run_flow(
        &mut client,
        &mut client_host,
        &mut server,
        &mut server_host,
        FlowExample {
            mode: DeliveryMode::ReliableOrdered,
            outbound_handle: 1,
            inbound_handle: 101,
            payload: b"reliable hello",
        },
    )
    .await;
    run_flow(
        &mut client,
        &mut client_host,
        &mut server,
        &mut server_host,
        FlowExample {
            mode: DeliveryMode::UnreliableUnordered,
            outbound_handle: 2,
            inbound_handle: 102,
            payload: b"unreliable hello",
        },
    )
    .await;

    let client_teardown = client.teardown(&mut client_host.negotiation, &mut client_host.delivery);
    let server_teardown = server.teardown(&mut server_host.negotiation, &mut server_host.delivery);
    assert!(client_teardown.cleanup_error().is_none());
    assert!(server_teardown.cleanup_error().is_none());
    assert!(client_teardown.flow_terminations().is_empty());
    assert!(server_teardown.flow_terminations().is_empty());

    client_endpoint.close();
    server_endpoint.close();
    join2(client_endpoint.wait_idle(), server_endpoint.wait_idle()).await;

    println!("standalone RunenNet QUIC example completed");
}

async fn run_flow(
    client: &mut Connection,
    client_host: &mut HostState,
    server: &mut Connection,
    server_host: &mut HostState,
    flow: FlowExample,
) {
    let FlowExample {
        mode,
        outbound_handle,
        inbound_handle,
        payload,
    } = flow;
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

    let request = loop {
        let (client_event, server_event) =
            next_pair_event(client, client_host, server, server_host).await;
        if let Some(event) = client_event {
            panic!("unexpected sender event before flow admission: {event:?}");
        }
        match server_event {
            Some(ConnectionEvent::IncomingFlowRequested { request }) => break request,
            Some(event) => panic!("unexpected receiver event before flow admission: {event:?}"),
            None => {}
        }
    };
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

    loop {
        let (client_event, server_event) =
            next_pair_event(client, client_host, server, server_host).await;
        if let Some(event) = server_event {
            panic!("unexpected receiver event while establishing flow: {event:?}");
        }
        match client_event {
            Some(ConnectionEvent::OutboundFlowEstablished { key }) if key == outbound => break,
            Some(event) => panic!("unexpected sender flow-establishment event: {event:?}"),
            None => {}
        }
    }

    let submission = client
        .submit(&mut client_host.delivery, outbound, payload.to_vec())
        .unwrap();
    assert!(matches!(submission, SubmitOutcome::Accepted { .. }));

    loop {
        let (client_event, server_event) =
            next_pair_event(client, client_host, server, server_host).await;
        if let Some(event) = client_event {
            panic!("unexpected sender event while receiving payload: {event:?}");
        }
        match server_event {
            Some(ConnectionEvent::DataReady { key, .. }) if key == inbound => break,
            Some(event) => panic!("unexpected receiver data event: {event:?}"),
            None => {}
        }
    }

    // The QUIC adapter signals readiness, but payload custody remains exclusively in Core.
    let exposed = server_host
        .delivery
        .poll_exposure(inbound)
        .unwrap()
        .expect("DataReady did not leave a payload in DeliveryEndpoint");
    assert_eq!(exposed.payload(), payload);
    println!("{mode:?}: {}", String::from_utf8_lossy(exposed.payload()));

    client
        .finish_outbound_flow_normal(&mut client_host.delivery, outbound)
        .unwrap();
    wait_for_normal_finish(client, client_host, server, server_host, outbound, inbound).await;
}

async fn wait_for_normal_finish(
    client: &mut Connection,
    client_host: &mut HostState,
    server: &mut Connection,
    server_host: &mut HostState,
    outbound: DeliveryFlowKey,
    inbound: DeliveryFlowKey,
) {
    let mut local_finished = false;
    let mut remote_finished = false;
    while !(local_finished && remote_finished) {
        let (client_event, server_event) =
            next_pair_event(client, client_host, server, server_host).await;
        if let Some(event) = client_event {
            match event {
                ConnectionEvent::FlowTerminated {
                    key,
                    origin: FlowTerminationOrigin::Local,
                    cause: FlowTerminationCause::Normal,
                    ..
                } if key == outbound => local_finished = true,
                event => panic!("unexpected sender finish event: {event:?}"),
            }
        }
        if let Some(event) = server_event {
            match event {
                ConnectionEvent::FlowTerminated {
                    key,
                    origin: FlowTerminationOrigin::Remote,
                    cause: FlowTerminationCause::Normal,
                    ..
                } if key == inbound => remote_finished = true,
                event => panic!("unexpected receiver finish event: {event:?}"),
            }
        }
    }
}

async fn profile_ready_pair(
    client: &ClientEndpoint,
    server: &ServerEndpoint,
    endpoint_config: EndpointConfig,
) -> (ProfileReadyConnection, ProfileReadyConnection) {
    let server_address = server.local_addr().unwrap();
    let (client_ready, server_ready) = join2(
        client.connect(
            server_address,
            "localhost",
            profile(endpoint_config, SemanticRole::NonAuthority),
        ),
        server.accept(profile(endpoint_config, SemanticRole::Authority)),
    )
    .await;
    (
        client_ready.expect("client failed to reach ProfileReady"),
        server_ready
            .expect("server failed to accept ProfileReady")
            .expect("server endpoint closed before ProfileReady"),
    )
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
            compatibility_offer(),
            NegotiationRequirements::default(),
            &mut client_host.negotiation,
        )
        .unwrap();
    let server = server_ready
        .activate(
            SERVER_CONNECTION,
            compatibility_offer(),
            NegotiationRequirements::default(),
            &mut server_host.negotiation,
        )
        .unwrap();
    (client, server)
}

async fn drive_until_authority_selection(
    client: &mut Connection,
    client_host: &mut HostState,
    server: &mut Connection,
    server_host: &mut HostState,
) {
    poll_fn(|cx| {
        match client.poll(cx, &mut client_host.negotiation, &mut client_host.delivery) {
            Poll::Pending => {}
            Poll::Ready(Ok(event)) => {
                panic!("client produced an event before server Authority selection: {event:?}")
            }
            Poll::Ready(Err(error)) => panic!("client negotiation failed: {error:?}"),
        }

        match server.poll(cx, &mut server_host.negotiation, &mut server_host.delivery) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(ConnectionEvent::AuthoritySelectionRequired { connection })) => {
                assert_eq!(connection, SERVER_CONNECTION);
                Poll::Ready(())
            }
            Poll::Ready(Ok(event)) => {
                panic!("unexpected server event before Authority selection: {event:?}")
            }
            Poll::Ready(Err(error)) => panic!("server negotiation failed: {error:?}"),
        }
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
                Poll::Ready(Ok(event)) => panic!("unexpected client event: {event:?}"),
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
                Poll::Ready(Ok(event)) => panic!("unexpected server event: {event:?}"),
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

async fn next_pair_event(
    client: &mut Connection,
    client_host: &mut HostState,
    server: &mut Connection,
    server_host: &mut HostState,
) -> (Option<ConnectionEvent>, Option<ConnectionEvent>) {
    poll_fn(|cx| {
        let client_event =
            match client.poll(cx, &mut client_host.negotiation, &mut client_host.delivery) {
                Poll::Pending => None,
                Poll::Ready(Ok(event)) => Some(event),
                Poll::Ready(Err(error)) => panic!("client connection failed: {error:?}"),
            };
        let server_event =
            match server.poll(cx, &mut server_host.negotiation, &mut server_host.delivery) {
                Poll::Pending => None,
                Poll::Ready(Ok(event)) => Some(event),
                Poll::Ready(Err(error)) => panic!("server connection failed: {error:?}"),
            };
        if client_event.is_some() || server_event.is_some() {
            Poll::Ready((client_event, server_event))
        } else {
            Poll::Pending
        }
    })
    .await
}

fn endpoints(endpoint_config: EndpointConfig) -> (ClientEndpoint, ServerEndpoint) {
    // Demo-only TLS: production applications should provide their own certificate lifecycle and
    // trust policy instead of generating a self-signed identity at startup.
    let (certificate, private_key) = demo_identity();
    let client = ClientEndpoint::bind(
        loopback_ephemeral(),
        endpoint_config,
        ClientTrust::new(vec![certificate.clone()]).unwrap(),
    )
    .unwrap();
    let server = ServerEndpoint::bind(
        loopback_ephemeral(),
        endpoint_config,
        ServerIdentity::new(vec![certificate], private_key).unwrap(),
    )
    .unwrap();
    (client, server)
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

fn compatibility_offer() -> CompatibilityOffer {
    CompatibilityOffer::builder()
        .protocol(ProtocolId::new(1), ProtocolRevision::new(1))
        .build()
}

fn negotiated_contract() -> NegotiatedContract {
    NegotiatedContract::new(protocol_contract())
}

const fn protocol_contract() -> ProtocolContract {
    ProtocolContract::new(ProtocolId::new(1), ProtocolRevision::new(1))
}

fn profile(endpoint_config: EndpointConfig, semantic_role: SemanticRole) -> ProfileConfig {
    ProfileConfig::baseline(
        endpoint_config,
        semantic_role,
        MAX_INCOMING_MESSAGE_BYTES,
    )
    .unwrap()
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

fn demo_identity() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
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
