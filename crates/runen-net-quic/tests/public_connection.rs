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
use runen_net_quic::{
    CertificateDer, ClientEndpoint, ClientTrust, Connection, ConnectionError, ConnectionEvent,
    ConnectionStateError, EndpointConfig, EndpointResourceLimits, FlowCommandError,
    FlowRejectionReason, FlowTerminationCause, FlowTerminationOrigin, InboundFlowConfig,
    NegotiationFailure, NegotiationReportStatus, OutboundFlowConfig, PrivateKeyDer, ProfileConfig,
    ProfileLimits, ProfileReadyConnection, ReliableReceiveLimits, SemanticRole, ServerEndpoint,
    ServerIdentity, SubmitOutcome,
};
use rustls_pki_types::PrivatePkcs8KeyDer;
use tokio::runtime::Builder;

const SCENARIO_TIMEOUT: Duration = Duration::from_secs(20);
const CLIENT_CONNECTION: ConnectionHandle = ConnectionHandle::new(41);
const SERVER_CONNECTION: ConnectionHandle = ConnectionHandle::new(99);
const MAX_MESSAGE_BYTES: usize = 512;

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
fn public_reliable_flow_uses_core_keys_for_open_data_and_normal_finish() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, run_public_reliable_flow())
            .await
            .expect("public RN6D reliable-flow scenario timed out");
    });
}

#[test]
fn public_unreliable_modes_use_core_keys_for_data_and_normal_finish() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, run_public_unreliable_flows())
            .await
            .expect("public RN6D unreliable-flow scenarios timed out");
    });
}

#[test]
fn public_incoming_capability_is_connection_scoped_and_rejections_round_trip() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, run_public_admission_contracts())
            .await
            .expect("public RN6D admission-contract scenario timed out");
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

async fn run_public_reliable_flow() {
    let config = resource_limits(2).validate().unwrap();
    let (client, server) = endpoints(config);
    let mut client_host = new_host();
    let mut server_host = new_host();
    let (mut client_connection, mut server_connection) = establish_public_connection_pair(
        &client,
        &server,
        config,
        &mut client_host,
        &mut server_host,
    )
    .await;

    let (outbound, inbound) = open_and_accept_public_flow(
        &mut client_connection,
        &mut client_host,
        &mut server_connection,
        &mut server_host,
        DeliveryMode::ReliableOrdered,
        1,
        101,
    )
    .await;
    submit_and_expect_public_payload(
        &mut client_connection,
        &mut client_host,
        &mut server_connection,
        &mut server_host,
        outbound,
        inbound,
        b"public-reliable",
    )
    .await;
    finish_and_expect_normal_termination(
        &mut client_connection,
        &mut client_host,
        &mut server_connection,
        &mut server_host,
        outbound,
        inbound,
    )
    .await;

    assert!(client_host.delivery.flow_contract(outbound).is_none());
    assert!(server_host.delivery.flow_contract(inbound).is_none());
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

async fn run_public_unreliable_flows() {
    let config = resource_limits(2).validate().unwrap();
    let (client, server) = endpoints(config);
    let mut client_host = new_host();
    let mut server_host = new_host();
    let (mut client_connection, mut server_connection) = establish_public_connection_pair(
        &client,
        &server,
        config,
        &mut client_host,
        &mut server_host,
    )
    .await;

    for (mode, outbound_handle, inbound_handle, payload) in [
        (
            DeliveryMode::UnreliableUnordered,
            10,
            110,
            b"public-unordered".as_slice(),
        ),
        (
            DeliveryMode::UnreliableSequenced,
            11,
            111,
            b"public-sequenced".as_slice(),
        ),
    ] {
        let (outbound, inbound) = open_and_accept_public_flow(
            &mut client_connection,
            &mut client_host,
            &mut server_connection,
            &mut server_host,
            mode,
            outbound_handle,
            inbound_handle,
        )
        .await;
        submit_and_expect_public_payload(
            &mut client_connection,
            &mut client_host,
            &mut server_connection,
            &mut server_host,
            outbound,
            inbound,
            payload,
        )
        .await;
        finish_and_expect_normal_termination(
            &mut client_connection,
            &mut client_host,
            &mut server_connection,
            &mut server_host,
            outbound,
            inbound,
        )
        .await;
        assert!(client_host.delivery.flow_contract(outbound).is_none());
        assert!(server_host.delivery.flow_contract(inbound).is_none());
    }

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

async fn run_public_admission_contracts() {
    let config = resource_limits(2).validate().unwrap();
    let (client, server) = endpoints(config);
    let mut client_host = new_host();
    let mut server_host = new_host();
    let (mut client_connection, mut server_connection) = establish_public_connection_pair(
        &client,
        &server,
        config,
        &mut client_host,
        &mut server_host,
    )
    .await;

    let outbound = DeliveryFlowKey::new(
        CLIENT_CONNECTION,
        FlowDirection::Outbound,
        DeliveryFlowHandle::new(20),
    );
    let inbound = DeliveryFlowKey::new(
        SERVER_CONNECTION,
        FlowDirection::Inbound,
        DeliveryFlowHandle::new(120),
    );
    client_connection
        .open_outbound_flow(
            &client_host.delivery,
            OutboundFlowConfig {
                key: outbound,
                mode: DeliveryMode::ReliableOrdered,
                policy: flow_policy(DeliveryMode::ReliableOrdered),
                connection_limits: flow_connection_limits(),
                stable_max_message_bytes: nz(MAX_MESSAGE_BYTES),
            },
        )
        .unwrap();
    let request = loop {
        let (client_event, server_event) = next_public_pair_event(
            &mut client_connection,
            &mut client_host,
            &mut server_connection,
            &mut server_host,
        )
        .await;
        assert!(client_event.is_none());
        match server_event {
            Some(ConnectionEvent::IncomingFlowRequested { request }) => break request,
            Some(event) => panic!("unexpected admission request event: {event:?}"),
            None => {}
        }
    };
    assert_eq!(request.connection(), SERVER_CONNECTION);
    let wrong_connection = client_connection
        .accept_incoming_flow(
            &mut client_host.delivery,
            request,
            InboundFlowConfig {
                key: inbound,
                policy: flow_policy(DeliveryMode::ReliableOrdered),
                connection_limits: flow_connection_limits(),
            },
        )
        .expect_err("incoming capability was accepted by the wrong public connection");
    assert_eq!(wrong_connection.reason(), FlowCommandError::WrongConnection);
    let request = wrong_connection
        .into_request()
        .expect("wrong-connection admission consumed a retryable incoming capability");
    assert_eq!(request.connection(), SERVER_CONNECTION);
    server_connection
        .accept_incoming_flow(
            &mut server_host.delivery,
            request,
            InboundFlowConfig {
                key: inbound,
                policy: flow_policy(DeliveryMode::ReliableOrdered),
                connection_limits: flow_connection_limits(),
            },
        )
        .unwrap();
    loop {
        let (client_event, server_event) = next_public_pair_event(
            &mut client_connection,
            &mut client_host,
            &mut server_connection,
            &mut server_host,
        )
        .await;
        assert!(server_event.is_none());
        match client_event {
            Some(ConnectionEvent::OutboundFlowEstablished { key }) => {
                assert_eq!(key, outbound);
                break;
            }
            Some(event) => panic!("unexpected post-retry establishment event: {event:?}"),
            None => {}
        }
    }
    finish_and_expect_normal_termination(
        &mut client_connection,
        &mut client_host,
        &mut server_connection,
        &mut server_host,
        outbound,
        inbound,
    )
    .await;

    for (handle, reason) in [
        (21, FlowRejectionReason::ResourceLimit),
        (22, FlowRejectionReason::MessageLimit),
    ] {
        let outbound = DeliveryFlowKey::new(
            CLIENT_CONNECTION,
            FlowDirection::Outbound,
            DeliveryFlowHandle::new(handle),
        );
        client_connection
            .open_outbound_flow(
                &client_host.delivery,
                OutboundFlowConfig {
                    key: outbound,
                    mode: DeliveryMode::ReliableOrdered,
                    policy: flow_policy(DeliveryMode::ReliableOrdered),
                    connection_limits: flow_connection_limits(),
                    stable_max_message_bytes: nz(MAX_MESSAGE_BYTES),
                },
            )
            .unwrap();
        let request = loop {
            let (client_event, server_event) = next_public_pair_event(
                &mut client_connection,
                &mut client_host,
                &mut server_connection,
                &mut server_host,
            )
            .await;
            assert!(client_event.is_none());
            match server_event {
                Some(ConnectionEvent::IncomingFlowRequested { request }) => break request,
                Some(event) => panic!("unexpected rejection request event: {event:?}"),
                None => {}
            }
        };
        server_connection
            .reject_incoming_flow(request, reason)
            .unwrap();
        loop {
            let (client_event, server_event) = next_public_pair_event(
                &mut client_connection,
                &mut client_host,
                &mut server_connection,
                &mut server_host,
            )
            .await;
            assert!(server_event.is_none());
            match client_event {
                Some(ConnectionEvent::OutboundFlowRejected {
                    key,
                    reason: observed,
                }) => {
                    assert_eq!(key, outbound);
                    assert_eq!(observed, reason);
                    break;
                }
                Some(event) => panic!("unexpected rejection result event: {event:?}"),
                None => {}
            }
        }
        assert!(client_host.delivery.flow_contract(outbound).is_none());
    }

    assert_eq!(client_host.delivery.active_flows(), 0);
    assert_eq!(server_host.delivery.active_flows(), 0);
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

async fn establish_public_connection_pair(
    client: &ClientEndpoint,
    server: &ServerEndpoint,
    config: EndpointConfig,
    client_host: &mut HostState,
    server_host: &mut HostState,
) -> (Connection, Connection) {
    let (client_ready, server_ready) =
        profile_ready_pair(client, server, config, AuthoritySide::Client).await;
    let (mut client_connection, mut server_connection) =
        activate_pair(client_ready, server_ready, client_host, server_host);
    drive_until_authority_selection(
        &mut client_connection,
        client_host,
        &mut server_connection,
        server_host,
        AuthoritySide::Client,
    )
    .await;
    client_connection
        .select_authority(&mut client_host.negotiation, contract())
        .unwrap();
    drive_until_established(
        &mut client_connection,
        client_host,
        &mut server_connection,
        server_host,
    )
    .await;
    (client_connection, server_connection)
}

async fn open_and_accept_public_flow(
    client: &mut Connection,
    client_host: &mut HostState,
    server: &mut Connection,
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
                stable_max_message_bytes: nz(MAX_MESSAGE_BYTES),
            },
        )
        .unwrap();

    let request = loop {
        let (client_event, server_event) =
            next_public_pair_event(client, client_host, server, server_host).await;
        if let Some(event) = client_event {
            panic!("sender surfaced durable flow progress before admission: {event:?}");
        }
        match server_event {
            Some(ConnectionEvent::IncomingFlowRequested { request }) => break request,
            Some(event) => panic!("receiver surfaced unexpected pre-admission event: {event:?}"),
            None => {}
        }
    };
    assert_eq!(request.connection(), SERVER_CONNECTION);
    assert_eq!(request.mode(), mode);
    assert_eq!(request.max_message_bytes(), MAX_MESSAGE_BYTES as u64);
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
            next_public_pair_event(client, client_host, server, server_host).await;
        if let Some(event) = server_event {
            panic!("receiver surfaced unexpected establishment event: {event:?}");
        }
        match client_event {
            Some(ConnectionEvent::OutboundFlowEstablished { key }) => {
                assert_eq!(key, outbound);
                break;
            }
            Some(event) => panic!("sender surfaced unexpected establishment event: {event:?}"),
            None => {}
        }
    }
    (outbound, inbound)
}

async fn submit_and_expect_public_payload(
    client: &mut Connection,
    client_host: &mut HostState,
    server: &mut Connection,
    server_host: &mut HostState,
    outbound: DeliveryFlowKey,
    inbound: DeliveryFlowKey,
    payload: &[u8],
) {
    assert_eq!(
        client
            .submit(&mut client_host.delivery, outbound, payload.to_vec())
            .unwrap(),
        SubmitOutcome::Accepted {
            accepted_index: 0,
            local_pressure_drops: 0,
        }
    );

    loop {
        let (client_event, server_event) =
            next_public_pair_event(client, client_host, server, server_host).await;
        if let Some(event) = client_event {
            panic!("sender surfaced unexpected data event: {event:?}");
        }
        match server_event {
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
            Some(event) => panic!("receiver surfaced unexpected data event: {event:?}"),
            None => {}
        }
    }
    let exposed = server_host
        .delivery
        .poll_exposure(inbound)
        .unwrap()
        .expect("DataReady did not leave payload in Core custody");
    assert_eq!(exposed.accepted_index(), 0);
    assert_eq!(exposed.payload(), payload);
}

async fn finish_and_expect_normal_termination(
    client: &mut Connection,
    client_host: &mut HostState,
    server: &mut Connection,
    server_host: &mut HostState,
    outbound: DeliveryFlowKey,
    inbound: DeliveryFlowKey,
) {
    client
        .finish_outbound_flow_normal(&mut client_host.delivery, outbound)
        .unwrap();
    let mut client_closed = false;
    let mut server_closed = false;
    while !(client_closed && server_closed) {
        let (client_event, server_event) =
            next_public_pair_event(client, client_host, server, server_host).await;
        if let Some(event) = client_event {
            match event {
                ConnectionEvent::FlowTerminated {
                    key,
                    origin,
                    cause,
                    termination: Some(termination),
                } => {
                    assert_eq!(key, outbound);
                    assert_eq!(origin, FlowTerminationOrigin::Local);
                    assert_eq!(cause, FlowTerminationCause::Normal);
                    assert_eq!(termination.key, outbound);
                    assert_eq!(termination.reason, FlowTerminationReason::Requested);
                    client_closed = true;
                }
                event => panic!("sender surfaced unexpected normal-finish event: {event:?}"),
            }
        }
        if let Some(event) = server_event {
            match event {
                ConnectionEvent::FlowTerminated {
                    key,
                    origin,
                    cause,
                    termination: Some(termination),
                } => {
                    assert_eq!(key, inbound);
                    assert_eq!(origin, FlowTerminationOrigin::Remote);
                    assert_eq!(cause, FlowTerminationCause::Normal);
                    assert_eq!(termination.key, inbound);
                    assert_eq!(termination.reason, FlowTerminationReason::Requested);
                    server_closed = true;
                }
                event => panic!("receiver surfaced unexpected normal-finish event: {event:?}"),
            }
        }
    }
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

async fn next_public_pair_event(
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
                Poll::Ready(Err(error)) => {
                    panic!("public client established driver failed: {error:?}")
                }
            };
        let server_event =
            match server.poll(cx, &mut server_host.negotiation, &mut server_host.delivery) {
                Poll::Pending => None,
                Poll::Ready(Ok(event)) => Some(event),
                Poll::Ready(Err(error)) => {
                    panic!("public server established driver failed: {error:?}")
                }
            };
        if client_event.is_some() || server_event.is_some() {
            Poll::Ready((client_event, server_event))
        } else {
            Poll::Pending
        }
    })
    .await
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
