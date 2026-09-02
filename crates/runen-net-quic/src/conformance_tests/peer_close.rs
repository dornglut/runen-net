use std::task::{Context, Waker};

use crate::{
    public_connection::{ConnectionError, ConnectionStateError},
    public_flow::{InboundFlowConfig, OutboundFlowConfig, SubmitOutcome as PublicSubmitOutcome},
};

use super::*;

struct PublicSide {
    connection: PublicConnection,
    host: HostState,
}

#[test]
fn consuming_teardown_repeatedly_surfaces_one_peer_close_then_terminal() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            for iteration in 0..8 {
                let (client, server, mut client_side, mut server_side) =
                    establish_public_pair_with_resources(resources()).await;

                let client_teardown = client_side.connection.teardown(
                    &mut client_side.host.negotiation,
                    &mut client_side.host.delivery,
                );
                assert_clean_public_teardown(&client_teardown, FIRST_CONNECTION);

                let event = next_public_result(&mut server_side)
                    .await
                    .expect("ordinary consuming teardown surfaced a public failure");
                assert!(matches!(
                    event,
                    ConnectionEvent::PeerClosed { connection }
                        if connection == FIRST_CONNECTION
                ));

                let error = next_public_result(&mut server_side)
                    .await
                    .expect_err("peer close was surfaced more than once");
                assert_eq!(
                    error.kind(),
                    PublicConnectionErrorKind::State(ConnectionStateError::Terminal),
                    "iteration {iteration} did not become terminal after the one peer-close event"
                );

                let server_teardown = server_side.connection.teardown(
                    &mut server_side.host.negotiation,
                    &mut server_side.host.delivery,
                );
                assert_clean_public_teardown(&server_teardown, FIRST_CONNECTION);
                close_test_endpoints(&client, &server, b"repeated teardown proof").await;
            }
        })
        .await
        .expect("repeated established teardown/peer-close proof timed out");
    });
}

#[test]
fn peer_no_error_preserves_connection_ended_flow_evidence_without_normal_flow_event() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            let (client, server, mut client_side, mut server_side) =
                establish_public_pair_with_resources(resources()).await;
            let (outbound, inbound) =
                establish_public_reliable_flow(&mut client_side, &mut server_side, 501, 601).await;

            let server_teardown = server_side.connection.teardown(
                &mut server_side.host.negotiation,
                &mut server_side.host.delivery,
            );
            let server_termination = only_termination(&server_teardown);
            assert_eq!(server_termination.key, inbound);
            assert_eq!(
                server_termination.reason,
                FlowTerminationReason::ConnectionEnded
            );

            let event = next_public_result(&mut client_side)
                .await
                .expect("peer NO_ERROR unexpectedly became a public failure");
            assert!(matches!(
                event,
                ConnectionEvent::PeerClosed { connection }
                    if connection == FIRST_CONNECTION
            ));

            let terminal = next_public_result(&mut client_side)
                .await
                .expect_err("peer close emitted a second durable event");
            assert_eq!(
                terminal.kind(),
                PublicConnectionErrorKind::State(ConnectionStateError::Terminal)
            );

            let client_teardown = client_side.connection.teardown(
                &mut client_side.host.negotiation,
                &mut client_side.host.delivery,
            );
            let client_termination = only_termination(&client_teardown);
            assert_eq!(client_termination.key, outbound);
            assert_eq!(
                client_termination.reason,
                FlowTerminationReason::ConnectionEnded
            );
            assert!(!client_termination.reliable_obligation_failed);

            close_test_endpoints(&client, &server, b"residual flow proof").await;
        })
        .await
        .expect("peer-close residual-flow proof timed out");
    });
}

#[test]
fn accepted_reliable_core_custody_remains_failed_obligation_at_connection_end() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            let (client, server, mut client_side, mut server_side) =
                establish_public_pair_with_resources(resources()).await;
            let (outbound, _inbound) =
                establish_public_reliable_flow(&mut client_side, &mut server_side, 502, 602).await;
            let payload = b"accepted-but-still-in-core-custody".to_vec();

            assert_eq!(
                client_side
                    .connection
                    .submit(&mut client_side.host.delivery, outbound, payload.clone())
                    .unwrap(),
                PublicSubmitOutcome::Accepted {
                    accepted_index: 0,
                    local_pressure_drops: 0,
                }
            );
            assert_eq!(
                client_side.host.delivery.flow_pending_usage(outbound),
                Some((1, payload.len()))
            );

            let server_teardown = server_side.connection.teardown(
                &mut server_side.host.negotiation,
                &mut server_side.host.delivery,
            );
            assert_eq!(
                only_termination(&server_teardown).reason,
                FlowTerminationReason::ConnectionEnded
            );

            // Do not poll the sender after Core acceptance: this case deliberately proves the
            // still-obligated custody branch rather than allowing transport to commit it first.
            let client_teardown = client_side.connection.teardown(
                &mut client_side.host.negotiation,
                &mut client_side.host.delivery,
            );
            let termination = only_termination(&client_teardown);
            assert_eq!(termination.key, outbound);
            assert_eq!(termination.reason, FlowTerminationReason::ConnectionEnded);
            assert_eq!(termination.pending_messages, 1);
            assert!(termination.reliable_obligation_failed);

            close_test_endpoints(&client, &server, b"reliable obligation proof").await;
        })
        .await
        .expect("reliable connection-end obligation proof timed out");
    });
}

#[test]
fn nonzero_runennet_application_close_stays_on_failure_path() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            let (client, server, mut client_side, server_side) =
                establish_public_pair_with_resources(resources()).await;
            let PublicSide {
                connection: server_connection,
                mut host: server_host,
            } = server_side;
            let (server_driver, _receive_limits) = server_connection
                .into_established_internal()
                .expect("server was not established for non-zero close injection");
            server_driver.close_for_test(ApplicationErrorCode::FlowProtocolError);

            let error = next_public_result(&mut client_side)
                .await
                .expect_err("non-zero RunenNet application close became normal peer close");
            assert_existing_connection_failure(error.kind());

            let client_teardown = client_side.connection.teardown(
                &mut client_side.host.negotiation,
                &mut client_side.host.delivery,
            );
            assert_clean_public_teardown(&client_teardown, FIRST_CONNECTION);
            let server_teardown = server_driver.teardown(
                &mut server_host.negotiation,
                &mut server_host.delivery,
            );
            assert_eq!(server_teardown.connection, FIRST_CONNECTION);
            assert!(server_teardown.negotiation_cleanup_error.is_none());
            assert!(server_teardown.flow_terminations.is_empty());

            close_test_endpoints(&client, &server, b"non-zero close proof").await;
        })
        .await
        .expect("non-zero application-close proof timed out");
    });
}

#[test]
fn quic_idle_timeout_stays_on_transport_failure_path() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            let resources = resources_with_idle_timeout(Duration::from_millis(200));
            let (client, server, mut client_side, mut server_side) =
                establish_public_pair_with_resources(resources).await;

            // No application close, endpoint close, sleep, or synthetic driver error: the
            // negotiated QUIC idle timer is the transport-loss mechanism under proof.
            let error = next_public_result(&mut client_side)
                .await
                .expect_err("genuine QUIC idle timeout became normal peer close");
            assert_existing_connection_failure(error.kind());

            let client_teardown = client_side.connection.teardown(
                &mut client_side.host.negotiation,
                &mut client_side.host.delivery,
            );
            let server_teardown = server_side.connection.teardown(
                &mut server_side.host.negotiation,
                &mut server_side.host.delivery,
            );
            assert_clean_public_teardown(&client_teardown, FIRST_CONNECTION);
            assert_clean_public_teardown(&server_teardown, FIRST_CONNECTION);
            close_test_endpoints(&client, &server, b"idle-timeout proof").await;
        })
        .await
        .expect("genuine transport-loss proof timed out");
    });
}

async fn establish_public_pair_with_resources(
    resources: ValidatedEndpointResources,
) -> (ConfiguredEndpoint, ConfiguredEndpoint, PublicSide, PublicSide) {
    let (client, server) = configured_endpoints_with_resources(resources);
    let server_address = server.endpoint().local_addr().unwrap();
    let (client_ready, server_ready) = join2(
        connect_profile_ready(
            &client,
            server_address,
            "localhost",
            profile(resources, SemanticRole::Authority),
        ),
        accept_profile_ready(&server, profile(resources, SemanticRole::NonAuthority)),
    )
    .await;
    let client_ready = client_ready.expect("public proof client failed ProfileReady");
    let server_ready = server_ready
        .expect("public proof server failed ProfileReady")
        .expect("public proof server endpoint closed before ProfileReady");

    let mut client_host = new_host();
    let mut server_host = new_host();
    let client_connection =
        activate_public_connection(client_ready, FIRST_CONNECTION, &mut client_host);
    let server_connection =
        activate_public_connection(server_ready, FIRST_CONNECTION, &mut server_host);
    let mut client_side = PublicSide {
        connection: client_connection,
        host: client_host,
    };
    let mut server_side = PublicSide {
        connection: server_connection,
        host: server_host,
    };
    drive_public_pair_to_established(&mut client_side, &mut server_side).await;
    (client, server, client_side, server_side)
}

async fn drive_public_pair_to_established(client: &mut PublicSide, server: &mut PublicSide) {
    let mut authority_selected = false;
    let mut client_established = false;
    let mut server_established = false;
    while !(client_established && server_established) {
        let (client_result, server_result) = next_public_pair_result(client, server).await;
        if let Some(result) = client_result {
            match result.expect("client public negotiation failed") {
                ConnectionEvent::AuthoritySelectionRequired { connection } => {
                    assert_eq!(connection, FIRST_CONNECTION);
                    assert!(!authority_selected, "Authority selection repeated");
                    client
                        .connection
                        .select_authority(&mut client.host.negotiation, contract())
                        .expect("valid public Authority selection failed");
                    authority_selected = true;
                }
                ConnectionEvent::Established { connection } => {
                    assert_eq!(connection, FIRST_CONNECTION);
                    client_established = true;
                }
                event => panic!("unexpected client pre-established event: {event:?}"),
            }
        }
        if let Some(result) = server_result {
            match result.expect("server public negotiation failed") {
                ConnectionEvent::Established { connection } => {
                    assert_eq!(connection, FIRST_CONNECTION);
                    server_established = true;
                }
                event => panic!("unexpected server pre-established event: {event:?}"),
            }
        }
    }
    assert!(authority_selected, "Authority was never explicitly selected");
}

async fn establish_public_reliable_flow(
    client: &mut PublicSide,
    server: &mut PublicSide,
    outbound_handle: u64,
    inbound_handle: u64,
) -> (DeliveryFlowKey, DeliveryFlowKey) {
    let outbound = DeliveryFlowKey::new(
        FIRST_CONNECTION,
        FlowDirection::Outbound,
        DeliveryFlowHandle::new(outbound_handle),
    );
    let inbound = DeliveryFlowKey::new(
        FIRST_CONNECTION,
        FlowDirection::Inbound,
        DeliveryFlowHandle::new(inbound_handle),
    );
    client
        .connection
        .open_outbound_flow(
            &client.host.delivery,
            OutboundFlowConfig {
                key: outbound,
                mode: DeliveryMode::ReliableOrdered,
                policy: policy(DeliveryMode::ReliableOrdered),
                connection_limits: connection_limits(),
            },
        )
        .expect("public reliable open failed");

    let request = loop {
        let (client_result, server_result) = next_public_pair_result(client, server).await;
        if let Some(result) = client_result {
            let event = result.expect("sender failed before incoming admission");
            panic!("sender surfaced progress before incoming admission: {event:?}");
        }
        if let Some(result) = server_result {
            match result.expect("receiver failed before incoming admission") {
                ConnectionEvent::IncomingFlowRequested { request } => break request,
                event => panic!("unexpected receiver pre-admission event: {event:?}"),
            }
        }
    };
    server
        .connection
        .accept_incoming_flow(
            &mut server.host.delivery,
            request,
            InboundFlowConfig {
                key: inbound,
                policy: policy(DeliveryMode::ReliableOrdered),
                connection_limits: connection_limits(),
            },
        )
        .expect("public reliable admission failed");

    loop {
        let (client_result, server_result) = next_public_pair_result(client, server).await;
        if let Some(result) = server_result {
            let event = result.expect("receiver failed during flow establishment");
            panic!("receiver surfaced unexpected establishment event: {event:?}");
        }
        if let Some(result) = client_result {
            match result.expect("sender failed during flow establishment") {
                ConnectionEvent::OutboundFlowEstablished { key } => {
                    assert_eq!(key, outbound);
                    break;
                }
                event => panic!("unexpected sender establishment event: {event:?}"),
            }
        }
    }
    (outbound, inbound)
}

async fn next_public_result(side: &mut PublicSide) -> Result<ConnectionEvent, ConnectionError> {
    poll_fn(|cx| {
        side.connection
            .poll(cx, &mut side.host.negotiation, &mut side.host.delivery)
    })
    .await
}

async fn next_public_pair_result(
    client: &mut PublicSide,
    server: &mut PublicSide,
) -> (
    Option<Result<ConnectionEvent, ConnectionError>>,
    Option<Result<ConnectionEvent, ConnectionError>>,
) {
    poll_fn(|cx| {
        let client_result = match client.connection.poll(
            cx,
            &mut client.host.negotiation,
            &mut client.host.delivery,
        ) {
            Poll::Pending => None,
            Poll::Ready(result) => Some(result),
        };
        let server_result = match server.connection.poll(
            cx,
            &mut server.host.negotiation,
            &mut server.host.delivery,
        ) {
            Poll::Pending => None,
            Poll::Ready(result) => Some(result),
        };
        if client_result.is_some() || server_result.is_some() {
            Poll::Ready((client_result, server_result))
        } else {
            Poll::Pending
        }
    })
    .await
}

#[allow(dead_code)]
fn poll_public_once(side: &mut PublicSide) -> Poll<Result<ConnectionEvent, ConnectionError>> {
    let mut cx = Context::from_waker(Waker::noop());
    side.connection
        .poll(&mut cx, &mut side.host.negotiation, &mut side.host.delivery)
}

fn only_termination(teardown: &crate::public_connection::ConnectionTeardown) -> FlowTermination {
    assert!(teardown.cleanup_error().is_none());
    assert_eq!(teardown.connection(), FIRST_CONNECTION);
    assert_eq!(teardown.flow_terminations().len(), 1);
    teardown.flow_terminations()[0]
}

fn assert_clean_public_teardown(
    teardown: &crate::public_connection::ConnectionTeardown,
    expected: ConnectionHandle,
) {
    assert_eq!(teardown.connection(), expected);
    assert!(teardown.cleanup_error().is_none());
    assert!(teardown.flow_terminations().is_empty());
}

fn assert_existing_connection_failure(kind: PublicConnectionErrorKind) {
    assert!(
        matches!(
            kind,
            PublicConnectionErrorKind::EstablishedTransport
                | PublicConnectionErrorKind::EstablishedControl(ProfileBootstrapFailure::Transport)
                | PublicConnectionErrorKind::EstablishedControl(ProfileBootstrapFailure::Control)
        ),
        "connection loss changed existing failure classification: {kind:?}"
    );
}

fn resources_with_idle_timeout(timeout: Duration) -> ValidatedEndpointResources {
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
        max_idle_timeout: timeout,
    }
    .validate()
    .unwrap()
}

async fn close_test_endpoints(
    client: &ConfiguredEndpoint,
    server: &ConfiguredEndpoint,
    reason: &'static [u8],
) {
    client.endpoint().close(VarInt::from_u32(0), reason);
    server.endpoint().close(VarInt::from_u32(0), reason);
    join2(client.endpoint().wait_idle(), server.endpoint().wait_idle()).await;
}
