#[test]
fn committed_local_termination_surfaces_before_peer_close() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            let (client, server, mut client_side, mut server_side) =
                establish_public_pair_with_resources(resources()).await;
            let (outbound, inbound) = establish_public_unreliable_flow(
                &mut client_side,
                &mut server_side,
                503,
                603,
            )
            .await;

            client_side
                .connection
                .finish_outbound_flow_normal(&mut client_side.host.delivery, outbound)
                .expect("local unreliable termination did not start");
            assert!(
                client_side
                    .host
                    .delivery
                    .flow_contract(outbound)
                    .is_none(),
                "local termination effect was not committed before peer close"
            );

            // Do not poll the local sender after committing termination. The peer closes first,
            // so the stored FLOW_TERMINATE effect must survive the connection-loss send result and
            // be surfaced before the connection-lifecycle PeerClosed event.
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

            let first = next_public_result(&mut client_side)
                .await
                .expect("committed local termination became a public failure");
            match first {
                ConnectionEvent::FlowTerminated {
                    key,
                    origin,
                    cause,
                    termination: Some(termination),
                } => {
                    assert_eq!(key, outbound);
                    assert_eq!(origin, crate::FlowTerminationOrigin::Local);
                    assert_eq!(cause, crate::FlowTerminationCause::Normal);
                    assert_eq!(termination.key, outbound);
                    assert_eq!(termination.reason, FlowTerminationReason::Requested);
                }
                event => panic!(
                    "peer close overtook the already-committed local termination effect: {event:?}"
                ),
            }

            let second = next_public_result(&mut client_side)
                .await
                .expect("peer NO_ERROR became a public failure after committed effect");
            assert!(matches!(
                second,
                ConnectionEvent::PeerClosed { connection }
                    if connection == FIRST_CONNECTION
            ));

            let terminal = next_public_result(&mut client_side)
                .await
                .expect_err("peer close was not one-shot after committed effect");
            assert_eq!(
                terminal.kind(),
                PublicConnectionErrorKind::State(ConnectionStateError::Terminal)
            );

            let client_teardown = client_side.connection.teardown(
                &mut client_side.host.negotiation,
                &mut client_side.host.delivery,
            );
            assert_clean_public_teardown(&client_teardown, FIRST_CONNECTION);
            close_test_endpoints(&client, &server, b"committed send effect proof").await;
        })
        .await
        .expect("committed send-effect/peer-close proof timed out");
    });
}

async fn establish_public_unreliable_flow(
    client: &mut PublicSide,
    server: &mut PublicSide,
    outbound_handle: u64,
    inbound_handle: u64,
) -> (DeliveryFlowKey, DeliveryFlowKey) {
    let mode = DeliveryMode::UnreliableUnordered;
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
                mode,
                policy: policy(mode),
                connection_limits: connection_limits(),
            },
        )
        .expect("public unreliable open failed");

    let request = loop {
        let (client_result, server_result) = next_public_pair_result(client, server).await;
        if let Some(result) = client_result {
            let event = result.expect("sender failed before unreliable incoming admission");
            panic!("sender surfaced progress before unreliable incoming admission: {event:?}");
        }
        if let Some(result) = server_result {
            match result.expect("receiver failed before unreliable incoming admission") {
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
                policy: policy(mode),
                connection_limits: connection_limits(),
            },
        )
        .expect("public unreliable admission failed");

    loop {
        let (client_result, server_result) = next_public_pair_result(client, server).await;
        if let Some(result) = server_result {
            let event = result.expect("receiver failed during unreliable flow establishment");
            panic!("receiver surfaced unexpected unreliable establishment event: {event:?}");
        }
        if let Some(result) = client_result {
            match result.expect("sender failed during unreliable flow establishment") {
                ConnectionEvent::OutboundFlowEstablished { key } => {
                    assert_eq!(key, outbound);
                    break;
                }
                event => panic!("unexpected sender unreliable establishment event: {event:?}"),
            }
        }
    }
    (outbound, inbound)
}
