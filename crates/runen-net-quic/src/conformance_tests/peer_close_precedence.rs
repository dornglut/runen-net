use std::{future::Future, future::poll_fn, task::Poll, time::Duration};

use crate::public_connection::{ConnectionError, ConnectionStateError};

use super::*;

struct PublicSide {
    connection: PublicConnection,
    host: HostState,
}

#[test]
fn partial_control_frame_remains_failure_when_peer_no_error_becomes_observable() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, async {
            let resources = resources_with_small_control_window();
            let (client, server, mut client_side, server_side) =
                establish_public_pair_with_resources(resources).await;
            let PublicSide {
                connection: server_connection,
                host: mut server_host,
            } = server_side;
            let (mut server_driver, _receive_limits) = server_connection
                .into_established_internal()
                .expect("server was not established for partial-control injection");

            // OPEN_FLOW is wire type 6 in runennet/1. Declare a body larger than the tiny
            // receive window, but omit its final byte. Driving the raw send to completion while
            // the peer public connection remains pending proves that the peer consumed the header
            // and almost all body bytes and is blocked inside this frame, rather than merely having
            // transport data queued when the connection is closed.
            const BODY_LEN: usize = 1024;
            let mut partial = encode_varint(6).unwrap().as_slice().to_vec();
            partial.extend_from_slice(encode_varint(BODY_LEN as u64).unwrap().as_slice());
            partial.resize(partial.len() + BODY_LEN - 1, 0);

            drive_raw_control_send_while_public_pending(
                &mut server_driver,
                partial.as_slice(),
                &mut client_side,
            )
            .await;

            server_driver.close_for_test(ApplicationErrorCode::NoError);

            let error = next_public_result(&mut client_side)
                .await
                .expect_err("partial control frame was normalized into peer NO_ERROR close");
            assert_eq!(
                error.kind(),
                PublicConnectionErrorKind::EstablishedControl(ProfileBootstrapFailure::Control)
            );

            let terminal = next_public_result(&mut client_side)
                .await
                .expect_err("failed partial-control connection did not remain terminal");
            assert_eq!(
                terminal.kind(),
                PublicConnectionErrorKind::State(ConnectionStateError::Terminal)
            );

            let client_teardown = client_side.connection.teardown(
                &mut client_side.host.negotiation,
                &mut client_side.host.delivery,
            );
            assert_eq!(client_teardown.connection(), FIRST_CONNECTION);
            assert!(client_teardown.cleanup_error().is_none());
            assert!(client_teardown.flow_terminations().is_empty());

            let server_teardown = server_driver.teardown(
                &mut server_host.negotiation,
                &mut server_host.delivery,
            );
            assert_eq!(server_teardown.connection, FIRST_CONNECTION);
            assert!(server_teardown.negotiation_cleanup_error.is_none());
            assert!(server_teardown.flow_terminations.is_empty());

            close_test_endpoints(&client, &server, b"partial-control precedence proof").await;
        })
        .await
        .expect("partial-control/NO_ERROR precedence proof timed out");
    });
}

async fn drive_raw_control_send_while_public_pending(
    driver: &mut EstablishedConnectionDriver,
    bytes: &[u8],
    peer: &mut PublicSide,
) {
    let mut send = Box::pin(driver.send_raw_control_bytes_for_test(bytes));
    poll_fn(|cx| {
        let send_complete = match send.as_mut().poll(cx) {
            Poll::Pending => false,
            Poll::Ready(Ok(())) => true,
            Poll::Ready(Err(error)) => panic!("partial raw control send failed: {error:?}"),
        };

        match peer.connection.poll(
            cx,
            &mut peer.host.negotiation,
            &mut peer.host.delivery,
        ) {
            Poll::Pending => {}
            Poll::Ready(Ok(event)) => {
                panic!("partial control frame unexpectedly produced public progress: {event:?}")
            }
            Poll::Ready(Err(error)) => {
                panic!("partial control frame failed before peer close: {error:?}")
            }
        }

        if send_complete {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    })
    .await;
}

async fn establish_public_pair_with_resources(
    resources: ValidatedEndpointResources,
) -> (
    ConfiguredEndpoint,
    ConfiguredEndpoint,
    PublicSide,
    PublicSide,
) {
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
    let client_ready = client_ready.expect("precedence client failed ProfileReady");
    let server_ready = server_ready
        .expect("precedence server failed ProfileReady")
        .expect("precedence server endpoint closed before ProfileReady");

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
        let (client_result, server_result) = poll_fn(|cx| {
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
        .await;

        if let Some(result) = client_result {
            match result.expect("precedence client negotiation failed") {
                ConnectionEvent::AuthoritySelectionRequired { connection } => {
                    assert_eq!(connection, FIRST_CONNECTION);
                    assert!(!authority_selected, "Authority selection repeated");
                    client
                        .connection
                        .select_authority(&mut client.host.negotiation, contract())
                        .expect("valid Authority selection failed");
                    authority_selected = true;
                }
                ConnectionEvent::Established { connection } => {
                    assert_eq!(connection, FIRST_CONNECTION);
                    client_established = true;
                }
                event => panic!("unexpected client negotiation event: {event:?}"),
            }
        }
        if let Some(result) = server_result {
            match result.expect("precedence server negotiation failed") {
                ConnectionEvent::Established { connection } => {
                    assert_eq!(connection, FIRST_CONNECTION);
                    server_established = true;
                }
                event => panic!("unexpected server negotiation event: {event:?}"),
            }
        }
    }

    assert!(authority_selected, "Authority was never explicitly selected");
}

async fn next_public_result(side: &mut PublicSide) -> Result<ConnectionEvent, ConnectionError> {
    poll_fn(|cx| {
        side.connection
            .poll(cx, &mut side.host.negotiation, &mut side.host.delivery)
    })
    .await
}

fn resources_with_small_control_window() -> ValidatedEndpointResources {
    EndpointResourceLimits {
        max_connections: 4,
        max_active_incoming_flows: 16,
        udp_payload_ceiling: 1_452,
        stream_receive_window: 32,
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

async fn close_test_endpoints(
    client: &ConfiguredEndpoint,
    server: &ConfiguredEndpoint,
    reason: &'static [u8],
) {
    client.endpoint().close(VarInt::from_u32(0), reason);
    server.endpoint().close(VarInt::from_u32(0), reason);
    join2(client.endpoint().wait_idle(), server.endpoint().wait_idle()).await;
}
