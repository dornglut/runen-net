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
    ConnectionError as QuinnConnectionError, VarInt,
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
    control::{
        LocalControlLimits, ProfileBootstrapError, SemanticRole, ValidatedControlProfile,
        bootstrap_server_control, confirm_profile_transport,
    },
    datagram::DatagramSubmissionOutcome,
    endpoint::{
        ConfiguredEndpoint, ConnectionAdmissionError, EndpointResourceLimits,
        ValidatedEndpointResources, bind_client_endpoint, bind_server_endpoint,
        bind_server_endpoint_with_incompatible_alpn, bind_server_endpoint_without_datagrams,
    },
    facade::{ProfileBootstrapFailure, ProfileReadyConnection},
    flow_control::{InboundAdmission, OutboundOpenRequest},
    lifecycle::{
        AdmittedProfileReadyConnection, ProfileConnectionError, accept_profile_ready,
        connect_profile_ready,
    },
    public_connection::{
        Connection as PublicConnection, ConnectionError as PublicConnectionError, ConnectionEvent,
        ReliableReceiveLimits,
    },
    quinn_binding::{ReceiveProgress, SendProgress},
    wire::{ApplicationErrorCode, FlowId, FlowTerminateReason, WireSide, encode_varint},
};

const SCENARIO_TIMEOUT: Duration = Duration::from_secs(20);
const FIRST_CONNECTION: ConnectionHandle = ConnectionHandle::new(1);
const REPLACEMENT_CONNECTION: ConnectionHandle = ConnectionHandle::new(2);
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
    connection: ConnectionHandle,
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

#[test]
fn replacement_connection_restarts_fresh_connection_scoped_state() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, run_replacement_scenario())
            .await
            .expect("live QUIC replacement-state conformance scenario timed out");
    });
}

#[test]
fn live_settings_role_mismatch_rejects_profile_and_releases_admission() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, run_role_mismatch_scenario())
            .await
            .expect("live SETTINGS role-mismatch conformance scenario timed out");
    });
}

#[test]
fn live_unknown_post_profile_control_frame_closes_with_control_frame_error() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, run_unknown_control_frame_scenario())
            .await
            .expect("live malformed-control conformance scenario timed out");
    });
}

async fn run_unknown_control_frame_scenario() {
    let resources = resources_with_max_connections(1);
    let (certificate, private_key, roots) = ephemeral_identity();
    let client = bind_client_endpoint(loopback_ephemeral(), resources, roots).unwrap();
    let server = bind_server_endpoint(
        loopback_ephemeral(),
        resources,
        vec![certificate],
        private_key,
    )
    .unwrap();
    let server_address = server.endpoint().local_addr().unwrap();

    let adversarial_server = async {
        let incoming = server
            .endpoint()
            .accept()
            .await
            .expect("adversarial server endpoint closed before connection");
        let connecting = incoming
            .accept()
            .expect("adversarial server rejected valid QUIC admission");
        let connection = connecting
            .await
            .expect("adversarial server failed valid runennet/1 handshake");
        let transport = confirm_profile_transport(connection)
            .expect("adversarial server failed production transport confirmation");
        bootstrap_server_control(transport, profile(resources, SemanticRole::NonAuthority))
            .await
            .expect("adversarial server failed production ProfileReady bootstrap")
    };

    let (client_ready, server_ready) = join2(
        connect_profile_ready(
            &client,
            server_address,
            "localhost",
            profile(resources, SemanticRole::Authority),
        ),
        adversarial_server,
    )
    .await;
    let client_ready = client_ready.expect("production client failed ProfileReady");
    let mut server_parts = server_ready.into_parts();

    let mut host = new_host();
    let mut public = activate_public_connection(client_ready, FIRST_CONNECTION, &mut host);

    server_parts
        .sender
        .send_raw_bytes_for_test(&[10])
        .await
        .expect("adversarial raw control byte was not written");

    let error = poll_fn(
        |cx| match public.poll(cx, &mut host.negotiation, &mut host.delivery) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(error),
            Poll::Ready(Ok(event)) => {
                panic!("unknown control frame unexpectedly produced public progress: {event:?}")
            }
        },
    )
    .await;
    assert_eq!(
        error,
        PublicConnectionError::Control {
            failure: ProfileBootstrapFailure::Control,
            cleanup_failed: false,
        }
    );

    let teardown = public.teardown(&mut host.negotiation, &mut host.delivery);
    assert_eq!(teardown.connection(), FIRST_CONNECTION);
    assert!(teardown.cleanup_error().is_none());
    assert!(teardown.flow_terminations().is_empty());

    match server_parts.connection.closed().await {
        QuinnConnectionError::ApplicationClosed(close) => assert_eq!(
            close.error_code,
            ApplicationErrorCode::ControlFrameError.quinn()
        ),
        other => panic!("malformed control produced wrong peer close: {other:?}"),
    }

    client
        .endpoint()
        .close(VarInt::from_u32(0), b"malformed-control test complete");
    server
        .endpoint()
        .close(VarInt::from_u32(0), b"malformed-control test complete");
    join2(client.endpoint().wait_idle(), server.endpoint().wait_idle()).await;
}

#[test]
fn live_known_flow_oversized_datagram_is_isolated_and_survivor_stays_live() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(
            SCENARIO_TIMEOUT,
            run_known_flow_datagram_isolation_scenario(),
        )
        .await
        .expect("live known-flow DATAGRAM isolation scenario timed out");
    });
}

async fn run_known_flow_datagram_isolation_scenario() {
    let (client, server, mut client_side, mut server_side) =
        establish_live_pair(AuthoritySide::Client).await;

    let target = establish_flow(
        &mut client_side,
        &mut server_side,
        DeliveryMode::UnreliableUnordered,
        41,
        141,
    )
    .await;
    let survivor = establish_flow(
        &mut client_side,
        &mut server_side,
        DeliveryMode::UnreliableSequenced,
        42,
        142,
    )
    .await;

    send_unreliable_and_expect(
        &mut client_side,
        &mut server_side,
        survivor,
        b"survivor-before-malformed".to_vec(),
    )
    .await;

    let flow_prefix = encode_varint(target.flow_id.value()).unwrap();
    let mut malformed = flow_prefix.as_slice().to_vec();
    malformed.resize(malformed.len() + MAX_MESSAGE_BYTES + 1, 0xa5);
    client_side
        .driver
        .send_raw_datagram_for_test(malformed)
        .expect("raw oversized DATAGRAM injection failed");

    let mut receiver_failure_handled = false;
    let mut receiver_report_sent = false;
    let mut sender_remote_terminated = false;
    while !(receiver_failure_handled && receiver_report_sent && sender_remote_terminated) {
        let (client_progress, server_progress) =
            next_pair_progress(&mut client_side, &mut server_side).await;

        if let Some(progress) = server_progress {
            match progress {
                EstablishedConnectionProgress::FlowFailureHandled { flow_id } => {
                    assert_eq!(flow_id, target.flow_id);
                    receiver_failure_handled = true;
                }
                EstablishedConnectionProgress::ControlSendCompleted(
                    crate::flow_driver::FlowControlSendEffect::LocalTerminated {
                        flow,
                        reason,
                        termination,
                    },
                ) => {
                    assert_eq!(flow.flow_id(), target.flow_id);
                    assert_eq!(flow.key(), target.inbound);
                    assert_eq!(reason, FlowTerminateReason::ProtocolFailure);
                    assert_eq!(termination.key, target.inbound);
                    assert_eq!(termination.reason, FlowTerminationReason::Requested);
                    receiver_report_sent = true;
                }
                other => assert_non_failure_progress(&other),
            }
        }

        if let Some(progress) = client_progress {
            match progress {
                EstablishedConnectionProgress::RemoteTerminated {
                    flow,
                    reason,
                    termination,
                } => {
                    assert_eq!(flow.flow_id(), target.flow_id);
                    assert_eq!(flow.key(), target.outbound);
                    assert_eq!(reason, FlowTerminateReason::ProtocolFailure);
                    assert_eq!(termination.key, target.outbound);
                    assert_eq!(termination.reason, FlowTerminationReason::Requested);
                    sender_remote_terminated = true;
                }
                other => assert_non_failure_progress(&other),
            }
        }
    }

    loop {
        let (client_progress, server_progress) =
            next_pair_progress(&mut client_side, &mut server_side).await;
        if let Some(progress) = server_progress.as_ref() {
            assert_non_failure_progress(progress);
        }
        if let Some(progress) = client_progress {
            match progress {
                EstablishedConnectionProgress::DatagramOutbound(
                    crate::datagram_driver::DatagramOutboundProgress::Cancelled { flow_id },
                ) if flow_id == target.flow_id => break,
                other => assert_non_failure_progress(&other),
            }
        }
    }

    assert!(
        client_side
            .host
            .delivery
            .flow_contract(target.outbound)
            .is_none()
    );
    assert!(
        server_side
            .host
            .delivery
            .flow_contract(target.inbound)
            .is_none()
    );
    assert!(
        client_side
            .host
            .delivery
            .flow_contract(survivor.outbound)
            .is_some()
    );
    assert!(
        server_side
            .host
            .delivery
            .flow_contract(survivor.inbound)
            .is_some()
    );

    send_unreliable_and_expect_index(
        &mut client_side,
        &mut server_side,
        survivor,
        b"survivor-after-malformed".to_vec(),
        1,
    )
    .await;

    let client_teardown = client_side.driver.teardown(
        &mut client_side.host.negotiation,
        &mut client_side.host.delivery,
    );
    let server_teardown = server_side.driver.teardown(
        &mut server_side.host.negotiation,
        &mut server_side.host.delivery,
    );
    for teardown in [&client_teardown, &server_teardown] {
        assert_eq!(teardown.connection, FIRST_CONNECTION);
        assert!(teardown.negotiation_cleanup_error.is_none());
        assert_eq!(teardown.flow_terminations.len(), 1);
        assert_eq!(
            teardown.flow_terminations[0].reason,
            FlowTerminationReason::ConnectionEnded
        );
    }
    assert_eq!(client_side.host.delivery.active_flows(), 0);
    assert_eq!(server_side.host.delivery.active_flows(), 0);

    client
        .endpoint()
        .close(VarInt::from_u32(0), b"known-flow DATAGRAM test complete");
    server
        .endpoint()
        .close(VarInt::from_u32(0), b"known-flow DATAGRAM test complete");
    join2(client.endpoint().wait_idle(), server.endpoint().wait_idle()).await;
}

#[test]
fn live_overlapping_connection_is_refused_at_server_capacity_and_slot_reopens() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, run_overlapping_admission_scenario())
            .await
            .expect("live overlapping-admission conformance scenario timed out");
    });
}

async fn run_overlapping_admission_scenario() {
    let resources = resources_with_max_connections(1);
    let (certificate, private_key, roots) = ephemeral_identity();
    let client_a =
        bind_client_endpoint(loopback_ephemeral(), resources, Arc::clone(&roots)).unwrap();
    let client_b = bind_client_endpoint(loopback_ephemeral(), resources, roots).unwrap();
    let server = bind_server_endpoint(
        loopback_ephemeral(),
        resources,
        vec![certificate],
        private_key,
    )
    .unwrap();
    let server_address = server.endpoint().local_addr().unwrap();

    let (client_a_ready, server_a_ready) = join2(
        connect_profile_ready(
            &client_a,
            server_address,
            "localhost",
            profile(resources, SemanticRole::Authority),
        ),
        accept_profile_ready(&server, profile(resources, SemanticRole::NonAuthority)),
    )
    .await;
    let client_a_ready = client_a_ready.expect("first capacity-1 client failed ProfileReady");
    let server_a_ready = server_a_ready
        .expect("first capacity-1 server failed ProfileReady")
        .expect("capacity-1 server endpoint closed before first ProfileReady");

    let (client_b_failure, server_rejection) = join2(
        connect_profile_ready(
            &client_b,
            server_address,
            "localhost",
            profile(resources, SemanticRole::Authority),
        ),
        accept_profile_ready(&server, profile(resources, SemanticRole::NonAuthority)),
    )
    .await;
    assert!(matches!(
        server_rejection,
        Err(ProfileConnectionError::Admission(
            ConnectionAdmissionError::AtCapacity
        ))
    ));
    assert!(matches!(
        client_b_failure,
        Err(ProfileConnectionError::Handshake(_))
    ));

    drop(client_a_ready);
    drop(server_a_ready);

    let (client_b_ready, server_b_ready) = join2(
        connect_profile_ready(
            &client_b,
            server_address,
            "localhost",
            profile(resources, SemanticRole::Authority),
        ),
        accept_profile_ready(&server, profile(resources, SemanticRole::NonAuthority)),
    )
    .await;
    let client_b_ready =
        client_b_ready.expect("client-B admission permit leaked after refused overlap");
    let server_b_ready = server_b_ready
        .expect("server slot did not reopen after first admitted connection was released")
        .expect("server endpoint closed before post-overlap retry");

    drop(client_b_ready);
    drop(server_b_ready);
    client_a
        .endpoint()
        .close(VarInt::from_u32(0), b"overlapping admission test complete");
    client_b
        .endpoint()
        .close(VarInt::from_u32(0), b"overlapping admission test complete");
    server
        .endpoint()
        .close(VarInt::from_u32(0), b"overlapping admission test complete");
    client_a.endpoint().wait_idle().await;
    client_b.endpoint().wait_idle().await;
    server.endpoint().wait_idle().await;
}

#[test]
fn live_incompatible_alpn_is_rejected_at_handshake_and_releases_admission() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, run_incompatible_alpn_scenario())
            .await
            .expect("live incompatible-ALPN conformance scenario timed out");
    });
}

async fn run_incompatible_alpn_scenario() {
    let resources = resources_with_max_connections(1);
    let (certificate, private_key, roots) = ephemeral_identity();
    let client = bind_client_endpoint(loopback_ephemeral(), resources, roots).unwrap();
    let incompatible_server = bind_server_endpoint_with_incompatible_alpn(
        loopback_ephemeral(),
        resources,
        vec![certificate.clone()],
        private_key.clone_key(),
    )
    .unwrap();
    let incompatible_address = incompatible_server.local_addr().unwrap();

    let negative_peer = async {
        let incoming = incompatible_server
            .accept()
            .await
            .expect("incompatible-ALPN endpoint closed before handshake attempt");
        if let Ok(connecting) = incoming.accept() {
            assert!(
                connecting.await.is_err(),
                "incompatible ALPN unexpectedly produced a QUIC Connection"
            );
        }
    };
    let (client_failure, ()) = join2(
        connect_profile_ready(
            &client,
            incompatible_address,
            "localhost",
            profile(resources, SemanticRole::Authority),
        ),
        negative_peer,
    )
    .await;
    assert!(matches!(
        client_failure,
        Err(ProfileConnectionError::Handshake(_))
    ));

    incompatible_server.close(VarInt::from_u32(0), b"incompatible ALPN test complete");
    incompatible_server.wait_idle().await;

    let server = bind_server_endpoint(
        loopback_ephemeral(),
        resources,
        vec![certificate],
        private_key,
    )
    .unwrap();
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
    let client_ready =
        client_ready.expect("capacity-1 client permit leaked after ALPN handshake failure");
    let server_ready = server_ready
        .expect("conforming retry server failed after ALPN handshake failure")
        .expect("conforming retry server endpoint closed unexpectedly");

    drop(client_ready);
    drop(server_ready);
    client
        .endpoint()
        .close(VarInt::from_u32(0), b"incompatible-ALPN test complete");
    server
        .endpoint()
        .close(VarInt::from_u32(0), b"incompatible-ALPN test complete");
    join2(client.endpoint().wait_idle(), server.endpoint().wait_idle()).await;
}

#[test]
fn live_missing_datagram_rejects_profile_and_releases_admission() {
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        tokio::time::timeout(SCENARIO_TIMEOUT, run_missing_datagram_scenario())
            .await
            .expect("live missing-DATAGRAM conformance scenario timed out");
    });
}

async fn run_missing_datagram_scenario() {
    let resources = resources_with_max_connections(1);
    let (certificate, private_key, roots) = ephemeral_identity();
    let client = bind_client_endpoint(loopback_ephemeral(), resources, roots).unwrap();
    let no_datagram_server = bind_server_endpoint_without_datagrams(
        loopback_ephemeral(),
        resources,
        vec![certificate.clone()],
        private_key.clone_key(),
    )
    .unwrap();
    let no_datagram_address = no_datagram_server.local_addr().unwrap();

    let negative_peer = async {
        let incoming = no_datagram_server
            .accept()
            .await
            .expect("negative DATAGRAM endpoint closed before handshake");
        let connecting = incoming
            .accept()
            .expect("negative DATAGRAM peer rejected compatible QUIC handshake");
        let connection = connecting
            .await
            .expect("negative DATAGRAM peer failed compatible runennet/1 handshake");
        assert!(
            connection.accept_bi().await.is_err(),
            "production client opened the RunenNet control stream before DATAGRAM confirmation"
        );
    };
    let (client_failure, ()) = join2(
        connect_profile_ready(
            &client,
            no_datagram_address,
            "localhost",
            profile(resources, SemanticRole::Authority),
        ),
        negative_peer,
    )
    .await;
    assert!(matches!(
        client_failure,
        Err(ProfileConnectionError::Bootstrap(
            ProfileBootstrapError::DatagramUnsupported
        ))
    ));

    no_datagram_server.close(VarInt::from_u32(0), b"negative DATAGRAM test complete");
    no_datagram_server.wait_idle().await;

    let server = bind_server_endpoint(
        loopback_ephemeral(),
        resources,
        vec![certificate],
        private_key,
    )
    .unwrap();
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
    let client_ready =
        client_ready.expect("capacity-1 client permit leaked after DATAGRAM rejection");
    let server_ready = server_ready
        .expect("conforming retry server failed after DATAGRAM rejection")
        .expect("conforming retry server endpoint closed unexpectedly");

    drop(client_ready);
    drop(server_ready);
    client
        .endpoint()
        .close(VarInt::from_u32(0), b"missing-DATAGRAM test complete");
    server
        .endpoint()
        .close(VarInt::from_u32(0), b"missing-DATAGRAM test complete");
    join2(client.endpoint().wait_idle(), server.endpoint().wait_idle()).await;
}

async fn run_role_mismatch_scenario() {
    let resources = resources_with_max_connections(1);
    let (client, server) = configured_endpoints_with_resources(resources);
    let server_address = server.endpoint().local_addr().unwrap();

    let (client_failure, server_failure) = join2(
        connect_profile_ready(
            &client,
            server_address,
            "localhost",
            profile(resources, SemanticRole::Authority),
        ),
        accept_profile_ready(&server, profile(resources, SemanticRole::Authority)),
    )
    .await;

    let client_mismatch = bootstrap_role_mismatch(client_failure.unwrap_err());
    let server_mismatch = bootstrap_role_mismatch(server_failure.unwrap_err());
    assert!(
        client_mismatch || server_mismatch,
        "neither production bootstrap observed the SETTINGS semantic-role mismatch"
    );

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
    let client_ready =
        client_ready.expect("capacity-1 client permit leaked after bootstrap failure");
    let server_ready = server_ready
        .expect("capacity-1 server permit leaked after bootstrap failure")
        .expect("server endpoint remained open for corrected retry");

    drop(client_ready);
    drop(server_ready);
    client
        .endpoint()
        .close(VarInt::from_u32(0), b"role-mismatch test complete");
    server
        .endpoint()
        .close(VarInt::from_u32(0), b"role-mismatch test complete");
    join2(client.endpoint().wait_idle(), server.endpoint().wait_idle()).await;
}

fn bootstrap_role_mismatch(error: ProfileConnectionError) -> bool {
    match error {
        ProfileConnectionError::Bootstrap(ProfileBootstrapError::PeerRoleMismatch {
            expected: SemanticRole::NonAuthority,
            received: SemanticRole::Authority,
        }) => true,
        ProfileConnectionError::Bootstrap(_) => false,
        other => panic!("role-mismatch connection failed outside bootstrap: {other:?}"),
    }
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
    assert_eq!(client_teardown.connection, FIRST_CONNECTION);
    assert_eq!(server_teardown.connection, FIRST_CONNECTION);
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

fn configured_endpoints() -> (ConfiguredEndpoint, ConfiguredEndpoint) {
    configured_endpoints_with_resources(resources())
}

fn configured_endpoints_with_resources(
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
        negotiation: new_manager(),
        delivery: DeliveryEndpoint::new(aggregate_limits()),
    }
}

async fn establish_live_pair(
    authority_side: AuthoritySide,
) -> (ConfiguredEndpoint, ConfiguredEndpoint, LiveSide, LiveSide) {
    let (client, server) = configured_endpoints();
    let (client_side, server_side) = establish_connection_on_endpoints(
        &client,
        &server,
        authority_side,
        FIRST_CONNECTION,
        new_host(),
        new_host(),
    )
    .await;
    (client, server, client_side, server_side)
}

async fn establish_connection_on_endpoints(
    client: &ConfiguredEndpoint,
    server: &ConfiguredEndpoint,
    authority_side: AuthoritySide,
    connection: ConnectionHandle,
    client_host: HostState,
    server_host: HostState,
) -> (LiveSide, LiveSide) {
    let resources = resources();
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
        connect_profile_ready(client, server_address, "localhost", client_profile),
        accept_profile_ready(server, server_profile),
    )
    .await;
    let client_ready = client_ready.unwrap();
    let server_ready = server_ready
        .unwrap()
        .expect("server endpoint remained open");

    let contract = contract();
    let client_authority = (authority_side == AuthoritySide::Client).then(|| contract.clone());
    let server_authority = (authority_side == AuthoritySide::Server).then(|| contract.clone());

    join2(
        negotiate_side(client_ready, connection, client_host, client_authority),
        negotiate_side(server_ready, connection, server_host, server_authority),
    )
    .await
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
        assert_eq!(teardown.connection, FIRST_CONNECTION);
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

async fn run_replacement_scenario() {
    let (client, server) = configured_endpoints();
    let (mut first_client, mut first_server) = establish_connection_on_endpoints(
        &client,
        &server,
        AuthoritySide::Client,
        FIRST_CONNECTION,
        new_host(),
        new_host(),
    )
    .await;

    let first_reliable = establish_flow(
        &mut first_client,
        &mut first_server,
        DeliveryMode::ReliableOrdered,
        21,
        121,
    )
    .await;
    let first_sequenced = establish_flow(
        &mut first_server,
        &mut first_client,
        DeliveryMode::UnreliableSequenced,
        22,
        122,
    )
    .await;

    assert_eq!(first_reliable.flow_id.side(), WireSide::Client);
    assert_eq!(first_reliable.flow_id.sequence(), 0);
    assert_eq!(first_sequenced.flow_id.side(), WireSide::Server);
    assert_eq!(first_sequenced.flow_id.sequence(), 0);
    for key in [
        first_reliable.outbound,
        first_reliable.inbound,
        first_sequenced.outbound,
        first_sequenced.inbound,
    ] {
        assert_eq!(key.connection(), FIRST_CONNECTION);
    }

    send_unreliable_and_expect(
        &mut first_server,
        &mut first_client,
        first_sequenced,
        b"first-connection-sequenced".to_vec(),
    )
    .await;

    send_reliable_and_expect(
        &mut first_client,
        &mut first_server,
        first_reliable,
        b"first-connection-reliable".to_vec(),
    )
    .await;

    let buffered_payload = b"first-connection-buffered-custody".to_vec();
    assert!(matches!(
        first_server.driver.submit_unreliable(
            &mut first_server.host.delivery,
            first_sequenced.flow_id,
            buffered_payload.clone(),
        ),
        Ok(DatagramSubmitOutcome::Submitted(
            DatagramSubmissionOutcome::Accepted {
                accepted_index: 1,
                local_pressure_drops: 0,
            }
        ))
    ));
    drive_until_buffered_without_exposure(
        &mut first_server,
        &mut first_client,
        first_sequenced.inbound,
        buffered_payload.len(),
    )
    .await;
    assert_eq!(first_client.host.delivery.pending_messages(), 1);
    assert_eq!(
        first_client.host.delivery.pending_payload_bytes(),
        buffered_payload.len()
    );

    let first_reliable_flow_id = first_reliable.flow_id;
    let first_sequenced_flow_id = first_sequenced.flow_id;
    let client_host = teardown_live_side(first_client, 2, 1);
    let server_host = teardown_live_side(first_server, 2, 0);

    let (mut replacement_client, mut replacement_server) = establish_connection_on_endpoints(
        &client,
        &server,
        AuthoritySide::Client,
        REPLACEMENT_CONNECTION,
        client_host,
        server_host,
    )
    .await;

    assert_eq!(replacement_client.host.delivery.active_flows(), 0);
    assert_eq!(replacement_server.host.delivery.active_flows(), 0);
    assert_eq!(replacement_client.host.delivery.pending_messages(), 0);
    assert_eq!(replacement_server.host.delivery.pending_messages(), 0);
    assert_eq!(replacement_client.host.delivery.pending_payload_bytes(), 0);
    assert_eq!(replacement_server.host.delivery.pending_payload_bytes(), 0);

    let replacement_reliable = establish_flow(
        &mut replacement_client,
        &mut replacement_server,
        DeliveryMode::ReliableOrdered,
        21,
        121,
    )
    .await;
    let replacement_sequenced = establish_flow(
        &mut replacement_server,
        &mut replacement_client,
        DeliveryMode::UnreliableSequenced,
        22,
        122,
    )
    .await;

    assert_eq!(replacement_reliable.flow_id, first_reliable_flow_id);
    assert_eq!(replacement_sequenced.flow_id, first_sequenced_flow_id);
    assert_eq!(replacement_reliable.flow_id.sequence(), 0);
    assert_eq!(replacement_sequenced.flow_id.sequence(), 0);
    for key in [
        replacement_reliable.outbound,
        replacement_reliable.inbound,
        replacement_sequenced.outbound,
        replacement_sequenced.inbound,
    ] {
        assert_eq!(key.connection(), REPLACEMENT_CONNECTION);
        assert_ne!(key.connection(), FIRST_CONNECTION);
    }

    send_reliable_and_expect(
        &mut replacement_client,
        &mut replacement_server,
        replacement_reliable,
        b"replacement-reliable".to_vec(),
    )
    .await;
    send_unreliable_and_expect(
        &mut replacement_server,
        &mut replacement_client,
        replacement_sequenced,
        b"replacement-sequenced".to_vec(),
    )
    .await;

    let _client_host = teardown_live_side(replacement_client, 2, 0);
    let _server_host = teardown_live_side(replacement_server, 2, 0);

    client
        .endpoint()
        .close(VarInt::from_u32(0), b"replacement-state test complete");
    server
        .endpoint()
        .close(VarInt::from_u32(0), b"replacement-state test complete");
    join2(client.endpoint().wait_idle(), server.endpoint().wait_idle()).await;
}

fn teardown_live_side(
    mut side: LiveSide,
    expected_flows: usize,
    expected_pending_messages: usize,
) -> HostState {
    let connection = side.connection;
    let teardown = side
        .driver
        .teardown(&mut side.host.negotiation, &mut side.host.delivery);
    assert_eq!(teardown.connection, connection);
    assert!(teardown.negotiation_cleanup_error.is_none());
    assert_eq!(teardown.flow_terminations.len(), expected_flows);
    assert!(
        teardown
            .flow_terminations
            .iter()
            .all(|termination| termination.reason == FlowTerminationReason::ConnectionEnded)
    );
    assert_eq!(
        teardown
            .flow_terminations
            .iter()
            .map(|termination| termination.pending_messages)
            .sum::<usize>(),
        expected_pending_messages
    );
    assert_eq!(side.host.delivery.active_flows(), 0);
    assert_eq!(side.host.delivery.pending_messages(), 0);
    assert_eq!(side.host.delivery.pending_payload_bytes(), 0);
    side.host
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
    resources_with_max_connections(4)
}

fn resources_with_max_connections(max_connections: usize) -> ValidatedEndpointResources {
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

fn reliable_receive_limits() -> ReliableReceiveLimits {
    ReliableReceiveLimits {
        scratch_bytes: nz(4 * 1024),
        max_staging_bytes: nz(128 * 1024),
    }
}

fn activate_public_connection(
    admitted: AdmittedProfileReadyConnection,
    connection: ConnectionHandle,
    host: &mut HostState,
) -> PublicConnection {
    ProfileReadyConnection::from(admitted)
        .activate(
            connection,
            offer(),
            NegotiationRequirements::default(),
            reliable_receive_limits(),
            &mut host.negotiation,
        )
        .expect("valid public ProfileReady activation failed")
}

async fn negotiate_side(
    admitted: AdmittedProfileReadyConnection,
    connection: ConnectionHandle,
    mut host: HostState,
    authority_contract: Option<NegotiatedContract>,
) -> LiveSide {
    let expected_authority = authority_contract.is_some();
    let mut authority_selection_events = 0usize;
    let mut public = activate_public_connection(admitted, connection, &mut host);

    loop {
        let event = poll_fn(|cx| public.poll(cx, &mut host.negotiation, &mut host.delivery))
            .await
            .expect("valid loopback public negotiation failed");
        match event {
            ConnectionEvent::AuthoritySelectionRequired {
                connection: event_connection,
            } => {
                assert_eq!(event_connection, connection);
                assert!(
                    expected_authority,
                    "NonAuthority received an Authority-selection event"
                );
                authority_selection_events += 1;
                assert_eq!(
                    authority_selection_events, 1,
                    "Authority selection was surfaced more than once"
                );
                public
                    .select_authority(
                        &mut host.negotiation,
                        authority_contract
                            .clone()
                            .expect("only the semantic Authority selects a contract"),
                    )
                    .expect("valid Authority selection command failed");
            }
            ConnectionEvent::Established {
                connection: event_connection,
            } => {
                assert_eq!(event_connection, connection);
                break;
            }
        }
    }

    assert_eq!(authority_selection_events, usize::from(expected_authority));
    let (established, reliable_receive) = public
        .into_established_internal()
        .expect("Established event did not retain negotiated ownership");
    assert_eq!(reliable_receive, reliable_receive_limits());
    let driver = established
        .into_flow_control()
        .unwrap()
        .into_reliable_io(
            reliable_receive.scratch_bytes,
            reliable_receive.max_staging_bytes,
        )
        .into_established_io()
        .into_connection_driver();
    LiveSide {
        connection,
        driver,
        host,
    }
}

async fn establish_flow(
    sender: &mut LiveSide,
    receiver: &mut LiveSide,
    mode: DeliveryMode,
    outbound_handle: u64,
    inbound_handle: u64,
) -> LiveFlow {
    let connection = sender.connection;
    assert_eq!(receiver.connection, connection);
    let outbound = DeliveryFlowKey::new(
        connection,
        FlowDirection::Outbound,
        DeliveryFlowHandle::new(outbound_handle),
    );
    let inbound = DeliveryFlowKey::new(
        connection,
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
    send_unreliable_and_expect_index(sender, receiver, flow, payload, 0).await;
}

async fn send_unreliable_and_expect_index(
    sender: &mut LiveSide,
    receiver: &mut LiveSide,
    flow: LiveFlow,
    payload: Vec<u8>,
    expected_index: u64,
) {
    assert!(matches!(
        sender
            .driver
            .submit_unreliable(&mut sender.host.delivery, flow.flow_id, payload.clone(),),
        Ok(DatagramSubmitOutcome::Submitted(
            DatagramSubmissionOutcome::Accepted {
                accepted_index,
                local_pressure_drops: 0,
            }
        )) if accepted_index == expected_index
    ));
    drive_until_exposed_index(sender, receiver, flow.inbound, &payload, expected_index).await;
}

async fn drive_until_buffered_without_exposure(
    sender: &mut LiveSide,
    receiver: &mut LiveSide,
    inbound: DeliveryFlowKey,
    expected_payload_bytes: usize,
) {
    loop {
        if receiver.host.delivery.flow_pending_usage(inbound) == Some((1, expected_payload_bytes)) {
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

async fn drive_until_exposed(
    sender: &mut LiveSide,
    receiver: &mut LiveSide,
    inbound: DeliveryFlowKey,
    expected: &[u8],
) {
    drive_until_exposed_index(sender, receiver, inbound, expected, 0).await;
}

async fn drive_until_exposed_index(
    sender: &mut LiveSide,
    receiver: &mut LiveSide,
    inbound: DeliveryFlowKey,
    expected: &[u8],
    expected_index: u64,
) {
    loop {
        if let Some(message) = receiver.host.delivery.poll_exposure(inbound).unwrap() {
            assert_eq!(message.accepted_index(), expected_index);
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
