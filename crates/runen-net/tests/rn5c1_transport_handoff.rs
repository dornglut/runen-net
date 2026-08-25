use std::num::NonZeroUsize;

use runen_net::delivery::{
    DeliveryEndpoint, DeliveryFlowHandle, DeliveryFlowKey, DeliveryMode, DeliveryOperationError,
    DeliveryScopeLimits, FlowDirection, FlowResourcePolicy, OutboundPressureBehavior,
    ReceiveOutcome, ReceiverPressureBehavior, SubmissionOutcome,
};
use runen_net::identity::ConnectionHandle;

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn limits(flows: usize, messages: usize, bytes: usize) -> DeliveryScopeLimits {
    DeliveryScopeLimits::new(nz(flows), nz(messages), nz(bytes))
}

fn reliable_policy(
    max_message_bytes: usize,
    max_pending_messages: usize,
    max_pending_payload_bytes: usize,
) -> FlowResourcePolicy {
    FlowResourcePolicy::new(
        nz(max_message_bytes),
        nz(max_pending_messages),
        nz(max_pending_payload_bytes),
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::TerminateReliable,
    )
}

#[test]
fn outbound_metadata_observes_front_without_transferring_custody() {
    let aggregate = limits(8, 8, 128);
    let connection = limits(4, 8, 128);
    let mut endpoint = DeliveryEndpoint::new(aggregate);
    let key = DeliveryFlowKey::new(
        ConnectionHandle::new(1),
        FlowDirection::Outbound,
        DeliveryFlowHandle::new(10),
    );

    endpoint
        .establish_flow(
            key,
            DeliveryMode::ReliableOrdered,
            reliable_policy(16, 4, 64),
            connection,
        )
        .unwrap();
    assert_eq!(
        endpoint.submit(key, vec![1, 2, 3]).unwrap(),
        SubmissionOutcome::Accepted {
            accepted_index: 0,
            local_pressure_drops: 0,
        }
    );

    let usage_before = (
        endpoint.pending_messages(),
        endpoint.pending_payload_bytes(),
        endpoint.flow_pending_usage(key),
    );
    let metadata = endpoint.peek_outbound_metadata(key).unwrap().unwrap();
    assert_eq!(metadata.mode(), DeliveryMode::ReliableOrdered);
    assert_eq!(metadata.accepted_index(), 0);
    assert_eq!(metadata.payload_len(), 3);
    assert_eq!(
        (
            endpoint.pending_messages(),
            endpoint.pending_payload_bytes(),
            endpoint.flow_pending_usage(key),
        ),
        usage_before
    );

    let transfer = endpoint.commit_outbound_custody(key, 0).unwrap();
    assert_eq!(transfer.payload(), &[1, 2, 3]);
    assert_eq!(endpoint.pending_messages(), 0);
    assert_eq!(endpoint.pending_payload_bytes(), 0);
}

#[test]
fn inbound_transport_ingress_derives_established_mode_and_preserves_exposure() {
    let aggregate = limits(8, 8, 128);
    let connection = limits(4, 8, 128);
    let mut endpoint = DeliveryEndpoint::new(aggregate);
    let key = DeliveryFlowKey::new(
        ConnectionHandle::new(2),
        FlowDirection::Inbound,
        DeliveryFlowHandle::new(20),
    );

    endpoint
        .establish_flow(
            key,
            DeliveryMode::ReliableOrdered,
            reliable_policy(16, 4, 64),
            connection,
        )
        .unwrap();

    assert_eq!(
        endpoint
            .receive_transport_payload(key, 0, b"first".to_vec())
            .unwrap(),
        ReceiveOutcome::Buffered {
            local_pressure_drops: 0,
        }
    );
    let exposed = endpoint.poll_exposure(key).unwrap().unwrap();
    assert_eq!(exposed.accepted_index(), 0);
    assert_eq!(exposed.payload(), b"first");
}

#[test]
fn inbound_transport_ingress_keeps_reliable_pressure_terminal() {
    let aggregate = limits(8, 8, 128);
    let connection = limits(4, 8, 128);
    let mut endpoint = DeliveryEndpoint::new(aggregate);
    let key = DeliveryFlowKey::new(
        ConnectionHandle::new(3),
        FlowDirection::Inbound,
        DeliveryFlowHandle::new(30),
    );

    endpoint
        .establish_flow(
            key,
            DeliveryMode::ReliableOrdered,
            reliable_policy(8, 1, 1),
            connection,
        )
        .unwrap();

    assert_eq!(
        endpoint
            .receive_transport_payload(key, 0, vec![1, 2])
            .unwrap(),
        ReceiveOutcome::TerminalReliableFailure
    );
    assert_eq!(endpoint.flow_contract(key), None);
}

#[test]
fn inbound_transport_ingress_rejects_outbound_direction() {
    let aggregate = limits(8, 8, 128);
    let connection = limits(4, 8, 128);
    let mut endpoint = DeliveryEndpoint::new(aggregate);
    let key = DeliveryFlowKey::new(
        ConnectionHandle::new(4),
        FlowDirection::Outbound,
        DeliveryFlowHandle::new(40),
    );

    endpoint
        .establish_flow(
            key,
            DeliveryMode::ReliableOrdered,
            reliable_policy(8, 1, 8),
            connection,
        )
        .unwrap();

    assert_eq!(
        endpoint.receive_transport_payload(key, 0, vec![1]),
        Err(DeliveryOperationError::WrongDirection)
    );
}
