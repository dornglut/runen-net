use std::num::NonZeroUsize;

use runen_net::delivery::{
    DeliveryEndpoint, DeliveryFlowHandle, DeliveryFlowKey, DeliveryMode, DeliveryOperationError,
    DeliveryScopeLimits, FlowDirection, FlowResourcePolicy, OutboundPressureBehavior,
    ReceiverPressureBehavior, SubmissionOutcome,
};
use runen_net::identity::ConnectionHandle;

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn limits(flows: usize, messages: usize, bytes: usize) -> DeliveryScopeLimits {
    DeliveryScopeLimits::new(nz(flows), nz(messages), nz(bytes))
}

fn unreliable_policy(
    max_message_bytes: usize,
    max_pending_messages: usize,
    max_pending_payload_bytes: usize,
    outbound_pressure: OutboundPressureBehavior,
) -> FlowResourcePolicy {
    FlowResourcePolicy::new(
        nz(max_message_bytes),
        nz(max_pending_messages),
        nz(max_pending_payload_bytes),
        outbound_pressure,
        ReceiverPressureBehavior::DropIncomingUnreliable,
    )
}

fn outbound_key(handle: u64) -> DeliveryFlowKey {
    DeliveryFlowKey::new(
        ConnectionHandle::new(1),
        FlowDirection::Outbound,
        DeliveryFlowHandle::new(handle),
    )
}

#[test]
fn next_outbound_index_is_read_only_and_matches_successful_acceptance() {
    let aggregate = limits(8, 8, 128);
    let connection = limits(4, 8, 128);
    let mut endpoint = DeliveryEndpoint::new(aggregate);
    let key = outbound_key(1);
    endpoint
        .establish_flow(
            key,
            DeliveryMode::UnreliableSequenced,
            unreliable_policy(4, 1, 4, OutboundPressureBehavior::RejectNew),
            connection,
        )
        .unwrap();

    assert_eq!(endpoint.next_outbound_accepted_index(key).unwrap(), Some(0));
    assert_eq!(endpoint.next_outbound_accepted_index(key).unwrap(), Some(0));
    assert_eq!(endpoint.pending_messages(), 0);
    assert_eq!(endpoint.pending_payload_bytes(), 0);

    assert_eq!(
        endpoint.submit(key, vec![1, 2, 3, 4]).unwrap(),
        SubmissionOutcome::Accepted {
            accepted_index: 0,
            local_pressure_drops: 0,
        }
    );
    assert_eq!(endpoint.next_outbound_accepted_index(key).unwrap(), Some(1));
}

#[test]
fn rejected_submissions_do_not_consume_the_candidate() {
    let aggregate = limits(8, 8, 128);
    let connection = limits(4, 8, 128);
    let mut endpoint = DeliveryEndpoint::new(aggregate);
    let key = outbound_key(2);
    endpoint
        .establish_flow(
            key,
            DeliveryMode::UnreliableSequenced,
            unreliable_policy(2, 1, 2, OutboundPressureBehavior::RejectNew),
            connection,
        )
        .unwrap();

    assert_eq!(endpoint.next_outbound_accepted_index(key).unwrap(), Some(0));
    assert_eq!(
        endpoint.submit(key, vec![1, 2, 3]).unwrap(),
        SubmissionOutcome::RejectedTooLarge
    );
    assert_eq!(endpoint.next_outbound_accepted_index(key).unwrap(), Some(0));

    assert!(matches!(
        endpoint.submit(key, vec![1, 2]).unwrap(),
        SubmissionOutcome::Accepted {
            accepted_index: 0,
            ..
        }
    ));
    assert_eq!(endpoint.next_outbound_accepted_index(key).unwrap(), Some(1));
    assert_eq!(
        endpoint.submit(key, vec![3]).unwrap(),
        SubmissionOutcome::RejectedPressure
    );
    assert_eq!(endpoint.next_outbound_accepted_index(key).unwrap(), Some(1));
}

#[test]
fn unreliable_eviction_does_not_rewind_the_next_candidate() {
    let aggregate = limits(8, 8, 128);
    let connection = limits(4, 8, 128);
    let mut endpoint = DeliveryEndpoint::new(aggregate);
    let key = outbound_key(3);
    endpoint
        .establish_flow(
            key,
            DeliveryMode::UnreliableSequenced,
            unreliable_policy(8, 1, 8, OutboundPressureBehavior::EvictOldestUnreliable),
            connection,
        )
        .unwrap();

    assert_eq!(endpoint.next_outbound_accepted_index(key).unwrap(), Some(0));
    assert!(matches!(
        endpoint.submit(key, vec![1]).unwrap(),
        SubmissionOutcome::Accepted {
            accepted_index: 0,
            local_pressure_drops: 0,
        }
    ));
    assert_eq!(endpoint.next_outbound_accepted_index(key).unwrap(), Some(1));
    assert_eq!(
        endpoint.submit(key, vec![2]).unwrap(),
        SubmissionOutcome::Accepted {
            accepted_index: 1,
            local_pressure_drops: 1,
        }
    );
    assert_eq!(endpoint.next_outbound_accepted_index(key).unwrap(), Some(2));
    assert_eq!(
        endpoint
            .peek_outbound_metadata(key)
            .unwrap()
            .unwrap()
            .accepted_index(),
        1
    );
}

#[test]
fn next_outbound_index_rejects_inbound_and_unknown_flows() {
    let aggregate = limits(8, 8, 128);
    let connection = limits(4, 8, 128);
    let mut endpoint = DeliveryEndpoint::new(aggregate);
    let inbound = DeliveryFlowKey::new(
        ConnectionHandle::new(1),
        FlowDirection::Inbound,
        DeliveryFlowHandle::new(4),
    );
    endpoint
        .establish_flow(
            inbound,
            DeliveryMode::UnreliableSequenced,
            unreliable_policy(8, 1, 8, OutboundPressureBehavior::RejectNew),
            connection,
        )
        .unwrap();

    assert_eq!(
        endpoint.next_outbound_accepted_index(inbound),
        Err(DeliveryOperationError::WrongDirection)
    );
    assert_eq!(
        endpoint.next_outbound_accepted_index(outbound_key(404)),
        Err(DeliveryOperationError::UnknownFlow)
    );
}
