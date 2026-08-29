use std::num::NonZeroUsize;

use runen_net::delivery::{
    DeliveryEndpoint, DeliveryFlowHandle, DeliveryFlowKey, DeliveryMode, DeliveryOperationError,
    DeliveryScopeLimits, FlowDirection, FlowResourcePolicy, FlowTerminationReason,
    OutboundPressureBehavior, ReceiveOutcome, ReceiverPressureBehavior, SessionAssociationError,
    SessionAssociationOutcome, SubmissionOutcome,
    adapter::{CustodyCommitError, DeliveryTransfer, DeliveryTransportAdapter},
};
use runen_net::identity::{ConnectionHandle, SessionId};
use runen_net::session::{Session, SessionLimits};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn scope_limits(active: usize, messages: usize, bytes: usize) -> DeliveryScopeLimits {
    DeliveryScopeLimits::new(nz(active), nz(messages), nz(bytes))
}

fn reliable_policy(
    max_message_bytes: usize,
    max_pending_messages: usize,
    max_pending_bytes: usize,
) -> FlowResourcePolicy {
    FlowResourcePolicy::new(
        nz(max_message_bytes),
        nz(max_pending_messages),
        nz(max_pending_bytes),
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::TerminateReliable,
    )
}

fn unreliable_policy(
    max_message_bytes: usize,
    max_pending_messages: usize,
    max_pending_bytes: usize,
    outbound: OutboundPressureBehavior,
    receiver: ReceiverPressureBehavior,
) -> FlowResourcePolicy {
    FlowResourcePolicy::new(
        nz(max_message_bytes),
        nz(max_pending_messages),
        nz(max_pending_bytes),
        outbound,
        receiver,
    )
}

fn flow(connection: u64, direction: FlowDirection, handle: u64) -> DeliveryFlowKey {
    DeliveryFlowKey::new(
        ConnectionHandle::new(connection),
        direction,
        DeliveryFlowHandle::new(handle),
    )
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct AccountingSnapshot {
    active_flows: usize,
    pending_messages: usize,
    pending_bytes: usize,
}

fn accounting(endpoint: &DeliveryEndpoint) -> AccountingSnapshot {
    AccountingSnapshot {
        active_flows: endpoint.active_flows(),
        pending_messages: endpoint.pending_messages(),
        pending_bytes: endpoint.pending_payload_bytes(),
    }
}

fn assert_accounting(endpoint: &DeliveryEndpoint, live_flows: &[DeliveryFlowKey]) {
    let mut messages = 0usize;
    let mut bytes = 0usize;
    for key in live_flows {
        let (flow_messages, flow_bytes) = endpoint
            .flow_pending_usage(*key)
            .expect("modeled live flow must remain observable");
        messages += flow_messages;
        bytes += flow_bytes;
    }

    assert_eq!(endpoint.active_flows(), live_flows.len());
    assert_eq!(endpoint.pending_messages(), messages);
    assert_eq!(endpoint.pending_payload_bytes(), bytes);
}

fn take_transfer(
    sender: &mut DeliveryEndpoint,
    key: DeliveryFlowKey,
    payload: &[u8],
) -> DeliveryTransfer {
    let accepted_index = match sender.submit(key, payload.to_vec()).unwrap() {
        SubmissionOutcome::Accepted { accepted_index, .. } => accepted_index,
        other => panic!("expected accepted transfer, got {other:?}"),
    };
    sender.commit_outbound_custody(key, accepted_index).unwrap()
}

#[test]
fn outbound_sequence_preserves_accounting_across_rejection_custody_and_teardown() {
    let aggregate = scope_limits(4, 4, 16);
    let connection_limits = scope_limits(4, 4, 16);
    let reliable = flow(1, FlowDirection::Outbound, 1);
    let unreliable = flow(1, FlowDirection::Outbound, 2);
    let mut endpoint = DeliveryEndpoint::new(aggregate);

    endpoint
        .establish_flow(
            reliable,
            DeliveryMode::ReliableOrdered,
            reliable_policy(4, 2, 8),
            connection_limits,
        )
        .unwrap();
    endpoint
        .establish_flow(
            unreliable,
            DeliveryMode::UnreliableUnordered,
            unreliable_policy(
                4,
                1,
                4,
                OutboundPressureBehavior::EvictOldestUnreliable,
                ReceiverPressureBehavior::DropIncomingUnreliable,
            ),
            connection_limits,
        )
        .unwrap();
    assert_accounting(&endpoint, &[reliable, unreliable]);

    assert_eq!(
        endpoint.submit(reliable, b"aa".to_vec()).unwrap(),
        SubmissionOutcome::Accepted {
            accepted_index: 0,
            local_pressure_drops: 0,
        }
    );
    assert_eq!(
        endpoint.submit(unreliable, b"bbb".to_vec()).unwrap(),
        SubmissionOutcome::Accepted {
            accepted_index: 0,
            local_pressure_drops: 0,
        }
    );
    assert_accounting(&endpoint, &[reliable, unreliable]);

    let before_rejection = accounting(&endpoint);
    assert_eq!(
        endpoint.submit(reliable, vec![0; 5]).unwrap(),
        SubmissionOutcome::RejectedTooLarge
    );
    assert_eq!(accounting(&endpoint), before_rejection);

    assert_eq!(
        endpoint.submit(unreliable, b"cccc".to_vec()).unwrap(),
        SubmissionOutcome::Accepted {
            accepted_index: 1,
            local_pressure_drops: 1,
        }
    );
    assert_accounting(&endpoint, &[reliable, unreliable]);
    assert_eq!(endpoint.flow_pending_usage(unreliable), Some((1, 4)));

    let before_wrong_commit = accounting(&endpoint);
    assert_eq!(
        endpoint.commit_outbound_custody(reliable, 99),
        Err(CustodyCommitError::NotFront)
    );
    assert_eq!(accounting(&endpoint), before_wrong_commit);
    assert_eq!(
        endpoint.poll_exposure(reliable),
        Err(DeliveryOperationError::WrongDirection)
    );
    assert_eq!(accounting(&endpoint), before_wrong_commit);

    let transfer = endpoint.commit_outbound_custody(reliable, 0).unwrap();
    assert_eq!(transfer.payload(), b"aa");
    assert_accounting(&endpoint, &[reliable, unreliable]);

    let terminated = endpoint
        .terminate_flow(unreliable, FlowTerminationReason::Requested)
        .unwrap();
    assert_eq!(terminated.pending_messages, 1);
    assert!(!terminated.reliable_obligation_failed);
    assert_accounting(&endpoint, &[reliable]);
    assert!(endpoint.flow_pending_usage(unreliable).is_none());

    let connection_terminations = endpoint.terminate_connection(ConnectionHandle::new(1));
    assert_eq!(connection_terminations.len(), 1);
    assert_accounting(&endpoint, &[]);
    assert_eq!(
        endpoint.submit(reliable, b"x".to_vec()),
        Err(DeliveryOperationError::UnknownFlow)
    );
    assert_eq!(
        endpoint.terminate_flow(reliable, FlowTerminationReason::Requested),
        Err(DeliveryOperationError::UnknownFlow)
    );
    assert_accounting(&endpoint, &[]);
}

#[test]
fn inbound_sequence_releases_accounting_on_terminal_pressure_and_unreliable_eviction() {
    let sender_limits = scope_limits(4, 8, 32);
    let receiver_limits = scope_limits(4, 4, 8);
    let reliable_out = flow(10, FlowDirection::Outbound, 1);
    let reliable_in = flow(10, FlowDirection::Inbound, 1);
    let mut sender = DeliveryEndpoint::new(sender_limits);
    let mut receiver = DeliveryEndpoint::new(receiver_limits);

    sender
        .establish_flow(
            reliable_out,
            DeliveryMode::ReliableOrdered,
            reliable_policy(4, 4, 16),
            sender_limits,
        )
        .unwrap();
    receiver
        .establish_flow(
            reliable_in,
            DeliveryMode::ReliableOrdered,
            reliable_policy(4, 2, 2),
            receiver_limits,
        )
        .unwrap();

    let zero = take_transfer(&mut sender, reliable_out, b"0");
    let one = take_transfer(&mut sender, reliable_out, b"1");
    let two = take_transfer(&mut sender, reliable_out, b"2");
    let wrong_mode_probe = two.clone();

    assert_eq!(
        receiver.receive_transfer(reliable_in, one.clone()).unwrap(),
        ReceiveOutcome::Buffered {
            local_pressure_drops: 0,
        }
    );
    assert_accounting(&receiver, &[reliable_in]);
    let before_duplicate = accounting(&receiver);
    assert_eq!(
        receiver.receive_transfer(reliable_in, one).unwrap(),
        ReceiveOutcome::DuplicateReliable
    );
    assert_eq!(accounting(&receiver), before_duplicate);

    assert_eq!(
        receiver.receive_transfer(reliable_in, two).unwrap(),
        ReceiveOutcome::Buffered {
            local_pressure_drops: 0,
        }
    );
    assert_eq!(receiver.flow_pending_usage(reliable_in), Some((2, 2)));
    assert_eq!(
        receiver.receive_transfer(reliable_in, zero).unwrap(),
        ReceiveOutcome::TerminalReliableFailure
    );
    assert_accounting(&receiver, &[]);
    assert!(receiver.flow_pending_usage(reliable_in).is_none());

    let unreliable_out = flow(11, FlowDirection::Outbound, 2);
    let unreliable_in = flow(11, FlowDirection::Inbound, 2);
    sender
        .establish_flow(
            unreliable_out,
            DeliveryMode::UnreliableUnordered,
            unreliable_policy(
                4,
                4,
                16,
                OutboundPressureBehavior::RejectNew,
                ReceiverPressureBehavior::DropIncomingUnreliable,
            ),
            sender_limits,
        )
        .unwrap();
    receiver
        .establish_flow(
            unreliable_in,
            DeliveryMode::UnreliableUnordered,
            unreliable_policy(
                4,
                2,
                2,
                OutboundPressureBehavior::RejectNew,
                ReceiverPressureBehavior::EvictOldestBufferedUnreliable,
            ),
            receiver_limits,
        )
        .unwrap();

    let before_mode_rejection = accounting(&receiver);
    assert_eq!(
        receiver
            .receive_transfer(unreliable_in, wrong_mode_probe)
            .unwrap(),
        ReceiveOutcome::RejectedModeMismatch
    );
    assert_eq!(accounting(&receiver), before_mode_rejection);

    let a = take_transfer(&mut sender, unreliable_out, b"a");
    let b = take_transfer(&mut sender, unreliable_out, b"b");
    let c = take_transfer(&mut sender, unreliable_out, b"c");
    assert_eq!(
        receiver.receive_transfer(unreliable_in, a).unwrap(),
        ReceiveOutcome::Buffered {
            local_pressure_drops: 0,
        }
    );
    assert_eq!(
        receiver.receive_transfer(unreliable_in, b).unwrap(),
        ReceiveOutcome::Buffered {
            local_pressure_drops: 0,
        }
    );
    assert_eq!(
        receiver.receive_transfer(unreliable_in, c).unwrap(),
        ReceiveOutcome::Buffered {
            local_pressure_drops: 1,
        }
    );
    assert_eq!(receiver.flow_pending_usage(unreliable_in), Some((2, 2)));
    assert_accounting(&receiver, &[unreliable_in]);

    assert_eq!(
        receiver
            .poll_exposure(unreliable_in)
            .unwrap()
            .unwrap()
            .payload(),
        b"b"
    );
    assert_accounting(&receiver, &[unreliable_in]);
    assert_eq!(
        receiver
            .poll_exposure(unreliable_in)
            .unwrap()
            .unwrap()
            .payload(),
        b"c"
    );
    assert_accounting(&receiver, &[unreliable_in]);
    assert!(receiver.poll_exposure(unreliable_in).unwrap().is_none());

    receiver
        .terminate_flow(unreliable_in, FlowTerminationReason::Requested)
        .unwrap();
    assert_accounting(&receiver, &[]);
}

#[test]
fn session_scope_capacity_is_released_only_when_associated_flow_ends() {
    let aggregate = scope_limits(4, 8, 32);
    let connection_limits = scope_limits(2, 4, 16);
    let session_delivery_limits = scope_limits(1, 1, 4);
    let flow_a = flow(21, FlowDirection::Outbound, 1);
    let flow_b = flow(22, FlowDirection::Outbound, 1);
    let flow_c = flow(23, FlowDirection::Outbound, 1);
    let policy = reliable_policy(4, 2, 8);
    let mut endpoint = DeliveryEndpoint::new(aggregate);
    let session = Session::new(
        SessionId::new(77),
        SessionLimits::new(nz(4), nz(2)).unwrap(),
    );

    endpoint
        .establish_flow(
            flow_a,
            DeliveryMode::ReliableOrdered,
            policy,
            connection_limits,
        )
        .unwrap();
    endpoint
        .establish_flow(
            flow_b,
            DeliveryMode::ReliableOrdered,
            policy,
            connection_limits,
        )
        .unwrap();
    let accepted_a = match endpoint.submit(flow_a, b"aa".to_vec()).unwrap() {
        SubmissionOutcome::Accepted { accepted_index, .. } => accepted_index,
        other => panic!("expected accepted session-accounted payload, got {other:?}"),
    };

    assert_eq!(
        endpoint
            .associate_flow_with_session(flow_a, &session, session_delivery_limits)
            .unwrap(),
        SessionAssociationOutcome::Associated
    );
    assert_eq!(
        endpoint.associate_flow_with_session(flow_b, &session, session_delivery_limits),
        Err(SessionAssociationError::ResourceLimitExceeded)
    );

    endpoint
        .commit_outbound_custody(flow_a, accepted_a)
        .unwrap();
    assert_eq!(endpoint.flow_pending_usage(flow_a), Some((0, 0)));
    assert_eq!(
        endpoint.associate_flow_with_session(flow_b, &session, session_delivery_limits),
        Err(SessionAssociationError::ResourceLimitExceeded)
    );

    endpoint
        .terminate_flow(flow_a, FlowTerminationReason::Requested)
        .unwrap();
    assert_eq!(
        endpoint
            .associate_flow_with_session(flow_b, &session, session_delivery_limits)
            .unwrap(),
        SessionAssociationOutcome::Associated
    );
    assert_accounting(&endpoint, &[flow_b]);

    assert!(matches!(
        endpoint.submit(flow_b, b"z".to_vec()).unwrap(),
        SubmissionOutcome::Accepted { .. }
    ));
    endpoint.terminate_connection(ConnectionHandle::new(22));
    assert_accounting(&endpoint, &[]);

    endpoint
        .establish_flow(
            flow_c,
            DeliveryMode::ReliableOrdered,
            policy,
            connection_limits,
        )
        .unwrap();
    assert_eq!(
        endpoint
            .associate_flow_with_session(flow_c, &session, session_delivery_limits)
            .unwrap(),
        SessionAssociationOutcome::Associated
    );
    assert_accounting(&endpoint, &[flow_c]);
}
