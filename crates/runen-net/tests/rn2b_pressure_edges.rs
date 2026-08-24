use std::num::NonZeroUsize;

use runen_net::delivery::{
    DeliveryEndpoint, DeliveryFlowHandle, DeliveryFlowKey, DeliveryMode, DeliveryScopeLimits,
    FlowDirection, FlowResourcePolicy, OutboundPressureBehavior, ReceiveOutcome,
    ReceiverPressureBehavior, SubmissionOutcome,
};
use runen_net::identity::ConnectionHandle;

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn scope(flows: usize, messages: usize, bytes: usize) -> DeliveryScopeLimits {
    DeliveryScopeLimits::new(nz(flows), nz(messages), nz(bytes))
}

fn unreliable(
    pending_messages: usize,
    pending_bytes: usize,
    outbound: OutboundPressureBehavior,
    receiver: ReceiverPressureBehavior,
) -> FlowResourcePolicy {
    FlowResourcePolicy::new(
        nz(pending_bytes),
        nz(pending_messages),
        nz(pending_bytes),
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

fn take(
    source: &mut DeliveryEndpoint,
    source_flow: DeliveryFlowKey,
    payload: &[u8],
) -> runen_net::delivery::DeliveryTransfer {
    let outcome = source.submit(source_flow, payload.to_vec()).unwrap();
    let SubmissionOutcome::Accepted { accepted_index, .. } = outcome else {
        panic!("expected accepted submission, got {outcome:?}");
    };
    source
        .commit_outbound_custody(source_flow, accepted_index)
        .unwrap()
}

#[test]
fn drop_incoming_unreliable_is_distinct_and_does_not_advance_sequence() {
    let limits = scope(4, 8, 64);
    let source_flow = flow(1, FlowDirection::Outbound, 1);
    let target_flow = flow(1, FlowDirection::Inbound, 1);
    let source_policy = unreliable(
        2,
        16,
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::DropIncomingUnreliable,
    );
    let target_policy = unreliable(
        1,
        8,
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::DropIncomingUnreliable,
    );
    let mut source = DeliveryEndpoint::new(limits);
    let mut target = DeliveryEndpoint::new(limits);
    source
        .establish_flow(
            source_flow,
            DeliveryMode::UnreliableSequenced,
            source_policy,
            limits,
        )
        .unwrap();
    target
        .establish_flow(
            target_flow,
            DeliveryMode::UnreliableSequenced,
            target_policy,
            limits,
        )
        .unwrap();

    let first = take(&mut source, source_flow, b"zero");
    let second = take(&mut source, source_flow, b"one");
    assert_eq!(
        target.receive(target_flow, first).unwrap(),
        ReceiveOutcome::Buffered {
            local_pressure_drops: 0
        }
    );
    assert_eq!(
        target.receive(target_flow, second).unwrap(),
        ReceiveOutcome::DroppedByPressure {
            local_pressure_drops: 1
        }
    );
    assert_eq!(target.last_exposed_sequence(target_flow).unwrap(), None);
    assert_eq!(target.diagnostics().inbound_unreliable_pressure_drops, 1);

    let exposed = target.poll_exposure(target_flow).unwrap().unwrap();
    assert_eq!(exposed.accepted_index(), 0);
    assert_eq!(target.last_exposed_sequence(target_flow).unwrap(), Some(0));
}

#[test]
fn connection_pending_pressure_never_evicts_another_flows_data() {
    let aggregate_limits = scope(8, 16, 128);
    let connection_limits = scope(4, 1, 64);
    let first = flow(1, FlowDirection::Outbound, 1);
    let second = flow(1, FlowDirection::Outbound, 2);
    let policy = unreliable(
        2,
        16,
        OutboundPressureBehavior::EvictOldestUnreliable,
        ReceiverPressureBehavior::DropIncomingUnreliable,
    );
    let mut endpoint = DeliveryEndpoint::new(aggregate_limits);
    endpoint
        .establish_flow(
            first,
            DeliveryMode::UnreliableUnordered,
            policy,
            connection_limits,
        )
        .unwrap();
    endpoint
        .establish_flow(
            second,
            DeliveryMode::UnreliableUnordered,
            policy,
            connection_limits,
        )
        .unwrap();

    assert!(matches!(
        endpoint.submit(first, b"keep".to_vec()).unwrap(),
        SubmissionOutcome::Accepted { .. }
    ));
    assert_eq!(
        endpoint.submit(second, b"reject".to_vec()).unwrap(),
        SubmissionOutcome::RejectedPressure
    );
    assert_eq!(endpoint.peek_outbound(first).unwrap().unwrap().payload(), b"keep");
    assert!(endpoint.peek_outbound(second).unwrap().is_none());
    assert_eq!(endpoint.diagnostics().outbound_unreliable_pressure_drops, 0);
}
