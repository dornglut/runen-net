mod support;

use std::num::NonZeroUsize;

use runen_net::delivery::{
    DeliveryEndpoint, DeliveryFlowHandle, DeliveryFlowKey, DeliveryMode, DeliveryOperationError,
    DeliveryPolicyError, DeliveryScopeLimits, DeliveryTransfer, FlowDirection,
    FlowEstablishmentError, FlowResourcePolicy, FlowTerminationReason, OutboundPressureBehavior,
    ReceiveOutcome, ReceiverPressureBehavior, ResourceScope, SessionAssociationError,
    SessionAssociationOutcome, SubmissionOutcome,
};
use runen_net::identity::{ConnectionHandle, SessionId};
use runen_net::session::{Session, SessionLimits};

use support::FaultStage;

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn scope(flows: usize, messages: usize, bytes: usize) -> DeliveryScopeLimits {
    DeliveryScopeLimits::new(nz(flows), nz(messages), nz(bytes))
}

fn reliable(messages: usize, bytes: usize) -> FlowResourcePolicy {
    FlowResourcePolicy::new(
        nz(bytes),
        nz(messages),
        nz(bytes),
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::TerminateReliable,
    )
}

fn unreliable(
    messages: usize,
    bytes: usize,
    outbound: OutboundPressureBehavior,
    receiver: ReceiverPressureBehavior,
) -> FlowResourcePolicy {
    FlowResourcePolicy::new(nz(bytes), nz(messages), nz(bytes), outbound, receiver)
}

fn flow(connection: u64, direction: FlowDirection, handle: u64) -> DeliveryFlowKey {
    DeliveryFlowKey::new(
        ConnectionHandle::new(connection),
        direction,
        DeliveryFlowHandle::new(handle),
    )
}

fn establish(
    endpoint: &mut DeliveryEndpoint,
    flow: DeliveryFlowKey,
    mode: DeliveryMode,
    policy: FlowResourcePolicy,
    limits: DeliveryScopeLimits,
) {
    endpoint.establish_flow(flow, mode, policy, limits).unwrap();
}

fn accepted(outcome: SubmissionOutcome) -> u64 {
    match outcome {
        SubmissionOutcome::Accepted { accepted_index, .. } => accepted_index,
        other => panic!("expected acceptance, got {other:?}"),
    }
}

fn submit_take(
    endpoint: &mut DeliveryEndpoint,
    flow: DeliveryFlowKey,
    payload: &[u8],
) -> DeliveryTransfer {
    let index = accepted(endpoint.submit(flow, payload.to_vec()).unwrap());
    endpoint.commit_outbound_custody(flow, index).unwrap()
}

fn exposed(endpoint: &mut DeliveryEndpoint, flow: DeliveryFlowKey) -> Vec<Vec<u8>> {
    let mut output = Vec::new();
    while let Some(message) = endpoint.poll_exposure(flow).unwrap() {
        output.push(message.payload().to_vec());
    }
    output
}

#[test]
fn fixed_contract_size_and_active_flow_limits_are_enforced() {
    let aggregate = scope(2, 8, 64);
    let connection = scope(1, 8, 64);
    let key = flow(1, FlowDirection::Outbound, 1);
    let mut endpoint = DeliveryEndpoint::new(aggregate);

    let invalid = FlowResourcePolicy::new(
        nz(8),
        nz(2),
        nz(16),
        OutboundPressureBehavior::EvictOldestUnreliable,
        ReceiverPressureBehavior::TerminateReliable,
    );
    let result = endpoint.establish_flow(key, DeliveryMode::ReliableOrdered, invalid, connection);
    assert_eq!(
        result,
        Err(FlowEstablishmentError::InvalidPolicy(
            DeliveryPolicyError::ReliableOutboundMustRejectNew
        ))
    );

    let policy = reliable(2, 8);
    establish(
        &mut endpoint,
        key,
        DeliveryMode::ReliableOrdered,
        policy,
        connection,
    );
    let too_large = endpoint.submit(key, vec![0; 9]).unwrap();
    assert_eq!(too_large, SubmissionOutcome::RejectedTooLarge);
    assert_eq!(
        endpoint.flow_contract(key),
        Some((DeliveryMode::ReliableOrdered, policy))
    );

    let same_connection = flow(1, FlowDirection::Inbound, 2);
    let result = endpoint.establish_flow(
        same_connection,
        DeliveryMode::ReliableOrdered,
        policy,
        connection,
    );
    assert_eq!(
        result,
        Err(FlowEstablishmentError::ActiveFlowLimitExceeded(
            ResourceScope::Connection
        ))
    );

    establish(
        &mut endpoint,
        flow(2, FlowDirection::Outbound, 1),
        DeliveryMode::ReliableOrdered,
        policy,
        connection,
    );
    let result = endpoint.establish_flow(
        flow(3, FlowDirection::Outbound, 1),
        DeliveryMode::ReliableOrdered,
        policy,
        connection,
    );
    assert_eq!(
        result,
        Err(FlowEstablishmentError::ActiveFlowLimitExceeded(
            ResourceScope::Aggregate
        ))
    );
}

#[test]
fn outbound_pressure_preserves_reliable_and_same_flow_unreliable_rules() {
    let limits = scope(4, 2, 64);
    let reliable_flow = flow(1, FlowDirection::Outbound, 1);
    let unreliable_flow = flow(2, FlowDirection::Outbound, 1);
    let other_flow = flow(3, FlowDirection::Outbound, 1);
    let mut endpoint = DeliveryEndpoint::new(limits);

    establish(
        &mut endpoint,
        reliable_flow,
        DeliveryMode::ReliableOrdered,
        reliable(1, 8),
        scope(2, 8, 64),
    );
    assert_eq!(
        accepted(endpoint.submit(reliable_flow, b"first".to_vec()).unwrap()),
        0
    );
    let blocked = endpoint.submit(reliable_flow, b"blocked".to_vec()).unwrap();
    assert_eq!(blocked, SubmissionOutcome::RejectedPressure);
    endpoint.commit_outbound_custody(reliable_flow, 0).unwrap();
    assert_eq!(
        accepted(endpoint.submit(reliable_flow, b"next".to_vec()).unwrap()),
        1
    );
    endpoint.commit_outbound_custody(reliable_flow, 1).unwrap();

    let policy = unreliable(
        2,
        16,
        OutboundPressureBehavior::EvictOldestUnreliable,
        ReceiverPressureBehavior::DropIncomingUnreliable,
    );
    establish(
        &mut endpoint,
        unreliable_flow,
        DeliveryMode::UnreliableUnordered,
        policy,
        scope(2, 8, 64),
    );
    endpoint.submit(unreliable_flow, b"a".to_vec()).unwrap();
    endpoint.submit(unreliable_flow, b"b".to_vec()).unwrap();
    let third = endpoint.submit(unreliable_flow, b"c".to_vec()).unwrap();
    assert_eq!(
        third,
        SubmissionOutcome::Accepted {
            accepted_index: 2,
            local_pressure_drops: 1,
        }
    );
    let front = endpoint.peek_outbound(unreliable_flow).unwrap().unwrap();
    assert_eq!(front.accepted_index(), 1);
    assert_eq!(front.payload(), b"b");

    establish(
        &mut endpoint,
        other_flow,
        DeliveryMode::UnreliableUnordered,
        policy,
        scope(2, 8, 64),
    );
    let rejected = endpoint.submit(other_flow, b"other".to_vec()).unwrap();
    assert_eq!(rejected, SubmissionOutcome::RejectedPressure);
    let front = endpoint.peek_outbound(unreliable_flow).unwrap().unwrap();
    assert_eq!(front.payload(), b"b");
}

#[test]
fn rejected_sequenced_submission_consumes_no_sequence() {
    let key = flow(1, FlowDirection::Outbound, 1);
    let mut endpoint = DeliveryEndpoint::new(scope(2, 8, 64));
    let policy = unreliable(
        1,
        8,
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::DropIncomingUnreliable,
    );
    establish(
        &mut endpoint,
        key,
        DeliveryMode::UnreliableSequenced,
        policy,
        scope(2, 8, 64),
    );

    assert_eq!(accepted(endpoint.submit(key, b"zero".to_vec()).unwrap()), 0);
    let rejected = endpoint.submit(key, b"reject".to_vec()).unwrap();
    assert_eq!(rejected, SubmissionOutcome::RejectedPressure);
    endpoint.commit_outbound_custody(key, 0).unwrap();
    assert_eq!(accepted(endpoint.submit(key, b"one".to_vec()).unwrap()), 1);
}

#[test]
fn session_resource_association_accounts_existing_state_only() {
    let limits = scope(4, 16, 128);
    let first = flow(1, FlowDirection::Outbound, 1);
    let second = flow(2, FlowDirection::Outbound, 1);
    let mut endpoint = DeliveryEndpoint::new(limits);
    let policy = reliable(4, 16);
    establish(
        &mut endpoint,
        first,
        DeliveryMode::ReliableOrdered,
        policy,
        scope(2, 8, 64),
    );
    endpoint.submit(first, b"a".to_vec()).unwrap();
    endpoint.submit(first, b"b".to_vec()).unwrap();

    let session = Session::new(SessionId::new(9), SessionLimits::new(nz(4), nz(2)).unwrap());
    assert_eq!(session.live_memberships(), 0);
    let too_small = endpoint.associate_flow_with_session(first, &session, scope(1, 1, 16));
    assert_eq!(
        too_small,
        Err(SessionAssociationError::ResourceLimitExceeded)
    );

    let session_limits = scope(1, 2, 16);
    let associated = endpoint
        .associate_flow_with_session(first, &session, session_limits)
        .unwrap();
    assert_eq!(associated, SessionAssociationOutcome::Associated);
    assert_eq!(session.live_memberships(), 0);

    establish(
        &mut endpoint,
        second,
        DeliveryMode::ReliableOrdered,
        policy,
        scope(2, 8, 64),
    );
    let exhausted = endpoint.associate_flow_with_session(second, &session, session_limits);
    assert_eq!(
        exhausted,
        Err(SessionAssociationError::ResourceLimitExceeded)
    );

    let mut closed = Session::new(
        SessionId::new(10),
        SessionLimits::new(nz(2), nz(1)).unwrap(),
    );
    closed.close();
    let closed_result = endpoint.associate_flow_with_session(second, &closed, scope(2, 4, 32));
    assert_eq!(closed_result, Err(SessionAssociationError::SessionClosed));
}

#[test]
fn reliable_reorder_and_duplication_expose_exactly_once_in_order() {
    let limits = scope(4, 8, 64);
    let policy = reliable(4, 32);
    let source_flow = flow(1, FlowDirection::Outbound, 1);
    let target_flow = flow(1, FlowDirection::Inbound, 1);
    let mut source = DeliveryEndpoint::new(limits);
    let mut target = DeliveryEndpoint::new(limits);
    establish(
        &mut source,
        source_flow,
        DeliveryMode::ReliableOrdered,
        policy,
        limits,
    );
    establish(
        &mut target,
        target_flow,
        DeliveryMode::ReliableOrdered,
        policy,
        limits,
    );

    source.submit(source_flow, b"a".to_vec()).unwrap();
    source.submit(source_flow, b"b".to_vec()).unwrap();
    let mut stage = FaultStage::new(4, 64);
    assert!(stage.take(&mut source, source_flow, target_flow));
    assert!(stage.take(&mut source, source_flow, target_flow));
    stage.swap(0, 1);
    assert!(stage.duplicate(0));

    let first = stage.deliver(0, &mut target);
    assert_eq!(
        first,
        ReceiveOutcome::Buffered {
            local_pressure_drops: 0
        }
    );
    let duplicate = stage.deliver(1, &mut target);
    assert_eq!(duplicate, ReceiveOutcome::DuplicateReliable);
    let second = stage.deliver(0, &mut target);
    assert_eq!(
        second,
        ReceiveOutcome::Buffered {
            local_pressure_drops: 0
        }
    );
    assert_eq!(
        exposed(&mut target, target_flow),
        vec![b"a".to_vec(), b"b".to_vec()]
    );
    assert_eq!(target.diagnostics().reliable_duplicate_suppressions, 1);
}

#[test]
fn reliable_receiver_pressure_and_custody_loss_are_terminal() {
    let limits = scope(4, 8, 64);
    let source_flow = flow(1, FlowDirection::Outbound, 1);
    let target_flow = flow(1, FlowDirection::Inbound, 1);
    let mut source = DeliveryEndpoint::new(limits);
    let mut target = DeliveryEndpoint::new(limits);
    establish(
        &mut source,
        source_flow,
        DeliveryMode::ReliableOrdered,
        reliable(4, 32),
        limits,
    );
    establish(
        &mut target,
        target_flow,
        DeliveryMode::ReliableOrdered,
        reliable(1, 8),
        limits,
    );

    source.submit(source_flow, b"a".to_vec()).unwrap();
    source.submit(source_flow, b"b".to_vec()).unwrap();
    let mut stage = FaultStage::new(2, 32);
    stage.take(&mut source, source_flow, target_flow);
    stage.take(&mut source, source_flow, target_flow);
    stage.swap(0, 1);
    let buffered = stage.deliver(0, &mut target);
    assert_eq!(
        buffered,
        ReceiveOutcome::Buffered {
            local_pressure_drops: 0
        }
    );
    let terminal = stage.deliver(0, &mut target);
    assert_eq!(terminal, ReceiveOutcome::TerminalReliableFailure);
    assert_eq!(
        target.poll_exposure(target_flow),
        Err(DeliveryOperationError::UnknownFlow)
    );

    let custody_flow = flow(2, FlowDirection::Outbound, 1);
    establish(
        &mut source,
        custody_flow,
        DeliveryMode::ReliableOrdered,
        reliable(2, 16),
        limits,
    );
    source.submit(custody_flow, b"critical".to_vec()).unwrap();
    let mut custody_stage = FaultStage::new(1, 16);
    assert!(custody_stage.take(&mut source, custody_flow, target_flow));
    custody_stage.drop_at(0);
    let termination = source.fail_reliable_custody(custody_flow).unwrap();
    assert_eq!(
        termination.reason,
        FlowTerminationReason::ReliableCustodyLost
    );
    assert!(termination.reliable_obligation_failed);
}

#[test]
fn unreliable_faults_preserve_unordered_and_sequenced_semantics() {
    let limits = scope(6, 16, 128);
    let unordered_source = flow(1, FlowDirection::Outbound, 1);
    let unordered_target = flow(1, FlowDirection::Inbound, 1);
    let unordered_policy = unreliable(
        4,
        32,
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::DropIncomingUnreliable,
    );
    let mut source = DeliveryEndpoint::new(limits);
    let mut target = DeliveryEndpoint::new(limits);
    establish(
        &mut source,
        unordered_source,
        DeliveryMode::UnreliableUnordered,
        unordered_policy,
        limits,
    );
    establish(
        &mut target,
        unordered_target,
        DeliveryMode::UnreliableUnordered,
        unordered_policy,
        limits,
    );

    for payload in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
        source.submit(unordered_source, payload.to_vec()).unwrap();
    }
    let mut stage = FaultStage::new(5, 128);
    for _ in 0..3 {
        stage.take(&mut source, unordered_source, unordered_target);
    }
    stage.drop_at(0);
    assert!(stage.duplicate(0));
    stage.swap(0, 1);
    while stage.len() > 0 {
        let outcome = stage.deliver(0, &mut target);
        assert_eq!(
            outcome,
            ReceiveOutcome::Buffered {
                local_pressure_drops: 0
            }
        );
    }
    assert_eq!(
        exposed(&mut target, unordered_target),
        vec![b"c".to_vec(), b"b".to_vec(), b"b".to_vec()]
    );

    let sequenced_source = flow(2, FlowDirection::Outbound, 1);
    let sequenced_target = flow(2, FlowDirection::Inbound, 1);
    establish(
        &mut source,
        sequenced_source,
        DeliveryMode::UnreliableSequenced,
        unordered_policy,
        limits,
    );
    establish(
        &mut target,
        sequenced_target,
        DeliveryMode::UnreliableSequenced,
        unordered_policy,
        limits,
    );
    for payload in [b"zero".as_slice(), b"one".as_slice(), b"two".as_slice()] {
        source.submit(sequenced_source, payload.to_vec()).unwrap();
    }
    let mut stage = FaultStage::new(3, 128);
    for _ in 0..3 {
        stage.take(&mut source, sequenced_source, sequenced_target);
    }
    stage.drop_at(1);
    stage.swap(0, 1);
    stage.deliver(0, &mut target);
    let newest = target.poll_exposure(sequenced_target).unwrap().unwrap();
    assert_eq!(newest.accepted_index(), 2);
    let stale = stage.deliver(0, &mut target);
    assert_eq!(stale, ReceiveOutcome::StaleSequenced);
    assert_eq!(
        target.last_exposed_sequence(sequenced_target).unwrap(),
        Some(2)
    );
}

#[test]
fn sequenced_receiver_pressure_does_not_advance_before_exposure() {
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
        ReceiverPressureBehavior::EvictOldestBufferedUnreliable,
    );
    let mut source = DeliveryEndpoint::new(limits);
    let mut target = DeliveryEndpoint::new(limits);
    establish(
        &mut source,
        source_flow,
        DeliveryMode::UnreliableSequenced,
        source_policy,
        limits,
    );
    establish(
        &mut target,
        target_flow,
        DeliveryMode::UnreliableSequenced,
        target_policy,
        limits,
    );

    source.submit(source_flow, b"zero".to_vec()).unwrap();
    source.submit(source_flow, b"one".to_vec()).unwrap();
    let mut stage = FaultStage::new(2, 32);
    stage.take(&mut source, source_flow, target_flow);
    stage.take(&mut source, source_flow, target_flow);
    stage.deliver(0, &mut target);
    let second = stage.deliver(0, &mut target);
    assert_eq!(
        second,
        ReceiveOutcome::Buffered {
            local_pressure_drops: 1
        }
    );
    assert_eq!(target.last_exposed_sequence(target_flow).unwrap(), None);
    let exposed = target.poll_exposure(target_flow).unwrap().unwrap();
    assert_eq!(exposed.accepted_index(), 1);
    assert_eq!(target.last_exposed_sequence(target_flow).unwrap(), Some(1));
}

#[test]
fn bounded_stage_saturation_retains_reliable_custody() {
    let limits = scope(2, 4, 32);
    let source_flow = flow(1, FlowDirection::Outbound, 1);
    let target_flow = flow(1, FlowDirection::Inbound, 1);
    let mut source = DeliveryEndpoint::new(limits);
    establish(
        &mut source,
        source_flow,
        DeliveryMode::ReliableOrdered,
        reliable(2, 16),
        limits,
    );
    source.submit(source_flow, b"held".to_vec()).unwrap();

    let mut saturated = FaultStage::new(0, 0);
    assert!(!saturated.take(&mut source, source_flow, target_flow));
    assert_eq!(source.pending_messages(), 1);
    let pending = source.peek_outbound(source_flow).unwrap().unwrap();
    assert_eq!(pending.payload(), b"held");

    let mut available = FaultStage::new(1, 16);
    assert!(available.take(&mut source, source_flow, target_flow));
    assert_eq!(source.pending_messages(), 0);
}

#[test]
fn connection_replacement_starts_fresh_delivery_state() {
    let limits = scope(4, 8, 64);
    let connection = ConnectionHandle::new(1);
    let reliable_flow = DeliveryFlowKey::new(
        connection,
        FlowDirection::Outbound,
        DeliveryFlowHandle::new(1),
    );
    let unreliable_flow = DeliveryFlowKey::new(
        connection,
        FlowDirection::Outbound,
        DeliveryFlowHandle::new(2),
    );
    let mut endpoint = DeliveryEndpoint::new(limits);
    establish(
        &mut endpoint,
        reliable_flow,
        DeliveryMode::ReliableOrdered,
        reliable(2, 16),
        limits,
    );
    establish(
        &mut endpoint,
        unreliable_flow,
        DeliveryMode::UnreliableUnordered,
        unreliable(
            2,
            16,
            OutboundPressureBehavior::RejectNew,
            ReceiverPressureBehavior::DropIncomingUnreliable,
        ),
        limits,
    );
    endpoint
        .submit(reliable_flow, b"reliable".to_vec())
        .unwrap();
    endpoint
        .submit(unreliable_flow, b"unreliable".to_vec())
        .unwrap();

    let terminations = endpoint.terminate_connection(connection);
    assert_eq!(terminations.len(), 2);
    assert_eq!(terminations[0].key, reliable_flow);
    assert!(terminations[0].reliable_obligation_failed);
    assert_eq!(terminations[1].key, unreliable_flow);
    assert!(!terminations[1].reliable_obligation_failed);
    assert_eq!(endpoint.pending_messages(), 0);

    let replacement = flow(2, FlowDirection::Outbound, 1);
    establish(
        &mut endpoint,
        replacement,
        DeliveryMode::ReliableOrdered,
        reliable(2, 16),
        limits,
    );
    let first = endpoint.submit(replacement, b"fresh".to_vec()).unwrap();
    assert_eq!(accepted(first), 0);
}

#[test]
fn direct_and_faulted_reliable_paths_have_identical_exposure() {
    let limits = scope(4, 8, 64);
    let policy = reliable(4, 32);

    let direct_source_flow = flow(1, FlowDirection::Outbound, 1);
    let direct_target_flow = flow(1, FlowDirection::Inbound, 1);
    let mut direct_source = DeliveryEndpoint::new(limits);
    let mut direct_target = DeliveryEndpoint::new(limits);
    establish(
        &mut direct_source,
        direct_source_flow,
        DeliveryMode::ReliableOrdered,
        policy,
        limits,
    );
    establish(
        &mut direct_target,
        direct_target_flow,
        DeliveryMode::ReliableOrdered,
        policy,
        limits,
    );
    for payload in [b"left".as_slice(), b"right".as_slice()] {
        let transfer = submit_take(&mut direct_source, direct_source_flow, payload);
        direct_target.receive(direct_target_flow, transfer).unwrap();
    }
    let direct = exposed(&mut direct_target, direct_target_flow);

    let fault_source_flow = flow(2, FlowDirection::Outbound, 1);
    let fault_target_flow = flow(2, FlowDirection::Inbound, 1);
    let mut fault_source = DeliveryEndpoint::new(limits);
    let mut fault_target = DeliveryEndpoint::new(limits);
    establish(
        &mut fault_source,
        fault_source_flow,
        DeliveryMode::ReliableOrdered,
        policy,
        limits,
    );
    establish(
        &mut fault_target,
        fault_target_flow,
        DeliveryMode::ReliableOrdered,
        policy,
        limits,
    );
    fault_source
        .submit(fault_source_flow, b"left".to_vec())
        .unwrap();
    fault_source
        .submit(fault_source_flow, b"right".to_vec())
        .unwrap();
    let mut stage = FaultStage::new(2, 64);
    stage.take(&mut fault_source, fault_source_flow, fault_target_flow);
    stage.take(&mut fault_source, fault_source_flow, fault_target_flow);
    stage.swap(0, 1);
    stage.deliver(0, &mut fault_target);
    stage.deliver(0, &mut fault_target);
    let faulted = exposed(&mut fault_target, fault_target_flow);

    assert_eq!(direct, vec![b"left".to_vec(), b"right".to_vec()]);
    assert_eq!(faulted, direct);
}
