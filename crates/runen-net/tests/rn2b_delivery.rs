use std::collections::VecDeque;
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

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn scope(flows: usize, messages: usize, bytes: usize) -> DeliveryScopeLimits {
    DeliveryScopeLimits::new(nz(flows), nz(messages), nz(bytes))
}

fn reliable_policy(messages: usize, bytes: usize) -> FlowResourcePolicy {
    FlowResourcePolicy::new(
        nz(bytes),
        nz(messages),
        nz(bytes),
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::TerminateReliable,
    )
}

fn unreliable_policy(
    messages: usize,
    bytes: usize,
    outbound: OutboundPressureBehavior,
    receiver: ReceiverPressureBehavior,
) -> FlowResourcePolicy {
    FlowResourcePolicy::new(nz(bytes), nz(messages), nz(bytes), outbound, receiver)
}

fn key(connection: u64, direction: FlowDirection, handle: u64) -> DeliveryFlowKey {
    DeliveryFlowKey::new(
        ConnectionHandle::new(connection),
        direction,
        DeliveryFlowHandle::new(handle),
    )
}

fn accepted_index(outcome: SubmissionOutcome) -> u64 {
    match outcome {
        SubmissionOutcome::Accepted { accepted_index, .. } => accepted_index,
        other => panic!("expected accepted submission, got {other:?}"),
    }
}

fn submit_and_take(
    endpoint: &mut DeliveryEndpoint,
    flow: DeliveryFlowKey,
    payload: &[u8],
) -> DeliveryTransfer {
    let index = accepted_index(endpoint.submit(flow, payload.to_vec()).unwrap());
    endpoint.commit_outbound_custody(flow, index).unwrap()
}

#[derive(Debug)]
struct StagedTransfer {
    source: DeliveryFlowKey,
    target: DeliveryFlowKey,
    transfer: DeliveryTransfer,
}

#[derive(Debug)]
struct FaultStage {
    max_messages: usize,
    max_payload_bytes: usize,
    payload_bytes: usize,
    queue: VecDeque<StagedTransfer>,
}

impl FaultStage {
    fn new(max_messages: usize, max_payload_bytes: usize) -> Self {
        Self {
            max_messages,
            max_payload_bytes,
            payload_bytes: 0,
            queue: VecDeque::new(),
        }
    }

    fn len(&self) -> usize {
        self.queue.len()
    }

    fn can_take(&self, payload_bytes: usize) -> bool {
        self.queue.len() < self.max_messages
            && self
                .payload_bytes
                .checked_add(payload_bytes)
                .is_some_and(|bytes| bytes <= self.max_payload_bytes)
    }

    fn try_take(
        &mut self,
        source: &mut DeliveryEndpoint,
        source_flow: DeliveryFlowKey,
        target_flow: DeliveryFlowKey,
    ) -> bool {
        let Some(preview) = source.peek_outbound(source_flow).unwrap() else {
            return false;
        };
        if !self.can_take(preview.payload_len()) {
            return false;
        }

        let transfer = source
            .commit_outbound_custody(source_flow, preview.accepted_index())
            .unwrap();
        self.payload_bytes += transfer.payload_len();
        self.queue.push_back(StagedTransfer {
            source: source_flow,
            target: target_flow,
            transfer,
        });
        true
    }

    fn duplicate(&mut self, index: usize) -> bool {
        let Some(staged) = self.queue.get(index) else {
            return false;
        };
        if !self.can_take(staged.transfer.payload_len()) {
            return false;
        }
        let duplicate = StagedTransfer {
            source: staged.source,
            target: staged.target,
            transfer: staged.transfer.clone(),
        };
        self.payload_bytes += duplicate.transfer.payload_len();
        self.queue.push_back(duplicate);
        true
    }

    fn swap(&mut self, first: usize, second: usize) {
        self.queue.swap(first, second);
    }

    fn remove(&mut self, index: usize) -> StagedTransfer {
        let staged = self.queue.remove(index).unwrap();
        self.payload_bytes -= staged.transfer.payload_len();
        staged
    }

    fn drop_at(&mut self, index: usize) -> StagedTransfer {
        self.remove(index)
    }

    fn deliver_at(
        &mut self,
        index: usize,
        target: &mut DeliveryEndpoint,
    ) -> ReceiveOutcome {
        let staged = self.remove(index);
        target.receive(staged.target, staged.transfer).unwrap()
    }
}

fn collect_exposed(endpoint: &mut DeliveryEndpoint, flow: DeliveryFlowKey) -> Vec<Vec<u8>> {
    let mut payloads = Vec::new();
    while let Some(message) = endpoint.poll_exposure(flow).unwrap() {
        payloads.push(message.payload().to_vec());
    }
    payloads
}

#[test]
fn flow_policy_and_payload_size_do_not_change_delivery_mode() {
    let aggregate = scope(4, 8, 64);
    let connection = scope(4, 8, 64);
    let flow = key(1, FlowDirection::Outbound, 1);
    let mut endpoint = DeliveryEndpoint::new(aggregate);

    let invalid = FlowResourcePolicy::new(
        nz(8),
        nz(2),
        nz(16),
        OutboundPressureBehavior::EvictOldestUnreliable,
        ReceiverPressureBehavior::TerminateReliable,
    );
    assert_eq!(
        endpoint.establish_flow(flow, DeliveryMode::ReliableOrdered, invalid, connection),
        Err(FlowEstablishmentError::InvalidPolicy(
            DeliveryPolicyError::ReliableOutboundMustRejectNew
        ))
    );

    let policy = reliable_policy(2, 8);
    endpoint
        .establish_flow(flow, DeliveryMode::ReliableOrdered, policy, connection)
        .unwrap();
    assert_eq!(
        endpoint.submit(flow, vec![0; 9]).unwrap(),
        SubmissionOutcome::RejectedTooLarge
    );
    assert_eq!(
        endpoint.flow_contract(flow),
        Some((DeliveryMode::ReliableOrdered, policy))
    );
    assert_eq!(endpoint.pending_messages(), 0);
}

#[test]
fn active_flow_limits_apply_at_connection_and_aggregate_scopes() {
    let mut endpoint = DeliveryEndpoint::new(scope(2, 16, 128));
    let connection = scope(1, 8, 64);
    let policy = reliable_policy(2, 16);

    endpoint
        .establish_flow(
            key(1, FlowDirection::Outbound, 1),
            DeliveryMode::ReliableOrdered,
            policy,
            connection,
        )
        .unwrap();
    assert_eq!(
        endpoint.establish_flow(
            key(1, FlowDirection::Inbound, 2),
            DeliveryMode::ReliableOrdered,
            policy,
            connection,
        ),
        Err(FlowEstablishmentError::ActiveFlowLimitExceeded(
            ResourceScope::Connection
        ))
    );

    endpoint
        .establish_flow(
            key(2, FlowDirection::Outbound, 1),
            DeliveryMode::ReliableOrdered,
            policy,
            connection,
        )
        .unwrap();
    assert_eq!(
        endpoint.establish_flow(
            key(3, FlowDirection::Outbound, 1),
            DeliveryMode::ReliableOrdered,
            policy,
            connection,
        ),
        Err(FlowEstablishmentError::ActiveFlowLimitExceeded(
            ResourceScope::Aggregate
        ))
    );
}

#[test]
fn reliable_reject_new_preserves_prior_acceptance_and_order_index() {
    let flow = key(1, FlowDirection::Outbound, 1);
    let mut endpoint = DeliveryEndpoint::new(scope(2, 8, 64));
    endpoint
        .establish_flow(
            flow,
            DeliveryMode::ReliableOrdered,
            reliable_policy(1, 8),
            scope(2, 8, 64),
        )
        .unwrap();

    assert_eq!(accepted_index(endpoint.submit(flow, b"first".to_vec()).unwrap()), 0);
    assert_eq!(
        endpoint.submit(flow, b"blocked".to_vec()).unwrap(),
        SubmissionOutcome::RejectedPressure
    );
    assert_eq!(endpoint.peek_outbound(flow).unwrap().unwrap().payload(), b"first");
    endpoint.commit_outbound_custody(flow, 0).unwrap();
    assert_eq!(accepted_index(endpoint.submit(flow, b"next".to_vec()).unwrap()), 1);
}

#[test]
fn unreliable_evict_oldest_is_same_flow_fifo_pressure_policy() {
    let flow = key(1, FlowDirection::Outbound, 1);
    let mut endpoint = DeliveryEndpoint::new(scope(2, 8, 64));
    endpoint
        .establish_flow(
            flow,
            DeliveryMode::UnreliableUnordered,
            unreliable_policy(
                2,
                16,
                OutboundPressureBehavior::EvictOldestUnreliable,
                ReceiverPressureBehavior::DropIncomingUnreliable,
            ),
            scope(2, 8, 64),
        )
        .unwrap();

    assert_eq!(accepted_index(endpoint.submit(flow, b"a".to_vec()).unwrap()), 0);
    assert_eq!(accepted_index(endpoint.submit(flow, b"b".to_vec()).unwrap()), 1);
    assert_eq!(
        endpoint.submit(flow, b"c".to_vec()).unwrap(),
        SubmissionOutcome::Accepted {
            accepted_index: 2,
            local_pressure_drops: 1,
        }
    );
    assert_eq!(endpoint.peek_outbound(flow).unwrap().unwrap().payload(), b"b");
    assert_eq!(endpoint.diagnostics().outbound_unreliable_pressure_drops, 1);
    assert_eq!(endpoint.commit_outbound_custody(flow, 1).unwrap().payload(), b"b");
    assert_eq!(endpoint.commit_outbound_custody(flow, 2).unwrap().payload(), b"c");
}

#[test]
fn aggregate_pressure_never_evicts_another_flows_unreliable_data() {
    let first = key(1, FlowDirection::Outbound, 1);
    let second = key(2, FlowDirection::Outbound, 1);
    let mut endpoint = DeliveryEndpoint::new(scope(2, 1, 64));
    let policy = unreliable_policy(
        2,
        16,
        OutboundPressureBehavior::EvictOldestUnreliable,
        ReceiverPressureBehavior::DropIncomingUnreliable,
    );
    endpoint
        .establish_flow(first, DeliveryMode::UnreliableUnordered, policy, scope(2, 4, 64))
        .unwrap();
    endpoint
        .establish_flow(second, DeliveryMode::UnreliableUnordered, policy, scope(2, 4, 64))
        .unwrap();

    endpoint.submit(first, b"keep".to_vec()).unwrap();
    assert_eq!(
        endpoint.submit(second, b"reject".to_vec()).unwrap(),
        SubmissionOutcome::RejectedPressure
    );
    assert_eq!(endpoint.peek_outbound(first).unwrap().unwrap().payload(), b"keep");
    assert!(endpoint.peek_outbound(second).unwrap().is_none());
}

#[test]
fn rejected_unreliable_sequenced_submission_consumes_no_sequence() {
    let flow = key(1, FlowDirection::Outbound, 1);
    let mut endpoint = DeliveryEndpoint::new(scope(2, 8, 64));
    endpoint
        .establish_flow(
            flow,
            DeliveryMode::UnreliableSequenced,
            unreliable_policy(
                1,
                8,
                OutboundPressureBehavior::RejectNew,
                ReceiverPressureBehavior::DropIncomingUnreliable,
            ),
            scope(2, 8, 64),
        )
        .unwrap();

    assert_eq!(accepted_index(endpoint.submit(flow, b"zero".to_vec()).unwrap()), 0);
    assert_eq!(
        endpoint.submit(flow, b"rejected".to_vec()).unwrap(),
        SubmissionOutcome::RejectedPressure
    );
    endpoint.commit_outbound_custody(flow, 0).unwrap();
    assert_eq!(accepted_index(endpoint.submit(flow, b"one".to_vec()).unwrap()), 1);
}

#[test]
fn session_association_accounts_existing_pending_state_without_granting_membership() {
    let first = key(1, FlowDirection::Outbound, 1);
    let second = key(2, FlowDirection::Outbound, 1);
    let mut endpoint = DeliveryEndpoint::new(scope(4, 16, 128));
    let policy = reliable_policy(4, 16);
    endpoint
        .establish_flow(first, DeliveryMode::ReliableOrdered, policy, scope(2, 8, 64))
        .unwrap();
    endpoint.submit(first, b"a".to_vec()).unwrap();
    endpoint.submit(first, b"b".to_vec()).unwrap();

    let session = Session::new(
        SessionId::new(9),
        SessionLimits::new(nz(4), nz(2)).unwrap(),
    );
    assert_eq!(session.live_memberships(), 0);
    assert_eq!(
        endpoint.associate_flow_with_session(first, &session, scope(1, 1, 16)),
        Err(SessionAssociationError::ResourceLimitExceeded)
    );

    let session_scope = scope(1, 2, 16);
    assert_eq!(
        endpoint
            .associate_flow_with_session(first, &session, session_scope)
            .unwrap(),
        SessionAssociationOutcome::Associated
    );
    assert_eq!(
        endpoint
            .associate_flow_with_session(first, &session, session_scope)
            .unwrap(),
        SessionAssociationOutcome::AlreadyAssociated
    );
    assert_eq!(session.live_memberships(), 0);

    endpoint
        .establish_flow(second, DeliveryMode::ReliableOrdered, policy, scope(2, 8, 64))
        .unwrap();
    assert_eq!(
        endpoint.associate_flow_with_session(second, &session, session_scope),
        Err(SessionAssociationError::ResourceLimitExceeded)
    );

    let mut closed = Session::new(
        SessionId::new(10),
        SessionLimits::new(nz(2), nz(1)).unwrap(),
    );
    closed.close();
    assert_eq!(
        endpoint.associate_flow_with_session(second, &closed, scope(2, 4, 32)),
        Err(SessionAssociationError::SessionClosed)
    );
}

#[test]
fn reliable_fault_reorder_and_duplication_expose_exactly_once_in_acceptance_order() {
    let source_flow = key(1, FlowDirection::Outbound, 1);
    let target_flow = key(1, FlowDirection::Inbound, 1);
    let policy = reliable_policy(4, 32);
    let limits = scope(4, 8, 64);
    let mut source = DeliveryEndpoint::new(limits);
    let mut target = DeliveryEndpoint::new(limits);
    source
        .establish_flow(source_flow, DeliveryMode::ReliableOrdered, policy, limits)
        .unwrap();
    target
        .establish_flow(target_flow, DeliveryMode::ReliableOrdered, policy, limits)
        .unwrap();

    source.submit(source_flow, b"a".to_vec()).unwrap();
    source.submit(source_flow, b"b".to_vec()).unwrap();
    let mut stage = FaultStage::new(4, 64);
    assert!(stage.try_take(&mut source, source_flow, target_flow));
    assert!(stage.try_take(&mut source, source_flow, target_flow));
    stage.swap(0, 1);
    assert!(stage.duplicate(0));

    assert!(matches!(stage.deliver_at(0, &mut target), ReceiveOutcome::Buffered { .. }));
    assert_eq!(stage.deliver_at(1, &mut target), ReceiveOutcome::DuplicateReliable);
    assert!(matches!(stage.deliver_at(0, &mut target), ReceiveOutcome::Buffered { .. }));
    assert_eq!(collect_exposed(&mut target, target_flow), vec![b"a".to_vec(), b"b".to_vec()]);
    assert_eq!(target.diagnostics().reliable_duplicate_suppressions, 1);
}

#[test]
fn reliable_receiver_pressure_is_terminal_instead_of_lossy() {
    let source_flow = key(1, FlowDirection::Outbound, 1);
    let target_flow = key(1, FlowDirection::Inbound, 1);
    let limits = scope(4, 8, 64);
    let mut source = DeliveryEndpoint::new(limits);
    let mut target = DeliveryEndpoint::new(limits);
    source
        .establish_flow(
            source_flow,
            DeliveryMode::ReliableOrdered,
            reliable_policy(4, 32),
            limits,
        )
        .unwrap();
    target
        .establish_flow(
            target_flow,
            DeliveryMode::ReliableOrdered,
            reliable_policy(1, 8),
            limits,
        )
        .unwrap();

    source.submit(source_flow, b"a".to_vec()).unwrap();
    source.submit(source_flow, b"b".to_vec()).unwrap();
    let mut stage = FaultStage::new(2, 32);
    stage.try_take(&mut source, source_flow, target_flow);
    stage.try_take(&mut source, source_flow, target_flow);
    stage.swap(0, 1);

    assert!(matches!(stage.deliver_at(0, &mut target), ReceiveOutcome::Buffered { .. }));
    assert_eq!(stage.deliver_at(0, &mut target), ReceiveOutcome::TerminalReliableFailure);
    assert_eq!(
        target.poll_exposure(target_flow),
        Err(DeliveryOperationError::UnknownFlow)
    );
}

#[test]
fn unreliable_unordered_fault_loss_reorder_and_duplication_remain_permitted() {
    let source_flow = key(1, FlowDirection::Outbound, 1);
    let target_flow = key(1, FlowDirection::Inbound, 1);
    let limits = scope(4, 12, 128);
    let policy = unreliable_policy(
        4,
        32,
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::DropIncomingUnreliable,
    );
    let mut source = DeliveryEndpoint::new(limits);
    let mut target = DeliveryEndpoint::new(limits);
    source
        .establish_flow(source_flow, DeliveryMode::UnreliableUnordered, policy, limits)
        .unwrap();
    target
        .establish_flow(target_flow, DeliveryMode::UnreliableUnordered, policy, limits)
        .unwrap();

    for payload in [b"a".as_slice(), b"b".as_slice(), b"c".as_slice()] {
        source.submit(source_flow, payload.to_vec()).unwrap();
    }
    let mut stage = FaultStage::new(5, 128);
    for _ in 0..3 {
        assert!(stage.try_take(&mut source, source_flow, target_flow));
    }
    stage.drop_at(0);
    assert!(stage.duplicate(0));
    stage.swap(0, 1);
    while stage.len() > 0 {
        assert!(matches!(stage.deliver_at(0, &mut target), ReceiveOutcome::Buffered { .. }));
    }

    assert_eq!(collect_exposed(&mut target, target_flow), vec![b"c".to_vec(), b"b".to_vec(), b"b".to_vec()]);
}

#[test]
fn unreliable_sequenced_skips_gaps_and_rejects_stale_after_exposure() {
    let source_flow = key(1, FlowDirection::Outbound, 1);
    let target_flow = key(1, FlowDirection::Inbound, 1);
    let limits = scope(4, 12, 128);
    let policy = unreliable_policy(
        4,
        32,
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::DropIncomingUnreliable,
    );
    let mut source = DeliveryEndpoint::new(limits);
    let mut target = DeliveryEndpoint::new(limits);
    source
        .establish_flow(source_flow, DeliveryMode::UnreliableSequenced, policy, limits)
        .unwrap();
    target
        .establish_flow(target_flow, DeliveryMode::UnreliableSequenced, policy, limits)
        .unwrap();

    for payload in [b"zero".as_slice(), b"one".as_slice(), b"two".as_slice()] {
        source.submit(source_flow, payload.to_vec()).unwrap();
    }
    let mut stage = FaultStage::new(3, 128);
    for _ in 0..3 {
        stage.try_take(&mut source, source_flow, target_flow);
    }
    stage.drop_at(1);
    stage.swap(0, 1);

    assert!(matches!(stage.deliver_at(0, &mut target), ReceiveOutcome::Buffered { .. }));
    let exposed = target.poll_exposure(target_flow).unwrap().unwrap();
    assert_eq!(exposed.accepted_index(), 2);
    assert_eq!(target.last_exposed_sequence(target_flow).unwrap(), Some(2));

    assert_eq!(stage.deliver_at(0, &mut target), ReceiveOutcome::StaleSequenced);
    assert!(target.poll_exposure(target_flow).unwrap().is_none());
}

#[test]
fn unreliable_receiver_pressure_does_not_advance_sequence_before_exposure() {
    let source_flow = key(1, FlowDirection::Outbound, 1);
    let target_flow = key(1, FlowDirection::Inbound, 1);
    let limits = scope(4, 8, 64);
    let source_policy = unreliable_policy(
        2,
        16,
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::DropIncomingUnreliable,
    );
    let target_policy = unreliable_policy(
        1,
        8,
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::DropIncomingUnreliable,
    );
    let mut source = DeliveryEndpoint::new(limits);
    let mut target = DeliveryEndpoint::new(limits);
    source
        .establish_flow(source_flow, DeliveryMode::UnreliableSequenced, source_policy, limits)
        .unwrap();
    target
        .establish_flow(target_flow, DeliveryMode::UnreliableSequenced, target_policy, limits)
        .unwrap();

    source.submit(source_flow, b"zero".to_vec()).unwrap();
    source.submit(source_flow, b"one".to_vec()).unwrap();
    let mut stage = FaultStage::new(2, 32);
    stage.try_take(&mut source, source_flow, target_flow);
    stage.try_take(&mut source, source_flow, target_flow);

    assert!(matches!(stage.deliver_at(0, &mut target), ReceiveOutcome::Buffered { .. }));
    assert_eq!(
        stage.deliver_at(0, &mut target),
        ReceiveOutcome::DroppedByPressure {
            local_pressure_drops: 1
        }
    );
    assert_eq!(target.last_exposed_sequence(target_flow).unwrap(), None);
    assert_eq!(target.poll_exposure(target_flow).unwrap().unwrap().accepted_index(), 0);
    assert_eq!(target.last_exposed_sequence(target_flow).unwrap(), Some(0));
}

#[test]
fn receiver_evict_oldest_drops_before_exposure_without_advancing_watermark() {
    let source_flow = key(1, FlowDirection::Outbound, 1);
    let target_flow = key(1, FlowDirection::Inbound, 1);
    let limits = scope(4, 8, 64);
    let source_policy = unreliable_policy(
        2,
        16,
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::DropIncomingUnreliable,
    );
    let target_policy = unreliable_policy(
        1,
        8,
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::EvictOldestBufferedUnreliable,
    );
    let mut source = DeliveryEndpoint::new(limits);
    let mut target = DeliveryEndpoint::new(limits);
    source
        .establish_flow(source_flow, DeliveryMode::UnreliableSequenced, source_policy, limits)
        .unwrap();
    target
        .establish_flow(target_flow, DeliveryMode::UnreliableSequenced, target_policy, limits)
        .unwrap();

    source.submit(source_flow, b"zero".to_vec()).unwrap();
    source.submit(source_flow, b"one".to_vec()).unwrap();
    let mut stage = FaultStage::new(2, 32);
    stage.try_take(&mut source, source_flow, target_flow);
    stage.try_take(&mut source, source_flow, target_flow);
    stage.deliver_at(0, &mut target);
    assert_eq!(
        stage.deliver_at(0, &mut target),
        ReceiveOutcome::Buffered {
            local_pressure_drops: 1
        }
    );
    assert_eq!(target.last_exposed_sequence(target_flow).unwrap(), None);
    assert_eq!(target.poll_exposure(target_flow).unwrap().unwrap().accepted_index(), 1);
    assert_eq!(target.last_exposed_sequence(target_flow).unwrap(), Some(1));
}

#[test]
fn bounded_downstream_saturation_keeps_reliable_message_in_runennet_custody() {
    let source_flow = key(1, FlowDirection::Outbound, 1);
    let target_flow = key(1, FlowDirection::Inbound, 1);
    let limits = scope(2, 4, 32);
    let mut source = DeliveryEndpoint::new(limits);
    source
        .establish_flow(
            source_flow,
            DeliveryMode::ReliableOrdered,
            reliable_policy(2, 16),
            limits,
        )
        .unwrap();
    source.submit(source_flow, b"held".to_vec()).unwrap();

    let mut saturated = FaultStage::new(0, 0);
    assert!(!saturated.try_take(&mut source, source_flow, target_flow));
    assert_eq!(source.pending_messages(), 1);
    assert_eq!(source.peek_outbound(source_flow).unwrap().unwrap().payload(), b"held");

    let mut available = FaultStage::new(1, 16);
    assert!(available.try_take(&mut source, source_flow, target_flow));
    assert_eq!(source.pending_messages(), 0);
}

#[test]
fn accepted_reliable_downstream_custody_loss_is_terminal() {
    let source_flow = key(1, FlowDirection::Outbound, 1);
    let target_flow = key(1, FlowDirection::Inbound, 1);
    let limits = scope(2, 4, 32);
    let mut source = DeliveryEndpoint::new(limits);
    source
        .establish_flow(
            source_flow,
            DeliveryMode::ReliableOrdered,
            reliable_policy(2, 16),
            limits,
        )
        .unwrap();
    source.submit(source_flow, b"critical".to_vec()).unwrap();

    let mut stage = FaultStage::new(1, 16);
    assert!(stage.try_take(&mut source, source_flow, target_flow));
    stage.drop_at(0);
    let termination = source.fail_reliable_custody(source_flow).unwrap();
    assert_eq!(termination.reason, FlowTerminationReason::ReliableCustodyLost);
    assert!(termination.reliable_obligation_failed);
    assert_eq!(
        source.peek_outbound(source_flow),
        Err(DeliveryOperationError::UnknownFlow)
    );
}

#[test]
fn connection_termination_ends_all_flows_and_replacement_starts_fresh() {
    let connection = ConnectionHandle::new(1);
    let reliable = DeliveryFlowKey::new(connection, FlowDirection::Outbound, DeliveryFlowHandle::new(1));
    let unreliable = DeliveryFlowKey::new(connection, FlowDirection::Outbound, DeliveryFlowHandle::new(2));
    let limits = scope(4, 8, 64);
    let mut endpoint = DeliveryEndpoint::new(limits);
    endpoint
        .establish_flow(
            reliable,
            DeliveryMode::ReliableOrdered,
            reliable_policy(2, 16),
            limits,
        )
        .unwrap();
    endpoint
        .establish_flow(
            unreliable,
            DeliveryMode::UnreliableUnordered,
            unreliable_policy(
                2,
                16,
                OutboundPressureBehavior::RejectNew,
                ReceiverPressureBehavior::DropIncomingUnreliable,
            ),
            limits,
        )
        .unwrap();
    endpoint.submit(reliable, b"reliable".to_vec()).unwrap();
    endpoint.submit(unreliable, b"unreliable".to_vec()).unwrap();

    let terminations = endpoint.terminate_connection(connection);
    assert_eq!(terminations.len(), 2);
    assert_eq!(terminations[0].key, reliable);
    assert!(terminations[0].reliable_obligation_failed);
    assert_eq!(terminations[1].key, unreliable);
    assert!(!terminations[1].reliable_obligation_failed);
    assert_eq!(endpoint.active_flows(), 0);
    assert_eq!(endpoint.pending_messages(), 0);

    let replacement = key(2, FlowDirection::Outbound, 1);
    endpoint
        .establish_flow(
            replacement,
            DeliveryMode::ReliableOrdered,
            reliable_policy(2, 16),
            limits,
        )
        .unwrap();
    assert_eq!(
        accepted_index(endpoint.submit(replacement, b"fresh".to_vec()).unwrap()),
        0
    );
}

#[test]
fn direct_and_faulted_reliable_delivery_have_the_same_exposure_semantics() {
    let limits = scope(4, 8, 64);
    let policy = reliable_policy(4, 32);

    let direct_source_flow = key(1, FlowDirection::Outbound, 1);
    let direct_target_flow = key(1, FlowDirection::Inbound, 1);
    let mut direct_source = DeliveryEndpoint::new(limits);
    let mut direct_target = DeliveryEndpoint::new(limits);
    direct_source
        .establish_flow(
            direct_source_flow,
            DeliveryMode::ReliableOrdered,
            policy,
            limits,
        )
        .unwrap();
    direct_target
        .establish_flow(
            direct_target_flow,
            DeliveryMode::ReliableOrdered,
            policy,
            limits,
        )
        .unwrap();
    for payload in [b"left".as_slice(), b"right".as_slice()] {
        let transfer = submit_and_take(&mut direct_source, direct_source_flow, payload);
        direct_target.receive(direct_target_flow, transfer).unwrap();
    }
    let direct = collect_exposed(&mut direct_target, direct_target_flow);

    let fault_source_flow = key(2, FlowDirection::Outbound, 1);
    let fault_target_flow = key(2, FlowDirection::Inbound, 1);
    let mut fault_source = DeliveryEndpoint::new(limits);
    let mut fault_target = DeliveryEndpoint::new(limits);
    fault_source
        .establish_flow(
            fault_source_flow,
            DeliveryMode::ReliableOrdered,
            policy,
            limits,
        )
        .unwrap();
    fault_target
        .establish_flow(
            fault_target_flow,
            DeliveryMode::ReliableOrdered,
            policy,
            limits,
        )
        .unwrap();
    fault_source.submit(fault_source_flow, b"left".to_vec()).unwrap();
    fault_source.submit(fault_source_flow, b"right".to_vec()).unwrap();
    let mut stage = FaultStage::new(2, 64);
    stage.try_take(&mut fault_source, fault_source_flow, fault_target_flow);
    stage.try_take(&mut fault_source, fault_source_flow, fault_target_flow);
    stage.swap(0, 1);
    stage.deliver_at(0, &mut fault_target);
    stage.deliver_at(0, &mut fault_target);
    let faulted = collect_exposed(&mut fault_target, fault_target_flow);

    assert_eq!(direct, vec![b"left".to_vec(), b"right".to_vec()]);
    assert_eq!(faulted, direct);
}
