use std::collections::BTreeMap;
use std::num::{NonZeroU64, NonZeroUsize};

use runen_net::DeliveryAcceptance;
use runen_net::delivery::{
    DeliveryEndpoint, DeliveryFlowHandle, DeliveryFlowKey, DeliveryMode, DeliveryScopeLimits,
    FlowDirection, FlowResourcePolicy, OutboundPressureBehavior, ReceiverPressureBehavior,
    SubmissionOutcome,
};
use runen_net::identity::{ConnectionHandle, ParticipantId, SessionId, SimulationTick};
use runen_net::protocol::{
    CompatibilityOffer, NegotiatedContract, NegotiationManager, NegotiationManagerLimits,
    NegotiationRequirements, OfferLimits, ProtocolContract, ProtocolId, ProtocolRevision,
};
use runen_net::replication::{
    AccountedState, AuthorityAckOutcome, AuthorityAggregateLimits, AuthorityRecoveryReason,
    AuthorityReplicationSession, AuthorityReplicationState, AuthoritySessionError,
    ClientAggregateLimits, ClientRecoveryReason, ClientReplicationSet, ClientReplicationState,
    ClientSnapshotOutcome, DeltaSnapshot, FullSnapshot, ReplicationCursor, ReplicationLineageKey,
    ReplicationRetentionLimits,
};
use runen_net::session::{RecoveryDuration, RetentionPolicy, Session, SessionLimits};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn retention(evidence: usize) -> ReplicationRetentionLimits {
    ReplicationRetentionLimits::new(nz(64), nz(8), nz(256), nz(128), nz(evidence)).unwrap()
}

fn client_limits(images: usize, bytes: usize) -> ClientAggregateLimits {
    ClientAggregateLimits::new(nz(8), nz(images), nz(bytes))
}

fn authority_limits() -> AuthorityAggregateLimits {
    AuthorityAggregateLimits::new(nz(8), nz(1024), nz(32), nz(512), nz(32))
}

fn state(value: i32) -> BTreeMap<&'static str, i32> {
    BTreeMap::from([("value", value)])
}

fn image(value: i32) -> AccountedState<BTreeMap<&'static str, i32>> {
    AccountedState::new(state(value), 8)
}

fn protocol() -> ProtocolContract {
    ProtocolContract::new(ProtocolId::new(1), ProtocolRevision::new(1))
}

fn negotiation_manager() -> NegotiationManager {
    NegotiationManager::new(OfferLimits::default(), NegotiationManagerLimits::default()).unwrap()
}

fn establish(manager: &mut NegotiationManager, connection: ConnectionHandle) {
    let offer = CompatibilityOffer::new(vec![protocol()], vec![], vec![], None);
    manager.start(connection, offer.clone(), offer).unwrap();
    manager
        .propose(
            connection,
            NegotiatedContract::new(protocol()),
            &NegotiationRequirements::default(),
        )
        .unwrap();
    manager.validate_authority(connection).unwrap();
    manager.validate_peer(connection).unwrap();
}

fn authorized_session(
    session_id: SessionId,
    participant: ParticipantId,
    connection: ConnectionHandle,
) -> Session {
    let mut manager = negotiation_manager();
    establish(&mut manager, connection);
    let mut session = Session::new(session_id, SessionLimits::new(nz(8), nz(4)).unwrap());
    session
        .admit_new(participant, manager.established(connection).unwrap())
        .unwrap();
    session
}

fn accepted() -> DeliveryAcceptance {
    DeliveryAcceptance::Accepted
}

fn emit_full(
    authority: &mut AuthorityReplicationSession<BTreeMap<&'static str, i32>, ()>,
    participant: ParticipantId,
    cursor: u64,
    tick: u64,
    value: i32,
    recovery: bool,
) {
    authority
        .prepare_full(
            participant,
            FullSnapshot::new(
                ReplicationCursor::new(cursor),
                SimulationTick::new(tick),
                image(value),
            ),
            recovery,
        )
        .unwrap();
    authority
        .record_delivery_acceptance(participant, accepted())
        .unwrap()
        .expect("accepted snapshot is emitted");
}

fn emit_delta(
    authority: &mut AuthorityReplicationSession<BTreeMap<&'static str, i32>, ()>,
    participant: ParticipantId,
    cursor: u64,
    tick: u64,
    value: i32,
) {
    authority
        .prepare_delta(
            participant,
            ReplicationCursor::new(cursor),
            SimulationTick::new(tick),
            image(value),
            (),
            0,
        )
        .unwrap();
    authority
        .record_delivery_acceptance(participant, accepted())
        .unwrap()
        .expect("accepted snapshot is emitted");
}

#[test]
fn historical_delta_uses_exact_session_scoped_baseline() {
    let participant = ParticipantId::new(7);
    let first = ReplicationLineageKey::new(SessionId::new(1), participant);
    let second = ReplicationLineageKey::new(SessionId::new(2), participant);
    let mut client = ClientReplicationSet::new(client_limits(8, 256));
    client.add_lineage(first, retention(8)).unwrap();
    client.add_lineage(second, retention(8)).unwrap();

    client
        .apply_full(
            first,
            FullSnapshot::new(ReplicationCursor::new(1), SimulationTick::new(1), image(1)),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();
    let mut current_three = state(3);
    current_three.insert("current_only", 99);
    client
        .apply_full(
            first,
            FullSnapshot::new(
                ReplicationCursor::new(3),
                SimulationTick::new(3),
                AccountedState::new(current_three, 16),
            ),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();
    client
        .apply_full(
            second,
            FullSnapshot::new(ReplicationCursor::new(1), SimulationTick::new(1), image(20)),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();

    let outcome = client
        .apply_delta(
            first,
            DeltaSnapshot::new(
                ReplicationCursor::new(1),
                ReplicationCursor::new(4),
                SimulationTick::new(4),
                (),
            ),
            |base, _, _budget| {
                assert_eq!(base.get("value"), Some(&1));
                assert!(!base.contains_key("current_only"));
                let mut target = base.clone();
                target.insert("value", 4);
                Ok(AccountedState::new(target, 8))
            },
            |_| Ok::<_, ()>(()),
        )
        .unwrap();

    assert_eq!(
        outcome,
        ClientSnapshotOutcome::Committed(ReplicationCursor::new(4))
    );
    let first_state = client.lineage(first).unwrap().current_state().unwrap();
    assert_eq!(first_state.get("value"), Some(&4));
    assert!(!first_state.contains_key("current_only"));
    assert_eq!(
        client
            .lineage(second)
            .unwrap()
            .current_state()
            .unwrap()
            .get("value"),
        Some(&20)
    );
}

#[test]
fn client_recovery_is_persistent_and_atomic() {
    let key = ReplicationLineageKey::new(SessionId::new(1), ParticipantId::new(1));
    let mut client = ClientReplicationSet::new(client_limits(8, 256));
    client.add_lineage(key, retention(8)).unwrap();
    client
        .apply_full(
            key,
            FullSnapshot::new(ReplicationCursor::new(1), SimulationTick::new(1), image(1)),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();
    client
        .apply_full(
            key,
            FullSnapshot::new(ReplicationCursor::new(3), SimulationTick::new(3), image(3)),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();
    client
        .evict_historical(key, ReplicationCursor::new(1))
        .unwrap();

    let missing = client
        .apply_delta(
            key,
            DeltaSnapshot::new(
                ReplicationCursor::new(1),
                ReplicationCursor::new(4),
                SimulationTick::new(4),
                (),
            ),
            |_, _, _| Ok(image(4)),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();
    assert_eq!(missing, ClientSnapshotOutcome::MissingBase);
    assert_eq!(
        client.lineage(key).unwrap().replication_state(),
        ClientReplicationState::FullSnapshotRequired(ClientRecoveryReason::MissingBase)
    );

    let blocked = client
        .apply_delta(
            key,
            DeltaSnapshot::new(
                ReplicationCursor::new(3),
                ReplicationCursor::new(5),
                SimulationTick::new(5),
                (),
            ),
            |_, _, _| Ok(image(5)),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();
    assert_eq!(blocked, ClientSnapshotOutcome::DeltaBlockedByRecovery);
    assert_eq!(
        client.lineage(key).unwrap().replication_state(),
        ClientReplicationState::FullSnapshotRequired(ClientRecoveryReason::MissingBase)
    );

    let invalid_full = client
        .apply_full(
            key,
            FullSnapshot::new(ReplicationCursor::new(4), SimulationTick::new(2), image(4)),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();
    assert_eq!(invalid_full, ClientSnapshotOutcome::TickRegression);
    assert_eq!(
        client.lineage(key).unwrap().current_cursor(),
        Some(ReplicationCursor::new(3))
    );
    assert_eq!(
        client.lineage(key).unwrap().current_state().unwrap(),
        &state(3)
    );

    let recovered = client
        .apply_full(
            key,
            FullSnapshot::new(ReplicationCursor::new(4), SimulationTick::new(4), image(4)),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();
    assert_eq!(
        recovered,
        ClientSnapshotOutcome::Committed(ReplicationCursor::new(4))
    );
    assert_eq!(
        client.lineage(key).unwrap().replication_state(),
        ClientReplicationState::Synchronized
    );
}

#[test]
fn delta_host_commit_failure_does_not_advance_protocol_state() {
    let key = ReplicationLineageKey::new(SessionId::new(1), ParticipantId::new(1));
    let mut client = ClientReplicationSet::new(client_limits(8, 256));
    client.add_lineage(key, retention(8)).unwrap();
    client
        .apply_full(
            key,
            FullSnapshot::new(ReplicationCursor::new(1), SimulationTick::new(1), image(1)),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();

    let failed = client
        .apply_delta(
            key,
            DeltaSnapshot::new(
                ReplicationCursor::new(1),
                ReplicationCursor::new(2),
                SimulationTick::new(2),
                (),
            ),
            |_, _, _| Ok(image(2)),
            |_| Err::<(), _>(()),
        )
        .unwrap();
    assert_eq!(failed, ClientSnapshotOutcome::HostCommitFailure);
    let lineage = client.lineage(key).unwrap();
    assert_eq!(lineage.current_cursor(), Some(ReplicationCursor::new(1)));
    assert_eq!(lineage.current_state().unwrap(), &state(1));
    assert_eq!(
        lineage.replication_state(),
        ClientReplicationState::FullSnapshotRequired(ClientRecoveryReason::DeltaCommitFailure)
    );
}

#[test]
fn rn1b_acceptance_is_the_only_authority_emission_boundary() {
    let participant = ParticipantId::new(1);
    let mut authority = AuthorityReplicationSession::<BTreeMap<&'static str, i32>, ()>::new(
        SessionId::new(1),
        authority_limits(),
    );
    authority.add_lineage(participant, retention(8)).unwrap();
    authority
        .prepare_full(
            participant,
            FullSnapshot::new(ReplicationCursor::new(1), SimulationTick::new(1), image(1)),
            true,
        )
        .unwrap();

    let flow = DeliveryFlowKey::new(
        ConnectionHandle::new(1),
        FlowDirection::Outbound,
        DeliveryFlowHandle::new(1),
    );
    let scope = DeliveryScopeLimits::new(nz(2), nz(2), nz(16));
    let policy = FlowResourcePolicy::new(
        nz(4),
        nz(1),
        nz(4),
        OutboundPressureBehavior::RejectNew,
        ReceiverPressureBehavior::TerminateReliable,
    );
    let mut delivery = DeliveryEndpoint::new(scope);
    delivery
        .establish_flow(flow, DeliveryMode::ReliableOrdered, policy, scope)
        .unwrap();

    let rejected = delivery.submit(flow, vec![0; 5]).unwrap();
    assert_eq!(rejected, SubmissionOutcome::RejectedTooLarge);
    assert_eq!(
        authority
            .record_delivery_acceptance(participant, rejected.acceptance())
            .unwrap(),
        None
    );
    assert_eq!(
        authority
            .lineage(participant)
            .unwrap()
            .greatest_emitted_cursor(),
        None
    );
    assert!(
        authority
            .lineage(participant)
            .unwrap()
            .pending_snapshot()
            .is_some()
    );

    let accepted = delivery.submit(flow, vec![0; 4]).unwrap();
    assert!(matches!(accepted, SubmissionOutcome::Accepted { .. }));
    authority
        .record_delivery_acceptance(participant, accepted.acceptance())
        .unwrap()
        .expect("accepted delivery records emission");
    assert_eq!(
        authority
            .lineage(participant)
            .unwrap()
            .greatest_emitted_cursor(),
        Some(ReplicationCursor::new(1))
    );
}

#[test]
fn ack_evidence_eviction_stays_distinct_from_future_confirmation() {
    let participant = ParticipantId::new(1);
    let connection = ConnectionHandle::new(1);
    let session = authorized_session(SessionId::new(1), participant, connection);
    let mut authority = AuthorityReplicationSession::<BTreeMap<&'static str, i32>, ()>::new(
        SessionId::new(1),
        authority_limits(),
    );
    authority.add_lineage(participant, retention(1)).unwrap();

    emit_full(&mut authority, participant, 1, 1, 1, true);
    assert_eq!(
        authority
            .acknowledge_authorized(&session, connection, participant, ReplicationCursor::new(1))
            .unwrap(),
        AuthorityAckOutcome::Confirmed
    );
    emit_delta(&mut authority, participant, 2, 2, 2);
    emit_delta(&mut authority, participant, 3, 3, 3);

    assert_eq!(
        authority
            .acknowledge_authorized(&session, connection, participant, ReplicationCursor::new(2))
            .unwrap(),
        AuthorityAckOutcome::UnverifiableConfirmation
    );
    assert_eq!(
        authority
            .acknowledge_authorized(&session, connection, participant, ReplicationCursor::new(4))
            .unwrap(),
        AuthorityAckOutcome::FutureConfirmation
    );
    assert_eq!(
        authority
            .acknowledge_authorized(&session, connection, participant, ReplicationCursor::new(3))
            .unwrap(),
        AuthorityAckOutcome::Confirmed
    );
    assert_eq!(
        authority
            .lineage(participant)
            .unwrap()
            .latest_confirmed_cursor(),
        Some(ReplicationCursor::new(3))
    );
}

#[test]
fn truthful_ack_without_retained_baseline_requires_full_recovery() {
    let participant = ParticipantId::new(1);
    let connection = ConnectionHandle::new(1);
    let session = authorized_session(SessionId::new(1), participant, connection);
    let mut authority = AuthorityReplicationSession::<BTreeMap<&'static str, i32>, ()>::new(
        SessionId::new(1),
        authority_limits(),
    );
    authority.add_lineage(participant, retention(8)).unwrap();
    emit_full(&mut authority, participant, 1, 1, 1, true);
    authority
        .acknowledge_authorized(&session, connection, participant, ReplicationCursor::new(1))
        .unwrap();
    emit_delta(&mut authority, participant, 2, 2, 2);
    assert!(
        authority
            .evict_retained_state(participant, ReplicationCursor::new(2))
            .unwrap()
    );

    assert_eq!(
        authority
            .acknowledge_authorized(&session, connection, participant, ReplicationCursor::new(2))
            .unwrap(),
        AuthorityAckOutcome::Confirmed
    );
    let lineage = authority.lineage(participant).unwrap();
    assert_eq!(
        lineage.latest_confirmed_cursor(),
        Some(ReplicationCursor::new(2))
    );
    assert!(matches!(
        lineage.replication_state(),
        AuthorityReplicationState::FullSnapshotRequired {
            reason: AuthorityRecoveryReason::ConfirmedBaselineUnavailable,
            ..
        }
    ));
}

#[test]
fn replacement_generation_rejects_old_recovery_completion() {
    let participant = ParticipantId::new(1);
    let first_connection = ConnectionHandle::new(1);
    let replacement = ConnectionHandle::new(2);
    let mut negotiation = negotiation_manager();
    establish(&mut negotiation, first_connection);
    establish(&mut negotiation, replacement);
    let mut session = Session::new(SessionId::new(1), SessionLimits::new(nz(8), nz(4)).unwrap());
    session
        .admit_new(
            participant,
            negotiation.established(first_connection).unwrap(),
        )
        .unwrap();

    let mut authority = AuthorityReplicationSession::<BTreeMap<&'static str, i32>, ()>::new(
        SessionId::new(1),
        authority_limits(),
    );
    authority.add_lineage(participant, retention(8)).unwrap();
    emit_full(&mut authority, participant, 1, 1, 1, true);
    authority
        .acknowledge_authorized(
            &session,
            first_connection,
            participant,
            ReplicationCursor::new(1),
        )
        .unwrap();

    authority
        .prepare_delta(
            participant,
            ReplicationCursor::new(2),
            SimulationTick::new(2),
            image(2),
            (),
            0,
        )
        .unwrap();
    assert!(
        authority
            .lineage(participant)
            .unwrap()
            .pending_snapshot()
            .is_some()
    );
    authority
        .require_full_recovery(participant, AuthorityRecoveryReason::RecoveryDemand)
        .unwrap();
    assert!(
        authority
            .lineage(participant)
            .unwrap()
            .pending_snapshot()
            .is_none()
    );

    emit_full(&mut authority, participant, 2, 2, 2, true);
    session
        .connection_lost(
            participant,
            first_connection,
            RetentionPolicy::RetainForRecovery {
                duration: RecoveryDuration::new(NonZeroU64::new(10).unwrap()),
            },
        )
        .unwrap();
    session
        .bind_replacement(participant, negotiation.established(replacement).unwrap())
        .unwrap();
    authority
        .connection_replaced(&session, replacement, participant)
        .unwrap();

    let generation = match authority.lineage(participant).unwrap().replication_state() {
        AuthorityReplicationState::FullSnapshotRequired { generation, .. } => generation,
        other => panic!("expected recovery, got {other:?}"),
    };
    assert_eq!(generation.get(), 2);

    assert_eq!(
        authority
            .acknowledge_authorized(
                &session,
                replacement,
                participant,
                ReplicationCursor::new(2),
            )
            .unwrap(),
        AuthorityAckOutcome::Confirmed
    );
    let generation_after_old_ack = match authority.lineage(participant).unwrap().replication_state()
    {
        AuthorityReplicationState::FullSnapshotRequired { generation, .. } => generation,
        other => panic!("old recovery ACK cleared generation: {other:?}"),
    };
    assert_eq!(generation_after_old_ack.get(), 2);

    emit_full(&mut authority, participant, 3, 3, 3, true);
    assert!(matches!(
        authority.lineage(participant).unwrap().replication_state(),
        AuthorityReplicationState::FullSnapshotRequired { .. }
    ));
    authority
        .acknowledge_authorized(
            &session,
            replacement,
            participant,
            ReplicationCursor::new(3),
        )
        .unwrap();
    assert_eq!(
        authority.lineage(participant).unwrap().replication_state(),
        AuthorityReplicationState::DeltaEligible(ReplicationCursor::new(3))
    );
}

#[test]
fn unrelated_connection_cannot_advance_confirmation() {
    let participant = ParticipantId::new(1);
    let connection = ConnectionHandle::new(1);
    let session = authorized_session(SessionId::new(1), participant, connection);
    let mut authority = AuthorityReplicationSession::<BTreeMap<&'static str, i32>, ()>::new(
        SessionId::new(1),
        authority_limits(),
    );
    authority.add_lineage(participant, retention(8)).unwrap();
    emit_full(&mut authority, participant, 1, 1, 1, true);

    assert!(matches!(
        authority.acknowledge_authorized(
            &session,
            ConnectionHandle::new(99),
            participant,
            ReplicationCursor::new(1),
        ),
        Err(AuthoritySessionError::Operation(_))
    ));
    assert_eq!(
        authority
            .lineage(participant)
            .unwrap()
            .latest_confirmed_cursor(),
        None
    );
}

#[test]
fn aggregate_client_retention_is_enforced_across_lineages() {
    let participant = ParticipantId::new(1);
    let first = ReplicationLineageKey::new(SessionId::new(1), participant);
    let second = ReplicationLineageKey::new(SessionId::new(2), participant);
    let mut client = ClientReplicationSet::new(client_limits(1, 16));
    client.add_lineage(first, retention(8)).unwrap();
    client.add_lineage(second, retention(8)).unwrap();

    assert_eq!(
        client
            .apply_full(
                first,
                FullSnapshot::new(ReplicationCursor::new(1), SimulationTick::new(1), image(1)),
                |_| Ok::<_, ()>(()),
            )
            .unwrap(),
        ClientSnapshotOutcome::Committed(ReplicationCursor::new(1))
    );
    assert_eq!(
        client
            .apply_full(
                second,
                FullSnapshot::new(ReplicationCursor::new(1), SimulationTick::new(1), image(2)),
                |_| Ok::<_, ()>(()),
            )
            .unwrap(),
        ClientSnapshotOutcome::StateResourceFailure
    );
    assert_eq!(client.retained_image_count(), 1);
    assert_eq!(client.retained_state_bytes(), 8);
}
