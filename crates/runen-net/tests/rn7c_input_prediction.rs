use std::num::{NonZeroU64, NonZeroUsize};

use runen_net::identity::{ConnectionHandle, ParticipantId, SessionId, SimulationTick};
use runen_net::input::{
    AuthorityInputAggregateLimits, AuthorityInputError, AuthorityInputLimitError,
    AuthorityInputLimits, AuthorityInputOutcome, AuthorityInputSession, InputWindow,
    InputWindowError, PredictionActivationError, PredictionInputOutcome,
    PredictionInvalidationReason, PredictionLimits, PredictionLineage,
    PredictionReconciliationError, PredictionReconciliationOutcome, PredictionState,
};
use runen_net::protocol::{
    CompatibilityOffer, NegotiatedContract, NegotiationManager, NegotiationManagerLimits,
    NegotiationRequirements, OfferLimits, ProtocolContract, ProtocolId, ProtocolRevision,
};
use runen_net::replication::{
    AccountedState, ClientAggregateLimits, ClientRecoveryReason, ClientReplicationSet,
    ClientReplicationState, ClientSnapshotOutcome, FullSnapshot, ReplicationCursor,
    ReplicationLineageKey, ReplicationRetentionLimits,
};
use runen_net::session::{RecoveryDuration, RetentionPolicy, Session, SessionLimits};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn tick(value: u64) -> SimulationTick {
    SimulationTick::new(value)
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

fn session_limits() -> SessionLimits {
    SessionLimits::new(nz(32), nz(16)).unwrap()
}

fn bound_session(
    session_id: SessionId,
    participant: ParticipantId,
    connection: ConnectionHandle,
) -> (Session, NegotiationManager) {
    let mut negotiation = negotiation_manager();
    establish(&mut negotiation, connection);
    let mut session = Session::new(session_id, session_limits());
    session
        .admit_new(participant, negotiation.established(connection).unwrap())
        .unwrap();
    (session, negotiation)
}

fn input_limits(
    max_batch_bytes: usize,
    max_keys: usize,
    max_bytes: usize,
    max_future_tick_distance: u64,
) -> AuthorityInputLimits {
    AuthorityInputLimits::new(
        nz(max_batch_bytes),
        nz(max_keys),
        nz(max_bytes),
        max_future_tick_distance,
    )
    .unwrap()
}

fn input_session<I>(session: SessionId) -> AuthorityInputSession<I> {
    AuthorityInputSession::new(
        session,
        AuthorityInputAggregateLimits::new(nz(32), nz(1024)),
    )
}

fn window(minimum: u64, maximum: u64) -> InputWindow {
    InputWindow::new(tick(minimum), tick(maximum)).unwrap()
}

fn retention_limits() -> ReplicationRetentionLimits {
    ReplicationRetentionLimits::new(nz(64), nz(8), nz(256), nz(128), nz(8)).unwrap()
}

fn replication(key: ReplicationLineageKey) -> ClientReplicationSet<u32> {
    let mut replication =
        ClientReplicationSet::new(ClientAggregateLimits::new(nz(4), nz(16), nz(512)));
    replication.add_lineage(key, retention_limits()).unwrap();
    replication
}

fn full(cursor: u64, target_tick: u64, value: u32) -> FullSnapshot<u32> {
    FullSnapshot::new(
        ReplicationCursor::new(cursor),
        tick(target_tick),
        AccountedState::new(value, 8),
    )
}

fn prediction_limits() -> PredictionLimits {
    PredictionLimits::new(nz(8), nz(64), 32)
}

fn activate_prediction(
    prediction: &mut PredictionLineage<u32>,
    replication: &mut ClientReplicationSet<u32>,
    key: ReplicationLineageKey,
    cursor: u64,
    target_tick: u64,
) {
    let outcome = replication
        .apply_full(key, full(cursor, target_tick, target_tick as u32), |_| {
            Ok::<_, ()>(())
        })
        .unwrap();
    assert_eq!(
        prediction
            .observe_replication(
                outcome,
                replication.lineage(key).unwrap(),
                |_, _| Ok::<_, ()>(()),
            )
            .unwrap(),
        PredictionReconciliationOutcome::ActivatedFromAuthoritative {
            frontier: tick(target_tick)
        }
    );
}

#[test]
fn authority_accepts_one_semantic_batch_at_most_once() {
    let session_id = SessionId::new(1);
    let participant = ParticipantId::new(1);
    let connection = ConnectionHandle::new(1);
    let (session, _) = bound_session(session_id, participant, connection);
    let mut input = input_session::<u32>(session_id);
    input
        .add_participant(
            &session,
            participant,
            window(10, 20),
            input_limits(8, 8, 64, 10),
        )
        .unwrap();

    assert_eq!(
        input
            .submit(&session, participant, connection, tick(12), &7, 4)
            .unwrap(),
        AuthorityInputOutcome::InputAccepted
    );
    assert_eq!(
        input
            .submit(&session, participant, connection, tick(12), &7, 4)
            .unwrap(),
        AuthorityInputOutcome::DuplicateInput
    );
    assert_eq!(input.participant_retained_key_count(participant), Some(1));
    assert_eq!(input.participant_retained_bytes(participant), Some(4));
}

#[test]
fn authority_conflicting_same_key_cannot_replace_accepted_input() {
    let session_id = SessionId::new(2);
    let participant = ParticipantId::new(2);
    let connection = ConnectionHandle::new(2);
    let (session, _) = bound_session(session_id, participant, connection);
    let mut input = input_session::<u32>(session_id);
    input
        .add_participant(
            &session,
            participant,
            window(1, 8),
            input_limits(8, 8, 64, 7),
        )
        .unwrap();

    assert_eq!(
        input
            .submit(&session, participant, connection, tick(3), &11, 4)
            .unwrap(),
        AuthorityInputOutcome::InputAccepted
    );
    assert_eq!(
        input
            .submit(&session, participant, connection, tick(3), &12, 4)
            .unwrap(),
        AuthorityInputOutcome::ConflictingInput
    );
    assert_eq!(
        input
            .submit(&session, participant, connection, tick(3), &11, 4)
            .unwrap(),
        AuthorityInputOutcome::DuplicateInput
    );
}

#[test]
fn stale_input_wins_after_monotonic_minimum_passes_key() {
    let session_id = SessionId::new(3);
    let participant = ParticipantId::new(3);
    let connection = ConnectionHandle::new(3);
    let (session, _) = bound_session(session_id, participant, connection);
    let mut input = input_session::<u32>(session_id);
    input
        .add_participant(
            &session,
            participant,
            window(10, 20),
            input_limits(8, 8, 64, 10),
        )
        .unwrap();
    assert_eq!(
        input
            .submit(&session, participant, connection, tick(12), &4, 4)
            .unwrap(),
        AuthorityInputOutcome::InputAccepted
    );

    input.advance_window(participant, window(13, 21)).unwrap();
    assert_eq!(input.participant_retained_key_count(participant), Some(0));
    assert_eq!(
        input
            .submit(&session, participant, connection, tick(12), &4, 4)
            .unwrap(),
        AuthorityInputOutcome::StaleInput
    );
    assert_eq!(
        input
            .submit(&session, participant, connection, tick(12), &99, 4)
            .unwrap(),
        AuthorityInputOutcome::StaleInput
    );
}

#[test]
fn authority_windows_cannot_regress_or_readmit_expired_keys() {
    let session_id = SessionId::new(4);
    let participant = ParticipantId::new(4);
    let connection = ConnectionHandle::new(4);
    let (session, _) = bound_session(session_id, participant, connection);
    let mut input = input_session::<u32>(session_id);
    input
        .add_participant(
            &session,
            participant,
            window(10, 20),
            input_limits(8, 8, 64, 10),
        )
        .unwrap();
    input.advance_window(participant, window(15, 25)).unwrap();

    assert_eq!(
        input.advance_window(participant, window(14, 26)),
        Err(AuthorityInputError::WindowMinimumRegression)
    );
    assert_eq!(
        input.advance_window(participant, window(15, 24)),
        Err(AuthorityInputError::WindowMaximumRegression)
    );
    assert_eq!(input.participant_window(participant), Some(window(15, 25)));
    assert_eq!(
        input
            .submit(&session, participant, connection, tick(12), &5, 4)
            .unwrap(),
        AuthorityInputOutcome::StaleInput
    );
}

#[test]
fn future_input_becomes_admissible_only_after_maximum_advances() {
    let session_id = SessionId::new(5);
    let participant = ParticipantId::new(5);
    let connection = ConnectionHandle::new(5);
    let (session, _) = bound_session(session_id, participant, connection);
    let mut input = input_session::<u32>(session_id);
    input
        .add_participant(
            &session,
            participant,
            window(10, 12),
            input_limits(8, 8, 64, 10),
        )
        .unwrap();

    assert_eq!(
        input
            .submit(&session, participant, connection, tick(14), &1, 4)
            .unwrap(),
        AuthorityInputOutcome::FutureInputOutsideWindow
    );
    assert_eq!(input.participant_retained_key_count(participant), Some(0));
    input.advance_window(participant, window(10, 14)).unwrap();
    assert_eq!(
        input
            .submit(&session, participant, connection, tick(14), &1, 4)
            .unwrap(),
        AuthorityInputOutcome::InputAccepted
    );
}

#[test]
fn authority_resource_rejection_is_bounded_and_non_mutating() {
    let session_id = SessionId::new(6);
    let first = ParticipantId::new(1);
    let second = ParticipantId::new(2);
    let first_connection = ConnectionHandle::new(61);
    let second_connection = ConnectionHandle::new(62);
    let mut negotiation = negotiation_manager();
    establish(&mut negotiation, first_connection);
    establish(&mut negotiation, second_connection);
    let mut session = Session::new(session_id, session_limits());
    session
        .admit_new(first, negotiation.established(first_connection).unwrap())
        .unwrap();
    session
        .admit_new(second, negotiation.established(second_connection).unwrap())
        .unwrap();

    let mut input = AuthorityInputSession::<u32>::new(
        session_id,
        AuthorityInputAggregateLimits::new(nz(2), nz(8)),
    );
    let participant_limits = input_limits(4, 1, 4, 8);
    input
        .add_participant(&session, first, window(1, 8), participant_limits)
        .unwrap();
    input
        .add_participant(&session, second, window(1, 8), participant_limits)
        .unwrap();

    assert_eq!(
        input
            .submit(&session, first, first_connection, tick(2), &1, 5)
            .unwrap(),
        AuthorityInputOutcome::InputResourceRejected
    );
    assert_eq!(input.retained_key_count(), 0);
    assert_eq!(input.retained_bytes(), 0);

    assert_eq!(
        input
            .submit(&session, first, first_connection, tick(2), &1, 4)
            .unwrap(),
        AuthorityInputOutcome::InputAccepted
    );
    assert_eq!(
        input
            .submit(&session, first, first_connection, tick(3), &2, 4)
            .unwrap(),
        AuthorityInputOutcome::InputResourceRejected
    );
    assert_eq!(input.participant_retained_key_count(first), Some(1));
    assert_eq!(input.participant_retained_bytes(first), Some(4));

    assert_eq!(
        input
            .submit(&session, second, second_connection, tick(2), &3, 4)
            .unwrap(),
        AuthorityInputOutcome::InputAccepted
    );
    assert_eq!(input.retained_key_count(), 2);
    assert_eq!(input.retained_bytes(), 8);
    assert_eq!(
        input
            .submit(&session, second, second_connection, tick(3), &4, 4)
            .unwrap(),
        AuthorityInputOutcome::InputResourceRejected
    );
    assert_eq!(input.retained_key_count(), 2);
    assert_eq!(input.retained_bytes(), 8);
}

#[test]
fn unauthorized_connection_cannot_create_applicable_input() {
    let session_id = SessionId::new(7);
    let participant = ParticipantId::new(7);
    let connection = ConnectionHandle::new(7);
    let unauthorized = ConnectionHandle::new(70);
    let (session, _) = bound_session(session_id, participant, connection);
    let mut input = input_session::<u32>(session_id);
    input
        .add_participant(
            &session,
            participant,
            window(1, 8),
            input_limits(8, 8, 64, 7),
        )
        .unwrap();

    assert_eq!(
        input
            .submit(&session, participant, unauthorized, tick(2), &1, 4)
            .unwrap(),
        AuthorityInputOutcome::UnauthorizedInput
    );
    assert_eq!(input.retained_key_count(), 0);
}

#[test]
fn pending_prediction_admission_failure_prevents_tracked_admission() {
    let key = ReplicationLineageKey::new(SessionId::new(8), ParticipantId::new(8));
    let mut replication = replication(key);
    let mut prediction = PredictionLineage::new(key, PredictionLimits::new(nz(1), nz(4), 2));
    activate_prediction(&mut prediction, &mut replication, key, 1, 10);

    assert_eq!(
        prediction.admit_local(tick(11), &1, 4),
        PredictionInputOutcome::InputAccepted
    );
    assert_eq!(
        prediction.admit_local(tick(12), &2, 4),
        PredictionInputOutcome::PendingPredictionResourceRejected
    );
    assert_eq!(prediction.pending_count(), 1);
    assert_eq!(prediction.pending_bytes(), 4);
}

#[test]
fn prediction_rejects_input_at_or_before_authoritative_frontier() {
    let key = ReplicationLineageKey::new(SessionId::new(9), ParticipantId::new(9));
    let mut replication = replication(key);
    let mut prediction = PredictionLineage::new(key, prediction_limits());
    activate_prediction(&mut prediction, &mut replication, key, 1, 10);

    assert_eq!(
        prediction.admit_local(tick(9), &1, 4),
        PredictionInputOutcome::PredictionInputNotNewerThanFrontier
    );
    assert_eq!(
        prediction.admit_local(tick(10), &1, 4),
        PredictionInputOutcome::PredictionInputNotNewerThanFrontier
    );
    assert_eq!(prediction.pending_count(), 0);
}

#[test]
fn local_prediction_same_key_duplicate_and_conflict_are_deterministic() {
    let key = ReplicationLineageKey::new(SessionId::new(10), ParticipantId::new(10));
    let mut replication = replication(key);
    let mut prediction = PredictionLineage::new(key, prediction_limits());
    activate_prediction(&mut prediction, &mut replication, key, 1, 10);

    assert_eq!(
        prediction.admit_local(tick(11), &5, 4),
        PredictionInputOutcome::InputAccepted
    );
    assert_eq!(
        prediction.admit_local(tick(11), &5, 4),
        PredictionInputOutcome::DuplicateInput
    );
    assert_eq!(
        prediction.admit_local(tick(11), &6, 4),
        PredictionInputOutcome::ConflictingInput
    );
    assert_eq!(prediction.pending_input(tick(11)), Some(&5));
}

#[test]
fn authoritative_commit_advances_frontier_and_retires_covered_prediction() {
    let key = ReplicationLineageKey::new(SessionId::new(11), ParticipantId::new(11));
    let mut replication = replication(key);
    let mut prediction = PredictionLineage::new(key, prediction_limits());
    activate_prediction(&mut prediction, &mut replication, key, 1, 10);
    prediction.admit_local(tick(11), &11, 4);
    prediction.admit_local(tick(12), &12, 4);

    let outcome = replication
        .apply_full(key, full(2, 12, 99), |_| Ok::<_, ()>(()))
        .unwrap();
    let mut replay_called = false;
    assert_eq!(
        prediction
            .observe_replication(outcome, replication.lineage(key).unwrap(), |_, _| {
                replay_called = true;
                Ok::<_, ()>(())
            })
            .unwrap(),
        PredictionReconciliationOutcome::ReconciledNoReplay { frontier: tick(12) }
    );
    assert!(!replay_called);
    assert_eq!(prediction.frontier(), Some(tick(12)));
    assert_eq!(prediction.pending_count(), 0);
}

#[test]
fn later_prediction_replays_once_in_target_tick_order() {
    let key = ReplicationLineageKey::new(SessionId::new(12), ParticipantId::new(12));
    let mut replication = replication(key);
    let mut prediction = PredictionLineage::new(key, prediction_limits());
    activate_prediction(&mut prediction, &mut replication, key, 1, 10);
    prediction.admit_local(tick(13), &13, 4);
    prediction.admit_local(tick(11), &11, 4);
    prediction.admit_local(tick(12), &12, 4);

    let outcome = replication
        .apply_full(key, full(2, 11, 50), |_| Ok::<_, ()>(()))
        .unwrap();
    let mut replayed = Vec::new();
    assert_eq!(
        prediction
            .observe_replication(
                outcome,
                replication.lineage(key).unwrap(),
                |target, value| {
                    replayed.push((target.get(), *value));
                    Ok::<_, ()>(())
                },
            )
            .unwrap(),
        PredictionReconciliationOutcome::ReconciledReplay {
            frontier: tick(11),
            replayed: 2
        }
    );
    assert_eq!(replayed, vec![(12, 12), (13, 13)]);
    assert_eq!(prediction.pending_count(), 2);
}

#[test]
fn failed_authoritative_candidate_does_not_mutate_prediction() {
    let key = ReplicationLineageKey::new(SessionId::new(13), ParticipantId::new(13));
    let mut replication = replication(key);
    let mut prediction = PredictionLineage::new(key, prediction_limits());
    activate_prediction(&mut prediction, &mut replication, key, 1, 10);
    prediction.admit_local(tick(11), &11, 4);
    prediction.admit_local(tick(12), &12, 4);

    let failed = replication
        .apply_full(key, full(2, 11, 99), |_| Err::<(), _>("commit failed"))
        .unwrap_err();
    assert_eq!(
        failed,
        runen_net::replication::ClientApplyError::HostCommitFailure {
            source: "commit failed"
        }
    );
    assert_eq!(
        replication.lineage(key).unwrap().current_cursor(),
        Some(ReplicationCursor::new(1))
    );
    assert_eq!(
        replication.lineage(key).unwrap().current_tick(),
        Some(tick(10))
    );
    assert_eq!(prediction.frontier(), Some(tick(10)));
    assert_eq!(prediction.pending_count(), 2);
    assert_eq!(prediction.pending_input(tick(11)), Some(&11));
    assert_eq!(prediction.pending_input(tick(12)), Some(&12));
}

#[test]
fn replay_failure_preserves_commit_but_invalidates_prediction_until_host_restore() {
    let key = ReplicationLineageKey::new(SessionId::new(14), ParticipantId::new(14));
    let mut replication = replication(key);
    let mut prediction = PredictionLineage::new(key, prediction_limits());
    activate_prediction(&mut prediction, &mut replication, key, 1, 10);
    prediction.admit_local(tick(12), &12, 4);
    prediction.admit_local(tick(13), &13, 4);

    let outcome = replication
        .apply_full(key, full(2, 11, 100), |_| Ok::<_, ()>(()))
        .unwrap();
    let mut replayed = Vec::new();
    let error = prediction
        .observe_replication(
            outcome,
            replication.lineage(key).unwrap(),
            |target, value| {
                replayed.push((target.get(), *value));
                if target == tick(13) {
                    Err("replay failed")
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
    match error {
        PredictionReconciliationError::ReplayFailed {
            tick: failed,
            source,
        } => {
            assert_eq!(failed, tick(13));
            assert_eq!(source, "replay failed");
        }
        other => panic!("unexpected reconciliation error: {other:?}"),
    }
    assert_eq!(replayed, vec![(12, 12), (13, 13)]);
    assert_eq!(
        replication.lineage(key).unwrap().current_cursor(),
        Some(ReplicationCursor::new(2))
    );
    assert_eq!(
        replication.lineage(key).unwrap().current_tick(),
        Some(tick(11))
    );
    assert_eq!(
        prediction.state(),
        PredictionState::Invalidated {
            reason: PredictionInvalidationReason::ReplayFailure,
            frontier: Some(tick(11))
        }
    );
    assert_eq!(prediction.pending_count(), 0);

    assert_eq!(
        prediction
            .confirm_host_restored_after_replay_failure(replication.lineage(key).unwrap())
            .unwrap(),
        tick(11)
    );
    assert_eq!(
        prediction.state(),
        PredictionState::Active { frontier: tick(11) }
    );
    assert_eq!(
        prediction.confirm_host_restored_after_replay_failure(replication.lineage(key).unwrap()),
        Err(PredictionActivationError::NotReplayFailure)
    );
}

#[test]
fn full_snapshot_required_invalidates_and_clears_pending_prediction() {
    let key = ReplicationLineageKey::new(SessionId::new(15), ParticipantId::new(15));
    let mut replication = replication(key);
    let mut prediction = PredictionLineage::new(key, prediction_limits());
    activate_prediction(&mut prediction, &mut replication, key, 1, 10);
    prediction.admit_local(tick(11), &11, 4);

    prediction
        .require_connection_replacement_full(&mut replication)
        .unwrap();
    assert_eq!(
        replication.lineage(key).unwrap().replication_state(),
        ClientReplicationState::FullSnapshotRequired(ClientRecoveryReason::ConnectionReplacement)
    );
    assert_eq!(
        prediction.state(),
        PredictionState::Invalidated {
            reason: PredictionInvalidationReason::ReplicationRecovery(
                ClientRecoveryReason::ConnectionReplacement
            ),
            frontier: Some(tick(10))
        }
    );
    assert_eq!(prediction.pending_count(), 0);
    assert_eq!(
        prediction.admit_local(tick(12), &12, 4),
        PredictionInputOutcome::PredictionInvalidated(
            PredictionInvalidationReason::ReplicationRecovery(
                ClientRecoveryReason::ConnectionReplacement
            )
        )
    );
}

#[test]
fn replacement_resets_prediction_but_preserves_participant_scoped_authority_input_state() {
    let session_id = SessionId::new(16);
    let participant = ParticipantId::new(16);
    let first_connection = ConnectionHandle::new(161);
    let replacement = ConnectionHandle::new(162);
    let key = ReplicationLineageKey::new(session_id, participant);
    let (mut session, mut negotiation) = bound_session(session_id, participant, first_connection);
    establish(&mut negotiation, replacement);

    let mut authority_input = input_session::<u32>(session_id);
    authority_input
        .add_participant(
            &session,
            participant,
            window(10, 20),
            input_limits(8, 8, 64, 10),
        )
        .unwrap();
    assert_eq!(
        authority_input
            .submit(&session, participant, first_connection, tick(12), &7, 4)
            .unwrap(),
        AuthorityInputOutcome::InputAccepted
    );

    let mut replication = replication(key);
    let mut prediction = PredictionLineage::new(key, prediction_limits());
    activate_prediction(&mut prediction, &mut replication, key, 1, 10);
    prediction.admit_local(tick(11), &11, 4);

    session
        .connection_lost(
            participant,
            first_connection,
            RetentionPolicy::RetainForRecovery {
                duration: RecoveryDuration::new(NonZeroU64::new(5).unwrap()),
            },
        )
        .unwrap();
    prediction.connection_lost();
    assert!(
        authority_input
            .reconcile_memberships(&session)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        authority_input.participant_retained_key_count(participant),
        Some(1)
    );
    assert_eq!(
        authority_input.participant_window(participant),
        Some(window(10, 20))
    );
    assert_eq!(prediction.pending_count(), 0);

    session
        .bind_replacement(participant, negotiation.established(replacement).unwrap())
        .unwrap();
    prediction
        .require_connection_replacement_full(&mut replication)
        .unwrap();

    assert_eq!(
        authority_input
            .submit(&session, participant, replacement, tick(12), &7, 4)
            .unwrap(),
        AuthorityInputOutcome::DuplicateInput
    );

    let outcome = replication
        .apply_full(key, full(2, 12, 120), |_| Ok::<_, ()>(()))
        .unwrap();
    let mut replay_called = false;
    assert_eq!(
        prediction
            .observe_replication(outcome, replication.lineage(key).unwrap(), |_, _| {
                replay_called = true;
                Ok::<_, ()>(())
            })
            .unwrap(),
        PredictionReconciliationOutcome::ActivatedFromAuthoritative { frontier: tick(12) }
    );
    assert!(!replay_called);
    assert_eq!(prediction.pending_count(), 0);
}

#[test]
fn participant_removal_and_session_close_terminate_old_input_and_prediction_state() {
    let session_id = SessionId::new(17);
    let participant = ParticipantId::new(17);
    let connection = ConnectionHandle::new(171);
    let key = ReplicationLineageKey::new(session_id, participant);
    let (mut session, _) = bound_session(session_id, participant, connection);
    let mut authority_input = input_session::<u32>(session_id);
    authority_input
        .add_participant(
            &session,
            participant,
            window(1, 8),
            input_limits(8, 8, 64, 7),
        )
        .unwrap();
    authority_input
        .submit(&session, participant, connection, tick(2), &2, 4)
        .unwrap();
    let mut first_replication = replication(key);
    let mut prediction = PredictionLineage::new(key, prediction_limits());
    activate_prediction(&mut prediction, &mut first_replication, key, 1, 1);
    prediction.admit_local(tick(2), &2, 4);

    session.remove_participant(participant).unwrap();
    assert_eq!(
        authority_input.reconcile_memberships(&session).unwrap(),
        vec![participant]
    );
    prediction.participant_membership_ended();
    assert_eq!(authority_input.participant_count(), 0);
    assert_eq!(authority_input.retained_key_count(), 0);
    assert_eq!(
        prediction.state(),
        PredictionState::Invalidated {
            reason: PredictionInvalidationReason::ParticipantMembershipEnded,
            frontier: None
        }
    );
    assert_eq!(
        prediction.admit_local(tick(3), &3, 4),
        PredictionInputOutcome::PredictionInvalidated(
            PredictionInvalidationReason::ParticipantMembershipEnded
        )
    );
    assert_eq!(
        prediction
            .observe_replication(
                ClientSnapshotOutcome::Committed(ReplicationCursor::new(1)),
                first_replication.lineage(key).unwrap(),
                |_, _| Ok::<_, ()>(()),
            )
            .unwrap(),
        PredictionReconciliationOutcome::RemainsInvalidated {
            reason: PredictionInvalidationReason::ParticipantMembershipEnded
        }
    );

    let second_session_id = SessionId::new(18);
    let second_participant = ParticipantId::new(18);
    let second_connection = ConnectionHandle::new(181);
    let second_key = ReplicationLineageKey::new(second_session_id, second_participant);
    let (mut second_session, _) =
        bound_session(second_session_id, second_participant, second_connection);
    let mut second_authority_input = input_session::<u32>(second_session_id);
    second_authority_input
        .add_participant(
            &second_session,
            second_participant,
            window(1, 8),
            input_limits(8, 8, 64, 7),
        )
        .unwrap();
    second_authority_input
        .submit(
            &second_session,
            second_participant,
            second_connection,
            tick(2),
            &2,
            4,
        )
        .unwrap();
    let mut second_replication = replication(second_key);
    let mut second_prediction = PredictionLineage::new(second_key, prediction_limits());
    activate_prediction(
        &mut second_prediction,
        &mut second_replication,
        second_key,
        1,
        1,
    );
    second_prediction.admit_local(tick(2), &2, 4);

    second_session.close();
    assert_eq!(
        second_authority_input
            .reconcile_memberships(&second_session)
            .unwrap(),
        vec![second_participant]
    );
    second_prediction.session_closed();
    assert_eq!(second_authority_input.participant_count(), 0);
    assert_eq!(second_authority_input.retained_key_count(), 0);
    assert_eq!(
        second_prediction.state(),
        PredictionState::Invalidated {
            reason: PredictionInvalidationReason::SessionClosed,
            frontier: None
        }
    );
}

#[test]
fn limit_constructors_reject_invalid_relationships() {
    assert_eq!(
        InputWindow::new(tick(2), tick(1)),
        Err(InputWindowError::MaximumBeforeMinimum)
    );
    assert_eq!(
        AuthorityInputLimits::new(nz(9), nz(2), nz(8), 4),
        Err(AuthorityInputLimitError::BatchExceedsParticipantBudget)
    );
}

#[test]
fn checked_accounting_overflow_fails_closed() {
    let session_id = SessionId::new(19);
    let first = ParticipantId::new(1);
    let second = ParticipantId::new(2);
    let first_connection = ConnectionHandle::new(191);
    let second_connection = ConnectionHandle::new(192);
    let mut negotiation = negotiation_manager();
    establish(&mut negotiation, first_connection);
    establish(&mut negotiation, second_connection);
    let mut session = Session::new(session_id, session_limits());
    session
        .admit_new(first, negotiation.established(first_connection).unwrap())
        .unwrap();
    session
        .admit_new(second, negotiation.established(second_connection).unwrap())
        .unwrap();
    let huge = input_limits(usize::MAX, 2, usize::MAX, 8);
    let mut authority = AuthorityInputSession::<u32>::new(
        session_id,
        AuthorityInputAggregateLimits::new(nz(4), nz(usize::MAX)),
    );
    authority
        .add_participant(&session, first, window(1, 8), huge)
        .unwrap();
    authority
        .add_participant(&session, second, window(1, 8), huge)
        .unwrap();
    assert_eq!(
        authority
            .submit(&session, first, first_connection, tick(2), &1, usize::MAX,)
            .unwrap(),
        AuthorityInputOutcome::InputAccepted
    );
    assert_eq!(
        authority
            .submit(&session, second, second_connection, tick(2), &2, 1)
            .unwrap(),
        AuthorityInputOutcome::InputResourceRejected
    );
    assert_eq!(authority.retained_key_count(), 1);
    assert_eq!(authority.retained_bytes(), usize::MAX);

    let key = ReplicationLineageKey::new(SessionId::new(20), ParticipantId::new(20));
    let mut replication = replication(key);
    let mut prediction =
        PredictionLineage::new(key, PredictionLimits::new(nz(2), nz(usize::MAX), 4));
    activate_prediction(&mut prediction, &mut replication, key, 1, 10);
    assert_eq!(
        prediction.admit_local(tick(11), &1, usize::MAX),
        PredictionInputOutcome::InputAccepted
    );
    assert_eq!(
        prediction.admit_local(tick(12), &2, 1),
        PredictionInputOutcome::PendingPredictionResourceRejected
    );
    assert_eq!(prediction.pending_count(), 1);
    assert_eq!(prediction.pending_bytes(), usize::MAX);
}

#[test]
fn same_tick_newer_authoritative_commit_replays_pending_prediction() {
    let key = ReplicationLineageKey::new(SessionId::new(21), ParticipantId::new(21));
    let mut replication = replication(key);
    let mut prediction = PredictionLineage::new(key, prediction_limits());
    activate_prediction(&mut prediction, &mut replication, key, 1, 10);
    prediction.admit_local(tick(11), &11, 4);

    let outcome = replication
        .apply_full(key, full(2, 10, 200), |_| Ok::<_, ()>(()))
        .unwrap();
    let mut replayed = Vec::new();
    assert_eq!(
        prediction
            .observe_replication(
                outcome,
                replication.lineage(key).unwrap(),
                |target, value| {
                    replayed.push((target.get(), *value));
                    Ok::<_, ()>(())
                },
            )
            .unwrap(),
        PredictionReconciliationOutcome::ReconciledReplay {
            frontier: tick(10),
            replayed: 1
        }
    );
    assert_eq!(replayed, vec![(11, 11)]);
    assert_eq!(prediction.frontier(), Some(tick(10)));
}
