use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use runen_net::delivery::SubmissionOutcome;
use runen_net::identity::{ConnectionHandle, ParticipantId, SessionId, SimulationTick};
use runen_net::protocol::{
    CompatibilityOffer, NegotiatedContract, NegotiationManager, NegotiationManagerLimits,
    NegotiationRequirements, OfferLimits, ProtocolContract, ProtocolId, ProtocolRevision,
};
use runen_net::replication::{
    AccountedState, AuthorityAckOutcome, AuthorityAggregateLimits, AuthorityPrepareError,
    AuthorityRecoveryReason, AuthorityReplicationSession, AuthorityReplicationState,
    AuthoritySessionError, ClientAggregateLimits, ClientRecoveryReason, ClientReplicationSet,
    ClientReplicationState, ClientSnapshotOutcome, DeltaReconstructionError, DeltaSnapshot,
    FullSnapshot, ReplicationCursor, ReplicationLineageKey, ReplicationRetentionLimits,
};
use runen_net::session::{Session, SessionLimits};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn retention() -> ReplicationRetentionLimits {
    ReplicationRetentionLimits::new(nz(32), nz(4), nz(64), nz(32), nz(4)).unwrap()
}

fn image(value: i32) -> AccountedState<BTreeMap<&'static str, i32>> {
    AccountedState::new(BTreeMap::from([("value", value)]), 8)
}

fn client() -> (
    ClientReplicationSet<BTreeMap<&'static str, i32>>,
    ReplicationLineageKey,
) {
    let key = ReplicationLineageKey::new(SessionId::new(1), ParticipantId::new(1));
    let limits = ClientAggregateLimits::new(nz(4), nz(8), nz(128));
    let mut client = ClientReplicationSet::new(limits);
    client.add_lineage(key, retention()).unwrap();
    (client, key)
}

fn protocol() -> ProtocolContract {
    ProtocolContract::new(ProtocolId::new(1), ProtocolRevision::new(1))
}

fn authorized_session(participant: ParticipantId, connection: ConnectionHandle) -> Session {
    let mut manager =
        NegotiationManager::new(OfferLimits::default(), NegotiationManagerLimits::default())
            .unwrap();
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

    let mut session = Session::new(SessionId::new(1), SessionLimits::new(nz(4), nz(2)).unwrap());
    session
        .admit_new(participant, manager.established(connection).unwrap())
        .unwrap();
    session
}

fn accepted() -> SubmissionOutcome {
    SubmissionOutcome::Accepted {
        accepted_index: 0,
        local_pressure_drops: 0,
    }
}

fn authority() -> AuthorityReplicationSession<BTreeMap<&'static str, i32>, ()> {
    AuthorityReplicationSession::new(
        SessionId::new(1),
        AuthorityAggregateLimits::new(nz(4), nz(256), nz(16), nz(128), nz(16)),
    )
}

fn emit_full(
    authority: &mut AuthorityReplicationSession<BTreeMap<&'static str, i32>, ()>,
    participant: ParticipantId,
    cursor: u64,
    tick: u64,
    recovery: bool,
) {
    authority
        .prepare_full(
            participant,
            FullSnapshot::new(
                ReplicationCursor::new(cursor),
                SimulationTick::new(tick),
                image(cursor as i32),
            ),
            recovery,
        )
        .unwrap();
    authority
        .record_delivery_submission(participant, accepted())
        .unwrap();
}

#[test]
fn initial_states_require_full_and_cursor_tick_regression_is_rejected() {
    let (mut client, key) = client();
    assert_eq!(
        client.lineage(key).unwrap().replication_state(),
        ClientReplicationState::FullSnapshotRequired(ClientRecoveryReason::InitialBaseline)
    );
    assert_eq!(client.lineage(key).unwrap().acknowledgement_cursor(), None);

    client
        .apply_full(
            key,
            FullSnapshot::new(ReplicationCursor::new(2), SimulationTick::new(2), image(2)),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();
    assert_eq!(
        client
            .apply_full(
                key,
                FullSnapshot::new(ReplicationCursor::new(1), SimulationTick::new(3), image(1)),
                |_| Ok::<_, ()>(()),
            )
            .unwrap(),
        ClientSnapshotOutcome::Stale
    );
    assert_eq!(
        client
            .apply_full(
                key,
                FullSnapshot::new(ReplicationCursor::new(3), SimulationTick::new(1), image(3)),
                |_| Ok::<_, ()>(()),
            )
            .unwrap(),
        ClientSnapshotOutcome::TickRegression
    );

    let participant = ParticipantId::new(1);
    let mut authority = authority();
    authority.add_lineage(participant, retention()).unwrap();
    assert!(matches!(
        authority.lineage(participant).unwrap().replication_state(),
        AuthorityReplicationState::FullSnapshotRequired {
            reason: AuthorityRecoveryReason::InitialBaseline,
            ..
        }
    ));
    emit_full(&mut authority, participant, 1, 2, true);
    assert_eq!(
        authority.prepare_full(
            participant,
            FullSnapshot::new(ReplicationCursor::new(1), SimulationTick::new(3), image(1)),
            false,
        ),
        Err(AuthoritySessionError::Prepare(
            AuthorityPrepareError::CursorNotNewer
        ))
    );
    assert_eq!(
        authority.prepare_full(
            participant,
            FullSnapshot::new(ReplicationCursor::new(2), SimulationTick::new(1), image(2)),
            false,
        ),
        Err(AuthoritySessionError::Prepare(
            AuthorityPrepareError::TickRegression
        ))
    );
}

#[test]
fn stale_duplicate_and_repeat_ack_do_not_recommit() {
    let (mut client, key) = client();
    client
        .apply_full(
            key,
            FullSnapshot::new(ReplicationCursor::new(3), SimulationTick::new(3), image(3)),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();

    let mut commits = 0;
    assert_eq!(
        client
            .apply_full(
                key,
                FullSnapshot::new(ReplicationCursor::new(3), SimulationTick::new(3), image(30)),
                |_| {
                    commits += 1;
                    Ok::<_, ()>(())
                },
            )
            .unwrap(),
        ClientSnapshotOutcome::DuplicateCurrent
    );
    assert_eq!(
        client
            .apply_full(
                key,
                FullSnapshot::new(ReplicationCursor::new(2), SimulationTick::new(4), image(2)),
                |_| {
                    commits += 1;
                    Ok::<_, ()>(())
                },
            )
            .unwrap(),
        ClientSnapshotOutcome::Stale
    );
    assert_eq!(commits, 0);
    assert_eq!(
        client.lineage(key).unwrap().acknowledgement_cursor(),
        Some(ReplicationCursor::new(3))
    );
}

#[test]
fn malformed_and_reconstruction_failures_are_atomic_recovery_transitions() {
    let (mut malformed, malformed_key) = client();
    malformed
        .apply_full(
            malformed_key,
            FullSnapshot::new(ReplicationCursor::new(1), SimulationTick::new(1), image(1)),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();
    let malformed_outcome = malformed
        .apply_delta(
            malformed_key,
            DeltaSnapshot::new(
                ReplicationCursor::new(3),
                ReplicationCursor::new(2),
                SimulationTick::new(2),
                (),
            ),
            |_, _, _| Ok(image(2)),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();
    assert_eq!(malformed_outcome, ClientSnapshotOutcome::MalformedDelta);
    assert_eq!(
        malformed
            .lineage(malformed_key)
            .unwrap()
            .replication_state(),
        ClientReplicationState::FullSnapshotRequired(ClientRecoveryReason::MalformedDelta)
    );
    assert_eq!(
        malformed.lineage(malformed_key).unwrap().current_cursor(),
        Some(ReplicationCursor::new(1))
    );

    let (mut reconstruction, reconstruction_key) = client();
    reconstruction
        .apply_full(
            reconstruction_key,
            FullSnapshot::new(ReplicationCursor::new(1), SimulationTick::new(1), image(1)),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();
    let failed = reconstruction
        .apply_delta(
            reconstruction_key,
            DeltaSnapshot::new(
                ReplicationCursor::new(1),
                ReplicationCursor::new(2),
                SimulationTick::new(2),
                (),
            ),
            |_, _, _| Err(DeltaReconstructionError::ReconstructionFailed),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();
    assert_eq!(failed, ClientSnapshotOutcome::ReconstructionFailure);
    assert_eq!(
        reconstruction
            .lineage(reconstruction_key)
            .unwrap()
            .replication_state(),
        ClientReplicationState::FullSnapshotRequired(ClientRecoveryReason::ReconstructionFailure)
    );
    assert_eq!(
        reconstruction
            .lineage(reconstruction_key)
            .unwrap()
            .current_state()
            .unwrap(),
        &BTreeMap::from([("value", 1)])
    );
}

#[test]
fn authority_keeps_latest_confirmed_as_the_only_delta_base() {
    let participant = ParticipantId::new(1);
    let connection = ConnectionHandle::new(1);
    let session = authorized_session(participant, connection);
    let mut authority = authority();
    authority.add_lineage(participant, retention()).unwrap();
    emit_full(&mut authority, participant, 1, 1, true);
    authority
        .acknowledge_authorized(&session, connection, participant, ReplicationCursor::new(1))
        .unwrap();

    let second = authority
        .prepare_delta(
            participant,
            ReplicationCursor::new(2),
            SimulationTick::new(2),
            image(2),
            (),
            0,
        )
        .unwrap();
    assert_eq!(second.base_cursor, Some(ReplicationCursor::new(1)));
    authority
        .record_delivery_submission(participant, accepted())
        .unwrap();
    let third = authority
        .prepare_delta(
            participant,
            ReplicationCursor::new(3),
            SimulationTick::new(3),
            image(3),
            (),
            0,
        )
        .unwrap();
    assert_eq!(third.base_cursor, Some(ReplicationCursor::new(1)));
    authority
        .record_delivery_submission(participant, accepted())
        .unwrap();

    assert_eq!(
        authority
            .acknowledge_authorized(&session, connection, participant, ReplicationCursor::new(3))
            .unwrap(),
        AuthorityAckOutcome::Confirmed
    );
    assert_eq!(
        authority
            .acknowledge_authorized(&session, connection, participant, ReplicationCursor::new(3))
            .unwrap(),
        AuthorityAckOutcome::DuplicateConfirmation
    );
    assert_eq!(
        authority
            .acknowledge_authorized(&session, connection, participant, ReplicationCursor::new(2))
            .unwrap(),
        AuthorityAckOutcome::StaleConfirmation
    );

    let fourth = authority
        .prepare_delta(
            participant,
            ReplicationCursor::new(4),
            SimulationTick::new(4),
            image(4),
            (),
            0,
        )
        .unwrap();
    assert_eq!(fourth.base_cursor, Some(ReplicationCursor::new(3)));
}

#[test]
fn candidate_and_aggregate_resource_failures_do_not_partially_reserve() {
    let participant = ParticipantId::new(1);
    let tight_retention =
        ReplicationRetentionLimits::new(nz(8), nz(2), nz(16), nz(8), nz(2)).unwrap();
    let mut authority = authority();
    authority.add_lineage(participant, tight_retention).unwrap();
    assert_eq!(
        authority.prepare_full(
            participant,
            FullSnapshot::new(
                ReplicationCursor::new(1),
                SimulationTick::new(1),
                AccountedState::new(BTreeMap::new(), 9),
            ),
            true,
        ),
        Err(AuthoritySessionError::Prepare(
            AuthorityPrepareError::CandidateTooLarge
        ))
    );
    assert!(
        authority
            .lineage(participant)
            .unwrap()
            .pending_snapshot()
            .is_none()
    );

    let limits = AuthorityAggregateLimits::new(nz(2), nz(8), nz(2), nz(16), nz(2));
    let mut aggregate = AuthorityReplicationSession::<BTreeMap<&'static str, i32>, ()>::new(
        SessionId::new(1),
        limits,
    );
    let first = ParticipantId::new(1);
    let second = ParticipantId::new(2);
    aggregate.add_lineage(first, tight_retention).unwrap();
    aggregate.add_lineage(second, tight_retention).unwrap();
    aggregate
        .prepare_full(
            first,
            FullSnapshot::new(ReplicationCursor::new(1), SimulationTick::new(1), image(1)),
            true,
        )
        .unwrap();
    assert_eq!(
        aggregate.prepare_full(
            second,
            FullSnapshot::new(ReplicationCursor::new(1), SimulationTick::new(1), image(2)),
            true,
        ),
        Err(AuthoritySessionError::AggregateResourceLimitExceeded)
    );
    assert!(
        aggregate
            .lineage(first)
            .unwrap()
            .pending_snapshot()
            .is_some()
    );
    assert!(
        aggregate
            .lineage(second)
            .unwrap()
            .pending_snapshot()
            .is_none()
    );
}

#[test]
fn qualifying_recovery_full_must_still_be_baseline_available_at_ack() {
    let participant = ParticipantId::new(1);
    let connection = ConnectionHandle::new(1);
    let session = authorized_session(participant, connection);
    let mut authority = authority();
    authority.add_lineage(participant, retention()).unwrap();
    emit_full(&mut authority, participant, 1, 1, true);
    authority
        .acknowledge_authorized(&session, connection, participant, ReplicationCursor::new(1))
        .unwrap();
    authority
        .require_full_recovery(participant, AuthorityRecoveryReason::RecoveryDemand)
        .unwrap();
    emit_full(&mut authority, participant, 2, 2, true);
    authority
        .evict_retained_state(participant, ReplicationCursor::new(2))
        .unwrap();

    assert_eq!(
        authority
            .acknowledge_authorized(&session, connection, participant, ReplicationCursor::new(2))
            .unwrap(),
        AuthorityAckOutcome::Confirmed
    );
    assert!(matches!(
        authority.lineage(participant).unwrap().replication_state(),
        AuthorityReplicationState::FullSnapshotRequired { .. }
    ));
}

#[test]
fn client_connection_replacement_barrier_blocks_deltas_until_new_full() {
    let (mut client, key) = client();
    client
        .apply_full(
            key,
            FullSnapshot::new(ReplicationCursor::new(1), SimulationTick::new(1), image(1)),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();
    client.require_connection_replacement_full(key).unwrap();
    assert_eq!(
        client.lineage(key).unwrap().replication_state(),
        ClientReplicationState::FullSnapshotRequired(ClientRecoveryReason::ConnectionReplacement)
    );
    assert_eq!(
        client
            .apply_delta(
                key,
                DeltaSnapshot::new(
                    ReplicationCursor::new(1),
                    ReplicationCursor::new(2),
                    SimulationTick::new(2),
                    (),
                ),
                |_, _, _| Ok(image(2)),
                |_| Ok::<_, ()>(()),
            )
            .unwrap(),
        ClientSnapshotOutcome::DeltaBlockedByRecovery
    );
    assert_eq!(
        client
            .apply_full(
                key,
                FullSnapshot::new(ReplicationCursor::new(2), SimulationTick::new(2), image(2)),
                |_| Ok::<_, ()>(()),
            )
            .unwrap(),
        ClientSnapshotOutcome::Committed(ReplicationCursor::new(2))
    );
}

#[test]
fn lineage_teardown_releases_client_and_authority_accounting() {
    let key = ReplicationLineageKey::new(SessionId::new(1), ParticipantId::new(1));
    let mut client = ClientReplicationSet::new(ClientAggregateLimits::new(nz(2), nz(4), nz(64)));
    client.add_lineage(key, retention()).unwrap();
    client
        .apply_full(
            key,
            FullSnapshot::new(ReplicationCursor::new(1), SimulationTick::new(1), image(1)),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();
    assert_eq!(client.retained_image_count(), 1);
    assert_eq!(client.retained_state_bytes(), 8);
    assert!(client.remove_lineage(key));
    assert_eq!(client.retained_image_count(), 0);
    assert_eq!(client.retained_state_bytes(), 0);

    let participant = ParticipantId::new(1);
    let mut authority = authority();
    authority.add_lineage(participant, retention()).unwrap();
    emit_full(&mut authority, participant, 1, 1, true);
    assert_eq!(authority.retained_image_count(), 1);
    assert_eq!(authority.retained_state_bytes(), 8);
    assert_eq!(authority.emission_evidence_count(), 1);
    assert!(authority.remove_lineage(participant));
    assert_eq!(authority.retained_image_count(), 0);
    assert_eq!(authority.retained_state_bytes(), 0);
    assert_eq!(authority.emission_evidence_count(), 0);
}
