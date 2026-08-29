use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use runen_net::DeliveryAcceptance;
use runen_net::identity::{ConnectionHandle, ParticipantId, SessionId, SimulationTick};
use runen_net::protocol::{
    CompatibilityOffer, NegotiatedContract, NegotiationManager, NegotiationManagerLimits,
    NegotiationRequirements, OfferLimits, ProtocolContract, ProtocolId, ProtocolRevision,
};
use runen_net::replication::{
    AccountedState, AuthorityAckOutcome, AuthorityAggregateLimits, AuthorityRecoveryReason,
    AuthorityReplicationSession, AuthorityReplicationState, AuthoritySessionError,
    ClientAggregateLimits, ClientRecoveryReason, ClientReplicationSet, ClientReplicationState,
    ClientSnapshotOutcome, FullSnapshot, ReplicationCursor, ReplicationLineageKey,
    ReplicationRetentionLimits,
};
use runen_net::session::{Session, SessionLimits};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn state(value: i32, accounted_bytes: usize) -> AccountedState<BTreeMap<&'static str, i32>> {
    AccountedState::new(BTreeMap::from([("value", value)]), accounted_bytes)
}

fn retention() -> ReplicationRetentionLimits {
    ReplicationRetentionLimits::new(nz(8), nz(4), nz(16), nz(8), nz(4)).unwrap()
}

fn protocol() -> ProtocolContract {
    ProtocolContract::new(ProtocolId::new(1), ProtocolRevision::new(1))
}

fn authorized_session(
    session_id: SessionId,
    participant: ParticipantId,
    connection: ConnectionHandle,
) -> Session {
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

    let mut session = Session::new(session_id, SessionLimits::new(nz(8), nz(4)).unwrap());
    session
        .admit_new(participant, manager.established(connection).unwrap())
        .unwrap();
    session
}

const fn accepted() -> DeliveryAcceptance {
    DeliveryAcceptance::Accepted
}

fn emit_full(
    authority: &mut AuthorityReplicationSession<BTreeMap<&'static str, i32>, ()>,
    participant: ParticipantId,
    cursor: u64,
    tick: u64,
    value: i32,
    bytes: usize,
    recovery: bool,
) {
    authority
        .prepare_full(
            participant,
            FullSnapshot::new(
                ReplicationCursor::new(cursor),
                SimulationTick::new(tick),
                state(value, bytes),
            ),
            recovery,
        )
        .unwrap();
    authority
        .record_delivery_acceptance(participant, accepted())
        .unwrap()
        .expect("accepted full is emitted");
}

#[test]
fn authority_recovery_survives_aggregate_rejection_and_retries_same_generation() {
    let session_id = SessionId::new(1);
    let participant = ParticipantId::new(1);
    let other = ParticipantId::new(2);
    let connection = ConnectionHandle::new(10);
    let session = authorized_session(session_id, participant, connection);
    let mut authority = AuthorityReplicationSession::<BTreeMap<&'static str, i32>, ()>::new(
        session_id,
        AuthorityAggregateLimits::new(nz(4), nz(12), nz(8), nz(12), nz(8)),
    );
    authority.add_lineage(participant, retention()).unwrap();
    authority.add_lineage(other, retention()).unwrap();

    emit_full(&mut authority, participant, 1, 1, 1, 4, true);
    assert_eq!(
        authority
            .acknowledge_authorized(&session, connection, participant, ReplicationCursor::new(1),)
            .unwrap(),
        AuthorityAckOutcome::Confirmed
    );
    emit_full(&mut authority, other, 1, 1, 20, 4, true);
    assert_eq!(authority.retained_state_bytes(), 8);

    authority
        .require_full_recovery(participant, AuthorityRecoveryReason::RecoveryDemand)
        .unwrap();
    let recovery_before = authority.lineage(participant).unwrap().replication_state();
    let latest_before = authority
        .lineage(participant)
        .unwrap()
        .latest_confirmed_cursor();
    let greatest_before = authority
        .lineage(participant)
        .unwrap()
        .greatest_emitted_cursor();
    let retained_before = authority.retained_state_bytes();
    let evidence_before = authority.emission_evidence_count();

    assert_eq!(
        authority.prepare_full(
            participant,
            FullSnapshot::new(
                ReplicationCursor::new(2),
                SimulationTick::new(2),
                state(2, 8),
            ),
            true,
        ),
        Err(AuthoritySessionError::AggregateResourceLimitExceeded)
    );
    let lineage = authority.lineage(participant).unwrap();
    assert_eq!(lineage.replication_state(), recovery_before);
    assert_eq!(lineage.latest_confirmed_cursor(), latest_before);
    assert_eq!(lineage.greatest_emitted_cursor(), greatest_before);
    assert!(lineage.pending_snapshot().is_none());
    assert_eq!(authority.retained_state_bytes(), retained_before);
    assert_eq!(authority.emission_evidence_count(), evidence_before);

    let summary = authority
        .prepare_full(
            participant,
            FullSnapshot::new(
                ReplicationCursor::new(2),
                SimulationTick::new(2),
                state(2, 4),
            ),
            true,
        )
        .unwrap();
    let generation_before = match recovery_before {
        AuthorityReplicationState::FullSnapshotRequired { generation, .. } => generation,
        other => panic!("expected recovery state, got {other:?}"),
    };
    assert_eq!(summary.recovery_generation, Some(generation_before));
    authority
        .record_delivery_acceptance(participant, accepted())
        .unwrap()
        .expect("conforming retry is emitted");
    assert_eq!(
        authority
            .acknowledge_authorized(&session, connection, participant, ReplicationCursor::new(2),)
            .unwrap(),
        AuthorityAckOutcome::Confirmed
    );
    assert_eq!(
        authority.lineage(participant).unwrap().replication_state(),
        AuthorityReplicationState::DeltaEligible(ReplicationCursor::new(2))
    );
}

#[test]
fn client_recovery_survives_aggregate_rejection_without_host_commit_and_retries() {
    let key = ReplicationLineageKey::new(SessionId::new(2), ParticipantId::new(1));
    let mut client = ClientReplicationSet::new(ClientAggregateLimits::new(nz(2), nz(4), nz(8)));
    client.add_lineage(key, retention()).unwrap();
    client
        .apply_full(
            key,
            FullSnapshot::new(
                ReplicationCursor::new(1),
                SimulationTick::new(1),
                state(1, 4),
            ),
            |_| Ok::<_, ()>(()),
        )
        .unwrap();
    client.require_connection_replacement_full(key).unwrap();

    let state_before = client
        .lineage(key)
        .unwrap()
        .current_state()
        .unwrap()
        .clone();
    let cursor_before = client.lineage(key).unwrap().current_cursor();
    let bytes_before = client.retained_state_bytes();
    let recovery_before = client.lineage(key).unwrap().replication_state();
    assert_eq!(
        recovery_before,
        ClientReplicationState::FullSnapshotRequired(ClientRecoveryReason::ConnectionReplacement)
    );

    let mut host_commits = 0usize;
    assert_eq!(
        client
            .apply_full(
                key,
                FullSnapshot::new(
                    ReplicationCursor::new(2),
                    SimulationTick::new(2),
                    state(2, 8),
                ),
                |_| {
                    host_commits += 1;
                    Ok::<_, ()>(())
                },
            )
            .unwrap(),
        ClientSnapshotOutcome::StateResourceFailure
    );
    assert_eq!(host_commits, 0);
    let lineage = client.lineage(key).unwrap();
    assert_eq!(lineage.replication_state(), recovery_before);
    assert_eq!(lineage.current_cursor(), cursor_before);
    assert_eq!(lineage.current_state(), Some(&state_before));
    assert_eq!(client.retained_state_bytes(), bytes_before);

    assert_eq!(
        client
            .apply_full(
                key,
                FullSnapshot::new(
                    ReplicationCursor::new(2),
                    SimulationTick::new(2),
                    state(2, 4),
                ),
                |_| {
                    host_commits += 1;
                    Ok::<_, ()>(())
                },
            )
            .unwrap(),
        ClientSnapshotOutcome::Committed(ReplicationCursor::new(2))
    );
    assert_eq!(host_commits, 1);
    assert_eq!(
        client.lineage(key).unwrap().replication_state(),
        ClientReplicationState::Synchronized
    );
    assert_eq!(
        client.lineage(key).unwrap().current_state(),
        Some(&BTreeMap::from([("value", 2)]))
    );
}
