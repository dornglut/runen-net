use std::num::NonZeroUsize;

use runen_net::identity::{ParticipantId, SessionId, SimulationTick};
use runen_net::replication::{
    AccountedState, AuthorityAggregateLimits, AuthorityReplicationSession, AuthoritySessionError,
    ClientAggregateLimits, ClientReplicationSet, ClientSnapshotOutcome, FullSnapshot,
    ReplicationCursor, ReplicationLineageKey, ReplicationRetentionLimits,
};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn extreme_retention() -> ReplicationRetentionLimits {
    ReplicationRetentionLimits::new(
        nz(usize::MAX),
        nz(2),
        nz(usize::MAX),
        nz(usize::MAX),
        nz(2),
    )
    .unwrap()
}

#[test]
fn authority_aggregate_overflow_rejects_second_candidate_without_mutation() {
    let first = ParticipantId::new(1);
    let second = ParticipantId::new(2);
    let first_bytes = usize::MAX - 8;
    let mut authority = AuthorityReplicationSession::<(), ()>::new(
        SessionId::new(1),
        AuthorityAggregateLimits::new(
            nz(2),
            nz(usize::MAX),
            nz(4),
            nz(usize::MAX),
            nz(4),
        ),
    );
    authority.add_lineage(first, extreme_retention()).unwrap();
    authority
        .add_lineage(second, extreme_retention())
        .unwrap();

    authority
        .prepare_full(
            first,
            FullSnapshot::new(
                ReplicationCursor::new(1),
                SimulationTick::new(1),
                AccountedState::new((), first_bytes),
            ),
            true,
        )
        .unwrap();
    assert_eq!(authority.accounted_state_bytes(), first_bytes);

    assert_eq!(
        authority.prepare_full(
            second,
            FullSnapshot::new(
                ReplicationCursor::new(1),
                SimulationTick::new(1),
                AccountedState::new((), 16),
            ),
            true,
        ),
        Err(AuthoritySessionError::AggregateResourceLimitExceeded)
    );
    assert!(
        authority
            .lineage(first)
            .unwrap()
            .pending_snapshot()
            .is_some()
    );
    assert!(
        authority
            .lineage(second)
            .unwrap()
            .pending_snapshot()
            .is_none()
    );
    assert_eq!(authority.accounted_state_bytes(), first_bytes);
}

#[test]
fn client_projection_overflow_fails_before_host_commit_and_preserves_state() {
    let first = ReplicationLineageKey::new(SessionId::new(1), ParticipantId::new(1));
    let second = ReplicationLineageKey::new(SessionId::new(1), ParticipantId::new(2));
    let first_bytes = usize::MAX - 32;
    let mut client = ClientReplicationSet::new(ClientAggregateLimits::new(
        nz(2),
        nz(4),
        nz(usize::MAX),
    ));
    client.add_lineage(first, extreme_retention()).unwrap();
    client.add_lineage(second, extreme_retention()).unwrap();

    assert_eq!(
        client
            .apply_full(
                first,
                FullSnapshot::new(
                    ReplicationCursor::new(1),
                    SimulationTick::new(1),
                    AccountedState::new((), first_bytes),
                ),
                |_| Ok::<_, ()>(()),
            )
            .unwrap(),
        ClientSnapshotOutcome::Committed(ReplicationCursor::new(1))
    );
    assert_eq!(
        client
            .apply_full(
                second,
                FullSnapshot::new(
                    ReplicationCursor::new(1),
                    SimulationTick::new(1),
                    AccountedState::new((), 8),
                ),
                |_| Ok::<_, ()>(()),
            )
            .unwrap(),
        ClientSnapshotOutcome::Committed(ReplicationCursor::new(1))
    );
    assert_eq!(client.retained_state_bytes(), usize::MAX - 24);

    let mut host_commit_called = false;
    assert_eq!(
        client
            .apply_full(
                second,
                FullSnapshot::new(
                    ReplicationCursor::new(2),
                    SimulationTick::new(2),
                    AccountedState::new((), 64),
                ),
                |_| {
                    host_commit_called = true;
                    Ok::<_, ()>(())
                },
            )
            .unwrap(),
        ClientSnapshotOutcome::StateResourceFailure
    );
    assert!(!host_commit_called);
    assert_eq!(
        client.lineage(second).unwrap().current_cursor(),
        Some(ReplicationCursor::new(1))
    );
    assert_eq!(client.retained_state_bytes(), usize::MAX - 24);
}
