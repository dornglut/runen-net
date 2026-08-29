use std::num::NonZeroUsize;

use runen_net::{
    DeliveryAcceptance,
    identity::{ParticipantId, SessionId, SimulationTick},
    replication::{
        AccountedState, AuthorityAggregateLimits, AuthorityReplicationSession,
        AuthoritySessionError, FullSnapshot, ReplicationCursor, ReplicationRetentionLimits,
    },
};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).unwrap()
}

fn authority() -> AuthorityReplicationSession<u32, ()> {
    AuthorityReplicationSession::new(
        SessionId::new(1),
        AuthorityAggregateLimits::new(nz(2), nz(128), nz(4), nz(128), nz(4)),
    )
}

fn retention() -> ReplicationRetentionLimits {
    ReplicationRetentionLimits::new(nz(16), nz(2), nz(32), nz(16), nz(2)).unwrap()
}

#[test]
fn replication_consumes_only_delivery_acceptance_evidence() {
    let participant = ParticipantId::new(1);
    let mut replication = authority();
    replication.add_lineage(participant, retention()).unwrap();
    replication
        .prepare_full(
            participant,
            FullSnapshot::new(
                ReplicationCursor::new(1),
                SimulationTick::new(1),
                AccountedState::new(7, 4),
            ),
            true,
        )
        .unwrap();

    assert_eq!(
        replication
            .record_delivery_acceptance(participant, DeliveryAcceptance::NotAccepted)
            .unwrap(),
        None
    );
    assert!(
        replication
            .lineage(participant)
            .unwrap()
            .pending_snapshot()
            .is_some()
    );
    assert_eq!(
        replication
            .lineage(participant)
            .unwrap()
            .greatest_emitted_cursor(),
        None
    );

    let emitted = replication
        .record_delivery_acceptance(participant, DeliveryAcceptance::Accepted)
        .unwrap()
        .expect("accepted delivery must emit the pending snapshot");
    assert_eq!(emitted.target_cursor, ReplicationCursor::new(1));
    assert!(
        replication
            .lineage(participant)
            .unwrap()
            .pending_snapshot()
            .is_none()
    );
    assert_eq!(
        replication
            .lineage(participant)
            .unwrap()
            .greatest_emitted_cursor(),
        Some(ReplicationCursor::new(1))
    );
}

#[test]
fn non_acceptance_still_rejects_unknown_lineage() {
    let mut replication = authority();
    assert_eq!(
        replication
            .record_delivery_acceptance(ParticipantId::new(99), DeliveryAcceptance::NotAccepted,),
        Err(AuthoritySessionError::UnknownLineage)
    );
}
