use std::num::NonZeroUsize;

use runen_net::{
    delivery::{DeliveryEndpoint, DeliveryFlowKey},
    identity::{ParticipantId, SessionId, SimulationTick},
    replication::{
        AccountedState, AuthorityAggregateLimits, AuthorityReplicationSession, FullSnapshot,
        ReplicationCursor, ReplicationRetentionLimits,
    },
};
use runen_net_quic::{Connection, SubmissionError, SubmitOutcome};

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
fn public_connection_submission_result_composes_directly_with_replication() {
    let _submit_signature: fn(
        &mut Connection,
        &mut DeliveryEndpoint,
        DeliveryFlowKey,
        Vec<u8>,
    ) -> Result<SubmitOutcome, SubmissionError> = Connection::submit;

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

    let adapter_pre_core_rejection = SubmitOutcome::RejectedCurrentDatagramSize;
    assert_eq!(
        replication
            .record_delivery_acceptance(participant, adapter_pre_core_rejection.acceptance())
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

    let accepted = SubmitOutcome::Accepted {
        accepted_index: 4,
        local_pressure_drops: 0,
    };
    replication
        .record_delivery_acceptance(participant, accepted.acceptance())
        .unwrap()
        .expect("public QUIC acceptance must record replication emission");
    assert_eq!(
        replication
            .lineage(participant)
            .unwrap()
            .greatest_emitted_cursor(),
        Some(ReplicationCursor::new(1))
    );
}
