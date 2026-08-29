mod authority;
mod client;
mod model;

use crate::{
    DeliveryAcceptance,
    delivery::SubmissionOutcome,
    identity::ParticipantId,
};

pub use authority::{
    AuthorityAckOutcome, AuthorityLineage, AuthorityOperationError, AuthorityPrepareError,
    AuthorityRecoveryReason, AuthorityReplicationSession, AuthorityReplicationState,
    AuthoritySessionError, EmittedSnapshot, PendingSnapshotRef, PendingSnapshotSummary,
    SnapshotKind,
};
pub use client::{
    ClientLineage, ClientRecoveryReason, ClientReplicationSet, ClientReplicationState,
    ClientSetError, ClientSnapshotOutcome, DeltaReconstructionError,
};
pub use model::{
    AccountedState, AuthorityAggregateLimits, ClientAggregateLimits, DeltaSnapshot, FullSnapshot,
    RecoveryGeneration, ReplicationCursor, ReplicationLimitError, ReplicationLineageKey,
    ReplicationRetentionLimits,
};

impl<S, D> AuthorityReplicationSession<S, D> {
    /// Record whether the complete message carrying the pending snapshot was accepted into its
    /// selected RunenNet delivery contract.
    ///
    /// Replication deliberately consumes only this acceptance fact. Detailed delivery or
    /// transport-adapter rejection reasons remain owned by the outcome that produced the evidence.
    pub fn record_delivery_acceptance(
        &mut self,
        participant: ParticipantId,
        acceptance: DeliveryAcceptance,
    ) -> Result<Option<EmittedSnapshot>, AuthoritySessionError> {
        match acceptance {
            DeliveryAcceptance::NotAccepted => {
                if self.lineage(participant).is_none() {
                    return Err(AuthoritySessionError::UnknownLineage);
                }
                Ok(None)
            }
            DeliveryAcceptance::Accepted => self.record_delivery_submission(
                participant,
                SubmissionOutcome::Accepted {
                    accepted_index: 0,
                    local_pressure_drops: 0,
                },
            ),
        }
    }
}
