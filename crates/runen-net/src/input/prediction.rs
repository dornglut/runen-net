use std::collections::BTreeMap;

use crate::identity::SimulationTick;
use crate::replication::{
    ClientLineage, ClientRecoveryReason, ClientReplicationSet, ClientReplicationState,
    ClientSetError, ClientSnapshotOutcome, ReplicationCursor, ReplicationLineageKey,
};

use super::model::PredictionLimits;

#[derive(Debug, Clone)]
struct PendingInput<I> {
    value: I,
    accounted_bytes: usize,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum PredictionInvalidationReason {
    InitialBaseline,
    ReplicationRecovery(ClientRecoveryReason),
    ConnectionLoss,
    ReplayFailure,
    ParticipantMembershipEnded,
    SessionClosed,
}

impl PredictionInvalidationReason {
    const fn is_terminal(self) -> bool {
        matches!(self, Self::ParticipantMembershipEnded | Self::SessionClosed)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum PredictionState {
    Active {
        frontier: SimulationTick,
    },
    Invalidated {
        reason: PredictionInvalidationReason,
        frontier: Option<SimulationTick>,
    },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum PredictionInputOutcome {
    InputAccepted,
    DuplicateInput,
    ConflictingInput,
    PredictionInputNotNewerThanFrontier,
    PendingPredictionResourceRejected,
    PredictionInvalidated(PredictionInvalidationReason),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum PredictionReconciliationOutcome {
    NoAuthoritativeCommit,
    AlreadyObservedCommit {
        cursor: ReplicationCursor,
    },
    ActivatedFromAuthoritative {
        frontier: SimulationTick,
    },
    ReconciledNoReplay {
        frontier: SimulationTick,
    },
    ReconciledReplay {
        frontier: SimulationTick,
        replayed: usize,
    },
    InvalidatedByRecovery {
        reason: ClientRecoveryReason,
    },
    RemainsInvalidated {
        reason: PredictionInvalidationReason,
    },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum PredictionActivationError {
    LineageMismatch,
    NotReplayFailure,
    ReplicationNotSynchronized,
    MissingCommittedTick,
    FrontierRegression,
}

#[derive(Debug)]
pub enum PredictionReconciliationError<E> {
    LineageMismatch,
    CommittedCursorMismatch,
    MissingCommittedTick,
    FrontierRegression,
    AccountingInvariantViolation,
    ReplayFailed { tick: SimulationTick, source: E },
}

#[derive(Debug)]
pub struct PredictionLineage<I> {
    key: ReplicationLineageKey,
    limits: PredictionLimits,
    state: PredictionState,
    pending: BTreeMap<SimulationTick, PendingInput<I>>,
    pending_bytes: usize,
    // Non-authoritative idempotence evidence only. ClientLineage remains the
    // sole owner of the current authoritative replication cursor and state.
    last_observed_commit: Option<ReplicationCursor>,
}

impl<I> PredictionLineage<I> {
    pub fn new(key: ReplicationLineageKey, limits: PredictionLimits) -> Self {
        Self {
            key,
            limits,
            state: PredictionState::Invalidated {
                reason: PredictionInvalidationReason::InitialBaseline,
                frontier: None,
            },
            pending: BTreeMap::new(),
            pending_bytes: 0,
            last_observed_commit: None,
        }
    }

    pub const fn key(&self) -> ReplicationLineageKey {
        self.key
    }

    pub const fn state(&self) -> PredictionState {
        self.state
    }

    pub const fn frontier(&self) -> Option<SimulationTick> {
        match self.state {
            PredictionState::Active { frontier } => Some(frontier),
            PredictionState::Invalidated { frontier, .. } => frontier,
        }
    }

    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    pub const fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    pub fn pending_input(&self, tick: SimulationTick) -> Option<&I> {
        self.pending.get(&tick).map(|pending| &pending.value)
    }

    pub fn admit_local(
        &mut self,
        target_tick: SimulationTick,
        input: &I,
        accounted_bytes: usize,
    ) -> PredictionInputOutcome
    where
        I: Clone + Eq,
    {
        let frontier = match self.state {
            PredictionState::Active { frontier } => frontier,
            PredictionState::Invalidated { reason, .. } => {
                return PredictionInputOutcome::PredictionInvalidated(reason);
            }
        };

        if target_tick <= frontier {
            return PredictionInputOutcome::PredictionInputNotNewerThanFrontier;
        }
        if let Some(pending) = self.pending.get(&target_tick) {
            return if pending.value == *input {
                PredictionInputOutcome::DuplicateInput
            } else {
                PredictionInputOutcome::ConflictingInput
            };
        }

        let future_distance = target_tick.get() - frontier.get();
        if future_distance > self.limits.max_future_tick_distance()
            || accounted_bytes > self.limits.max_pending_bytes()
            || self.pending.len() >= self.limits.max_pending_inputs()
        {
            return PredictionInputOutcome::PendingPredictionResourceRejected;
        }
        let Some(next_pending_bytes) = self.pending_bytes.checked_add(accounted_bytes) else {
            return PredictionInputOutcome::PendingPredictionResourceRejected;
        };
        if next_pending_bytes > self.limits.max_pending_bytes() {
            return PredictionInputOutcome::PendingPredictionResourceRejected;
        }

        let previous = self.pending.insert(
            target_tick,
            PendingInput {
                value: input.clone(),
                accounted_bytes,
            },
        );
        debug_assert!(previous.is_none());
        self.pending_bytes = next_pending_bytes;
        PredictionInputOutcome::InputAccepted
    }

    pub fn connection_lost(&mut self) {
        if self.terminal_reason().is_none() {
            self.invalidate(PredictionInvalidationReason::ConnectionLoss);
        }
    }

    pub fn participant_membership_ended(&mut self) {
        self.terminate(PredictionInvalidationReason::ParticipantMembershipEnded);
    }

    pub fn session_closed(&mut self) {
        self.terminate(PredictionInvalidationReason::SessionClosed);
    }

    pub fn invalidate_for_recovery(&mut self, reason: ClientRecoveryReason) {
        if self.terminal_reason().is_none() {
            self.invalidate(PredictionInvalidationReason::ReplicationRecovery(reason));
        }
    }

    pub fn require_connection_replacement_full<S>(
        &mut self,
        replication: &mut ClientReplicationSet<S>,
    ) -> Result<(), ClientSetError> {
        replication.require_connection_replacement_full(self.key)?;
        self.invalidate_for_recovery(ClientRecoveryReason::ConnectionReplacement);
        Ok(())
    }

    pub fn confirm_host_restored_after_replay_failure<S>(
        &mut self,
        lineage: &ClientLineage<S>,
    ) -> Result<SimulationTick, PredictionActivationError> {
        if lineage.key() != self.key {
            return Err(PredictionActivationError::LineageMismatch);
        }
        if !matches!(
            self.state,
            PredictionState::Invalidated {
                reason: PredictionInvalidationReason::ReplayFailure,
                ..
            }
        ) {
            return Err(PredictionActivationError::NotReplayFailure);
        }
        if !matches!(
            lineage.replication_state(),
            ClientReplicationState::Synchronized
        ) {
            return Err(PredictionActivationError::ReplicationNotSynchronized);
        }
        let tick = lineage
            .current_tick()
            .ok_or(PredictionActivationError::MissingCommittedTick)?;
        if self.frontier().is_some_and(|frontier| tick < frontier) {
            return Err(PredictionActivationError::FrontierRegression);
        }
        self.pending.clear();
        self.pending_bytes = 0;
        self.state = PredictionState::Active { frontier: tick };
        Ok(tick)
    }

    pub fn observe_replication<S, E, F>(
        &mut self,
        outcome: ClientSnapshotOutcome,
        lineage: &ClientLineage<S>,
        replay: F,
    ) -> Result<PredictionReconciliationOutcome, PredictionReconciliationError<E>>
    where
        F: FnMut(SimulationTick, &I) -> Result<(), E>,
    {
        if lineage.key() != self.key {
            return Err(PredictionReconciliationError::LineageMismatch);
        }

        if let ClientSnapshotOutcome::Committed(cursor) = outcome {
            if lineage.current_cursor() != Some(cursor) {
                return Err(PredictionReconciliationError::CommittedCursorMismatch);
            }
            if let Some(terminal_reason) = self.terminal_reason() {
                return Ok(PredictionReconciliationOutcome::RemainsInvalidated {
                    reason: terminal_reason,
                });
            }
            if self.last_observed_commit == Some(cursor) {
                return Ok(PredictionReconciliationOutcome::AlreadyObservedCommit { cursor });
            }
            let tick = lineage
                .current_tick()
                .ok_or(PredictionReconciliationError::MissingCommittedTick)?;
            return self.reconcile_committed(cursor, tick, replay);
        }

        if let ClientReplicationState::FullSnapshotRequired(reason) = lineage.replication_state() {
            if let Some(terminal_reason) = self.terminal_reason() {
                return Ok(PredictionReconciliationOutcome::RemainsInvalidated {
                    reason: terminal_reason,
                });
            }
            self.invalidate_for_recovery(reason);
            return Ok(PredictionReconciliationOutcome::InvalidatedByRecovery { reason });
        }

        Ok(PredictionReconciliationOutcome::NoAuthoritativeCommit)
    }

    fn reconcile_committed<E, F>(
        &mut self,
        cursor: ReplicationCursor,
        tick: SimulationTick,
        mut replay: F,
    ) -> Result<PredictionReconciliationOutcome, PredictionReconciliationError<E>>
    where
        F: FnMut(SimulationTick, &I) -> Result<(), E>,
    {
        if self.frontier().is_some_and(|frontier| tick < frontier) {
            return Err(PredictionReconciliationError::FrontierRegression);
        }

        if let PredictionState::Invalidated { reason, .. } = self.state {
            self.last_observed_commit = Some(cursor);
            if matches!(
                reason,
                PredictionInvalidationReason::InitialBaseline
                    | PredictionInvalidationReason::ReplicationRecovery(_)
            ) {
                self.pending.clear();
                self.pending_bytes = 0;
                self.state = PredictionState::Active { frontier: tick };
                return Ok(
                    PredictionReconciliationOutcome::ActivatedFromAuthoritative { frontier: tick },
                );
            }
            return Ok(PredictionReconciliationOutcome::RemainsInvalidated { reason });
        }

        let mut retired_ticks = Vec::new();
        let mut retired_bytes = 0usize;
        for (target_tick, pending) in self.pending.range(..=tick) {
            retired_ticks.push(*target_tick);
            retired_bytes = retired_bytes
                .checked_add(pending.accounted_bytes)
                .ok_or(PredictionReconciliationError::AccountingInvariantViolation)?;
        }
        let next_pending_bytes = self
            .pending_bytes
            .checked_sub(retired_bytes)
            .ok_or(PredictionReconciliationError::AccountingInvariantViolation)?;

        self.last_observed_commit = Some(cursor);
        self.state = PredictionState::Active { frontier: tick };
        for target_tick in retired_ticks {
            self.pending.remove(&target_tick);
        }
        self.pending_bytes = next_pending_bytes;

        if self.pending.is_empty() {
            return Ok(PredictionReconciliationOutcome::ReconciledNoReplay { frontier: tick });
        }

        let mut replayed = 0usize;
        let mut failure = None;
        for (target_tick, pending) in &self.pending {
            match replay(*target_tick, &pending.value) {
                Ok(()) => {
                    replayed = replayed
                        .checked_add(1)
                        .ok_or(PredictionReconciliationError::AccountingInvariantViolation)?;
                }
                Err(source) => {
                    failure = Some((*target_tick, source));
                    break;
                }
            }
        }

        if let Some((failed_tick, source)) = failure {
            self.invalidate(PredictionInvalidationReason::ReplayFailure);
            return Err(PredictionReconciliationError::ReplayFailed {
                tick: failed_tick,
                source,
            });
        }

        Ok(PredictionReconciliationOutcome::ReconciledReplay {
            frontier: tick,
            replayed,
        })
    }

    fn terminal_reason(&self) -> Option<PredictionInvalidationReason> {
        match self.state {
            PredictionState::Invalidated { reason, .. } if reason.is_terminal() => Some(reason),
            _ => None,
        }
    }

    fn invalidate(&mut self, reason: PredictionInvalidationReason) {
        let frontier = self.frontier();
        self.pending.clear();
        self.pending_bytes = 0;
        self.state = PredictionState::Invalidated { reason, frontier };
    }

    fn terminate(&mut self, reason: PredictionInvalidationReason) {
        debug_assert!(reason.is_terminal());
        self.pending.clear();
        self.pending_bytes = 0;
        self.state = PredictionState::Invalidated {
            reason,
            frontier: None,
        };
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use super::*;
    use crate::identity::{ParticipantId, SessionId};
    use crate::replication::{
        AccountedState, ClientAggregateLimits, FullSnapshot, ReplicationRetentionLimits,
    };

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    #[test]
    fn repeated_commit_observation_does_not_replay_twice() {
        let key = ReplicationLineageKey::new(SessionId::new(1), ParticipantId::new(1));
        let retention =
            ReplicationRetentionLimits::new(nz(64), nz(4), nz(128), nz(64), nz(4)).unwrap();
        let mut replication =
            ClientReplicationSet::new(ClientAggregateLimits::new(nz(2), nz(8), nz(256)));
        replication.add_lineage(key, retention).unwrap();
        let mut prediction = PredictionLineage::new(key, PredictionLimits::new(nz(4), nz(32), 8));

        let initial = replication
            .apply_full(
                key,
                FullSnapshot::new(
                    ReplicationCursor::new(1),
                    SimulationTick::new(10),
                    AccountedState::new(10u32, 4),
                ),
                |_| Ok::<_, ()>(()),
            )
            .unwrap();
        prediction
            .observe_replication(initial, replication.lineage(key).unwrap(), |_, _| {
                Ok::<_, ()>(())
            })
            .unwrap();
        assert_eq!(
            prediction.admit_local(SimulationTick::new(11), &11, 4),
            PredictionInputOutcome::InputAccepted
        );

        let committed_cursor = ReplicationCursor::new(2);
        let committed = replication
            .apply_full(
                key,
                FullSnapshot::new(
                    committed_cursor,
                    SimulationTick::new(10),
                    AccountedState::new(20u32, 4),
                ),
                |_| Ok::<_, ()>(()),
            )
            .unwrap();
        let mut replayed = 0usize;
        assert_eq!(
            prediction
                .observe_replication(committed, replication.lineage(key).unwrap(), |_, _| {
                    replayed += 1;
                    Ok::<_, ()>(())
                })
                .unwrap(),
            PredictionReconciliationOutcome::ReconciledReplay {
                frontier: SimulationTick::new(10),
                replayed: 1
            }
        );
        assert_eq!(replayed, 1);

        assert_eq!(
            prediction
                .observe_replication(committed, replication.lineage(key).unwrap(), |_, _| {
                    replayed += 1;
                    Ok::<_, ()>(())
                })
                .unwrap(),
            PredictionReconciliationOutcome::AlreadyObservedCommit {
                cursor: committed_cursor
            }
        );
        assert_eq!(replayed, 1);
    }
}
