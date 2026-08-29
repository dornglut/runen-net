use std::collections::BTreeMap;

use crate::identity::SimulationTick;
use crate::replication::{
    ClientRecoveryReason, ClientReplicationSet, ClientReplicationState, ClientSetError,
    ReplicationCursor, ReplicationLineageKey,
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
    ReplicationLineageMissing,
    NotReplayFailure,
    ReplicationNotSynchronized,
    MissingCommittedTick,
    FrontierRegression,
}

#[derive(Debug)]
pub enum PredictionReconciliationError<E> {
    ReplicationLineageMissing,
    CommittedCursorRegression {
        last_observed: ReplicationCursor,
        current: ReplicationCursor,
    },
    MissingCommittedTick,
    FrontierRegression,
    AccountingInvariantViolation,
    ReplayFailed {
        tick: SimulationTick,
        source: E,
    },
}

#[derive(Debug)]
pub struct PredictionLineage<I> {
    key: ReplicationLineageKey,
    limits: PredictionLimits,
    state: PredictionState,
    pending: BTreeMap<SimulationTick, PendingInput<I>>,
    pending_bytes: usize,
    // Non-authoritative observation evidence only. ClientLineage remains the
    // sole owner of the current authoritative replication cursor and state.
    last_observed_commit: Option<ReplicationCursor>,
    last_observed_recovery_boundary: Option<ReplicationCursor>,
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
            last_observed_recovery_boundary: None,
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
        if let Some((cursor, _)) = replication
            .lineage(self.key)
            .and_then(|lineage| lineage.latest_recovery_boundary())
        {
            self.last_observed_recovery_boundary = Some(cursor);
        }
        self.invalidate_for_recovery(ClientRecoveryReason::ConnectionReplacement);
        Ok(())
    }

    pub fn confirm_host_restored_after_replay_failure<S>(
        &mut self,
        replication: &ClientReplicationSet<S>,
    ) -> Result<SimulationTick, PredictionActivationError> {
        let lineage = replication
            .lineage(self.key)
            .ok_or(PredictionActivationError::ReplicationLineageMissing)?;
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
        let cursor = lineage
            .current_cursor()
            .expect("synchronized lineage with committed tick has a current cursor");
        if self.frontier().is_some_and(|frontier| tick < frontier) {
            return Err(PredictionActivationError::FrontierRegression);
        }
        self.pending.clear();
        self.pending_bytes = 0;
        self.state = PredictionState::Active { frontier: tick };
        self.last_observed_commit = Some(cursor);
        if let Some((boundary, _)) = lineage.latest_recovery_boundary() {
            self.last_observed_recovery_boundary = Some(boundary);
        }
        Ok(tick)
    }

    pub fn observe_replication<S, E, F>(
        &mut self,
        replication: &ClientReplicationSet<S>,
        replay: F,
    ) -> Result<PredictionReconciliationOutcome, PredictionReconciliationError<E>>
    where
        F: FnMut(SimulationTick, &I) -> Result<(), E>,
    {
        let lineage = replication
            .lineage(self.key)
            .ok_or(PredictionReconciliationError::ReplicationLineageMissing)?;

        let mut observed_new_recovery = false;
        if let Some((boundary, reason)) = lineage.latest_recovery_boundary()
            && self
                .last_observed_recovery_boundary
                .is_none_or(|last_observed| boundary > last_observed)
        {
            self.last_observed_recovery_boundary = Some(boundary);
            observed_new_recovery = true;
            self.invalidate_for_recovery(reason);
        }

        if let ClientReplicationState::FullSnapshotRequired(reason) = lineage.replication_state() {
            if let Some(terminal_reason) = self.terminal_reason() {
                return Ok(PredictionReconciliationOutcome::RemainsInvalidated {
                    reason: terminal_reason,
                });
            }
            if !observed_new_recovery {
                self.invalidate_for_recovery(reason);
            }
            return Ok(PredictionReconciliationOutcome::InvalidatedByRecovery { reason });
        }

        let Some(cursor) = lineage.current_cursor() else {
            return Ok(PredictionReconciliationOutcome::NoAuthoritativeCommit);
        };
        if let Some(terminal_reason) = self.terminal_reason() {
            return Ok(PredictionReconciliationOutcome::RemainsInvalidated {
                reason: terminal_reason,
            });
        }
        if let Some(last_observed) = self.last_observed_commit {
            if cursor < last_observed {
                return Err(PredictionReconciliationError::CommittedCursorRegression {
                    last_observed,
                    current: cursor,
                });
            }
            if cursor == last_observed {
                return Ok(PredictionReconciliationOutcome::NoAuthoritativeCommit);
            }
        }

        let tick = lineage
            .current_tick()
            .ok_or(PredictionReconciliationError::MissingCommittedTick)?;
        self.reconcile_committed(cursor, tick, replay)
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

    fn retention() -> ReplicationRetentionLimits {
        ReplicationRetentionLimits::new(nz(64), nz(4), nz(128), nz(64), nz(4)).unwrap()
    }

    fn replication(key: ReplicationLineageKey) -> ClientReplicationSet<u32> {
        let mut replication =
            ClientReplicationSet::new(ClientAggregateLimits::new(nz(2), nz(8), nz(256)));
        replication.add_lineage(key, retention()).unwrap();
        replication
    }

    fn full(cursor: u64, tick: u64, value: u32) -> FullSnapshot<u32> {
        FullSnapshot::new(
            ReplicationCursor::new(cursor),
            SimulationTick::new(tick),
            AccountedState::new(value, 4),
        )
    }

    #[test]
    fn repeated_live_observation_does_not_replay_twice() {
        let key = ReplicationLineageKey::new(SessionId::new(1), ParticipantId::new(1));
        let mut replication = replication(key);
        let mut prediction = PredictionLineage::new(key, PredictionLimits::new(nz(4), nz(32), 8));

        replication
            .apply_full(key, full(1, 10, 10), |_| Ok::<_, ()>(()))
            .unwrap();
        prediction
            .observe_replication(&replication, |_, _| Ok::<_, ()>(()))
            .unwrap();
        assert_eq!(
            prediction.admit_local(SimulationTick::new(11), &11, 4),
            PredictionInputOutcome::InputAccepted
        );

        replication
            .apply_full(key, full(2, 10, 20), |_| Ok::<_, ()>(()))
            .unwrap();
        let mut replayed = 0usize;
        assert_eq!(
            prediction
                .observe_replication(&replication, |_, _| {
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
                .observe_replication(&replication, |_, _| {
                    replayed += 1;
                    Ok::<_, ()>(())
                })
                .unwrap(),
            PredictionReconciliationOutcome::NoAuthoritativeCommit
        );
        assert_eq!(replayed, 1);
    }

    #[test]
    fn live_observation_reconciles_only_the_latest_unobserved_commit() {
        let key = ReplicationLineageKey::new(SessionId::new(2), ParticipantId::new(2));
        let mut replication = replication(key);
        let mut prediction = PredictionLineage::new(key, PredictionLimits::new(nz(4), nz(32), 8));

        replication
            .apply_full(key, full(1, 10, 10), |_| Ok::<_, ()>(()))
            .unwrap();
        prediction
            .observe_replication(&replication, |_, _| Ok::<_, ()>(()))
            .unwrap();
        assert_eq!(
            prediction.admit_local(SimulationTick::new(13), &13, 4),
            PredictionInputOutcome::InputAccepted
        );

        replication
            .apply_full(key, full(2, 11, 20), |_| Ok::<_, ()>(()))
            .unwrap();
        replication
            .apply_full(key, full(3, 12, 30), |_| Ok::<_, ()>(()))
            .unwrap();

        let mut replayed = Vec::new();
        assert_eq!(
            prediction
                .observe_replication(&replication, |target, value| {
                    replayed.push((target, *value));
                    Ok::<_, ()>(())
                })
                .unwrap(),
            PredictionReconciliationOutcome::ReconciledReplay {
                frontier: SimulationTick::new(12),
                replayed: 1
            }
        );
        assert_eq!(replayed, vec![(SimulationTick::new(13), 13)]);
        assert_eq!(prediction.frontier(), Some(SimulationTick::new(12)));
    }

    #[test]
    fn recovery_cleared_before_observation_still_discards_old_prediction_continuity() {
        let key = ReplicationLineageKey::new(SessionId::new(3), ParticipantId::new(3));
        let mut replication = replication(key);
        let mut prediction = PredictionLineage::new(key, PredictionLimits::new(nz(4), nz(32), 8));

        replication
            .apply_full(key, full(1, 10, 10), |_| Ok::<_, ()>(()))
            .unwrap();
        prediction
            .observe_replication(&replication, |_, _| Ok::<_, ()>(()))
            .unwrap();
        assert_eq!(
            prediction.admit_local(SimulationTick::new(12), &12, 4),
            PredictionInputOutcome::InputAccepted
        );

        replication
            .require_connection_replacement_full(key)
            .unwrap();
        replication
            .apply_full(key, full(2, 11, 20), |_| Ok::<_, ()>(()))
            .unwrap();

        let mut replay_called = false;
        assert_eq!(
            prediction
                .observe_replication(&replication, |_, _| {
                    replay_called = true;
                    Ok::<_, ()>(())
                })
                .unwrap(),
            PredictionReconciliationOutcome::ActivatedFromAuthoritative {
                frontier: SimulationTick::new(11)
            }
        );
        assert!(!replay_called);
        assert_eq!(prediction.pending_count(), 0);
        assert_eq!(prediction.frontier(), Some(SimulationTick::new(11)));
    }

    #[test]
    fn missing_replication_lineage_is_explicit() {
        let key = ReplicationLineageKey::new(SessionId::new(4), ParticipantId::new(4));
        let replication =
            ClientReplicationSet::<u32>::new(ClientAggregateLimits::new(nz(2), nz(8), nz(256)));
        let mut prediction =
            PredictionLineage::<u32>::new(key, PredictionLimits::new(nz(4), nz(32), 8));

        assert!(matches!(
            prediction.observe_replication(&replication, |_, _| Ok::<_, ()>(())),
            Err(PredictionReconciliationError::ReplicationLineageMissing)
        ));
    }

    #[test]
    fn replaced_lineage_with_older_cursor_fails_closed() {
        let key = ReplicationLineageKey::new(SessionId::new(5), ParticipantId::new(5));
        let mut replication = replication(key);
        let mut prediction =
            PredictionLineage::<u32>::new(key, PredictionLimits::new(nz(4), nz(32), 8));

        replication
            .apply_full(key, full(5, 10, 50), |_| Ok::<_, ()>(()))
            .unwrap();
        prediction
            .observe_replication(&replication, |_, _| Ok::<_, ()>(()))
            .unwrap();

        assert!(replication.remove_lineage(key));
        replication.add_lineage(key, retention()).unwrap();
        replication
            .apply_full(key, full(4, 11, 40), |_| Ok::<_, ()>(()))
            .unwrap();

        match prediction.observe_replication(&replication, |_, _| Ok::<_, ()>(())) {
            Err(PredictionReconciliationError::CommittedCursorRegression {
                last_observed,
                current,
            }) => {
                assert_eq!(last_observed, ReplicationCursor::new(5));
                assert_eq!(current, ReplicationCursor::new(4));
            }
            other => panic!("unexpected observation result: {other:?}"),
        }
    }
}
