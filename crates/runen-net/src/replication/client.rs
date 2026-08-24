use std::collections::{BTreeMap, HashMap};

use crate::identity::SimulationTick;

use super::model::{
    AccountedState, ClientAggregateLimits, DeltaSnapshot, FullSnapshot, ReplicationCursor,
    ReplicationLineageKey, ReplicationRetentionLimits,
};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ClientRecoveryReason {
    InitialBaseline,
    MissingBase,
    MalformedDelta,
    ReconstructionFailure,
    DeltaTickRegression,
    DeltaCommitFailure,
    ConnectionReplacement,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ClientReplicationState {
    Synchronized,
    FullSnapshotRequired(ClientRecoveryReason),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ClientSnapshotOutcome {
    Committed(ReplicationCursor),
    Stale,
    DuplicateCurrent,
    TickRegression,
    MissingBase,
    DeltaBlockedByRecovery,
    MalformedDelta,
    ReconstructionFailure,
    HostCommitFailure,
    StateResourceFailure,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DeltaReconstructionError {
    Malformed,
    ReconstructionFailed,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ClientSetError {
    LineageAlreadyExists,
    UnknownLineage,
    AggregateLineageLimitExceeded,
}

#[derive(Debug)]
struct RetainedState<S> {
    tick: SimulationTick,
    image: AccountedState<S>,
}

#[derive(Debug)]
struct CommitPlan {
    evict: Vec<ReplicationCursor>,
    resulting_count: usize,
    resulting_bytes: usize,
}

#[derive(Debug)]
pub struct ClientLineage<S> {
    key: ReplicationLineageKey,
    limits: ReplicationRetentionLimits,
    state: ClientReplicationState,
    current: Option<ReplicationCursor>,
    retained: BTreeMap<ReplicationCursor, RetainedState<S>>,
    retained_bytes: usize,
}

impl<S> ClientLineage<S> {
    pub(crate) fn new(key: ReplicationLineageKey, limits: ReplicationRetentionLimits) -> Self {
        Self {
            key,
            limits,
            state: ClientReplicationState::FullSnapshotRequired(
                ClientRecoveryReason::InitialBaseline,
            ),
            current: None,
            retained: BTreeMap::new(),
            retained_bytes: 0,
        }
    }

    pub const fn key(&self) -> ReplicationLineageKey {
        self.key
    }

    pub const fn replication_state(&self) -> ClientReplicationState {
        self.state
    }

    pub const fn current_cursor(&self) -> Option<ReplicationCursor> {
        self.current
    }

    pub fn current_tick(&self) -> Option<SimulationTick> {
        self.current
            .and_then(|cursor| self.retained.get(&cursor).map(|retained| retained.tick))
    }

    pub fn current_state(&self) -> Option<&S> {
        self.current.and_then(|cursor| {
            self.retained
                .get(&cursor)
                .map(|retained| retained.image.state())
        })
    }

    pub fn retained_state(&self, cursor: ReplicationCursor) -> Option<&S> {
        self.retained
            .get(&cursor)
            .map(|retained| retained.image.state())
    }

    pub fn retained_image_count(&self) -> usize {
        self.retained.len()
    }

    pub const fn retained_state_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub const fn acknowledgement_cursor(&self) -> Option<ReplicationCursor> {
        self.current
    }

    fn classify_cursor(&self, target: ReplicationCursor) -> Option<ClientSnapshotOutcome> {
        let current = self.current?;
        if target < current {
            Some(ClientSnapshotOutcome::Stale)
        } else if target == current {
            Some(ClientSnapshotOutcome::DuplicateCurrent)
        } else {
            None
        }
    }

    fn tick_regresses_current(&self, tick: SimulationTick) -> bool {
        self.current_tick().is_some_and(|current| tick < current)
    }

    fn state_image_fits(&self, bytes: usize) -> bool {
        bytes <= self.limits.max_state_image_bytes()
            && bytes <= self.limits.max_candidate_bytes_per_lineage()
            && bytes <= self.limits.max_retained_state_bytes_per_lineage()
    }

    fn plan_commit(&self, target_bytes: usize) -> Option<CommitPlan> {
        if !self.state_image_fits(target_bytes) {
            return None;
        }

        let max_count = self.limits.max_retained_images_per_lineage();
        let max_bytes = self.limits.max_retained_state_bytes_per_lineage();
        let mut count = self.retained.len();
        let mut bytes = self.retained_bytes;
        let mut evict = Vec::new();

        for (cursor, retained) in &self.retained {
            if count < max_count && bytes <= max_bytes - target_bytes {
                break;
            }
            count -= 1;
            bytes -= retained.image.accounted_bytes();
            evict.push(*cursor);
        }

        if count >= max_count || bytes > max_bytes - target_bytes {
            return None;
        }

        Some(CommitPlan {
            evict,
            resulting_count: count + 1,
            resulting_bytes: bytes + target_bytes,
        })
    }

    fn commit_with_plan(
        &mut self,
        cursor: ReplicationCursor,
        tick: SimulationTick,
        image: AccountedState<S>,
        plan: CommitPlan,
    ) {
        for evict in plan.evict {
            let removed = self
                .retained
                .remove(&evict)
                .expect("commit plan references retained cursor");
            self.retained_bytes -= removed.image.accounted_bytes();
        }

        self.retained_bytes += image.accounted_bytes();
        let previous = self.retained.insert(cursor, RetainedState { tick, image });
        debug_assert!(previous.is_none());
        self.current = Some(cursor);
        self.state = ClientReplicationState::Synchronized;
        debug_assert_eq!(self.retained.len(), plan.resulting_count);
        debug_assert_eq!(self.retained_bytes, plan.resulting_bytes);
    }

    fn enter_recovery(&mut self, reason: ClientRecoveryReason) {
        self.state = ClientReplicationState::FullSnapshotRequired(reason);
    }

    fn evict_historical(&mut self, cursor: ReplicationCursor) -> bool {
        if self.current == Some(cursor) {
            return false;
        }
        let Some(removed) = self.retained.remove(&cursor) else {
            return false;
        };
        self.retained_bytes -= removed.image.accounted_bytes();
        true
    }
}

#[derive(Debug)]
pub struct ClientReplicationSet<S> {
    limits: ClientAggregateLimits,
    lineages: HashMap<ReplicationLineageKey, ClientLineage<S>>,
}

impl<S> ClientReplicationSet<S> {
    pub fn new(limits: ClientAggregateLimits) -> Self {
        Self {
            limits,
            lineages: HashMap::new(),
        }
    }

    pub fn add_lineage(
        &mut self,
        key: ReplicationLineageKey,
        retention: ReplicationRetentionLimits,
    ) -> Result<(), ClientSetError> {
        if self.lineages.contains_key(&key) {
            return Err(ClientSetError::LineageAlreadyExists);
        }
        if self.lineages.len() >= self.limits.max_lineages() {
            return Err(ClientSetError::AggregateLineageLimitExceeded);
        }
        self.lineages.insert(key, ClientLineage::new(key, retention));
        Ok(())
    }

    pub fn remove_lineage(&mut self, key: ReplicationLineageKey) -> bool {
        self.lineages.remove(&key).is_some()
    }

    pub fn lineage(&self, key: ReplicationLineageKey) -> Option<&ClientLineage<S>> {
        self.lineages.get(&key)
    }

    pub fn lineage_count(&self) -> usize {
        self.lineages.len()
    }

    pub fn retained_image_count(&self) -> usize {
        self.lineages
            .values()
            .map(ClientLineage::retained_image_count)
            .sum()
    }

    pub fn retained_state_bytes(&self) -> usize {
        self.lineages
            .values()
            .map(ClientLineage::retained_state_bytes)
            .sum()
    }

    pub fn apply_full<E, F>(
        &mut self,
        key: ReplicationLineageKey,
        snapshot: FullSnapshot<S>,
        host_commit: F,
    ) -> Result<ClientSnapshotOutcome, ClientSetError>
    where
        F: FnOnce(&S) -> Result<(), E>,
    {
        let (target, tick, image) = snapshot.into_parts();
        let lineage = self.lineages.get(&key).ok_or(ClientSetError::UnknownLineage)?;

        if let Some(classification) = lineage.classify_cursor(target) {
            return Ok(classification);
        }
        if lineage.tick_regresses_current(tick) {
            return Ok(ClientSnapshotOutcome::TickRegression);
        }

        let Some(plan) = lineage.plan_commit(image.accounted_bytes()) else {
            return Ok(ClientSnapshotOutcome::StateResourceFailure);
        };
        if !self.aggregate_commit_fits(key, &plan, image.accounted_bytes()) {
            return Ok(ClientSnapshotOutcome::StateResourceFailure);
        }
        if host_commit(image.state()).is_err() {
            return Ok(ClientSnapshotOutcome::HostCommitFailure);
        }

        self.lineages
            .get_mut(&key)
            .expect("lineage checked above")
            .commit_with_plan(target, tick, image, plan);
        Ok(ClientSnapshotOutcome::Committed(target))
    }

    pub fn apply_delta<D, E, R, C>(
        &mut self,
        key: ReplicationLineageKey,
        snapshot: DeltaSnapshot<D>,
        reconstruct: R,
        host_commit: C,
    ) -> Result<ClientSnapshotOutcome, ClientSetError>
    where
        R: FnOnce(&S, &D, usize) -> Result<AccountedState<S>, DeltaReconstructionError>,
        C: FnOnce(&S) -> Result<(), E>,
    {
        let (base_cursor, target, tick, delta) = snapshot.into_parts();

        {
            let lineage = self.lineages.get(&key).ok_or(ClientSetError::UnknownLineage)?;
            if let Some(classification) = lineage.classify_cursor(target) {
                return Ok(classification);
            }
            if matches!(
                lineage.replication_state(),
                ClientReplicationState::FullSnapshotRequired(_)
            ) {
                return Ok(ClientSnapshotOutcome::DeltaBlockedByRecovery);
            }
        }

        if target <= base_cursor {
            self.enter_recovery(key, ClientRecoveryReason::MalformedDelta)?;
            return Ok(ClientSnapshotOutcome::MalformedDelta);
        }

        let candidate = {
            let lineage = self.lineages.get(&key).expect("lineage checked above");
            if lineage.tick_regresses_current(tick) {
                None
            } else {
                let Some(base) = lineage.retained.get(&base_cursor) else {
                    self.enter_recovery(key, ClientRecoveryReason::MissingBase)?;
                    return Ok(ClientSnapshotOutcome::MissingBase);
                };
                if tick < base.tick {
                    None
                } else {
                    Some(reconstruct(
                        base.image.state(),
                        &delta,
                        lineage.limits.max_candidate_bytes_per_lineage(),
                    ))
                }
            }
        };

        let Some(candidate) = candidate else {
            self.enter_recovery(key, ClientRecoveryReason::DeltaTickRegression)?;
            return Ok(ClientSnapshotOutcome::TickRegression);
        };
        let candidate = match candidate {
            Ok(candidate) => candidate,
            Err(DeltaReconstructionError::Malformed) => {
                self.enter_recovery(key, ClientRecoveryReason::MalformedDelta)?;
                return Ok(ClientSnapshotOutcome::MalformedDelta);
            }
            Err(DeltaReconstructionError::ReconstructionFailed) => {
                self.enter_recovery(key, ClientRecoveryReason::ReconstructionFailure)?;
                return Ok(ClientSnapshotOutcome::ReconstructionFailure);
            }
        };

        let lineage = self.lineages.get(&key).expect("lineage checked above");
        let Some(plan) = lineage.plan_commit(candidate.accounted_bytes()) else {
            return Ok(ClientSnapshotOutcome::StateResourceFailure);
        };
        if !self.aggregate_commit_fits(key, &plan, candidate.accounted_bytes()) {
            return Ok(ClientSnapshotOutcome::StateResourceFailure);
        }
        if host_commit(candidate.state()).is_err() {
            self.enter_recovery(key, ClientRecoveryReason::DeltaCommitFailure)?;
            return Ok(ClientSnapshotOutcome::HostCommitFailure);
        }

        self.lineages
            .get_mut(&key)
            .expect("lineage checked above")
            .commit_with_plan(target, tick, candidate, plan);
        Ok(ClientSnapshotOutcome::Committed(target))
    }

    pub fn require_connection_replacement_full(
        &mut self,
        key: ReplicationLineageKey,
    ) -> Result<(), ClientSetError> {
        self.enter_recovery(key, ClientRecoveryReason::ConnectionReplacement)
    }

    pub fn evict_historical(
        &mut self,
        key: ReplicationLineageKey,
        cursor: ReplicationCursor,
    ) -> Result<bool, ClientSetError> {
        Ok(self
            .lineages
            .get_mut(&key)
            .ok_or(ClientSetError::UnknownLineage)?
            .evict_historical(cursor))
    }

    fn enter_recovery(
        &mut self,
        key: ReplicationLineageKey,
        reason: ClientRecoveryReason,
    ) -> Result<(), ClientSetError> {
        self.lineages
            .get_mut(&key)
            .ok_or(ClientSetError::UnknownLineage)?
            .enter_recovery(reason);
        Ok(())
    }

    fn aggregate_commit_fits(
        &self,
        key: ReplicationLineageKey,
        plan: &CommitPlan,
        candidate_bytes: usize,
    ) -> bool {
        let lineage = self
            .lineages
            .get(&key)
            .expect("aggregate projection uses existing lineage");
        let current_count = self.retained_image_count();
        let current_bytes = self.retained_state_bytes();
        let projected_count = current_count - lineage.retained_image_count() + plan.resulting_count;
        let projected_bytes = current_bytes - lineage.retained_state_bytes() + plan.resulting_bytes;

        projected_count <= self.limits.max_retained_images()
            && projected_bytes <= self.limits.max_retained_state_bytes()
            && current_bytes
                .checked_add(candidate_bytes)
                .is_some_and(|bytes| bytes <= self.limits.max_retained_state_bytes())
    }
}
