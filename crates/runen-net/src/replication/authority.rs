use std::collections::{BTreeMap, HashMap};

use crate::delivery::SubmissionOutcome;
use crate::identity::{ConnectionHandle, ParticipantId, SessionId, SimulationTick};
use crate::session::Session;

use super::model::{
    AccountedState, AuthorityAggregateLimits, FullSnapshot, RecoveryGeneration, ReplicationCursor,
    ReplicationLineageKey, ReplicationRetentionLimits,
};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SnapshotKind {
    Full,
    Delta,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AuthorityRecoveryReason {
    InitialBaseline,
    RecoveryDemand,
    BaselineEvicted,
    ConfirmedBaselineUnavailable,
    ConnectionReplacement,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AuthorityReplicationState {
    DeltaEligible(ReplicationCursor),
    FullSnapshotRequired {
        reason: AuthorityRecoveryReason,
        generation: RecoveryGeneration,
        start_cursor_watermark: Option<ReplicationCursor>,
    },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AuthorityPrepareError {
    PendingCandidateExists,
    CursorNotNewer,
    TickRegression,
    DeltaNotEligible,
    BaselineUnavailable,
    CandidateTooLarge,
    RecoveryDesignationRequiresRecovery,
    RecoveryFullNotNewerThanWatermark,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AuthorityOperationError {
    Unauthorized,
    NoPendingCandidate,
    RecoveryGenerationExhausted,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AuthorityAckOutcome {
    DuplicateConfirmation,
    StaleConfirmation,
    Confirmed,
    FutureConfirmation,
    UnverifiableConfirmation,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AuthoritySessionError {
    LineageAlreadyExists,
    UnknownLineage,
    AggregateResourceLimitExceeded,
    Prepare(AuthorityPrepareError),
    Operation(AuthorityOperationError),
}

impl From<AuthorityPrepareError> for AuthoritySessionError {
    fn from(value: AuthorityPrepareError) -> Self {
        Self::Prepare(value)
    }
}

impl From<AuthorityOperationError> for AuthoritySessionError {
    fn from(value: AuthorityOperationError) -> Self {
        Self::Operation(value)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct PendingSnapshotSummary {
    pub kind: SnapshotKind,
    pub base_cursor: Option<ReplicationCursor>,
    pub target_cursor: ReplicationCursor,
    pub target_tick: SimulationTick,
    pub recovery_generation: Option<RecoveryGeneration>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct EmittedSnapshot {
    pub kind: SnapshotKind,
    pub base_cursor: Option<ReplicationCursor>,
    pub target_cursor: ReplicationCursor,
    pub target_tick: SimulationTick,
    pub recovery_generation: Option<RecoveryGeneration>,
}

#[derive(Debug)]
struct RetainedState<S> {
    tick: SimulationTick,
    image: AccountedState<S>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct EmissionEvidence {
    kind: SnapshotKind,
    recovery_generation: Option<RecoveryGeneration>,
}

#[derive(Debug)]
enum SnapshotCandidate<S, D> {
    Full {
        snapshot: FullSnapshot<S>,
        recovery_generation: Option<RecoveryGeneration>,
    },
    Delta {
        base_cursor: ReplicationCursor,
        target_cursor: ReplicationCursor,
        target_tick: SimulationTick,
        target_image: AccountedState<S>,
        delta: D,
        additional_candidate_bytes: usize,
    },
}

impl<S, D> SnapshotCandidate<S, D> {
    fn state_bytes(&self) -> usize {
        match self {
            Self::Full { snapshot, .. } => snapshot.image().accounted_bytes(),
            Self::Delta { target_image, .. } => target_image.accounted_bytes(),
        }
    }

    fn total_candidate_bytes(&self) -> usize {
        match self {
            Self::Full { snapshot, .. } => snapshot.image().accounted_bytes(),
            Self::Delta {
                target_image,
                additional_candidate_bytes,
                ..
            } => target_image
                .accounted_bytes()
                .saturating_add(*additional_candidate_bytes),
        }
    }
}

#[derive(Debug)]
pub enum PendingSnapshotRef<'a, S, D> {
    Full {
        snapshot: &'a FullSnapshot<S>,
        recovery_generation: Option<RecoveryGeneration>,
    },
    Delta {
        base_cursor: ReplicationCursor,
        target_cursor: ReplicationCursor,
        target_tick: SimulationTick,
        target_image: &'a AccountedState<S>,
        delta: &'a D,
    },
}

#[derive(Debug)]
pub struct AuthorityLineage<S, D> {
    key: ReplicationLineageKey,
    limits: ReplicationRetentionLimits,
    state: AuthorityReplicationState,
    generation: RecoveryGeneration,
    latest_confirmed: Option<ReplicationCursor>,
    greatest_emitted: Option<ReplicationCursor>,
    greatest_emitted_tick: Option<SimulationTick>,
    retained: BTreeMap<ReplicationCursor, RetainedState<S>>,
    retained_bytes: usize,
    evidence: BTreeMap<ReplicationCursor, EmissionEvidence>,
    pending: Option<SnapshotCandidate<S, D>>,
}

impl<S, D> AuthorityLineage<S, D> {
    pub(crate) fn new(key: ReplicationLineageKey, limits: ReplicationRetentionLimits) -> Self {
        let generation = RecoveryGeneration::new(0);
        Self {
            key,
            limits,
            state: AuthorityReplicationState::FullSnapshotRequired {
                reason: AuthorityRecoveryReason::InitialBaseline,
                generation,
                start_cursor_watermark: None,
            },
            generation,
            latest_confirmed: None,
            greatest_emitted: None,
            greatest_emitted_tick: None,
            retained: BTreeMap::new(),
            retained_bytes: 0,
            evidence: BTreeMap::new(),
            pending: None,
        }
    }

    pub const fn key(&self) -> ReplicationLineageKey {
        self.key
    }

    pub const fn replication_state(&self) -> AuthorityReplicationState {
        self.state
    }

    pub const fn latest_confirmed_cursor(&self) -> Option<ReplicationCursor> {
        self.latest_confirmed
    }

    pub const fn greatest_emitted_cursor(&self) -> Option<ReplicationCursor> {
        self.greatest_emitted
    }

    pub fn baseline_available(&self, cursor: ReplicationCursor) -> bool {
        self.retained.contains_key(&cursor)
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

    pub fn emission_evidence_count(&self) -> usize {
        self.evidence.len()
    }

    pub fn pending_snapshot(&self) -> Option<PendingSnapshotRef<'_, S, D>> {
        match self.pending.as_ref()? {
            SnapshotCandidate::Full {
                snapshot,
                recovery_generation,
            } => Some(PendingSnapshotRef::Full {
                snapshot,
                recovery_generation: *recovery_generation,
            }),
            SnapshotCandidate::Delta {
                base_cursor,
                target_cursor,
                target_tick,
                target_image,
                delta,
                ..
            } => Some(PendingSnapshotRef::Delta {
                base_cursor: *base_cursor,
                target_cursor: *target_cursor,
                target_tick: *target_tick,
                target_image,
                delta,
            }),
        }
    }

    pub fn pending_summary(&self) -> Option<PendingSnapshotSummary> {
        match self.pending.as_ref()? {
            SnapshotCandidate::Full {
                snapshot,
                recovery_generation,
            } => Some(PendingSnapshotSummary {
                kind: SnapshotKind::Full,
                base_cursor: None,
                target_cursor: snapshot.target_cursor(),
                target_tick: snapshot.target_tick(),
                recovery_generation: *recovery_generation,
            }),
            SnapshotCandidate::Delta {
                base_cursor,
                target_cursor,
                target_tick,
                ..
            } => Some(PendingSnapshotSummary {
                kind: SnapshotKind::Delta,
                base_cursor: Some(*base_cursor),
                target_cursor: *target_cursor,
                target_tick: *target_tick,
                recovery_generation: None,
            }),
        }
    }

    fn pending_state_bytes(&self) -> usize {
        self.pending
            .as_ref()
            .map(SnapshotCandidate::state_bytes)
            .unwrap_or(0)
    }

    fn pending_candidate_bytes(&self) -> usize {
        self.pending
            .as_ref()
            .map(SnapshotCandidate::total_candidate_bytes)
            .unwrap_or(0)
    }

    fn has_pending(&self) -> bool {
        self.pending.is_some()
    }

    fn prepare_full(
        &mut self,
        snapshot: FullSnapshot<S>,
        designate_recovery_full: bool,
    ) -> Result<PendingSnapshotSummary, AuthorityPrepareError> {
        self.require_candidate_slot()?;
        self.validate_new_emission_target(snapshot.target_cursor(), snapshot.target_tick())?;
        if !self.candidate_fits(snapshot.image().accounted_bytes(), 0) {
            return Err(AuthorityPrepareError::CandidateTooLarge);
        }

        let recovery_generation = if designate_recovery_full {
            match self.state {
                AuthorityReplicationState::FullSnapshotRequired {
                    generation,
                    start_cursor_watermark,
                    ..
                } => {
                    if start_cursor_watermark
                        .is_some_and(|watermark| snapshot.target_cursor() <= watermark)
                    {
                        return Err(AuthorityPrepareError::RecoveryFullNotNewerThanWatermark);
                    }
                    Some(generation)
                }
                AuthorityReplicationState::DeltaEligible(_) => {
                    return Err(AuthorityPrepareError::RecoveryDesignationRequiresRecovery);
                }
            }
        } else {
            None
        };

        let summary = PendingSnapshotSummary {
            kind: SnapshotKind::Full,
            base_cursor: None,
            target_cursor: snapshot.target_cursor(),
            target_tick: snapshot.target_tick(),
            recovery_generation,
        };
        self.pending = Some(SnapshotCandidate::Full {
            snapshot,
            recovery_generation,
        });
        Ok(summary)
    }

    fn prepare_delta(
        &mut self,
        target_cursor: ReplicationCursor,
        target_tick: SimulationTick,
        target_image: AccountedState<S>,
        delta: D,
        additional_candidate_bytes: usize,
    ) -> Result<PendingSnapshotSummary, AuthorityPrepareError> {
        self.require_candidate_slot()?;
        let base_cursor = match self.state {
            AuthorityReplicationState::DeltaEligible(base) => base,
            AuthorityReplicationState::FullSnapshotRequired { .. } => {
                return Err(AuthorityPrepareError::DeltaNotEligible);
            }
        };
        let base = self
            .retained
            .get(&base_cursor)
            .ok_or(AuthorityPrepareError::BaselineUnavailable)?;
        if target_cursor <= base_cursor {
            return Err(AuthorityPrepareError::CursorNotNewer);
        }
        self.validate_new_emission_target(target_cursor, target_tick)?;
        if target_tick < base.tick {
            return Err(AuthorityPrepareError::TickRegression);
        }
        if !self.candidate_fits(target_image.accounted_bytes(), additional_candidate_bytes) {
            return Err(AuthorityPrepareError::CandidateTooLarge);
        }

        let summary = PendingSnapshotSummary {
            kind: SnapshotKind::Delta,
            base_cursor: Some(base_cursor),
            target_cursor,
            target_tick,
            recovery_generation: None,
        };
        self.pending = Some(SnapshotCandidate::Delta {
            base_cursor,
            target_cursor,
            target_tick,
            target_image,
            delta,
            additional_candidate_bytes,
        });
        Ok(summary)
    }

    fn record_delivery_submission(
        &mut self,
        outcome: SubmissionOutcome,
    ) -> Result<Option<EmittedSnapshot>, AuthorityOperationError> {
        if !matches!(outcome, SubmissionOutcome::Accepted { .. }) {
            return Ok(None);
        }

        let candidate = self
            .pending
            .take()
            .ok_or(AuthorityOperationError::NoPendingCandidate)?;
        let emitted = match candidate {
            SnapshotCandidate::Full {
                snapshot,
                recovery_generation,
            } => {
                let (target_cursor, target_tick, image) = snapshot.into_parts();
                self.record_emitted_state(
                    target_cursor,
                    target_tick,
                    image,
                    SnapshotKind::Full,
                    recovery_generation,
                );
                EmittedSnapshot {
                    kind: SnapshotKind::Full,
                    base_cursor: None,
                    target_cursor,
                    target_tick,
                    recovery_generation,
                }
            }
            SnapshotCandidate::Delta {
                base_cursor,
                target_cursor,
                target_tick,
                target_image,
                ..
            } => {
                self.record_emitted_state(
                    target_cursor,
                    target_tick,
                    target_image,
                    SnapshotKind::Delta,
                    None,
                );
                EmittedSnapshot {
                    kind: SnapshotKind::Delta,
                    base_cursor: Some(base_cursor),
                    target_cursor,
                    target_tick,
                    recovery_generation: None,
                }
            }
        };
        Ok(Some(emitted))
    }

    fn cancel_pending(&mut self) -> bool {
        self.pending.take().is_some()
    }

    fn acknowledge_authorized(
        &mut self,
        session: &Session,
        connection: ConnectionHandle,
        cursor: ReplicationCursor,
    ) -> Result<AuthorityAckOutcome, AuthorityOperationError> {
        if session.id() != self.key.session()
            || !session.is_authorized(self.key.participant(), connection)
        {
            return Err(AuthorityOperationError::Unauthorized);
        }

        if let Some(latest) = self.latest_confirmed {
            if cursor == latest {
                return Ok(AuthorityAckOutcome::DuplicateConfirmation);
            }
            if cursor < latest {
                return Ok(AuthorityAckOutcome::StaleConfirmation);
            }
        }
        if self
            .greatest_emitted
            .is_none_or(|greatest| cursor > greatest)
        {
            return Ok(AuthorityAckOutcome::FutureConfirmation);
        }

        let Some(evidence) = self.evidence.get(&cursor).copied() else {
            return Ok(AuthorityAckOutcome::UnverifiableConfirmation);
        };
        let baseline_available = self.baseline_available(cursor);
        let needs_new_recovery = matches!(self.state, AuthorityReplicationState::DeltaEligible(_))
            && !baseline_available;
        let next_generation = if needs_new_recovery {
            Some(
                self.generation
                    .checked_next()
                    .ok_or(AuthorityOperationError::RecoveryGenerationExhausted)?,
            )
        } else {
            None
        };

        self.latest_confirmed = Some(cursor);
        match self.state {
            AuthorityReplicationState::DeltaEligible(_) => {
                if baseline_available {
                    self.state = AuthorityReplicationState::DeltaEligible(cursor);
                } else {
                    self.install_new_recovery(
                        AuthorityRecoveryReason::ConfirmedBaselineUnavailable,
                        next_generation.expect("precomputed above"),
                    );
                }
            }
            AuthorityReplicationState::FullSnapshotRequired { generation, .. } => {
                if evidence.kind == SnapshotKind::Full
                    && evidence.recovery_generation == Some(generation)
                    && baseline_available
                {
                    self.state = AuthorityReplicationState::DeltaEligible(cursor);
                }
            }
        }
        Ok(AuthorityAckOutcome::Confirmed)
    }

    fn require_full_recovery(
        &mut self,
        reason: AuthorityRecoveryReason,
    ) -> Result<(), AuthorityOperationError> {
        if matches!(
            self.state,
            AuthorityReplicationState::FullSnapshotRequired { .. }
        ) {
            return Ok(());
        }
        let next = self
            .generation
            .checked_next()
            .ok_or(AuthorityOperationError::RecoveryGenerationExhausted)?;
        self.install_new_recovery(reason, next);
        Ok(())
    }

    fn connection_replaced(
        &mut self,
        session: &Session,
        connection: ConnectionHandle,
    ) -> Result<(), AuthorityOperationError> {
        if session.id() != self.key.session()
            || !session.is_authorized(self.key.participant(), connection)
        {
            return Err(AuthorityOperationError::Unauthorized);
        }
        let next = self
            .generation
            .checked_next()
            .ok_or(AuthorityOperationError::RecoveryGenerationExhausted)?;
        self.install_new_recovery(AuthorityRecoveryReason::ConnectionReplacement, next);
        Ok(())
    }

    fn evict_retained_state(
        &mut self,
        cursor: ReplicationCursor,
    ) -> Result<bool, AuthorityOperationError> {
        let delta_base_is_evicted = matches!(
            self.state,
            AuthorityReplicationState::DeltaEligible(base) if base == cursor
        );
        let next_generation = if delta_base_is_evicted {
            Some(
                self.generation
                    .checked_next()
                    .ok_or(AuthorityOperationError::RecoveryGenerationExhausted)?,
            )
        } else {
            None
        };

        let Some(removed) = self.retained.remove(&cursor) else {
            return Ok(false);
        };
        self.retained_bytes -= removed.image.accounted_bytes();
        if delta_base_is_evicted {
            self.install_new_recovery(
                AuthorityRecoveryReason::BaselineEvicted,
                next_generation.expect("precomputed above"),
            );
        }
        Ok(true)
    }

    fn require_candidate_slot(&self) -> Result<(), AuthorityPrepareError> {
        if self.pending.is_some() {
            Err(AuthorityPrepareError::PendingCandidateExists)
        } else {
            Ok(())
        }
    }

    fn validate_new_emission_target(
        &self,
        cursor: ReplicationCursor,
        tick: SimulationTick,
    ) -> Result<(), AuthorityPrepareError> {
        if self
            .greatest_emitted
            .is_some_and(|greatest| cursor <= greatest)
        {
            return Err(AuthorityPrepareError::CursorNotNewer);
        }
        if self
            .greatest_emitted_tick
            .is_some_and(|greatest_tick| tick < greatest_tick)
        {
            return Err(AuthorityPrepareError::TickRegression);
        }
        Ok(())
    }

    fn candidate_fits(&self, state_bytes: usize, additional_bytes: usize) -> bool {
        if state_bytes > self.limits.max_state_image_bytes() {
            return false;
        }
        let max = self.limits.max_candidate_bytes_per_lineage();
        state_bytes <= max && additional_bytes <= max - state_bytes
    }

    fn record_emitted_state(
        &mut self,
        cursor: ReplicationCursor,
        tick: SimulationTick,
        image: AccountedState<S>,
        kind: SnapshotKind,
        recovery_generation: Option<RecoveryGeneration>,
    ) {
        self.greatest_emitted = Some(cursor);
        self.greatest_emitted_tick = Some(tick);
        self.retain_authority_state(cursor, tick, image);
        self.retain_emission_evidence(
            cursor,
            EmissionEvidence {
                kind,
                recovery_generation,
            },
        );
    }

    fn retain_authority_state(
        &mut self,
        cursor: ReplicationCursor,
        tick: SimulationTick,
        image: AccountedState<S>,
    ) {
        let target_bytes = image.accounted_bytes();
        let max_count = self.limits.max_retained_images_per_lineage();
        let max_bytes = self.limits.max_retained_state_bytes_per_lineage();
        let protected = match self.state {
            AuthorityReplicationState::DeltaEligible(base) => Some(base),
            AuthorityReplicationState::FullSnapshotRequired { .. } => None,
        };

        while self.retained.len() >= max_count || self.retained_bytes > max_bytes - target_bytes {
            let candidate = self
                .retained
                .keys()
                .copied()
                .find(|existing| Some(*existing) != protected);
            let Some(evict) = candidate else {
                return;
            };
            let removed = self
                .retained
                .remove(&evict)
                .expect("cursor selected from retained map");
            self.retained_bytes -= removed.image.accounted_bytes();
        }

        self.retained_bytes += target_bytes;
        let previous = self.retained.insert(cursor, RetainedState { tick, image });
        debug_assert!(previous.is_none());
    }

    fn retain_emission_evidence(&mut self, cursor: ReplicationCursor, evidence: EmissionEvidence) {
        let max = self.limits.max_emission_evidence_per_lineage();
        while self.evidence.len() >= max {
            let oldest = self
                .evidence
                .keys()
                .next()
                .copied()
                .expect("non-empty while at evidence limit");
            self.evidence.remove(&oldest);
        }
        self.evidence.insert(cursor, evidence);
    }

    fn install_new_recovery(
        &mut self,
        reason: AuthorityRecoveryReason,
        generation: RecoveryGeneration,
    ) {
        self.pending = None;
        self.generation = generation;
        self.state = AuthorityReplicationState::FullSnapshotRequired {
            reason,
            generation,
            start_cursor_watermark: self.greatest_emitted,
        };
    }
}

#[derive(Debug)]
pub struct AuthorityReplicationSession<S, D> {
    session_id: SessionId,
    limits: AuthorityAggregateLimits,
    lineages: HashMap<ParticipantId, AuthorityLineage<S, D>>,
}

impl<S, D> AuthorityReplicationSession<S, D> {
    pub fn new(session_id: SessionId, limits: AuthorityAggregateLimits) -> Self {
        Self {
            session_id,
            limits,
            lineages: HashMap::new(),
        }
    }

    pub const fn session_id(&self) -> SessionId {
        self.session_id
    }

    pub fn add_lineage(
        &mut self,
        participant: ParticipantId,
        retention: ReplicationRetentionLimits,
    ) -> Result<ReplicationLineageKey, AuthoritySessionError> {
        if self.lineages.contains_key(&participant) {
            return Err(AuthoritySessionError::LineageAlreadyExists);
        }
        if self.lineages.len() >= self.limits.max_lineages() {
            return Err(AuthoritySessionError::AggregateResourceLimitExceeded);
        }

        let key = ReplicationLineageKey::new(self.session_id, participant);
        self.lineages
            .insert(participant, AuthorityLineage::new(key, retention));
        Ok(key)
    }

    pub fn remove_lineage(&mut self, participant: ParticipantId) -> bool {
        self.lineages.remove(&participant).is_some()
    }

    pub fn lineage(&self, participant: ParticipantId) -> Option<&AuthorityLineage<S, D>> {
        self.lineages.get(&participant)
    }

    pub fn lineage_count(&self) -> usize {
        self.lineages.len()
    }

    pub fn retained_image_count(&self) -> usize {
        self.checked_retained_image_count().unwrap_or(usize::MAX)
    }

    pub fn retained_state_bytes(&self) -> usize {
        self.checked_retained_state_bytes().unwrap_or(usize::MAX)
    }

    pub fn emission_evidence_count(&self) -> usize {
        self.checked_emission_evidence_count().unwrap_or(usize::MAX)
    }

    pub fn accounted_state_bytes(&self) -> usize {
        self.checked_accounted_state_bytes().unwrap_or(usize::MAX)
    }

    pub fn prepare_full(
        &mut self,
        participant: ParticipantId,
        snapshot: FullSnapshot<S>,
        designate_recovery_full: bool,
    ) -> Result<PendingSnapshotSummary, AuthoritySessionError> {
        let summary = self
            .lineages
            .get_mut(&participant)
            .ok_or(AuthoritySessionError::UnknownLineage)?
            .prepare_full(snapshot, designate_recovery_full)?;
        if !self.aggregate_reservations_fit() {
            self.lineages
                .get_mut(&participant)
                .expect("lineage checked above")
                .cancel_pending();
            return Err(AuthoritySessionError::AggregateResourceLimitExceeded);
        }
        Ok(summary)
    }

    pub fn prepare_delta(
        &mut self,
        participant: ParticipantId,
        target_cursor: ReplicationCursor,
        target_tick: SimulationTick,
        target_image: AccountedState<S>,
        delta: D,
        additional_candidate_bytes: usize,
    ) -> Result<PendingSnapshotSummary, AuthoritySessionError> {
        let summary = self
            .lineages
            .get_mut(&participant)
            .ok_or(AuthoritySessionError::UnknownLineage)?
            .prepare_delta(
                target_cursor,
                target_tick,
                target_image,
                delta,
                additional_candidate_bytes,
            )?;
        if !self.aggregate_reservations_fit() {
            self.lineages
                .get_mut(&participant)
                .expect("lineage checked above")
                .cancel_pending();
            return Err(AuthoritySessionError::AggregateResourceLimitExceeded);
        }
        Ok(summary)
    }

    pub fn record_delivery_submission(
        &mut self,
        participant: ParticipantId,
        outcome: SubmissionOutcome,
    ) -> Result<Option<EmittedSnapshot>, AuthoritySessionError> {
        Ok(self
            .lineages
            .get_mut(&participant)
            .ok_or(AuthoritySessionError::UnknownLineage)?
            .record_delivery_submission(outcome)?)
    }

    pub fn cancel_pending(
        &mut self,
        participant: ParticipantId,
    ) -> Result<bool, AuthoritySessionError> {
        Ok(self
            .lineages
            .get_mut(&participant)
            .ok_or(AuthoritySessionError::UnknownLineage)?
            .cancel_pending())
    }

    pub fn acknowledge_authorized(
        &mut self,
        session: &Session,
        connection: ConnectionHandle,
        participant: ParticipantId,
        cursor: ReplicationCursor,
    ) -> Result<AuthorityAckOutcome, AuthoritySessionError> {
        Ok(self
            .lineages
            .get_mut(&participant)
            .ok_or(AuthoritySessionError::UnknownLineage)?
            .acknowledge_authorized(session, connection, cursor)?)
    }

    pub fn require_full_recovery(
        &mut self,
        participant: ParticipantId,
        reason: AuthorityRecoveryReason,
    ) -> Result<(), AuthoritySessionError> {
        Ok(self
            .lineages
            .get_mut(&participant)
            .ok_or(AuthoritySessionError::UnknownLineage)?
            .require_full_recovery(reason)?)
    }

    pub fn connection_replaced(
        &mut self,
        session: &Session,
        connection: ConnectionHandle,
        participant: ParticipantId,
    ) -> Result<(), AuthoritySessionError> {
        Ok(self
            .lineages
            .get_mut(&participant)
            .ok_or(AuthoritySessionError::UnknownLineage)?
            .connection_replaced(session, connection)?)
    }

    pub fn evict_retained_state(
        &mut self,
        participant: ParticipantId,
        cursor: ReplicationCursor,
    ) -> Result<bool, AuthoritySessionError> {
        Ok(self
            .lineages
            .get_mut(&participant)
            .ok_or(AuthoritySessionError::UnknownLineage)?
            .evict_retained_state(cursor)?)
    }

    fn checked_retained_image_count(&self) -> Option<usize> {
        self.lineages.values().try_fold(0usize, |total, lineage| {
            total.checked_add(lineage.retained_image_count())
        })
    }

    fn checked_retained_state_bytes(&self) -> Option<usize> {
        self.lineages.values().try_fold(0usize, |total, lineage| {
            total.checked_add(lineage.retained_state_bytes())
        })
    }

    fn checked_emission_evidence_count(&self) -> Option<usize> {
        self.lineages.values().try_fold(0usize, |total, lineage| {
            total.checked_add(lineage.emission_evidence_count())
        })
    }

    fn checked_pending_state_bytes(&self) -> Option<usize> {
        self.lineages.values().try_fold(0usize, |total, lineage| {
            total.checked_add(lineage.pending_state_bytes())
        })
    }

    fn checked_pending_candidate_bytes(&self) -> Option<usize> {
        self.lineages.values().try_fold(0usize, |total, lineage| {
            total.checked_add(lineage.pending_candidate_bytes())
        })
    }

    fn checked_accounted_state_bytes(&self) -> Option<usize> {
        self.checked_retained_state_bytes()?.checked_add(self.checked_pending_candidate_bytes()?)
    }

    fn aggregate_reservations_fit(&self) -> bool {
        let pending_count = self
            .lineages
            .values()
            .filter(|lineage| lineage.has_pending())
            .count();
        let Some(accounted_state_bytes) = self.checked_accounted_state_bytes() else {
            return false;
        };
        let Some(retained_image_count) = self.checked_retained_image_count() else {
            return false;
        };
        let Some(retained_state_bytes) = self.checked_retained_state_bytes() else {
            return false;
        };
        let Some(pending_target_state_bytes) = self.checked_pending_state_bytes() else {
            return false;
        };
        let Some(emission_evidence_count) = self.checked_emission_evidence_count() else {
            return false;
        };
        let Some(projected_image_count) = retained_image_count.checked_add(pending_count) else {
            return false;
        };
        let Some(projected_retained_state_bytes) =
            retained_state_bytes.checked_add(pending_target_state_bytes)
        else {
            return false;
        };
        let Some(projected_evidence_count) = emission_evidence_count.checked_add(pending_count) else {
            return false;
        };

        accounted_state_bytes <= self.limits.max_state_bytes()
            && projected_image_count <= self.limits.max_retained_images()
            && projected_retained_state_bytes <= self.limits.max_retained_state_bytes()
            && projected_evidence_count <= self.limits.max_emission_evidence()
    }
}
