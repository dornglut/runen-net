use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use crate::delivery::SubmissionOutcome;
use crate::identity::{ConnectionHandle, ParticipantId, SessionId, SimulationTick};
use crate::session::Session;

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct ReplicationLineageKey {
    session: SessionId,
    participant: ParticipantId,
}

impl ReplicationLineageKey {
    pub const fn new(session: SessionId, participant: ParticipantId) -> Self {
        Self {
            session,
            participant,
        }
    }

    pub const fn session(self) -> SessionId {
        self.session
    }

    pub const fn participant(self) -> ParticipantId {
        self.participant
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReplicationCursor(u64);

impl ReplicationCursor {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecoveryGeneration(u64);

impl RecoveryGeneration {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    fn checked_next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountedState<S> {
    state: S,
    accounted_bytes: usize,
}

impl<S> AccountedState<S> {
    pub const fn new(state: S, accounted_bytes: usize) -> Self {
        Self {
            state,
            accounted_bytes,
        }
    }

    pub const fn state(&self) -> &S {
        &self.state
    }

    pub const fn accounted_bytes(&self) -> usize {
        self.accounted_bytes
    }

    pub fn into_state(self) -> S {
        self.state
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ReplicationRetentionLimits {
    max_state_image_bytes: NonZeroUsize,
    max_retained_images_per_lineage: NonZeroUsize,
    max_retained_state_bytes_per_lineage: NonZeroUsize,
    max_candidate_bytes_per_lineage: NonZeroUsize,
    max_emission_evidence_per_lineage: NonZeroUsize,
}

impl ReplicationRetentionLimits {
    pub fn new(
        max_state_image_bytes: NonZeroUsize,
        max_retained_images_per_lineage: NonZeroUsize,
        max_retained_state_bytes_per_lineage: NonZeroUsize,
        max_candidate_bytes_per_lineage: NonZeroUsize,
        max_emission_evidence_per_lineage: NonZeroUsize,
    ) -> Result<Self, ReplicationLimitError> {
        if max_state_image_bytes.get() > max_retained_state_bytes_per_lineage.get() {
            return Err(ReplicationLimitError::StateImageExceedsRetainedBudget);
        }
        if max_state_image_bytes.get() > max_candidate_bytes_per_lineage.get() {
            return Err(ReplicationLimitError::StateImageExceedsCandidateBudget);
        }

        Ok(Self {
            max_state_image_bytes,
            max_retained_images_per_lineage,
            max_retained_state_bytes_per_lineage,
            max_candidate_bytes_per_lineage,
            max_emission_evidence_per_lineage,
        })
    }

    pub const fn max_state_image_bytes(self) -> usize {
        self.max_state_image_bytes.get()
    }

    pub const fn max_retained_images_per_lineage(self) -> usize {
        self.max_retained_images_per_lineage.get()
    }

    pub const fn max_retained_state_bytes_per_lineage(self) -> usize {
        self.max_retained_state_bytes_per_lineage.get()
    }

    pub const fn max_candidate_bytes_per_lineage(self) -> usize {
        self.max_candidate_bytes_per_lineage.get()
    }

    pub const fn max_emission_evidence_per_lineage(self) -> usize {
        self.max_emission_evidence_per_lineage.get()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ReplicationLimitError {
    StateImageExceedsRetainedBudget,
    StateImageExceedsCandidateBudget,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullSnapshot<S> {
    target_cursor: ReplicationCursor,
    target_tick: SimulationTick,
    image: AccountedState<S>,
}

impl<S> FullSnapshot<S> {
    pub const fn new(
        target_cursor: ReplicationCursor,
        target_tick: SimulationTick,
        image: AccountedState<S>,
    ) -> Self {
        Self {
            target_cursor,
            target_tick,
            image,
        }
    }

    pub const fn target_cursor(&self) -> ReplicationCursor {
        self.target_cursor
    }

    pub const fn target_tick(&self) -> SimulationTick {
        self.target_tick
    }

    pub const fn image(&self) -> &AccountedState<S> {
        &self.image
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeltaSnapshot<D> {
    base_cursor: ReplicationCursor,
    target_cursor: ReplicationCursor,
    target_tick: SimulationTick,
    delta: D,
}

impl<D> DeltaSnapshot<D> {
    pub const fn new(
        base_cursor: ReplicationCursor,
        target_cursor: ReplicationCursor,
        target_tick: SimulationTick,
        delta: D,
    ) -> Self {
        Self {
            base_cursor,
            target_cursor,
            target_tick,
            delta,
        }
    }

    pub const fn base_cursor(&self) -> ReplicationCursor {
        self.base_cursor
    }

    pub const fn target_cursor(&self) -> ReplicationCursor {
        self.target_cursor
    }

    pub const fn target_tick(&self) -> SimulationTick {
        self.target_tick
    }

    pub const fn delta(&self) -> &D {
        &self.delta
    }
}

#[derive(Debug)]
struct RetainedState<S> {
    tick: SimulationTick,
    image: AccountedState<S>,
}

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
    pub fn new(key: ReplicationLineageKey, limits: ReplicationRetentionLimits) -> Self {
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
        self.current.and_then(|cursor| {
            self.retained
                .get(&cursor)
                .map(|retained| retained.tick)
        })
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

    pub fn acknowledgement_cursor(&self) -> Option<ReplicationCursor> {
        self.current
    }

    pub fn apply_full<E, F>(
        &mut self,
        snapshot: FullSnapshot<S>,
        host_commit: F,
    ) -> ClientSnapshotOutcome
    where
        F: FnOnce(&S) -> Result<(), E>,
    {
        if let Some(classification) =
            self.classify_target(snapshot.target_cursor, snapshot.target_tick)
        {
            return classification;
        }

        if !self.image_fits(snapshot.image.accounted_bytes()) {
            return ClientSnapshotOutcome::StateResourceFailure;
        }

        if host_commit(snapshot.image.state()).is_err() {
            return ClientSnapshotOutcome::HostCommitFailure;
        }

        let target = snapshot.target_cursor;
        self.commit_image(target, snapshot.target_tick, snapshot.image);
        self.state = ClientReplicationState::Synchronized;
        ClientSnapshotOutcome::Committed(target)
    }

    pub fn apply_delta<D, E, R, C>(
        &mut self,
        snapshot: DeltaSnapshot<D>,
        reconstruct: R,
        host_commit: C,
    ) -> ClientSnapshotOutcome
    where
        R: FnOnce(&S, &D) -> Result<AccountedState<S>, DeltaReconstructionError>,
        C: FnOnce(&S) -> Result<(), E>,
    {
        if let Some(classification) =
            self.classify_target(snapshot.target_cursor, snapshot.target_tick)
        {
            return classification;
        }

        if matches!(
            self.state,
            ClientReplicationState::FullSnapshotRequired(_)
        ) {
            return ClientSnapshotOutcome::DeltaBlockedByRecovery;
        }

        if snapshot.target_cursor <= snapshot.base_cursor {
            self.state = ClientReplicationState::FullSnapshotRequired(
                ClientRecoveryReason::MalformedDelta,
            );
            return ClientSnapshotOutcome::MalformedDelta;
        }

        let Some(base) = self.retained.get(&snapshot.base_cursor) else {
            self.state = ClientReplicationState::FullSnapshotRequired(ClientRecoveryReason::MissingBase);
            return ClientSnapshotOutcome::MissingBase;
        };

        if snapshot.target_tick < base.tick {
            self.state = ClientReplicationState::FullSnapshotRequired(
                ClientRecoveryReason::DeltaTickRegression,
            );
            return ClientSnapshotOutcome::TickRegression;
        }

        let candidate = match reconstruct(base.image.state(), &snapshot.delta) {
            Ok(candidate) => candidate,
            Err(DeltaReconstructionError::Malformed) => {
                self.state = ClientReplicationState::FullSnapshotRequired(
                    ClientRecoveryReason::MalformedDelta,
                );
                return ClientSnapshotOutcome::MalformedDelta;
            }
            Err(DeltaReconstructionError::ReconstructionFailed) => {
                self.state = ClientReplicationState::FullSnapshotRequired(
                    ClientRecoveryReason::ReconstructionFailure,
                );
                return ClientSnapshotOutcome::ReconstructionFailure;
            }
        };

        if !self.image_fits(candidate.accounted_bytes()) {
            return ClientSnapshotOutcome::StateResourceFailure;
        }

        if host_commit(candidate.state()).is_err() {
            self.state = ClientReplicationState::FullSnapshotRequired(
                ClientRecoveryReason::DeltaCommitFailure,
            );
            return ClientSnapshotOutcome::HostCommitFailure;
        }

        let target = snapshot.target_cursor;
        self.commit_image(target, snapshot.target_tick, candidate);
        ClientSnapshotOutcome::Committed(target)
    }

    pub fn require_connection_replacement_full(&mut self) {
        self.state = ClientReplicationState::FullSnapshotRequired(
            ClientRecoveryReason::ConnectionReplacement,
        );
    }

    pub fn evict_historical(&mut self, cursor: ReplicationCursor) -> bool {
        if self.current == Some(cursor) {
            return false;
        }
        self.remove_retained(cursor).is_some()
    }

    fn classify_target(
        &self,
        target: ReplicationCursor,
        tick: SimulationTick,
    ) -> Option<ClientSnapshotOutcome> {
        let current = self.current?;
        if target < current {
            return Some(ClientSnapshotOutcome::Stale);
        }
        if target == current {
            return Some(ClientSnapshotOutcome::DuplicateCurrent);
        }
        let current_tick = self
            .retained
            .get(&current)
            .expect("current cursor has retained state")
            .tick;
        if tick < current_tick {
            return Some(ClientSnapshotOutcome::TickRegression);
        }
        None
    }

    fn image_fits(&self, bytes: usize) -> bool {
        bytes <= self.limits.max_state_image_bytes()
            && bytes <= self.limits.max_candidate_bytes_per_lineage()
            && bytes <= self.limits.max_retained_state_bytes_per_lineage()
    }

    fn commit_image(
        &mut self,
        cursor: ReplicationCursor,
        tick: SimulationTick,
        image: AccountedState<S>,
    ) {
        self.make_room_for_client_commit(image.accounted_bytes());
        self.retained_bytes += image.accounted_bytes();
        let previous = self.retained.insert(cursor, RetainedState { tick, image });
        debug_assert!(previous.is_none());
        self.current = Some(cursor);
        self.state = ClientReplicationState::Synchronized;
    }

    fn make_room_for_client_commit(&mut self, target_bytes: usize) {
        let max_count = self.limits.max_retained_images_per_lineage();
        let max_bytes = self.limits.max_retained_state_bytes_per_lineage();
        while self.retained.len() >= max_count
            || self.retained_bytes > max_bytes - target_bytes
        {
            let Some(cursor) = self.retained.keys().next().copied() else {
                break;
            };
            let _ = self.remove_retained(cursor);
        }
    }

    fn remove_retained(&mut self, cursor: ReplicationCursor) -> Option<RetainedState<S>> {
        let removed = self.retained.remove(&cursor)?;
        self.retained_bytes -= removed.image.accounted_bytes();
        Some(removed)
    }
}

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

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct EmissionEvidence {
    kind: SnapshotKind,
    recovery_generation: Option<RecoveryGeneration>,
}

#[derive(Debug)]
enum SnapshotCandidate<S, D> {
    Full {
        image: FullSnapshot<S>,
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
    pub fn new(key: ReplicationLineageKey, limits: ReplicationRetentionLimits) -> Self {
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
                image,
                recovery_generation,
            } => Some(PendingSnapshotRef::Full {
                snapshot: image,
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
                image,
                recovery_generation,
            } => Some(PendingSnapshotSummary {
                kind: SnapshotKind::Full,
                base_cursor: None,
                target_cursor: image.target_cursor,
                target_tick: image.target_tick,
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

    pub fn prepare_full(
        &mut self,
        snapshot: FullSnapshot<S>,
        designate_recovery_full: bool,
    ) -> Result<PendingSnapshotSummary, AuthorityPrepareError> {
        self.require_candidate_slot()?;
        self.validate_new_emission_target(snapshot.target_cursor, snapshot.target_tick)?;
        if !self.candidate_fits(snapshot.image.accounted_bytes(), 0) {
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
                        .is_some_and(|watermark| snapshot.target_cursor <= watermark)
                    {
                        return Err(
                            AuthorityPrepareError::RecoveryFullNotNewerThanWatermark,
                        );
                    }
                    Some(generation)
                }
                AuthorityReplicationState::DeltaEligible(_) => None,
            }
        } else {
            None
        };

        let summary = PendingSnapshotSummary {
            kind: SnapshotKind::Full,
            base_cursor: None,
            target_cursor: snapshot.target_cursor,
            target_tick: snapshot.target_tick,
            recovery_generation,
        };
        self.pending = Some(SnapshotCandidate::Full {
            image: snapshot,
            recovery_generation,
        });
        Ok(summary)
    }

    pub fn prepare_delta(
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

    pub fn record_delivery_submission(
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
                image,
                recovery_generation,
            } => {
                let emitted = EmittedSnapshot {
                    kind: SnapshotKind::Full,
                    base_cursor: None,
                    target_cursor: image.target_cursor,
                    target_tick: image.target_tick,
                    recovery_generation,
                };
                self.record_emitted_state(
                    image.target_cursor,
                    image.target_tick,
                    image.image,
                    SnapshotKind::Full,
                    recovery_generation,
                );
                emitted
            }
            SnapshotCandidate::Delta {
                base_cursor,
                target_cursor,
                target_tick,
                target_image,
                additional_candidate_bytes,
                ..
            } => {
                let _ = additional_candidate_bytes;
                let emitted = EmittedSnapshot {
                    kind: SnapshotKind::Delta,
                    base_cursor: Some(base_cursor),
                    target_cursor,
                    target_tick,
                    recovery_generation: None,
                };
                self.record_emitted_state(
                    target_cursor,
                    target_tick,
                    target_image,
                    SnapshotKind::Delta,
                    None,
                );
                emitted
            }
        };
        Ok(Some(emitted))
    }

    pub fn cancel_pending(&mut self) -> bool {
        self.pending.take().is_some()
    }

    pub fn acknowledge_authorized(
        &mut self,
        session: &Session,
        connection: ConnectionHandle,
        cursor: ReplicationCursor,
    ) -> Result<AuthorityAckOutcome, AuthorityOperationError> {
        if session.id() != self.key.session
            || !session.is_authorized(self.key.participant, connection)
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
        let needs_new_recovery = matches!(
            self.state,
            AuthorityReplicationState::DeltaEligible(_)
        ) && !baseline_available;
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
                    let generation = next_generation.expect("precomputed above");
                    self.install_new_recovery(
                        AuthorityRecoveryReason::ConfirmedBaselineUnavailable,
                        generation,
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

    pub fn require_full_recovery(
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

    pub fn connection_replaced(
        &mut self,
        session: &Session,
        connection: ConnectionHandle,
    ) -> Result<(), AuthorityOperationError> {
        if session.id() != self.key.session
            || !session.is_authorized(self.key.participant, connection)
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

    pub fn evict_retained_state(
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
        state_bytes <= max
            && additional_bytes <= max - state_bytes
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

        while self.retained.len() >= max_count
            || self.retained_bytes > max_bytes - target_bytes
        {
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

    fn retain_emission_evidence(
        &mut self,
        cursor: ReplicationCursor,
        evidence: EmissionEvidence,
    ) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{
        CompatibilityOffer, NegotiatedContract, NegotiationManager, NegotiationManagerLimits,
        NegotiationRequirements, NegotiationStatus, OfferLimits, ProtocolContract, ProtocolId,
        ProtocolRevision,
    };
    use crate::session::SessionLimits;

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    fn limits() -> ReplicationRetentionLimits {
        ReplicationRetentionLimits::new(nz(64), nz(4), nz(256), nz(96), nz(4)).unwrap()
    }

    fn key(session: u128, participant: u128) -> ReplicationLineageKey {
        ReplicationLineageKey::new(SessionId::new(session), ParticipantId::new(participant))
    }

    fn full(cursor: u64, tick: u64, value: i32) -> FullSnapshot<i32> {
        FullSnapshot::new(
            ReplicationCursor::new(cursor),
            SimulationTick::new(tick),
            AccountedState::new(value, 8),
        )
    }

    fn protocol() -> ProtocolContract {
        ProtocolContract::new(ProtocolId::new(1), ProtocolRevision::new(1))
    }

    fn authorized_session(
        lineage: ReplicationLineageKey,
        connection: ConnectionHandle,
    ) -> Session {
        let mut negotiation = NegotiationManager::new(
            OfferLimits::default(),
            NegotiationManagerLimits::default(),
        )
        .unwrap();
        let offer = CompatibilityOffer::new(vec![protocol()], vec![], vec![], None);
        negotiation
            .start(connection, offer.clone(), offer)
            .unwrap();
        let contract = NegotiatedContract::new(protocol());
        negotiation
            .propose(
                connection,
                contract.clone(),
                &NegotiationRequirements::default(),
            )
            .unwrap();
        assert_ne!(
            negotiation
                .validate_authority(connection, &contract)
                .unwrap(),
            NegotiationStatus::Established
        );
        assert_eq!(
            negotiation.validate_peer(connection, &contract).unwrap(),
            NegotiationStatus::Established
        );

        let mut session = Session::new(
            lineage.session(),
            SessionLimits::new(nz(8), nz(4)).unwrap(),
        );
        session
            .admit_new(
                lineage.participant(),
                negotiation.established(connection).unwrap(),
            )
            .unwrap();
        session
    }

    #[test]
    fn lineage_key_keeps_equal_participants_in_different_sessions_distinct() {
        assert_ne!(key(1, 7), key(2, 7));
    }

    #[test]
    fn client_reconstructs_from_declared_historical_base_not_current() {
        let mut client = ClientLineage::new(key(1, 7), limits());
        assert_eq!(
            client.apply_full(full(1, 1, 10), |_| Ok::<_, ()>(())),
            ClientSnapshotOutcome::Committed(ReplicationCursor::new(1))
        );
        assert_eq!(
            client.apply_full(full(3, 3, 30), |_| Ok::<_, ()>(())),
            ClientSnapshotOutcome::Committed(ReplicationCursor::new(3))
        );

        let delta = DeltaSnapshot::new(
            ReplicationCursor::new(1),
            ReplicationCursor::new(4),
            SimulationTick::new(4),
            5,
        );
        let outcome = client.apply_delta(
            delta,
            |base, delta| Ok(AccountedState::new(*base + *delta, 8)),
            |_| Ok::<_, ()>(()),
        );
        assert_eq!(
            outcome,
            ClientSnapshotOutcome::Committed(ReplicationCursor::new(4))
        );
        assert_eq!(client.current_state(), Some(&15));
    }

    #[test]
    fn evidence_eviction_is_unverifiable_not_future() {
        let small_evidence =
            ReplicationRetentionLimits::new(nz(64), nz(4), nz(256), nz(96), nz(1)).unwrap();
        let lineage = key(1, 7);
        let connection = ConnectionHandle::new(1);
        let session = authorized_session(lineage, connection);
        let mut authority: AuthorityLineage<i32, ()> =
            AuthorityLineage::new(lineage, small_evidence);

        authority.prepare_full(full(1, 1, 10), true).unwrap();
        authority
            .record_delivery_submission(SubmissionOutcome::Accepted {
                accepted_index: 0,
                local_pressure_drops: 0,
            })
            .unwrap();
        authority.prepare_full(full(2, 2, 20), true).unwrap();
        authority
            .record_delivery_submission(SubmissionOutcome::Accepted {
                accepted_index: 1,
                local_pressure_drops: 0,
            })
            .unwrap();

        assert_eq!(
            authority
                .acknowledge_authorized(&session, connection, ReplicationCursor::new(1))
                .unwrap(),
            AuthorityAckOutcome::UnverifiableConfirmation
        );
        assert_eq!(
            authority
                .acknowledge_authorized(&session, connection, ReplicationCursor::new(3))
                .unwrap(),
            AuthorityAckOutcome::FutureConfirmation
        );
    }

    #[test]
    fn connection_replacement_always_starts_new_recovery_generation() {
        let lineage = key(1, 7);
        let first = ConnectionHandle::new(1);
        let session = authorized_session(lineage, first);
        let mut authority: AuthorityLineage<i32, ()> = AuthorityLineage::new(lineage, limits());
        let before = match authority.replication_state() {
            AuthorityReplicationState::FullSnapshotRequired { generation, .. } => generation,
            AuthorityReplicationState::DeltaEligible(_) => panic!("new lineage must require full"),
        };

        authority.connection_replaced(&session, first).unwrap();
        let after = match authority.replication_state() {
            AuthorityReplicationState::FullSnapshotRequired { generation, .. } => generation,
            AuthorityReplicationState::DeltaEligible(_) => panic!("replacement must require full"),
        };
        assert!(after > before);
    }
}
