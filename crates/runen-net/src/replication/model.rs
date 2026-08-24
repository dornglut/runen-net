use std::num::NonZeroUsize;

use crate::identity::{ParticipantId, SessionId, SimulationTick};

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

    pub(crate) fn checked_next(self) -> Option<Self> {
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

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct ClientAggregateLimits {
    max_lineages: NonZeroUsize,
    max_retained_images: NonZeroUsize,
    max_retained_state_bytes: NonZeroUsize,
}

impl ClientAggregateLimits {
    pub const fn new(
        max_lineages: NonZeroUsize,
        max_retained_images: NonZeroUsize,
        max_retained_state_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            max_lineages,
            max_retained_images,
            max_retained_state_bytes,
        }
    }

    pub const fn max_lineages(self) -> usize {
        self.max_lineages.get()
    }

    pub const fn max_retained_images(self) -> usize {
        self.max_retained_images.get()
    }

    pub const fn max_retained_state_bytes(self) -> usize {
        self.max_retained_state_bytes.get()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct AuthorityAggregateLimits {
    max_lineages: NonZeroUsize,
    max_state_bytes: NonZeroUsize,
    max_retained_images: NonZeroUsize,
    max_retained_state_bytes: NonZeroUsize,
    max_emission_evidence: NonZeroUsize,
}

impl AuthorityAggregateLimits {
    pub const fn new(
        max_lineages: NonZeroUsize,
        max_state_bytes: NonZeroUsize,
        max_retained_images: NonZeroUsize,
        max_retained_state_bytes: NonZeroUsize,
        max_emission_evidence: NonZeroUsize,
    ) -> Self {
        Self {
            max_lineages,
            max_state_bytes,
            max_retained_images,
            max_retained_state_bytes,
            max_emission_evidence,
        }
    }

    pub const fn max_lineages(self) -> usize {
        self.max_lineages.get()
    }

    pub const fn max_state_bytes(self) -> usize {
        self.max_state_bytes.get()
    }

    pub const fn max_retained_images(self) -> usize {
        self.max_retained_images.get()
    }

    pub const fn max_retained_state_bytes(self) -> usize {
        self.max_retained_state_bytes.get()
    }

    pub const fn max_emission_evidence(self) -> usize {
        self.max_emission_evidence.get()
    }
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

    pub(crate) fn into_parts(self) -> (ReplicationCursor, SimulationTick, AccountedState<S>) {
        (self.target_cursor, self.target_tick, self.image)
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

    pub(crate) fn into_parts(self) -> (ReplicationCursor, ReplicationCursor, SimulationTick, D) {
        (
            self.base_cursor,
            self.target_cursor,
            self.target_tick,
            self.delta,
        )
    }
}
