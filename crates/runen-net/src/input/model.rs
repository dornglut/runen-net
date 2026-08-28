use std::num::NonZeroUsize;

use crate::identity::SimulationTick;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct InputWindow {
    minimum: SimulationTick,
    maximum: SimulationTick,
}

impl InputWindow {
    pub fn new(
        minimum: SimulationTick,
        maximum: SimulationTick,
    ) -> Result<Self, InputWindowError> {
        if maximum < minimum {
            return Err(InputWindowError::MaximumBeforeMinimum);
        }
        Ok(Self { minimum, maximum })
    }

    pub const fn minimum(self) -> SimulationTick {
        self.minimum
    }

    pub const fn maximum(self) -> SimulationTick {
        self.maximum
    }

    pub(crate) const fn span(self) -> u64 {
        self.maximum.get() - self.minimum.get()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum InputWindowError {
    MaximumBeforeMinimum,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct AuthorityInputLimits {
    max_batch_bytes: NonZeroUsize,
    max_retained_keys_per_participant: NonZeroUsize,
    max_retained_bytes_per_participant: NonZeroUsize,
    max_future_tick_distance: u64,
}

impl AuthorityInputLimits {
    pub fn new(
        max_batch_bytes: NonZeroUsize,
        max_retained_keys_per_participant: NonZeroUsize,
        max_retained_bytes_per_participant: NonZeroUsize,
        max_future_tick_distance: u64,
    ) -> Result<Self, AuthorityInputLimitError> {
        if max_batch_bytes.get() > max_retained_bytes_per_participant.get() {
            return Err(AuthorityInputLimitError::BatchExceedsParticipantBudget);
        }
        Ok(Self {
            max_batch_bytes,
            max_retained_keys_per_participant,
            max_retained_bytes_per_participant,
            max_future_tick_distance,
        })
    }

    pub const fn max_batch_bytes(self) -> usize {
        self.max_batch_bytes.get()
    }

    pub const fn max_retained_keys_per_participant(self) -> usize {
        self.max_retained_keys_per_participant.get()
    }

    pub const fn max_retained_bytes_per_participant(self) -> usize {
        self.max_retained_bytes_per_participant.get()
    }

    pub const fn max_future_tick_distance(self) -> u64 {
        self.max_future_tick_distance
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AuthorityInputLimitError {
    BatchExceedsParticipantBudget,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct AuthorityInputAggregateLimits {
    max_retained_keys: NonZeroUsize,
    max_retained_bytes: NonZeroUsize,
}

impl AuthorityInputAggregateLimits {
    pub const fn new(max_retained_keys: NonZeroUsize, max_retained_bytes: NonZeroUsize) -> Self {
        Self {
            max_retained_keys,
            max_retained_bytes,
        }
    }

    pub const fn max_retained_keys(self) -> usize {
        self.max_retained_keys.get()
    }

    pub const fn max_retained_bytes(self) -> usize {
        self.max_retained_bytes.get()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AuthorityInputOutcome {
    InputAccepted,
    DuplicateInput,
    ConflictingInput,
    StaleInput,
    FutureInputOutsideWindow,
    InputResourceRejected,
    UnauthorizedInput,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct PredictionLimits {
    max_pending_inputs: NonZeroUsize,
    max_pending_bytes: NonZeroUsize,
    max_future_tick_distance: u64,
}

impl PredictionLimits {
    pub const fn new(
        max_pending_inputs: NonZeroUsize,
        max_pending_bytes: NonZeroUsize,
        max_future_tick_distance: u64,
    ) -> Self {
        Self {
            max_pending_inputs,
            max_pending_bytes,
            max_future_tick_distance,
        }
    }

    pub const fn max_pending_inputs(self) -> usize {
        self.max_pending_inputs.get()
    }

    pub const fn max_pending_bytes(self) -> usize {
        self.max_pending_bytes.get()
    }

    pub const fn max_future_tick_distance(self) -> u64 {
        self.max_future_tick_distance
    }
}
