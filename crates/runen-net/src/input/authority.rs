use std::collections::{BTreeMap, HashMap};

use crate::identity::{ConnectionHandle, ParticipantId, SessionId, SimulationTick};
use crate::session::Session;

use super::model::{
    AuthorityInputAggregateLimits, AuthorityInputLimits, AuthorityInputOutcome, InputWindow,
};

#[derive(Debug, Clone)]
struct RetainedInput<I> {
    value: I,
    accounted_bytes: usize,
}

#[derive(Debug)]
struct ParticipantInputState<I> {
    limits: AuthorityInputLimits,
    window: InputWindow,
    retained: BTreeMap<SimulationTick, RetainedInput<I>>,
    retained_bytes: usize,
}

impl<I> ParticipantInputState<I> {
    fn new(limits: AuthorityInputLimits, window: InputWindow) -> Self {
        Self {
            limits,
            window,
            retained: BTreeMap::new(),
            retained_bytes: 0,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum AuthorityInputError {
    SessionMismatch,
    ParticipantAlreadyConfigured,
    ParticipantNotConfigured,
    ParticipantNotLive,
    WindowMinimumRegression,
    WindowMaximumRegression,
    WindowExceedsFutureHorizon,
    AccountingOverflow,
    AccountingInvariantViolation,
}

#[derive(Debug)]
pub struct AuthorityInputSession<I> {
    session: SessionId,
    limits: AuthorityInputAggregateLimits,
    participants: HashMap<ParticipantId, ParticipantInputState<I>>,
    retained_keys: usize,
    retained_bytes: usize,
}

impl<I> AuthorityInputSession<I> {
    pub fn new(session: SessionId, limits: AuthorityInputAggregateLimits) -> Self {
        Self {
            session,
            limits,
            participants: HashMap::new(),
            retained_keys: 0,
            retained_bytes: 0,
        }
    }

    pub const fn session_id(&self) -> SessionId {
        self.session
    }

    pub fn participant_count(&self) -> usize {
        self.participants.len()
    }

    pub const fn retained_key_count(&self) -> usize {
        self.retained_keys
    }

    pub const fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }

    pub fn participant_window(&self, participant: ParticipantId) -> Option<InputWindow> {
        self.participants.get(&participant).map(|state| state.window)
    }

    pub fn participant_retained_key_count(&self, participant: ParticipantId) -> Option<usize> {
        self.participants
            .get(&participant)
            .map(|state| state.retained.len())
    }

    pub fn participant_retained_bytes(&self, participant: ParticipantId) -> Option<usize> {
        self.participants
            .get(&participant)
            .map(|state| state.retained_bytes)
    }

    pub fn add_participant(
        &mut self,
        session: &Session,
        participant: ParticipantId,
        window: InputWindow,
        limits: AuthorityInputLimits,
    ) -> Result<(), AuthorityInputError> {
        self.ensure_session(session)?;
        if session.membership_state(participant).is_none() {
            return Err(AuthorityInputError::ParticipantNotLive);
        }
        if self.participants.contains_key(&participant) {
            return Err(AuthorityInputError::ParticipantAlreadyConfigured);
        }
        if window.span() > limits.max_future_tick_distance() {
            return Err(AuthorityInputError::WindowExceedsFutureHorizon);
        }
        self.participants
            .insert(participant, ParticipantInputState::new(limits, window));
        Ok(())
    }

    pub fn advance_window(
        &mut self,
        participant: ParticipantId,
        next: InputWindow,
    ) -> Result<(), AuthorityInputError> {
        let state = self
            .participants
            .get(&participant)
            .ok_or(AuthorityInputError::ParticipantNotConfigured)?;
        if next.minimum() < state.window.minimum() {
            return Err(AuthorityInputError::WindowMinimumRegression);
        }
        if next.maximum() < state.window.maximum() {
            return Err(AuthorityInputError::WindowMaximumRegression);
        }
        if next.span() > state.limits.max_future_tick_distance() {
            return Err(AuthorityInputError::WindowExceedsFutureHorizon);
        }

        let mut removed_keys = 0usize;
        let mut removed_bytes = 0usize;
        for retained in state.retained.range(..next.minimum()).map(|(_, retained)| retained) {
            removed_keys = removed_keys
                .checked_add(1)
                .ok_or(AuthorityInputError::AccountingOverflow)?;
            removed_bytes = removed_bytes
                .checked_add(retained.accounted_bytes)
                .ok_or(AuthorityInputError::AccountingOverflow)?;
        }

        let next_participant_bytes = state
            .retained_bytes
            .checked_sub(removed_bytes)
            .ok_or(AuthorityInputError::AccountingInvariantViolation)?;
        let next_retained_keys = self
            .retained_keys
            .checked_sub(removed_keys)
            .ok_or(AuthorityInputError::AccountingInvariantViolation)?;
        let next_retained_bytes = self
            .retained_bytes
            .checked_sub(removed_bytes)
            .ok_or(AuthorityInputError::AccountingInvariantViolation)?;

        let state = self
            .participants
            .get_mut(&participant)
            .expect("participant checked above");
        let minimum = next.minimum();
        state.retained = state.retained.split_off(&minimum);
        state.retained_bytes = next_participant_bytes;
        state.window = next;
        self.retained_keys = next_retained_keys;
        self.retained_bytes = next_retained_bytes;
        Ok(())
    }

    pub fn reconcile_memberships(
        &mut self,
        session: &Session,
    ) -> Result<Vec<ParticipantId>, AuthorityInputError> {
        self.ensure_session(session)?;
        let mut removed: Vec<_> = self
            .participants
            .keys()
            .copied()
            .filter(|participant| session.membership_state(*participant).is_none())
            .collect();
        removed.sort_by_key(|participant| participant.get());

        let mut removed_keys = 0usize;
        let mut removed_bytes = 0usize;
        for participant in &removed {
            let state = self
                .participants
                .get(participant)
                .expect("participant selected from current map");
            removed_keys = removed_keys
                .checked_add(state.retained.len())
                .ok_or(AuthorityInputError::AccountingOverflow)?;
            removed_bytes = removed_bytes
                .checked_add(state.retained_bytes)
                .ok_or(AuthorityInputError::AccountingOverflow)?;
        }

        let next_retained_keys = self
            .retained_keys
            .checked_sub(removed_keys)
            .ok_or(AuthorityInputError::AccountingInvariantViolation)?;
        let next_retained_bytes = self
            .retained_bytes
            .checked_sub(removed_bytes)
            .ok_or(AuthorityInputError::AccountingInvariantViolation)?;

        for participant in &removed {
            self.participants.remove(participant);
        }
        self.retained_keys = next_retained_keys;
        self.retained_bytes = next_retained_bytes;
        Ok(removed)
    }

    pub fn submit(
        &mut self,
        session: &Session,
        participant: ParticipantId,
        connection: ConnectionHandle,
        target_tick: SimulationTick,
        input: &I,
        accounted_bytes: usize,
    ) -> Result<AuthorityInputOutcome, AuthorityInputError>
    where
        I: Clone + Eq,
    {
        self.ensure_session(session)?;
        if !session.is_authorized(participant, connection) {
            return Ok(AuthorityInputOutcome::UnauthorizedInput);
        }

        let state = self
            .participants
            .get(&participant)
            .ok_or(AuthorityInputError::ParticipantNotConfigured)?;

        if target_tick < state.window.minimum() {
            return Ok(AuthorityInputOutcome::StaleInput);
        }
        if let Some(retained) = state.retained.get(&target_tick) {
            return Ok(if retained.value == *input {
                AuthorityInputOutcome::DuplicateInput
            } else {
                AuthorityInputOutcome::ConflictingInput
            });
        }
        if target_tick > state.window.maximum() {
            return Ok(AuthorityInputOutcome::FutureInputOutsideWindow);
        }

        if accounted_bytes > state.limits.max_batch_bytes()
            || state.retained.len() >= state.limits.max_retained_keys_per_participant()
            || self.retained_keys >= self.limits.max_retained_keys()
        {
            return Ok(AuthorityInputOutcome::InputResourceRejected);
        }

        let Some(next_participant_bytes) = state.retained_bytes.checked_add(accounted_bytes) else {
            return Ok(AuthorityInputOutcome::InputResourceRejected);
        };
        if next_participant_bytes > state.limits.max_retained_bytes_per_participant() {
            return Ok(AuthorityInputOutcome::InputResourceRejected);
        }
        let Some(next_retained_keys) = self.retained_keys.checked_add(1) else {
            return Ok(AuthorityInputOutcome::InputResourceRejected);
        };
        if next_retained_keys > self.limits.max_retained_keys() {
            return Ok(AuthorityInputOutcome::InputResourceRejected);
        }
        let Some(next_retained_bytes) = self.retained_bytes.checked_add(accounted_bytes) else {
            return Ok(AuthorityInputOutcome::InputResourceRejected);
        };
        if next_retained_bytes > self.limits.max_retained_bytes() {
            return Ok(AuthorityInputOutcome::InputResourceRejected);
        }

        let state = self
            .participants
            .get_mut(&participant)
            .expect("participant checked above");
        let previous = state.retained.insert(
            target_tick,
            RetainedInput {
                value: input.clone(),
                accounted_bytes,
            },
        );
        debug_assert!(previous.is_none());
        state.retained_bytes = next_participant_bytes;
        self.retained_keys = next_retained_keys;
        self.retained_bytes = next_retained_bytes;
        Ok(AuthorityInputOutcome::InputAccepted)
    }

    fn ensure_session(&self, session: &Session) -> Result<(), AuthorityInputError> {
        if session.id() == self.session {
            Ok(())
        } else {
            Err(AuthorityInputError::SessionMismatch)
        }
    }
}
