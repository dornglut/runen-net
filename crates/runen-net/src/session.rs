use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};

use crate::identity::{IncarnationClaimError, IncarnationRegistry, ParticipantId, SessionId};
use crate::protocol::EstablishedNegotiation;
use crate::identity::ConnectionHandle;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SessionPhase {
    Open,
    Closed,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct SessionLimits {
    max_participant_incarnations: NonZeroUsize,
    max_memberships: NonZeroUsize,
}

impl SessionLimits {
    pub fn new(
        max_participant_incarnations: NonZeroUsize,
        max_memberships: NonZeroUsize,
    ) -> Result<Self, SessionLimitError> {
        if max_memberships.get() > max_participant_incarnations.get() {
            return Err(SessionLimitError::MembershipsExceedParticipantIncarnations);
        }

        Ok(Self {
            max_participant_incarnations,
            max_memberships,
        })
    }

    pub const fn max_participant_incarnations(self) -> usize {
        self.max_participant_incarnations.get()
    }

    pub const fn max_memberships(self) -> usize {
        self.max_memberships.get()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SessionLimitError {
    MembershipsExceedParticipantIncarnations,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum MembershipState {
    Bound(ConnectionHandle),
    Unbound {
        expires_at: u64,
        previous_connection: ConnectionHandle,
    },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RetentionPolicy {
    Terminate,
    RetainForRecovery { duration: NonZeroU64 },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ConnectionLossOutcome {
    Terminated,
    Retained { expires_at: u64 },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SessionError {
    Closed,
    ParticipantIdAlreadyUsed,
    ParticipantIncarnationLimitExceeded,
    MembershipLimitExceeded,
    ConnectionAlreadyBound,
    ParticipantNotFound,
    BindingMismatch,
    MembershipNotUnbound,
    MembershipExpired,
    PreviousConnectionCannotReplaceItself,
    RecoveryClockRegression,
}

#[derive(Debug)]
pub struct Session {
    id: SessionId,
    phase: SessionPhase,
    limits: SessionLimits,
    recovery_clock: u64,
    used_participants: IncarnationRegistry<ParticipantId>,
    memberships: HashMap<ParticipantId, MembershipState>,
    bindings: HashMap<ConnectionHandle, ParticipantId>,
}

impl Session {
    pub fn new(id: SessionId, limits: SessionLimits) -> Self {
        Self {
            id,
            phase: SessionPhase::Open,
            limits,
            recovery_clock: 0,
            used_participants: IncarnationRegistry::new(limits.max_participant_incarnations),
            memberships: HashMap::new(),
            bindings: HashMap::new(),
        }
    }

    pub const fn id(&self) -> SessionId {
        self.id
    }

    pub const fn phase(&self) -> SessionPhase {
        self.phase
    }

    pub const fn recovery_clock(&self) -> u64 {
        self.recovery_clock
    }

    pub const fn limits(&self) -> SessionLimits {
        self.limits
    }

    pub fn live_memberships(&self) -> usize {
        self.memberships.len()
    }

    pub fn retained_memberships(&self) -> usize {
        self.memberships
            .values()
            .filter(|state| matches!(state, MembershipState::Unbound { .. }))
            .count()
    }

    pub fn membership_state(&self, participant: ParticipantId) -> Option<MembershipState> {
        self.memberships.get(&participant).copied()
    }

    pub fn participant_for_connection(
        &self,
        connection: ConnectionHandle,
    ) -> Option<ParticipantId> {
        self.bindings.get(&connection).copied()
    }

    pub fn is_authorized(&self, participant: ParticipantId, connection: ConnectionHandle) -> bool {
        self.bindings.get(&connection).copied() == Some(participant)
            && self.membership_state(participant) == Some(MembershipState::Bound(connection))
    }

    pub fn admit_new(
        &mut self,
        participant: ParticipantId,
        established: EstablishedNegotiation<'_>,
    ) -> Result<(), SessionError> {
        self.require_open()?;
        let connection = established.connection();

        if self.memberships.len() >= self.limits.max_memberships() {
            return Err(SessionError::MembershipLimitExceeded);
        }
        if self.bindings.contains_key(&connection) {
            return Err(SessionError::ConnectionAlreadyBound);
        }
        if self.used_participants.contains(participant) {
            return Err(SessionError::ParticipantIdAlreadyUsed);
        }

        self.used_participants
            .claim(participant)
            .map_err(map_participant_claim_error)?;
        self.bindings.insert(connection, participant);
        self.memberships
            .insert(participant, MembershipState::Bound(connection));
        Ok(())
    }

    pub fn connection_lost(
        &mut self,
        participant: ParticipantId,
        connection: ConnectionHandle,
        policy: RetentionPolicy,
    ) -> Result<ConnectionLossOutcome, SessionError> {
        let state = self
            .memberships
            .get(&participant)
            .copied()
            .ok_or(SessionError::ParticipantNotFound)?;
        if state != MembershipState::Bound(connection)
            || !self.is_authorized(participant, connection)
        {
            return Err(SessionError::BindingMismatch);
        }

        self.bindings.remove(&connection);

        match policy {
            RetentionPolicy::Terminate => {
                self.memberships.remove(&participant);
                Ok(ConnectionLossOutcome::Terminated)
            }
            RetentionPolicy::RetainForRecovery { duration } => {
                let expires_at = self.recovery_clock.saturating_add(duration.get());
                self.memberships.insert(
                    participant,
                    MembershipState::Unbound {
                        expires_at,
                        previous_connection: connection,
                    },
                );
                Ok(ConnectionLossOutcome::Retained { expires_at })
            }
        }
    }

    pub fn bind_replacement(
        &mut self,
        participant: ParticipantId,
        established: EstablishedNegotiation<'_>,
    ) -> Result<(), SessionError> {
        self.require_open()?;
        let connection = established.connection();

        if self.bindings.contains_key(&connection) {
            return Err(SessionError::ConnectionAlreadyBound);
        }

        let state = self
            .memberships
            .get(&participant)
            .copied()
            .ok_or(SessionError::ParticipantNotFound)?;
        let MembershipState::Unbound {
            expires_at,
            previous_connection,
        } = state
        else {
            return Err(SessionError::MembershipNotUnbound);
        };

        if expires_at <= self.recovery_clock {
            self.memberships.remove(&participant);
            return Err(SessionError::MembershipExpired);
        }
        if connection == previous_connection {
            return Err(SessionError::PreviousConnectionCannotReplaceItself);
        }

        self.bindings.insert(connection, participant);
        self.memberships
            .insert(participant, MembershipState::Bound(connection));
        Ok(())
    }

    pub fn remove_participant(
        &mut self,
        participant: ParticipantId,
    ) -> Result<MembershipState, SessionError> {
        self.require_open()?;
        let state = self
            .memberships
            .remove(&participant)
            .ok_or(SessionError::ParticipantNotFound)?;
        if let MembershipState::Bound(connection) = state {
            self.bindings.remove(&connection);
        }
        Ok(state)
    }

    /// Advances the host/runtime recovery clock used only for retained-membership expiry.
    ///
    /// This clock is an RN2 implementation policy and is not `SimulationTick` or wire time.
    pub fn advance_recovery_clock(
        &mut self,
        new_value: u64,
    ) -> Result<Vec<ParticipantId>, SessionError> {
        if new_value < self.recovery_clock {
            return Err(SessionError::RecoveryClockRegression);
        }
        self.recovery_clock = new_value;

        let expired: Vec<_> = self
            .memberships
            .iter()
            .filter_map(|(participant, state)| match state {
                MembershipState::Unbound { expires_at, .. } if *expires_at <= new_value => {
                    Some(*participant)
                }
                _ => None,
            })
            .collect();

        for participant in &expired {
            self.memberships.remove(participant);
        }
        Ok(expired)
    }

    pub fn close(&mut self) {
        self.phase = SessionPhase::Closed;
        self.bindings.clear();
        self.memberships.clear();
    }

    fn require_open(&self) -> Result<(), SessionError> {
        if self.phase == SessionPhase::Open {
            Ok(())
        } else {
            Err(SessionError::Closed)
        }
    }
}

fn map_participant_claim_error(error: IncarnationClaimError) -> SessionError {
    match error {
        IncarnationClaimError::AlreadyUsed => SessionError::ParticipantIdAlreadyUsed,
        IncarnationClaimError::CapacityExceeded => {
            SessionError::ParticipantIncarnationLimitExceeded
        }
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

    fn limits() -> SessionLimits {
        SessionLimits::new(
            NonZeroUsize::new(16).unwrap(),
            NonZeroUsize::new(8).unwrap(),
        )
        .unwrap()
    }

    fn one_membership_limits() -> SessionLimits {
        SessionLimits::new(
            NonZeroUsize::new(16).unwrap(),
            NonZeroUsize::new(1).unwrap(),
        )
        .unwrap()
    }

    fn protocol() -> ProtocolContract {
        ProtocolContract::new(ProtocolId::new(1), ProtocolRevision::new(1))
    }

    fn manager() -> NegotiationManager {
        NegotiationManager::new(OfferLimits::default(), NegotiationManagerLimits::default())
            .unwrap()
    }

    fn establish(manager: &mut NegotiationManager, connection: ConnectionHandle) {
        let offer = CompatibilityOffer::new(vec![protocol()], vec![], vec![], None);
        manager.start(connection, offer.clone(), offer).unwrap();
        let contract = NegotiatedContract::new(protocol());
        manager
            .propose(
                connection,
                contract.clone(),
                &NegotiationRequirements::default(),
            )
            .unwrap();
        assert_ne!(
            manager.validate_authority(connection, &contract).unwrap(),
            NegotiationStatus::Established
        );
        assert_eq!(
            manager.validate_peer(connection, &contract).unwrap(),
            NegotiationStatus::Established
        );
    }

    #[test]
    fn participant_identity_cannot_be_reused_after_membership_ends() {
        let participant = ParticipantId::new(5);
        let connection = ConnectionHandle::new(1);
        let second_connection = ConnectionHandle::new(2);
        let mut negotiation = manager();
        establish(&mut negotiation, connection);
        establish(&mut negotiation, second_connection);
        let mut session = Session::new(SessionId::new(10), limits());

        session
            .admit_new(participant, negotiation.established(connection).unwrap())
            .unwrap();
        assert_eq!(
            session
                .connection_lost(participant, connection, RetentionPolicy::Terminate)
                .unwrap(),
            ConnectionLossOutcome::Terminated
        );
        assert_eq!(
            session.admit_new(
                participant,
                negotiation.established(second_connection).unwrap()
            ),
            Err(SessionError::ParticipantIdAlreadyUsed)
        );
    }

    #[test]
    fn connection_loss_removes_authorization_and_retention_expires() {
        let participant = ParticipantId::new(5);
        let connection = ConnectionHandle::new(1);
        let mut negotiation = manager();
        establish(&mut negotiation, connection);
        let mut session = Session::new(SessionId::new(10), limits());
        session
            .admit_new(participant, negotiation.established(connection).unwrap())
            .unwrap();
        assert!(session.is_authorized(participant, connection));

        let outcome = session
            .connection_lost(
                participant,
                connection,
                RetentionPolicy::RetainForRecovery {
                    duration: NonZeroU64::new(5).unwrap(),
                },
            )
            .unwrap();
        assert_eq!(outcome, ConnectionLossOutcome::Retained { expires_at: 5 });
        assert!(!session.is_authorized(participant, connection));
        assert_eq!(
            session.membership_state(participant),
            Some(MembershipState::Unbound {
                expires_at: 5,
                previous_connection: connection
            })
        );

        assert!(session.advance_recovery_clock(4).unwrap().is_empty());
        assert_eq!(
            session.advance_recovery_clock(5).unwrap(),
            vec![participant]
        );
        assert_eq!(session.membership_state(participant), None);
    }

    #[test]
    fn replacement_requires_unbound_membership_and_new_negotiation() {
        let participant = ParticipantId::new(5);
        let first_connection = ConnectionHandle::new(1);
        let replacement = ConnectionHandle::new(2);
        let mut negotiation = manager();
        establish(&mut negotiation, first_connection);
        establish(&mut negotiation, replacement);
        let mut session = Session::new(SessionId::new(10), limits());
        session
            .admit_new(
                participant,
                negotiation.established(first_connection).unwrap(),
            )
            .unwrap();

        assert_eq!(
            session.bind_replacement(
                participant,
                negotiation.established(replacement).unwrap()
            ),
            Err(SessionError::MembershipNotUnbound)
        );

        session
            .connection_lost(
                participant,
                first_connection,
                RetentionPolicy::RetainForRecovery {
                    duration: NonZeroU64::new(5).unwrap(),
                },
            )
            .unwrap();
        session
            .bind_replacement(
                participant,
                negotiation.established(replacement).unwrap(),
            )
            .unwrap();

        assert!(session.is_authorized(participant, replacement));
        assert!(!session.is_authorized(participant, first_connection));
    }

    #[test]
    fn previous_lost_connection_cannot_replace_itself() {
        let participant = ParticipantId::new(5);
        let connection = ConnectionHandle::new(1);
        let mut negotiation = manager();
        establish(&mut negotiation, connection);
        let mut session = Session::new(SessionId::new(10), limits());
        session
            .admit_new(participant, negotiation.established(connection).unwrap())
            .unwrap();
        session
            .connection_lost(
                participant,
                connection,
                RetentionPolicy::RetainForRecovery {
                    duration: NonZeroU64::new(5).unwrap(),
                },
            )
            .unwrap();

        assert_eq!(
            session.bind_replacement(participant, negotiation.established(connection).unwrap()),
            Err(SessionError::PreviousConnectionCannotReplaceItself)
        );
    }

    #[test]
    fn rejected_admission_preserves_established_connection_contract() {
        let first_connection = ConnectionHandle::new(1);
        let second_connection = ConnectionHandle::new(2);
        let mut negotiation = manager();
        establish(&mut negotiation, first_connection);
        establish(&mut negotiation, second_connection);
        let mut session = Session::new(SessionId::new(10), one_membership_limits());
        session
            .admit_new(
                ParticipantId::new(1),
                negotiation.established(first_connection).unwrap(),
            )
            .unwrap();

        assert_eq!(
            session.admit_new(
                ParticipantId::new(2),
                negotiation.established(second_connection).unwrap(),
            ),
            Err(SessionError::MembershipLimitExceeded)
        );
        assert_eq!(
            negotiation.status(second_connection).unwrap(),
            NegotiationStatus::Established
        );
        assert_eq!(
            negotiation.established(second_connection).unwrap().contract().protocol(),
            protocol()
        );

        session.remove_participant(ParticipantId::new(1)).unwrap();
        session
            .admit_new(
                ParticipantId::new(2),
                negotiation.established(second_connection).unwrap(),
            )
            .unwrap();
    }

    #[test]
    fn explicit_authority_removal_ends_bound_membership_and_authorization() {
        let participant = ParticipantId::new(5);
        let connection = ConnectionHandle::new(1);
        let mut negotiation = manager();
        establish(&mut negotiation, connection);
        let mut session = Session::new(SessionId::new(10), limits());
        session
            .admit_new(participant, negotiation.established(connection).unwrap())
            .unwrap();

        assert_eq!(
            session.remove_participant(participant).unwrap(),
            MembershipState::Bound(connection)
        );
        assert!(!session.is_authorized(participant, connection));
        assert_eq!(session.membership_state(participant), None);
    }

    #[test]
    fn explicit_authority_removal_ends_unbound_membership() {
        let participant = ParticipantId::new(5);
        let connection = ConnectionHandle::new(1);
        let mut negotiation = manager();
        establish(&mut negotiation, connection);
        let mut session = Session::new(SessionId::new(10), limits());
        session
            .admit_new(participant, negotiation.established(connection).unwrap())
            .unwrap();
        session
            .connection_lost(
                participant,
                connection,
                RetentionPolicy::RetainForRecovery {
                    duration: NonZeroU64::new(5).unwrap(),
                },
            )
            .unwrap();

        assert_eq!(
            session.remove_participant(participant).unwrap(),
            MembershipState::Unbound {
                expires_at: 5,
                previous_connection: connection
            }
        );
        assert_eq!(session.membership_state(participant), None);
    }

    #[test]
    fn same_live_connection_can_admit_new_participant_after_explicit_removal() {
        let connection = ConnectionHandle::new(1);
        let mut negotiation = manager();
        establish(&mut negotiation, connection);
        let mut session = Session::new(SessionId::new(10), limits());
        session
            .admit_new(
                ParticipantId::new(1),
                negotiation.established(connection).unwrap(),
            )
            .unwrap();
        session.remove_participant(ParticipantId::new(1)).unwrap();
        session
            .admit_new(
                ParticipantId::new(2),
                negotiation.established(connection).unwrap(),
            )
            .unwrap();
        assert!(session.is_authorized(ParticipantId::new(2), connection));
    }

    #[test]
    fn expired_membership_cannot_be_rebound() {
        let participant = ParticipantId::new(5);
        let first_connection = ConnectionHandle::new(1);
        let replacement = ConnectionHandle::new(2);
        let mut negotiation = manager();
        establish(&mut negotiation, first_connection);
        establish(&mut negotiation, replacement);
        let mut session = Session::new(SessionId::new(10), limits());
        session
            .admit_new(
                participant,
                negotiation.established(first_connection).unwrap(),
            )
            .unwrap();
        session
            .connection_lost(
                participant,
                first_connection,
                RetentionPolicy::RetainForRecovery {
                    duration: NonZeroU64::new(1).unwrap(),
                },
            )
            .unwrap();
        session.advance_recovery_clock(1).unwrap();

        assert_eq!(
            session.bind_replacement(
                participant,
                negotiation.established(replacement).unwrap(),
            ),
            Err(SessionError::ParticipantNotFound)
        );
    }

    #[test]
    fn session_close_is_terminal_for_admission_and_bindings() {
        let participant = ParticipantId::new(5);
        let connection = ConnectionHandle::new(1);
        let second_connection = ConnectionHandle::new(2);
        let mut negotiation = manager();
        establish(&mut negotiation, connection);
        establish(&mut negotiation, second_connection);
        let mut session = Session::new(SessionId::new(10), limits());
        session
            .admit_new(participant, negotiation.established(connection).unwrap())
            .unwrap();
        session.close();

        assert_eq!(session.phase(), SessionPhase::Closed);
        assert!(!session.is_authorized(participant, connection));
        assert_eq!(
            session.admit_new(
                ParticipantId::new(6),
                negotiation.established(second_connection).unwrap(),
            ),
            Err(SessionError::Closed)
        );
    }

    #[test]
    fn recovery_clock_cannot_regress() {
        let mut session = Session::new(SessionId::new(10), limits());
        session.advance_recovery_clock(10).unwrap();
        assert_eq!(
            session.advance_recovery_clock(9),
            Err(SessionError::RecoveryClockRegression)
        );
    }
}
