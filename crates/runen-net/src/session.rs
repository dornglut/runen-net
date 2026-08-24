use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};

use crate::identity::{
    ConnectionHandle, IncarnationClaimError, IncarnationRegistry, ParticipantId, SessionId,
};
use crate::protocol::{EstablishedNegotiation, NegotiatedContract};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SessionPhase {
    Open,
    Closed,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct SessionLimits {
    max_connection_incarnations: NonZeroUsize,
    max_participant_incarnations: NonZeroUsize,
    max_active_memberships: NonZeroUsize,
    max_retained_unbound: NonZeroUsize,
}

impl SessionLimits {
    pub fn new(
        max_connection_incarnations: NonZeroUsize,
        max_participant_incarnations: NonZeroUsize,
        max_active_memberships: NonZeroUsize,
        max_retained_unbound: NonZeroUsize,
    ) -> Result<Self, SessionLimitError> {
        if max_active_memberships.get() > max_participant_incarnations.get() {
            return Err(SessionLimitError::ActiveExceedsParticipantIncarnations);
        }
        if max_retained_unbound.get() < max_active_memberships.get() {
            return Err(SessionLimitError::RetentionBelowActiveMemberships);
        }

        Ok(Self {
            max_connection_incarnations,
            max_participant_incarnations,
            max_active_memberships,
            max_retained_unbound,
        })
    }

    pub const fn max_connection_incarnations(self) -> usize {
        self.max_connection_incarnations.get()
    }

    pub const fn max_participant_incarnations(self) -> usize {
        self.max_participant_incarnations.get()
    }

    pub const fn max_active_memberships(self) -> usize {
        self.max_active_memberships.get()
    }

    pub const fn max_retained_unbound(self) -> usize {
        self.max_retained_unbound.get()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SessionLimitError {
    ActiveExceedsParticipantIncarnations,
    RetentionBelowActiveMemberships,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum MembershipState {
    Bound(ConnectionHandle),
    Unbound { expires_at: u64 },
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
    ActiveMembershipLimitExceeded,
    ConnectionHandleAlreadyUsed,
    ConnectionIncarnationLimitExceeded,
    ConnectionAlreadyBound,
    ParticipantNotFound,
    BindingMismatch,
    MembershipNotUnbound,
    MembershipExpired,
    RecoveryClockRegression,
}

#[derive(Debug)]
struct ConnectionBinding {
    participant: ParticipantId,
    contract: NegotiatedContract,
}

#[derive(Debug)]
pub struct Session {
    id: SessionId,
    phase: SessionPhase,
    limits: SessionLimits,
    recovery_clock: u64,
    used_connections: IncarnationRegistry<ConnectionHandle>,
    used_participants: IncarnationRegistry<ParticipantId>,
    memberships: HashMap<ParticipantId, MembershipState>,
    bindings: HashMap<ConnectionHandle, ConnectionBinding>,
}

impl Session {
    pub fn new(id: SessionId, limits: SessionLimits) -> Self {
        Self {
            id,
            phase: SessionPhase::Open,
            limits,
            recovery_clock: 0,
            used_connections: IncarnationRegistry::new(limits.max_connection_incarnations),
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

    pub fn active_memberships(&self) -> usize {
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

    pub fn negotiated_contract(&self, connection: ConnectionHandle) -> Option<&NegotiatedContract> {
        self.bindings.get(&connection).map(|binding| &binding.contract)
    }

    pub fn participant_for_connection(
        &self,
        connection: ConnectionHandle,
    ) -> Option<ParticipantId> {
        self.bindings.get(&connection).map(|binding| binding.participant)
    }

    pub fn is_authorized(
        &self,
        participant: ParticipantId,
        connection: ConnectionHandle,
    ) -> bool {
        self.bindings
            .get(&connection)
            .is_some_and(|binding| binding.participant == participant)
            && self.membership_state(participant) == Some(MembershipState::Bound(connection))
    }

    pub fn admit_new(
        &mut self,
        participant: ParticipantId,
        established: EstablishedNegotiation,
    ) -> Result<(), SessionError> {
        self.require_open()?;
        let connection = established.connection();

        if self.memberships.len() >= self.limits.max_active_memberships() {
            return Err(SessionError::ActiveMembershipLimitExceeded);
        }
        if self.bindings.contains_key(&connection) {
            return Err(SessionError::ConnectionAlreadyBound);
        }
        if self.used_connections.contains(connection) {
            return Err(SessionError::ConnectionHandleAlreadyUsed);
        }
        if self.used_participants.contains(participant) {
            return Err(SessionError::ParticipantIdAlreadyUsed);
        }

        self.used_connections
            .claim(connection)
            .map_err(map_connection_claim_error)?;
        self.used_participants
            .claim(participant)
            .map_err(map_participant_claim_error)?;

        let (connection, contract) = established.into_parts();
        self.bindings.insert(
            connection,
            ConnectionBinding {
                participant,
                contract,
            },
        );
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
        if state != MembershipState::Bound(connection) || !self.is_authorized(participant, connection)
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
                debug_assert!(self.retained_memberships() < self.limits.max_retained_unbound());
                let expires_at = self.recovery_clock.saturating_add(duration.get());
                self.memberships
                    .insert(participant, MembershipState::Unbound { expires_at });
                Ok(ConnectionLossOutcome::Retained { expires_at })
            }
        }
    }

    pub fn bind_replacement(
        &mut self,
        participant: ParticipantId,
        established: EstablishedNegotiation,
    ) -> Result<(), SessionError> {
        self.require_open()?;
        let connection = established.connection();

        if self.bindings.contains_key(&connection) {
            return Err(SessionError::ConnectionAlreadyBound);
        }
        if self.used_connections.contains(connection) {
            return Err(SessionError::ConnectionHandleAlreadyUsed);
        }

        let state = self
            .memberships
            .get(&participant)
            .copied()
            .ok_or(SessionError::ParticipantNotFound)?;
        let MembershipState::Unbound { expires_at } = state else {
            return Err(SessionError::MembershipNotUnbound);
        };

        if expires_at <= self.recovery_clock {
            self.memberships.remove(&participant);
            return Err(SessionError::MembershipExpired);
        }

        self.used_connections
            .claim(connection)
            .map_err(map_connection_claim_error)?;
        let (connection, contract) = established.into_parts();
        self.bindings.insert(
            connection,
            ConnectionBinding {
                participant,
                contract,
            },
        );
        self.memberships
            .insert(participant, MembershipState::Bound(connection));
        Ok(())
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
                MembershipState::Unbound { expires_at } if *expires_at <= new_value => {
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

fn map_connection_claim_error(error: IncarnationClaimError) -> SessionError {
    match error {
        IncarnationClaimError::AlreadyUsed => SessionError::ConnectionHandleAlreadyUsed,
        IncarnationClaimError::CapacityExceeded => SessionError::ConnectionIncarnationLimitExceeded,
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
        NegotiationRequirements, OfferLimits, ProtocolContract, ProtocolId, ProtocolRevision,
    };

    fn limits() -> SessionLimits {
        SessionLimits::new(
            NonZeroUsize::new(16).unwrap(),
            NonZeroUsize::new(16).unwrap(),
            NonZeroUsize::new(8).unwrap(),
            NonZeroUsize::new(8).unwrap(),
        )
        .unwrap()
    }

    fn protocol() -> ProtocolContract {
        ProtocolContract::new(ProtocolId::new(1), ProtocolRevision::new(1))
    }

    fn established(connection: ConnectionHandle) -> EstablishedNegotiation {
        let mut manager = NegotiationManager::new(
            OfferLimits::default(),
            NegotiationManagerLimits::default(),
        )
        .unwrap();
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
        manager.validate_authority(connection, &contract).unwrap();
        manager.validate_peer(connection, &contract).unwrap();
        manager.take_established(connection).unwrap()
    }

    #[test]
    fn participant_identity_cannot_be_reused_after_membership_ends() {
        let participant = ParticipantId::new(5);
        let first_connection = ConnectionHandle::new(1);
        let mut session = Session::new(SessionId::new(10), limits());
        session
            .admit_new(participant, established(first_connection))
            .unwrap();
        assert_eq!(
            session
                .connection_lost(participant, first_connection, RetentionPolicy::Terminate)
                .unwrap(),
            ConnectionLossOutcome::Terminated
        );
        assert_eq!(
            session.admit_new(participant, established(ConnectionHandle::new(2))),
            Err(SessionError::ParticipantIdAlreadyUsed)
        );
    }

    #[test]
    fn connection_loss_removes_authorization_and_retention_expires() {
        let participant = ParticipantId::new(5);
        let connection = ConnectionHandle::new(1);
        let mut session = Session::new(SessionId::new(10), limits());
        session
            .admit_new(participant, established(connection))
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
            Some(MembershipState::Unbound { expires_at: 5 })
        );

        assert!(session.advance_recovery_clock(4).unwrap().is_empty());
        assert_eq!(session.advance_recovery_clock(5).unwrap(), vec![participant]);
        assert_eq!(session.membership_state(participant), None);
    }

    #[test]
    fn replacement_requires_unbound_membership_and_new_negotiation() {
        let participant = ParticipantId::new(5);
        let first_connection = ConnectionHandle::new(1);
        let replacement = ConnectionHandle::new(2);
        let mut session = Session::new(SessionId::new(10), limits());
        session
            .admit_new(participant, established(first_connection))
            .unwrap();

        assert_eq!(
            session.bind_replacement(participant, established(replacement)),
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
            .bind_replacement(participant, established(replacement))
            .unwrap();

        assert!(session.is_authorized(participant, replacement));
        assert!(!session.is_authorized(participant, first_connection));
        assert_eq!(session.negotiated_contract(replacement).unwrap().protocol(), protocol());
    }

    #[test]
    fn expired_membership_cannot_be_rebound() {
        let participant = ParticipantId::new(5);
        let first_connection = ConnectionHandle::new(1);
        let mut session = Session::new(SessionId::new(10), limits());
        session
            .admit_new(participant, established(first_connection))
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
            session.bind_replacement(participant, established(ConnectionHandle::new(2))),
            Err(SessionError::ParticipantNotFound)
        );
    }

    #[test]
    fn session_close_is_terminal_for_admission_and_bindings() {
        let participant = ParticipantId::new(5);
        let connection = ConnectionHandle::new(1);
        let mut session = Session::new(SessionId::new(10), limits());
        session
            .admit_new(participant, established(connection))
            .unwrap();
        session.close();

        assert_eq!(session.phase(), SessionPhase::Closed);
        assert!(!session.is_authorized(participant, connection));
        assert_eq!(
            session.admit_new(
                ParticipantId::new(6),
                established(ConnectionHandle::new(2))
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
