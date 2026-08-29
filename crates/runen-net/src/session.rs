use std::collections::HashMap;
use std::num::{NonZeroU64, NonZeroUsize};

use crate::identity::ConnectionHandle;
use crate::identity::{IncarnationClaimError, IncarnationRegistry, ParticipantId, SessionId};
use crate::protocol::EstablishedNegotiation;

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

/// One absolute position on a host-supplied session recovery clock.
///
/// The host chooses the units and origin consistently for one [`Session`]. RunenNet uses this
/// value only to order retained-membership expiry. It is not wall-clock time, `SimulationTick`,
/// transport time, retry scheduling, or a wire timestamp.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RecoveryTime(u64);

impl RecoveryTime {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }

    fn checked_add(self, duration: RecoveryDuration) -> Option<Self> {
        self.0.checked_add(duration.get()).map(Self)
    }
}

/// One positive finite retention span on the host-supplied session recovery clock.
///
/// This span uses the same host-selected units as [`RecoveryTime`]. Its representation is a Rust
/// type-safety boundary for session retention, not a standardized duration unit or wall-clock API.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct RecoveryDuration(NonZeroU64);

impl RecoveryDuration {
    pub const fn new(value: NonZeroU64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0.get()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum MembershipState {
    Bound(ConnectionHandle),
    Unbound { expires_at: RecoveryTime },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RetentionPolicy {
    Terminate,
    RetainForRecovery { duration: RecoveryDuration },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ConnectionLossOutcome {
    Terminated,
    Retained { expires_at: RecoveryTime },
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
    RecoveryExpiryOverflow,
}

#[derive(Debug, Copy, Clone)]
struct MembershipRecord {
    state: MembershipState,
    previous_connection: Option<ConnectionHandle>,
}

#[derive(Debug)]
pub struct Session {
    id: SessionId,
    phase: SessionPhase,
    limits: SessionLimits,
    recovery_clock: RecoveryTime,
    used_participants: IncarnationRegistry<ParticipantId>,
    memberships: HashMap<ParticipantId, MembershipRecord>,
    bindings: HashMap<ConnectionHandle, ParticipantId>,
}

impl Session {
    pub fn new(id: SessionId, limits: SessionLimits) -> Self {
        Self {
            id,
            phase: SessionPhase::Open,
            limits,
            recovery_clock: RecoveryTime::new(0),
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

    pub const fn recovery_clock(&self) -> RecoveryTime {
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
            .filter(|record| matches!(record.state, MembershipState::Unbound { .. }))
            .count()
    }

    pub fn membership_state(&self, participant: ParticipantId) -> Option<MembershipState> {
        self.memberships
            .get(&participant)
            .map(|record| record.state)
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
        self.memberships.insert(
            participant,
            MembershipRecord {
                state: MembershipState::Bound(connection),
                previous_connection: None,
            },
        );
        Ok(())
    }

    pub fn connection_lost(
        &mut self,
        participant: ParticipantId,
        connection: ConnectionHandle,
        policy: RetentionPolicy,
    ) -> Result<ConnectionLossOutcome, SessionError> {
        self.require_open()?;
        let state = self
            .memberships
            .get(&participant)
            .map(|record| record.state)
            .ok_or(SessionError::ParticipantNotFound)?;
        if state != MembershipState::Bound(connection)
            || !self.is_authorized(participant, connection)
        {
            return Err(SessionError::BindingMismatch);
        }

        match policy {
            RetentionPolicy::Terminate => {
                self.bindings.remove(&connection);
                self.memberships.remove(&participant);
                Ok(ConnectionLossOutcome::Terminated)
            }
            RetentionPolicy::RetainForRecovery { duration } => {
                let expires_at = self
                    .recovery_clock
                    .checked_add(duration)
                    .ok_or(SessionError::RecoveryExpiryOverflow)?;
                self.bindings.remove(&connection);
                self.memberships.insert(
                    participant,
                    MembershipRecord {
                        state: MembershipState::Unbound { expires_at },
                        previous_connection: Some(connection),
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

        let record = self
            .memberships
            .get(&participant)
            .copied()
            .ok_or(SessionError::ParticipantNotFound)?;
        let MembershipState::Unbound { expires_at } = record.state else {
            return Err(SessionError::MembershipNotUnbound);
        };

        if expires_at <= self.recovery_clock {
            self.memberships.remove(&participant);
            return Err(SessionError::MembershipExpired);
        }
        if record.previous_connection == Some(connection) {
            return Err(SessionError::PreviousConnectionCannotReplaceItself);
        }

        self.bindings.insert(connection, participant);
        self.memberships.insert(
            participant,
            MembershipRecord {
                state: MembershipState::Bound(connection),
                previous_connection: None,
            },
        );
        Ok(())
    }

    pub fn remove_participant(
        &mut self,
        participant: ParticipantId,
    ) -> Result<MembershipState, SessionError> {
        self.require_open()?;
        let record = self
            .memberships
            .remove(&participant)
            .ok_or(SessionError::ParticipantNotFound)?;
        if let MembershipState::Bound(connection) = record.state {
            self.bindings.remove(&connection);
        }
        Ok(record.state)
    }

    /// Advances the host/runtime recovery clock used only for retained-membership expiry.
    ///
    /// Units and origin are host-selected and must be used consistently for this [`Session`].
    /// This clock is an RN2 implementation policy and is not `SimulationTick`, transport time,
    /// retry scheduling, wall-clock time, or wire time.
    pub fn advance_recovery_clock(
        &mut self,
        new_value: RecoveryTime,
    ) -> Result<Vec<ParticipantId>, SessionError> {
        self.require_open()?;
        if new_value < self.recovery_clock {
            return Err(SessionError::RecoveryClockRegression);
        }
        self.recovery_clock = new_value;

        let mut expired: Vec<_> = self
            .memberships
            .iter()
            .filter_map(|(participant, record)| match record.state {
                MembershipState::Unbound { expires_at } if expires_at <= new_value => {
                    Some(*participant)
                }
                _ => None,
            })
            .collect();
        expired.sort_by_key(|participant| participant.get());

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

    fn recovery_time(value: u64) -> RecoveryTime {
        RecoveryTime::new(value)
    }

    fn recovery_duration(value: u64) -> RecoveryDuration {
        RecoveryDuration::new(NonZeroU64::new(value).unwrap())
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
        manager
            .propose(
                connection,
                NegotiatedContract::new(protocol()),
                &NegotiationRequirements::default(),
            )
            .unwrap();
        assert_ne!(
            manager.validate_authority(connection).unwrap(),
            NegotiationStatus::Established
        );
        assert_eq!(
            manager.validate_peer(connection).unwrap(),
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
        assert_eq!(session.recovery_clock(), recovery_time(0));
        session
            .admit_new(participant, negotiation.established(connection).unwrap())
            .unwrap();
        assert!(session.is_authorized(participant, connection));

        let outcome = session
            .connection_lost(
                participant,
                connection,
                RetentionPolicy::RetainForRecovery {
                    duration: recovery_duration(5),
                },
            )
            .unwrap();
        assert_eq!(
            outcome,
            ConnectionLossOutcome::Retained {
                expires_at: recovery_time(5),
            }
        );
        assert!(!session.is_authorized(participant, connection));
        assert_eq!(
            session.membership_state(participant),
            Some(MembershipState::Unbound {
                expires_at: recovery_time(5),
            })
        );

        assert!(
            session
                .advance_recovery_clock(recovery_time(4))
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            session.advance_recovery_clock(recovery_time(5)).unwrap(),
            vec![participant]
        );
        assert_eq!(session.membership_state(participant), None);
    }

    #[test]
    fn connection_loss_rejects_recovery_expiry_overflow_without_mutating_binding() {
        let participant = ParticipantId::new(5);
        let connection = ConnectionHandle::new(1);
        let mut negotiation = manager();
        establish(&mut negotiation, connection);
        let mut session = Session::new(SessionId::new(10), limits());
        session.advance_recovery_clock(recovery_time(u64::MAX)).unwrap();
        session
            .admit_new(participant, negotiation.established(connection).unwrap())
            .unwrap();

        assert_eq!(
            session.connection_lost(
                participant,
                connection,
                RetentionPolicy::RetainForRecovery {
                    duration: recovery_duration(1),
                },
            ),
            Err(SessionError::RecoveryExpiryOverflow)
        );
        assert!(session.is_authorized(participant, connection));
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
            session.bind_replacement(participant, negotiation.established(replacement).unwrap()),
            Err(SessionError::MembershipNotUnbound)
        );

        session
            .connection_lost(
                participant,
                first_connection,
                RetentionPolicy::RetainForRecovery {
                    duration: recovery_duration(5),
                },
            )
            .unwrap();
        session
            .bind_replacement(participant, negotiation.established(replacement).unwrap())
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
                    duration: recovery_duration(5),
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
            negotiation
                .established(second_connection)
                .unwrap()
                .contract()
                .protocol(),
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
                    duration: recovery_duration(5),
                },
            )
            .unwrap();

        assert_eq!(
            session.remove_participant(participant).unwrap(),
            MembershipState::Unbound {
                expires_at: recovery_time(5),
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
                    duration: recovery_duration(1),
                },
            )
            .unwrap();
        session.advance_recovery_clock(recovery_time(1)).unwrap();

        assert_eq!(
            session.bind_replacement(participant, negotiation.established(replacement).unwrap()),
            Err(SessionError::ParticipantNotFound)
        );
    }

    #[test]
    fn expiry_reporting_is_deterministic_by_participant_identity() {
        let first = ParticipantId::new(9);
        let second = ParticipantId::new(3);
        let first_connection = ConnectionHandle::new(1);
        let second_connection = ConnectionHandle::new(2);
        let mut negotiation = manager();
        establish(&mut negotiation, first_connection);
        establish(&mut negotiation, second_connection);
        let mut session = Session::new(SessionId::new(10), limits());
        session
            .admit_new(first, negotiation.established(first_connection).unwrap())
            .unwrap();
        session
            .admit_new(second, negotiation.established(second_connection).unwrap())
            .unwrap();
        let policy = RetentionPolicy::RetainForRecovery {
            duration: recovery_duration(1),
        };
        session
            .connection_lost(first, first_connection, policy)
            .unwrap();
        session
            .connection_lost(second, second_connection, policy)
            .unwrap();

        assert_eq!(
            session.advance_recovery_clock(recovery_time(1)).unwrap(),
            vec![second, first]
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
        assert_eq!(
            session.connection_lost(participant, connection, RetentionPolicy::Terminate),
            Err(SessionError::Closed)
        );
        assert_eq!(
            session.advance_recovery_clock(recovery_time(1)),
            Err(SessionError::Closed)
        );
    }

    #[test]
    fn recovery_clock_cannot_regress() {
        let mut session = Session::new(SessionId::new(10), limits());
        session.advance_recovery_clock(recovery_time(10)).unwrap();
        assert_eq!(
            session.advance_recovery_clock(recovery_time(9)),
            Err(SessionError::RecoveryClockRegression)
        );
    }
}
