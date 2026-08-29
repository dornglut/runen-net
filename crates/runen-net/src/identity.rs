use std::collections::HashSet;
use std::hash::Hash;
use std::num::NonZeroUsize;

macro_rules! opaque_u128_id {
    ($name:ident) => {
        #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
        pub struct $name(u128);

        impl $name {
            pub const fn new(value: u128) -> Self {
                Self(value)
            }

            pub const fn get(self) -> u128 {
                self.0
            }
        }
    };
}

opaque_u128_id!(SessionId);
opaque_u128_id!(ParticipantId);
opaque_u128_id!(NetworkEntityId);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SimulationTick(u64);

impl SimulationTick {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Local implementation handle for one transport-connection lifetime.
///
/// This is deliberately not RunenNet protocol identity and has no wire meaning.
/// The transport/runtime integration must keep handles distinct while any
/// RunenNet state may still refer to different transport-connection lifetimes;
/// RunenNet does not provide a process-global connection-handle allocator.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct ConnectionHandle(u64);

impl ConnectionHandle {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum IncarnationClaimError {
    AlreadyUsed,
    CapacityExceeded,
}

/// Bounded implementation evidence that an incarnation identity has already been used.
///
/// Entries are intentionally never released during the registry lifetime. Public lifecycle APIs
/// expose semantic non-reuse outcomes; this generic storage mechanism is crate-private.
#[derive(Debug, Clone)]
pub(crate) struct IncarnationRegistry<I> {
    max_claims: NonZeroUsize,
    used: HashSet<I>,
}

impl<I> IncarnationRegistry<I>
where
    I: Copy + Eq + Hash,
{
    pub(crate) fn new(max_claims: NonZeroUsize) -> Self {
        Self {
            max_claims,
            used: HashSet::new(),
        }
    }

    pub(crate) fn claim(&mut self, id: I) -> Result<(), IncarnationClaimError> {
        if self.used.contains(&id) {
            return Err(IncarnationClaimError::AlreadyUsed);
        }
        if self.used.len() >= self.max_claims.get() {
            return Err(IncarnationClaimError::CapacityExceeded);
        }
        self.used.insert(id);
        Ok(())
    }

    pub(crate) fn contains(&self, id: I) -> bool {
        self.used.contains(&id)
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.used.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn incarnation_registry_rejects_reuse_and_capacity_overflow() {
        let mut registry = IncarnationRegistry::new(NonZeroUsize::new(2).unwrap());
        let first = NetworkEntityId::new(11);
        let second = NetworkEntityId::new(12);
        let third = NetworkEntityId::new(13);

        assert_eq!(registry.claim(first), Ok(()));
        assert_eq!(
            registry.claim(first),
            Err(IncarnationClaimError::AlreadyUsed)
        );
        assert_eq!(registry.claim(second), Ok(()));
        assert_eq!(
            registry.claim(third),
            Err(IncarnationClaimError::CapacityExceeded)
        );
        assert!(registry.contains(first));
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn session_identity_non_reuse_can_be_owned_without_global_state() {
        let mut registry = IncarnationRegistry::new(NonZeroUsize::new(2).unwrap());
        let session = SessionId::new(17);
        assert_eq!(registry.claim(session), Ok(()));
        assert_eq!(
            registry.claim(session),
            Err(IncarnationClaimError::AlreadyUsed)
        );
    }
}
