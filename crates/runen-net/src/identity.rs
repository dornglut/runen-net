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
pub enum IncarnationClaimError {
    AlreadyUsed,
    CapacityExceeded,
}

/// Bounded evidence that an incarnation identity has already been used.
///
/// Entries are intentionally never released during the registry lifetime. This
/// allows callers to enforce non-reuse without process-global allocation state.
#[derive(Debug, Clone)]
pub struct IncarnationRegistry<I> {
    max_claims: NonZeroUsize,
    used: HashSet<I>,
}

impl<I> IncarnationRegistry<I>
where
    I: Copy + Eq + Hash,
{
    pub fn new(max_claims: NonZeroUsize) -> Self {
        Self {
            max_claims,
            used: HashSet::new(),
        }
    }

    pub fn claim(&mut self, id: I) -> Result<(), IncarnationClaimError> {
        if self.used.contains(&id) {
            return Err(IncarnationClaimError::AlreadyUsed);
        }
        if self.used.len() >= self.max_claims.get() {
            return Err(IncarnationClaimError::CapacityExceeded);
        }
        self.used.insert(id);
        Ok(())
    }

    pub fn contains(&self, id: I) -> bool {
        self.used.contains(&id)
    }

    pub fn len(&self) -> usize {
        self.used.len()
    }

    pub fn is_empty(&self) -> bool {
        self.used.is_empty()
    }

    pub const fn capacity(&self) -> usize {
        self.max_claims.get()
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
}
