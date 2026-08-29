use std::{error::Error, fmt};

use crate::{
    input::{
        AuthorityInputError, AuthorityInputLimitError, InputWindowError, PredictionActivationError,
        PredictionReconciliationError,
    },
    protocol::{
        NegotiationError, NegotiationManagerConfigError, NegotiationManagerError,
        OfferValidationError, SchemaBindingError,
    },
    replication::{
        AuthorityOperationError, AuthorityPrepareError, AuthoritySessionError, ClientApplyError,
        ClientSetError, DeltaReconstructionError, ReplicationLimitError,
    },
    session::{SessionError, SessionLimitError},
};

macro_rules! debug_error {
    ($type:ty, $label:literal) => {
        impl fmt::Display for $type {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(
                    formatter,
                    concat!("RunenNet ", $label, " failure: {:?}"),
                    self
                )
            }
        }

        impl Error for $type {}
    };
}

debug_error!(SessionLimitError, "session-limit");
debug_error!(SessionError, "session");
debug_error!(OfferValidationError, "offer-validation");
debug_error!(SchemaBindingError, "schema-binding");
debug_error!(
    NegotiationManagerConfigError,
    "negotiation-manager configuration"
);
debug_error!(ReplicationLimitError, "replication-limit");
debug_error!(AuthorityPrepareError, "authority replication preparation");
debug_error!(AuthorityOperationError, "authority replication operation");
debug_error!(DeltaReconstructionError, "delta-reconstruction");
debug_error!(ClientSetError, "client replication-set");
debug_error!(InputWindowError, "input-window");
debug_error!(AuthorityInputLimitError, "authority-input limit");
debug_error!(AuthorityInputError, "authority-input");
debug_error!(PredictionActivationError, "prediction activation");

impl fmt::Display for NegotiationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AuthorityOfferInvalid(error) => {
                write!(formatter, "RunenNet authority offer is invalid: {error}")
            }
            Self::PeerOfferInvalid(error) => {
                write!(formatter, "RunenNet peer offer is invalid: {error}")
            }
            _ => write!(formatter, "RunenNet negotiation failure: {self:?}"),
        }
    }
}

impl Error for NegotiationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::AuthorityOfferInvalid(error) | Self::PeerOfferInvalid(error) => Some(error),
            _ => None,
        }
    }
}

impl fmt::Display for NegotiationManagerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Negotiation(error) => {
                write!(formatter, "RunenNet negotiation-manager failure: {error}")
            }
            _ => write!(formatter, "RunenNet negotiation-manager failure: {self:?}"),
        }
    }
}

impl Error for NegotiationManagerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Negotiation(error) => Some(error),
            _ => None,
        }
    }
}

impl fmt::Display for AuthoritySessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Prepare(error) => {
                write!(formatter, "RunenNet authority replication failure: {error}")
            }
            Self::Operation(error) => {
                write!(formatter, "RunenNet authority replication failure: {error}")
            }
            _ => write!(
                formatter,
                "RunenNet authority replication failure: {self:?}"
            ),
        }
    }
}

impl Error for AuthoritySessionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Prepare(error) => Some(error),
            Self::Operation(error) => Some(error),
            _ => None,
        }
    }
}

impl<E> fmt::Display for ClientApplyError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Set(error) => write!(formatter, "RunenNet client replication failure: {error}"),
            Self::HostCommitFailure { source } => {
                write!(formatter, "RunenNet client host commit failed: {source}")
            }
        }
    }
}

impl<E> Error for ClientApplyError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Set(error) => Some(error),
            Self::HostCommitFailure { source } => Some(source),
        }
    }
}

impl<E> fmt::Display for PredictionReconciliationError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReplicationLineageMissing => {
                formatter.write_str("RunenNet prediction replication lineage is missing")
            }
            Self::CommittedCursorRegression {
                last_observed,
                current,
            } => write!(
                formatter,
                "RunenNet prediction observed committed-cursor regression from {} to {}",
                last_observed.get(),
                current.get()
            ),
            Self::MissingCommittedTick => {
                formatter.write_str("RunenNet prediction committed tick is missing")
            }
            Self::FrontierRegression => {
                formatter.write_str("RunenNet prediction frontier would regress")
            }
            Self::AccountingInvariantViolation => {
                formatter.write_str("RunenNet prediction accounting invariant was violated")
            }
            Self::ReplayFailed { tick, source } => write!(
                formatter,
                "RunenNet prediction replay failed at tick {}: {source}",
                tick.get()
            ),
        }
    }
}

impl<E> Error for PredictionReconciliationError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ReplayFailed { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, io};

    use super::*;
    use crate::{identity::SimulationTick, replication::ReplicationCursor};

    fn assert_error<T: Error>() {}

    #[test]
    fn retained_leaf_failures_are_standard_errors_with_display_text() {
        assert_error::<SessionError>();
        assert_error::<OfferValidationError>();
        assert_error::<ReplicationLimitError>();
        assert_error::<AuthorityInputError>();
        assert_error::<PredictionActivationError>();

        for message in [
            SessionError::Closed.to_string(),
            OfferValidationError::OfferTooLarge.to_string(),
            ReplicationLimitError::StateImageExceedsCandidateBudget.to_string(),
            AuthorityInputError::AccountingInvariantViolation.to_string(),
            PredictionActivationError::FrontierRegression.to_string(),
        ] {
            assert!(!message.is_empty());
        }
    }

    #[test]
    fn nested_public_failures_expose_sources() {
        let negotiation = NegotiationManagerError::Negotiation(
            NegotiationError::AuthorityOfferInvalid(OfferValidationError::OfferTooLarge),
        );
        let negotiation_source = negotiation.source().unwrap();
        assert_eq!(
            negotiation_source.source().unwrap().to_string(),
            OfferValidationError::OfferTooLarge.to_string()
        );

        let prepare = AuthoritySessionError::Prepare(AuthorityPrepareError::CandidateTooLarge);
        assert_eq!(
            prepare.source().unwrap().to_string(),
            AuthorityPrepareError::CandidateTooLarge.to_string()
        );

        let operation =
            AuthoritySessionError::Operation(AuthorityOperationError::NoPendingCandidate);
        assert_eq!(
            operation.source().unwrap().to_string(),
            AuthorityOperationError::NoPendingCandidate.to_string()
        );
    }

    #[test]
    fn generic_replication_and_prediction_sources_remain_lossless() {
        let set_error: ClientApplyError<io::Error> =
            ClientApplyError::Set(ClientSetError::UnknownLineage);
        assert_eq!(
            set_error.source().unwrap().to_string(),
            ClientSetError::UnknownLineage.to_string()
        );

        let host_error = ClientApplyError::HostCommitFailure {
            source: io::Error::other("host commit sentinel"),
        };
        assert_eq!(
            host_error.source().unwrap().to_string(),
            "host commit sentinel"
        );

        let replay_error = PredictionReconciliationError::ReplayFailed {
            tick: SimulationTick::new(42),
            source: io::Error::other("replay sentinel"),
        };
        assert_eq!(
            replay_error.source().unwrap().to_string(),
            "replay sentinel"
        );

        let regression: PredictionReconciliationError<io::Error> =
            PredictionReconciliationError::CommittedCursorRegression {
                last_observed: ReplicationCursor::new(8),
                current: ReplicationCursor::new(7),
            };
        assert!(regression.source().is_none());
        assert!(regression.to_string().contains("8"));
        assert!(regression.to_string().contains("7"));
    }
}
