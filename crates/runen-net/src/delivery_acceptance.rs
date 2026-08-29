use crate::delivery::SubmissionOutcome;

/// Whether one complete message was accepted into its selected RunenNet delivery contract.
///
/// This is intentionally narrower than [`SubmissionOutcome`]. It carries only the semantic fact
/// needed by higher layers such as replication; detailed rejection reasons, accepted indices, and
/// pressure diagnostics remain owned by the delivery or transport-adapter outcome that produced it.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DeliveryAcceptance {
    Accepted,
    NotAccepted,
}

impl SubmissionOutcome {
    /// Project this detailed Core submission outcome onto transport-independent acceptance evidence.
    pub const fn acceptance(self) -> DeliveryAcceptance {
        match self {
            Self::Accepted { .. } => DeliveryAcceptance::Accepted,
            Self::RejectedTooLarge
            | Self::RejectedPressure
            | Self::RejectedCounterExhausted => DeliveryAcceptance::NotAccepted,
        }
    }
}
