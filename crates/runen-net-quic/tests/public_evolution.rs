use runen_net_quic::{
    EndpointBindError, EndpointResourceError, FlowCommandError, FlowRejectionReason,
    NegotiationFailure, ProfileBootstrapFailure, ProfileConfigError, ProfileConnectionErrorKind,
    SemanticRole, SubmitOutcome, TlsMaterialError,
};

macro_rules! known_or_other {
    ($value:expr, $known:pat) => {
        match $value {
            $known => true,
            _ => false,
        }
    };
}

#[test]
fn public_evolution_boundaries_are_deliberate_for_external_consumers() {
    assert!(known_or_other!(
        EndpointResourceError::ZeroConnections,
        EndpointResourceError::ZeroConnections
    ));
    assert!(known_or_other!(
        ProfileConfigError::ZeroIncomingMessageBytes,
        ProfileConfigError::ZeroIncomingMessageBytes
    ));
    assert!(known_or_other!(
        TlsMaterialError::EmptyClientTrust,
        TlsMaterialError::EmptyClientTrust
    ));
    assert!(known_or_other!(
        EndpointBindError::TrustCertificateRejected,
        EndpointBindError::TrustCertificateRejected
    ));
    assert!(known_or_other!(
        ProfileBootstrapFailure::RoleMismatch,
        ProfileBootstrapFailure::RoleMismatch
    ));
    assert!(known_or_other!(
        ProfileConnectionErrorKind::AdmissionAtCapacity,
        ProfileConnectionErrorKind::AdmissionAtCapacity
    ));
    assert!(known_or_other!(
        FlowCommandError::Busy,
        FlowCommandError::Busy
    ));
    assert!(known_or_other!(
        SubmitOutcome::RejectedCurrentDatagramSize,
        SubmitOutcome::RejectedCurrentDatagramSize
    ));

    assert!(match SemanticRole::Authority {
        SemanticRole::Authority => true,
        SemanticRole::NonAuthority => false,
    });
    assert!(match NegotiationFailure::ProtocolIncompatible {
        NegotiationFailure::ProtocolIncompatible => true,
        NegotiationFailure::MalformedOffer
        | NegotiationFailure::RequiredCapabilityUnavailable
        | NegotiationFailure::RequiredSchemaUnavailable
        | NegotiationFailure::ResourceLimitExceeded
        | NegotiationFailure::InvalidSelection => false,
    });
    assert!(match FlowRejectionReason::ResourceLimit {
        FlowRejectionReason::ResourceLimit => true,
        FlowRejectionReason::MessageLimit => false,
    });
}
