use runen_net::identity::ConnectionHandle;
use runen_net::protocol::{
    CapabilityId, CapabilityOffer, CodecId, CompatibilityOffer, ConnectionNegotiationTermination,
    NegotiationManager, NegotiationManagerLimits, OfferLimits, OfferValidationError,
    ProtocolContract, ProtocolId, ProtocolRevision, RequirementLevel, SchemaContractId,
    SchemaContractOffer, SchemaId, SchemaOffer,
};

fn protocol(revision: u128) -> ProtocolContract {
    ProtocolContract::new(ProtocolId::new(1), ProtocolRevision::new(revision))
}

fn offer(label: &str, protocols: Vec<ProtocolContract>) -> CompatibilityOffer {
    CompatibilityOffer::new(
        protocols,
        vec![CapabilityOffer::new(
            CapabilityId::new(7),
            RequirementLevel::Optional,
        )],
        vec![SchemaOffer::new(
            SchemaId::new(9),
            RequirementLevel::Optional,
            vec![SchemaContractOffer::new(
                SchemaContractId::new(10),
                vec![CodecId::new(11)],
            )],
        )],
        Some(label.to_owned()),
    )
}

fn compatible_offer(label: &str) -> CompatibilityOffer {
    offer(label, vec![protocol(1)])
}

#[test]
fn manager_scoped_offer_validation_uses_exact_manager_policy_without_reservation() {
    let manager_limits = NegotiationManagerLimits::default();
    let offer_limits = OfferLimits {
        max_protocols: 1,
        ..OfferLimits::default()
    };
    let manager = NegotiationManager::new(offer_limits, manager_limits).unwrap();

    assert!(
        offer("permissive", vec![protocol(1), protocol(2)])
            .validate(&OfferLimits::default())
            .is_ok()
    );
    assert_eq!(
        manager.validate_offer(offer("manager", vec![protocol(1), protocol(2)])),
        Err(OfferValidationError::TooManyProtocolAlternatives)
    );
    assert_eq!(manager.active_attempts(), 0);
    assert_eq!(manager.established_connections(), 0);
    assert_eq!(manager.reserved_bytes(), 0);
}

#[test]
fn validated_offer_round_trip_preserves_owned_allocations_and_remains_admissible() {
    let mut manager = NegotiationManager::new(
        OfferLimits::default(),
        NegotiationManagerLimits::default(),
    )
    .unwrap();
    let local = compatible_offer("local");
    let expected = compatible_offer("local");

    let protocols_ptr = local.protocols.as_ptr();
    let capabilities_ptr = local.capabilities.as_ptr();
    let schemas_ptr = local.schemas.as_ptr();
    let contracts_ptr = local.schemas[0].contracts.as_ptr();
    let codecs_ptr = local.schemas[0].contracts[0].codecs.as_ptr();
    let label_ptr = local.diagnostic_label.as_ref().unwrap().as_ptr();

    let validated = manager.validate_offer(local).unwrap();
    let view = validated.offer();
    assert_eq!(view, &expected);
    assert_eq!(view.protocols.as_ptr(), protocols_ptr);
    assert_eq!(view.capabilities.as_ptr(), capabilities_ptr);
    assert_eq!(view.schemas.as_ptr(), schemas_ptr);
    assert_eq!(view.schemas[0].contracts.as_ptr(), contracts_ptr);
    assert_eq!(view.schemas[0].contracts[0].codecs.as_ptr(), codecs_ptr);
    assert_eq!(view.diagnostic_label.as_ref().unwrap().as_ptr(), label_ptr);
    assert_eq!(manager.reserved_bytes(), 0);

    let recovered = validated.into_offer();
    assert_eq!(recovered, expected);
    assert_eq!(recovered.protocols.as_ptr(), protocols_ptr);
    assert_eq!(recovered.capabilities.as_ptr(), capabilities_ptr);
    assert_eq!(recovered.schemas.as_ptr(), schemas_ptr);
    assert_eq!(recovered.schemas[0].contracts.as_ptr(), contracts_ptr);
    assert_eq!(recovered.schemas[0].contracts[0].codecs.as_ptr(), codecs_ptr);
    assert_eq!(
        recovered.diagnostic_label.as_ref().unwrap().as_ptr(),
        label_ptr
    );

    let connection = ConnectionHandle::new(67);
    manager
        .start(connection, recovered, compatible_offer("peer"))
        .unwrap();
    assert_eq!(manager.active_attempts(), 1);
    assert!(manager.reserved_bytes() > 0);
    assert_eq!(
        manager.terminate(connection).unwrap(),
        ConnectionNegotiationTermination::NegotiationAborted
    );
    assert_eq!(manager.reserved_bytes(), 0);
}
