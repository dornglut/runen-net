use std::collections::{HashMap, HashSet};

use runen_net::identity::ConnectionHandle;
use runen_net::protocol::{
    CapabilityId, CapabilityOffer, CodecId, CompatibilityOffer, ConnectionNegotiationTermination,
    NegotiatedContract, NegotiationError, NegotiationManager, NegotiationManagerError,
    NegotiationManagerLimits, NegotiationRequirements, NegotiationStatus, OfferLimits,
    ProtocolContract, ProtocolId, ProtocolRevision, RequirementLevel, SchemaContractId,
    SchemaContractOffer, SchemaId, SchemaOffer, SelectedSchema,
};

fn protocol(revision: u128) -> ProtocolContract {
    ProtocolContract::new(ProtocolId::new(1), ProtocolRevision::new(revision))
}

fn schema(id: u128, contract: u128, codec: u128) -> SchemaOffer {
    SchemaOffer::new(
        SchemaId::new(id),
        RequirementLevel::Optional,
        vec![SchemaContractOffer::new(
            SchemaContractId::new(contract),
            vec![CodecId::new(codec)],
        )],
    )
}

fn offer(label: &str) -> CompatibilityOffer {
    CompatibilityOffer::new(
        vec![protocol(1), protocol(2)],
        vec![
            CapabilityOffer::new(CapabilityId::new(7), RequirementLevel::Optional),
            CapabilityOffer::new(CapabilityId::new(8), RequirementLevel::Optional),
        ],
        vec![schema(9, 10, 11), schema(12, 13, 14)],
        Some(label.to_owned()),
    )
}

fn contract() -> NegotiatedContract {
    let mut contract = NegotiatedContract::new(protocol(1));
    assert!(contract.enable_capability(CapabilityId::new(7)));
    assert!(contract.enable_capability(CapabilityId::new(8)));
    contract
        .bind_schema(
            SchemaId::new(9),
            SelectedSchema::new(SchemaContractId::new(10), CodecId::new(11)),
        )
        .unwrap();
    contract
        .bind_schema(
            SchemaId::new(12),
            SelectedSchema::new(SchemaContractId::new(13), CodecId::new(14)),
        )
        .unwrap();
    contract
}

#[test]
fn manager_borrows_attempt_offers_and_proposal_without_changing_owned_state() {
    let mut manager =
        NegotiationManager::new(OfferLimits::default(), NegotiationManagerLimits::default())
            .unwrap();
    let connection = ConnectionHandle::new(41);
    let authority = offer("authority");
    let peer = offer("peer");
    let expected_authority = authority.clone();
    let expected_peer = peer.clone();

    assert!(matches!(
        manager.attempt_offers(ConnectionHandle::new(99)),
        Err(NegotiationManagerError::UnknownConnection)
    ));

    manager.start(connection, authority, peer).unwrap();
    let reservation = manager.reserved_bytes();
    assert_eq!(
        manager.status(connection).unwrap(),
        NegotiationStatus::AwaitingProposal
    );

    let offers = manager.attempt_offers(connection).unwrap();
    assert_eq!(offers.authority().offer(), &expected_authority);
    assert_eq!(offers.peer().offer(), &expected_peer);
    assert_eq!(manager.reserved_bytes(), reservation);
    assert_eq!(
        manager.status(connection).unwrap(),
        NegotiationStatus::AwaitingProposal
    );
    assert!(matches!(
        manager.attempt_proposal(connection),
        Err(NegotiationManagerError::Negotiation(
            NegotiationError::NoProposal
        ))
    ));

    let expected_contract = contract();
    manager
        .propose(
            connection,
            expected_contract.clone(),
            &NegotiationRequirements::default(),
        )
        .unwrap();
    assert_eq!(
        manager.attempt_proposal(connection).unwrap(),
        &expected_contract
    );
    assert_eq!(manager.reserved_bytes(), reservation);

    assert_eq!(
        manager.validate_authority(connection).unwrap(),
        NegotiationStatus::AwaitingValidation {
            authority_validated: true,
            peer_validated: false,
        }
    );
    assert_eq!(
        manager.attempt_proposal(connection).unwrap(),
        &expected_contract
    );

    assert_eq!(
        manager.validate_peer(connection).unwrap(),
        NegotiationStatus::Established
    );
    assert!(matches!(
        manager.attempt_offers(connection),
        Err(NegotiationManagerError::Negotiation(
            NegotiationError::AlreadyEstablished
        ))
    ));
    assert!(matches!(
        manager.attempt_proposal(connection),
        Err(NegotiationManagerError::Negotiation(
            NegotiationError::AlreadyEstablished
        ))
    ));
    assert_eq!(
        manager.established(connection).unwrap().contract(),
        &expected_contract
    );

    assert_eq!(
        manager.terminate(connection).unwrap(),
        ConnectionNegotiationTermination::EstablishedContractEnded
    );
    assert_eq!(manager.reserved_bytes(), 0);
}

#[test]
fn negotiated_contract_enumeration_preserves_set_and_map_semantics() {
    let contract = contract();

    let capabilities: HashSet<_> = contract.enabled_capabilities().collect();
    assert_eq!(
        capabilities,
        HashSet::from([CapabilityId::new(7), CapabilityId::new(8)])
    );
    assert_eq!(contract.capability_count(), capabilities.len());
    assert!(contract.has_capability(CapabilityId::new(7)));

    let schemas: HashMap<_, _> = contract.selected_schemas().collect();
    assert_eq!(
        schemas,
        HashMap::from([
            (
                SchemaId::new(9),
                SelectedSchema::new(SchemaContractId::new(10), CodecId::new(11)),
            ),
            (
                SchemaId::new(12),
                SelectedSchema::new(SchemaContractId::new(13), CodecId::new(14)),
            ),
        ])
    );
    assert_eq!(contract.schema_count(), schemas.len());
    assert_eq!(
        contract.schema_binding(SchemaId::new(9)),
        Some(SelectedSchema::new(
            SchemaContractId::new(10),
            CodecId::new(11)
        ))
    );
}
