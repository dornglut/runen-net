use crate::protocol::{
    CapabilityId, CapabilityOffer, CodecId, CompatibilityOffer, ProtocolContract, ProtocolId,
    ProtocolRevision, RequirementLevel, SchemaContractId, SchemaContractOffer, SchemaId,
    SchemaOffer,
};

/// Fluent assembly for the existing [`CompatibilityOffer`] semantic value.
///
/// This builder performs no validation, normalization, deduplication, identity derivation, or
/// negotiation policy. `CompatibilityOffer::validate` and `NegotiationManager::validate_offer`
/// remain the validation authorities.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityOfferBuilder {
    offer: CompatibilityOffer,
}

impl CompatibilityOfferBuilder {
    fn new() -> Self {
        Self {
            offer: CompatibilityOffer::new(Vec::new(), Vec::new(), Vec::new(), None),
        }
    }

    pub fn protocol(mut self, id: ProtocolId, revision: ProtocolRevision) -> Self {
        self.offer
            .protocols
            .push(ProtocolContract::new(id, revision));
        self
    }

    pub fn capability(mut self, id: CapabilityId, requirement: RequirementLevel) -> Self {
        self.offer
            .capabilities
            .push(CapabilityOffer::new(id, requirement));
        self
    }

    pub fn schema(mut self, schema: SchemaOffer) -> Self {
        self.offer.schemas.push(schema);
        self
    }

    pub fn diagnostic_label(mut self, label: impl Into<String>) -> Self {
        self.offer.diagnostic_label = Some(label.into());
        self
    }

    pub fn build(self) -> CompatibilityOffer {
        self.offer
    }
}

/// Fluent assembly for the existing [`SchemaOffer`] semantic value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaOfferBuilder {
    schema: SchemaOffer,
}

impl SchemaOfferBuilder {
    fn new(id: SchemaId, requirement: RequirementLevel) -> Self {
        Self {
            schema: SchemaOffer::new(id, requirement, Vec::new()),
        }
    }

    pub fn contract(mut self, contract: SchemaContractOffer) -> Self {
        self.schema.contracts.push(contract);
        self
    }

    pub fn build(self) -> SchemaOffer {
        self.schema
    }
}

/// Fluent assembly for the existing [`SchemaContractOffer`] semantic value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaContractOfferBuilder {
    contract: SchemaContractOffer,
}

impl SchemaContractOfferBuilder {
    fn new(contract_id: SchemaContractId) -> Self {
        Self {
            contract: SchemaContractOffer::new(contract_id, Vec::new()),
        }
    }

    pub fn codec(mut self, codec_id: CodecId) -> Self {
        self.contract.codecs.push(codec_id);
        self
    }

    pub fn build(self) -> SchemaContractOffer {
        self.contract
    }
}

impl CompatibilityOffer {
    pub fn builder() -> CompatibilityOfferBuilder {
        CompatibilityOfferBuilder::new()
    }
}

impl SchemaOffer {
    pub fn builder(id: SchemaId, requirement: RequirementLevel) -> SchemaOfferBuilder {
        SchemaOfferBuilder::new(id, requirement)
    }
}

impl SchemaContractOffer {
    pub fn builder(contract_id: SchemaContractId) -> SchemaContractOfferBuilder {
        SchemaContractOfferBuilder::new(contract_id)
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::{OfferLimits, OfferValidationError};

    use super::*;

    #[test]
    fn builders_preserve_the_exact_manual_offer_value_and_order() {
        let protocol_a = ProtocolContract::new(ProtocolId::new(1), ProtocolRevision::new(10));
        let protocol_b = ProtocolContract::new(ProtocolId::new(2), ProtocolRevision::new(20));
        let capability_required =
            CapabilityOffer::new(CapabilityId::new(30), RequirementLevel::Required);
        let capability_optional =
            CapabilityOffer::new(CapabilityId::new(31), RequirementLevel::Optional);
        let contract_a = SchemaContractOffer::new(
            SchemaContractId::new(50),
            vec![CodecId::new(60), CodecId::new(61)],
        );
        let contract_b =
            SchemaContractOffer::new(SchemaContractId::new(51), vec![CodecId::new(62)]);
        let optional_contract =
            SchemaContractOffer::new(SchemaContractId::new(52), vec![CodecId::new(63)]);
        let required_schema = SchemaOffer::new(
            SchemaId::new(40),
            RequirementLevel::Required,
            vec![contract_a.clone(), contract_b.clone()],
        );
        let optional_schema = SchemaOffer::new(
            SchemaId::new(41),
            RequirementLevel::Optional,
            vec![optional_contract.clone()],
        );
        let manual = CompatibilityOffer::new(
            vec![protocol_a, protocol_b],
            vec![capability_required, capability_optional],
            vec![required_schema, optional_schema],
            Some("public-client".to_owned()),
        );

        let built = CompatibilityOffer::builder()
            .protocol(protocol_a.id, protocol_a.revision)
            .protocol(protocol_b.id, protocol_b.revision)
            .capability(capability_required.id, capability_required.requirement)
            .capability(capability_optional.id, capability_optional.requirement)
            .schema(
                SchemaOffer::builder(SchemaId::new(40), RequirementLevel::Required)
                    .contract(
                        SchemaContractOffer::builder(SchemaContractId::new(50))
                            .codec(CodecId::new(60))
                            .codec(CodecId::new(61))
                            .build(),
                    )
                    .contract(
                        SchemaContractOffer::builder(SchemaContractId::new(51))
                            .codec(CodecId::new(62))
                            .build(),
                    )
                    .build(),
            )
            .schema(
                SchemaOffer::builder(SchemaId::new(41), RequirementLevel::Optional)
                    .contract(
                        SchemaContractOffer::builder(SchemaContractId::new(52))
                            .codec(CodecId::new(63))
                            .build(),
                    )
                    .build(),
            )
            .diagnostic_label("public-client")
            .build();

        assert_eq!(built, manual);
    }

    #[test]
    fn builders_leave_duplicate_empty_limit_and_accounting_failures_to_existing_validation() {
        let limits = OfferLimits::default();

        let duplicate = CompatibilityOffer::builder()
            .protocol(ProtocolId::new(1), ProtocolRevision::new(1))
            .capability(CapabilityId::new(2), RequirementLevel::Required)
            .capability(CapabilityId::new(2), RequirementLevel::Optional)
            .build();
        assert_eq!(
            duplicate.validate(&limits),
            Err(OfferValidationError::DuplicateCapability)
        );

        let empty = CompatibilityOffer::builder()
            .protocol(ProtocolId::new(1), ProtocolRevision::new(1))
            .schema(
                SchemaOffer::builder(SchemaId::new(3), RequirementLevel::Required)
                    .contract(SchemaContractOffer::builder(SchemaContractId::new(4)).build())
                    .build(),
            )
            .build();
        assert_eq!(
            empty.validate(&limits),
            Err(OfferValidationError::EmptyCodecs)
        );

        let limited = CompatibilityOffer::builder()
            .protocol(ProtocolId::new(1), ProtocolRevision::new(1))
            .protocol(ProtocolId::new(2), ProtocolRevision::new(1))
            .build();
        let one_protocol = OfferLimits {
            max_protocols: 1,
            ..limits
        };
        assert_eq!(
            limited.validate(&one_protocol),
            Err(OfferValidationError::TooManyProtocolAlternatives)
        );

        let accounted = CompatibilityOffer::builder()
            .protocol(ProtocolId::new(1), ProtocolRevision::new(1))
            .build();
        let tiny_accounting_budget = OfferLimits {
            max_offer_accounted_bytes: 1,
            ..limits
        };
        assert_eq!(
            accounted.validate(&tiny_accounting_budget),
            Err(OfferValidationError::OfferTooLarge)
        );
    }
}
