use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::hash::Hash;
use std::num::NonZeroUsize;

use crate::identity::ConnectionHandle;

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

opaque_u128_id!(ProtocolId);
opaque_u128_id!(ProtocolRevision);
opaque_u128_id!(SchemaId);
opaque_u128_id!(SchemaContractId);
opaque_u128_id!(CodecId);
opaque_u128_id!(CapabilityId);

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct ProtocolContract {
    pub id: ProtocolId,
    pub revision: ProtocolRevision,
}

impl ProtocolContract {
    pub const fn new(id: ProtocolId, revision: ProtocolRevision) -> Self {
        Self { id, revision }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RequirementLevel {
    Required,
    Optional,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct CapabilityOffer {
    pub id: CapabilityId,
    pub requirement: RequirementLevel,
}

impl CapabilityOffer {
    pub const fn new(id: CapabilityId, requirement: RequirementLevel) -> Self {
        Self { id, requirement }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaContractOffer {
    pub contract_id: SchemaContractId,
    pub codecs: Vec<CodecId>,
}

impl SchemaContractOffer {
    pub fn new(contract_id: SchemaContractId, codecs: Vec<CodecId>) -> Self {
        Self {
            contract_id,
            codecs,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaOffer {
    pub id: SchemaId,
    pub requirement: RequirementLevel,
    pub contracts: Vec<SchemaContractOffer>,
}

impl SchemaOffer {
    pub fn new(
        id: SchemaId,
        requirement: RequirementLevel,
        contracts: Vec<SchemaContractOffer>,
    ) -> Self {
        Self {
            id,
            requirement,
            contracts,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilityOffer {
    pub protocols: Vec<ProtocolContract>,
    pub capabilities: Vec<CapabilityOffer>,
    pub schemas: Vec<SchemaOffer>,
    pub diagnostic_label: Option<String>,
}

impl CompatibilityOffer {
    pub fn new(
        protocols: Vec<ProtocolContract>,
        capabilities: Vec<CapabilityOffer>,
        schemas: Vec<SchemaOffer>,
        diagnostic_label: Option<String>,
    ) -> Self {
        Self {
            protocols,
            capabilities,
            schemas,
            diagnostic_label,
        }
    }

    pub fn validate(self, limits: &OfferLimits) -> Result<ValidatedOffer, OfferValidationError> {
        limits.validate()?;

        if self.protocols.is_empty() {
            return Err(OfferValidationError::EmptyProtocolAlternatives);
        }
        if self.protocols.len() > limits.max_protocols {
            return Err(OfferValidationError::TooManyProtocolAlternatives);
        }
        if self.capabilities.len() > limits.max_capabilities {
            return Err(OfferValidationError::TooManyCapabilities);
        }
        if self.schemas.len() > limits.max_schemas {
            return Err(OfferValidationError::TooManySchemas);
        }
        if self
            .diagnostic_label
            .as_ref()
            .is_some_and(|label| label.len() > limits.max_diagnostic_label_bytes)
        {
            return Err(OfferValidationError::DiagnosticLabelTooLong);
        }

        let mut protocols = HashSet::with_capacity(self.protocols.len());
        for protocol in &self.protocols {
            if !protocols.insert(*protocol) {
                return Err(OfferValidationError::DuplicateProtocolAlternative);
            }
        }

        let mut capabilities = HashSet::with_capacity(self.capabilities.len());
        for capability in &self.capabilities {
            if !capabilities.insert(capability.id) {
                return Err(OfferValidationError::DuplicateCapability);
            }
        }

        let mut schemas = HashSet::with_capacity(self.schemas.len());
        for schema in &self.schemas {
            if !schemas.insert(schema.id) {
                return Err(OfferValidationError::DuplicateSchema);
            }
            if schema.contracts.is_empty() {
                return Err(OfferValidationError::EmptySchemaContracts);
            }
            if schema.contracts.len() > limits.max_contracts_per_schema {
                return Err(OfferValidationError::TooManySchemaContracts);
            }

            let mut contracts = HashSet::with_capacity(schema.contracts.len());
            for contract in &schema.contracts {
                if !contracts.insert(contract.contract_id) {
                    return Err(OfferValidationError::DuplicateSchemaContract);
                }
                if contract.codecs.is_empty() {
                    return Err(OfferValidationError::EmptyCodecs);
                }
                if contract.codecs.len() > limits.max_codecs_per_contract {
                    return Err(OfferValidationError::TooManyCodecs);
                }

                let mut codecs = HashSet::with_capacity(contract.codecs.len());
                for codec in &contract.codecs {
                    if !codecs.insert(*codec) {
                        return Err(OfferValidationError::DuplicateCodec);
                    }
                }
            }
        }

        let accounted_bytes = accounted_offer_bytes(&self)?;
        if accounted_bytes > limits.max_offer_accounted_bytes {
            return Err(OfferValidationError::OfferTooLarge);
        }

        Ok(ValidatedOffer {
            offer: self,
            accounted_bytes,
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct OfferLimits {
    pub max_protocols: usize,
    pub max_capabilities: usize,
    pub max_schemas: usize,
    pub max_contracts_per_schema: usize,
    pub max_codecs_per_contract: usize,
    pub max_diagnostic_label_bytes: usize,
    pub max_offer_accounted_bytes: usize,
}

impl Default for OfferLimits {
    fn default() -> Self {
        Self {
            max_protocols: 8,
            max_capabilities: 64,
            max_schemas: 128,
            max_contracts_per_schema: 8,
            max_codecs_per_contract: 8,
            max_diagnostic_label_bytes: 256,
            max_offer_accounted_bytes: 64 * 1024,
        }
    }
}

impl OfferLimits {
    fn validate(&self) -> Result<(), OfferValidationError> {
        if self.max_protocols == 0
            || self.max_capabilities == 0
            || self.max_schemas == 0
            || self.max_contracts_per_schema == 0
            || self.max_codecs_per_contract == 0
            || self.max_offer_accounted_bytes == 0
        {
            return Err(OfferValidationError::InvalidLimits);
        }
        Ok(())
    }

    fn max_attempt_reservation(&self) -> Result<usize, NegotiationManagerConfigError> {
        self.max_offer_accounted_bytes
            .checked_mul(3)
            .ok_or(NegotiationManagerConfigError::AccountingOverflow)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OfferValidationError {
    InvalidLimits,
    EmptyProtocolAlternatives,
    TooManyProtocolAlternatives,
    DuplicateProtocolAlternative,
    TooManyCapabilities,
    DuplicateCapability,
    TooManySchemas,
    DuplicateSchema,
    EmptySchemaContracts,
    TooManySchemaContracts,
    DuplicateSchemaContract,
    EmptyCodecs,
    TooManyCodecs,
    DuplicateCodec,
    DiagnosticLabelTooLong,
    OfferTooLarge,
    AccountingOverflow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedOffer {
    offer: CompatibilityOffer,
    accounted_bytes: usize,
}

impl ValidatedOffer {
    pub const fn accounted_bytes(&self) -> usize {
        self.accounted_bytes
    }

    pub const fn offer(&self) -> &CompatibilityOffer {
        &self.offer
    }

    fn supports_protocol(&self, protocol: ProtocolContract) -> bool {
        self.offer.protocols.contains(&protocol)
    }

    fn capability(&self, id: CapabilityId) -> Option<&CapabilityOffer> {
        self.offer.capabilities.iter().find(|entry| entry.id == id)
    }

    fn schema(&self, id: SchemaId) -> Option<&SchemaOffer> {
        self.offer.schemas.iter().find(|entry| entry.id == id)
    }

    fn supports_schema_binding(&self, id: SchemaId, binding: SelectedSchema) -> bool {
        self.schema(id).is_some_and(|schema| {
            schema.contracts.iter().any(|contract| {
                contract.contract_id == binding.contract_id
                    && contract.codecs.contains(&binding.codec_id)
            })
        })
    }

    fn required_capabilities(&self) -> impl Iterator<Item = CapabilityId> + '_ {
        self.offer.capabilities.iter().filter_map(|entry| {
            (entry.requirement == RequirementLevel::Required).then_some(entry.id)
        })
    }

    fn required_schemas(&self) -> impl Iterator<Item = SchemaId> + '_ {
        self.offer.schemas.iter().filter_map(|entry| {
            (entry.requirement == RequirementLevel::Required).then_some(entry.id)
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct SelectedSchema {
    pub contract_id: SchemaContractId,
    pub codec_id: CodecId,
}

impl SelectedSchema {
    pub const fn new(contract_id: SchemaContractId, codec_id: CodecId) -> Self {
        Self {
            contract_id,
            codec_id,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedContract {
    protocol: ProtocolContract,
    capabilities: HashSet<CapabilityId>,
    schemas: HashMap<SchemaId, SelectedSchema>,
}

impl NegotiatedContract {
    pub fn new(protocol: ProtocolContract) -> Self {
        Self {
            protocol,
            capabilities: HashSet::new(),
            schemas: HashMap::new(),
        }
    }

    pub const fn protocol(&self) -> ProtocolContract {
        self.protocol
    }

    pub fn enable_capability(&mut self, id: CapabilityId) -> bool {
        self.capabilities.insert(id)
    }

    pub fn has_capability(&self, id: CapabilityId) -> bool {
        self.capabilities.contains(&id)
    }

    pub fn bind_schema(
        &mut self,
        schema_id: SchemaId,
        binding: SelectedSchema,
    ) -> Result<(), SchemaBindingError> {
        match self.schemas.entry(schema_id) {
            Entry::Occupied(_) => Err(SchemaBindingError::AlreadyBound),
            Entry::Vacant(entry) => {
                entry.insert(binding);
                Ok(())
            }
        }
    }

    pub fn schema_binding(&self, schema_id: SchemaId) -> Option<SelectedSchema> {
        self.schemas.get(&schema_id).copied()
    }

    pub fn capability_count(&self) -> usize {
        self.capabilities.len()
    }

    pub fn schema_count(&self) -> usize {
        self.schemas.len()
    }

    pub fn enabled_capabilities(&self) -> impl Iterator<Item = CapabilityId> + '_ {
        self.capabilities.iter().copied()
    }

    pub fn selected_schemas(&self) -> impl Iterator<Item = (SchemaId, SelectedSchema)> + '_ {
        self.schemas
            .iter()
            .map(|(schema_id, binding)| (*schema_id, *binding))
    }

    fn accounted_bytes(&self) -> Result<usize, OfferValidationError> {
        const ID_BYTES: usize = 16;
        const PROTOCOL_BYTES: usize = ID_BYTES * 2;
        const SCHEMA_BINDING_BYTES: usize = ID_BYTES * 3;

        let capability_bytes = self
            .capabilities
            .len()
            .checked_mul(ID_BYTES)
            .ok_or(OfferValidationError::AccountingOverflow)?;
        let schema_bytes = self
            .schemas
            .len()
            .checked_mul(SCHEMA_BINDING_BYTES)
            .ok_or(OfferValidationError::AccountingOverflow)?;

        PROTOCOL_BYTES
            .checked_add(capability_bytes)
            .and_then(|value| value.checked_add(schema_bytes))
            .ok_or(OfferValidationError::AccountingOverflow)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SchemaBindingError {
    AlreadyBound,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct NegotiationRequirements {
    required_capabilities: HashSet<CapabilityId>,
    required_schemas: HashSet<SchemaId>,
}

impl NegotiationRequirements {
    pub fn require_capability(&mut self, id: CapabilityId) -> bool {
        self.required_capabilities.insert(id)
    }

    pub fn require_schema(&mut self, id: SchemaId) -> bool {
        self.required_schemas.insert(id)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum NegotiationStatus {
    AwaitingProposal,
    AwaitingValidation {
        authority_validated: bool,
        peer_validated: bool,
    },
    Established,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum NegotiationError {
    AuthorityOfferInvalid(OfferValidationError),
    PeerOfferInvalid(OfferValidationError),
    ProtocolIncompatible,
    RequiredCapabilityUnavailable(CapabilityId),
    RequiredSchemaUnavailable(SchemaId),
    InvalidSelection,
    SelectionTooLarge,
    AlreadyProposed,
    NoProposal,
    AlreadyEstablished,
}

#[derive(Debug, Copy, Clone)]
pub struct NegotiationOffers<'a> {
    authority: &'a ValidatedOffer,
    peer: &'a ValidatedOffer,
}

impl<'a> NegotiationOffers<'a> {
    pub const fn authority(self) -> &'a ValidatedOffer {
        self.authority
    }

    pub const fn peer(self) -> &'a ValidatedOffer {
        self.peer
    }
}

#[derive(Debug, Copy, Clone)]
pub struct EstablishedNegotiation<'a> {
    connection: ConnectionHandle,
    contract: &'a NegotiatedContract,
}

impl<'a> EstablishedNegotiation<'a> {
    pub const fn connection(self) -> ConnectionHandle {
        self.connection
    }

    pub const fn contract(self) -> &'a NegotiatedContract {
        self.contract
    }
}

#[derive(Debug, PartialEq, Eq)]
struct NegotiationAttempt {
    authority_offer: ValidatedOffer,
    peer_offer: ValidatedOffer,
    offer_limits: OfferLimits,
    proposal: Option<NegotiatedContract>,
    authority_validated: bool,
    peer_validated: bool,
}

impl NegotiationAttempt {
    fn new(
        authority_offer: CompatibilityOffer,
        peer_offer: CompatibilityOffer,
        offer_limits: OfferLimits,
    ) -> Result<Self, NegotiationError> {
        let authority_offer = authority_offer
            .validate(&offer_limits)
            .map_err(NegotiationError::AuthorityOfferInvalid)?;
        let peer_offer = peer_offer
            .validate(&offer_limits)
            .map_err(NegotiationError::PeerOfferInvalid)?;

        if !authority_offer
            .offer
            .protocols
            .iter()
            .any(|protocol| peer_offer.supports_protocol(*protocol))
        {
            return Err(NegotiationError::ProtocolIncompatible);
        }

        verify_offer_requirements(&authority_offer, &peer_offer)?;
        verify_offer_requirements(&peer_offer, &authority_offer)?;

        Ok(Self {
            authority_offer,
            peer_offer,
            offer_limits,
            proposal: None,
            authority_validated: false,
            peer_validated: false,
        })
    }

    fn status(&self) -> NegotiationStatus {
        if self.authority_validated && self.peer_validated {
            NegotiationStatus::Established
        } else if self.proposal.is_some() {
            NegotiationStatus::AwaitingValidation {
                authority_validated: self.authority_validated,
                peer_validated: self.peer_validated,
            }
        } else {
            NegotiationStatus::AwaitingProposal
        }
    }

    fn propose(
        &mut self,
        contract: NegotiatedContract,
        requirements: &NegotiationRequirements,
    ) -> Result<(), NegotiationError> {
        if self.status() == NegotiationStatus::Established {
            return Err(NegotiationError::AlreadyEstablished);
        }
        if self.proposal.is_some() {
            return Err(NegotiationError::AlreadyProposed);
        }

        self.validate_selection(&contract, requirements)?;
        self.proposal = Some(contract);
        Ok(())
    }

    fn validate_authority(&mut self) -> Result<NegotiationStatus, NegotiationError> {
        self.validate_party(true)
    }

    fn validate_peer(&mut self) -> Result<NegotiationStatus, NegotiationError> {
        self.validate_party(false)
    }

    fn validate_party(&mut self, authority: bool) -> Result<NegotiationStatus, NegotiationError> {
        self.proposal.as_ref().ok_or(NegotiationError::NoProposal)?;

        if authority {
            self.authority_validated = true;
        } else {
            self.peer_validated = true;
        }
        Ok(self.status())
    }

    fn validate_selection(
        &self,
        contract: &NegotiatedContract,
        requirements: &NegotiationRequirements,
    ) -> Result<(), NegotiationError> {
        if !self.authority_offer.supports_protocol(contract.protocol())
            || !self.peer_offer.supports_protocol(contract.protocol())
        {
            return Err(NegotiationError::InvalidSelection);
        }

        if contract.capability_count() > self.offer_limits.max_capabilities
            || contract.schema_count() > self.offer_limits.max_schemas
            || contract
                .accounted_bytes()
                .map_err(|_| NegotiationError::SelectionTooLarge)?
                > self.offer_limits.max_offer_accounted_bytes
        {
            return Err(NegotiationError::SelectionTooLarge);
        }

        for capability in &contract.capabilities {
            if self.authority_offer.capability(*capability).is_none()
                || self.peer_offer.capability(*capability).is_none()
            {
                return Err(NegotiationError::InvalidSelection);
            }
        }

        for (schema_id, binding) in &contract.schemas {
            if !self
                .authority_offer
                .supports_schema_binding(*schema_id, *binding)
                || !self
                    .peer_offer
                    .supports_schema_binding(*schema_id, *binding)
            {
                return Err(NegotiationError::InvalidSelection);
            }
        }

        for capability in self
            .authority_offer
            .required_capabilities()
            .chain(self.peer_offer.required_capabilities())
        {
            if !contract.has_capability(capability) {
                return Err(NegotiationError::InvalidSelection);
            }
        }

        for schema_id in self
            .authority_offer
            .required_schemas()
            .chain(self.peer_offer.required_schemas())
        {
            if contract.schema_binding(schema_id).is_none() {
                return Err(NegotiationError::InvalidSelection);
            }
        }

        for capability in &requirements.required_capabilities {
            let mutual = self.authority_offer.capability(*capability).is_some()
                && self.peer_offer.capability(*capability).is_some();
            if !mutual {
                return Err(NegotiationError::RequiredCapabilityUnavailable(*capability));
            }
            if !contract.has_capability(*capability) {
                return Err(NegotiationError::InvalidSelection);
            }
        }

        for schema_id in &requirements.required_schemas {
            if !has_common_schema_binding(&self.authority_offer, &self.peer_offer, *schema_id) {
                return Err(NegotiationError::RequiredSchemaUnavailable(*schema_id));
            }
            if contract.schema_binding(*schema_id).is_none() {
                return Err(NegotiationError::InvalidSelection);
            }
        }

        Ok(())
    }
}

fn verify_offer_requirements(
    requiring: &ValidatedOffer,
    other: &ValidatedOffer,
) -> Result<(), NegotiationError> {
    for capability in requiring.required_capabilities() {
        if other.capability(capability).is_none() {
            return Err(NegotiationError::RequiredCapabilityUnavailable(capability));
        }
    }

    for schema_id in requiring.required_schemas() {
        if !has_common_schema_binding(requiring, other, schema_id) {
            return Err(NegotiationError::RequiredSchemaUnavailable(schema_id));
        }
    }

    Ok(())
}

fn has_common_schema_binding(
    first: &ValidatedOffer,
    second: &ValidatedOffer,
    schema_id: SchemaId,
) -> bool {
    let Some(first_schema) = first.schema(schema_id) else {
        return false;
    };
    let Some(second_schema) = second.schema(schema_id) else {
        return false;
    };

    first_schema.contracts.iter().any(|first_contract| {
        second_schema.contracts.iter().any(|second_contract| {
            first_contract.contract_id == second_contract.contract_id
                && first_contract
                    .codecs
                    .iter()
                    .any(|codec| second_contract.codecs.contains(codec))
        })
    })
}

/// Computes the RN2 implementation's accountable in-memory representation size.
///
/// These values are implementation accounting units based on the current Rust
/// identity representation. They are not RunenNet wire-format sizes.
fn accounted_offer_bytes(offer: &CompatibilityOffer) -> Result<usize, OfferValidationError> {
    const ID_BYTES: usize = 16;
    const REQUIREMENT_BYTES: usize = 1;
    const PROTOCOL_BYTES: usize = ID_BYTES * 2;

    let mut total = 0usize;
    total = checked_add_mul(total, offer.protocols.len(), PROTOCOL_BYTES)?;
    total = checked_add_mul(
        total,
        offer.capabilities.len(),
        ID_BYTES + REQUIREMENT_BYTES,
    )?;

    for schema in &offer.schemas {
        total = total
            .checked_add(ID_BYTES + REQUIREMENT_BYTES)
            .ok_or(OfferValidationError::AccountingOverflow)?;
        total = checked_add_mul(total, schema.contracts.len(), ID_BYTES)?;
        for contract in &schema.contracts {
            total = checked_add_mul(total, contract.codecs.len(), ID_BYTES)?;
        }
    }

    if let Some(label) = &offer.diagnostic_label {
        total = total
            .checked_add(label.len())
            .ok_or(OfferValidationError::AccountingOverflow)?;
    }

    Ok(total)
}

fn checked_add_mul(
    total: usize,
    count: usize,
    bytes_per_item: usize,
) -> Result<usize, OfferValidationError> {
    count
        .checked_mul(bytes_per_item)
        .and_then(|bytes| total.checked_add(bytes))
        .ok_or(OfferValidationError::AccountingOverflow)
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct NegotiationManagerLimits {
    pub max_concurrent_attempts: usize,
    pub max_aggregate_accounted_bytes: usize,
}

impl Default for NegotiationManagerLimits {
    fn default() -> Self {
        Self {
            max_concurrent_attempts: 64,
            max_aggregate_accounted_bytes: 16 * 1024 * 1024,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum NegotiationManagerConfigError {
    InvalidLimits,
    AccountingOverflow,
    AttemptReservationExceedsAggregate,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum NegotiationManagerError {
    AttemptLimitExceeded,
    AggregateLimitExceeded,
    ConnectionAlreadyKnown,
    UnknownConnection,
    Negotiation(NegotiationError),
}

impl From<NegotiationError> for NegotiationManagerError {
    fn from(value: NegotiationError) -> Self {
        Self::Negotiation(value)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ConnectionNegotiationTermination {
    NegotiationAborted,
    EstablishedContractEnded,
}

#[derive(Debug)]
struct EstablishedState {
    contract: NegotiatedContract,
    accounted_bytes: usize,
}

#[derive(Debug)]
pub struct NegotiationManager {
    offer_limits: OfferLimits,
    limits: NegotiationManagerLimits,
    per_attempt_reservation: usize,
    reserved_bytes: usize,
    attempts: HashMap<ConnectionHandle, NegotiationAttempt>,
    established: HashMap<ConnectionHandle, EstablishedState>,
}

impl NegotiationManager {
    pub fn new(
        offer_limits: OfferLimits,
        limits: NegotiationManagerLimits,
    ) -> Result<Self, NegotiationManagerConfigError> {
        if offer_limits.validate().is_err()
            || limits.max_concurrent_attempts == 0
            || limits.max_aggregate_accounted_bytes == 0
        {
            return Err(NegotiationManagerConfigError::InvalidLimits);
        }

        let per_attempt_reservation = offer_limits.max_attempt_reservation()?;
        if per_attempt_reservation > limits.max_aggregate_accounted_bytes {
            return Err(NegotiationManagerConfigError::AttemptReservationExceedsAggregate);
        }

        Ok(Self {
            offer_limits,
            limits,
            per_attempt_reservation,
            reserved_bytes: 0,
            attempts: HashMap::new(),
            established: HashMap::new(),
        })
    }

    pub fn start(
        &mut self,
        connection: ConnectionHandle,
        authority_offer: CompatibilityOffer,
        peer_offer: CompatibilityOffer,
    ) -> Result<(), NegotiationManagerError> {
        if self.attempts.contains_key(&connection) || self.established.contains_key(&connection) {
            return Err(NegotiationManagerError::ConnectionAlreadyKnown);
        }
        if self.attempts.len() >= self.limits.max_concurrent_attempts {
            return Err(NegotiationManagerError::AttemptLimitExceeded);
        }
        if self
            .reserved_bytes
            .checked_add(self.per_attempt_reservation)
            .is_none_or(|value| value > self.limits.max_aggregate_accounted_bytes)
        {
            return Err(NegotiationManagerError::AggregateLimitExceeded);
        }

        let attempt = NegotiationAttempt::new(authority_offer, peer_offer, self.offer_limits)?;
        self.attempts.insert(connection, attempt);
        self.reserved_bytes += self.per_attempt_reservation;
        Ok(())
    }

    pub fn attempt_offers(
        &self,
        connection: ConnectionHandle,
    ) -> Result<NegotiationOffers<'_>, NegotiationManagerError> {
        let attempt = self.attempt(connection)?;
        Ok(NegotiationOffers {
            authority: &attempt.authority_offer,
            peer: &attempt.peer_offer,
        })
    }

    pub fn attempt_proposal(
        &self,
        connection: ConnectionHandle,
    ) -> Result<&NegotiatedContract, NegotiationManagerError> {
        self.attempt(connection)?
            .proposal
            .as_ref()
            .ok_or_else(|| NegotiationError::NoProposal.into())
    }

    pub fn propose(
        &mut self,
        connection: ConnectionHandle,
        contract: NegotiatedContract,
        requirements: &NegotiationRequirements,
    ) -> Result<(), NegotiationManagerError> {
        self.attempt_mut(connection)?
            .propose(contract, requirements)?;
        Ok(())
    }

    pub fn validate_authority(
        &mut self,
        connection: ConnectionHandle,
    ) -> Result<NegotiationStatus, NegotiationManagerError> {
        let status = self.attempt_mut(connection)?.validate_authority()?;
        if status == NegotiationStatus::Established {
            self.promote_established(connection)?;
        }
        Ok(status)
    }

    pub fn validate_peer(
        &mut self,
        connection: ConnectionHandle,
    ) -> Result<NegotiationStatus, NegotiationManagerError> {
        let status = self.attempt_mut(connection)?.validate_peer()?;
        if status == NegotiationStatus::Established {
            self.promote_established(connection)?;
        }
        Ok(status)
    }

    pub fn status(
        &self,
        connection: ConnectionHandle,
    ) -> Result<NegotiationStatus, NegotiationManagerError> {
        if self.established.contains_key(&connection) {
            return Ok(NegotiationStatus::Established);
        }
        self.attempts
            .get(&connection)
            .map(NegotiationAttempt::status)
            .ok_or(NegotiationManagerError::UnknownConnection)
    }

    pub fn established(
        &self,
        connection: ConnectionHandle,
    ) -> Result<EstablishedNegotiation<'_>, NegotiationManagerError> {
        let state = self
            .established
            .get(&connection)
            .ok_or(NegotiationManagerError::UnknownConnection)?;
        Ok(EstablishedNegotiation {
            connection,
            contract: &state.contract,
        })
    }

    pub fn terminate(
        &mut self,
        connection: ConnectionHandle,
    ) -> Result<ConnectionNegotiationTermination, NegotiationManagerError> {
        if self.attempts.remove(&connection).is_some() {
            self.reserved_bytes -= self.per_attempt_reservation;
            return Ok(ConnectionNegotiationTermination::NegotiationAborted);
        }

        if let Some(state) = self.established.remove(&connection) {
            self.reserved_bytes -= state.accounted_bytes;
            return Ok(ConnectionNegotiationTermination::EstablishedContractEnded);
        }

        Err(NegotiationManagerError::UnknownConnection)
    }

    pub const fn reserved_bytes(&self) -> usize {
        self.reserved_bytes
    }

    pub fn active_attempts(&self) -> usize {
        self.attempts.len()
    }

    pub fn established_connections(&self) -> usize {
        self.established.len()
    }

    fn attempt(
        &self,
        connection: ConnectionHandle,
    ) -> Result<&NegotiationAttempt, NegotiationManagerError> {
        if self.established.contains_key(&connection) {
            return Err(NegotiationManagerError::Negotiation(
                NegotiationError::AlreadyEstablished,
            ));
        }
        self.attempts
            .get(&connection)
            .ok_or(NegotiationManagerError::UnknownConnection)
    }

    fn attempt_mut(
        &mut self,
        connection: ConnectionHandle,
    ) -> Result<&mut NegotiationAttempt, NegotiationManagerError> {
        if self.established.contains_key(&connection) {
            return Err(NegotiationManagerError::Negotiation(
                NegotiationError::AlreadyEstablished,
            ));
        }
        self.attempts
            .get_mut(&connection)
            .ok_or(NegotiationManagerError::UnknownConnection)
    }

    fn promote_established(
        &mut self,
        connection: ConnectionHandle,
    ) -> Result<(), NegotiationManagerError> {
        let contract_bytes = self
            .attempts
            .get(&connection)
            .and_then(|attempt| attempt.proposal.as_ref())
            .ok_or(NegotiationManagerError::Negotiation(
                NegotiationError::NoProposal,
            ))?
            .accounted_bytes()
            .map_err(|_| {
                NegotiationManagerError::Negotiation(NegotiationError::SelectionTooLarge)
            })?;

        let attempt = self
            .attempts
            .remove(&connection)
            .ok_or(NegotiationManagerError::UnknownConnection)?;
        debug_assert_eq!(attempt.status(), NegotiationStatus::Established);
        let contract = attempt
            .proposal
            .ok_or(NegotiationManagerError::Negotiation(
                NegotiationError::NoProposal,
            ))?;

        self.reserved_bytes -= self.per_attempt_reservation;
        self.reserved_bytes += contract_bytes;
        let previous = self.established.insert(
            connection,
            EstablishedState {
                contract,
                accounted_bytes: contract_bytes,
            },
        );
        debug_assert!(previous.is_none());
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SemanticRegistrationOutcome {
    Inserted,
    AlreadyRegistered,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SemanticRegistrationError {
    ContradictoryRegistration,
    CapacityExceeded,
}

#[derive(Debug, Clone)]
pub struct SemanticRegistry<K, V> {
    max_entries: NonZeroUsize,
    entries: HashMap<K, V>,
}

impl<K, V> SemanticRegistry<K, V>
where
    K: Eq + Hash,
    V: Eq,
{
    pub fn new(max_entries: NonZeroUsize) -> Self {
        Self {
            max_entries,
            entries: HashMap::new(),
        }
    }

    pub fn register(
        &mut self,
        key: K,
        value: V,
    ) -> Result<SemanticRegistrationOutcome, SemanticRegistrationError> {
        let at_capacity = self.entries.len() >= self.max_entries.get();
        match self.entries.entry(key) {
            Entry::Occupied(entry) => {
                if entry.get() == &value {
                    Ok(SemanticRegistrationOutcome::AlreadyRegistered)
                } else {
                    Err(SemanticRegistrationError::ContradictoryRegistration)
                }
            }
            Entry::Vacant(entry) => {
                if at_capacity {
                    return Err(SemanticRegistrationError::CapacityExceeded);
                }
                entry.insert(value);
                Ok(SemanticRegistrationOutcome::Inserted)
            }
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn protocol(value: u128) -> ProtocolContract {
        ProtocolContract::new(ProtocolId::new(1), ProtocolRevision::new(value))
    }

    fn schema(id: u128, requirement: RequirementLevel, contract: u128, codec: u128) -> SchemaOffer {
        SchemaOffer::new(
            SchemaId::new(id),
            requirement,
            vec![SchemaContractOffer::new(
                SchemaContractId::new(contract),
                vec![CodecId::new(codec)],
            )],
        )
    }

    fn offer(
        protocol_contract: ProtocolContract,
        capability: Option<CapabilityOffer>,
        schema_offer: Option<SchemaOffer>,
    ) -> CompatibilityOffer {
        CompatibilityOffer::new(
            vec![protocol_contract],
            capability.into_iter().collect(),
            schema_offer.into_iter().collect(),
            None,
        )
    }

    fn common_offers() -> (CompatibilityOffer, CompatibilityOffer) {
        let capability = CapabilityId::new(7);
        let schema_id = SchemaId::new(9);
        (
            offer(
                protocol(1),
                Some(CapabilityOffer::new(capability, RequirementLevel::Optional)),
                Some(schema(schema_id.get(), RequirementLevel::Optional, 10, 11)),
            ),
            offer(
                protocol(1),
                Some(CapabilityOffer::new(capability, RequirementLevel::Optional)),
                Some(schema(schema_id.get(), RequirementLevel::Optional, 10, 11)),
            ),
        )
    }

    fn common_contract() -> NegotiatedContract {
        let mut contract = NegotiatedContract::new(protocol(1));
        contract.enable_capability(CapabilityId::new(7));
        contract
            .bind_schema(
                SchemaId::new(9),
                SelectedSchema::new(SchemaContractId::new(10), CodecId::new(11)),
            )
            .unwrap();
        contract
    }

    fn establish(manager: &mut NegotiationManager, connection: ConnectionHandle) {
        let (authority, peer) = common_offers();
        manager.start(connection, authority, peer).unwrap();
        manager
            .propose(
                connection,
                common_contract(),
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
    fn offer_validation_rejects_duplicate_and_empty_schema_entries() {
        let duplicate_protocol =
            CompatibilityOffer::new(vec![protocol(1), protocol(1)], vec![], vec![], None);
        assert_eq!(
            duplicate_protocol.validate(&OfferLimits::default()),
            Err(OfferValidationError::DuplicateProtocolAlternative)
        );

        let empty_schema = CompatibilityOffer::new(
            vec![protocol(1)],
            vec![],
            vec![SchemaOffer::new(
                SchemaId::new(1),
                RequirementLevel::Optional,
                vec![],
            )],
            None,
        );
        assert_eq!(
            empty_schema.validate(&OfferLimits::default()),
            Err(OfferValidationError::EmptySchemaContracts)
        );
    }

    #[test]
    fn offer_validation_enforces_resource_limits() {
        let limits = OfferLimits {
            max_protocols: 1,
            ..OfferLimits::default()
        };
        let too_many =
            CompatibilityOffer::new(vec![protocol(1), protocol(2)], vec![], vec![], None);
        assert_eq!(
            too_many.validate(&limits),
            Err(OfferValidationError::TooManyProtocolAlternatives)
        );
    }

    #[test]
    fn exact_protocol_and_required_contracts_are_enforced() {
        let required_capability = CapabilityId::new(2);
        let authority = offer(
            protocol(1),
            Some(CapabilityOffer::new(
                required_capability,
                RequirementLevel::Required,
            )),
            None,
        );
        let peer = offer(protocol(2), None, None);

        assert_eq!(
            NegotiationAttempt::new(authority.clone(), peer, OfferLimits::default()),
            Err(NegotiationError::ProtocolIncompatible)
        );

        let peer_same_protocol_without_capability = offer(protocol(1), None, None);
        assert_eq!(
            NegotiationAttempt::new(
                authority,
                peer_same_protocol_without_capability,
                OfferLimits::default(),
            ),
            Err(NegotiationError::RequiredCapabilityUnavailable(
                required_capability
            ))
        );
    }

    #[test]
    fn optional_unsupported_entries_may_be_omitted() {
        let authority = offer(
            protocol(1),
            Some(CapabilityOffer::new(
                CapabilityId::new(99),
                RequirementLevel::Optional,
            )),
            Some(schema(99, RequirementLevel::Optional, 100, 101)),
        );
        let peer = offer(protocol(1), None, None);
        let mut attempt = NegotiationAttempt::new(authority, peer, OfferLimits::default()).unwrap();
        attempt
            .propose(
                NegotiatedContract::new(protocol(1)),
                &NegotiationRequirements::default(),
            )
            .unwrap();
        assert_eq!(
            attempt.validate_authority().unwrap(),
            NegotiationStatus::AwaitingValidation {
                authority_validated: true,
                peer_validated: false
            }
        );
        assert_eq!(
            attempt.validate_peer().unwrap(),
            NegotiationStatus::Established
        );
    }

    #[test]
    fn validation_acknowledges_only_the_stored_proposal() {
        let (authority, peer) = common_offers();
        let mut attempt = NegotiationAttempt::new(authority, peer, OfferLimits::default()).unwrap();

        assert_eq!(
            attempt.validate_authority(),
            Err(NegotiationError::NoProposal)
        );
        attempt
            .propose(common_contract(), &NegotiationRequirements::default())
            .unwrap();
        assert_eq!(
            attempt.validate_authority().unwrap(),
            NegotiationStatus::AwaitingValidation {
                authority_validated: true,
                peer_validated: false
            }
        );
        assert_eq!(
            attempt.validate_peer().unwrap(),
            NegotiationStatus::Established
        );
    }

    #[test]
    fn imposed_requirements_cannot_be_negotiated_away() {
        let (authority, peer) = common_offers();
        let mut attempt = NegotiationAttempt::new(authority, peer, OfferLimits::default()).unwrap();
        let mut requirements = NegotiationRequirements::default();
        requirements.require_capability(CapabilityId::new(7));
        requirements.require_schema(SchemaId::new(9));

        assert_eq!(
            attempt.propose(NegotiatedContract::new(protocol(1)), &requirements),
            Err(NegotiationError::InvalidSelection)
        );
    }

    #[test]
    fn manager_bounds_concurrent_negotiation_state() {
        let offer_limits = OfferLimits::default();
        let reservation = offer_limits.max_attempt_reservation().unwrap();
        let manager_limits = NegotiationManagerLimits {
            max_concurrent_attempts: 2,
            max_aggregate_accounted_bytes: reservation,
        };
        let mut manager = NegotiationManager::new(offer_limits, manager_limits).unwrap();
        let (authority, peer) = common_offers();
        manager
            .start(ConnectionHandle::new(1), authority.clone(), peer.clone())
            .unwrap();
        assert_eq!(manager.active_attempts(), 1);
        assert_eq!(manager.reserved_bytes(), reservation);
        assert_eq!(
            manager.start(ConnectionHandle::new(2), authority, peer),
            Err(NegotiationManagerError::AggregateLimitExceeded)
        );
    }

    #[test]
    fn manager_also_bounds_attempt_count() {
        let offer_limits = OfferLimits::default();
        let reservation = offer_limits.max_attempt_reservation().unwrap();
        let manager_limits = NegotiationManagerLimits {
            max_concurrent_attempts: 1,
            max_aggregate_accounted_bytes: reservation * 2,
        };
        let mut manager = NegotiationManager::new(offer_limits, manager_limits).unwrap();
        let (authority, peer) = common_offers();
        manager
            .start(ConnectionHandle::new(1), authority.clone(), peer.clone())
            .unwrap();
        assert_eq!(
            manager.start(ConnectionHandle::new(2), authority, peer),
            Err(NegotiationManagerError::AttemptLimitExceeded)
        );
    }

    #[test]
    fn established_contract_remains_manager_owned_for_connection_lifetime() {
        let offer_limits = OfferLimits::default();
        let attempt_reservation = offer_limits.max_attempt_reservation().unwrap();
        let mut manager =
            NegotiationManager::new(offer_limits, NegotiationManagerLimits::default()).unwrap();
        let connection = ConnectionHandle::new(44);
        establish(&mut manager, connection);

        let established = manager.established(connection).unwrap();
        assert_eq!(established.connection(), connection);
        assert_eq!(established.contract(), &common_contract());
        assert_eq!(manager.active_attempts(), 0);
        assert_eq!(manager.established_connections(), 1);
        assert!(manager.reserved_bytes() < attempt_reservation);
        assert!(manager.reserved_bytes() > 0);
    }

    #[test]
    fn terminating_attempt_or_established_connection_releases_owned_state() {
        let offer_limits = OfferLimits::default();
        let reservation = offer_limits.max_attempt_reservation().unwrap();
        let mut manager =
            NegotiationManager::new(offer_limits, NegotiationManagerLimits::default()).unwrap();
        let (authority, peer) = common_offers();
        let attempt_connection = ConnectionHandle::new(1);
        manager.start(attempt_connection, authority, peer).unwrap();
        assert_eq!(manager.reserved_bytes(), reservation);
        assert_eq!(
            manager.terminate(attempt_connection).unwrap(),
            ConnectionNegotiationTermination::NegotiationAborted
        );
        assert_eq!(manager.reserved_bytes(), 0);

        let established_connection = ConnectionHandle::new(2);
        establish(&mut manager, established_connection);
        assert!(manager.reserved_bytes() > 0);
        assert_eq!(
            manager.terminate(established_connection).unwrap(),
            ConnectionNegotiationTermination::EstablishedContractEnded
        );
        assert_eq!(manager.reserved_bytes(), 0);
    }

    #[test]
    fn established_state_counts_against_aggregate_resource_budget() {
        let offer_limits = OfferLimits::default();
        let attempt_reservation = offer_limits.max_attempt_reservation().unwrap();
        let manager_limits = NegotiationManagerLimits {
            max_concurrent_attempts: 2,
            max_aggregate_accounted_bytes: attempt_reservation,
        };
        let mut manager = NegotiationManager::new(offer_limits, manager_limits).unwrap();
        let first = ConnectionHandle::new(1);
        establish(&mut manager, first);
        let established_bytes = manager.reserved_bytes();
        assert!(established_bytes > 0);

        let (authority, peer) = common_offers();
        assert_eq!(
            manager.start(ConnectionHandle::new(2), authority, peer),
            Err(NegotiationManagerError::AggregateLimitExceeded)
        );

        manager.terminate(first).unwrap();
        let (authority, peer) = common_offers();
        manager
            .start(ConnectionHandle::new(2), authority, peer)
            .unwrap();
    }

    #[test]
    fn contradictory_registration_is_rejected_but_equal_registration_is_idempotent() {
        let mut registry = SemanticRegistry::new(NonZeroUsize::new(2).unwrap());
        let id = SchemaId::new(5);
        assert_eq!(
            registry.register(id, "contract-a"),
            Ok(SemanticRegistrationOutcome::Inserted)
        );
        assert_eq!(
            registry.register(id, "contract-a"),
            Ok(SemanticRegistrationOutcome::AlreadyRegistered)
        );
        assert_eq!(
            registry.register(id, "contract-b"),
            Err(SemanticRegistrationError::ContradictoryRegistration)
        );
    }
}
