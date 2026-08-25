use runen_net::{
    identity::ConnectionHandle,
    protocol::{
        CapabilityId, CapabilityOffer, CodecId, CompatibilityOffer, NegotiatedContract,
        NegotiationError, NegotiationManager, NegotiationManagerError, NegotiationRequirements,
        NegotiationStatus, OfferLimits, ProtocolContract, ProtocolId, ProtocolRevision,
        RequirementLevel, SchemaContractId, SchemaContractOffer, SchemaId, SchemaOffer,
        SelectedSchema,
    },
};

use crate::{
    control::{ControlFrame, ControlFrameType, ProfileReadyConnection, SemanticRole},
    wire::{VarIntDecodeError, decode_varint, encode_varint},
};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum NegotiationOutcome {
    MalformedOffer,
    ProtocolIncompatible,
    RequiredCapabilityUnavailable,
    RequiredSchemaUnavailable,
    ResourceLimitExceeded,
    InvalidSelection,
}

impl NegotiationOutcome {
    const fn wire(self) -> u64 {
        match self {
            Self::MalformedOffer => 0,
            Self::ProtocolIncompatible => 1,
            Self::RequiredCapabilityUnavailable => 2,
            Self::RequiredSchemaUnavailable => 3,
            Self::ResourceLimitExceeded => 4,
            Self::InvalidSelection => 5,
        }
    }

    fn from_wire(value: u64) -> Result<Self, NegotiationWireError> {
        match value {
            0 => Ok(Self::MalformedOffer),
            1 => Ok(Self::ProtocolIncompatible),
            2 => Ok(Self::RequiredCapabilityUnavailable),
            3 => Ok(Self::RequiredSchemaUnavailable),
            4 => Ok(Self::ResourceLimitExceeded),
            5 => Ok(Self::InvalidSelection),
            value => Err(NegotiationWireError::UnknownFailureOutcome(value)),
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum NegotiationState {
    Ready,
    AwaitingPeerOffer,
    AwaitingAuthoritySelection,
    AwaitingProposal,
    AwaitingValidated,
    AwaitingEstablished,
    Established,
    Failed,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum NegotiationProgress {
    Waiting,
    AuthoritySelectionRequired,
    Send(ControlFrame),
    Established,
    RemoteFailed(NegotiationOutcome),
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum NegotiationControlError {
    ProfileProtocol(NegotiationProtocolError),
    LocalFailure {
        outcome: NegotiationOutcome,
        report: Option<ControlFrame>,
    },
    ManagerState(NegotiationManagerError),
    UnexpectedCoreStatus(NegotiationStatus),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum NegotiationProtocolError {
    UnexpectedFrame {
        state: NegotiationState,
        frame_type: ControlFrameType,
    },
    UnexpectedLocalOperation {
        state: NegotiationState,
    },
    WrongLocalRole {
        expected: SemanticRole,
        actual: SemanticRole,
    },
    NonEmptyAcknowledgement(ControlFrameType),
    Wire(NegotiationWireError),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum NegotiationWireError {
    VarInt(VarIntDecodeError),
    Truncated,
    TrailingBytes,
    UnknownRequirementLevel(u8),
    UnknownFailureOutcome(u64),
}

impl From<VarIntDecodeError> for NegotiationWireError {
    fn from(error: VarIntDecodeError) -> Self {
        Self::VarInt(error)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum BodyDecodeError {
    Protocol(NegotiationWireError),
    Failure(NegotiationOutcome),
}

impl From<NegotiationWireError> for BodyDecodeError {
    fn from(error: NegotiationWireError) -> Self {
        Self::Protocol(error)
    }
}

#[derive(Debug)]
pub(super) struct NegotiationExchange {
    connection: ConnectionHandle,
    role: SemanticRole,
    max_negotiation_frame_bytes: u64,
    state: NegotiationState,
    local_offer: Option<CompatibilityOffer>,
    manager_owned: bool,
}

impl NegotiationExchange {
    pub(super) fn from_profile(
        connection: ConnectionHandle,
        profile: &ProfileReadyConnection,
    ) -> Self {
        let local = profile.local_profile().local_settings();
        let peer = profile.peer_settings();
        Self {
            connection,
            role: local.semantic_role,
            max_negotiation_frame_bytes: local
                .max_negotiation_frame_bytes
                .min(peer.max_negotiation_frame_bytes),
            state: NegotiationState::Ready,
            local_offer: None,
            manager_owned: false,
        }
    }

    #[cfg(test)]
    fn for_test(
        connection: ConnectionHandle,
        role: SemanticRole,
        max_negotiation_frame_bytes: u64,
    ) -> Self {
        Self {
            connection,
            role,
            max_negotiation_frame_bytes,
            state: NegotiationState::Ready,
            local_offer: None,
            manager_owned: false,
        }
    }

    pub(super) const fn state(&self) -> NegotiationState {
        self.state
    }

    pub(super) fn prepare_offer(
        &mut self,
        manager: &mut NegotiationManager,
        offer: CompatibilityOffer,
    ) -> Result<ControlFrame, NegotiationControlError> {
        if self.state != NegotiationState::Ready {
            return self.fail_protocol(
                manager,
                NegotiationProtocolError::UnexpectedLocalOperation { state: self.state },
            );
        }

        let validated = match manager.validate_offer(offer) {
            Ok(validated) => validated,
            Err(_) => return self.fail_local(manager, NegotiationOutcome::MalformedOffer),
        };
        if validated.offer().diagnostic_label.is_some() {
            return self.fail_local(manager, NegotiationOutcome::MalformedOffer);
        }
        let body = match encode_offer(validated.offer(), self.max_negotiation_frame_bytes) {
            Ok(body) => body,
            Err(outcome) => return self.fail_local(manager, outcome),
        };

        self.local_offer = Some(validated.into_offer());
        self.state = NegotiationState::AwaitingPeerOffer;
        Ok(ControlFrame {
            frame_type: ControlFrameType::NegotiationOffer,
            body,
        })
    }

    pub(super) fn propose_authority(
        &mut self,
        manager: &mut NegotiationManager,
        contract: NegotiatedContract,
        requirements: &NegotiationRequirements,
    ) -> Result<ControlFrame, NegotiationControlError> {
        if self.role != SemanticRole::Authority {
            return self.fail_protocol(
                manager,
                NegotiationProtocolError::WrongLocalRole {
                    expected: SemanticRole::Authority,
                    actual: self.role,
                },
            );
        }
        if self.state != NegotiationState::AwaitingAuthoritySelection {
            return self.fail_protocol(
                manager,
                NegotiationProtocolError::UnexpectedLocalOperation { state: self.state },
            );
        }

        if let Err(error) = manager.propose(self.connection, contract, requirements) {
            return self.fail_from_manager(manager, error);
        }
        let status = match manager.validate_authority(self.connection) {
            Ok(status) => status,
            Err(error) => return Err(self.fail_manager_state(manager, error)),
        };
        let expected = NegotiationStatus::AwaitingValidation {
            authority_validated: true,
            peer_validated: false,
        };
        if status != expected {
            return self.fail_unexpected_status(manager, status);
        }

        let body = {
            let proposal = match manager.attempt_proposal(self.connection) {
                Ok(proposal) => proposal,
                Err(error) => return Err(self.fail_manager_state(manager, error)),
            };
            match encode_proposal(proposal, self.max_negotiation_frame_bytes) {
                Ok(body) => body,
                Err(outcome) => return self.fail_local(manager, outcome),
            }
        };

        self.state = NegotiationState::AwaitingValidated;
        Ok(ControlFrame {
            frame_type: ControlFrameType::NegotiationProposal,
            body,
        })
    }

    pub(super) fn receive(
        &mut self,
        manager: &mut NegotiationManager,
        requirements: &NegotiationRequirements,
        frame: ControlFrame,
    ) -> Result<NegotiationProgress, NegotiationControlError> {
        if frame.frame_type == ControlFrameType::NegotiationFailed
            && !matches!(
                self.state,
                NegotiationState::Established | NegotiationState::Failed
            )
        {
            return self.receive_failure(manager, frame.body);
        }

        match (self.state, frame.frame_type) {
            (NegotiationState::AwaitingPeerOffer, ControlFrameType::NegotiationOffer) => {
                self.receive_offer(manager, frame.body)
            }
            (NegotiationState::AwaitingProposal, ControlFrameType::NegotiationProposal) => {
                self.receive_proposal(manager, requirements, frame.body)
            }
            (NegotiationState::AwaitingValidated, ControlFrameType::NegotiationValidated) => {
                self.receive_validated(manager, frame.body)
            }
            (NegotiationState::AwaitingEstablished, ControlFrameType::NegotiationEstablished) => {
                self.receive_established(manager, frame.body)
            }
            _ => self.fail_protocol(
                manager,
                NegotiationProtocolError::UnexpectedFrame {
                    state: self.state,
                    frame_type: frame.frame_type,
                },
            ),
        }
    }

    pub(super) fn abort(
        &mut self,
        manager: &mut NegotiationManager,
    ) -> Result<(), NegotiationControlError> {
        self.local_offer = None;
        self.state = NegotiationState::Failed;
        self.release_manager(manager)
            .map_err(NegotiationControlError::ManagerState)
    }

    fn receive_offer(
        &mut self,
        manager: &mut NegotiationManager,
        body: Vec<u8>,
    ) -> Result<NegotiationProgress, NegotiationControlError> {
        let peer_offer = match decode_offer(&body, manager.offer_limits()) {
            Ok(offer) => offer,
            Err(error) => return self.fail_decode(manager, error),
        };
        let Some(local_offer) = self.local_offer.take() else {
            let state = self.state;
            self.state = NegotiationState::Failed;
            return Err(NegotiationControlError::ProfileProtocol(
                NegotiationProtocolError::UnexpectedLocalOperation { state },
            ));
        };

        let result = match self.role {
            SemanticRole::Authority => manager.start(self.connection, local_offer, peer_offer),
            SemanticRole::NonAuthority => manager.start(self.connection, peer_offer, local_offer),
        };
        if let Err(error) = result {
            return self.fail_from_manager(manager, error);
        }
        self.manager_owned = true;

        match self.role {
            SemanticRole::Authority => {
                self.state = NegotiationState::AwaitingAuthoritySelection;
                Ok(NegotiationProgress::AuthoritySelectionRequired)
            }
            SemanticRole::NonAuthority => {
                self.state = NegotiationState::AwaitingProposal;
                Ok(NegotiationProgress::Waiting)
            }
        }
    }

    fn receive_proposal(
        &mut self,
        manager: &mut NegotiationManager,
        requirements: &NegotiationRequirements,
        body: Vec<u8>,
    ) -> Result<NegotiationProgress, NegotiationControlError> {
        let contract = match decode_proposal(&body, manager.offer_limits()) {
            Ok(contract) => contract,
            Err(error) => return self.fail_decode(manager, error),
        };
        if let Err(error) = manager.propose(self.connection, contract, requirements) {
            return self.fail_from_manager(manager, error);
        }
        let status = match manager.validate_peer(self.connection) {
            Ok(status) => status,
            Err(error) => return Err(self.fail_manager_state(manager, error)),
        };
        let expected = NegotiationStatus::AwaitingValidation {
            authority_validated: false,
            peer_validated: true,
        };
        if status != expected {
            return self.fail_unexpected_status(manager, status);
        }

        self.state = NegotiationState::AwaitingEstablished;
        Ok(NegotiationProgress::Send(ControlFrame {
            frame_type: ControlFrameType::NegotiationValidated,
            body: Vec::new(),
        }))
    }

    fn receive_validated(
        &mut self,
        manager: &mut NegotiationManager,
        body: Vec<u8>,
    ) -> Result<NegotiationProgress, NegotiationControlError> {
        if !body.is_empty() {
            return self.fail_protocol(
                manager,
                NegotiationProtocolError::NonEmptyAcknowledgement(
                    ControlFrameType::NegotiationValidated,
                ),
            );
        }
        let status = match manager.validate_peer(self.connection) {
            Ok(status) => status,
            Err(error) => return Err(self.fail_manager_state(manager, error)),
        };
        if status != NegotiationStatus::Established {
            return self.fail_unexpected_status(manager, status);
        }
        self.state = NegotiationState::Established;
        Ok(NegotiationProgress::Send(ControlFrame {
            frame_type: ControlFrameType::NegotiationEstablished,
            body: Vec::new(),
        }))
    }

    fn receive_established(
        &mut self,
        manager: &mut NegotiationManager,
        body: Vec<u8>,
    ) -> Result<NegotiationProgress, NegotiationControlError> {
        if !body.is_empty() {
            return self.fail_protocol(
                manager,
                NegotiationProtocolError::NonEmptyAcknowledgement(
                    ControlFrameType::NegotiationEstablished,
                ),
            );
        }
        let status = match manager.validate_authority(self.connection) {
            Ok(status) => status,
            Err(error) => return Err(self.fail_manager_state(manager, error)),
        };
        if status != NegotiationStatus::Established {
            return self.fail_unexpected_status(manager, status);
        }
        self.state = NegotiationState::Established;
        Ok(NegotiationProgress::Established)
    }

    fn receive_failure(
        &mut self,
        manager: &mut NegotiationManager,
        body: Vec<u8>,
    ) -> Result<NegotiationProgress, NegotiationControlError> {
        let outcome = match decode_failure(&body) {
            Ok(outcome) => outcome,
            Err(error) => {
                return self.fail_protocol(manager, NegotiationProtocolError::Wire(error));
            }
        };
        self.local_offer = None;
        self.state = NegotiationState::Failed;
        self.release_manager(manager)
            .map_err(NegotiationControlError::ManagerState)?;
        Ok(NegotiationProgress::RemoteFailed(outcome))
    }

    fn fail_decode<T>(
        &mut self,
        manager: &mut NegotiationManager,
        error: BodyDecodeError,
    ) -> Result<T, NegotiationControlError> {
        match error {
            BodyDecodeError::Protocol(error) => {
                self.fail_protocol(manager, NegotiationProtocolError::Wire(error))
            }
            BodyDecodeError::Failure(outcome) => self.fail_local(manager, outcome),
        }
    }

    fn fail_from_manager<T>(
        &mut self,
        manager: &mut NegotiationManager,
        error: NegotiationManagerError,
    ) -> Result<T, NegotiationControlError> {
        match manager_failure_outcome(error) {
            Ok(outcome) => self.fail_local(manager, outcome),
            Err(error) => Err(self.fail_manager_state(manager, error)),
        }
    }

    fn fail_local<T>(
        &mut self,
        manager: &mut NegotiationManager,
        outcome: NegotiationOutcome,
    ) -> Result<T, NegotiationControlError> {
        self.local_offer = None;
        self.state = NegotiationState::Failed;
        self.release_manager(manager)
            .map_err(NegotiationControlError::ManagerState)?;
        Err(NegotiationControlError::LocalFailure {
            outcome,
            report: failure_frame(outcome),
        })
    }

    fn fail_protocol<T>(
        &mut self,
        manager: &mut NegotiationManager,
        error: NegotiationProtocolError,
    ) -> Result<T, NegotiationControlError> {
        self.local_offer = None;
        self.state = NegotiationState::Failed;
        self.release_manager(manager)
            .map_err(NegotiationControlError::ManagerState)?;
        Err(NegotiationControlError::ProfileProtocol(error))
    }

    fn fail_unexpected_status<T>(
        &mut self,
        manager: &mut NegotiationManager,
        status: NegotiationStatus,
    ) -> Result<T, NegotiationControlError> {
        self.local_offer = None;
        self.state = NegotiationState::Failed;
        self.release_manager(manager)
            .map_err(NegotiationControlError::ManagerState)?;
        Err(NegotiationControlError::UnexpectedCoreStatus(status))
    }

    fn fail_manager_state(
        &mut self,
        manager: &mut NegotiationManager,
        error: NegotiationManagerError,
    ) -> NegotiationControlError {
        self.local_offer = None;
        self.state = NegotiationState::Failed;
        if let Err(cleanup_error) = self.release_manager(manager) {
            return NegotiationControlError::ManagerState(cleanup_error);
        }
        NegotiationControlError::ManagerState(error)
    }

    fn release_manager(
        &mut self,
        manager: &mut NegotiationManager,
    ) -> Result<(), NegotiationManagerError> {
        if !self.manager_owned {
            return Ok(());
        }
        self.manager_owned = false;
        manager.terminate(self.connection).map(|_| ())
    }
}

fn manager_failure_outcome(
    error: NegotiationManagerError,
) -> Result<NegotiationOutcome, NegotiationManagerError> {
    match error {
        NegotiationManagerError::AttemptLimitExceeded
        | NegotiationManagerError::AggregateLimitExceeded => {
            Ok(NegotiationOutcome::ResourceLimitExceeded)
        }
        NegotiationManagerError::Negotiation(error) => match error {
            NegotiationError::AuthorityOfferInvalid(_) | NegotiationError::PeerOfferInvalid(_) => {
                Ok(NegotiationOutcome::MalformedOffer)
            }
            NegotiationError::ProtocolIncompatible => Ok(NegotiationOutcome::ProtocolIncompatible),
            NegotiationError::RequiredCapabilityUnavailable(_) => {
                Ok(NegotiationOutcome::RequiredCapabilityUnavailable)
            }
            NegotiationError::RequiredSchemaUnavailable(_) => {
                Ok(NegotiationOutcome::RequiredSchemaUnavailable)
            }
            NegotiationError::SelectionTooLarge => Ok(NegotiationOutcome::ResourceLimitExceeded),
            NegotiationError::InvalidSelection => Ok(NegotiationOutcome::InvalidSelection),
            NegotiationError::AlreadyProposed
            | NegotiationError::NoProposal
            | NegotiationError::AlreadyEstablished => Err(error.into()),
        },
        NegotiationManagerError::ConnectionAlreadyKnown
        | NegotiationManagerError::UnknownConnection => Err(error),
    }
}

fn failure_frame(outcome: NegotiationOutcome) -> Option<ControlFrame> {
    let mut body = Vec::new();
    body.try_reserve_exact(1).ok()?;
    body.push(outcome.wire() as u8);
    Some(ControlFrame {
        frame_type: ControlFrameType::NegotiationFailed,
        body,
    })
}

fn encode_offer(
    offer: &CompatibilityOffer,
    max_body_bytes: u64,
) -> Result<Vec<u8>, NegotiationOutcome> {
    if offer.diagnostic_label.is_some() {
        return Err(NegotiationOutcome::MalformedOffer);
    }
    let body_len = offer_body_len(offer).ok_or(NegotiationOutcome::ResourceLimitExceeded)?;
    ensure_body_fits(body_len, max_body_bytes)?;
    let mut writer = BodyWriter::new(body_len)?;

    writer.push_count(offer.protocols.len())?;
    for protocol in &offer.protocols {
        writer.push_id(protocol.id.get());
        writer.push_id(protocol.revision.get());
    }

    writer.push_count(offer.capabilities.len())?;
    for capability in &offer.capabilities {
        writer.push_id(capability.id.get());
        writer.push_byte(encode_requirement(capability.requirement));
    }

    writer.push_count(offer.schemas.len())?;
    for schema in &offer.schemas {
        writer.push_id(schema.id.get());
        writer.push_byte(encode_requirement(schema.requirement));
        writer.push_count(schema.contracts.len())?;
        for contract in &schema.contracts {
            writer.push_id(contract.contract_id.get());
            writer.push_count(contract.codecs.len())?;
            for codec in &contract.codecs {
                writer.push_id(codec.get());
            }
        }
    }

    Ok(writer.finish())
}

fn decode_offer(input: &[u8], limits: OfferLimits) -> Result<CompatibilityOffer, BodyDecodeError> {
    let mut reader = BodyReader::new(input);
    let mut budget = DecodeBudget::new(
        limits.max_offer_accounted_bytes,
        NegotiationOutcome::MalformedOffer,
    );

    let protocol_count =
        reader.read_bounded_count(limits.max_protocols, NegotiationOutcome::MalformedOffer)?;
    budget.charge_items(protocol_count, 32)?;
    let mut protocols = reserve_vec(protocol_count)?;
    for _ in 0..protocol_count {
        protocols.push(ProtocolContract::new(
            ProtocolId::new(reader.read_id()?),
            ProtocolRevision::new(reader.read_id()?),
        ));
    }

    let capability_count =
        reader.read_bounded_count(limits.max_capabilities, NegotiationOutcome::MalformedOffer)?;
    budget.charge_items(capability_count, 17)?;
    let mut capabilities = reserve_vec(capability_count)?;
    for _ in 0..capability_count {
        capabilities.push(CapabilityOffer::new(
            CapabilityId::new(reader.read_id()?),
            reader.read_requirement()?,
        ));
    }

    let schema_count =
        reader.read_bounded_count(limits.max_schemas, NegotiationOutcome::MalformedOffer)?;
    budget.charge_items(schema_count, 17)?;
    let mut schemas = reserve_vec(schema_count)?;
    for _ in 0..schema_count {
        let id = SchemaId::new(reader.read_id()?);
        let requirement = reader.read_requirement()?;
        let contract_count = reader.read_bounded_count(
            limits.max_contracts_per_schema,
            NegotiationOutcome::MalformedOffer,
        )?;
        budget.charge_items(contract_count, 16)?;
        let mut contracts = reserve_vec(contract_count)?;
        for _ in 0..contract_count {
            let contract_id = SchemaContractId::new(reader.read_id()?);
            let codec_count = reader.read_bounded_count(
                limits.max_codecs_per_contract,
                NegotiationOutcome::MalformedOffer,
            )?;
            budget.charge_items(codec_count, 16)?;
            let mut codecs = reserve_vec(codec_count)?;
            for _ in 0..codec_count {
                codecs.push(CodecId::new(reader.read_id()?));
            }
            contracts.push(SchemaContractOffer::new(contract_id, codecs));
        }
        schemas.push(SchemaOffer::new(id, requirement, contracts));
    }

    reader.finish()?;
    Ok(CompatibilityOffer::new(
        protocols,
        capabilities,
        schemas,
        None,
    ))
}

fn encode_proposal(
    contract: &NegotiatedContract,
    max_body_bytes: u64,
) -> Result<Vec<u8>, NegotiationOutcome> {
    let body_len = proposal_body_len(contract).ok_or(NegotiationOutcome::ResourceLimitExceeded)?;
    ensure_body_fits(body_len, max_body_bytes)?;
    let mut writer = BodyWriter::new(body_len)?;

    writer.push_id(contract.protocol().id.get());
    writer.push_id(contract.protocol().revision.get());
    writer.push_count(contract.capability_count())?;
    for capability in contract.enabled_capabilities() {
        writer.push_id(capability.get());
    }
    writer.push_count(contract.schema_count())?;
    for (schema_id, selected) in contract.selected_schemas() {
        writer.push_id(schema_id.get());
        writer.push_id(selected.contract_id.get());
        writer.push_id(selected.codec_id.get());
    }

    Ok(writer.finish())
}

fn decode_proposal(
    input: &[u8],
    limits: OfferLimits,
) -> Result<NegotiatedContract, BodyDecodeError> {
    let mut reader = BodyReader::new(input);
    let mut budget = DecodeBudget::new(
        limits.max_offer_accounted_bytes,
        NegotiationOutcome::ResourceLimitExceeded,
    );
    let protocol = ProtocolContract::new(
        ProtocolId::new(reader.read_id()?),
        ProtocolRevision::new(reader.read_id()?),
    );
    budget.charge_bytes(32)?;

    let capability_count = reader.read_bounded_count(
        limits.max_capabilities,
        NegotiationOutcome::ResourceLimitExceeded,
    )?;
    budget.charge_items(capability_count, 16)?;
    let mut capabilities = reserve_vec(capability_count)?;
    for _ in 0..capability_count {
        let capability = CapabilityId::new(reader.read_id()?);
        if capabilities.contains(&capability) {
            return Err(BodyDecodeError::Failure(
                NegotiationOutcome::InvalidSelection,
            ));
        }
        capabilities.push(capability);
    }

    let schema_count = reader.read_bounded_count(
        limits.max_schemas,
        NegotiationOutcome::ResourceLimitExceeded,
    )?;
    budget.charge_items(schema_count, 48)?;
    let mut schemas = reserve_vec(schema_count)?;
    for _ in 0..schema_count {
        let schema_id = SchemaId::new(reader.read_id()?);
        if schemas
            .iter()
            .any(|(existing_id, _): &(SchemaId, SelectedSchema)| *existing_id == schema_id)
        {
            return Err(BodyDecodeError::Failure(
                NegotiationOutcome::InvalidSelection,
            ));
        }
        let selected = SelectedSchema::new(
            SchemaContractId::new(reader.read_id()?),
            CodecId::new(reader.read_id()?),
        );
        schemas.push((schema_id, selected));
    }
    reader.finish()?;

    let mut contract = NegotiatedContract::new(protocol);
    for capability in capabilities {
        let inserted = contract.enable_capability(capability);
        debug_assert!(inserted);
    }
    for (schema_id, selected) in schemas {
        contract
            .bind_schema(schema_id, selected)
            .expect("duplicate schema bindings were rejected during bounded decode");
    }
    Ok(contract)
}

fn decode_failure(input: &[u8]) -> Result<NegotiationOutcome, NegotiationWireError> {
    let mut reader = BodyReader::new(input);
    let outcome = NegotiationOutcome::from_wire(reader.read_varint()?)?;
    reader.finish()?;
    Ok(outcome)
}

fn ensure_body_fits(body_len: usize, max_body_bytes: u64) -> Result<(), NegotiationOutcome> {
    let body_len =
        u64::try_from(body_len).map_err(|_| NegotiationOutcome::ResourceLimitExceeded)?;
    if body_len > max_body_bytes {
        Err(NegotiationOutcome::ResourceLimitExceeded)
    } else {
        Ok(())
    }
}

fn offer_body_len(offer: &CompatibilityOffer) -> Option<usize> {
    let mut total = count_len(offer.protocols.len())?;
    total = checked_add_mul(total, offer.protocols.len(), 32)?;
    total = total.checked_add(count_len(offer.capabilities.len())?)?;
    total = checked_add_mul(total, offer.capabilities.len(), 17)?;
    total = total.checked_add(count_len(offer.schemas.len())?)?;
    for schema in &offer.schemas {
        total = total.checked_add(17)?;
        total = total.checked_add(count_len(schema.contracts.len())?)?;
        for contract in &schema.contracts {
            total = total.checked_add(16)?;
            total = total.checked_add(count_len(contract.codecs.len())?)?;
            total = checked_add_mul(total, contract.codecs.len(), 16)?;
        }
    }
    Some(total)
}

fn proposal_body_len(contract: &NegotiatedContract) -> Option<usize> {
    let mut total = 32usize;
    total = total.checked_add(count_len(contract.capability_count())?)?;
    total = checked_add_mul(total, contract.capability_count(), 16)?;
    total = total.checked_add(count_len(contract.schema_count())?)?;
    checked_add_mul(total, contract.schema_count(), 48)
}

fn count_len(count: usize) -> Option<usize> {
    let value = u64::try_from(count).ok()?;
    encode_varint(value).ok().map(|encoded| encoded.len())
}

fn checked_add_mul(total: usize, count: usize, width: usize) -> Option<usize> {
    count
        .checked_mul(width)
        .and_then(|bytes| total.checked_add(bytes))
}

struct DecodeBudget {
    used: usize,
    limit: usize,
    exceeded: NegotiationOutcome,
}

impl DecodeBudget {
    const fn new(limit: usize, exceeded: NegotiationOutcome) -> Self {
        Self {
            used: 0,
            limit,
            exceeded,
        }
    }

    fn charge_bytes(&mut self, bytes: usize) -> Result<(), BodyDecodeError> {
        let next = self
            .used
            .checked_add(bytes)
            .ok_or(BodyDecodeError::Failure(self.exceeded))?;
        if next > self.limit {
            return Err(BodyDecodeError::Failure(self.exceeded));
        }
        self.used = next;
        Ok(())
    }

    fn charge_items(&mut self, count: usize, width: usize) -> Result<(), BodyDecodeError> {
        let bytes = count
            .checked_mul(width)
            .ok_or(BodyDecodeError::Failure(self.exceeded))?;
        self.charge_bytes(bytes)
    }
}

fn reserve_vec<T>(count: usize) -> Result<Vec<T>, BodyDecodeError> {
    let mut values = Vec::new();
    values
        .try_reserve_exact(count)
        .map_err(|_| BodyDecodeError::Failure(NegotiationOutcome::ResourceLimitExceeded))?;
    Ok(values)
}

const fn encode_requirement(requirement: RequirementLevel) -> u8 {
    match requirement {
        RequirementLevel::Optional => 0,
        RequirementLevel::Required => 1,
    }
}

fn decode_requirement(value: u8) -> Result<RequirementLevel, NegotiationWireError> {
    match value {
        0 => Ok(RequirementLevel::Optional),
        1 => Ok(RequirementLevel::Required),
        value => Err(NegotiationWireError::UnknownRequirementLevel(value)),
    }
}

struct BodyWriter {
    body: Vec<u8>,
    expected_len: usize,
}

impl BodyWriter {
    fn new(expected_len: usize) -> Result<Self, NegotiationOutcome> {
        let mut body = Vec::new();
        body.try_reserve_exact(expected_len)
            .map_err(|_| NegotiationOutcome::ResourceLimitExceeded)?;
        Ok(Self { body, expected_len })
    }

    fn push_byte(&mut self, value: u8) {
        self.body.push(value);
    }

    fn push_id(&mut self, value: u128) {
        self.body.extend_from_slice(&value.to_be_bytes());
    }

    fn push_count(&mut self, count: usize) -> Result<(), NegotiationOutcome> {
        let value = u64::try_from(count).map_err(|_| NegotiationOutcome::ResourceLimitExceeded)?;
        let encoded =
            encode_varint(value).map_err(|_| NegotiationOutcome::ResourceLimitExceeded)?;
        self.body.extend_from_slice(encoded.as_slice());
        Ok(())
    }

    fn finish(self) -> Vec<u8> {
        debug_assert_eq!(self.body.len(), self.expected_len);
        self.body
    }
}

struct BodyReader<'a> {
    input: &'a [u8],
    offset: usize,
}

impl<'a> BodyReader<'a> {
    const fn new(input: &'a [u8]) -> Self {
        Self { input, offset: 0 }
    }

    fn read_byte(&mut self) -> Result<u8, NegotiationWireError> {
        let value = *self
            .input
            .get(self.offset)
            .ok_or(NegotiationWireError::Truncated)?;
        self.offset += 1;
        Ok(value)
    }

    fn read_id(&mut self) -> Result<u128, NegotiationWireError> {
        let end = self
            .offset
            .checked_add(16)
            .ok_or(NegotiationWireError::Truncated)?;
        let bytes = self
            .input
            .get(self.offset..end)
            .ok_or(NegotiationWireError::Truncated)?;
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| NegotiationWireError::Truncated)?;
        self.offset = end;
        Ok(u128::from_be_bytes(bytes))
    }

    fn read_varint(&mut self) -> Result<u64, NegotiationWireError> {
        let input = self
            .input
            .get(self.offset..)
            .ok_or(NegotiationWireError::Truncated)?;
        let (value, consumed) = decode_varint(input)?;
        self.offset += consumed;
        Ok(value)
    }

    fn read_requirement(&mut self) -> Result<RequirementLevel, NegotiationWireError> {
        decode_requirement(self.read_byte()?)
    }

    fn read_bounded_count(
        &mut self,
        limit: usize,
        exceeded: NegotiationOutcome,
    ) -> Result<usize, BodyDecodeError> {
        let count = self.read_varint()?;
        let limit = u64::try_from(limit).unwrap_or(u64::MAX);
        if count > limit {
            return Err(BodyDecodeError::Failure(exceeded));
        }
        usize::try_from(count).map_err(|_| BodyDecodeError::Failure(exceeded))
    }

    fn finish(self) -> Result<(), NegotiationWireError> {
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err(NegotiationWireError::TrailingBytes)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use runen_net::protocol::NegotiationManagerLimits;

    const MAX_FRAME: u64 = 64 * 1024;

    fn protocol(revision: u128) -> ProtocolContract {
        ProtocolContract::new(ProtocolId::new(1), ProtocolRevision::new(revision))
    }

    fn schema(id: u128, requirement: RequirementLevel) -> SchemaOffer {
        SchemaOffer::new(
            SchemaId::new(id),
            requirement,
            vec![SchemaContractOffer::new(
                SchemaContractId::new(id + 1),
                vec![CodecId::new(id + 2)],
            )],
        )
    }

    fn offer(label: Option<&str>) -> CompatibilityOffer {
        CompatibilityOffer::new(
            vec![protocol(1), protocol(2)],
            vec![CapabilityOffer::new(
                CapabilityId::new(7),
                RequirementLevel::Optional,
            )],
            vec![schema(9, RequirementLevel::Optional)],
            label.map(str::to_owned),
        )
    }

    fn compact_offer() -> CompatibilityOffer {
        CompatibilityOffer::new(vec![protocol(1)], vec![], vec![], None)
    }

    fn contract() -> NegotiatedContract {
        let mut contract = NegotiatedContract::new(protocol(1));
        assert!(contract.enable_capability(CapabilityId::new(7)));
        contract
            .bind_schema(
                SchemaId::new(9),
                SelectedSchema::new(SchemaContractId::new(10), CodecId::new(11)),
            )
            .unwrap();
        contract
    }

    fn new_manager() -> NegotiationManager {
        NegotiationManager::new(OfferLimits::default(), NegotiationManagerLimits::default())
            .unwrap()
    }

    fn exchange(role: SemanticRole, connection: ConnectionHandle) -> NegotiationExchange {
        NegotiationExchange::for_test(connection, role, MAX_FRAME)
    }

    fn frame(frame_type: ControlFrameType, body: Vec<u8>) -> ControlFrame {
        ControlFrame { frame_type, body }
    }

    #[test]
    fn offer_codec_round_trips_exact_identity_bytes_without_labels() {
        let original = offer(None);
        let body = encode_offer(&original, MAX_FRAME).unwrap();
        assert_eq!(body[0], 2);
        assert_eq!(&body[1..17], &ProtocolId::new(1).get().to_be_bytes());
        assert_eq!(&body[17..33], &ProtocolRevision::new(1).get().to_be_bytes());
        assert_eq!(
            decode_offer(&body, OfferLimits::default()).unwrap(),
            original
        );
    }

    #[test]
    fn proposal_codec_round_trips_sets_without_order_contract() {
        let mut original = contract();
        assert!(original.enable_capability(CapabilityId::new(8)));
        original
            .bind_schema(
                SchemaId::new(12),
                SelectedSchema::new(SchemaContractId::new(13), CodecId::new(14)),
            )
            .unwrap();

        let body = encode_proposal(&original, MAX_FRAME).unwrap();
        let decoded = decode_proposal(&body, OfferLimits::default()).unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn offer_count_limits_are_checked_before_entry_bytes_are_read() {
        let limits = OfferLimits {
            max_protocols: 1,
            ..OfferLimits::default()
        };
        assert_eq!(
            decode_offer(&[2], limits),
            Err(BodyDecodeError::Failure(NegotiationOutcome::MalformedOffer))
        );
    }

    #[test]
    fn offer_accounted_budget_is_checked_before_entry_bytes_are_read() {
        let limits = OfferLimits {
            max_offer_accounted_bytes: 31,
            ..OfferLimits::default()
        };
        assert_eq!(
            decode_offer(&[1], limits),
            Err(BodyDecodeError::Failure(NegotiationOutcome::MalformedOffer))
        );
    }

    #[test]
    fn proposal_limits_are_checked_before_selected_entry_bytes_are_read() {
        let mut body = Vec::new();
        body.extend_from_slice(&1u128.to_be_bytes());
        body.extend_from_slice(&1u128.to_be_bytes());
        body.push(2);
        let limits = OfferLimits {
            max_capabilities: 1,
            ..OfferLimits::default()
        };
        assert_eq!(
            decode_proposal(&body, limits),
            Err(BodyDecodeError::Failure(
                NegotiationOutcome::ResourceLimitExceeded
            ))
        );

        let mut body = Vec::new();
        body.extend_from_slice(&1u128.to_be_bytes());
        body.extend_from_slice(&1u128.to_be_bytes());
        body.push(0);
        body.push(0);
        let limits = OfferLimits {
            max_offer_accounted_bytes: 31,
            ..OfferLimits::default()
        };
        assert_eq!(
            decode_proposal(&body, limits),
            Err(BodyDecodeError::Failure(
                NegotiationOutcome::ResourceLimitExceeded
            ))
        );
    }

    #[test]
    fn offer_wire_rejects_non_minimal_truncated_trailing_and_unknown_requirement() {
        assert_eq!(
            decode_offer(&[0x40, 0x01], OfferLimits::default()),
            Err(BodyDecodeError::Protocol(NegotiationWireError::VarInt(
                VarIntDecodeError::NonMinimal
            )))
        );
        assert_eq!(
            decode_offer(&[1], OfferLimits::default()),
            Err(BodyDecodeError::Protocol(NegotiationWireError::Truncated))
        );

        let mut valid = encode_offer(&compact_offer(), MAX_FRAME).unwrap();
        valid.push(0);
        assert_eq!(
            decode_offer(&valid, OfferLimits::default()),
            Err(BodyDecodeError::Protocol(
                NegotiationWireError::TrailingBytes
            ))
        );

        let mut unknown_requirement = Vec::new();
        unknown_requirement.push(1);
        unknown_requirement.extend_from_slice(&1u128.to_be_bytes());
        unknown_requirement.extend_from_slice(&1u128.to_be_bytes());
        unknown_requirement.push(1);
        unknown_requirement.extend_from_slice(&7u128.to_be_bytes());
        unknown_requirement.push(2);
        unknown_requirement.push(0);
        assert_eq!(
            decode_offer(&unknown_requirement, OfferLimits::default()),
            Err(BodyDecodeError::Protocol(
                NegotiationWireError::UnknownRequirementLevel(2)
            ))
        );
    }

    #[test]
    fn proposal_decoder_rejects_duplicate_selection_without_normalization() {
        let mut body = Vec::new();
        body.extend_from_slice(&1u128.to_be_bytes());
        body.extend_from_slice(&1u128.to_be_bytes());
        body.push(2);
        body.extend_from_slice(&7u128.to_be_bytes());
        body.extend_from_slice(&7u128.to_be_bytes());
        body.push(0);
        assert_eq!(
            decode_proposal(&body, OfferLimits::default()),
            Err(BodyDecodeError::Failure(
                NegotiationOutcome::InvalidSelection
            ))
        );

        let mut body = Vec::new();
        body.extend_from_slice(&1u128.to_be_bytes());
        body.extend_from_slice(&1u128.to_be_bytes());
        body.push(0);
        body.push(2);
        for contract_id in [10u128, 12u128] {
            body.extend_from_slice(&9u128.to_be_bytes());
            body.extend_from_slice(&contract_id.to_be_bytes());
            body.extend_from_slice(&(contract_id + 1).to_be_bytes());
        }
        assert_eq!(
            decode_proposal(&body, OfferLimits::default()),
            Err(BodyDecodeError::Failure(
                NegotiationOutcome::InvalidSelection
            ))
        );
    }

    #[test]
    fn failure_body_is_exact_and_unknown_or_trailing_values_are_protocol_errors() {
        assert_eq!(
            decode_failure(&[NegotiationOutcome::InvalidSelection.wire() as u8]).unwrap(),
            NegotiationOutcome::InvalidSelection
        );
        assert_eq!(
            decode_failure(&[6]),
            Err(NegotiationWireError::UnknownFailureOutcome(6))
        );
        assert_eq!(
            decode_failure(&[0x40, 0x01]),
            Err(NegotiationWireError::VarInt(VarIntDecodeError::NonMinimal))
        );
        assert_eq!(
            decode_failure(&[0, 0]),
            Err(NegotiationWireError::TrailingBytes)
        );
    }

    #[test]
    fn local_diagnostic_label_and_frame_ceiling_fail_before_manager_reservation() {
        let connection = ConnectionHandle::new(1);
        let mut manager = new_manager();
        let mut exchange = exchange(SemanticRole::Authority, connection);
        assert!(matches!(
            exchange.prepare_offer(&mut manager, offer(Some("local-only"))),
            Err(NegotiationControlError::LocalFailure {
                outcome: NegotiationOutcome::MalformedOffer,
                ..
            })
        ));
        assert_eq!(manager.reserved_bytes(), 0);

        let mut exchange = NegotiationExchange::for_test(connection, SemanticRole::Authority, 34);
        assert!(matches!(
            exchange.prepare_offer(&mut manager, compact_offer()),
            Err(NegotiationControlError::LocalFailure {
                outcome: NegotiationOutcome::ResourceLimitExceeded,
                ..
            })
        ));
        assert_eq!(manager.reserved_bytes(), 0);
    }

    #[test]
    fn malformed_peer_offer_reaches_core_and_is_not_normalized() {
        let connection = ConnectionHandle::new(2);
        let mut manager = new_manager();
        let mut exchange = exchange(SemanticRole::Authority, connection);
        exchange
            .prepare_offer(&mut manager, compact_offer())
            .unwrap();

        let malformed =
            CompatibilityOffer::new(vec![protocol(1), protocol(1)], vec![], vec![], None);
        let body = encode_offer(&malformed, MAX_FRAME).unwrap();
        assert!(matches!(
            exchange.receive(
                &mut manager,
                &NegotiationRequirements::default(),
                frame(ControlFrameType::NegotiationOffer, body)
            ),
            Err(NegotiationControlError::LocalFailure {
                outcome: NegotiationOutcome::MalformedOffer,
                ..
            })
        ));
        assert_eq!(manager.reserved_bytes(), 0);
    }

    #[test]
    fn empty_peer_offer_structures_reach_core_and_are_not_normalized() {
        let malformed_offers = [
            CompatibilityOffer::new(vec![], vec![], vec![], None),
            CompatibilityOffer::new(
                vec![protocol(1)],
                vec![],
                vec![SchemaOffer::new(
                    SchemaId::new(9),
                    RequirementLevel::Optional,
                    vec![],
                )],
                None,
            ),
            CompatibilityOffer::new(
                vec![protocol(1)],
                vec![],
                vec![SchemaOffer::new(
                    SchemaId::new(9),
                    RequirementLevel::Optional,
                    vec![SchemaContractOffer::new(SchemaContractId::new(10), vec![])],
                )],
                None,
            ),
        ];

        for (index, malformed) in malformed_offers.into_iter().enumerate() {
            let connection = ConnectionHandle::new(20 + index as u64);
            let mut manager = new_manager();
            let mut exchange = exchange(SemanticRole::Authority, connection);
            exchange
                .prepare_offer(&mut manager, compact_offer())
                .unwrap();
            let body = encode_offer(&malformed, MAX_FRAME).unwrap();
            assert!(matches!(
                exchange.receive(
                    &mut manager,
                    &NegotiationRequirements::default(),
                    frame(ControlFrameType::NegotiationOffer, body)
                ),
                Err(NegotiationControlError::LocalFailure {
                    outcome: NegotiationOutcome::MalformedOffer,
                    ..
                })
            ));
            assert_eq!(manager.reserved_bytes(), 0);
        }
    }

    #[test]
    fn manager_pressure_maps_to_resource_failure_without_new_reservation() {
        let offer_limits = OfferLimits::default();
        let reservation = offer_limits.max_offer_accounted_bytes * 3;
        let mut manager = NegotiationManager::new(
            offer_limits,
            NegotiationManagerLimits {
                max_concurrent_attempts: 1,
                max_aggregate_accounted_bytes: reservation * 2,
            },
        )
        .unwrap();
        manager
            .start(ConnectionHandle::new(99), compact_offer(), compact_offer())
            .unwrap();
        let before = manager.reserved_bytes();

        let connection = ConnectionHandle::new(3);
        let mut exchange = exchange(SemanticRole::Authority, connection);
        exchange
            .prepare_offer(&mut manager, compact_offer())
            .unwrap();
        let body = encode_offer(&compact_offer(), MAX_FRAME).unwrap();
        assert!(matches!(
            exchange.receive(
                &mut manager,
                &NegotiationRequirements::default(),
                frame(ControlFrameType::NegotiationOffer, body)
            ),
            Err(NegotiationControlError::LocalFailure {
                outcome: NegotiationOutcome::ResourceLimitExceeded,
                ..
            })
        ));
        assert_eq!(manager.reserved_bytes(), before);
    }

    #[test]
    fn authority_and_non_authority_reach_the_same_established_contract() {
        let connection = ConnectionHandle::new(4);
        let mut authority_manager = new_manager();
        let mut peer_manager = new_manager();
        let mut authority = exchange(SemanticRole::Authority, connection);
        let mut peer = exchange(SemanticRole::NonAuthority, connection);
        let requirements = NegotiationRequirements::default();

        let authority_offer = authority
            .prepare_offer(&mut authority_manager, offer(None))
            .unwrap();
        let peer_offer = peer.prepare_offer(&mut peer_manager, offer(None)).unwrap();

        assert_eq!(
            authority
                .receive(&mut authority_manager, &requirements, peer_offer)
                .unwrap(),
            NegotiationProgress::AuthoritySelectionRequired
        );
        assert_eq!(
            peer.receive(&mut peer_manager, &requirements, authority_offer)
                .unwrap(),
            NegotiationProgress::Waiting
        );

        let observed = authority_manager.attempt_offers(connection).unwrap();
        assert_eq!(observed.authority().offer(), &offer(None));
        assert_eq!(observed.peer().offer(), &offer(None));

        let proposal = authority
            .propose_authority(&mut authority_manager, contract(), &requirements)
            .unwrap();
        let validated = match peer
            .receive(&mut peer_manager, &requirements, proposal)
            .unwrap()
        {
            NegotiationProgress::Send(frame) => frame,
            other => panic!("expected validation frame, got {other:?}"),
        };
        let established = match authority
            .receive(&mut authority_manager, &requirements, validated)
            .unwrap()
        {
            NegotiationProgress::Send(frame) => frame,
            other => panic!("expected established frame, got {other:?}"),
        };
        assert_eq!(authority.state(), NegotiationState::Established);
        assert_eq!(
            peer.receive(&mut peer_manager, &requirements, established)
                .unwrap(),
            NegotiationProgress::Established
        );
        assert_eq!(peer.state(), NegotiationState::Established);
        assert_eq!(
            authority_manager
                .established(connection)
                .unwrap()
                .contract(),
            peer_manager.established(connection).unwrap().contract()
        );
        assert_eq!(
            authority_manager
                .established(connection)
                .unwrap()
                .contract(),
            &contract()
        );
    }

    #[test]
    fn invalid_proposal_maps_to_invalid_selection_and_releases_attempt() {
        let connection = ConnectionHandle::new(5);
        let mut manager = new_manager();
        let mut peer = exchange(SemanticRole::NonAuthority, connection);
        let requirements = NegotiationRequirements::default();
        peer.prepare_offer(&mut manager, offer(None)).unwrap();
        let authority_offer = encode_offer(&offer(None), MAX_FRAME).unwrap();
        assert_eq!(
            peer.receive(
                &mut manager,
                &requirements,
                frame(ControlFrameType::NegotiationOffer, authority_offer)
            )
            .unwrap(),
            NegotiationProgress::Waiting
        );
        assert!(manager.reserved_bytes() > 0);

        let invalid = NegotiatedContract::new(protocol(99));
        let body = encode_proposal(&invalid, MAX_FRAME).unwrap();
        assert!(matches!(
            peer.receive(
                &mut manager,
                &requirements,
                frame(ControlFrameType::NegotiationProposal, body)
            ),
            Err(NegotiationControlError::LocalFailure {
                outcome: NegotiationOutcome::InvalidSelection,
                ..
            })
        ));
        assert_eq!(manager.reserved_bytes(), 0);
    }

    #[test]
    fn wrong_order_and_nonempty_acknowledgement_fail_closed() {
        let connection = ConnectionHandle::new(6);
        let mut manager = new_manager();
        let mut peer = exchange(SemanticRole::NonAuthority, connection);
        let requirements = NegotiationRequirements::default();
        peer.prepare_offer(&mut manager, offer(None)).unwrap();
        let authority_offer = encode_offer(&offer(None), MAX_FRAME).unwrap();
        peer.receive(
            &mut manager,
            &requirements,
            frame(ControlFrameType::NegotiationOffer, authority_offer),
        )
        .unwrap();
        assert!(manager.reserved_bytes() > 0);

        assert!(matches!(
            peer.receive(
                &mut manager,
                &requirements,
                frame(ControlFrameType::NegotiationValidated, Vec::new())
            ),
            Err(NegotiationControlError::ProfileProtocol(
                NegotiationProtocolError::UnexpectedFrame { .. }
            ))
        ));
        assert_eq!(manager.reserved_bytes(), 0);

        let mut authority_manager = new_manager();
        let mut peer_manager = new_manager();
        let mut authority = exchange(SemanticRole::Authority, connection);
        let mut peer = exchange(SemanticRole::NonAuthority, connection);
        let authority_offer = authority
            .prepare_offer(&mut authority_manager, offer(None))
            .unwrap();
        let peer_offer = peer.prepare_offer(&mut peer_manager, offer(None)).unwrap();
        authority
            .receive(&mut authority_manager, &requirements, peer_offer)
            .unwrap();
        peer.receive(&mut peer_manager, &requirements, authority_offer)
            .unwrap();
        let proposal = authority
            .propose_authority(&mut authority_manager, contract(), &requirements)
            .unwrap();
        peer.receive(&mut peer_manager, &requirements, proposal)
            .unwrap();

        assert!(matches!(
            authority.receive(
                &mut authority_manager,
                &requirements,
                frame(ControlFrameType::NegotiationValidated, vec![0])
            ),
            Err(NegotiationControlError::ProfileProtocol(
                NegotiationProtocolError::NonEmptyAcknowledgement(
                    ControlFrameType::NegotiationValidated
                )
            ))
        ));
        assert_eq!(authority_manager.reserved_bytes(), 0);

        let mut authority_manager = new_manager();
        let mut peer_manager = new_manager();
        let mut authority = exchange(SemanticRole::Authority, connection);
        let mut peer = exchange(SemanticRole::NonAuthority, connection);
        let authority_offer = authority
            .prepare_offer(&mut authority_manager, offer(None))
            .unwrap();
        let peer_offer = peer.prepare_offer(&mut peer_manager, offer(None)).unwrap();
        authority
            .receive(&mut authority_manager, &requirements, peer_offer)
            .unwrap();
        peer.receive(&mut peer_manager, &requirements, authority_offer)
            .unwrap();
        let proposal = authority
            .propose_authority(&mut authority_manager, contract(), &requirements)
            .unwrap();
        let validated = match peer
            .receive(&mut peer_manager, &requirements, proposal)
            .unwrap()
        {
            NegotiationProgress::Send(frame) => frame,
            other => panic!("expected validation frame, got {other:?}"),
        };
        let _established = authority
            .receive(&mut authority_manager, &requirements, validated)
            .unwrap();
        assert!(matches!(
            peer.receive(
                &mut peer_manager,
                &requirements,
                frame(ControlFrameType::NegotiationEstablished, vec![0])
            ),
            Err(NegotiationControlError::ProfileProtocol(
                NegotiationProtocolError::NonEmptyAcknowledgement(
                    ControlFrameType::NegotiationEstablished
                )
            ))
        ));
        assert_eq!(peer_manager.reserved_bytes(), 0);
    }

    #[test]
    fn duplicate_offer_and_proposal_frames_fail_closed_and_release_core_state() {
        let connection = ConnectionHandle::new(61);
        let requirements = NegotiationRequirements::default();

        let mut manager = new_manager();
        let mut peer = exchange(SemanticRole::NonAuthority, connection);
        peer.prepare_offer(&mut manager, offer(None)).unwrap();
        let authority_offer = encode_offer(&offer(None), MAX_FRAME).unwrap();
        peer.receive(
            &mut manager,
            &requirements,
            frame(ControlFrameType::NegotiationOffer, authority_offer.clone()),
        )
        .unwrap();
        assert!(manager.reserved_bytes() > 0);
        assert!(matches!(
            peer.receive(
                &mut manager,
                &requirements,
                frame(ControlFrameType::NegotiationOffer, authority_offer)
            ),
            Err(NegotiationControlError::ProfileProtocol(
                NegotiationProtocolError::UnexpectedFrame { .. }
            ))
        ));
        assert_eq!(manager.reserved_bytes(), 0);

        let mut manager = new_manager();
        let mut peer = exchange(SemanticRole::NonAuthority, connection);
        peer.prepare_offer(&mut manager, offer(None)).unwrap();
        let authority_offer = encode_offer(&offer(None), MAX_FRAME).unwrap();
        peer.receive(
            &mut manager,
            &requirements,
            frame(ControlFrameType::NegotiationOffer, authority_offer),
        )
        .unwrap();
        let proposal_body = encode_proposal(&contract(), MAX_FRAME).unwrap();
        peer.receive(
            &mut manager,
            &requirements,
            frame(ControlFrameType::NegotiationProposal, proposal_body.clone()),
        )
        .unwrap();
        assert!(manager.reserved_bytes() > 0);
        assert!(matches!(
            peer.receive(
                &mut manager,
                &requirements,
                frame(ControlFrameType::NegotiationProposal, proposal_body)
            ),
            Err(NegotiationControlError::ProfileProtocol(
                NegotiationProtocolError::UnexpectedFrame { .. }
            ))
        ));
        assert_eq!(manager.reserved_bytes(), 0);
    }

    #[test]
    fn remote_failure_releases_active_attempt_without_reply_loop() {
        let connection = ConnectionHandle::new(7);
        let mut manager = new_manager();
        let mut exchange = exchange(SemanticRole::Authority, connection);
        let requirements = NegotiationRequirements::default();
        exchange
            .prepare_offer(&mut manager, compact_offer())
            .unwrap();
        let peer_offer = encode_offer(&compact_offer(), MAX_FRAME).unwrap();
        exchange
            .receive(
                &mut manager,
                &requirements,
                frame(ControlFrameType::NegotiationOffer, peer_offer),
            )
            .unwrap();
        assert!(manager.reserved_bytes() > 0);

        assert_eq!(
            exchange
                .receive(
                    &mut manager,
                    &requirements,
                    frame(ControlFrameType::NegotiationFailed, vec![1])
                )
                .unwrap(),
            NegotiationProgress::RemoteFailed(NegotiationOutcome::ProtocolIncompatible)
        );
        assert_eq!(exchange.state(), NegotiationState::Failed);
        assert_eq!(manager.reserved_bytes(), 0);
    }

    #[test]
    fn abort_releases_attempt_or_established_contract() {
        let connection = ConnectionHandle::new(8);
        let mut manager = new_manager();
        let mut exchange = exchange(SemanticRole::Authority, connection);
        exchange
            .prepare_offer(&mut manager, compact_offer())
            .unwrap();
        let peer_offer = encode_offer(&compact_offer(), MAX_FRAME).unwrap();
        exchange
            .receive(
                &mut manager,
                &NegotiationRequirements::default(),
                frame(ControlFrameType::NegotiationOffer, peer_offer),
            )
            .unwrap();
        assert!(manager.reserved_bytes() > 0);
        exchange.abort(&mut manager).unwrap();
        assert_eq!(manager.reserved_bytes(), 0);
        assert_eq!(exchange.state(), NegotiationState::Failed);
    }

    #[test]
    fn abort_releases_an_established_contract() {
        let connection = ConnectionHandle::new(81);
        let requirements = NegotiationRequirements::default();
        let mut authority_manager = new_manager();
        let mut peer_manager = new_manager();
        let mut authority = exchange(SemanticRole::Authority, connection);
        let mut peer = exchange(SemanticRole::NonAuthority, connection);

        let authority_offer = authority
            .prepare_offer(&mut authority_manager, offer(None))
            .unwrap();
        let peer_offer = peer.prepare_offer(&mut peer_manager, offer(None)).unwrap();
        authority
            .receive(&mut authority_manager, &requirements, peer_offer)
            .unwrap();
        peer.receive(&mut peer_manager, &requirements, authority_offer)
            .unwrap();
        let proposal = authority
            .propose_authority(&mut authority_manager, contract(), &requirements)
            .unwrap();
        let validated = match peer
            .receive(&mut peer_manager, &requirements, proposal)
            .unwrap()
        {
            NegotiationProgress::Send(frame) => frame,
            other => panic!("expected validation frame, got {other:?}"),
        };
        let established = match authority
            .receive(&mut authority_manager, &requirements, validated)
            .unwrap()
        {
            NegotiationProgress::Send(frame) => frame,
            other => panic!("expected established frame, got {other:?}"),
        };
        peer.receive(&mut peer_manager, &requirements, established)
            .unwrap();
        assert!(authority_manager.reserved_bytes() > 0);
        assert!(peer_manager.reserved_bytes() > 0);

        authority.abort(&mut authority_manager).unwrap();
        peer.abort(&mut peer_manager).unwrap();
        assert_eq!(authority_manager.reserved_bytes(), 0);
        assert_eq!(peer_manager.reserved_bytes(), 0);
        assert_eq!(authority.state(), NegotiationState::Failed);
        assert_eq!(peer.state(), NegotiationState::Failed);
    }

    #[test]
    fn manager_failure_mapping_preserves_normative_outcome_classes() {
        assert_eq!(
            manager_failure_outcome(NegotiationManagerError::AttemptLimitExceeded),
            Ok(NegotiationOutcome::ResourceLimitExceeded)
        );
        assert_eq!(
            manager_failure_outcome(NegotiationError::ProtocolIncompatible.into()),
            Ok(NegotiationOutcome::ProtocolIncompatible)
        );
        assert_eq!(
            manager_failure_outcome(
                NegotiationError::RequiredCapabilityUnavailable(CapabilityId::new(1)).into()
            ),
            Ok(NegotiationOutcome::RequiredCapabilityUnavailable)
        );
        assert_eq!(
            manager_failure_outcome(
                NegotiationError::RequiredSchemaUnavailable(SchemaId::new(1)).into()
            ),
            Ok(NegotiationOutcome::RequiredSchemaUnavailable)
        );
        assert_eq!(
            manager_failure_outcome(NegotiationError::SelectionTooLarge.into()),
            Ok(NegotiationOutcome::ResourceLimitExceeded)
        );
        assert_eq!(
            manager_failure_outcome(NegotiationError::InvalidSelection.into()),
            Ok(NegotiationOutcome::InvalidSelection)
        );
    }
}
