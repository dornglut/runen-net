from pathlib import Path

path = Path("crates/runen-net/src/protocol.rs")
text = path.read_text()


def replace_once(old: str, new: str, label: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected one match, found {count}")
    text = text.replace(old, new, 1)


replace_once(
    """    NoProposal,\n    ValidationMismatch,\n    AlreadyEstablished,\n""",
    """    NoProposal,\n    AlreadyEstablished,\n""",
    "remove unreachable mismatch error",
)

replace_once(
    """    fn validate_authority(\n        &mut self,\n        contract: &NegotiatedContract,\n    ) -> Result<NegotiationStatus, NegotiationError> {\n        self.validate_party(contract, true)\n    }\n\n    fn validate_peer(\n        &mut self,\n        contract: &NegotiatedContract,\n    ) -> Result<NegotiationStatus, NegotiationError> {\n        self.validate_party(contract, false)\n    }\n\n    fn validate_party(\n        &mut self,\n        contract: &NegotiatedContract,\n        authority: bool,\n    ) -> Result<NegotiationStatus, NegotiationError> {\n        let proposal = self.proposal.as_ref().ok_or(NegotiationError::NoProposal)?;\n        if proposal != contract {\n            return Err(NegotiationError::ValidationMismatch);\n        }\n\n        if authority {\n""",
    """    fn validate_authority(&mut self) -> Result<NegotiationStatus, NegotiationError> {\n        self.validate_party(true)\n    }\n\n    fn validate_peer(&mut self) -> Result<NegotiationStatus, NegotiationError> {\n        self.validate_party(false)\n    }\n\n    fn validate_party(&mut self, authority: bool) -> Result<NegotiationStatus, NegotiationError> {\n        self.proposal.as_ref().ok_or(NegotiationError::NoProposal)?;\n\n        if authority {\n""",
    "attempt acknowledgement API",
)

replace_once(
    """    pub fn validate_authority(\n        &mut self,\n        connection: ConnectionHandle,\n        contract: &NegotiatedContract,\n    ) -> Result<NegotiationStatus, NegotiationManagerError> {\n        let status = self.attempt_mut(connection)?.validate_authority(contract)?;\n        if status == NegotiationStatus::Established {\n            self.promote_established(connection)?;\n        }\n        Ok(status)\n    }\n\n    pub fn validate_peer(\n        &mut self,\n        connection: ConnectionHandle,\n        contract: &NegotiatedContract,\n    ) -> Result<NegotiationStatus, NegotiationManagerError> {\n        let status = self.attempt_mut(connection)?.validate_peer(contract)?;\n        if status == NegotiationStatus::Established {\n            self.promote_established(connection)?;\n        }\n        Ok(status)\n    }\n""",
    """    pub fn validate_authority(\n        &mut self,\n        connection: ConnectionHandle,\n    ) -> Result<NegotiationStatus, NegotiationManagerError> {\n        let status = self.attempt_mut(connection)?.validate_authority()?;\n        if status == NegotiationStatus::Established {\n            self.promote_established(connection)?;\n        }\n        Ok(status)\n    }\n\n    pub fn validate_peer(\n        &mut self,\n        connection: ConnectionHandle,\n    ) -> Result<NegotiationStatus, NegotiationManagerError> {\n        let status = self.attempt_mut(connection)?.validate_peer()?;\n        if status == NegotiationStatus::Established {\n            self.promote_established(connection)?;\n        }\n        Ok(status)\n    }\n""",
    "manager acknowledgement API",
)

replace_once(
    """        let contract = common_contract();\n        manager\n            .propose(\n                connection,\n                contract.clone(),\n                &NegotiationRequirements::default(),\n            )\n            .unwrap();\n        assert_ne!(\n            manager.validate_authority(connection, &contract).unwrap(),\n            NegotiationStatus::Established\n        );\n        assert_eq!(\n            manager.validate_peer(connection, &contract).unwrap(),\n            NegotiationStatus::Established\n        );\n""",
    """        manager\n            .propose(\n                connection,\n                common_contract(),\n                &NegotiationRequirements::default(),\n            )\n            .unwrap();\n        assert_ne!(\n            manager.validate_authority(connection).unwrap(),\n            NegotiationStatus::Established\n        );\n        assert_eq!(\n            manager.validate_peer(connection).unwrap(),\n            NegotiationStatus::Established\n        );\n""",
    "manager establish helper",
)

replace_once(
    """        let contract = NegotiatedContract::new(protocol(1));\n        attempt\n            .propose(contract.clone(), &NegotiationRequirements::default())\n            .unwrap();\n        assert_eq!(\n            attempt.validate_authority(&contract).unwrap(),\n            NegotiationStatus::AwaitingValidation {\n                authority_validated: true,\n                peer_validated: false\n            }\n        );\n        assert_eq!(\n            attempt.validate_peer(&contract).unwrap(),\n            NegotiationStatus::Established\n        );\n""",
    """        attempt\n            .propose(\n                NegotiatedContract::new(protocol(1)),\n                &NegotiationRequirements::default(),\n            )\n            .unwrap();\n        assert_eq!(\n            attempt.validate_authority().unwrap(),\n            NegotiationStatus::AwaitingValidation {\n                authority_validated: true,\n                peer_validated: false\n            }\n        );\n        assert_eq!(\n            attempt.validate_peer().unwrap(),\n            NegotiationStatus::Established\n        );\n""",
    "optional proposal acknowledgement test",
)

replace_once(
    """    #[test]\n    fn mutual_validation_must_reference_the_same_contract() {\n        let (authority, peer) = common_offers();\n        let mut attempt = NegotiationAttempt::new(authority, peer, OfferLimits::default()).unwrap();\n        let contract = common_contract();\n        attempt\n            .propose(contract.clone(), &NegotiationRequirements::default())\n            .unwrap();\n        assert_ne!(\n            attempt.validate_authority(&contract).unwrap(),\n            NegotiationStatus::Established\n        );\n\n        let different = NegotiatedContract::new(protocol(1));\n        assert_eq!(\n            attempt.validate_peer(&different),\n            Err(NegotiationError::ValidationMismatch)\n        );\n        assert_ne!(attempt.status(), NegotiationStatus::Established);\n        assert_eq!(\n            attempt.validate_peer(&contract).unwrap(),\n            NegotiationStatus::Established\n        );\n    }\n""",
    """    #[test]\n    fn validation_acknowledges_only_the_stored_proposal() {\n        let (authority, peer) = common_offers();\n        let mut attempt = NegotiationAttempt::new(authority, peer, OfferLimits::default()).unwrap();\n\n        assert_eq!(\n            attempt.validate_authority(),\n            Err(NegotiationError::NoProposal)\n        );\n        attempt\n            .propose(common_contract(), &NegotiationRequirements::default())\n            .unwrap();\n        assert_eq!(\n            attempt.validate_authority().unwrap(),\n            NegotiationStatus::AwaitingValidation {\n                authority_validated: true,\n                peer_validated: false\n            }\n        );\n        assert_eq!(attempt.validate_peer().unwrap(), NegotiationStatus::Established);\n    }\n""",
    "stored proposal validation test",
)

if "ValidationMismatch" in text:
    raise SystemExit("ValidationMismatch remained after patch")

path.write_text(text)
