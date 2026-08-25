from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{label}: expected exactly one match, found {count}")
    return text.replace(old, new, 1)


# Minimal sibling-module visibility for the accepted-flow registry.
path = Path("crates/runen-net-quic/src/quinn_binding.rs")
text = path.read_text()
text = replace_once(
    text,
    "struct RegisteredFlow {\n    key: DeliveryFlowKey,\n    mode: DeliveryMode,\n    max_message_bytes: usize,\n    reliable_association: Option<ReliableAssociationState>,\n}\n",
    "pub(super) struct RegisteredFlow {\n    key: DeliveryFlowKey,\n    mode: DeliveryMode,\n    max_message_bytes: usize,\n    reliable_association: Option<ReliableAssociationState>,\n}\n\nimpl RegisteredFlow {\n    pub(super) const fn key(self) -> DeliveryFlowKey {\n        self.key\n    }\n\n    pub(super) const fn mode(self) -> DeliveryMode {\n        self.mode\n    }\n\n    pub(super) const fn max_message_bytes(self) -> usize {\n        self.max_message_bytes\n    }\n}\n",
    "RegisteredFlow visibility/accessors",
)
text = replace_once(
    text,
    "enum RegistryError {",
    "pub(super) enum RegistryError {",
    "RegistryError visibility",
)
text = replace_once(
    text,
    "struct AcceptedFlowRegistry {",
    "pub(super) struct AcceptedFlowRegistry {",
    "AcceptedFlowRegistry visibility",
)
text = replace_once(
    text,
    "    fn new(local_side: WireSide, max_active: NonZeroUsize) -> Self {",
    "    pub(super) fn new(local_side: WireSide, max_active: NonZeroUsize) -> Self {",
    "registry constructor visibility",
)
text = replace_once(
    text,
    "    fn register_consumed_accepted_flow(\n",
    "    pub(super) fn register_consumed_accepted_flow(\n",
    "registry registration visibility",
)
text = replace_once(
    text,
    "    fn registered_flow(&self, flow_id: FlowId) -> Option<RegisteredFlow> {",
    "    pub(super) fn registered_flow(&self, flow_id: FlowId) -> Option<RegisteredFlow> {",
    "registry lookup visibility",
)
path.write_text(text)

# Register the crate-private RN5D3 realization without pulling RN6 surface forward.
path = Path("crates/runen-net-quic/src/lib.rs")
text = path.read_text()
needle = "//! semantics.\n\n"
insert = "//! semantics.\n\n#[allow(\n    dead_code,\n    reason = \"RN5D3 lands crate-private DATAGRAM realization before RN5E/RN6 wiring\"\n)]\nmod datagram;\n"
text = replace_once(text, needle, insert, "datagram module registration")
path.write_text(text)

# Correct and harden the RN5D3 draft.
path = Path("crates/runen-net-quic/src/datagram.rs")
text = path.read_text()
text = replace_once(
    text,
    "    decode_varint, encode_varint, FlowId, FlowIdError, VarIntDecodeError, VarIntEncodeError,\n};",
    "    decode_varint, encode_varint, FlowId, FlowIdError, VarIntDecodeError, VarIntEncodeError,\n    MAX_VARINT,\n};",
    "MAX_VARINT import",
)
text = replace_once(
    text,
    "    LengthOverflow,\n    AcceptedIndexMismatch { expected: u64, accepted: u64 },",
    "    LengthOverflow,\n    SequenceExhausted,\n    AcceptedIndexMismatch { expected: u64, accepted: u64 },",
    "submission exhaustion error",
)
text = replace_once(
    text,
    "    LengthOverflow,\n    ModeMismatch,",
    "    LengthOverflow,\n    SequenceExhausted,\n    ModeMismatch,",
    "send exhaustion error",
)
text = replace_once(
    text,
    "        let wire_len = datagram_len(flow_id, flow.mode(), sequence, transfer.payload_len())?;",
    "        let wire_len =\n            datagram_len_for_send(flow_id, flow.mode(), sequence, transfer.payload_len())?;",
    "send length error domain",
)
text = replace_once(
    text,
    "        DeliveryMode::UnreliableSequenced => {\n            encode_varint(sequence.ok_or(DatagramSubmissionError::LengthOverflow)?)?.len()\n        }",
    "        DeliveryMode::UnreliableSequenced => {\n            let sequence = sequence.ok_or(DatagramSubmissionError::LengthOverflow)?;\n            if sequence > MAX_VARINT {\n                return Err(DatagramSubmissionError::SequenceExhausted);\n            }\n            encode_varint(sequence)?.len()\n        }",
    "submission sequence exhaustion",
)
text = replace_once(
    text,
    "        DeliveryMode::UnreliableSequenced => {\n            encode_varint(sequence.ok_or(DatagramSendError::LengthOverflow)?)?.len()\n        }",
    "        DeliveryMode::UnreliableSequenced => {\n            let sequence = sequence.ok_or(DatagramSendError::LengthOverflow)?;\n            if sequence > MAX_VARINT {\n                return Err(DatagramSendError::SequenceExhausted);\n            }\n            encode_varint(sequence)?.len()\n        }",
    "send sequence exhaustion",
)

# Add focused wire/exhaustion coverage before the existing preflight test.
needle = "    #[test]\n    fn sequenced_preflight_uses_exact_core_candidate_and_mtu_rejection_consumes_nothing() {"
insert = """    #[test]
    fn envelopes_use_minimal_varints_and_sequence_exhaustion_is_explicit() {
        let flow_id = FlowId::new(WireSide::Client, 32).unwrap();
        assert_eq!(
            encode_datagram(
                flow_id,
                DeliveryMode::UnreliableUnordered,
                None,
                b\"x\",
            )
            .unwrap(),
            vec![0x40, 0x40, b'x']
        );
        assert_eq!(
            encode_datagram(
                flow_id,
                DeliveryMode::UnreliableSequenced,
                Some(64),
                b\"x\",
            )
            .unwrap(),
            vec![0x40, 0x40, 0x40, 0x40, b'x']
        );
        assert_eq!(
            datagram_len(
                flow_id,
                DeliveryMode::UnreliableSequenced,
                Some(MAX_VARINT + 1),
                0,
            ),
            Err(DatagramSubmissionError::SequenceExhausted)
        );
        assert_eq!(
            datagram_len_for_send(
                flow_id,
                DeliveryMode::UnreliableSequenced,
                Some(MAX_VARINT + 1),
                0,
            ),
            Err(DatagramSendError::SequenceExhausted)
        );
    }

    #[test]
    fn sequenced_preflight_uses_exact_core_candidate_and_mtu_rejection_consumes_nothing() {"""
text = replace_once(text, needle, insert, "wire/exhaustion tests")

# Add a known-flow non-minimal sequence test before the broad malformed-profile test.
needle = "    #[test]\n    fn inbound_rejects_malformed_wrong_mode_direction_and_profile_oversize() {"
insert = """    #[test]
    fn inbound_rejects_non_minimal_sequence_metadata() {
        let flow_id = FlowId::new(WireSide::Server, 5).unwrap();
        let (mut endpoint, registry, _) = endpoint_with_registered_flow(
            FlowDirection::Inbound,
            DeliveryMode::UnreliableSequenced,
            11,
            flow_id,
            policy(
                8,
                4,
                32,
                OutboundPressureBehavior::RejectNew,
                ReceiverPressureBehavior::DropIncomingUnreliable,
            ),
            8,
        );
        let mut datagram = encode_varint(flow_id.value()).unwrap().as_slice().to_vec();
        datagram.extend_from_slice(&[0x40, 0x01, b'x']);
        assert_eq!(
            receive_datagram(&mut endpoint, &registry, &datagram),
            Err(DatagramReceiveError::VarInt(VarIntDecodeError::NonMinimal))
        );
        assert_eq!(endpoint.pending_messages(), 0);
    }

    #[test]
    fn inbound_rejects_malformed_wrong_mode_direction_and_profile_oversize() {"""
text = replace_once(text, needle, insert, "non-minimal sequence test")
path.write_text(text)
