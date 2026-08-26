use quinn::VarInt;
use runen_net::delivery::DeliveryMode;

pub(crate) const MAX_VARINT: u64 = (1u64 << 62) - 1;
const MAX_FLOW_SEQUENCE: u64 = MAX_VARINT >> 1;
const MAX_CONTROL_BODY_BYTES: usize = 24;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum ApplicationErrorCode {
    NoError,
    ProfileProtocolError,
    ControlFrameError,
    ResourceLimitError,
    NegotiationFailed,
    FlowProtocolError,
    ReliableDeliveryFailed,
}

impl ApplicationErrorCode {
    const fn wire(self) -> u32 {
        match self {
            Self::NoError => 0,
            Self::ProfileProtocolError => 1,
            Self::ControlFrameError => 2,
            Self::ResourceLimitError => 3,
            Self::NegotiationFailed => 4,
            Self::FlowProtocolError => 5,
            Self::ReliableDeliveryFailed => 6,
        }
    }

    pub(crate) const fn quinn(self) -> VarInt {
        VarInt::from_u32(self.wire())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum VarIntEncodeError {
    OutOfRange,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum VarIntDecodeError {
    Incomplete { needed: usize, available: usize },
    NonMinimal,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct EncodedVarInt {
    bytes: [u8; 8],
    len: usize,
}

impl EncodedVarInt {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }

    pub(crate) const fn len(&self) -> usize {
        self.len
    }
}

pub(crate) fn encode_varint(value: u64) -> Result<EncodedVarInt, VarIntEncodeError> {
    let mut bytes = [0u8; 8];
    let len = varint_len(value).ok_or(VarIntEncodeError::OutOfRange)?;

    match len {
        1 => bytes[0] = value as u8,
        2 => {
            let encoded = (value as u16) | 0x4000;
            bytes[..2].copy_from_slice(&encoded.to_be_bytes());
        }
        4 => {
            let encoded = (value as u32) | 0x8000_0000;
            bytes[..4].copy_from_slice(&encoded.to_be_bytes());
        }
        8 => {
            let encoded = value | 0xc000_0000_0000_0000;
            bytes.copy_from_slice(&encoded.to_be_bytes());
        }
        _ => unreachable!("QUIC varints have only 1, 2, 4, or 8 octets"),
    }

    Ok(EncodedVarInt { bytes, len })
}

pub(crate) fn decode_varint(input: &[u8]) -> Result<(u64, usize), VarIntDecodeError> {
    let Some(&first) = input.first() else {
        return Err(VarIntDecodeError::Incomplete {
            needed: 1,
            available: 0,
        });
    };

    let len = encoded_len_from_first(first);
    if input.len() < len {
        return Err(VarIntDecodeError::Incomplete {
            needed: len,
            available: input.len(),
        });
    }

    let mut value = u64::from(first & 0x3f);
    for &byte in &input[1..len] {
        value = (value << 8) | u64::from(byte);
    }

    if varint_len(value) != Some(len) {
        return Err(VarIntDecodeError::NonMinimal);
    }

    Ok((value, len))
}

const fn encoded_len_from_first(first: u8) -> usize {
    1usize << (first >> 6)
}

const fn varint_len(value: u64) -> Option<usize> {
    if value < (1 << 6) {
        Some(1)
    } else if value < (1 << 14) {
        Some(2)
    } else if value < (1 << 30) {
        Some(4)
    } else if value <= MAX_VARINT {
        Some(8)
    } else {
        None
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum WireSide {
    Client,
    Server,
}

impl WireSide {
    const fn bit(self) -> u64 {
        match self {
            Self::Client => 0,
            Self::Server => 1,
        }
    }

    const fn from_bit(bit: u64) -> Self {
        if bit == 0 { Self::Client } else { Self::Server }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum FlowIdError {
    SequenceOutOfRange,
    WireValueOutOfRange,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct FlowId(u64);

impl FlowId {
    pub(crate) fn new(side: WireSide, sequence: u64) -> Result<Self, FlowIdError> {
        if sequence > MAX_FLOW_SEQUENCE {
            return Err(FlowIdError::SequenceOutOfRange);
        }
        Ok(Self((sequence << 1) | side.bit()))
    }

    pub(crate) fn from_wire(value: u64) -> Result<Self, FlowIdError> {
        if value > MAX_VARINT {
            return Err(FlowIdError::WireValueOutOfRange);
        }
        Ok(Self(value))
    }

    pub(crate) const fn value(self) -> u64 {
        self.0
    }

    pub(crate) const fn side(self) -> WireSide {
        WireSide::from_bit(self.0 & 1)
    }

    pub(crate) const fn sequence(self) -> u64 {
        self.0 >> 1
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum FlowIdCursorError {
    Exhausted,
    WrongSide {
        expected: WireSide,
        received: WireSide,
    },
    UnexpectedSequence {
        expected: u64,
        received: u64,
    },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct FlowIdCursor {
    side: WireSide,
    next_sequence: Option<u64>,
}

impl FlowIdCursor {
    pub(crate) const fn new(side: WireSide) -> Self {
        Self {
            side,
            next_sequence: Some(0),
        }
    }

    pub(crate) fn allocate(&mut self) -> Result<FlowId, FlowIdCursorError> {
        let sequence = self.next_sequence.ok_or(FlowIdCursorError::Exhausted)?;
        let flow_id = FlowId::new(self.side, sequence)
            .expect("cursor sequence is always inside the FlowId domain");
        self.advance(sequence);
        Ok(flow_id)
    }

    pub(crate) fn validate_and_consume(
        &mut self,
        flow_id: FlowId,
    ) -> Result<(), FlowIdCursorError> {
        let expected = self.next_sequence.ok_or(FlowIdCursorError::Exhausted)?;
        if flow_id.side() != self.side {
            return Err(FlowIdCursorError::WrongSide {
                expected: self.side,
                received: flow_id.side(),
            });
        }
        if flow_id.sequence() != expected {
            return Err(FlowIdCursorError::UnexpectedSequence {
                expected,
                received: flow_id.sequence(),
            });
        }
        self.advance(expected);
        Ok(())
    }

    fn advance(&mut self, sequence: u64) {
        self.next_sequence = if sequence == MAX_FLOW_SEQUENCE {
            None
        } else {
            Some(sequence + 1)
        };
    }

    #[cfg(test)]
    const fn with_next(side: WireSide, next_sequence: Option<u64>) -> Self {
        Self {
            side,
            next_sequence,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum FlowRejectReason {
    ResourceLimit,
    MessageLimit,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum FlowTerminateReason {
    Normal,
    ResourceFailure,
    ProtocolFailure,
    ReliableDeliveryFailure,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) enum ControlBodyError {
    VarInt(VarIntDecodeError),
    InvalidFlowId(FlowIdError),
    UnknownDeliveryMode(u64),
    ZeroMaxMessageBytes,
    MaxMessageBytesOutOfRange,
    UnknownRejectReason(u64),
    UnknownTerminateReason(u64),
    TrailingBytes,
}

impl From<VarIntDecodeError> for ControlBodyError {
    fn from(error: VarIntDecodeError) -> Self {
        Self::VarInt(error)
    }
}

impl From<FlowIdError> for ControlBodyError {
    fn from(error: FlowIdError) -> Self {
        Self::InvalidFlowId(error)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct OpenFlow {
    pub(crate) flow_id: FlowId,
    pub(crate) delivery_mode: DeliveryMode,
    pub(crate) max_message_bytes: u64,
}

impl OpenFlow {
    pub(crate) fn new(
        flow_id: FlowId,
        delivery_mode: DeliveryMode,
        max_message_bytes: u64,
    ) -> Result<Self, ControlBodyError> {
        if max_message_bytes == 0 {
            return Err(ControlBodyError::ZeroMaxMessageBytes);
        }
        if max_message_bytes > MAX_VARINT {
            return Err(ControlBodyError::MaxMessageBytesOutOfRange);
        }
        Ok(Self {
            flow_id,
            delivery_mode,
            max_message_bytes,
        })
    }

    pub(crate) fn encode(self) -> EncodedControlBody {
        let mut writer = BodyWriter::new();
        writer.push_varint(self.flow_id.value());
        writer.push_varint(encode_delivery_mode(self.delivery_mode));
        writer.push_varint(self.max_message_bytes);
        writer.finish()
    }

    pub(crate) fn decode(input: &[u8]) -> Result<Self, ControlBodyError> {
        let mut reader = BodyReader::new(input);
        let flow_id = FlowId::from_wire(reader.read_varint()?)?;
        let delivery_mode = decode_delivery_mode(reader.read_varint()?)?;
        let max_message_bytes = reader.read_varint()?;
        if max_message_bytes == 0 {
            return Err(ControlBodyError::ZeroMaxMessageBytes);
        }
        reader.finish()?;
        Ok(Self {
            flow_id,
            delivery_mode,
            max_message_bytes,
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct FlowAccept {
    pub(crate) flow_id: FlowId,
}

impl FlowAccept {
    pub(crate) fn encode(self) -> EncodedControlBody {
        let mut writer = BodyWriter::new();
        writer.push_varint(self.flow_id.value());
        writer.finish()
    }

    pub(crate) fn decode(input: &[u8]) -> Result<Self, ControlBodyError> {
        let mut reader = BodyReader::new(input);
        let flow_id = FlowId::from_wire(reader.read_varint()?)?;
        reader.finish()?;
        Ok(Self { flow_id })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct FlowReject {
    pub(crate) flow_id: FlowId,
    pub(crate) reason: FlowRejectReason,
}

impl FlowReject {
    pub(crate) fn encode(self) -> EncodedControlBody {
        let mut writer = BodyWriter::new();
        writer.push_varint(self.flow_id.value());
        writer.push_varint(match self.reason {
            FlowRejectReason::ResourceLimit => 0,
            FlowRejectReason::MessageLimit => 1,
        });
        writer.finish()
    }

    pub(crate) fn decode(input: &[u8]) -> Result<Self, ControlBodyError> {
        let mut reader = BodyReader::new(input);
        let flow_id = FlowId::from_wire(reader.read_varint()?)?;
        let reason = match reader.read_varint()? {
            0 => FlowRejectReason::ResourceLimit,
            1 => FlowRejectReason::MessageLimit,
            value => return Err(ControlBodyError::UnknownRejectReason(value)),
        };
        reader.finish()?;
        Ok(Self { flow_id, reason })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct FlowTerminate {
    pub(crate) flow_id: FlowId,
    pub(crate) reason: FlowTerminateReason,
}

impl FlowTerminate {
    pub(crate) fn encode(self) -> EncodedControlBody {
        let mut writer = BodyWriter::new();
        writer.push_varint(self.flow_id.value());
        writer.push_varint(match self.reason {
            FlowTerminateReason::Normal => 0,
            FlowTerminateReason::ResourceFailure => 1,
            FlowTerminateReason::ProtocolFailure => 2,
            FlowTerminateReason::ReliableDeliveryFailure => 3,
        });
        writer.finish()
    }

    pub(crate) fn decode(input: &[u8]) -> Result<Self, ControlBodyError> {
        let mut reader = BodyReader::new(input);
        let flow_id = FlowId::from_wire(reader.read_varint()?)?;
        let reason = match reader.read_varint()? {
            0 => FlowTerminateReason::Normal,
            1 => FlowTerminateReason::ResourceFailure,
            2 => FlowTerminateReason::ProtocolFailure,
            3 => FlowTerminateReason::ReliableDeliveryFailure,
            value => return Err(ControlBodyError::UnknownTerminateReason(value)),
        };
        reader.finish()?;
        Ok(Self { flow_id, reason })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(crate) struct EncodedControlBody {
    bytes: [u8; MAX_CONTROL_BODY_BYTES],
    len: usize,
}

impl EncodedControlBody {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

struct BodyWriter {
    bytes: [u8; MAX_CONTROL_BODY_BYTES],
    len: usize,
}

impl BodyWriter {
    const fn new() -> Self {
        Self {
            bytes: [0; MAX_CONTROL_BODY_BYTES],
            len: 0,
        }
    }

    fn push_varint(&mut self, value: u64) {
        let encoded = encode_varint(value).expect("control values are validated before encoding");
        let end = self.len + encoded.len();
        debug_assert!(end <= self.bytes.len());
        self.bytes[self.len..end].copy_from_slice(encoded.as_slice());
        self.len = end;
    }

    const fn finish(self) -> EncodedControlBody {
        EncodedControlBody {
            bytes: self.bytes,
            len: self.len,
        }
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

    fn read_varint(&mut self) -> Result<u64, ControlBodyError> {
        let (value, consumed) = decode_varint(&self.input[self.offset..])?;
        self.offset += consumed;
        Ok(value)
    }

    fn finish(self) -> Result<(), ControlBodyError> {
        if self.offset == self.input.len() {
            Ok(())
        } else {
            Err(ControlBodyError::TrailingBytes)
        }
    }
}

const fn encode_delivery_mode(mode: DeliveryMode) -> u64 {
    match mode {
        DeliveryMode::ReliableOrdered => 0,
        DeliveryMode::UnreliableUnordered => 1,
        DeliveryMode::UnreliableSequenced => 2,
    }
}

fn decode_delivery_mode(value: u64) -> Result<DeliveryMode, ControlBodyError> {
    match value {
        0 => Ok(DeliveryMode::ReliableOrdered),
        1 => Ok(DeliveryMode::UnreliableUnordered),
        2 => Ok(DeliveryMode::UnreliableSequenced),
        value => Err(ControlBodyError::UnknownDeliveryMode(value)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn application_error_codes_match_revision_one_exactly() {
        let cases = [
            (ApplicationErrorCode::NoError, 0),
            (ApplicationErrorCode::ProfileProtocolError, 1),
            (ApplicationErrorCode::ControlFrameError, 2),
            (ApplicationErrorCode::ResourceLimitError, 3),
            (ApplicationErrorCode::NegotiationFailed, 4),
            (ApplicationErrorCode::FlowProtocolError, 5),
            (ApplicationErrorCode::ReliableDeliveryFailed, 6),
        ];

        for (code, expected) in cases {
            assert_eq!(code.wire(), expected);
            assert_eq!(code.quinn().into_inner(), u64::from(expected));
        }
    }

    #[test]
    fn varint_boundaries_are_minimal_and_round_trip() {
        let cases = [
            (0, 1),
            ((1 << 6) - 1, 1),
            (1 << 6, 2),
            ((1 << 14) - 1, 2),
            (1 << 14, 4),
            ((1 << 30) - 1, 4),
            (1 << 30, 8),
            (MAX_VARINT, 8),
        ];

        for (value, expected_len) in cases {
            let encoded = encode_varint(value).unwrap();
            assert_eq!(encoded.len(), expected_len);
            assert_eq!(decode_varint(encoded.as_slice()), Ok((value, expected_len)));
        }
        assert_eq!(
            encode_varint(MAX_VARINT + 1),
            Err(VarIntEncodeError::OutOfRange)
        );
    }

    #[test]
    fn decoder_rejects_non_minimal_and_truncated_varints() {
        assert_eq!(
            decode_varint(&[0x40, 0x01]),
            Err(VarIntDecodeError::NonMinimal)
        );
        assert_eq!(
            decode_varint(&[0x80, 0x00, 0x00, 0x01]),
            Err(VarIntDecodeError::NonMinimal)
        );
        assert_eq!(
            decode_varint(&[0xc0, 0, 0, 0, 0, 0, 0, 1]),
            Err(VarIntDecodeError::NonMinimal)
        );
        assert_eq!(
            decode_varint(&[]),
            Err(VarIntDecodeError::Incomplete {
                needed: 1,
                available: 0,
            })
        );
        assert_eq!(
            decode_varint(&[0x40]),
            Err(VarIntDecodeError::Incomplete {
                needed: 2,
                available: 1,
            })
        );
        assert_eq!(
            decode_varint(&[0x80, 0, 0]),
            Err(VarIntDecodeError::Incomplete {
                needed: 4,
                available: 3,
            })
        );
    }

    #[test]
    fn flow_id_namespaces_are_directional_and_exact_next() {
        let client_zero = FlowId::new(WireSide::Client, 0).unwrap();
        let server_zero = FlowId::new(WireSide::Server, 0).unwrap();
        assert_eq!(client_zero.value(), 0);
        assert_eq!(server_zero.value(), 1);
        assert_eq!(client_zero.side(), WireSide::Client);
        assert_eq!(server_zero.side(), WireSide::Server);

        let mut client = FlowIdCursor::new(WireSide::Client);
        assert_eq!(client.allocate(), Ok(client_zero));
        assert_eq!(
            client.allocate(),
            Ok(FlowId::new(WireSide::Client, 1).unwrap())
        );

        let mut peer = FlowIdCursor::new(WireSide::Server);
        assert_eq!(
            peer.validate_and_consume(FlowId::new(WireSide::Server, 1).unwrap()),
            Err(FlowIdCursorError::UnexpectedSequence {
                expected: 0,
                received: 1,
            })
        );
        assert_eq!(
            peer.validate_and_consume(client_zero),
            Err(FlowIdCursorError::WrongSide {
                expected: WireSide::Server,
                received: WireSide::Client,
            })
        );
        assert_eq!(peer.validate_and_consume(server_zero), Ok(()));
        assert_eq!(
            peer.validate_and_consume(server_zero),
            Err(FlowIdCursorError::UnexpectedSequence {
                expected: 1,
                received: 0,
            })
        );
    }

    #[test]
    fn flow_id_cursor_exhaustion_never_wraps() {
        let mut cursor = FlowIdCursor::with_next(WireSide::Client, Some(MAX_FLOW_SEQUENCE));
        let last = cursor.allocate().unwrap();
        assert_eq!(last.sequence(), MAX_FLOW_SEQUENCE);
        assert_eq!(cursor.allocate(), Err(FlowIdCursorError::Exhausted));
        assert_eq!(
            FlowId::new(WireSide::Client, MAX_FLOW_SEQUENCE + 1),
            Err(FlowIdError::SequenceOutOfRange)
        );
    }

    #[test]
    fn reliable_control_bodies_round_trip_exactly() {
        let flow_id = FlowId::new(WireSide::Server, 7).unwrap();
        let open = OpenFlow::new(flow_id, DeliveryMode::ReliableOrdered, 4096).unwrap();
        assert_eq!(OpenFlow::decode(open.encode().as_slice()), Ok(open));

        let accept = FlowAccept { flow_id };
        assert_eq!(FlowAccept::decode(accept.encode().as_slice()), Ok(accept));

        for reason in [
            FlowRejectReason::ResourceLimit,
            FlowRejectReason::MessageLimit,
        ] {
            let value = FlowReject { flow_id, reason };
            assert_eq!(FlowReject::decode(value.encode().as_slice()), Ok(value));
        }

        for reason in [
            FlowTerminateReason::Normal,
            FlowTerminateReason::ResourceFailure,
            FlowTerminateReason::ProtocolFailure,
            FlowTerminateReason::ReliableDeliveryFailure,
        ] {
            let value = FlowTerminate { flow_id, reason };
            assert_eq!(FlowTerminate::decode(value.encode().as_slice()), Ok(value));
        }
    }

    #[test]
    fn control_bodies_reject_unknown_zero_out_of_range_non_minimal_truncated_and_trailing_input() {
        let flow_id = FlowId::new(WireSide::Client, 0).unwrap();
        assert_eq!(
            OpenFlow::new(flow_id, DeliveryMode::ReliableOrdered, MAX_VARINT + 1,),
            Err(ControlBodyError::MaxMessageBytesOutOfRange)
        );
        assert_eq!(
            OpenFlow::decode(&[0, 3, 1]),
            Err(ControlBodyError::UnknownDeliveryMode(3))
        );
        assert_eq!(
            OpenFlow::decode(&[0, 0, 0]),
            Err(ControlBodyError::ZeroMaxMessageBytes)
        );
        assert_eq!(
            OpenFlow::decode(&[0, 0]),
            Err(ControlBodyError::VarInt(VarIntDecodeError::Incomplete {
                needed: 1,
                available: 0,
            }))
        );
        assert_eq!(
            OpenFlow::decode(&[0x40, 0, 0, 1]),
            Err(ControlBodyError::VarInt(VarIntDecodeError::NonMinimal))
        );
        assert_eq!(
            FlowReject::decode(&[0, 2]),
            Err(ControlBodyError::UnknownRejectReason(2))
        );
        assert_eq!(
            FlowTerminate::decode(&[0, 4]),
            Err(ControlBodyError::UnknownTerminateReason(4))
        );

        let mut accept = FlowAccept { flow_id }.encode().as_slice().to_vec();
        accept.push(0);
        assert_eq!(
            FlowAccept::decode(&accept),
            Err(ControlBodyError::TrailingBytes)
        );
    }
}
