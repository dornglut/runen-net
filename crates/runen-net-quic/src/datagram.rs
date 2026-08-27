use std::{future::Future, pin::Pin};

use quinn::{Connection, ReadDatagram, SendDatagramError};
use runen_net::delivery::{
    CustodyCommitError, DeliveryEndpoint, DeliveryFlowKey, DeliveryMode, DeliveryOperationError,
    FlowDirection, ReceiveOutcome, SubmissionOutcome,
};

use crate::quinn_binding::{AcceptedFlowRegistry, RegisteredFlow};
use crate::wire::{
    FlowId, FlowIdError, MAX_VARINT, VarIntDecodeError, VarIntEncodeError, decode_varint,
    encode_varint,
};

const UNORDERED_INGRESS_INDEX: u64 = 0;

pub(super) type DatagramReadResult = <ReadDatagram<'static> as Future>::Output;
pub(super) type OwnedDatagramReadFuture =
    Pin<Box<dyn Future<Output = DatagramReadResult> + Send + 'static>>;

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub(super) struct DatagramSenderDiagnostics {
    outbound_transport_drops: usize,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum DatagramTransportError {
    TooLarge,
    UnsupportedByPeer,
    Disabled,
    ConnectionLost,
}

pub(super) trait DatagramSendTransport {
    fn max_datagram_size(&self) -> Option<usize>;
    fn send_buffer_space(&self) -> usize;
    fn try_send(&mut self, datagram: Vec<u8>) -> Result<(), DatagramTransportError>;
}

impl DatagramSendTransport for Connection {
    fn max_datagram_size(&self) -> Option<usize> {
        Connection::max_datagram_size(self)
    }

    fn send_buffer_space(&self) -> usize {
        Connection::datagram_send_buffer_space(self)
    }

    fn try_send(&mut self, datagram: Vec<u8>) -> Result<(), DatagramTransportError> {
        Connection::send_datagram(self, datagram.into()).map_err(|error| match error {
            SendDatagramError::TooLarge => DatagramTransportError::TooLarge,
            SendDatagramError::UnsupportedByPeer => DatagramTransportError::UnsupportedByPeer,
            SendDatagramError::Disabled => DatagramTransportError::Disabled,
            SendDatagramError::ConnectionLost(_) => DatagramTransportError::ConnectionLost,
        })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum DatagramSubmissionOutcome {
    Accepted {
        accepted_index: u64,
        local_pressure_drops: usize,
    },
    RejectedTooLarge,
    RejectedPressure,
    RejectedCounterExhausted,
    RejectedTransportUnavailable,
    RejectedCurrentDatagramSize,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum DatagramSubmissionError {
    UnknownFlowId,
    WrongDirection,
    ReliableFlow,
    Core(DeliveryOperationError),
    Wire(VarIntEncodeError),
    LengthOverflow,
    SequenceExhausted,
    AcceptedIndexMismatch { expected: u64, accepted: u64 },
}

impl From<DeliveryOperationError> for DatagramSubmissionError {
    fn from(error: DeliveryOperationError) -> Self {
        Self::Core(error)
    }
}

impl From<VarIntEncodeError> for DatagramSubmissionError {
    fn from(error: VarIntEncodeError) -> Self {
        Self::Wire(error)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum DatagramSendProgress {
    Idle,
    BlockedNativeBuffer { needed: usize, available: usize },
    Enqueued { accepted_index: u64 },
    DroppedTransport { accepted_index: u64 },
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum DatagramSendError {
    UnknownFlowId,
    WrongDirection,
    ReliableFlow,
    Core(DeliveryOperationError),
    Custody(CustodyCommitError),
    Wire(VarIntEncodeError),
    LengthOverflow,
    SequenceExhausted,
    ModeMismatch,
    PayloadExceedsProfile,
    AllocationFailed,
    ProfileUnavailable,
    ConnectionLost,
}

impl From<DeliveryOperationError> for DatagramSendError {
    fn from(error: DeliveryOperationError) -> Self {
        Self::Core(error)
    }
}

impl From<CustodyCommitError> for DatagramSendError {
    fn from(error: CustodyCommitError) -> Self {
        Self::Custody(error)
    }
}

impl From<VarIntEncodeError> for DatagramSendError {
    fn from(error: VarIntEncodeError) -> Self {
        Self::Wire(error)
    }
}

#[derive(Debug)]
pub(super) struct DatagramSender<T> {
    transport: T,
    diagnostics: DatagramSenderDiagnostics,
}

impl DatagramSender<Connection> {
    /// Construct the single RunenNet DATAGRAM submitter for one Quinn connection.
    ///
    /// RN5E must route every RunenNet `send_datagram` call for this connection
    /// through this object. The native free-space guarantee is only sufficient
    /// when a competing direct DATAGRAM writer cannot race the checked handoff.
    pub(super) fn new_quinn(connection: Connection) -> Self {
        Self::new(connection)
    }
}

impl<T: DatagramSendTransport> DatagramSender<T> {
    fn new(transport: T) -> Self {
        Self {
            transport,
            diagnostics: DatagramSenderDiagnostics::default(),
        }
    }

    pub(super) const fn outbound_transport_drops(&self) -> usize {
        self.diagnostics.outbound_transport_drops
    }

    pub(super) fn submit(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        registry: &AcceptedFlowRegistry,
        flow_id: FlowId,
        payload: Vec<u8>,
    ) -> Result<DatagramSubmissionOutcome, DatagramSubmissionError> {
        let flow = registered_outbound_unreliable(registry, flow_id)
            .map_err(map_registration_submission_error)?;

        if payload.len() > flow.max_message_bytes() {
            return Ok(DatagramSubmissionOutcome::RejectedTooLarge);
        }

        let candidate = match flow.mode() {
            DeliveryMode::UnreliableUnordered => None,
            DeliveryMode::UnreliableSequenced => {
                match endpoint.next_outbound_accepted_index(flow.key())? {
                    Some(candidate) => Some(candidate),
                    None => return Ok(DatagramSubmissionOutcome::RejectedCounterExhausted),
                }
            }
            DeliveryMode::ReliableOrdered => return Err(DatagramSubmissionError::ReliableFlow),
        };

        let wire_len = datagram_len(flow_id, flow.mode(), candidate, payload.len())?;
        let Some(max_datagram_size) = self.transport.max_datagram_size() else {
            return Ok(DatagramSubmissionOutcome::RejectedTransportUnavailable);
        };
        if wire_len > max_datagram_size {
            return Ok(DatagramSubmissionOutcome::RejectedCurrentDatagramSize);
        }

        match endpoint.submit(flow.key(), payload)? {
            SubmissionOutcome::Accepted {
                accepted_index,
                local_pressure_drops,
            } => {
                if let Some(expected) = candidate
                    && accepted_index != expected
                {
                    return Err(DatagramSubmissionError::AcceptedIndexMismatch {
                        expected,
                        accepted: accepted_index,
                    });
                }
                Ok(DatagramSubmissionOutcome::Accepted {
                    accepted_index,
                    local_pressure_drops,
                })
            }
            SubmissionOutcome::RejectedTooLarge => Ok(DatagramSubmissionOutcome::RejectedTooLarge),
            SubmissionOutcome::RejectedPressure => Ok(DatagramSubmissionOutcome::RejectedPressure),
            SubmissionOutcome::RejectedCounterExhausted => {
                Ok(DatagramSubmissionOutcome::RejectedCounterExhausted)
            }
        }
    }

    pub(super) fn drive_one(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        registry: &AcceptedFlowRegistry,
        flow_id: FlowId,
    ) -> Result<DatagramSendProgress, DatagramSendError> {
        let flow = registered_outbound_unreliable(registry, flow_id)
            .map_err(map_registration_send_error)?;
        let Some(transfer) = endpoint.peek_outbound(flow.key())? else {
            return Ok(DatagramSendProgress::Idle);
        };
        if transfer.mode() != flow.mode() {
            return Err(DatagramSendError::ModeMismatch);
        }
        if transfer.payload_len() > flow.max_message_bytes() {
            return Err(DatagramSendError::PayloadExceedsProfile);
        }

        let sequence = match flow.mode() {
            DeliveryMode::UnreliableUnordered => None,
            DeliveryMode::UnreliableSequenced => Some(transfer.accepted_index()),
            DeliveryMode::ReliableOrdered => return Err(DatagramSendError::ReliableFlow),
        };
        let wire_len =
            datagram_len_for_send(flow_id, flow.mode(), sequence, transfer.payload_len())?;

        let Some(max_datagram_size) = self.transport.max_datagram_size() else {
            return Err(DatagramSendError::ProfileUnavailable);
        };
        if wire_len > max_datagram_size {
            return self.drop_transport(endpoint, flow.key(), transfer.accepted_index());
        }

        let available = self.transport.send_buffer_space();
        if wire_len > available {
            return Ok(DatagramSendProgress::BlockedNativeBuffer {
                needed: wire_len,
                available,
            });
        }

        let datagram = encode_datagram(flow_id, flow.mode(), sequence, transfer.payload())?;

        let Some(max_datagram_size) = self.transport.max_datagram_size() else {
            return Err(DatagramSendError::ProfileUnavailable);
        };
        if datagram.len() > max_datagram_size {
            return self.drop_transport(endpoint, flow.key(), transfer.accepted_index());
        }
        let available = self.transport.send_buffer_space();
        if datagram.len() > available {
            return Ok(DatagramSendProgress::BlockedNativeBuffer {
                needed: datagram.len(),
                available,
            });
        }

        match self.transport.try_send(datagram) {
            Ok(()) => {
                endpoint.commit_outbound_custody(flow.key(), transfer.accepted_index())?;
                Ok(DatagramSendProgress::Enqueued {
                    accepted_index: transfer.accepted_index(),
                })
            }
            Err(DatagramTransportError::TooLarge) => {
                self.drop_transport(endpoint, flow.key(), transfer.accepted_index())
            }
            Err(DatagramTransportError::UnsupportedByPeer | DatagramTransportError::Disabled) => {
                Err(DatagramSendError::ProfileUnavailable)
            }
            Err(DatagramTransportError::ConnectionLost) => Err(DatagramSendError::ConnectionLost),
        }
    }

    fn drop_transport(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        key: DeliveryFlowKey,
        accepted_index: u64,
    ) -> Result<DatagramSendProgress, DatagramSendError> {
        endpoint.commit_outbound_custody(key, accepted_index)?;
        self.diagnostics.outbound_transport_drops =
            self.diagnostics.outbound_transport_drops.saturating_add(1);
        Ok(DatagramSendProgress::DroppedTransport { accepted_index })
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum RegistrationUseError {
    UnknownFlowId,
    WrongDirection,
    ReliableFlow,
}

fn registered_outbound_unreliable(
    registry: &AcceptedFlowRegistry,
    flow_id: FlowId,
) -> Result<RegisteredFlow, RegistrationUseError> {
    let flow = registry
        .registered_flow(flow_id)
        .ok_or(RegistrationUseError::UnknownFlowId)?;
    if flow.key().direction() != FlowDirection::Outbound {
        return Err(RegistrationUseError::WrongDirection);
    }
    if flow.mode() == DeliveryMode::ReliableOrdered {
        return Err(RegistrationUseError::ReliableFlow);
    }
    Ok(flow)
}

const fn map_registration_submission_error(error: RegistrationUseError) -> DatagramSubmissionError {
    match error {
        RegistrationUseError::UnknownFlowId => DatagramSubmissionError::UnknownFlowId,
        RegistrationUseError::WrongDirection => DatagramSubmissionError::WrongDirection,
        RegistrationUseError::ReliableFlow => DatagramSubmissionError::ReliableFlow,
    }
}

const fn map_registration_send_error(error: RegistrationUseError) -> DatagramSendError {
    match error {
        RegistrationUseError::UnknownFlowId => DatagramSendError::UnknownFlowId,
        RegistrationUseError::WrongDirection => DatagramSendError::WrongDirection,
        RegistrationUseError::ReliableFlow => DatagramSendError::ReliableFlow,
    }
}

pub(super) fn datagram_len(
    flow_id: FlowId,
    mode: DeliveryMode,
    sequence: Option<u64>,
    payload_len: usize,
) -> Result<usize, DatagramSubmissionError> {
    let flow_len = encode_varint(flow_id.value())?.len();
    let sequence_len = match mode {
        DeliveryMode::UnreliableUnordered => 0,
        DeliveryMode::UnreliableSequenced => {
            let sequence = sequence.ok_or(DatagramSubmissionError::LengthOverflow)?;
            if sequence > MAX_VARINT {
                return Err(DatagramSubmissionError::SequenceExhausted);
            }
            encode_varint(sequence)?.len()
        }
        DeliveryMode::ReliableOrdered => return Err(DatagramSubmissionError::ReliableFlow),
    };
    flow_len
        .checked_add(sequence_len)
        .and_then(|len| len.checked_add(payload_len))
        .ok_or(DatagramSubmissionError::LengthOverflow)
}

fn datagram_len_for_send(
    flow_id: FlowId,
    mode: DeliveryMode,
    sequence: Option<u64>,
    payload_len: usize,
) -> Result<usize, DatagramSendError> {
    let flow_len = encode_varint(flow_id.value())?.len();
    let sequence_len = match mode {
        DeliveryMode::UnreliableUnordered => 0,
        DeliveryMode::UnreliableSequenced => {
            let sequence = sequence.ok_or(DatagramSendError::LengthOverflow)?;
            if sequence > MAX_VARINT {
                return Err(DatagramSendError::SequenceExhausted);
            }
            encode_varint(sequence)?.len()
        }
        DeliveryMode::ReliableOrdered => return Err(DatagramSendError::ReliableFlow),
    };
    flow_len
        .checked_add(sequence_len)
        .and_then(|len| len.checked_add(payload_len))
        .ok_or(DatagramSendError::LengthOverflow)
}

fn encode_datagram(
    flow_id: FlowId,
    mode: DeliveryMode,
    sequence: Option<u64>,
    payload: &[u8],
) -> Result<Vec<u8>, DatagramSendError> {
    let total = datagram_len_for_send(flow_id, mode, sequence, payload.len())?;
    let mut datagram = Vec::new();
    datagram
        .try_reserve_exact(total)
        .map_err(|_| DatagramSendError::AllocationFailed)?;
    datagram.extend_from_slice(encode_varint(flow_id.value())?.as_slice());
    match mode {
        DeliveryMode::UnreliableUnordered => {}
        DeliveryMode::UnreliableSequenced => datagram.extend_from_slice(
            encode_varint(sequence.ok_or(DatagramSendError::LengthOverflow)?)?.as_slice(),
        ),
        DeliveryMode::ReliableOrdered => return Err(DatagramSendError::ReliableFlow),
    }
    datagram.extend_from_slice(payload);
    debug_assert_eq!(datagram.len(), total);
    Ok(datagram)
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum DatagramReceiveOutcome {
    DiscardedUnknownFlow,
    Core(ReceiveOutcome),
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum DatagramReceiveError {
    VarInt(VarIntDecodeError),
    FlowId(FlowIdError),
    WrongDirection,
    ReliableFlow,
    PayloadExceedsProfile,
    AllocationFailed,
    Core(DeliveryOperationError),
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct DatagramReceiveFailure {
    pub(super) flow_id: Option<FlowId>,
    pub(super) error: DatagramReceiveError,
}

impl DatagramReceiveFailure {
    const fn unresolved(error: DatagramReceiveError) -> Self {
        Self {
            flow_id: None,
            error,
        }
    }

    const fn resolved(flow_id: FlowId, error: DatagramReceiveError) -> Self {
        Self {
            flow_id: Some(flow_id),
            error,
        }
    }
}

pub(super) fn receive_datagram(
    endpoint: &mut DeliveryEndpoint,
    registry: &AcceptedFlowRegistry,
    datagram: &[u8],
) -> Result<DatagramReceiveOutcome, DatagramReceiveFailure> {
    let (flow_value, flow_bytes) = decode_varint(datagram)
        .map_err(|error| DatagramReceiveFailure::unresolved(DatagramReceiveError::VarInt(error)))?;
    let flow_id = FlowId::from_wire(flow_value)
        .map_err(|error| DatagramReceiveFailure::unresolved(DatagramReceiveError::FlowId(error)))?;
    let Some(flow) = registry.registered_flow(flow_id) else {
        return Ok(DatagramReceiveOutcome::DiscardedUnknownFlow);
    };
    if flow.key().direction() != FlowDirection::Inbound {
        return Err(DatagramReceiveFailure::resolved(
            flow_id,
            DatagramReceiveError::WrongDirection,
        ));
    }
    if flow.mode() == DeliveryMode::ReliableOrdered {
        return Err(DatagramReceiveFailure::resolved(
            flow_id,
            DatagramReceiveError::ReliableFlow,
        ));
    }

    let (accepted_index, payload_offset) = match flow.mode() {
        DeliveryMode::UnreliableUnordered => (UNORDERED_INGRESS_INDEX, flow_bytes),
        DeliveryMode::UnreliableSequenced => {
            let (sequence, sequence_bytes) =
                decode_varint(&datagram[flow_bytes..]).map_err(|error| {
                    DatagramReceiveFailure::resolved(flow_id, DatagramReceiveError::VarInt(error))
                })?;
            (sequence, flow_bytes + sequence_bytes)
        }
        DeliveryMode::ReliableOrdered => {
            return Err(DatagramReceiveFailure::resolved(
                flow_id,
                DatagramReceiveError::ReliableFlow,
            ));
        }
    };
    let payload = &datagram[payload_offset..];
    if payload.len() > flow.max_message_bytes() {
        return Err(DatagramReceiveFailure::resolved(
            flow_id,
            DatagramReceiveError::PayloadExceedsProfile,
        ));
    }

    let mut owned = Vec::new();
    owned.try_reserve_exact(payload.len()).map_err(|_| {
        DatagramReceiveFailure::resolved(flow_id, DatagramReceiveError::AllocationFailed)
    })?;
    owned.extend_from_slice(payload);
    let outcome = endpoint
        .receive_transport_payload(flow.key(), accepted_index, owned)
        .map_err(|error| {
            DatagramReceiveFailure::resolved(flow_id, DatagramReceiveError::Core(error))
        })?;
    Ok(DatagramReceiveOutcome::Core(outcome))
}

pub(super) fn read_quinn_datagram_owned(connection: Connection) -> OwnedDatagramReadFuture {
    Box::pin(async move { connection.read_datagram().await })
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use runen_net::delivery::{
        DeliveryFlowHandle, DeliveryScopeLimits, FlowResourcePolicy, OutboundPressureBehavior,
        ReceiverPressureBehavior,
    };
    use runen_net::identity::ConnectionHandle;

    use crate::wire::WireSide;

    use super::*;

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    fn limits(flows: usize, messages: usize, bytes: usize) -> DeliveryScopeLimits {
        DeliveryScopeLimits::new(nz(flows), nz(messages), nz(bytes))
    }

    fn policy(
        max_message_bytes: usize,
        max_pending_messages: usize,
        max_pending_payload_bytes: usize,
        outbound_pressure: OutboundPressureBehavior,
        receiver_pressure: ReceiverPressureBehavior,
    ) -> FlowResourcePolicy {
        FlowResourcePolicy::new(
            nz(max_message_bytes),
            nz(max_pending_messages),
            nz(max_pending_payload_bytes),
            outbound_pressure,
            receiver_pressure,
        )
    }

    fn key(direction: FlowDirection, handle: u64) -> DeliveryFlowKey {
        DeliveryFlowKey::new(
            ConnectionHandle::new(1),
            direction,
            DeliveryFlowHandle::new(handle),
        )
    }

    fn endpoint_with_registered_flow(
        direction: FlowDirection,
        mode: DeliveryMode,
        handle: u64,
        flow_id: FlowId,
        flow_policy: FlowResourcePolicy,
        stable_max: usize,
    ) -> (DeliveryEndpoint, AcceptedFlowRegistry, DeliveryFlowKey) {
        let mut endpoint = DeliveryEndpoint::new(limits(16, 32, 4096));
        let key = key(direction, handle);
        endpoint
            .establish_flow(key, mode, flow_policy, limits(16, 32, 4096))
            .unwrap();
        let mut registry = AcceptedFlowRegistry::new(WireSide::Client, nz(16));
        registry
            .register_consumed_accepted_flow(&endpoint, flow_id, key, nz(stable_max))
            .unwrap();
        (endpoint, registry, key)
    }

    #[derive(Debug)]
    struct MockTransport {
        max_datagram_size: Option<usize>,
        send_buffer_space: usize,
        sent: Vec<Vec<u8>>,
        next_error: Option<DatagramTransportError>,
    }

    impl MockTransport {
        fn new(max_datagram_size: Option<usize>, send_buffer_space: usize) -> Self {
            Self {
                max_datagram_size,
                send_buffer_space,
                sent: Vec::new(),
                next_error: None,
            }
        }
    }

    impl DatagramSendTransport for MockTransport {
        fn max_datagram_size(&self) -> Option<usize> {
            self.max_datagram_size
        }

        fn send_buffer_space(&self) -> usize {
            self.send_buffer_space
        }

        fn try_send(&mut self, datagram: Vec<u8>) -> Result<(), DatagramTransportError> {
            if let Some(error) = self.next_error.take() {
                return Err(error);
            }
            self.sent.push(datagram);
            Ok(())
        }
    }

    fn assert_owned_send<T: Send + 'static>() {}

    #[test]
    fn owned_datagram_read_future_is_send_and_static() {
        assert_owned_send::<OwnedDatagramReadFuture>();
    }

    #[test]
    fn envelopes_use_minimal_varints_and_sequence_exhaustion_is_explicit() {
        let flow_id = FlowId::new(WireSide::Client, 32).unwrap();
        assert_eq!(
            encode_datagram(flow_id, DeliveryMode::UnreliableUnordered, None, b"x",).unwrap(),
            vec![0x40, 0x40, b'x']
        );
        assert_eq!(
            encode_datagram(flow_id, DeliveryMode::UnreliableSequenced, Some(64), b"x",).unwrap(),
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
    fn sequenced_preflight_uses_exact_core_candidate_and_mtu_rejection_consumes_nothing() {
        let flow_id = FlowId::new(WireSide::Client, 0).unwrap();
        let (mut endpoint, registry, key) = endpoint_with_registered_flow(
            FlowDirection::Outbound,
            DeliveryMode::UnreliableSequenced,
            1,
            flow_id,
            policy(
                64,
                4,
                256,
                OutboundPressureBehavior::RejectNew,
                ReceiverPressureBehavior::DropIncomingUnreliable,
            ),
            64,
        );
        let mut sender = DatagramSender::new(MockTransport::new(Some(4), 128));

        assert_eq!(endpoint.next_outbound_accepted_index(key).unwrap(), Some(0));
        assert_eq!(
            sender.submit(&mut endpoint, &registry, flow_id, vec![7, 8]),
            Ok(DatagramSubmissionOutcome::Accepted {
                accepted_index: 0,
                local_pressure_drops: 0,
            })
        );
        assert_eq!(endpoint.next_outbound_accepted_index(key).unwrap(), Some(1));
        assert_eq!(endpoint.pending_messages(), 1);

        sender.transport.max_datagram_size = Some(3);
        assert_eq!(
            sender.submit(&mut endpoint, &registry, flow_id, vec![9, 10]),
            Ok(DatagramSubmissionOutcome::RejectedCurrentDatagramSize)
        );
        assert_eq!(endpoint.next_outbound_accepted_index(key).unwrap(), Some(1));
        assert_eq!(endpoint.pending_messages(), 1);
    }

    #[test]
    fn native_buffer_blockage_keeps_core_custody_until_non_evicting_handoff() {
        let flow_id = FlowId::new(WireSide::Client, 1).unwrap();
        let (mut endpoint, registry, key) = endpoint_with_registered_flow(
            FlowDirection::Outbound,
            DeliveryMode::UnreliableUnordered,
            2,
            flow_id,
            policy(
                64,
                4,
                256,
                OutboundPressureBehavior::RejectNew,
                ReceiverPressureBehavior::DropIncomingUnreliable,
            ),
            64,
        );
        let mut sender = DatagramSender::new(MockTransport::new(Some(64), 0));
        assert!(matches!(
            sender.submit(&mut endpoint, &registry, flow_id, b"abc".to_vec()),
            Ok(DatagramSubmissionOutcome::Accepted {
                accepted_index: 0,
                ..
            })
        ));

        assert_eq!(
            sender.drive_one(&mut endpoint, &registry, flow_id),
            Ok(DatagramSendProgress::BlockedNativeBuffer {
                needed: 4,
                available: 0,
            })
        );
        assert_eq!(endpoint.pending_messages(), 1);
        assert!(sender.transport.sent.is_empty());

        sender.transport.send_buffer_space = 64;
        assert_eq!(
            sender.drive_one(&mut endpoint, &registry, flow_id),
            Ok(DatagramSendProgress::Enqueued { accepted_index: 0 })
        );
        assert_eq!(endpoint.pending_messages(), 0);
        assert_eq!(sender.transport.sent, vec![vec![2, b'a', b'b', b'c']]);
        assert_eq!(sender.outbound_transport_drops(), 0);
        assert_eq!(endpoint.flow_pending_usage(key), Some((0, 0)));
    }

    #[test]
    fn core_eviction_cannot_leave_a_stale_adapter_datagram() {
        let flow_id = FlowId::new(WireSide::Client, 2).unwrap();
        let (mut endpoint, registry, _) = endpoint_with_registered_flow(
            FlowDirection::Outbound,
            DeliveryMode::UnreliableSequenced,
            3,
            flow_id,
            policy(
                64,
                1,
                64,
                OutboundPressureBehavior::EvictOldestUnreliable,
                ReceiverPressureBehavior::DropIncomingUnreliable,
            ),
            64,
        );
        let mut sender = DatagramSender::new(MockTransport::new(Some(64), 0));
        assert!(matches!(
            sender.submit(&mut endpoint, &registry, flow_id, b"old".to_vec()),
            Ok(DatagramSubmissionOutcome::Accepted {
                accepted_index: 0,
                ..
            })
        ));
        assert!(matches!(
            sender.drive_one(&mut endpoint, &registry, flow_id),
            Ok(DatagramSendProgress::BlockedNativeBuffer { .. })
        ));
        assert_eq!(
            sender.submit(&mut endpoint, &registry, flow_id, b"new".to_vec()),
            Ok(DatagramSubmissionOutcome::Accepted {
                accepted_index: 1,
                local_pressure_drops: 1,
            })
        );

        sender.transport.send_buffer_space = 64;
        assert_eq!(
            sender.drive_one(&mut endpoint, &registry, flow_id),
            Ok(DatagramSendProgress::Enqueued { accepted_index: 1 })
        );
        assert_eq!(sender.transport.sent, vec![vec![4, 1, b'n', b'e', b'w']]);
    }

    #[test]
    fn mtu_shrink_after_acceptance_drops_only_front_and_is_observable() {
        let flow_id = FlowId::new(WireSide::Client, 3).unwrap();
        let (mut endpoint, registry, _) = endpoint_with_registered_flow(
            FlowDirection::Outbound,
            DeliveryMode::UnreliableUnordered,
            4,
            flow_id,
            policy(
                64,
                4,
                256,
                OutboundPressureBehavior::RejectNew,
                ReceiverPressureBehavior::DropIncomingUnreliable,
            ),
            64,
        );
        let mut sender = DatagramSender::new(MockTransport::new(Some(64), 64));
        sender
            .submit(&mut endpoint, &registry, flow_id, b"abc".to_vec())
            .unwrap();
        sender.transport.max_datagram_size = Some(3);

        assert_eq!(
            sender.drive_one(&mut endpoint, &registry, flow_id),
            Ok(DatagramSendProgress::DroppedTransport { accepted_index: 0 })
        );
        assert_eq!(endpoint.pending_messages(), 0);
        assert_eq!(sender.outbound_transport_drops(), 1);
        assert!(sender.transport.sent.is_empty());
    }

    #[test]
    fn too_large_race_after_checks_is_a_transport_drop_not_fallback() {
        let flow_id = FlowId::new(WireSide::Client, 4).unwrap();
        let (mut endpoint, registry, _) = endpoint_with_registered_flow(
            FlowDirection::Outbound,
            DeliveryMode::UnreliableUnordered,
            5,
            flow_id,
            policy(
                64,
                4,
                256,
                OutboundPressureBehavior::RejectNew,
                ReceiverPressureBehavior::DropIncomingUnreliable,
            ),
            64,
        );
        let mut sender = DatagramSender::new(MockTransport::new(Some(64), 64));
        sender
            .submit(&mut endpoint, &registry, flow_id, b"abc".to_vec())
            .unwrap();
        sender.transport.next_error = Some(DatagramTransportError::TooLarge);

        assert_eq!(
            sender.drive_one(&mut endpoint, &registry, flow_id),
            Ok(DatagramSendProgress::DroppedTransport { accepted_index: 0 })
        );
        assert_eq!(sender.outbound_transport_drops(), 1);
        assert!(sender.transport.sent.is_empty());
    }

    #[test]
    fn inbound_unordered_uses_no_sequence_authority_and_core_owns_pressure() {
        let flow_id = FlowId::new(WireSide::Server, 0).unwrap();
        let (mut endpoint, registry, key) = endpoint_with_registered_flow(
            FlowDirection::Inbound,
            DeliveryMode::UnreliableUnordered,
            6,
            flow_id,
            policy(
                8,
                1,
                8,
                OutboundPressureBehavior::RejectNew,
                ReceiverPressureBehavior::DropIncomingUnreliable,
            ),
            8,
        );

        assert_eq!(
            receive_datagram(&mut endpoint, &registry, &[1, b'a']),
            Ok(DatagramReceiveOutcome::Core(ReceiveOutcome::Buffered {
                local_pressure_drops: 0,
            }))
        );
        assert_eq!(
            receive_datagram(&mut endpoint, &registry, &[1, b'b']),
            Ok(DatagramReceiveOutcome::Core(
                ReceiveOutcome::DroppedByPressure {
                    local_pressure_drops: 1,
                }
            ))
        );
        let exposed = endpoint.poll_exposure(key).unwrap().unwrap();
        assert_eq!(exposed.accepted_index(), UNORDERED_INGRESS_INDEX);
        assert_eq!(exposed.payload(), b"a");
        assert_eq!(endpoint.diagnostics().inbound_unreliable_pressure_drops, 1);
    }

    #[test]
    fn inbound_sequenced_preserves_wire_sequence_and_core_stale_filtering() {
        let flow_id = FlowId::new(WireSide::Server, 1).unwrap();
        let (mut endpoint, registry, key) = endpoint_with_registered_flow(
            FlowDirection::Inbound,
            DeliveryMode::UnreliableSequenced,
            7,
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

        assert_eq!(
            receive_datagram(&mut endpoint, &registry, &[3, 2, b'n']),
            Ok(DatagramReceiveOutcome::Core(ReceiveOutcome::Buffered {
                local_pressure_drops: 0,
            }))
        );
        let exposed = endpoint.poll_exposure(key).unwrap().unwrap();
        assert_eq!(exposed.accepted_index(), 2);
        assert_eq!(exposed.payload(), b"n");
        assert_eq!(
            receive_datagram(&mut endpoint, &registry, &[3, 1, b'o']),
            Ok(DatagramReceiveOutcome::Core(ReceiveOutcome::StaleSequenced))
        );
        assert_eq!(endpoint.diagnostics().stale_sequenced_drops, 1);
    }

    #[test]
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
            Err(DatagramReceiveFailure {
                flow_id: Some(flow_id),
                error: DatagramReceiveError::VarInt(VarIntDecodeError::NonMinimal),
            })
        );
        assert_eq!(endpoint.pending_messages(), 0);
    }

    #[test]
    fn inbound_rejects_malformed_wrong_mode_direction_and_profile_oversize() {
        let reliable_flow = FlowId::new(WireSide::Server, 2).unwrap();
        let (mut reliable_endpoint, reliable_registry, _) = endpoint_with_registered_flow(
            FlowDirection::Inbound,
            DeliveryMode::ReliableOrdered,
            8,
            reliable_flow,
            FlowResourcePolicy::new(
                nz(8),
                nz(4),
                nz(32),
                OutboundPressureBehavior::RejectNew,
                ReceiverPressureBehavior::TerminateReliable,
            ),
            8,
        );
        assert_eq!(
            receive_datagram(&mut reliable_endpoint, &reliable_registry, &[5, b'x']),
            Err(DatagramReceiveFailure {
                flow_id: Some(reliable_flow),
                error: DatagramReceiveError::ReliableFlow,
            })
        );

        let outbound_flow = FlowId::new(WireSide::Client, 3).unwrap();
        let (mut outbound_endpoint, outbound_registry, _) = endpoint_with_registered_flow(
            FlowDirection::Outbound,
            DeliveryMode::UnreliableUnordered,
            9,
            outbound_flow,
            policy(
                8,
                4,
                32,
                OutboundPressureBehavior::RejectNew,
                ReceiverPressureBehavior::DropIncomingUnreliable,
            ),
            8,
        );
        assert_eq!(
            receive_datagram(&mut outbound_endpoint, &outbound_registry, &[6, b'x']),
            Err(DatagramReceiveFailure {
                flow_id: Some(outbound_flow),
                error: DatagramReceiveError::WrongDirection,
            })
        );

        assert!(matches!(
            receive_datagram(&mut outbound_endpoint, &outbound_registry, &[0x40, 0x06]),
            Err(DatagramReceiveFailure {
                flow_id: None,
                error: DatagramReceiveError::VarInt(VarIntDecodeError::NonMinimal),
            })
        ));

        let inbound_flow = FlowId::new(WireSide::Server, 4).unwrap();
        let (mut inbound_endpoint, inbound_registry, _) = endpoint_with_registered_flow(
            FlowDirection::Inbound,
            DeliveryMode::UnreliableUnordered,
            10,
            inbound_flow,
            policy(
                2,
                4,
                16,
                OutboundPressureBehavior::RejectNew,
                ReceiverPressureBehavior::DropIncomingUnreliable,
            ),
            2,
        );
        assert_eq!(
            receive_datagram(&mut inbound_endpoint, &inbound_registry, &[9, 1, 2, 3]),
            Err(DatagramReceiveFailure {
                flow_id: Some(inbound_flow),
                error: DatagramReceiveError::PayloadExceedsProfile,
            })
        );

        let unknown_flow = FlowId::new(WireSide::Server, 7).unwrap();
        let encoded = encode_varint(unknown_flow.value()).unwrap();
        assert_eq!(
            receive_datagram(&mut inbound_endpoint, &inbound_registry, encoded.as_slice()),
            Ok(DatagramReceiveOutcome::DiscardedUnknownFlow)
        );
    }
}
