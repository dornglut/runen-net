use std::collections::HashMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{Context, Poll};

use quinn::{RecvStream, SendStream, VarInt};
use runen_net::delivery::{
    CustodyCommitError, DeliveryEndpoint, DeliveryFlowKey, DeliveryMode, DeliveryOperationError,
    DeliveryTransfer, FlowDirection, FlowTerminationReason, ReceiveOutcome,
};

use super::reliable::{ReliableFrameDecoder, ReliableFrameError, encode_payload_length};
use super::wire::{
    EncodedVarInt, FlowId, FlowIdError, VarIntDecodeError, WireSide, decode_varint, encode_varint,
};

const PROFILE_PROTOCOL_ERROR: VarInt = VarInt::from_u32(1);
const RESOURCE_LIMIT_ERROR: VarInt = VarInt::from_u32(3);
const FLOW_PROTOCOL_ERROR: VarInt = VarInt::from_u32(5);
const RELIABLE_DELIVERY_FAILED: VarInt = VarInt::from_u32(6);

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ReliableAssociationState {
    Unbound,
    Outbound,
    Inbound,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) struct RegisteredFlow {
    key: DeliveryFlowKey,
    mode: DeliveryMode,
    max_message_bytes: usize,
    reliable_association: Option<ReliableAssociationState>,
}

impl RegisteredFlow {
    pub(super) const fn key(self) -> DeliveryFlowKey {
        self.key
    }

    pub(super) const fn mode(self) -> DeliveryMode {
        self.mode
    }

    pub(super) const fn max_message_bytes(self) -> usize {
        self.max_message_bytes
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum RegistryError {
    CapacityExceeded,
    AllocationFailed,
    DuplicateFlowId,
    UnknownFlowId,
    UnknownCoreFlow,
    NotReliable,
    OutboundMessageLimitExceedsProfile,
    InboundMessageLimitCannotSupportProfile,
    WrongFlowSide,
    WrongDirection,
    AlreadyAssociated,
}

#[derive(Debug)]
pub(super) struct AcceptedFlowRegistry {
    local_side: WireSide,
    max_active: usize,
    flows: HashMap<u64, RegisteredFlow>,
}

impl AcceptedFlowRegistry {
    pub(super) fn new(local_side: WireSide, max_active: NonZeroUsize) -> Self {
        Self {
            local_side,
            max_active: max_active.get(),
            flows: HashMap::new(),
        }
    }

    /// Register a FlowId already consumed and accepted by the C1/control authority.
    ///
    /// This registry owns the finite connection-scoped FlowId to Core-flow mapping.
    /// Reliable flows additionally acquire one persistent stream association later.
    /// It does not allocate, recycle, or retain retired FlowIds.
    pub(super) fn register_consumed_accepted_flow(
        &mut self,
        endpoint: &DeliveryEndpoint,
        flow_id: FlowId,
        key: DeliveryFlowKey,
        stable_max_message_bytes: NonZeroUsize,
    ) -> Result<(), RegistryError> {
        if self.flows.contains_key(&flow_id.value()) {
            return Err(RegistryError::DuplicateFlowId);
        }
        if self.flows.len() >= self.max_active {
            return Err(RegistryError::CapacityExceeded);
        }
        let (mode, policy) = endpoint
            .flow_contract(key)
            .ok_or(RegistryError::UnknownCoreFlow)?;
        let stable_max_message_bytes = stable_max_message_bytes.get();
        match key.direction() {
            FlowDirection::Outbound if policy.max_message_bytes() > stable_max_message_bytes => {
                return Err(RegistryError::OutboundMessageLimitExceedsProfile);
            }
            FlowDirection::Inbound if policy.max_message_bytes() < stable_max_message_bytes => {
                return Err(RegistryError::InboundMessageLimitCannotSupportProfile);
            }
            FlowDirection::Outbound | FlowDirection::Inbound => {}
        }
        let expected_side = match key.direction() {
            FlowDirection::Outbound => self.local_side,
            FlowDirection::Inbound => opposite_side(self.local_side),
        };
        if flow_id.side() != expected_side {
            return Err(RegistryError::WrongFlowSide);
        }
        self.flows
            .try_reserve(1)
            .map_err(|_| RegistryError::AllocationFailed)?;
        self.flows.insert(
            flow_id.value(),
            RegisteredFlow {
                key,
                mode,
                max_message_bytes: stable_max_message_bytes,
                reliable_association: match mode {
                    DeliveryMode::ReliableOrdered => Some(ReliableAssociationState::Unbound),
                    DeliveryMode::UnreliableUnordered | DeliveryMode::UnreliableSequenced => None,
                },
            },
        );
        Ok(())
    }

    pub(super) fn registered_flow(&self, flow_id: FlowId) -> Option<RegisteredFlow> {
        self.flows.get(&flow_id.value()).copied()
    }

    pub(super) fn active_direction_len(&self, direction: FlowDirection) -> usize {
        self.flows
            .values()
            .filter(|flow| flow.key.direction() == direction)
            .count()
    }

    fn associate_outbound(&mut self, flow_id: FlowId) -> Result<RegisteredFlow, RegistryError> {
        self.associate(
            flow_id,
            FlowDirection::Outbound,
            ReliableAssociationState::Outbound,
        )
    }

    fn associate_inbound(&mut self, flow_id: FlowId) -> Result<RegisteredFlow, RegistryError> {
        self.associate(
            flow_id,
            FlowDirection::Inbound,
            ReliableAssociationState::Inbound,
        )
    }

    fn associate(
        &mut self,
        flow_id: FlowId,
        direction: FlowDirection,
        target: ReliableAssociationState,
    ) -> Result<RegisteredFlow, RegistryError> {
        let flow = self
            .flows
            .get_mut(&flow_id.value())
            .ok_or(RegistryError::UnknownFlowId)?;
        if flow.mode != DeliveryMode::ReliableOrdered {
            return Err(RegistryError::NotReliable);
        }
        if flow.key.direction() != direction {
            return Err(RegistryError::WrongDirection);
        }
        match flow.reliable_association {
            Some(ReliableAssociationState::Unbound) => {
                flow.reliable_association = Some(target);
                Ok(*flow)
            }
            Some(ReliableAssociationState::Outbound | ReliableAssociationState::Inbound) => {
                Err(RegistryError::AlreadyAssociated)
            }
            None => Err(RegistryError::NotReliable),
        }
    }

    pub(super) fn release(&mut self, flow_id: FlowId) {
        self.flows.remove(&flow_id.value());
    }

    #[cfg(test)]
    fn active_len(&self) -> usize {
        self.flows.len()
    }
}

const fn opposite_side(side: WireSide) -> WireSide {
    match side {
        WireSide::Client => WireSide::Server,
        WireSide::Server => WireSide::Client,
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum IoFailure {
    Read,
    Write,
}

type FinishAckFuture =
    Pin<Box<dyn Future<Output = Result<Option<VarInt>, IoFailure>> + Send + Sync + 'static>>;

trait PollWriteReliable {
    fn poll_write_step(
        &mut self,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<Result<usize, IoFailure>>;
    fn reset_reliable(&mut self, code: VarInt);
    fn finish_reliable(&mut self) -> Result<(), IoFailure>;
    fn finish_ack_future(&self) -> FinishAckFuture;
}

impl PollWriteReliable for SendStream {
    fn poll_write_step(
        &mut self,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<Result<usize, IoFailure>> {
        Pin::new(self)
            .poll_write(cx, bytes)
            .map(|result| result.map_err(|_| IoFailure::Write))
    }

    fn reset_reliable(&mut self, code: VarInt) {
        let _ = self.reset(code);
    }

    fn finish_reliable(&mut self) -> Result<(), IoFailure> {
        self.finish().map_err(|_| IoFailure::Write)
    }

    fn finish_ack_future(&self) -> FinishAckFuture {
        let future = self.stopped();
        Box::pin(async move { future.await.map_err(|_| IoFailure::Write) })
    }
}

trait PollReadReliable {
    fn is_zero_rtt(&self) -> bool;
    fn poll_read_step(
        &mut self,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<Result<usize, IoFailure>>;
    fn stop_reliable(&mut self, code: VarInt);
}

impl PollReadReliable for RecvStream {
    fn is_zero_rtt(&self) -> bool {
        self.is_0rtt()
    }

    fn poll_read_step(
        &mut self,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<Result<usize, IoFailure>> {
        self.poll_read(cx, bytes)
            .map(|result| result.map_err(|_| IoFailure::Read))
    }

    fn stop_reliable(&mut self, code: VarInt) {
        let _ = self.stop(code);
    }
}

#[derive(Debug, PartialEq, Eq)]
enum SendError {
    Registry(RegistryError),
    Core(DeliveryOperationError),
    Custody(CustodyCommitError),
    Framing(ReliableFrameError),
    UnexpectedAcceptedIndex { expected: u64, actual: u64 },
    AcceptedIndexExhausted,
    InvalidWriteCount,
    WriteZero,
    Io(IoFailure),
    PendingData,
    AlreadyFinishing,
    Terminal,
}

impl From<DeliveryOperationError> for SendError {
    fn from(error: DeliveryOperationError) -> Self {
        Self::Core(error)
    }
}

impl From<CustodyCommitError> for SendError {
    fn from(error: CustodyCommitError) -> Self {
        Self::Custody(error)
    }
}

impl From<ReliableFrameError> for SendError {
    fn from(error: ReliableFrameError) -> Self {
        Self::Framing(error)
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum SendProgress {
    Progressed { bytes: usize },
    Committed { accepted_index: u64 },
    Idle,
    Closed,
}

#[derive(Debug)]
struct OutboundMessage {
    snapshot: DeliveryTransfer,
    length: EncodedVarInt,
    length_offset: usize,
    payload_offset: usize,
}

#[derive(Debug)]
struct OutboundState {
    flow_header: EncodedVarInt,
    flow_header_offset: usize,
    max_message_bytes: usize,
    next_accepted_index: Option<u64>,
    current: Option<OutboundMessage>,
}

impl OutboundState {
    fn new(flow_id: FlowId, max_message_bytes: usize) -> Result<Self, SendError> {
        let flow_header = encode_varint(flow_id.value())
            .map_err(|_| SendError::Registry(RegistryError::UnknownFlowId))?;
        Ok(Self {
            flow_header,
            flow_header_offset: 0,
            max_message_bytes,
            next_accepted_index: Some(0),
            current: None,
        })
    }

    fn next_segment<'a>(
        &'a mut self,
        endpoint: &DeliveryEndpoint,
        key: DeliveryFlowKey,
    ) -> Result<Option<&'a [u8]>, SendError> {
        if self.flow_header_offset < self.flow_header.len() {
            return Ok(Some(
                &self.flow_header.as_slice()[self.flow_header_offset..],
            ));
        }
        if self.current.is_none() {
            let Some(snapshot) = endpoint.peek_outbound(key)? else {
                return Ok(None);
            };
            if snapshot.mode() != DeliveryMode::ReliableOrdered {
                return Err(SendError::Registry(RegistryError::NotReliable));
            }
            let expected = self
                .next_accepted_index
                .ok_or(SendError::AcceptedIndexExhausted)?;
            if snapshot.accepted_index() != expected {
                return Err(SendError::UnexpectedAcceptedIndex {
                    expected,
                    actual: snapshot.accepted_index(),
                });
            }
            self.current = Some(OutboundMessage {
                length: encode_payload_length(snapshot.payload_len(), self.max_message_bytes)?,
                snapshot,
                length_offset: 0,
                payload_offset: 0,
            });
        }
        let current = self.current.as_ref().expect("message loaded above");
        if current.length_offset < current.length.len() {
            return Ok(Some(&current.length.as_slice()[current.length_offset..]));
        }
        if current.payload_offset < current.snapshot.payload_len() {
            return Ok(Some(&current.snapshot.payload()[current.payload_offset..]));
        }
        unreachable!("completed message is committed by advance")
    }

    fn advance(
        &mut self,
        bytes: usize,
        endpoint: &mut DeliveryEndpoint,
        key: DeliveryFlowKey,
    ) -> Result<Option<u64>, SendError> {
        if bytes == 0 {
            return Err(SendError::WriteZero);
        }
        if self.flow_header_offset < self.flow_header.len() {
            let remaining = self.flow_header.len() - self.flow_header_offset;
            if bytes > remaining {
                return Err(SendError::InvalidWriteCount);
            }
            self.flow_header_offset += bytes;
            return Ok(None);
        }
        let current = self
            .current
            .as_mut()
            .expect("segment loaded before advance");
        if current.length_offset < current.length.len() {
            let remaining = current.length.len() - current.length_offset;
            if bytes > remaining {
                return Err(SendError::InvalidWriteCount);
            }
            current.length_offset += bytes;
            if current.length_offset == current.length.len() && current.snapshot.payload_len() == 0
            {
                return self.commit_current(endpoint, key).map(Some);
            }
            return Ok(None);
        }
        let remaining = current.snapshot.payload_len() - current.payload_offset;
        if bytes > remaining {
            return Err(SendError::InvalidWriteCount);
        }
        current.payload_offset += bytes;
        if current.payload_offset == current.snapshot.payload_len() {
            return self.commit_current(endpoint, key).map(Some);
        }
        Ok(None)
    }

    fn commit_current(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        key: DeliveryFlowKey,
    ) -> Result<u64, SendError> {
        let current = self.current.take().expect("completed message exists");
        let accepted_index = current.snapshot.accepted_index();
        let _ = endpoint.commit_outbound_custody(key, accepted_index)?;
        self.next_accepted_index = accepted_index.checked_add(1);
        Ok(accepted_index)
    }

    #[cfg(test)]
    fn exhaust_indices(&mut self) {
        self.next_accepted_index = None;
    }
}

struct OutboundReliable<W> {
    flow_id: FlowId,
    key: DeliveryFlowKey,
    writer: W,
    state: OutboundState,
    finish_ack: Option<FinishAckFuture>,
    terminal: bool,
}

impl OutboundReliable<SendStream> {
    fn bind_quinn(
        registry: &mut AcceptedFlowRegistry,
        flow_id: FlowId,
        stream: SendStream,
    ) -> Result<Self, SendError> {
        Self::bind(registry, flow_id, stream)
    }
}

impl<W: PollWriteReliable> OutboundReliable<W> {
    fn bind(
        registry: &mut AcceptedFlowRegistry,
        flow_id: FlowId,
        writer: W,
    ) -> Result<Self, SendError> {
        let flow = registry
            .associate_outbound(flow_id)
            .map_err(SendError::Registry)?;
        Ok(Self {
            flow_id,
            key: flow.key,
            writer,
            state: OutboundState::new(flow_id, flow.max_message_bytes)?,
            finish_ack: None,
            terminal: false,
        })
    }

    fn poll_step(
        &mut self,
        cx: &mut Context<'_>,
        endpoint: &mut DeliveryEndpoint,
        registry: &mut AcceptedFlowRegistry,
    ) -> Poll<Result<SendProgress, SendError>> {
        if self.terminal {
            return Poll::Ready(Err(SendError::Terminal));
        }
        if endpoint.flow_contract(self.key).is_none() {
            return self.fail(
                endpoint,
                registry,
                RELIABLE_DELIVERY_FAILED,
                SendError::Core(DeliveryOperationError::UnknownFlow),
            );
        }
        if let Some(finish_ack) = self.finish_ack.as_mut() {
            match endpoint.peek_outbound_metadata(self.key) {
                Ok(Some(_)) => {
                    return self.fail(
                        endpoint,
                        registry,
                        RELIABLE_DELIVERY_FAILED,
                        SendError::PendingData,
                    );
                }
                Ok(None) => {}
                Err(error) => {
                    return self.fail(
                        endpoint,
                        registry,
                        RELIABLE_DELIVERY_FAILED,
                        SendError::Core(error),
                    );
                }
            }
            return match finish_ack.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(None)) => {
                    if let Err(error) =
                        endpoint.terminate_flow(self.key, FlowTerminationReason::Requested)
                    {
                        return self.fail(
                            endpoint,
                            registry,
                            RELIABLE_DELIVERY_FAILED,
                            SendError::Core(error),
                        );
                    }
                    registry.release(self.flow_id);
                    self.finish_ack = None;
                    self.terminal = true;
                    Poll::Ready(Ok(SendProgress::Closed))
                }
                Poll::Ready(Ok(Some(_))) | Poll::Ready(Err(_)) => self.fail(
                    endpoint,
                    registry,
                    RELIABLE_DELIVERY_FAILED,
                    SendError::Io(IoFailure::Write),
                ),
            };
        }
        let segment = match self.state.next_segment(endpoint, self.key) {
            Ok(Some(segment)) => segment,
            Ok(None) => return Poll::Ready(Ok(SendProgress::Idle)),
            Err(error) => return self.fail(endpoint, registry, FLOW_PROTOCOL_ERROR, error),
        };
        match self.writer.poll_write_step(cx, segment) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => self.fail(
                endpoint,
                registry,
                RELIABLE_DELIVERY_FAILED,
                SendError::Io(error),
            ),
            Poll::Ready(Ok(0)) => self.fail(
                endpoint,
                registry,
                RELIABLE_DELIVERY_FAILED,
                SendError::WriteZero,
            ),
            Poll::Ready(Ok(bytes)) => match self.state.advance(bytes, endpoint, self.key) {
                Ok(Some(accepted_index)) => {
                    Poll::Ready(Ok(SendProgress::Committed { accepted_index }))
                }
                Ok(None) => Poll::Ready(Ok(SendProgress::Progressed { bytes })),
                Err(error) => self.fail(endpoint, registry, RELIABLE_DELIVERY_FAILED, error),
            },
        }
    }

    fn request_finish_normal(&mut self, endpoint: &DeliveryEndpoint) -> Result<(), SendError> {
        if self.terminal {
            return Err(SendError::Terminal);
        }
        if self.finish_ack.is_some() {
            return Err(SendError::AlreadyFinishing);
        }
        if endpoint.flow_contract(self.key).is_none() {
            return Err(SendError::Core(DeliveryOperationError::UnknownFlow));
        }
        if self.state.next_segment(endpoint, self.key)?.is_some() {
            return Err(SendError::PendingData);
        }
        self.writer.finish_reliable().map_err(SendError::Io)?;
        self.finish_ack = Some(self.writer.finish_ack_future());
        Ok(())
    }

    fn fail(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        registry: &mut AcceptedFlowRegistry,
        code: VarInt,
        error: SendError,
    ) -> Poll<Result<SendProgress, SendError>> {
        if !self.terminal {
            self.writer.reset_reliable(code);
            let _ = endpoint.fail_reliable_custody(self.key);
            registry.release(self.flow_id);
            self.terminal = true;
        }
        Poll::Ready(Err(error))
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum PrefixError {
    VarInt(VarIntDecodeError),
    FlowId(FlowIdError),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct PrefixConsume {
    consumed: usize,
    flow_id: Option<FlowId>,
}

#[derive(Debug)]
struct FlowIdPrefix {
    bytes: [u8; 8],
    filled: usize,
    needed: usize,
}

impl FlowIdPrefix {
    const fn new() -> Self {
        Self {
            bytes: [0; 8],
            filled: 0,
            needed: 0,
        }
    }

    fn consume(&mut self, input: &[u8]) -> Result<PrefixConsume, PrefixError> {
        if self.filled == 0 {
            let Some(&first) = input.first() else {
                return Ok(PrefixConsume {
                    consumed: 0,
                    flow_id: None,
                });
            };
            self.needed = 1usize << (first >> 6);
        }
        let take = (self.needed - self.filled).min(input.len());
        self.bytes[self.filled..self.filled + take].copy_from_slice(&input[..take]);
        self.filled += take;
        if self.filled != self.needed {
            return Ok(PrefixConsume {
                consumed: take,
                flow_id: None,
            });
        }
        let (value, decoded) =
            decode_varint(&self.bytes[..self.needed]).map_err(PrefixError::VarInt)?;
        debug_assert_eq!(decoded, self.needed);
        Ok(PrefixConsume {
            consumed: take,
            flow_id: Some(FlowId::from_wire(value).map_err(PrefixError::FlowId)?),
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ReceiveError {
    Registry(RegistryError),
    Prefix(PrefixError),
    Framing(ReliableFrameError),
    Core(DeliveryOperationError),
    Io(IoFailure),
    ZeroRtt,
    TruncatedAssociation,
    AdapterStagingBelowFlowMaximum {
        max_message_bytes: usize,
        max_staging_bytes: usize,
    },
    AcceptedIndexExhausted,
    UnexpectedCoreOutcome(ReceiveOutcome),
    AllocationFailed,
    Terminal,
}

impl From<ReliableFrameError> for ReceiveError {
    fn from(error: ReliableFrameError) -> Self {
        Self::Framing(error)
    }
}

impl From<DeliveryOperationError> for ReceiveError {
    fn from(error: DeliveryOperationError) -> Self {
        Self::Core(error)
    }
}

#[derive(Debug)]
struct InboundFrames {
    decoder: ReliableFrameDecoder,
    next_accepted_index: Option<u64>,
}

impl InboundFrames {
    fn new(max_message_bytes: usize, max_staging_bytes: usize) -> Result<Self, ReliableFrameError> {
        Ok(Self {
            decoder: ReliableFrameDecoder::new(max_message_bytes, max_staging_bytes)?,
            next_accepted_index: Some(0),
        })
    }

    fn consume(
        &mut self,
        input: &[u8],
        endpoint: &mut DeliveryEndpoint,
        key: DeliveryFlowKey,
    ) -> Result<usize, ReceiveError> {
        let mut offset = 0usize;
        let mut messages = 0usize;
        while offset < input.len() {
            let result = self.decoder.consume(&input[offset..])?;
            if result.consumed == 0 && result.message.is_none() {
                break;
            }
            offset += result.consumed;
            if let Some(message) = result.message {
                let accepted_index = self
                    .next_accepted_index
                    .ok_or(ReceiveError::AcceptedIndexExhausted)?;
                let outcome = endpoint.receive_transport_payload(key, accepted_index, message)?;
                if outcome
                    != (ReceiveOutcome::Buffered {
                        local_pressure_drops: 0,
                    })
                {
                    return Err(ReceiveError::UnexpectedCoreOutcome(outcome));
                }
                self.next_accepted_index = accepted_index.checked_add(1);
                messages += 1;
            }
        }
        Ok(messages)
    }

    fn finish(&self) -> Result<(), ReliableFrameError> {
        self.decoder.finish()
    }

    #[cfg(test)]
    fn exhaust_indices(&mut self) {
        self.next_accepted_index = None;
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ReceiveProgress {
    Progressed { bytes: usize },
    Associated { flow_id: FlowId },
    MessagesBuffered { count: usize },
    Draining,
    Closed,
}

#[derive(Debug)]
struct InboundReliable<R> {
    reader: R,
    scratch: Vec<u8>,
    max_staging_bytes: usize,
    prefix: FlowIdPrefix,
    flow_id: Option<FlowId>,
    flow: Option<RegisteredFlow>,
    frames: Option<InboundFrames>,
    draining: bool,
    terminal: bool,
}

impl InboundReliable<RecvStream> {
    fn bind_quinn(
        stream: RecvStream,
        scratch_bytes: NonZeroUsize,
        max_staging_bytes: NonZeroUsize,
    ) -> Result<Self, ReceiveError> {
        Self::new(stream, scratch_bytes, max_staging_bytes)
    }
}

impl<R: PollReadReliable> InboundReliable<R> {
    fn new(
        mut reader: R,
        scratch_bytes: NonZeroUsize,
        max_staging_bytes: NonZeroUsize,
    ) -> Result<Self, ReceiveError> {
        if reader.is_zero_rtt() {
            reader.stop_reliable(PROFILE_PROTOCOL_ERROR);
            return Err(ReceiveError::ZeroRtt);
        }
        let mut scratch = Vec::new();
        scratch
            .try_reserve_exact(scratch_bytes.get())
            .map_err(|_| ReceiveError::AllocationFailed)?;
        scratch.resize(scratch_bytes.get(), 0);
        Ok(Self {
            reader,
            scratch,
            max_staging_bytes: max_staging_bytes.get(),
            prefix: FlowIdPrefix::new(),
            flow_id: None,
            flow: None,
            frames: None,
            draining: false,
            terminal: false,
        })
    }

    fn poll_step(
        &mut self,
        cx: &mut Context<'_>,
        endpoint: &mut DeliveryEndpoint,
        registry: &mut AcceptedFlowRegistry,
    ) -> Poll<Result<ReceiveProgress, ReceiveError>> {
        if self.terminal {
            return Poll::Ready(Err(ReceiveError::Terminal));
        }
        if self.draining {
            return Poll::Ready(self.drain(endpoint, registry));
        }
        let read = match self.reader.poll_read_step(cx, &mut self.scratch) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Err(error)) => {
                return self.fail(
                    endpoint,
                    registry,
                    RELIABLE_DELIVERY_FAILED,
                    ReceiveError::Io(error),
                );
            }
            Poll::Ready(Ok(read)) => read,
        };
        if read == 0 {
            if self.flow.is_none() {
                self.reader.stop_reliable(FLOW_PROTOCOL_ERROR);
                self.terminal = true;
                return Poll::Ready(Err(ReceiveError::TruncatedAssociation));
            }
            if let Err(error) = self.frames.as_ref().expect("associated decoder").finish() {
                return self.fail(
                    endpoint,
                    registry,
                    frame_error_code(&error),
                    ReceiveError::Framing(error),
                );
            }
            self.draining = true;
            return Poll::Ready(self.drain(endpoint, registry));
        }

        let mut offset = 0usize;
        if self.flow.is_none() {
            let prefix = match self.prefix.consume(&self.scratch[..read]) {
                Ok(prefix) => prefix,
                Err(error) => {
                    self.reader.stop_reliable(FLOW_PROTOCOL_ERROR);
                    self.terminal = true;
                    return Poll::Ready(Err(ReceiveError::Prefix(error)));
                }
            };
            offset = prefix.consumed;
            let Some(flow_id) = prefix.flow_id else {
                return Poll::Ready(Ok(ReceiveProgress::Progressed { bytes: read }));
            };
            let flow = match registry.associate_inbound(flow_id) {
                Ok(flow) => flow,
                Err(error) => {
                    self.reader.stop_reliable(FLOW_PROTOCOL_ERROR);
                    self.terminal = true;
                    return Poll::Ready(Err(ReceiveError::Registry(error)));
                }
            };
            self.flow_id = Some(flow_id);
            self.flow = Some(flow);
            if self.max_staging_bytes < flow.max_message_bytes {
                return self.fail(
                    endpoint,
                    registry,
                    RESOURCE_LIMIT_ERROR,
                    ReceiveError::AdapterStagingBelowFlowMaximum {
                        max_message_bytes: flow.max_message_bytes,
                        max_staging_bytes: self.max_staging_bytes,
                    },
                );
            }
            let frames = match InboundFrames::new(flow.max_message_bytes, self.max_staging_bytes) {
                Ok(frames) => frames,
                Err(error) => {
                    return self.fail(
                        endpoint,
                        registry,
                        frame_error_code(&error),
                        ReceiveError::Framing(error),
                    );
                }
            };
            self.frames = Some(frames);
            if offset == read {
                return Poll::Ready(Ok(ReceiveProgress::Associated { flow_id }));
            }
        }

        let flow = self.flow.expect("flow associated above");
        match self.frames.as_mut().expect("associated decoder").consume(
            &self.scratch[offset..read],
            endpoint,
            flow.key,
        ) {
            Ok(0) => Poll::Ready(Ok(ReceiveProgress::Progressed { bytes: read })),
            Ok(count) => Poll::Ready(Ok(ReceiveProgress::MessagesBuffered { count })),
            Err(error) => self.fail(endpoint, registry, receive_error_code(&error), error),
        }
    }

    fn drain(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        registry: &mut AcceptedFlowRegistry,
    ) -> Result<ReceiveProgress, ReceiveError> {
        let flow = self.flow.ok_or(ReceiveError::Terminal)?;
        match endpoint.flow_pending_usage(flow.key) {
            Some((0, 0)) => {
                endpoint.terminate_flow(flow.key, FlowTerminationReason::Requested)?;
                if let Some(flow_id) = self.flow_id {
                    registry.release(flow_id);
                }
                self.terminal = true;
                Ok(ReceiveProgress::Closed)
            }
            Some(_) => Ok(ReceiveProgress::Draining),
            None => {
                if let Some(flow_id) = self.flow_id {
                    registry.release(flow_id);
                }
                self.terminal = true;
                Ok(ReceiveProgress::Closed)
            }
        }
    }

    fn fail(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        registry: &mut AcceptedFlowRegistry,
        code: VarInt,
        error: ReceiveError,
    ) -> Poll<Result<ReceiveProgress, ReceiveError>> {
        if !self.terminal {
            self.reader.stop_reliable(code);
            if let Some(flow) = self.flow {
                let _ = endpoint.fail_reliable_custody(flow.key);
            }
            if let Some(flow_id) = self.flow_id {
                registry.release(flow_id);
            }
            self.terminal = true;
        }
        Poll::Ready(Err(error))
    }
}

fn frame_error_code(error: &ReliableFrameError) -> VarInt {
    match error {
        ReliableFrameError::StagingLimitExceeded { .. } | ReliableFrameError::AllocationFailed => {
            RESOURCE_LIMIT_ERROR
        }
        _ => FLOW_PROTOCOL_ERROR,
    }
}

fn receive_error_code(error: &ReceiveError) -> VarInt {
    match error {
        ReceiveError::Framing(frame) => frame_error_code(frame),
        ReceiveError::UnexpectedCoreOutcome(ReceiveOutcome::TerminalReliableFailure)
        | ReceiveError::Io(_) => RELIABLE_DELIVERY_FAILED,
        ReceiveError::AllocationFailed | ReceiveError::AdapterStagingBelowFlowMaximum { .. } => {
            RESOURCE_LIMIT_ERROR
        }
        _ => FLOW_PROTOCOL_ERROR,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::task::Waker;

    use runen_net::delivery::{
        DeliveryFlowHandle, DeliveryScopeLimits, FlowResourcePolicy, OutboundPressureBehavior,
        ReceiverPressureBehavior, SubmissionOutcome,
    };
    use runen_net::identity::ConnectionHandle;

    use super::*;

    fn context() -> Context<'static> {
        Context::from_waker(Waker::noop())
    }

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    fn limits(flows: usize, messages: usize, bytes: usize) -> DeliveryScopeLimits {
        DeliveryScopeLimits::new(nz(flows), nz(messages), nz(bytes))
    }

    fn policy() -> FlowResourcePolicy {
        FlowResourcePolicy::new(
            nz(64),
            nz(8),
            nz(256),
            OutboundPressureBehavior::RejectNew,
            ReceiverPressureBehavior::TerminateReliable,
        )
    }

    fn endpoint_with_flow(
        direction: FlowDirection,
        handle: u64,
    ) -> (DeliveryEndpoint, DeliveryFlowKey) {
        let mut endpoint = DeliveryEndpoint::new(limits(16, 32, 1024));
        let key = DeliveryFlowKey::new(
            ConnectionHandle::new(1),
            direction,
            DeliveryFlowHandle::new(handle),
        );
        endpoint
            .establish_flow(
                key,
                DeliveryMode::ReliableOrdered,
                policy(),
                limits(16, 32, 1024),
            )
            .unwrap();
        (endpoint, key)
    }

    #[derive(Debug)]
    enum WriteAction {
        Pending,
        Write(usize),
        Error,
    }

    #[derive(Debug, Copy, Clone)]
    enum FinishAckAction {
        Ack,
        PendingThenAck,
        Stopped,
        Error,
    }

    #[derive(Debug)]
    struct MockFinishAck {
        action: FinishAckAction,
        pending_emitted: bool,
    }

    impl Future for MockFinishAck {
        type Output = Result<Option<VarInt>, IoFailure>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            match self.action {
                FinishAckAction::Ack => Poll::Ready(Ok(None)),
                FinishAckAction::PendingThenAck if !self.pending_emitted => {
                    self.pending_emitted = true;
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                FinishAckAction::PendingThenAck => Poll::Ready(Ok(None)),
                FinishAckAction::Stopped => Poll::Ready(Ok(Some(RELIABLE_DELIVERY_FAILED))),
                FinishAckAction::Error => Poll::Ready(Err(IoFailure::Write)),
            }
        }
    }

    #[derive(Debug)]
    struct MockWriter {
        actions: VecDeque<WriteAction>,
        bytes: Vec<u8>,
        resets: Vec<u64>,
        finished: bool,
        finish_ack: FinishAckAction,
    }

    impl MockWriter {
        fn new(actions: impl IntoIterator<Item = WriteAction>) -> Self {
            Self {
                actions: actions.into_iter().collect(),
                bytes: Vec::new(),
                resets: Vec::new(),
                finished: false,
                finish_ack: FinishAckAction::Ack,
            }
        }

        fn with_finish_ack(mut self, finish_ack: FinishAckAction) -> Self {
            self.finish_ack = finish_ack;
            self
        }
    }

    impl PollWriteReliable for MockWriter {
        fn poll_write_step(
            &mut self,
            _cx: &mut Context<'_>,
            bytes: &[u8],
        ) -> Poll<Result<usize, IoFailure>> {
            match self
                .actions
                .pop_front()
                .unwrap_or(WriteAction::Write(bytes.len()))
            {
                WriteAction::Pending => Poll::Pending,
                WriteAction::Error => Poll::Ready(Err(IoFailure::Write)),
                WriteAction::Write(limit) => {
                    let written = limit.min(bytes.len());
                    self.bytes.extend_from_slice(&bytes[..written]);
                    Poll::Ready(Ok(written))
                }
            }
        }

        fn reset_reliable(&mut self, code: VarInt) {
            self.resets.push(code.into_inner());
        }

        fn finish_reliable(&mut self) -> Result<(), IoFailure> {
            self.finished = true;
            Ok(())
        }

        fn finish_ack_future(&self) -> FinishAckFuture {
            Box::pin(MockFinishAck {
                action: self.finish_ack,
                pending_emitted: false,
            })
        }
    }

    #[derive(Debug)]
    enum ReadAction {
        Pending,
        Data(Vec<u8>),
        Fin,
        Error,
    }

    #[derive(Debug)]
    struct MockReader {
        zero_rtt: bool,
        actions: VecDeque<ReadAction>,
        stops: Vec<u64>,
    }

    impl MockReader {
        fn new(zero_rtt: bool, actions: impl IntoIterator<Item = ReadAction>) -> Self {
            Self {
                zero_rtt,
                actions: actions.into_iter().collect(),
                stops: Vec::new(),
            }
        }
    }

    impl PollReadReliable for MockReader {
        fn is_zero_rtt(&self) -> bool {
            self.zero_rtt
        }

        fn poll_read_step(
            &mut self,
            _cx: &mut Context<'_>,
            bytes: &mut [u8],
        ) -> Poll<Result<usize, IoFailure>> {
            match self.actions.pop_front().unwrap_or(ReadAction::Pending) {
                ReadAction::Pending => Poll::Pending,
                ReadAction::Fin => Poll::Ready(Ok(0)),
                ReadAction::Error => Poll::Ready(Err(IoFailure::Read)),
                ReadAction::Data(data) => {
                    assert!(data.len() <= bytes.len());
                    bytes[..data.len()].copy_from_slice(&data);
                    Poll::Ready(Ok(data.len()))
                }
            }
        }

        fn stop_reliable(&mut self, code: VarInt) {
            self.stops.push(code.into_inner());
        }
    }

    #[test]
    fn registry_is_finite_and_rejects_duplicate_active_association() {
        let (endpoint, key) = endpoint_with_flow(FlowDirection::Outbound, 1);
        let flow_id = FlowId::new(WireSide::Client, 0).unwrap();
        let mut registry = AcceptedFlowRegistry::new(WireSide::Client, nz(1));
        registry
            .register_consumed_accepted_flow(&endpoint, flow_id, key, nz(64))
            .unwrap();
        assert_eq!(
            registry.register_consumed_accepted_flow(&endpoint, flow_id, key, nz(64)),
            Err(RegistryError::DuplicateFlowId)
        );
        registry.associate_outbound(flow_id).unwrap();
        assert_eq!(
            registry.associate_outbound(flow_id),
            Err(RegistryError::AlreadyAssociated)
        );
        assert_eq!(registry.active_len(), 1);
    }

    #[test]
    fn registry_enforces_profile_limits_side_capacity_and_direction() {
        let (outbound_endpoint, outbound_key) = endpoint_with_flow(FlowDirection::Outbound, 20);
        let client_flow = FlowId::new(WireSide::Client, 0).unwrap();
        let server_flow = FlowId::new(WireSide::Server, 0).unwrap();

        let mut registry = AcceptedFlowRegistry::new(WireSide::Client, nz(4));
        assert_eq!(
            registry.register_consumed_accepted_flow(
                &outbound_endpoint,
                client_flow,
                outbound_key,
                nz(32),
            ),
            Err(RegistryError::OutboundMessageLimitExceedsProfile)
        );
        assert_eq!(
            registry.register_consumed_accepted_flow(
                &outbound_endpoint,
                server_flow,
                outbound_key,
                nz(64),
            ),
            Err(RegistryError::WrongFlowSide)
        );

        let (inbound_endpoint, inbound_key) = endpoint_with_flow(FlowDirection::Inbound, 21);
        assert_eq!(
            registry.register_consumed_accepted_flow(
                &inbound_endpoint,
                server_flow,
                inbound_key,
                nz(65),
            ),
            Err(RegistryError::InboundMessageLimitCannotSupportProfile)
        );

        let mut unreliable_endpoint = DeliveryEndpoint::new(limits(16, 32, 1024));
        let unreliable_key = DeliveryFlowKey::new(
            ConnectionHandle::new(1),
            FlowDirection::Outbound,
            DeliveryFlowHandle::new(22),
        );
        let unreliable_policy = FlowResourcePolicy::new(
            nz(64),
            nz(8),
            nz(256),
            OutboundPressureBehavior::RejectNew,
            ReceiverPressureBehavior::DropIncomingUnreliable,
        );
        unreliable_endpoint
            .establish_flow(
                unreliable_key,
                DeliveryMode::UnreliableUnordered,
                unreliable_policy,
                limits(16, 32, 1024),
            )
            .unwrap();
        registry
            .register_consumed_accepted_flow(
                &unreliable_endpoint,
                client_flow,
                unreliable_key,
                nz(64),
            )
            .unwrap();
        let registered = registry.registered_flow(client_flow).unwrap();
        assert_eq!(registered.key, unreliable_key);
        assert_eq!(registered.mode, DeliveryMode::UnreliableUnordered);
        assert_eq!(registered.max_message_bytes, 64);
        assert_eq!(registered.reliable_association, None);
        assert_eq!(
            registry.associate_outbound(client_flow),
            Err(RegistryError::NotReliable)
        );
        assert_eq!(registry.registered_flow(client_flow), Some(registered));
        registry.release(client_flow);

        let mut capacity_endpoint = DeliveryEndpoint::new(limits(16, 32, 1024));
        let first_key = DeliveryFlowKey::new(
            ConnectionHandle::new(1),
            FlowDirection::Outbound,
            DeliveryFlowHandle::new(23),
        );
        let second_key = DeliveryFlowKey::new(
            ConnectionHandle::new(1),
            FlowDirection::Outbound,
            DeliveryFlowHandle::new(24),
        );
        capacity_endpoint
            .establish_flow(
                first_key,
                DeliveryMode::ReliableOrdered,
                policy(),
                limits(16, 32, 1024),
            )
            .unwrap();
        capacity_endpoint
            .establish_flow(
                second_key,
                DeliveryMode::ReliableOrdered,
                policy(),
                limits(16, 32, 1024),
            )
            .unwrap();
        let first_flow = FlowId::new(WireSide::Client, 0).unwrap();
        let second_flow = FlowId::new(WireSide::Client, 1).unwrap();
        let mut capacity_registry = AcceptedFlowRegistry::new(WireSide::Client, nz(1));
        capacity_registry
            .register_consumed_accepted_flow(&capacity_endpoint, first_flow, first_key, nz(64))
            .unwrap();
        assert_eq!(
            capacity_registry.register_consumed_accepted_flow(
                &capacity_endpoint,
                second_flow,
                second_key,
                nz(64),
            ),
            Err(RegistryError::CapacityExceeded)
        );
        assert_eq!(
            capacity_registry.associate_inbound(first_flow),
            Err(RegistryError::WrongDirection)
        );
        assert_eq!(
            capacity_registry.associate_inbound(FlowId::new(WireSide::Server, 7).unwrap()),
            Err(RegistryError::UnknownFlowId)
        );
    }

    #[test]
    fn registry_supports_sequenced_lookup_without_stream_association() {
        let mut endpoint = DeliveryEndpoint::new(limits(16, 32, 1024));
        let key = DeliveryFlowKey::new(
            ConnectionHandle::new(1),
            FlowDirection::Inbound,
            DeliveryFlowHandle::new(25),
        );
        let unreliable_policy = FlowResourcePolicy::new(
            nz(64),
            nz(8),
            nz(256),
            OutboundPressureBehavior::RejectNew,
            ReceiverPressureBehavior::DropIncomingUnreliable,
        );
        endpoint
            .establish_flow(
                key,
                DeliveryMode::UnreliableSequenced,
                unreliable_policy,
                limits(16, 32, 1024),
            )
            .unwrap();

        let flow_id = FlowId::new(WireSide::Server, 2).unwrap();
        let mut registry = AcceptedFlowRegistry::new(WireSide::Client, nz(2));
        registry
            .register_consumed_accepted_flow(&endpoint, flow_id, key, nz(64))
            .unwrap();

        let registered = registry.registered_flow(flow_id).unwrap();
        assert_eq!(registered.key, key);
        assert_eq!(registered.mode, DeliveryMode::UnreliableSequenced);
        assert_eq!(registered.max_message_bytes, 64);
        assert_eq!(registered.reliable_association, None);
        assert_eq!(registry.active_len(), 1);
        assert_eq!(
            registry.associate_inbound(flow_id),
            Err(RegistryError::NotReliable)
        );
        assert_eq!(registry.registered_flow(flow_id), Some(registered));
        assert_eq!(registry.active_len(), 1);

        registry.release(flow_id);
        assert_eq!(registry.registered_flow(flow_id), None);
        assert_eq!(registry.active_len(), 0);

        let unknown_key = DeliveryFlowKey::new(
            ConnectionHandle::new(1),
            FlowDirection::Inbound,
            DeliveryFlowHandle::new(404),
        );
        let unknown_flow = FlowId::new(WireSide::Server, 3).unwrap();
        assert_eq!(
            registry.register_consumed_accepted_flow(&endpoint, unknown_flow, unknown_key, nz(64)),
            Err(RegistryError::UnknownCoreFlow)
        );
        assert_eq!(registry.registered_flow(unknown_flow), None);
    }

    #[test]
    fn outbound_partial_progress_preserves_order_and_custody() {
        let (mut endpoint, key) = endpoint_with_flow(FlowDirection::Outbound, 2);
        assert!(matches!(
            endpoint.submit(key, b"abc".to_vec()).unwrap(),
            SubmissionOutcome::Accepted {
                accepted_index: 0,
                ..
            }
        ));
        assert!(matches!(
            endpoint.submit(key, b"de".to_vec()).unwrap(),
            SubmissionOutcome::Accepted {
                accepted_index: 1,
                ..
            }
        ));
        let flow_id = FlowId::new(WireSide::Client, 1 << 20).unwrap();
        let mut registry = AcceptedFlowRegistry::new(WireSide::Client, nz(4));
        registry
            .register_consumed_accepted_flow(&endpoint, flow_id, key, nz(64))
            .unwrap();
        let writer = MockWriter::new([
            WriteAction::Pending,
            WriteAction::Write(1),
            WriteAction::Write(1),
            WriteAction::Write(2),
            WriteAction::Write(1),
            WriteAction::Write(1),
            WriteAction::Write(1),
            WriteAction::Write(2),
        ]);
        let mut binding = OutboundReliable::bind(&mut registry, flow_id, writer).unwrap();
        let mut cx = context();

        assert!(matches!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Pending
        ));
        assert_eq!(endpoint.pending_messages(), 2);
        let mut committed = Vec::new();
        loop {
            match binding.poll_step(&mut cx, &mut endpoint, &mut registry) {
                Poll::Pending => {}
                Poll::Ready(Ok(SendProgress::Progressed { .. })) => {}
                Poll::Ready(Ok(SendProgress::Committed { accepted_index })) => {
                    committed.push(accepted_index);
                }
                Poll::Ready(Ok(SendProgress::Idle)) => break,
                Poll::Ready(Ok(SendProgress::Closed)) => panic!("unexpected close"),
                Poll::Ready(Err(error)) => panic!("unexpected error: {error:?}"),
            }
        }
        assert_eq!(committed, vec![0, 1]);
        assert_eq!(endpoint.pending_messages(), 0);
        let mut expected = encode_varint(flow_id.value()).unwrap().as_slice().to_vec();
        expected.extend_from_slice(&[3, b'a', b'b', b'c', 2, b'd', b'e']);
        assert_eq!(binding.writer.bytes, expected);
    }

    #[test]
    fn outbound_error_is_terminal_without_retry() {
        let (mut endpoint, key) = endpoint_with_flow(FlowDirection::Outbound, 3);
        endpoint.submit(key, b"abc".to_vec()).unwrap();
        let flow_id = FlowId::new(WireSide::Client, 0).unwrap();
        let mut registry = AcceptedFlowRegistry::new(WireSide::Client, nz(4));
        registry
            .register_consumed_accepted_flow(&endpoint, flow_id, key, nz(64))
            .unwrap();
        let mut binding = OutboundReliable::bind(
            &mut registry,
            flow_id,
            MockWriter::new([WriteAction::Write(1), WriteAction::Error]),
        )
        .unwrap();
        let mut cx = context();
        assert!(matches!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Ok(SendProgress::Progressed { .. }))
        ));
        assert_eq!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Err(SendError::Io(IoFailure::Write)))
        );
        assert_eq!(endpoint.flow_contract(key), None);
        assert_eq!(registry.active_len(), 0);
        assert_eq!(binding.writer.resets, vec![6]);
        assert_eq!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Err(SendError::Terminal))
        );
    }

    #[test]
    fn outbound_accepted_index_exhaustion_is_terminal_and_non_wrapping() {
        let (mut endpoint, key) = endpoint_with_flow(FlowDirection::Outbound, 29);
        endpoint.submit(key, b"x".to_vec()).unwrap();
        let flow_id = FlowId::new(WireSide::Client, 0).unwrap();
        let mut registry = AcceptedFlowRegistry::new(WireSide::Client, nz(4));
        registry
            .register_consumed_accepted_flow(&endpoint, flow_id, key, nz(64))
            .unwrap();
        let mut binding =
            OutboundReliable::bind(&mut registry, flow_id, MockWriter::new([])).unwrap();
        let mut cx = context();
        assert!(matches!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Ok(SendProgress::Progressed { .. }))
        ));
        binding.state.exhaust_indices();
        assert_eq!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Err(SendError::AcceptedIndexExhausted))
        );
        assert_eq!(endpoint.flow_contract(key), None);
        assert_eq!(registry.active_len(), 0);
        assert_eq!(binding.writer.resets, vec![5]);
    }

    #[test]
    fn outbound_normal_finish_waits_for_ack_before_core_termination() {
        let (mut endpoint, key) = endpoint_with_flow(FlowDirection::Outbound, 30);
        let flow_id = FlowId::new(WireSide::Client, 0).unwrap();
        let mut registry = AcceptedFlowRegistry::new(WireSide::Client, nz(4));
        registry
            .register_consumed_accepted_flow(&endpoint, flow_id, key, nz(64))
            .unwrap();
        let writer = MockWriter::new([]).with_finish_ack(FinishAckAction::PendingThenAck);
        let mut binding = OutboundReliable::bind(&mut registry, flow_id, writer).unwrap();
        let mut cx = context();
        assert!(matches!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Ok(SendProgress::Progressed { .. }))
        ));
        assert_eq!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Ok(SendProgress::Idle))
        );
        binding.request_finish_normal(&endpoint).unwrap();
        assert!(binding.writer.finished);
        assert!(endpoint.flow_contract(key).is_some());
        assert_eq!(registry.active_len(), 1);
        assert!(matches!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Pending
        ));
        assert!(endpoint.flow_contract(key).is_some());
        assert_eq!(registry.active_len(), 1);
        assert_eq!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Ok(SendProgress::Closed))
        );
        assert_eq!(endpoint.flow_contract(key), None);
        assert_eq!(registry.active_len(), 0);
    }

    #[test]
    fn outbound_stop_or_ack_error_after_fin_is_terminal() {
        for (handle, action) in [(31, FinishAckAction::Stopped), (32, FinishAckAction::Error)] {
            let (mut endpoint, key) = endpoint_with_flow(FlowDirection::Outbound, handle);
            let flow_id = FlowId::new(WireSide::Client, 0).unwrap();
            let mut registry = AcceptedFlowRegistry::new(WireSide::Client, nz(4));
            registry
                .register_consumed_accepted_flow(&endpoint, flow_id, key, nz(64))
                .unwrap();
            let writer = MockWriter::new([]).with_finish_ack(action);
            let mut binding = OutboundReliable::bind(&mut registry, flow_id, writer).unwrap();
            let mut cx = context();
            assert!(matches!(
                binding.poll_step(&mut cx, &mut endpoint, &mut registry),
                Poll::Ready(Ok(SendProgress::Progressed { .. }))
            ));
            assert_eq!(
                binding.poll_step(&mut cx, &mut endpoint, &mut registry),
                Poll::Ready(Ok(SendProgress::Idle))
            );
            binding.request_finish_normal(&endpoint).unwrap();
            assert_eq!(
                binding.poll_step(&mut cx, &mut endpoint, &mut registry),
                Poll::Ready(Err(SendError::Io(IoFailure::Write)))
            );
            assert_eq!(endpoint.flow_contract(key), None);
            assert_eq!(registry.active_len(), 0);
            assert_eq!(binding.writer.resets, vec![6]);
        }
    }

    #[test]
    fn outbound_submission_after_fin_and_external_termination_fail_closed() {
        let (mut endpoint, key) = endpoint_with_flow(FlowDirection::Outbound, 33);
        let flow_id = FlowId::new(WireSide::Client, 0).unwrap();
        let mut registry = AcceptedFlowRegistry::new(WireSide::Client, nz(4));
        registry
            .register_consumed_accepted_flow(&endpoint, flow_id, key, nz(64))
            .unwrap();
        let writer = MockWriter::new([]).with_finish_ack(FinishAckAction::PendingThenAck);
        let mut binding = OutboundReliable::bind(&mut registry, flow_id, writer).unwrap();
        let mut cx = context();
        assert!(matches!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Ok(SendProgress::Progressed { .. }))
        ));
        assert_eq!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Ok(SendProgress::Idle))
        );
        binding.request_finish_normal(&endpoint).unwrap();
        endpoint.submit(key, b"late".to_vec()).unwrap();
        assert_eq!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Err(SendError::PendingData))
        );
        assert_eq!(endpoint.flow_contract(key), None);
        assert_eq!(registry.active_len(), 0);
        assert_eq!(binding.writer.resets, vec![6]);

        let (mut endpoint, key) = endpoint_with_flow(FlowDirection::Outbound, 34);
        endpoint.submit(key, b"abc".to_vec()).unwrap();
        let mut registry = AcceptedFlowRegistry::new(WireSide::Client, nz(4));
        registry
            .register_consumed_accepted_flow(&endpoint, flow_id, key, nz(64))
            .unwrap();
        let writer = MockWriter::new([WriteAction::Write(1), WriteAction::Write(1)]);
        let mut binding = OutboundReliable::bind(&mut registry, flow_id, writer).unwrap();
        assert!(matches!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Ok(SendProgress::Progressed { .. }))
        ));
        assert!(matches!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Ok(SendProgress::Progressed { .. }))
        ));
        let bytes_before_termination = binding.writer.bytes.clone();
        endpoint
            .terminate_flow(key, FlowTerminationReason::Requested)
            .unwrap();
        assert_eq!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Err(SendError::Core(DeliveryOperationError::UnknownFlow)))
        );
        assert_eq!(binding.writer.bytes, bytes_before_termination);
        assert_eq!(binding.writer.resets, vec![6]);
        assert_eq!(registry.active_len(), 0);
    }

    #[test]
    fn prefix_handles_arbitrary_boundaries_and_non_minimal_encoding() {
        let flow_id = FlowId::new(WireSide::Server, 1 << 20).unwrap();
        let encoded = encode_varint(flow_id.value()).unwrap();
        for split in 1..encoded.len() {
            let mut prefix = FlowIdPrefix::new();
            assert_eq!(
                prefix
                    .consume(&encoded.as_slice()[..split])
                    .unwrap()
                    .flow_id,
                None
            );
            assert_eq!(
                prefix
                    .consume(&encoded.as_slice()[split..])
                    .unwrap()
                    .flow_id,
                Some(flow_id)
            );
        }
        let mut prefix = FlowIdPrefix::new();
        assert_eq!(
            prefix.consume(&[0x40, 0x01]),
            Err(PrefixError::VarInt(VarIntDecodeError::NonMinimal))
        );
    }

    #[test]
    fn inbound_rejects_truncated_unknown_and_underprovisioned_associations() {
        let mut cx = context();

        let multi_byte_flow = FlowId::new(WireSide::Server, 32).unwrap();
        let encoded = encode_varint(multi_byte_flow.value()).unwrap();
        assert!(encoded.len() > 1);
        let reader = MockReader::new(
            false,
            [
                ReadAction::Data(vec![encoded.as_slice()[0]]),
                ReadAction::Fin,
            ],
        );
        let mut binding = InboundReliable::new(reader, nz(8), nz(64)).unwrap();
        let mut endpoint = DeliveryEndpoint::new(limits(16, 32, 1024));
        let mut registry = AcceptedFlowRegistry::new(WireSide::Client, nz(4));
        assert!(matches!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Ok(ReceiveProgress::Progressed { .. }))
        ));
        assert_eq!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Err(ReceiveError::TruncatedAssociation))
        );
        assert_eq!(binding.reader.stops, vec![5]);

        let unknown_flow = FlowId::new(WireSide::Server, 0).unwrap();
        let unknown_bytes = encode_varint(unknown_flow.value())
            .unwrap()
            .as_slice()
            .to_vec();
        let reader = MockReader::new(false, [ReadAction::Data(unknown_bytes)]);
        let mut binding = InboundReliable::new(reader, nz(8), nz(64)).unwrap();
        assert_eq!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Err(ReceiveError::Registry(RegistryError::UnknownFlowId)))
        );
        assert_eq!(binding.reader.stops, vec![5]);

        let (mut endpoint, key) = endpoint_with_flow(FlowDirection::Inbound, 40);
        let flow_id = FlowId::new(WireSide::Server, 0).unwrap();
        let mut registry = AcceptedFlowRegistry::new(WireSide::Client, nz(4));
        registry
            .register_consumed_accepted_flow(&endpoint, flow_id, key, nz(64))
            .unwrap();
        let bytes = encode_varint(flow_id.value()).unwrap().as_slice().to_vec();
        let reader = MockReader::new(false, [ReadAction::Data(bytes)]);
        let mut binding = InboundReliable::new(reader, nz(8), nz(32)).unwrap();
        assert_eq!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Err(ReceiveError::AdapterStagingBelowFlowMaximum {
                max_message_bytes: 64,
                max_staging_bytes: 32,
            }))
        );
        assert_eq!(binding.reader.stops, vec![3]);
        assert_eq!(endpoint.flow_contract(key), None);
        assert_eq!(registry.active_len(), 0);
    }

    #[test]
    fn inbound_rejects_zero_rtt_before_flow_association() {
        let result = InboundReliable::new(MockReader::new(true, []), nz(8), nz(64));
        assert!(matches!(result, Err(ReceiveError::ZeroRtt)));
    }

    #[test]
    fn inbound_buffers_multiple_frames_and_drains_clean_fin() {
        let (mut endpoint, key) = endpoint_with_flow(FlowDirection::Inbound, 4);
        let flow_id = FlowId::new(WireSide::Server, 0).unwrap();
        let mut registry = AcceptedFlowRegistry::new(WireSide::Client, nz(4));
        registry
            .register_consumed_accepted_flow(&endpoint, flow_id, key, nz(64))
            .unwrap();
        let reader = MockReader::new(
            false,
            [
                ReadAction::Data(vec![1, 3, b'o', b'n', b'e', 0, 2, b'o', b'k']),
                ReadAction::Fin,
            ],
        );
        let mut binding = InboundReliable::new(reader, nz(32), nz(64)).unwrap();
        let mut cx = context();
        assert_eq!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Ok(ReceiveProgress::MessagesBuffered { count: 3 }))
        );
        assert_eq!(endpoint.pending_messages(), 3);
        assert_eq!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Ok(ReceiveProgress::Draining))
        );
        assert_eq!(
            endpoint.poll_exposure(key).unwrap().unwrap().payload(),
            b"one"
        );
        assert_eq!(endpoint.poll_exposure(key).unwrap().unwrap().payload(), b"");
        assert_eq!(
            endpoint.poll_exposure(key).unwrap().unwrap().payload(),
            b"ok"
        );
        assert_eq!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Ok(ReceiveProgress::Closed))
        );
        assert_eq!(endpoint.flow_contract(key), None);
        assert_eq!(registry.active_len(), 0);
    }

    #[test]
    fn inbound_malformed_frame_and_read_error_are_terminal() {
        let (mut endpoint, key) = endpoint_with_flow(FlowDirection::Inbound, 5);
        let flow_id = FlowId::new(WireSide::Server, 0).unwrap();
        let mut registry = AcceptedFlowRegistry::new(WireSide::Client, nz(4));
        registry
            .register_consumed_accepted_flow(&endpoint, flow_id, key, nz(64))
            .unwrap();
        let reader = MockReader::new(false, [ReadAction::Data(vec![1, 0x40, 0x01])]);
        let mut binding = InboundReliable::new(reader, nz(8), nz(64)).unwrap();
        let mut cx = context();
        assert!(matches!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Err(ReceiveError::Framing(ReliableFrameError::VarInt(
                VarIntDecodeError::NonMinimal
            ))))
        ));
        assert_eq!(endpoint.flow_contract(key), None);
        assert_eq!(binding.reader.stops, vec![5]);

        let (mut endpoint, key) = endpoint_with_flow(FlowDirection::Inbound, 6);
        let mut registry = AcceptedFlowRegistry::new(WireSide::Client, nz(4));
        registry
            .register_consumed_accepted_flow(&endpoint, flow_id, key, nz(64))
            .unwrap();
        let reader = MockReader::new(false, [ReadAction::Data(vec![1]), ReadAction::Error]);
        let mut binding = InboundReliable::new(reader, nz(8), nz(64)).unwrap();
        assert_eq!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Ok(ReceiveProgress::Associated { flow_id }))
        );
        assert_eq!(
            binding.poll_step(&mut cx, &mut endpoint, &mut registry),
            Poll::Ready(Err(ReceiveError::Io(IoFailure::Read)))
        );
        assert_eq!(endpoint.flow_contract(key), None);
        assert_eq!(binding.reader.stops, vec![6]);
    }

    #[test]
    fn inbound_index_exhaustion_is_non_wrapping() {
        let (mut endpoint, key) = endpoint_with_flow(FlowDirection::Inbound, 7);
        let mut frames = InboundFrames::new(64, 64).unwrap();
        frames.exhaust_indices();
        assert_eq!(
            frames.consume(&[1, b'x'], &mut endpoint, key),
            Err(ReceiveError::AcceptedIndexExhausted)
        );
    }
}
