use std::collections::{BTreeMap, HashMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::Arc;

use crate::identity::{ConnectionHandle, SessionId};
use crate::session::{Session, SessionPhase};

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct DeliveryFlowHandle(u64);

impl DeliveryFlowHandle {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum FlowDirection {
    Outbound,
    Inbound,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct DeliveryFlowKey {
    connection: ConnectionHandle,
    direction: FlowDirection,
    handle: DeliveryFlowHandle,
}

impl DeliveryFlowKey {
    pub const fn new(
        connection: ConnectionHandle,
        direction: FlowDirection,
        handle: DeliveryFlowHandle,
    ) -> Self {
        Self {
            connection,
            direction,
            handle,
        }
    }

    pub const fn connection(self) -> ConnectionHandle {
        self.connection
    }

    pub const fn direction(self) -> FlowDirection {
        self.direction
    }

    pub const fn handle(self) -> DeliveryFlowHandle {
        self.handle
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DeliveryMode {
    ReliableOrdered,
    UnreliableUnordered,
    UnreliableSequenced,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum OutboundPressureBehavior {
    RejectNew,
    EvictOldestUnreliable,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ReceiverPressureBehavior {
    TerminateReliable,
    DropIncomingUnreliable,
    EvictOldestBufferedUnreliable,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FlowResourcePolicy {
    max_message_bytes: NonZeroUsize,
    max_pending_messages: NonZeroUsize,
    max_pending_payload_bytes: NonZeroUsize,
    outbound_pressure: OutboundPressureBehavior,
    receiver_pressure: ReceiverPressureBehavior,
}

impl FlowResourcePolicy {
    pub const fn new(
        max_message_bytes: NonZeroUsize,
        max_pending_messages: NonZeroUsize,
        max_pending_payload_bytes: NonZeroUsize,
        outbound_pressure: OutboundPressureBehavior,
        receiver_pressure: ReceiverPressureBehavior,
    ) -> Self {
        Self {
            max_message_bytes,
            max_pending_messages,
            max_pending_payload_bytes,
            outbound_pressure,
            receiver_pressure,
        }
    }

    pub const fn max_message_bytes(self) -> usize {
        self.max_message_bytes.get()
    }

    pub const fn max_pending_messages(self) -> usize {
        self.max_pending_messages.get()
    }

    pub const fn max_pending_payload_bytes(self) -> usize {
        self.max_pending_payload_bytes.get()
    }

    pub const fn outbound_pressure(self) -> OutboundPressureBehavior {
        self.outbound_pressure
    }

    pub const fn receiver_pressure(self) -> ReceiverPressureBehavior {
        self.receiver_pressure
    }

    pub fn validate_for_mode(self, mode: DeliveryMode) -> Result<(), DeliveryPolicyError> {
        match mode {
            DeliveryMode::ReliableOrdered => {
                if self.outbound_pressure != OutboundPressureBehavior::RejectNew {
                    return Err(DeliveryPolicyError::ReliableOutboundMustRejectNew);
                }
                if self.receiver_pressure != ReceiverPressureBehavior::TerminateReliable {
                    return Err(DeliveryPolicyError::ReliableReceiverMustTerminate);
                }
            }
            DeliveryMode::UnreliableUnordered | DeliveryMode::UnreliableSequenced => {
                if self.receiver_pressure == ReceiverPressureBehavior::TerminateReliable {
                    return Err(DeliveryPolicyError::UnreliableReceiverPolicyRequired);
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DeliveryPolicyError {
    ReliableOutboundMustRejectNew,
    ReliableReceiverMustTerminate,
    UnreliableReceiverPolicyRequired,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct DeliveryScopeLimits {
    max_active_flows: NonZeroUsize,
    max_pending_messages: NonZeroUsize,
    max_pending_payload_bytes: NonZeroUsize,
}

impl DeliveryScopeLimits {
    pub const fn new(
        max_active_flows: NonZeroUsize,
        max_pending_messages: NonZeroUsize,
        max_pending_payload_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            max_active_flows,
            max_pending_messages,
            max_pending_payload_bytes,
        }
    }

    pub const fn max_active_flows(self) -> usize {
        self.max_active_flows.get()
    }

    pub const fn max_pending_messages(self) -> usize {
        self.max_pending_messages.get()
    }

    pub const fn max_pending_payload_bytes(self) -> usize {
        self.max_pending_payload_bytes.get()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ResourceScope {
    Flow,
    Connection,
    Session,
    Aggregate,
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
struct ScopeUsage {
    active_flows: usize,
    pending_messages: usize,
    pending_payload_bytes: usize,
}

impl ScopeUsage {
    fn can_add(
        self,
        limits: DeliveryScopeLimits,
        active_flows: usize,
        pending_messages: usize,
        pending_payload_bytes: usize,
    ) -> bool {
        self.active_flows
            .checked_add(active_flows)
            .is_some_and(|value| value <= limits.max_active_flows())
            && self
                .pending_messages
                .checked_add(pending_messages)
                .is_some_and(|value| value <= limits.max_pending_messages())
            && self
                .pending_payload_bytes
                .checked_add(pending_payload_bytes)
                .is_some_and(|value| value <= limits.max_pending_payload_bytes())
    }

    fn add(&mut self, active_flows: usize, pending_messages: usize, pending_payload_bytes: usize) {
        self.active_flows = self
            .active_flows
            .checked_add(active_flows)
            .expect("usage addition validated before mutation");
        self.pending_messages = self
            .pending_messages
            .checked_add(pending_messages)
            .expect("usage addition validated before mutation");
        self.pending_payload_bytes = self
            .pending_payload_bytes
            .checked_add(pending_payload_bytes)
            .expect("usage addition validated before mutation");
    }

    fn remove(
        &mut self,
        active_flows: usize,
        pending_messages: usize,
        pending_payload_bytes: usize,
    ) {
        self.active_flows -= active_flows;
        self.pending_messages -= pending_messages;
        self.pending_payload_bytes -= pending_payload_bytes;
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct ScopeState {
    limits: DeliveryScopeLimits,
    usage: ScopeUsage,
}

impl ScopeState {
    const fn new(limits: DeliveryScopeLimits) -> Self {
        Self {
            limits,
            usage: ScopeUsage {
                active_flows: 0,
                pending_messages: 0,
                pending_payload_bytes: 0,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryTransfer {
    mode: DeliveryMode,
    accepted_index: u64,
    payload: Arc<Vec<u8>>,
}

impl DeliveryTransfer {
    pub const fn mode(&self) -> DeliveryMode {
        self.mode
    }

    /// Implementation-local acceptance metadata used to preserve one flow's
    /// ordering/sequencing contract across transport realization.
    ///
    /// This value is not a RunenNet wire identifier and has no meaning across
    /// delivery-flow lifetimes.
    pub const fn accepted_index(&self) -> u64 {
        self.accepted_index
    }

    pub fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }

    pub fn payload_len(&self) -> usize {
        self.payload.len()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct OutboundTransferMetadata {
    mode: DeliveryMode,
    accepted_index: u64,
    payload_len: usize,
}

impl OutboundTransferMetadata {
    pub const fn mode(self) -> DeliveryMode {
        self.mode
    }

    pub const fn accepted_index(self) -> u64 {
        self.accepted_index
    }

    pub const fn payload_len(self) -> usize {
        self.payload_len
    }
}

#[derive(Debug)]
enum ReceiverBuffer {
    Reliable {
        by_index: BTreeMap<u64, DeliveryTransfer>,
        next_exposure: Option<u64>,
    },
    Unreliable {
        queue: VecDeque<DeliveryTransfer>,
        last_exposed_sequence: Option<u64>,
    },
}

impl ReceiverBuffer {
    fn new(mode: DeliveryMode) -> Self {
        match mode {
            DeliveryMode::ReliableOrdered => Self::Reliable {
                by_index: BTreeMap::new(),
                next_exposure: Some(0),
            },
            DeliveryMode::UnreliableUnordered | DeliveryMode::UnreliableSequenced => {
                Self::Unreliable {
                    queue: VecDeque::new(),
                    last_exposed_sequence: None,
                }
            }
        }
    }
}

#[derive(Debug)]
struct FlowState {
    mode: DeliveryMode,
    policy: FlowResourcePolicy,
    next_accepted_index: Option<u64>,
    outbound: VecDeque<DeliveryTransfer>,
    receiver: ReceiverBuffer,
    pending_messages: usize,
    pending_payload_bytes: usize,
    session: Option<SessionId>,
}

impl FlowState {
    fn new(mode: DeliveryMode, policy: FlowResourcePolicy) -> Self {
        Self {
            mode,
            policy,
            next_accepted_index: Some(0),
            outbound: VecDeque::new(),
            receiver: ReceiverBuffer::new(mode),
            pending_messages: 0,
            pending_payload_bytes: 0,
            session: None,
        }
    }

    fn can_add_pending(&self, messages: usize, bytes: usize) -> bool {
        self.pending_messages
            .checked_add(messages)
            .is_some_and(|value| value <= self.policy.max_pending_messages())
            && self
                .pending_payload_bytes
                .checked_add(bytes)
                .is_some_and(|value| value <= self.policy.max_pending_payload_bytes())
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FlowEstablishmentError {
    InvalidPolicy(DeliveryPolicyError),
    FlowAlreadyExists,
    ConnectionLimitsMismatch,
    ActiveFlowLimitExceeded(ResourceScope),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SessionAssociationOutcome {
    Associated,
    AlreadyAssociated,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SessionAssociationError {
    UnknownFlow,
    SessionClosed,
    AlreadyAssociatedWithDifferentSession,
    SessionLimitsMismatch,
    ResourceLimitExceeded,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum SubmissionOutcome {
    Accepted {
        accepted_index: u64,
        local_pressure_drops: usize,
    },
    RejectedTooLarge,
    RejectedPressure,
    RejectedCounterExhausted,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DeliveryOperationError {
    UnknownFlow,
    WrongDirection,
    NotReliable,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CustodyCommitError {
    UnknownFlow,
    WrongDirection,
    NoPendingMessage,
    NotFront,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ReceiveOutcome {
    Buffered { local_pressure_drops: usize },
    DroppedByPressure { local_pressure_drops: usize },
    DroppedTooLarge,
    StaleSequenced,
    DuplicateReliable,
    RejectedModeMismatch,
    TerminalReliableFailure,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExposedMessage {
    accepted_index: u64,
    payload: Arc<Vec<u8>>,
}

impl ExposedMessage {
    pub const fn accepted_index(&self) -> u64 {
        self.accepted_index
    }

    pub fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum FlowTerminationReason {
    Requested,
    ConnectionEnded,
    ReliableCustodyLost,
    ReliableReceiverPressure,
    ReliableTransferConflict,
    ReliableInboundTooLarge,
    ReliableModeMismatch,
}

impl FlowTerminationReason {
    const fn forces_reliable_failure(self) -> bool {
        matches!(
            self,
            Self::ReliableCustodyLost
                | Self::ReliableReceiverPressure
                | Self::ReliableTransferConflict
                | Self::ReliableInboundTooLarge
                | Self::ReliableModeMismatch
        )
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct FlowTermination {
    pub key: DeliveryFlowKey,
    pub reason: FlowTerminationReason,
    pub pending_messages: usize,
    pub reliable_obligation_failed: bool,
}

#[derive(Debug, Copy, Clone, Default, PartialEq, Eq)]
pub struct DeliveryDiagnostics {
    pub outbound_unreliable_pressure_drops: usize,
    pub inbound_unreliable_pressure_drops: usize,
    pub stale_sequenced_drops: usize,
    pub reliable_duplicate_suppressions: usize,
}

#[derive(Debug)]
pub struct DeliveryEndpoint {
    aggregate: ScopeState,
    connections: HashMap<ConnectionHandle, ScopeState>,
    sessions: HashMap<SessionId, ScopeState>,
    flows: HashMap<DeliveryFlowKey, FlowState>,
    diagnostics: DeliveryDiagnostics,
}

impl DeliveryEndpoint {
    pub fn new(aggregate_limits: DeliveryScopeLimits) -> Self {
        Self {
            aggregate: ScopeState::new(aggregate_limits),
            connections: HashMap::new(),
            sessions: HashMap::new(),
            flows: HashMap::new(),
            diagnostics: DeliveryDiagnostics::default(),
        }
    }

    pub const fn diagnostics(&self) -> DeliveryDiagnostics {
        self.diagnostics
    }

    pub fn active_flows(&self) -> usize {
        self.aggregate.usage.active_flows
    }

    pub fn pending_messages(&self) -> usize {
        self.aggregate.usage.pending_messages
    }

    pub fn pending_payload_bytes(&self) -> usize {
        self.aggregate.usage.pending_payload_bytes
    }

    pub fn flow_contract(
        &self,
        key: DeliveryFlowKey,
    ) -> Option<(DeliveryMode, FlowResourcePolicy)> {
        self.flows.get(&key).map(|flow| (flow.mode, flow.policy))
    }

    pub fn flow_pending_usage(&self, key: DeliveryFlowKey) -> Option<(usize, usize)> {
        self.flows
            .get(&key)
            .map(|flow| (flow.pending_messages, flow.pending_payload_bytes))
    }

    /// Inspect the acceptance index that the next successful outbound submission would consume.
    ///
    /// This is a read-only transport handoff. It does not reserve the index, admit a message,
    /// mutate pressure/accounting state, or change submission semantics. `submit` remains the
    /// sole authority that accepts a message and advances the index.
    pub fn next_outbound_accepted_index(
        &self,
        key: DeliveryFlowKey,
    ) -> Result<Option<u64>, DeliveryOperationError> {
        let flow = self
            .flows
            .get(&key)
            .ok_or(DeliveryOperationError::UnknownFlow)?;
        if key.direction != FlowDirection::Outbound {
            return Err(DeliveryOperationError::WrongDirection);
        }
        Ok(flow.next_accepted_index)
    }

    pub fn establish_flow(
        &mut self,
        key: DeliveryFlowKey,
        mode: DeliveryMode,
        policy: FlowResourcePolicy,
        connection_limits: DeliveryScopeLimits,
    ) -> Result<(), FlowEstablishmentError> {
        policy
            .validate_for_mode(mode)
            .map_err(FlowEstablishmentError::InvalidPolicy)?;

        if self.flows.contains_key(&key) {
            return Err(FlowEstablishmentError::FlowAlreadyExists);
        }

        if !self.aggregate.usage.can_add(self.aggregate.limits, 1, 0, 0) {
            return Err(FlowEstablishmentError::ActiveFlowLimitExceeded(
                ResourceScope::Aggregate,
            ));
        }

        if let Some(connection) = self.connections.get(&key.connection) {
            if connection.limits != connection_limits {
                return Err(FlowEstablishmentError::ConnectionLimitsMismatch);
            }
            if !connection.usage.can_add(connection.limits, 1, 0, 0) {
                return Err(FlowEstablishmentError::ActiveFlowLimitExceeded(
                    ResourceScope::Connection,
                ));
            }
        } else if !ScopeUsage::default().can_add(connection_limits, 1, 0, 0) {
            return Err(FlowEstablishmentError::ActiveFlowLimitExceeded(
                ResourceScope::Connection,
            ));
        }

        self.aggregate.usage.add(1, 0, 0);
        self.connections
            .entry(key.connection)
            .or_insert_with(|| ScopeState::new(connection_limits))
            .usage
            .add(1, 0, 0);
        self.flows.insert(key, FlowState::new(mode, policy));
        Ok(())
    }

    pub fn associate_flow_with_session(
        &mut self,
        key: DeliveryFlowKey,
        session: &Session,
        session_limits: DeliveryScopeLimits,
    ) -> Result<SessionAssociationOutcome, SessionAssociationError> {
        if session.phase() != SessionPhase::Open {
            return Err(SessionAssociationError::SessionClosed);
        }

        let (current_session, pending_messages, pending_payload_bytes) = self
            .flows
            .get(&key)
            .map(|flow| {
                (
                    flow.session,
                    flow.pending_messages,
                    flow.pending_payload_bytes,
                )
            })
            .ok_or(SessionAssociationError::UnknownFlow)?;

        if let Some(current_session) = current_session {
            if current_session != session.id() {
                return Err(SessionAssociationError::AlreadyAssociatedWithDifferentSession);
            }
            let state = self
                .sessions
                .get(&current_session)
                .expect("associated flow has session accounting");
            return if state.limits == session_limits {
                Ok(SessionAssociationOutcome::AlreadyAssociated)
            } else {
                Err(SessionAssociationError::SessionLimitsMismatch)
            };
        }

        let session_id = session.id();
        if let Some(state) = self.sessions.get(&session_id) {
            if state.limits != session_limits {
                return Err(SessionAssociationError::SessionLimitsMismatch);
            }
            if !state
                .usage
                .can_add(session_limits, 1, pending_messages, pending_payload_bytes)
            {
                return Err(SessionAssociationError::ResourceLimitExceeded);
            }
        } else if !ScopeUsage::default().can_add(
            session_limits,
            1,
            pending_messages,
            pending_payload_bytes,
        ) {
            return Err(SessionAssociationError::ResourceLimitExceeded);
        }

        self.sessions
            .entry(session_id)
            .or_insert_with(|| ScopeState::new(session_limits))
            .usage
            .add(1, pending_messages, pending_payload_bytes);
        self.flows
            .get_mut(&key)
            .expect("flow checked above")
            .session = Some(session_id);
        Ok(SessionAssociationOutcome::Associated)
    }

    pub fn submit(
        &mut self,
        key: DeliveryFlowKey,
        payload: Vec<u8>,
    ) -> Result<SubmissionOutcome, DeliveryOperationError> {
        let (mode, policy, next_index) = {
            let flow = self
                .flows
                .get(&key)
                .ok_or(DeliveryOperationError::UnknownFlow)?;
            if key.direction != FlowDirection::Outbound {
                return Err(DeliveryOperationError::WrongDirection);
            }
            (flow.mode, flow.policy, flow.next_accepted_index)
        };

        if payload.len() > policy.max_message_bytes() {
            return Ok(SubmissionOutcome::RejectedTooLarge);
        }

        let Some(accepted_index) = next_index else {
            return Ok(SubmissionOutcome::RejectedCounterExhausted);
        };

        let mut pressure_drops = 0usize;
        if !self.can_add_pending(key, 1, payload.len())? {
            match policy.outbound_pressure() {
                OutboundPressureBehavior::RejectNew => {
                    return Ok(SubmissionOutcome::RejectedPressure);
                }
                OutboundPressureBehavior::EvictOldestUnreliable => {
                    debug_assert_ne!(mode, DeliveryMode::ReliableOrdered);
                    while !self.can_add_pending(key, 1, payload.len())? {
                        if self.evict_oldest_outbound_unreliable(key)?.is_none() {
                            return Ok(SubmissionOutcome::RejectedPressure);
                        }
                        pressure_drops = pressure_drops.saturating_add(1);
                    }
                }
            }
        }

        let transfer = DeliveryTransfer {
            mode,
            accepted_index,
            payload: Arc::new(payload),
        };
        let payload_len = transfer.payload_len();

        {
            let flow = self
                .flows
                .get_mut(&key)
                .ok_or(DeliveryOperationError::UnknownFlow)?;
            flow.next_accepted_index = accepted_index.checked_add(1);
            flow.outbound.push_back(transfer);
        }
        self.add_pending(key, 1, payload_len);

        if pressure_drops > 0 {
            self.diagnostics.outbound_unreliable_pressure_drops = self
                .diagnostics
                .outbound_unreliable_pressure_drops
                .saturating_add(pressure_drops);
        }

        Ok(SubmissionOutcome::Accepted {
            accepted_index,
            local_pressure_drops: pressure_drops,
        })
    }

    pub fn peek_outbound(
        &self,
        key: DeliveryFlowKey,
    ) -> Result<Option<DeliveryTransfer>, DeliveryOperationError> {
        let flow = self
            .flows
            .get(&key)
            .ok_or(DeliveryOperationError::UnknownFlow)?;
        if key.direction != FlowDirection::Outbound {
            return Err(DeliveryOperationError::WrongDirection);
        }
        Ok(flow.outbound.front().cloned())
    }

    pub fn peek_outbound_metadata(
        &self,
        key: DeliveryFlowKey,
    ) -> Result<Option<OutboundTransferMetadata>, DeliveryOperationError> {
        let flow = self
            .flows
            .get(&key)
            .ok_or(DeliveryOperationError::UnknownFlow)?;
        if key.direction != FlowDirection::Outbound {
            return Err(DeliveryOperationError::WrongDirection);
        }
        Ok(flow
            .outbound
            .front()
            .map(|transfer| OutboundTransferMetadata {
                mode: transfer.mode,
                accepted_index: transfer.accepted_index,
                payload_len: transfer.payload_len(),
            }))
    }

    pub fn commit_outbound_custody(
        &mut self,
        key: DeliveryFlowKey,
        accepted_index: u64,
    ) -> Result<DeliveryTransfer, CustodyCommitError> {
        if key.direction != FlowDirection::Outbound {
            return Err(CustodyCommitError::WrongDirection);
        }

        let transfer = {
            let flow = self
                .flows
                .get_mut(&key)
                .ok_or(CustodyCommitError::UnknownFlow)?;
            let front = flow
                .outbound
                .front()
                .ok_or(CustodyCommitError::NoPendingMessage)?;
            if front.accepted_index != accepted_index {
                return Err(CustodyCommitError::NotFront);
            }
            flow.outbound
                .pop_front()
                .expect("front was checked immediately above")
        };
        self.remove_pending(key, 1, transfer.payload_len());
        Ok(transfer)
    }

    pub fn receive_transport_payload(
        &mut self,
        key: DeliveryFlowKey,
        accepted_index: u64,
        payload: Vec<u8>,
    ) -> Result<ReceiveOutcome, DeliveryOperationError> {
        if key.direction != FlowDirection::Inbound {
            return Err(DeliveryOperationError::WrongDirection);
        }
        let mode = self
            .flows
            .get(&key)
            .map(|flow| flow.mode)
            .ok_or(DeliveryOperationError::UnknownFlow)?;
        self.receive(
            key,
            DeliveryTransfer {
                mode,
                accepted_index,
                payload: Arc::new(payload),
            },
        )
    }

    pub fn receive(
        &mut self,
        key: DeliveryFlowKey,
        transfer: DeliveryTransfer,
    ) -> Result<ReceiveOutcome, DeliveryOperationError> {
        if key.direction != FlowDirection::Inbound {
            return Err(DeliveryOperationError::WrongDirection);
        }

        let (mode, policy) = self
            .flows
            .get(&key)
            .map(|flow| (flow.mode, flow.policy))
            .ok_or(DeliveryOperationError::UnknownFlow)?;

        if transfer.mode != mode {
            if mode == DeliveryMode::ReliableOrdered {
                let _ = self.terminate_flow(key, FlowTerminationReason::ReliableModeMismatch);
                return Ok(ReceiveOutcome::TerminalReliableFailure);
            }
            return Ok(ReceiveOutcome::RejectedModeMismatch);
        }

        if transfer.payload_len() > policy.max_message_bytes() {
            if mode == DeliveryMode::ReliableOrdered {
                let _ = self.terminate_flow(key, FlowTerminationReason::ReliableInboundTooLarge);
                return Ok(ReceiveOutcome::TerminalReliableFailure);
            }
            return Ok(ReceiveOutcome::DroppedTooLarge);
        }

        match mode {
            DeliveryMode::ReliableOrdered => self.receive_reliable(key, transfer),
            DeliveryMode::UnreliableUnordered => self.receive_unreliable(key, transfer, false),
            DeliveryMode::UnreliableSequenced => self.receive_unreliable(key, transfer, true),
        }
    }

    pub fn poll_exposure(
        &mut self,
        key: DeliveryFlowKey,
    ) -> Result<Option<ExposedMessage>, DeliveryOperationError> {
        if key.direction != FlowDirection::Inbound {
            return Err(DeliveryOperationError::WrongDirection);
        }

        let mode = self
            .flows
            .get(&key)
            .map(|flow| flow.mode)
            .ok_or(DeliveryOperationError::UnknownFlow)?;

        match mode {
            DeliveryMode::ReliableOrdered => self.poll_reliable_exposure(key),
            DeliveryMode::UnreliableUnordered => self.poll_unordered_exposure(key),
            DeliveryMode::UnreliableSequenced => self.poll_sequenced_exposure(key),
        }
    }

    pub fn last_exposed_sequence(
        &self,
        key: DeliveryFlowKey,
    ) -> Result<Option<u64>, DeliveryOperationError> {
        let flow = self
            .flows
            .get(&key)
            .ok_or(DeliveryOperationError::UnknownFlow)?;
        if key.direction != FlowDirection::Inbound {
            return Err(DeliveryOperationError::WrongDirection);
        }
        if flow.mode != DeliveryMode::UnreliableSequenced {
            return Ok(None);
        }
        match &flow.receiver {
            ReceiverBuffer::Unreliable {
                last_exposed_sequence,
                ..
            } => Ok(*last_exposed_sequence),
            ReceiverBuffer::Reliable { .. } => {
                unreachable!("mode and buffer are constructed together")
            }
        }
    }

    pub fn terminate_flow(
        &mut self,
        key: DeliveryFlowKey,
        reason: FlowTerminationReason,
    ) -> Result<FlowTermination, DeliveryOperationError> {
        let flow = self
            .flows
            .remove(&key)
            .ok_or(DeliveryOperationError::UnknownFlow)?;

        let pending_messages = flow.pending_messages;
        let pending_payload_bytes = flow.pending_payload_bytes;
        let reliable_obligation_failed = flow.mode == DeliveryMode::ReliableOrdered
            && (pending_messages > 0 || reason.forces_reliable_failure());

        self.aggregate
            .usage
            .remove(1, pending_messages, pending_payload_bytes);

        let remove_connection = {
            let connection = self
                .connections
                .get_mut(&key.connection)
                .expect("active flow has connection accounting");
            connection
                .usage
                .remove(1, pending_messages, pending_payload_bytes);
            connection.usage.active_flows == 0
        };
        if remove_connection {
            let removed = self.connections.remove(&key.connection);
            debug_assert!(removed.is_some());
        }

        if let Some(session_id) = flow.session {
            let remove_session = {
                let session = self
                    .sessions
                    .get_mut(&session_id)
                    .expect("associated flow has session accounting");
                session
                    .usage
                    .remove(1, pending_messages, pending_payload_bytes);
                session.usage.active_flows == 0
            };
            if remove_session {
                let removed = self.sessions.remove(&session_id);
                debug_assert!(removed.is_some());
            }
        }

        Ok(FlowTermination {
            key,
            reason,
            pending_messages,
            reliable_obligation_failed,
        })
    }

    pub fn fail_reliable_custody(
        &mut self,
        key: DeliveryFlowKey,
    ) -> Result<FlowTermination, DeliveryOperationError> {
        let mode = self
            .flows
            .get(&key)
            .map(|flow| flow.mode)
            .ok_or(DeliveryOperationError::UnknownFlow)?;
        if mode != DeliveryMode::ReliableOrdered {
            return Err(DeliveryOperationError::NotReliable);
        }
        self.terminate_flow(key, FlowTerminationReason::ReliableCustodyLost)
    }

    pub fn terminate_connection(&mut self, connection: ConnectionHandle) -> Vec<FlowTermination> {
        let mut keys: Vec<_> = self
            .flows
            .keys()
            .copied()
            .filter(|key| key.connection == connection)
            .collect();
        keys.sort_by_key(|key| {
            (
                match key.direction {
                    FlowDirection::Outbound => 0u8,
                    FlowDirection::Inbound => 1u8,
                },
                key.handle.get(),
            )
        });

        keys.into_iter()
            .filter_map(|key| {
                self.terminate_flow(key, FlowTerminationReason::ConnectionEnded)
                    .ok()
            })
            .collect()
    }

    fn receive_reliable(
        &mut self,
        key: DeliveryFlowKey,
        transfer: DeliveryTransfer,
    ) -> Result<ReceiveOutcome, DeliveryOperationError> {
        let accepted_index = transfer.accepted_index;
        let duplicate_or_conflict = {
            let flow = self
                .flows
                .get(&key)
                .ok_or(DeliveryOperationError::UnknownFlow)?;
            let ReceiverBuffer::Reliable {
                by_index,
                next_exposure,
            } = &flow.receiver
            else {
                unreachable!("mode and buffer are constructed together");
            };

            if next_exposure.is_none_or(|next| accepted_index < next) {
                Some(false)
            } else {
                by_index
                    .get(&accepted_index)
                    .map(|existing| existing != &transfer)
            }
        };

        if let Some(conflict) = duplicate_or_conflict {
            if conflict {
                let _ = self.terminate_flow(key, FlowTerminationReason::ReliableTransferConflict);
                return Ok(ReceiveOutcome::TerminalReliableFailure);
            }
            self.diagnostics.reliable_duplicate_suppressions = self
                .diagnostics
                .reliable_duplicate_suppressions
                .saturating_add(1);
            return Ok(ReceiveOutcome::DuplicateReliable);
        }

        if !self.can_add_pending(key, 1, transfer.payload_len())? {
            let _ = self.terminate_flow(key, FlowTerminationReason::ReliableReceiverPressure);
            return Ok(ReceiveOutcome::TerminalReliableFailure);
        }

        let payload_len = transfer.payload_len();
        {
            let flow = self
                .flows
                .get_mut(&key)
                .ok_or(DeliveryOperationError::UnknownFlow)?;
            let ReceiverBuffer::Reliable { by_index, .. } = &mut flow.receiver else {
                unreachable!("mode and buffer are constructed together");
            };
            let previous = by_index.insert(accepted_index, transfer);
            debug_assert!(previous.is_none());
        }
        self.add_pending(key, 1, payload_len);
        Ok(ReceiveOutcome::Buffered {
            local_pressure_drops: 0,
        })
    }

    fn receive_unreliable(
        &mut self,
        key: DeliveryFlowKey,
        transfer: DeliveryTransfer,
        sequenced: bool,
    ) -> Result<ReceiveOutcome, DeliveryOperationError> {
        if sequenced {
            let stale = {
                let flow = self
                    .flows
                    .get(&key)
                    .ok_or(DeliveryOperationError::UnknownFlow)?;
                let ReceiverBuffer::Unreliable {
                    last_exposed_sequence,
                    ..
                } = &flow.receiver
                else {
                    unreachable!("mode and buffer are constructed together");
                };
                last_exposed_sequence.is_some_and(|last| transfer.accepted_index <= last)
            };
            if stale {
                self.diagnostics.stale_sequenced_drops =
                    self.diagnostics.stale_sequenced_drops.saturating_add(1);
                return Ok(ReceiveOutcome::StaleSequenced);
            }
        }

        let receiver_pressure = self
            .flows
            .get(&key)
            .map(|flow| flow.policy.receiver_pressure())
            .ok_or(DeliveryOperationError::UnknownFlow)?;

        let mut pressure_drops = 0usize;
        if !self.can_add_pending(key, 1, transfer.payload_len())? {
            match receiver_pressure {
                ReceiverPressureBehavior::DropIncomingUnreliable => {
                    self.diagnostics.inbound_unreliable_pressure_drops = self
                        .diagnostics
                        .inbound_unreliable_pressure_drops
                        .saturating_add(1);
                    return Ok(ReceiveOutcome::DroppedByPressure {
                        local_pressure_drops: 1,
                    });
                }
                ReceiverPressureBehavior::EvictOldestBufferedUnreliable => {
                    while !self.can_add_pending(key, 1, transfer.payload_len())? {
                        if self.evict_oldest_inbound_unreliable(key)?.is_none() {
                            pressure_drops = pressure_drops.saturating_add(1);
                            self.diagnostics.inbound_unreliable_pressure_drops = self
                                .diagnostics
                                .inbound_unreliable_pressure_drops
                                .saturating_add(pressure_drops);
                            return Ok(ReceiveOutcome::DroppedByPressure {
                                local_pressure_drops: pressure_drops,
                            });
                        }
                        pressure_drops = pressure_drops.saturating_add(1);
                    }
                }
                ReceiverPressureBehavior::TerminateReliable => {
                    unreachable!("unreliable policy validation rejects this combination")
                }
            }
        }

        let payload_len = transfer.payload_len();
        {
            let flow = self
                .flows
                .get_mut(&key)
                .ok_or(DeliveryOperationError::UnknownFlow)?;
            let ReceiverBuffer::Unreliable { queue, .. } = &mut flow.receiver else {
                unreachable!("mode and buffer are constructed together");
            };
            queue.push_back(transfer);
        }
        self.add_pending(key, 1, payload_len);

        if pressure_drops > 0 {
            self.diagnostics.inbound_unreliable_pressure_drops = self
                .diagnostics
                .inbound_unreliable_pressure_drops
                .saturating_add(pressure_drops);
        }

        Ok(ReceiveOutcome::Buffered {
            local_pressure_drops: pressure_drops,
        })
    }

    fn poll_reliable_exposure(
        &mut self,
        key: DeliveryFlowKey,
    ) -> Result<Option<ExposedMessage>, DeliveryOperationError> {
        let transfer = {
            let flow = self
                .flows
                .get_mut(&key)
                .ok_or(DeliveryOperationError::UnknownFlow)?;
            let ReceiverBuffer::Reliable {
                by_index,
                next_exposure,
            } = &mut flow.receiver
            else {
                unreachable!("mode and buffer are constructed together");
            };
            let Some(expected) = *next_exposure else {
                return Ok(None);
            };
            let Some(transfer) = by_index.remove(&expected) else {
                return Ok(None);
            };
            *next_exposure = expected.checked_add(1);
            transfer
        };

        self.remove_pending(key, 1, transfer.payload_len());
        Ok(Some(ExposedMessage {
            accepted_index: transfer.accepted_index,
            payload: transfer.payload,
        }))
    }

    fn poll_unordered_exposure(
        &mut self,
        key: DeliveryFlowKey,
    ) -> Result<Option<ExposedMessage>, DeliveryOperationError> {
        let transfer = {
            let flow = self
                .flows
                .get_mut(&key)
                .ok_or(DeliveryOperationError::UnknownFlow)?;
            let ReceiverBuffer::Unreliable { queue, .. } = &mut flow.receiver else {
                unreachable!("mode and buffer are constructed together");
            };
            queue.pop_front()
        };

        let Some(transfer) = transfer else {
            return Ok(None);
        };
        self.remove_pending(key, 1, transfer.payload_len());
        Ok(Some(ExposedMessage {
            accepted_index: transfer.accepted_index,
            payload: transfer.payload,
        }))
    }

    fn poll_sequenced_exposure(
        &mut self,
        key: DeliveryFlowKey,
    ) -> Result<Option<ExposedMessage>, DeliveryOperationError> {
        loop {
            let transfer = {
                let flow = self
                    .flows
                    .get_mut(&key)
                    .ok_or(DeliveryOperationError::UnknownFlow)?;
                let ReceiverBuffer::Unreliable { queue, .. } = &mut flow.receiver else {
                    unreachable!("mode and buffer are constructed together");
                };
                queue.pop_front()
            };

            let Some(transfer) = transfer else {
                return Ok(None);
            };
            self.remove_pending(key, 1, transfer.payload_len());

            let stale = {
                let flow = self
                    .flows
                    .get(&key)
                    .ok_or(DeliveryOperationError::UnknownFlow)?;
                let ReceiverBuffer::Unreliable {
                    last_exposed_sequence,
                    ..
                } = &flow.receiver
                else {
                    unreachable!("mode and buffer are constructed together");
                };
                last_exposed_sequence.is_some_and(|last| transfer.accepted_index <= last)
            };

            if stale {
                self.diagnostics.stale_sequenced_drops =
                    self.diagnostics.stale_sequenced_drops.saturating_add(1);
                continue;
            }

            {
                let flow = self
                    .flows
                    .get_mut(&key)
                    .ok_or(DeliveryOperationError::UnknownFlow)?;
                let ReceiverBuffer::Unreliable {
                    last_exposed_sequence,
                    ..
                } = &mut flow.receiver
                else {
                    unreachable!("mode and buffer are constructed together");
                };
                *last_exposed_sequence = Some(transfer.accepted_index);
            }
            return Ok(Some(ExposedMessage {
                accepted_index: transfer.accepted_index,
                payload: transfer.payload,
            }));
        }
    }

    fn evict_oldest_outbound_unreliable(
        &mut self,
        key: DeliveryFlowKey,
    ) -> Result<Option<DeliveryTransfer>, DeliveryOperationError> {
        let transfer = {
            let flow = self
                .flows
                .get_mut(&key)
                .ok_or(DeliveryOperationError::UnknownFlow)?;
            if key.direction != FlowDirection::Outbound {
                return Err(DeliveryOperationError::WrongDirection);
            }
            debug_assert_ne!(flow.mode, DeliveryMode::ReliableOrdered);
            flow.outbound.pop_front()
        };
        if let Some(transfer) = &transfer {
            self.remove_pending(key, 1, transfer.payload_len());
        }
        Ok(transfer)
    }

    fn evict_oldest_inbound_unreliable(
        &mut self,
        key: DeliveryFlowKey,
    ) -> Result<Option<DeliveryTransfer>, DeliveryOperationError> {
        let transfer = {
            let flow = self
                .flows
                .get_mut(&key)
                .ok_or(DeliveryOperationError::UnknownFlow)?;
            if key.direction != FlowDirection::Inbound {
                return Err(DeliveryOperationError::WrongDirection);
            }
            debug_assert_ne!(flow.mode, DeliveryMode::ReliableOrdered);
            let ReceiverBuffer::Unreliable { queue, .. } = &mut flow.receiver else {
                unreachable!("mode and buffer are constructed together");
            };
            queue.pop_front()
        };
        if let Some(transfer) = &transfer {
            self.remove_pending(key, 1, transfer.payload_len());
        }
        Ok(transfer)
    }

    fn can_add_pending(
        &self,
        key: DeliveryFlowKey,
        messages: usize,
        payload_bytes: usize,
    ) -> Result<bool, DeliveryOperationError> {
        let flow = self
            .flows
            .get(&key)
            .ok_or(DeliveryOperationError::UnknownFlow)?;

        if !flow.can_add_pending(messages, payload_bytes) {
            return Ok(false);
        }

        let connection = self
            .connections
            .get(&key.connection)
            .expect("active flow has connection accounting");
        if !connection
            .usage
            .can_add(connection.limits, 0, messages, payload_bytes)
        {
            return Ok(false);
        }

        if !self
            .aggregate
            .usage
            .can_add(self.aggregate.limits, 0, messages, payload_bytes)
        {
            return Ok(false);
        }

        if let Some(session_id) = flow.session {
            let session = self
                .sessions
                .get(&session_id)
                .expect("associated flow has session accounting");
            if !session
                .usage
                .can_add(session.limits, 0, messages, payload_bytes)
            {
                return Ok(false);
            }
        }

        Ok(true)
    }

    fn add_pending(&mut self, key: DeliveryFlowKey, messages: usize, payload_bytes: usize) {
        let session_id = self
            .flows
            .get(&key)
            .expect("flow exists while adding pending usage")
            .session;

        {
            let flow = self
                .flows
                .get_mut(&key)
                .expect("flow exists while adding pending usage");
            flow.pending_messages += messages;
            flow.pending_payload_bytes += payload_bytes;
        }

        self.connections
            .get_mut(&key.connection)
            .expect("active flow has connection accounting")
            .usage
            .add(0, messages, payload_bytes);
        self.aggregate.usage.add(0, messages, payload_bytes);

        if let Some(session_id) = session_id {
            self.sessions
                .get_mut(&session_id)
                .expect("associated flow has session accounting")
                .usage
                .add(0, messages, payload_bytes);
        }
    }

    fn remove_pending(&mut self, key: DeliveryFlowKey, messages: usize, payload_bytes: usize) {
        let session_id = self
            .flows
            .get(&key)
            .expect("flow exists while removing pending usage")
            .session;

        {
            let flow = self
                .flows
                .get_mut(&key)
                .expect("flow exists while removing pending usage");
            flow.pending_messages -= messages;
            flow.pending_payload_bytes -= payload_bytes;
        }

        self.connections
            .get_mut(&key.connection)
            .expect("active flow has connection accounting")
            .usage
            .remove(0, messages, payload_bytes);
        self.aggregate.usage.remove(0, messages, payload_bytes);

        if let Some(session_id) = session_id {
            self.sessions
                .get_mut(&session_id)
                .expect("associated flow has session accounting")
                .usage
                .remove(0, messages, payload_bytes);
        }
    }
}

#[cfg(test)]
mod rn5d1_tests {
    use super::*;

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    #[test]
    fn next_outbound_accepted_index_reports_exhaustion_without_consuming_it() {
        let limits = DeliveryScopeLimits::new(nz(2), nz(2), nz(16));
        let mut endpoint = DeliveryEndpoint::new(limits);
        let key = DeliveryFlowKey::new(
            ConnectionHandle::new(99),
            FlowDirection::Outbound,
            DeliveryFlowHandle::new(99),
        );
        let policy = FlowResourcePolicy::new(
            nz(8),
            nz(2),
            nz(16),
            OutboundPressureBehavior::RejectNew,
            ReceiverPressureBehavior::DropIncomingUnreliable,
        );
        endpoint
            .establish_flow(key, DeliveryMode::UnreliableSequenced, policy, limits)
            .unwrap();

        endpoint
            .flows
            .get_mut(&key)
            .expect("flow established above")
            .next_accepted_index = None;

        assert_eq!(endpoint.next_outbound_accepted_index(key), Ok(None));
        assert_eq!(
            endpoint.submit(key, vec![1]).unwrap(),
            SubmissionOutcome::RejectedCounterExhausted
        );
        assert_eq!(endpoint.next_outbound_accepted_index(key), Ok(None));
        assert_eq!(endpoint.flow_pending_usage(key), Some((0, 0)));
    }
}
