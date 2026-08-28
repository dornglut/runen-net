use std::collections::{HashMap, TryReserveError};
use std::num::NonZeroUsize;

use runen_net::{
    delivery::{
        DeliveryEndpoint, DeliveryFlowKey, DeliveryMode, DeliveryOperationError,
        DeliveryPolicyError, DeliveryScopeLimits, FlowDirection, FlowEstablishmentError,
        FlowResourcePolicy, FlowTermination, FlowTerminationReason,
    },
    identity::ConnectionHandle,
};

use crate::{
    control::{ControlFrame, ControlFrameType, ProfileReadyParts, Settings},
    datagram::{DatagramSubmissionError, datagram_len},
    quinn_binding::{AcceptedFlowRegistry, RegisteredFlow, RegistryError},
    wire::{
        ControlBodyError, FlowAccept, FlowId, FlowIdCursor, FlowIdCursorError, FlowReject,
        FlowRejectReason, FlowTerminate, FlowTerminateReason, MAX_VARINT, OpenFlow, WireSide,
    },
};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum FlowControlConfigError {
    ActiveRegistryCapacityOverflow,
    ActiveRegistryCapacityOutOfRange,
}

#[derive(Debug)]
pub(super) enum OutboundOpenError {
    WrongConnection {
        expected: ConnectionHandle,
        actual: ConnectionHandle,
    },
    WrongDirection(FlowDirection),
    InvalidPolicy(DeliveryPolicyError),
    CoreFlowAlreadyExists(DeliveryFlowKey),
    PendingCoreFlow(DeliveryFlowKey),
    StableMessageLimitMismatch {
        policy_max: usize,
        stable_max: usize,
    },
    StableMessageLimitOutOfRange,
    PeerMessageLimit {
        requested: u64,
        peer_limit: u64,
    },
    PeerActiveFlowLimit {
        in_use: u64,
        peer_limit: u64,
    },
    DatagramUnavailable,
    DatagramTooSmall {
        needed: usize,
        available: usize,
    },
    DatagramEnvelope(DatagramSubmissionError),
    FlowId(FlowIdCursorError),
    Body(ControlBodyError),
    Allocation(TryReserveError),
}

#[derive(Debug)]
pub(super) enum FlowControlError {
    InboundDecisionPending(FlowId),
    UnexpectedFrame(ControlFrameType),
    Body(ControlBodyError),
    FlowId(FlowIdCursorError),
    WrongResponseSide {
        expected: WireSide,
        received: WireSide,
    },
    UnknownPendingFlow(FlowId),
    UnknownActiveFlow(FlowId),
    ReliableNormalUsesFin(FlowId),
    CoreState(DeliveryOperationError),
    LocalEstablishment(FlowEstablishmentError),
    Registry(RegistryError),
    Allocation(TryReserveError),
}

#[derive(Debug)]
pub(super) enum InboundAdmissionError {
    RequestNotPending(FlowId),
    WrongConnection {
        expected: ConnectionHandle,
        actual: ConnectionHandle,
    },
    WrongDirection(FlowDirection),
    InvalidPolicy(DeliveryPolicyError),
    LocalEstablishment(FlowEstablishmentError),
    Registry(RegistryError),
    CoreRollback(DeliveryOperationError),
    Allocation(TryReserveError),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) struct PreparedFlow {
    flow_id: FlowId,
    key: DeliveryFlowKey,
    mode: DeliveryMode,
    max_message_bytes: usize,
}

impl PreparedFlow {
    pub(super) const fn flow_id(self) -> FlowId {
        self.flow_id
    }

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
pub(super) struct EstablishedFlow {
    flow_id: FlowId,
    key: DeliveryFlowKey,
    mode: DeliveryMode,
    max_message_bytes: usize,
}

impl EstablishedFlow {
    pub(super) const fn flow_id(self) -> FlowId {
        self.flow_id
    }

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
pub(super) struct OutboundOpenRequest {
    pub(super) key: DeliveryFlowKey,
    pub(super) mode: DeliveryMode,
    pub(super) policy: FlowResourcePolicy,
    pub(super) stable_max_message_bytes: NonZeroUsize,
    pub(super) connection_limits: DeliveryScopeLimits,
}

#[derive(Debug)]
pub(super) struct PreparedOutboundOpen {
    pub(super) frame: ControlFrame,
    pub(super) flow: PreparedFlow,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct InboundOpenRequest {
    flow_id: FlowId,
    mode: DeliveryMode,
    max_message_bytes: u64,
}

impl InboundOpenRequest {
    pub(super) const fn flow_id(&self) -> FlowId {
        self.flow_id
    }

    pub(super) const fn mode(&self) -> DeliveryMode {
        self.mode
    }

    pub(super) const fn max_message_bytes(&self) -> u64 {
        self.max_message_bytes
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) struct InboundAdmission {
    pub(super) key: DeliveryFlowKey,
    pub(super) policy: FlowResourcePolicy,
    pub(super) connection_limits: DeliveryScopeLimits,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum FlowControlProgress {
    InboundOpen(InboundOpenRequest),
    InboundRejected {
        flow_id: FlowId,
        reason: FlowRejectReason,
        frame: ControlFrame,
    },
    OutboundEstablished(EstablishedFlow),
    OutboundRejected {
        flow_id: FlowId,
        key: DeliveryFlowKey,
        reason: FlowRejectReason,
    },
    OutboundFailedAfterAccept {
        flow_id: FlowId,
        key: DeliveryFlowKey,
        reason: FlowTerminateReason,
        termination: Option<FlowTermination>,
        frame: ControlFrame,
    },
    RemoteTerminated {
        flow: EstablishedFlow,
        reason: FlowTerminateReason,
        termination: FlowTermination,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum InboundResolution {
    Accepted {
        flow: EstablishedFlow,
        frame: ControlFrame,
    },
    Rejected {
        flow_id: FlowId,
        reason: FlowRejectReason,
        frame: ControlFrame,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct LocalTermination {
    pub(super) flow: EstablishedFlow,
    pub(super) reason: FlowTerminateReason,
    pub(super) termination: FlowTermination,
    pub(super) frame: ControlFrame,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct PendingOutboundFlow {
    key: DeliveryFlowKey,
    mode: DeliveryMode,
    policy: FlowResourcePolicy,
    connection_limits: DeliveryScopeLimits,
    stable_max_message_bytes: NonZeroUsize,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct PendingInboundFlow {
    flow_id: FlowId,
    mode: DeliveryMode,
    max_message_bytes: u64,
}

#[derive(Debug)]
pub(super) struct FlowControl {
    connection: ConnectionHandle,
    local_side: WireSide,
    local_settings: Settings,
    peer_settings: Settings,
    local_flow_ids: FlowIdCursor,
    peer_flow_ids: FlowIdCursor,
    pending_outbound: HashMap<u64, PendingOutboundFlow>,
    pending_inbound: Option<PendingInboundFlow>,
    registry: AcceptedFlowRegistry,
}

impl FlowControl {
    pub(super) fn from_profile_parts(
        connection: ConnectionHandle,
        profile: &ProfileReadyParts,
    ) -> Result<Self, FlowControlConfigError> {
        Self::new(
            connection,
            profile.side,
            profile.profile.local_settings(),
            profile.peer_settings,
        )
    }

    fn new(
        connection: ConnectionHandle,
        local_side: WireSide,
        local_settings: Settings,
        peer_settings: Settings,
    ) -> Result<Self, FlowControlConfigError> {
        let registry_capacity = local_settings
            .max_active_incoming_flows
            .checked_add(peer_settings.max_active_incoming_flows)
            .ok_or(FlowControlConfigError::ActiveRegistryCapacityOverflow)?;
        let registry_capacity = usize::try_from(registry_capacity)
            .map_err(|_| FlowControlConfigError::ActiveRegistryCapacityOutOfRange)?;
        let registry_capacity = NonZeroUsize::new(registry_capacity)
            .ok_or(FlowControlConfigError::ActiveRegistryCapacityOutOfRange)?;

        Ok(Self {
            connection,
            local_side,
            local_settings,
            peer_settings,
            local_flow_ids: FlowIdCursor::new(local_side),
            peer_flow_ids: FlowIdCursor::new(opposite_side(local_side)),
            pending_outbound: HashMap::new(),
            pending_inbound: None,
            registry: AcceptedFlowRegistry::new(local_side, registry_capacity),
        })
    }

    pub(super) const fn registry(&self) -> &AcceptedFlowRegistry {
        &self.registry
    }

    pub(super) fn registry_mut(&mut self) -> &mut AcceptedFlowRegistry {
        &mut self.registry
    }

    pub(super) const fn has_pending_inbound(&self) -> bool {
        self.pending_inbound.is_some()
    }

    pub(super) fn prepare_outbound_open(
        &mut self,
        endpoint: &DeliveryEndpoint,
        request: OutboundOpenRequest,
        current_datagram_size: Option<usize>,
    ) -> Result<PreparedOutboundOpen, OutboundOpenError> {
        let OutboundOpenRequest {
            key,
            mode,
            policy,
            stable_max_message_bytes,
            connection_limits,
        } = request;
        self.validate_outbound_identity(endpoint, key)?;
        policy
            .validate_for_mode(mode)
            .map_err(OutboundOpenError::InvalidPolicy)?;

        let stable_max = stable_max_message_bytes.get();
        if policy.max_message_bytes() != stable_max {
            return Err(OutboundOpenError::StableMessageLimitMismatch {
                policy_max: policy.max_message_bytes(),
                stable_max,
            });
        }
        let stable_wire = u64::try_from(stable_max)
            .map_err(|_| OutboundOpenError::StableMessageLimitOutOfRange)?;
        if stable_wire > MAX_VARINT {
            return Err(OutboundOpenError::StableMessageLimitOutOfRange);
        }
        if stable_wire > self.peer_settings.max_incoming_message_bytes {
            return Err(OutboundOpenError::PeerMessageLimit {
                requested: stable_wire,
                peer_limit: self.peer_settings.max_incoming_message_bytes,
            });
        }

        let active = self.registry.active_direction_len(FlowDirection::Outbound);
        let in_use = active
            .checked_add(self.pending_outbound.len())
            .and_then(|value| u64::try_from(value).ok())
            .unwrap_or(u64::MAX);
        if in_use >= self.peer_settings.max_active_incoming_flows {
            return Err(OutboundOpenError::PeerActiveFlowLimit {
                in_use,
                peer_limit: self.peer_settings.max_active_incoming_flows,
            });
        }

        let mut preview = self.local_flow_ids;
        let flow_id = preview.allocate().map_err(OutboundOpenError::FlowId)?;
        if mode != DeliveryMode::ReliableOrdered {
            let sequence = match mode {
                DeliveryMode::UnreliableUnordered => None,
                DeliveryMode::UnreliableSequenced => Some(MAX_VARINT),
                DeliveryMode::ReliableOrdered => unreachable!(),
            };
            let needed = datagram_len(flow_id, mode, sequence, stable_max)
                .map_err(OutboundOpenError::DatagramEnvelope)?;
            let available = current_datagram_size.ok_or(OutboundOpenError::DatagramUnavailable)?;
            if needed > available {
                return Err(OutboundOpenError::DatagramTooSmall { needed, available });
            }
        }

        self.pending_outbound
            .try_reserve(1)
            .map_err(OutboundOpenError::Allocation)?;
        let open = OpenFlow::new(flow_id, mode, stable_wire).map_err(OutboundOpenError::Body)?;
        let frame = owned_frame(ControlFrameType::OpenFlow, open.encode())
            .map_err(OutboundOpenError::Allocation)?;

        let allocated = self
            .local_flow_ids
            .allocate()
            .map_err(OutboundOpenError::FlowId)?;
        debug_assert_eq!(allocated, flow_id);
        let previous = self.pending_outbound.insert(
            flow_id.value(),
            PendingOutboundFlow {
                key,
                mode,
                policy,
                connection_limits,
                stable_max_message_bytes,
            },
        );
        debug_assert!(previous.is_none());

        Ok(PreparedOutboundOpen {
            frame,
            flow: PreparedFlow {
                flow_id,
                key,
                mode,
                max_message_bytes: stable_max,
            },
        })
    }

    fn validate_outbound_identity(
        &self,
        endpoint: &DeliveryEndpoint,
        key: DeliveryFlowKey,
    ) -> Result<(), OutboundOpenError> {
        if key.connection() != self.connection {
            return Err(OutboundOpenError::WrongConnection {
                expected: self.connection,
                actual: key.connection(),
            });
        }
        if key.direction() != FlowDirection::Outbound {
            return Err(OutboundOpenError::WrongDirection(key.direction()));
        }
        if endpoint.flow_contract(key).is_some() {
            return Err(OutboundOpenError::CoreFlowAlreadyExists(key));
        }
        if self
            .pending_outbound
            .values()
            .any(|pending| pending.key == key)
        {
            return Err(OutboundOpenError::PendingCoreFlow(key));
        }
        Ok(())
    }

    pub(super) fn receive(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        frame: ControlFrame,
    ) -> Result<FlowControlProgress, FlowControlError> {
        match frame.frame_type {
            ControlFrameType::OpenFlow => {
                if let Some(pending) = self.pending_inbound {
                    return Err(FlowControlError::InboundDecisionPending(pending.flow_id));
                }
                self.receive_open(frame.body)
            }
            ControlFrameType::FlowAccept => self.receive_accept(endpoint, frame.body),
            ControlFrameType::FlowReject => self.receive_reject(frame.body),
            ControlFrameType::FlowTerminate => self.receive_terminate(endpoint, frame.body),
            frame_type => Err(FlowControlError::UnexpectedFrame(frame_type)),
        }
    }

    fn receive_open(&mut self, body: Vec<u8>) -> Result<FlowControlProgress, FlowControlError> {
        let open = OpenFlow::decode(&body).map_err(FlowControlError::Body)?;
        self.peer_flow_ids
            .validate_and_consume(open.flow_id)
            .map_err(FlowControlError::FlowId)?;

        if open.max_message_bytes > self.local_settings.max_incoming_message_bytes {
            return self.inbound_rejection(open.flow_id, FlowRejectReason::MessageLimit);
        }
        let active = self.registry.active_direction_len(FlowDirection::Inbound);
        let active = u64::try_from(active).unwrap_or(u64::MAX);
        if active >= self.local_settings.max_active_incoming_flows {
            return self.inbound_rejection(open.flow_id, FlowRejectReason::ResourceLimit);
        }

        let pending = PendingInboundFlow {
            flow_id: open.flow_id,
            mode: open.delivery_mode,
            max_message_bytes: open.max_message_bytes,
        };
        self.pending_inbound = Some(pending);
        Ok(FlowControlProgress::InboundOpen(InboundOpenRequest {
            flow_id: pending.flow_id,
            mode: pending.mode,
            max_message_bytes: pending.max_message_bytes,
        }))
    }

    fn inbound_rejection(
        &self,
        flow_id: FlowId,
        reason: FlowRejectReason,
    ) -> Result<FlowControlProgress, FlowControlError> {
        let frame = reject_frame(flow_id, reason).map_err(FlowControlError::Allocation)?;
        Ok(FlowControlProgress::InboundRejected {
            flow_id,
            reason,
            frame,
        })
    }

    fn receive_accept(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        body: Vec<u8>,
    ) -> Result<FlowControlProgress, FlowControlError> {
        let accept = FlowAccept::decode(&body).map_err(FlowControlError::Body)?;
        self.require_local_response_side(accept.flow_id)?;
        let pending = self
            .pending_outbound
            .remove(&accept.flow_id.value())
            .ok_or(FlowControlError::UnknownPendingFlow(accept.flow_id))?;

        match endpoint.establish_flow(
            pending.key,
            pending.mode,
            pending.policy,
            pending.connection_limits,
        ) {
            Ok(()) => {}
            Err(FlowEstablishmentError::ActiveFlowLimitExceeded(_)) => {
                return self.outbound_resource_failure(accept.flow_id, pending.key, None);
            }
            Err(error) => return Err(FlowControlError::LocalEstablishment(error)),
        }

        match self.registry.register_consumed_accepted_flow(
            endpoint,
            accept.flow_id,
            pending.key,
            pending.stable_max_message_bytes,
        ) {
            Ok(()) => Ok(FlowControlProgress::OutboundEstablished(EstablishedFlow {
                flow_id: accept.flow_id,
                key: pending.key,
                mode: pending.mode,
                max_message_bytes: pending.stable_max_message_bytes.get(),
            })),
            Err(RegistryError::CapacityExceeded | RegistryError::AllocationFailed) => {
                let termination = endpoint
                    .terminate_flow(pending.key, FlowTerminationReason::Requested)
                    .map_err(FlowControlError::CoreState)?;
                self.outbound_resource_failure(accept.flow_id, pending.key, Some(termination))
            }
            Err(error) => {
                endpoint
                    .terminate_flow(pending.key, FlowTerminationReason::Requested)
                    .map_err(FlowControlError::CoreState)?;
                Err(FlowControlError::Registry(error))
            }
        }
    }

    fn outbound_resource_failure(
        &self,
        flow_id: FlowId,
        key: DeliveryFlowKey,
        termination: Option<FlowTermination>,
    ) -> Result<FlowControlProgress, FlowControlError> {
        let reason = FlowTerminateReason::ResourceFailure;
        let frame = terminate_frame(flow_id, reason).map_err(FlowControlError::Allocation)?;
        Ok(FlowControlProgress::OutboundFailedAfterAccept {
            flow_id,
            key,
            reason,
            termination,
            frame,
        })
    }

    fn receive_reject(&mut self, body: Vec<u8>) -> Result<FlowControlProgress, FlowControlError> {
        let reject = FlowReject::decode(&body).map_err(FlowControlError::Body)?;
        self.require_local_response_side(reject.flow_id)?;
        let pending = self
            .pending_outbound
            .remove(&reject.flow_id.value())
            .ok_or(FlowControlError::UnknownPendingFlow(reject.flow_id))?;
        Ok(FlowControlProgress::OutboundRejected {
            flow_id: reject.flow_id,
            key: pending.key,
            reason: reject.reason,
        })
    }

    fn receive_terminate(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        body: Vec<u8>,
    ) -> Result<FlowControlProgress, FlowControlError> {
        let terminate = FlowTerminate::decode(&body).map_err(FlowControlError::Body)?;
        let registered = self
            .registry
            .registered_flow(terminate.flow_id)
            .ok_or(FlowControlError::UnknownActiveFlow(terminate.flow_id))?;
        if registered.mode() == DeliveryMode::ReliableOrdered
            && terminate.reason == FlowTerminateReason::Normal
        {
            return Err(FlowControlError::ReliableNormalUsesFin(terminate.flow_id));
        }
        let flow = established_from_registered(terminate.flow_id, registered);
        let termination = endpoint
            .terminate_flow(flow.key, FlowTerminationReason::Requested)
            .map_err(FlowControlError::CoreState)?;
        self.registry.release(terminate.flow_id);
        Ok(FlowControlProgress::RemoteTerminated {
            flow,
            reason: terminate.reason,
            termination,
        })
    }

    fn require_local_response_side(&self, flow_id: FlowId) -> Result<(), FlowControlError> {
        if flow_id.side() == self.local_side {
            Ok(())
        } else {
            Err(FlowControlError::WrongResponseSide {
                expected: self.local_side,
                received: flow_id.side(),
            })
        }
    }

    pub(super) fn accept_inbound(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        request: InboundOpenRequest,
        admission: InboundAdmission,
    ) -> Result<InboundResolution, InboundAdmissionError> {
        self.require_pending_inbound(&request)?;
        if admission.key.connection() != self.connection {
            self.pending_inbound = None;
            return Err(InboundAdmissionError::WrongConnection {
                expected: self.connection,
                actual: admission.key.connection(),
            });
        }
        if admission.key.direction() != FlowDirection::Inbound {
            self.pending_inbound = None;
            return Err(InboundAdmissionError::WrongDirection(
                admission.key.direction(),
            ));
        }
        if let Err(error) = admission.policy.validate_for_mode(request.mode) {
            self.pending_inbound = None;
            return Err(InboundAdmissionError::InvalidPolicy(error));
        }

        let policy_max = u64::try_from(admission.policy.max_message_bytes()).unwrap_or(u64::MAX);
        if request.max_message_bytes > policy_max {
            return self.resolve_inbound_rejection(request, FlowRejectReason::MessageLimit);
        }
        let active = self.registry.active_direction_len(FlowDirection::Inbound);
        let active = u64::try_from(active).unwrap_or(u64::MAX);
        if active >= self.local_settings.max_active_incoming_flows {
            return self.resolve_inbound_rejection(request, FlowRejectReason::ResourceLimit);
        }

        let frame = accept_frame(request.flow_id).map_err(|error| {
            self.pending_inbound = None;
            InboundAdmissionError::Allocation(error)
        })?;

        match endpoint.establish_flow(
            admission.key,
            request.mode,
            admission.policy,
            admission.connection_limits,
        ) {
            Ok(()) => {}
            Err(FlowEstablishmentError::ActiveFlowLimitExceeded(_)) => {
                return self.resolve_inbound_rejection(request, FlowRejectReason::ResourceLimit);
            }
            Err(error) => {
                self.pending_inbound = None;
                return Err(InboundAdmissionError::LocalEstablishment(error));
            }
        }

        let stable_max = usize::try_from(request.max_message_bytes)
            .ok()
            .and_then(NonZeroUsize::new)
            .expect("accepted inbound policy maximum proves platform-sized non-zero stable limit");
        match self.registry.register_consumed_accepted_flow(
            endpoint,
            request.flow_id,
            admission.key,
            stable_max,
        ) {
            Ok(()) => {
                self.pending_inbound = None;
                Ok(InboundResolution::Accepted {
                    flow: EstablishedFlow {
                        flow_id: request.flow_id,
                        key: admission.key,
                        mode: request.mode,
                        max_message_bytes: stable_max.get(),
                    },
                    frame,
                })
            }
            Err(RegistryError::CapacityExceeded | RegistryError::AllocationFailed) => {
                endpoint
                    .terminate_flow(admission.key, FlowTerminationReason::Requested)
                    .map_err(|error| {
                        self.pending_inbound = None;
                        InboundAdmissionError::CoreRollback(error)
                    })?;
                self.resolve_inbound_rejection(request, FlowRejectReason::ResourceLimit)
            }
            Err(error) => {
                endpoint
                    .terminate_flow(admission.key, FlowTerminationReason::Requested)
                    .map_err(|rollback| {
                        self.pending_inbound = None;
                        InboundAdmissionError::CoreRollback(rollback)
                    })?;
                self.pending_inbound = None;
                Err(InboundAdmissionError::Registry(error))
            }
        }
    }

    pub(super) fn reject_inbound(
        &mut self,
        request: InboundOpenRequest,
        reason: FlowRejectReason,
    ) -> Result<InboundResolution, InboundAdmissionError> {
        self.require_pending_inbound(&request)?;
        self.resolve_inbound_rejection(request, reason)
    }

    fn resolve_inbound_rejection(
        &mut self,
        request: InboundOpenRequest,
        reason: FlowRejectReason,
    ) -> Result<InboundResolution, InboundAdmissionError> {
        let frame = reject_frame(request.flow_id, reason).map_err(|error| {
            self.pending_inbound = None;
            InboundAdmissionError::Allocation(error)
        })?;
        self.pending_inbound = None;
        Ok(InboundResolution::Rejected {
            flow_id: request.flow_id,
            reason,
            frame,
        })
    }

    fn require_pending_inbound(
        &self,
        request: &InboundOpenRequest,
    ) -> Result<(), InboundAdmissionError> {
        let expected = PendingInboundFlow {
            flow_id: request.flow_id,
            mode: request.mode,
            max_message_bytes: request.max_message_bytes,
        };
        if self.pending_inbound == Some(expected) {
            Ok(())
        } else {
            Err(InboundAdmissionError::RequestNotPending(request.flow_id))
        }
    }

    pub(super) fn terminate_local(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        flow_id: FlowId,
        reason: FlowTerminateReason,
    ) -> Result<LocalTermination, FlowControlError> {
        let registered = self
            .registry
            .registered_flow(flow_id)
            .ok_or(FlowControlError::UnknownActiveFlow(flow_id))?;
        if registered.mode() == DeliveryMode::ReliableOrdered
            && reason == FlowTerminateReason::Normal
        {
            return Err(FlowControlError::ReliableNormalUsesFin(flow_id));
        }
        let frame = terminate_frame(flow_id, reason).map_err(FlowControlError::Allocation)?;
        let flow = established_from_registered(flow_id, registered);
        let termination = endpoint
            .terminate_flow(flow.key, FlowTerminationReason::Requested)
            .map_err(FlowControlError::CoreState)?;
        self.registry.release(flow_id);
        Ok(LocalTermination {
            flow,
            reason,
            termination,
            frame,
        })
    }

    #[cfg(test)]
    fn pending_outbound_len(&self) -> usize {
        self.pending_outbound.len()
    }
}

fn established_from_registered(flow_id: FlowId, flow: RegisteredFlow) -> EstablishedFlow {
    EstablishedFlow {
        flow_id,
        key: flow.key(),
        mode: flow.mode(),
        max_message_bytes: flow.max_message_bytes(),
    }
}

const fn opposite_side(side: WireSide) -> WireSide {
    match side {
        WireSide::Client => WireSide::Server,
        WireSide::Server => WireSide::Client,
    }
}

fn owned_frame(
    frame_type: ControlFrameType,
    body: crate::wire::EncodedControlBody,
) -> Result<ControlFrame, TryReserveError> {
    let bytes = body.as_slice();
    let mut owned = Vec::new();
    owned.try_reserve_exact(bytes.len())?;
    owned.extend_from_slice(bytes);
    Ok(ControlFrame {
        frame_type,
        body: owned,
    })
}

fn accept_frame(flow_id: FlowId) -> Result<ControlFrame, TryReserveError> {
    owned_frame(
        ControlFrameType::FlowAccept,
        FlowAccept { flow_id }.encode(),
    )
}

fn reject_frame(
    flow_id: FlowId,
    reason: FlowRejectReason,
) -> Result<ControlFrame, TryReserveError> {
    owned_frame(
        ControlFrameType::FlowReject,
        FlowReject { flow_id, reason }.encode(),
    )
}

fn terminate_frame(
    flow_id: FlowId,
    reason: FlowTerminateReason,
) -> Result<ControlFrame, TryReserveError> {
    owned_frame(
        ControlFrameType::FlowTerminate,
        FlowTerminate { flow_id, reason }.encode(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        control::SemanticRole,
        datagram::{DatagramReceiveOutcome, receive_datagram},
        wire::{OpenFlow, encode_varint},
    };
    use runen_net::delivery::{
        DeliveryFlowHandle, OutboundPressureBehavior, ReceiverPressureBehavior, SubmissionOutcome,
    };

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    fn settings(
        role: SemanticRole,
        max_active_incoming_flows: u64,
        max_incoming_message_bytes: u64,
    ) -> Settings {
        Settings {
            semantic_role: role,
            max_control_frame_bytes: 256,
            max_negotiation_frame_bytes: 128,
            max_active_incoming_flows,
            max_incoming_message_bytes,
        }
    }

    fn control(
        connection: ConnectionHandle,
        side: WireSide,
        local_active: u64,
        local_message: u64,
        peer_active: u64,
        peer_message: u64,
    ) -> FlowControl {
        let (local_role, peer_role) = match side {
            WireSide::Client => (SemanticRole::Authority, SemanticRole::NonAuthority),
            WireSide::Server => (SemanticRole::NonAuthority, SemanticRole::Authority),
        };
        FlowControl::new(
            connection,
            side,
            settings(local_role, local_active, local_message),
            settings(peer_role, peer_active, peer_message),
        )
        .unwrap()
    }

    fn limits(active: usize) -> DeliveryScopeLimits {
        DeliveryScopeLimits::new(nz(active), nz(16), nz(4096))
    }

    fn key(connection: ConnectionHandle, direction: FlowDirection, handle: u64) -> DeliveryFlowKey {
        DeliveryFlowKey::new(connection, direction, DeliveryFlowHandle::new(handle))
    }

    fn policy(mode: DeliveryMode, max_message_bytes: usize) -> FlowResourcePolicy {
        match mode {
            DeliveryMode::ReliableOrdered => FlowResourcePolicy::new(
                nz(max_message_bytes),
                nz(8),
                nz(2048),
                OutboundPressureBehavior::RejectNew,
                ReceiverPressureBehavior::TerminateReliable,
            ),
            DeliveryMode::UnreliableUnordered | DeliveryMode::UnreliableSequenced => {
                FlowResourcePolicy::new(
                    nz(max_message_bytes),
                    nz(8),
                    nz(2048),
                    OutboundPressureBehavior::RejectNew,
                    ReceiverPressureBehavior::DropIncomingUnreliable,
                )
            }
        }
    }

    fn frame(frame_type: ControlFrameType, body: crate::wire::EncodedControlBody) -> ControlFrame {
        ControlFrame {
            frame_type,
            body: body.as_slice().to_vec(),
        }
    }

    fn accept(flow_id: FlowId) -> ControlFrame {
        frame(
            ControlFrameType::FlowAccept,
            FlowAccept { flow_id }.encode(),
        )
    }

    fn reject(flow_id: FlowId, reason: FlowRejectReason) -> ControlFrame {
        frame(
            ControlFrameType::FlowReject,
            FlowReject { flow_id, reason }.encode(),
        )
    }

    fn terminate(flow_id: FlowId, reason: FlowTerminateReason) -> ControlFrame {
        frame(
            ControlFrameType::FlowTerminate,
            FlowTerminate { flow_id, reason }.encode(),
        )
    }

    fn prepare_reliable(
        control: &mut FlowControl,
        endpoint: &DeliveryEndpoint,
        key: DeliveryFlowKey,
    ) -> PreparedOutboundOpen {
        control
            .prepare_outbound_open(
                endpoint,
                OutboundOpenRequest {
                    key,
                    mode: DeliveryMode::ReliableOrdered,
                    policy: policy(DeliveryMode::ReliableOrdered, 64),
                    stable_max_message_bytes: nz(64),
                    connection_limits: limits(8),
                },
                None,
            )
            .unwrap()
    }

    #[test]
    fn outbound_preflight_failures_do_not_consume_flow_id_or_create_core_state() {
        let connection = ConnectionHandle::new(1);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 64);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let outbound = key(connection, FlowDirection::Outbound, 900);
        let reliable = policy(DeliveryMode::ReliableOrdered, 64);

        assert!(matches!(
            control.prepare_outbound_open(
                &endpoint,
                OutboundOpenRequest {
                    key: outbound,
                    mode: DeliveryMode::ReliableOrdered,
                    policy: reliable,
                    stable_max_message_bytes: nz(32),
                    connection_limits: limits(8),
                },
                None,
            ),
            Err(OutboundOpenError::StableMessageLimitMismatch { .. })
        ));
        assert!(matches!(
            control.prepare_outbound_open(
                &endpoint,
                OutboundOpenRequest {
                    key: outbound,
                    mode: DeliveryMode::ReliableOrdered,
                    policy: policy(DeliveryMode::ReliableOrdered, 128),
                    stable_max_message_bytes: nz(128),
                    connection_limits: limits(8),
                },
                None,
            ),
            Err(OutboundOpenError::PeerMessageLimit { .. })
        ));

        endpoint
            .establish_flow(outbound, DeliveryMode::ReliableOrdered, reliable, limits(8))
            .unwrap();
        assert!(matches!(
            control.prepare_outbound_open(
                &endpoint,
                OutboundOpenRequest {
                    key: outbound,
                    mode: DeliveryMode::ReliableOrdered,
                    policy: reliable,
                    stable_max_message_bytes: nz(64),
                    connection_limits: limits(8),
                },
                None,
            ),
            Err(OutboundOpenError::CoreFlowAlreadyExists(key)) if key == outbound
        ));
        endpoint
            .terminate_flow(outbound, FlowTerminationReason::Requested)
            .unwrap();

        let prepared = prepare_reliable(&mut control, &endpoint, outbound);
        assert_eq!(prepared.flow.flow_id().sequence(), 0);
        assert_eq!(endpoint.active_flows(), 0);
        assert_eq!(endpoint.flow_contract(outbound), None);
        assert_eq!(control.pending_outbound_len(), 1);

        assert!(matches!(
            control.prepare_outbound_open(
                &endpoint,
                OutboundOpenRequest {
                    key: outbound,
                    mode: DeliveryMode::ReliableOrdered,
                    policy: reliable,
                    stable_max_message_bytes: nz(64),
                    connection_limits: limits(8),
                },
                None,
            ),
            Err(OutboundOpenError::PendingCoreFlow(key)) if key == outbound
        ));
        assert_eq!(control.pending_outbound_len(), 1);
    }

    #[test]
    fn sequenced_unreliable_open_uses_worst_case_sequence_envelope_before_consumption() {
        let connection = ConnectionHandle::new(2);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 256);
        let endpoint = DeliveryEndpoint::new(limits(8));
        let outbound = key(connection, FlowDirection::Outbound, 1);
        let p = policy(DeliveryMode::UnreliableSequenced, 64);

        assert!(matches!(
            control.prepare_outbound_open(
                &endpoint,
                OutboundOpenRequest {
                    key: outbound,
                    mode: DeliveryMode::UnreliableSequenced,
                    policy: p,
                    stable_max_message_bytes: nz(64),
                    connection_limits: limits(8),
                },
                Some(72),
            ),
            Err(OutboundOpenError::DatagramTooSmall {
                needed: 73,
                available: 72,
            })
        ));
        let prepared = control
            .prepare_outbound_open(
                &endpoint,
                OutboundOpenRequest {
                    key: outbound,
                    mode: DeliveryMode::UnreliableSequenced,
                    policy: p,
                    stable_max_message_bytes: nz(64),
                    connection_limits: limits(8),
                },
                Some(73),
            )
            .unwrap();
        assert_eq!(prepared.flow.flow_id().sequence(), 0);
    }

    #[test]
    fn peer_active_ceiling_bounds_pending_and_active_outbound_flows() {
        let connection = ConnectionHandle::new(3);
        let mut control = control(connection, WireSide::Client, 4, 256, 1, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let first_key = key(connection, FlowDirection::Outbound, 1);
        let first = prepare_reliable(&mut control, &endpoint, first_key);

        assert!(matches!(
            control.prepare_outbound_open(
                &endpoint,
                OutboundOpenRequest {
                    key: key(connection, FlowDirection::Outbound, 2),
                    mode: DeliveryMode::ReliableOrdered,
                    policy: policy(DeliveryMode::ReliableOrdered, 64),
                    stable_max_message_bytes: nz(64),
                    connection_limits: limits(8),
                },
                None,
            ),
            Err(OutboundOpenError::PeerActiveFlowLimit { .. })
        ));

        assert!(matches!(
            control
                .receive(
                    &mut endpoint,
                    reject(first.flow.flow_id(), FlowRejectReason::ResourceLimit),
                )
                .unwrap(),
            FlowControlProgress::OutboundRejected { .. }
        ));
        let second = prepare_reliable(
            &mut control,
            &endpoint,
            key(connection, FlowDirection::Outbound, 2),
        );
        assert_eq!(second.flow.flow_id().sequence(), 1);

        let established = control
            .receive(&mut endpoint, accept(second.flow.flow_id()))
            .unwrap();
        assert!(matches!(
            established,
            FlowControlProgress::OutboundEstablished(_)
        ));
        assert!(matches!(
            control.prepare_outbound_open(
                &endpoint,
                OutboundOpenRequest {
                    key: key(connection, FlowDirection::Outbound, 3),
                    mode: DeliveryMode::ReliableOrdered,
                    policy: policy(DeliveryMode::ReliableOrdered, 64),
                    stable_max_message_bytes: nz(64),
                    connection_limits: limits(8),
                },
                None,
            ),
            Err(OutboundOpenError::PeerActiveFlowLimit { .. })
        ));
    }

    #[test]
    fn outbound_flow_is_established_only_after_matching_accept() {
        let connection = ConnectionHandle::new(4);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let outbound = key(connection, FlowDirection::Outbound, 444);
        let prepared = prepare_reliable(&mut control, &endpoint, outbound);

        assert_eq!(endpoint.flow_contract(outbound), None);
        assert_eq!(
            control.registry().registered_flow(prepared.flow.flow_id()),
            None
        );
        let established = control
            .receive(&mut endpoint, accept(prepared.flow.flow_id()))
            .unwrap();
        let FlowControlProgress::OutboundEstablished(flow) = established else {
            panic!("expected established outbound flow");
        };
        assert_eq!(flow.flow_id(), prepared.flow.flow_id());
        assert_eq!(flow.key(), outbound);
        assert_eq!(flow.mode(), DeliveryMode::ReliableOrdered);
        assert_eq!(flow.max_message_bytes(), 64);
        assert_eq!(
            endpoint.flow_contract(outbound),
            Some((
                DeliveryMode::ReliableOrdered,
                policy(DeliveryMode::ReliableOrdered, 64)
            ))
        );
        assert_eq!(
            control
                .registry()
                .registered_flow(prepared.flow.flow_id())
                .unwrap()
                .key(),
            outbound
        );
    }

    #[test]
    fn pressure_after_open_before_accept_terminates_peer_accepted_flow() {
        let connection = ConnectionHandle::new(5);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(1));
        let requested = key(connection, FlowDirection::Outbound, 1);
        let prepared = control
            .prepare_outbound_open(
                &endpoint,
                OutboundOpenRequest {
                    key: requested,
                    mode: DeliveryMode::ReliableOrdered,
                    policy: policy(DeliveryMode::ReliableOrdered, 64),
                    stable_max_message_bytes: nz(64),
                    connection_limits: limits(1),
                },
                None,
            )
            .unwrap();

        endpoint
            .establish_flow(
                key(connection, FlowDirection::Inbound, 999),
                DeliveryMode::ReliableOrdered,
                policy(DeliveryMode::ReliableOrdered, 64),
                limits(1),
            )
            .unwrap();
        let progress = control
            .receive(&mut endpoint, accept(prepared.flow.flow_id()))
            .unwrap();
        let FlowControlProgress::OutboundFailedAfterAccept {
            flow_id,
            key,
            reason,
            termination,
            frame,
        } = progress
        else {
            panic!("expected resource termination after peer accept");
        };
        assert_eq!(flow_id, prepared.flow.flow_id());
        assert_eq!(key, requested);
        assert_eq!(reason, FlowTerminateReason::ResourceFailure);
        assert_eq!(termination, None);
        assert_eq!(frame.frame_type, ControlFrameType::FlowTerminate);
        assert_eq!(endpoint.flow_contract(requested), None);
        assert_eq!(control.registry().registered_flow(flow_id), None);
    }

    #[test]
    fn registry_failure_after_accept_preserves_core_rollback_termination() {
        let connection = ConnectionHandle::new(18);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 256);
        control.registry = AcceptedFlowRegistry::new(WireSide::Client, nz(1));
        let mut endpoint = DeliveryEndpoint::new(limits(8));

        let occupied_key = key(connection, FlowDirection::Inbound, 99);
        endpoint
            .establish_flow(
                occupied_key,
                DeliveryMode::ReliableOrdered,
                policy(DeliveryMode::ReliableOrdered, 64),
                limits(8),
            )
            .unwrap();
        let occupied_flow = FlowId::new(WireSide::Server, 0).unwrap();
        control
            .registry
            .register_consumed_accepted_flow(&endpoint, occupied_flow, occupied_key, nz(64))
            .unwrap();

        let requested = key(connection, FlowDirection::Outbound, 1);
        let prepared = prepare_reliable(&mut control, &endpoint, requested);
        let progress = control
            .receive(&mut endpoint, accept(prepared.flow.flow_id()))
            .unwrap();
        let FlowControlProgress::OutboundFailedAfterAccept {
            flow_id,
            key,
            reason,
            termination: Some(termination),
            frame,
        } = progress
        else {
            panic!("registry rollback did not preserve Core termination evidence");
        };
        assert_eq!(flow_id, prepared.flow.flow_id());
        assert_eq!(key, requested);
        assert_eq!(reason, FlowTerminateReason::ResourceFailure);
        assert_eq!(termination.key, requested);
        assert_eq!(termination.reason, FlowTerminationReason::Requested);
        assert_eq!(termination.pending_messages, 0);
        assert!(!termination.reliable_obligation_failed);
        assert_eq!(frame.frame_type, ControlFrameType::FlowTerminate);
        assert_eq!(endpoint.flow_contract(requested), None);
        assert_eq!(endpoint.flow_contract(occupied_key).is_some(), true);
        assert_eq!(control.registry().registered_flow(flow_id), None);
        assert_eq!(
            control
                .registry()
                .registered_flow(occupied_flow)
                .unwrap()
                .key(),
            occupied_key
        );
    }

    #[test]
    fn malformed_wrong_side_and_out_of_order_inbound_open_do_not_advance_peer_cursor() {
        let connection = ConnectionHandle::new(6);
        let mut control = control(connection, WireSide::Server, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));

        let malformed = ControlFrame {
            frame_type: ControlFrameType::OpenFlow,
            body: vec![0, 3, 1],
        };
        assert!(matches!(
            control.receive(&mut endpoint, malformed),
            Err(FlowControlError::Body(
                ControlBodyError::UnknownDeliveryMode(3)
            ))
        ));

        let wrong_side = frame(
            ControlFrameType::OpenFlow,
            OpenFlow::new(
                FlowId::new(WireSide::Server, 0).unwrap(),
                DeliveryMode::ReliableOrdered,
                64,
            )
            .unwrap()
            .encode(),
        );
        assert!(matches!(
            control.receive(&mut endpoint, wrong_side),
            Err(FlowControlError::FlowId(
                FlowIdCursorError::WrongSide { .. }
            ))
        ));

        let out_of_order = frame(
            ControlFrameType::OpenFlow,
            OpenFlow::new(
                FlowId::new(WireSide::Client, 1).unwrap(),
                DeliveryMode::ReliableOrdered,
                64,
            )
            .unwrap()
            .encode(),
        );
        assert!(matches!(
            control.receive(&mut endpoint, out_of_order),
            Err(FlowControlError::FlowId(
                FlowIdCursorError::UnexpectedSequence {
                    expected: 0,
                    received: 1,
                }
            ))
        ));

        let valid = frame(
            ControlFrameType::OpenFlow,
            OpenFlow::new(
                FlowId::new(WireSide::Client, 0).unwrap(),
                DeliveryMode::ReliableOrdered,
                64,
            )
            .unwrap()
            .encode(),
        );
        let FlowControlProgress::InboundOpen(request) =
            control.receive(&mut endpoint, valid).unwrap()
        else {
            panic!("expected inbound request");
        };
        assert_eq!(request.flow_id().sequence(), 0);
    }

    #[test]
    fn valid_rejected_inbound_flow_id_is_consumed() {
        let connection = ConnectionHandle::new(7);
        let mut control = control(connection, WireSide::Server, 4, 32, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let first = frame(
            ControlFrameType::OpenFlow,
            OpenFlow::new(
                FlowId::new(WireSide::Client, 0).unwrap(),
                DeliveryMode::ReliableOrdered,
                64,
            )
            .unwrap()
            .encode(),
        );
        assert!(matches!(
            control.receive(&mut endpoint, first).unwrap(),
            FlowControlProgress::InboundRejected {
                reason: FlowRejectReason::MessageLimit,
                ..
            }
        ));

        let second = frame(
            ControlFrameType::OpenFlow,
            OpenFlow::new(
                FlowId::new(WireSide::Client, 1).unwrap(),
                DeliveryMode::ReliableOrdered,
                32,
            )
            .unwrap()
            .encode(),
        );
        let FlowControlProgress::InboundOpen(request) =
            control.receive(&mut endpoint, second).unwrap()
        else {
            panic!("expected next consumed flow id");
        };
        assert_eq!(request.flow_id().sequence(), 1);
    }

    #[test]
    fn inbound_admission_uses_host_core_identity_and_exact_profile_ceiling() {
        let connection = ConnectionHandle::new(8);
        let mut control = control(connection, WireSide::Server, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let wire_flow = FlowId::new(WireSide::Client, 0).unwrap();
        let open = frame(
            ControlFrameType::OpenFlow,
            OpenFlow::new(wire_flow, DeliveryMode::ReliableOrdered, 64)
                .unwrap()
                .encode(),
        );
        let FlowControlProgress::InboundOpen(request) =
            control.receive(&mut endpoint, open).unwrap()
        else {
            panic!("expected inbound request");
        };
        let host_key = key(connection, FlowDirection::Inbound, 9_999);
        let resolution = control
            .accept_inbound(
                &mut endpoint,
                request,
                InboundAdmission {
                    key: host_key,
                    policy: policy(DeliveryMode::ReliableOrdered, 128),
                    connection_limits: limits(8),
                },
            )
            .unwrap();
        let InboundResolution::Accepted { flow, frame } = resolution else {
            panic!("expected accept");
        };
        assert_eq!(flow.flow_id(), wire_flow);
        assert_eq!(flow.key(), host_key);
        assert_ne!(flow.key().handle().get(), wire_flow.value());
        assert_eq!(flow.max_message_bytes(), 64);
        assert_eq!(frame.frame_type, ControlFrameType::FlowAccept);
        assert_eq!(
            endpoint
                .flow_contract(host_key)
                .unwrap()
                .1
                .max_message_bytes(),
            128
        );
        assert_eq!(
            control
                .registry()
                .registered_flow(wire_flow)
                .unwrap()
                .max_message_bytes(),
            64
        );
    }

    #[test]
    fn inbound_policy_message_limit_and_core_pressure_reject_without_state() {
        let connection = ConnectionHandle::new(9);
        let mut control = control(connection, WireSide::Server, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(1));
        let first_id = FlowId::new(WireSide::Client, 0).unwrap();
        let first = frame(
            ControlFrameType::OpenFlow,
            OpenFlow::new(first_id, DeliveryMode::ReliableOrdered, 64)
                .unwrap()
                .encode(),
        );
        let FlowControlProgress::InboundOpen(request) =
            control.receive(&mut endpoint, first).unwrap()
        else {
            panic!("expected request");
        };
        let result = control
            .accept_inbound(
                &mut endpoint,
                request,
                InboundAdmission {
                    key: key(connection, FlowDirection::Inbound, 1),
                    policy: policy(DeliveryMode::ReliableOrdered, 32),
                    connection_limits: limits(1),
                },
            )
            .unwrap();
        assert!(matches!(
            result,
            InboundResolution::Rejected {
                reason: FlowRejectReason::MessageLimit,
                ..
            }
        ));
        assert_eq!(endpoint.active_flows(), 0);

        endpoint
            .establish_flow(
                key(connection, FlowDirection::Outbound, 500),
                DeliveryMode::ReliableOrdered,
                policy(DeliveryMode::ReliableOrdered, 64),
                limits(1),
            )
            .unwrap();
        let second_id = FlowId::new(WireSide::Client, 1).unwrap();
        let second = frame(
            ControlFrameType::OpenFlow,
            OpenFlow::new(second_id, DeliveryMode::ReliableOrdered, 64)
                .unwrap()
                .encode(),
        );
        let FlowControlProgress::InboundOpen(request) =
            control.receive(&mut endpoint, second).unwrap()
        else {
            panic!("expected request");
        };
        let result = control
            .accept_inbound(
                &mut endpoint,
                request,
                InboundAdmission {
                    key: key(connection, FlowDirection::Inbound, 2),
                    policy: policy(DeliveryMode::ReliableOrdered, 64),
                    connection_limits: limits(1),
                },
            )
            .unwrap();
        assert!(matches!(
            result,
            InboundResolution::Rejected {
                reason: FlowRejectReason::ResourceLimit,
                ..
            }
        ));
        assert_eq!(endpoint.active_flows(), 1);
    }

    #[test]
    fn pending_inbound_blocks_only_another_open_not_other_flow_control() {
        let connection = ConnectionHandle::new(10);
        let mut control = control(connection, WireSide::Server, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let outbound = key(connection, FlowDirection::Outbound, 700);
        let prepared = prepare_reliable(&mut control, &endpoint, outbound);

        let inbound_open = frame(
            ControlFrameType::OpenFlow,
            OpenFlow::new(
                FlowId::new(WireSide::Client, 0).unwrap(),
                DeliveryMode::ReliableOrdered,
                64,
            )
            .unwrap()
            .encode(),
        );
        let FlowControlProgress::InboundOpen(request) =
            control.receive(&mut endpoint, inbound_open).unwrap()
        else {
            panic!("expected pending inbound request");
        };

        assert!(matches!(
            control
                .receive(
                    &mut endpoint,
                    reject(prepared.flow.flow_id(), FlowRejectReason::ResourceLimit),
                )
                .unwrap(),
            FlowControlProgress::OutboundRejected { .. }
        ));

        let second_open = frame(
            ControlFrameType::OpenFlow,
            OpenFlow::new(
                FlowId::new(WireSide::Client, 1).unwrap(),
                DeliveryMode::ReliableOrdered,
                64,
            )
            .unwrap()
            .encode(),
        );
        assert!(matches!(
            control.receive(&mut endpoint, second_open),
            Err(FlowControlError::InboundDecisionPending(flow_id))
                if flow_id == request.flow_id()
        ));
        control
            .reject_inbound(request, FlowRejectReason::ResourceLimit)
            .unwrap();
    }

    #[test]
    fn full_two_endpoint_exchange_uses_one_wire_flow_and_independent_core_handles() {
        let client_connection = ConnectionHandle::new(11);
        let server_connection = ConnectionHandle::new(22);
        let mut client = control(client_connection, WireSide::Client, 4, 256, 4, 256);
        let mut server = control(server_connection, WireSide::Server, 4, 256, 4, 256);
        let mut client_endpoint = DeliveryEndpoint::new(limits(8));
        let mut server_endpoint = DeliveryEndpoint::new(limits(8));
        let client_key = key(client_connection, FlowDirection::Outbound, 1001);
        let server_key = key(server_connection, FlowDirection::Inbound, 7007);

        let prepared = prepare_reliable(&mut client, &client_endpoint, client_key);
        let FlowControlProgress::InboundOpen(request) = server
            .receive(&mut server_endpoint, prepared.frame)
            .unwrap()
        else {
            panic!("server must receive open");
        };
        let InboundResolution::Accepted {
            flow: server_flow,
            frame: accept,
        } = server
            .accept_inbound(
                &mut server_endpoint,
                request,
                InboundAdmission {
                    key: server_key,
                    policy: policy(DeliveryMode::ReliableOrdered, 128),
                    connection_limits: limits(8),
                },
            )
            .unwrap()
        else {
            panic!("server must accept");
        };
        let FlowControlProgress::OutboundEstablished(client_flow) =
            client.receive(&mut client_endpoint, accept).unwrap()
        else {
            panic!("client must establish");
        };

        assert_eq!(client_flow.flow_id(), server_flow.flow_id());
        assert_eq!(client_flow.mode(), server_flow.mode());
        assert_eq!(
            client_flow.max_message_bytes(),
            server_flow.max_message_bytes()
        );
        assert_ne!(client_flow.key().handle(), server_flow.key().handle());
        assert_eq!(client_endpoint.active_flows(), 1);
        assert_eq!(server_endpoint.active_flows(), 1);
    }

    #[test]
    fn remote_unreliable_termination_retires_core_registry_and_stale_datagram() {
        let connection = ConnectionHandle::new(12);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let outbound = key(connection, FlowDirection::Outbound, 1);
        let prepared = control
            .prepare_outbound_open(
                &endpoint,
                OutboundOpenRequest {
                    key: outbound,
                    mode: DeliveryMode::UnreliableUnordered,
                    policy: policy(DeliveryMode::UnreliableUnordered, 64),
                    stable_max_message_bytes: nz(64),
                    connection_limits: limits(8),
                },
                Some(128),
            )
            .unwrap();
        control
            .receive(&mut endpoint, accept(prepared.flow.flow_id()))
            .unwrap();

        assert!(matches!(
            control
                .receive(
                    &mut endpoint,
                    terminate(
                        prepared.flow.flow_id(),
                        FlowTerminateReason::ResourceFailure,
                    ),
                )
                .unwrap(),
            FlowControlProgress::RemoteTerminated {
                reason: FlowTerminateReason::ResourceFailure,
                ..
            }
        ));
        assert_eq!(endpoint.flow_contract(outbound), None);
        assert_eq!(
            control.registry().registered_flow(prepared.flow.flow_id()),
            None
        );

        let mut datagram = encode_varint(prepared.flow.flow_id().value())
            .unwrap()
            .as_slice()
            .to_vec();
        datagram.extend_from_slice(b"stale");
        assert_eq!(
            receive_datagram(&mut endpoint, control.registry(), &datagram).unwrap(),
            DatagramReceiveOutcome::DiscardedUnknownFlow
        );
    }

    #[test]
    fn remote_reliable_exception_preserves_wire_reason_without_redefining_core_reason() {
        let connection = ConnectionHandle::new(13);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let outbound = key(connection, FlowDirection::Outbound, 1);
        let prepared = prepare_reliable(&mut control, &endpoint, outbound);
        control
            .receive(&mut endpoint, accept(prepared.flow.flow_id()))
            .unwrap();
        assert!(matches!(
            endpoint.submit(outbound, b"accepted".to_vec()).unwrap(),
            SubmissionOutcome::Accepted {
                accepted_index: 0,
                ..
            }
        ));

        let result = control
            .receive(
                &mut endpoint,
                terminate(
                    prepared.flow.flow_id(),
                    FlowTerminateReason::ReliableDeliveryFailure,
                ),
            )
            .unwrap();
        let FlowControlProgress::RemoteTerminated {
            flow,
            reason,
            termination,
        } = result
        else {
            panic!("expected reliable terminal progress");
        };
        assert_eq!(flow.key(), outbound);
        assert_eq!(reason, FlowTerminateReason::ReliableDeliveryFailure);
        assert_eq!(termination.reason, FlowTerminationReason::Requested);
        assert_eq!(termination.pending_messages, 1);
        assert!(termination.reliable_obligation_failed);
        assert_eq!(endpoint.flow_contract(outbound), None);
        assert_eq!(
            control.registry().registered_flow(prepared.flow.flow_id()),
            None
        );
    }

    #[test]
    fn local_reliable_exception_preserves_wire_reason_without_redefining_core_reason() {
        let connection = ConnectionHandle::new(14);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let outbound = key(connection, FlowDirection::Outbound, 1);
        let prepared = prepare_reliable(&mut control, &endpoint, outbound);
        control
            .receive(&mut endpoint, accept(prepared.flow.flow_id()))
            .unwrap();
        endpoint.submit(outbound, b"accepted".to_vec()).unwrap();

        let terminated = control
            .terminate_local(
                &mut endpoint,
                prepared.flow.flow_id(),
                FlowTerminateReason::ProtocolFailure,
            )
            .unwrap();
        assert_eq!(terminated.reason, FlowTerminateReason::ProtocolFailure);
        assert_eq!(terminated.frame.frame_type, ControlFrameType::FlowTerminate);
        assert_eq!(
            terminated.termination.reason,
            FlowTerminationReason::Requested
        );
        assert!(terminated.termination.reliable_obligation_failed);
        assert_eq!(endpoint.flow_contract(outbound), None);
    }

    #[test]
    fn duplicate_unknown_wrong_side_and_state_inapplicable_responses_fail_closed() {
        let connection = ConnectionHandle::new(15);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let prepared = prepare_reliable(
            &mut control,
            &endpoint,
            key(connection, FlowDirection::Outbound, 1),
        );
        let flow_id = prepared.flow.flow_id();

        control
            .receive(
                &mut endpoint,
                reject(flow_id, FlowRejectReason::ResourceLimit),
            )
            .unwrap();
        assert!(matches!(
            control.receive(
                &mut endpoint,
                reject(flow_id, FlowRejectReason::ResourceLimit),
            ),
            Err(FlowControlError::UnknownPendingFlow(id)) if id == flow_id
        ));

        let wrong = FlowId::new(WireSide::Server, 0).unwrap();
        assert!(matches!(
            control.receive(&mut endpoint, accept(wrong)),
            Err(FlowControlError::WrongResponseSide {
                expected: WireSide::Client,
                received: WireSide::Server,
            })
        ));
        assert!(matches!(
            control.receive(
                &mut endpoint,
                terminate(flow_id, FlowTerminateReason::ResourceFailure),
            ),
            Err(FlowControlError::UnknownActiveFlow(id)) if id == flow_id
        ));
    }

    #[test]
    fn reliable_normal_control_termination_is_not_a_fin_substitute() {
        let connection = ConnectionHandle::new(16);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let outbound = key(connection, FlowDirection::Outbound, 1);
        let prepared = prepare_reliable(&mut control, &endpoint, outbound);
        control
            .receive(&mut endpoint, accept(prepared.flow.flow_id()))
            .unwrap();

        assert!(matches!(
            control.receive(
                &mut endpoint,
                terminate(prepared.flow.flow_id(), FlowTerminateReason::Normal),
            ),
            Err(FlowControlError::ReliableNormalUsesFin(id))
                if id == prepared.flow.flow_id()
        ));
        assert!(matches!(
            control.terminate_local(
                &mut endpoint,
                prepared.flow.flow_id(),
                FlowTerminateReason::Normal,
            ),
            Err(FlowControlError::ReliableNormalUsesFin(id))
                if id == prepared.flow.flow_id()
        ));
        assert!(endpoint.flow_contract(outbound).is_some());
    }

    #[test]
    fn local_unreliable_normal_termination_retires_active_mapping_once() {
        let connection = ConnectionHandle::new(17);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let outbound = key(connection, FlowDirection::Outbound, 1);
        let prepared = control
            .prepare_outbound_open(
                &endpoint,
                OutboundOpenRequest {
                    key: outbound,
                    mode: DeliveryMode::UnreliableUnordered,
                    policy: policy(DeliveryMode::UnreliableUnordered, 64),
                    stable_max_message_bytes: nz(64),
                    connection_limits: limits(8),
                },
                Some(128),
            )
            .unwrap();
        control
            .receive(&mut endpoint, accept(prepared.flow.flow_id()))
            .unwrap();

        let terminated = control
            .terminate_local(
                &mut endpoint,
                prepared.flow.flow_id(),
                FlowTerminateReason::Normal,
            )
            .unwrap();
        assert_eq!(terminated.reason, FlowTerminateReason::Normal);
        assert_eq!(terminated.frame.frame_type, ControlFrameType::FlowTerminate);
        assert_eq!(
            terminated.termination.reason,
            FlowTerminationReason::Requested
        );
        assert_eq!(endpoint.flow_contract(outbound), None);
        assert_eq!(
            control.registry().registered_flow(prepared.flow.flow_id()),
            None
        );
        assert!(matches!(
            control.terminate_local(
                &mut endpoint,
                prepared.flow.flow_id(),
                FlowTerminateReason::Normal,
            ),
            Err(FlowControlError::UnknownActiveFlow(id))
                if id == prepared.flow.flow_id()
        ));
    }
}
