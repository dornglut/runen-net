use std::collections::{HashMap, TryReserveError};
use std::num::NonZeroUsize;

use runen_net::{
    delivery::{
        DeliveryEndpoint, DeliveryFlowKey, DeliveryMode, DeliveryOperationError, DeliveryPolicyError,
        DeliveryScopeLimits, FlowDirection, FlowEstablishmentError, FlowResourcePolicy,
        FlowTermination, FlowTerminationReason,
    },
    identity::ConnectionHandle,
};

use crate::{
    control::{ControlFrame, ControlFrameType, ProfileReadyConnection, Settings},
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

#[derive(Debug)]
pub(super) struct PreparedOutboundOpen {
    pub(super) frame: ControlFrame,
    pub(super) flow: EstablishedFlow,
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

#[derive(Debug)]
pub(super) struct FlowControl {
    connection: ConnectionHandle,
    local_side: WireSide,
    local_settings: Settings,
    peer_settings: Settings,
    local_flow_ids: FlowIdCursor,
    peer_flow_ids: FlowIdCursor,
    pending_outbound: HashMap<u64, PendingOutboundFlow>,
    pending_inbound: Option<FlowId>,
    registry: AcceptedFlowRegistry,
}

impl FlowControl {
    pub(super) fn from_profile(
        connection: ConnectionHandle,
        profile: &ProfileReadyConnection,
    ) -> Result<Self, FlowControlConfigError> {
        Self::new(
            connection,
            profile.side(),
            profile.local_profile().local_settings(),
            profile.peer_settings(),
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

    pub(super) fn prepare_outbound_open(
        &mut self,
        key: DeliveryFlowKey,
        mode: DeliveryMode,
        policy: FlowResourcePolicy,
        stable_max_message_bytes: NonZeroUsize,
        connection_limits: DeliveryScopeLimits,
        current_datagram_size: Option<usize>,
    ) -> Result<PreparedOutboundOpen, OutboundOpenError> {
        if key.connection() != self.connection {
            return Err(OutboundOpenError::WrongConnection {
                expected: self.connection,
                actual: key.connection(),
            });
        }
        if key.direction() != FlowDirection::Outbound {
            return Err(OutboundOpenError::WrongDirection(key.direction()));
        }
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

        let active = self
            .registry
            .active_direction_len(FlowDirection::Outbound);
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

        let allocated = self.local_flow_ids.allocate().map_err(OutboundOpenError::FlowId)?;
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
            flow: EstablishedFlow {
                flow_id,
                key,
                mode,
                max_message_bytes: stable_max,
            },
        })
    }

    pub(super) fn receive(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        frame: ControlFrame,
    ) -> Result<FlowControlProgress, FlowControlError> {
        if let Some(flow_id) = self.pending_inbound {
            return Err(FlowControlError::InboundDecisionPending(flow_id));
        }
        match frame.frame_type {
            ControlFrameType::OpenFlow => self.receive_open(frame.body),
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
        let active = self
            .registry
            .active_direction_len(FlowDirection::Inbound);
        let active = u64::try_from(active).unwrap_or(u64::MAX);
        if active >= self.local_settings.max_active_incoming_flows {
            return self.inbound_rejection(open.flow_id, FlowRejectReason::ResourceLimit);
        }

        self.pending_inbound = Some(open.flow_id);
        Ok(FlowControlProgress::InboundOpen(InboundOpenRequest {
            flow_id: open.flow_id,
            mode: open.delivery_mode,
            max_message_bytes: open.max_message_bytes,
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
                return self.outbound_resource_failure(accept.flow_id, pending.key);
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
                endpoint
                    .terminate_flow(pending.key, FlowTerminationReason::Requested)
                    .map_err(FlowControlError::CoreState)?;
                self.outbound_resource_failure(accept.flow_id, pending.key)
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
    ) -> Result<FlowControlProgress, FlowControlError> {
        let reason = FlowTerminateReason::ResourceFailure;
        let frame = terminate_frame(flow_id, reason).map_err(FlowControlError::Allocation)?;
        Ok(FlowControlProgress::OutboundFailedAfterAccept {
            flow_id,
            key,
            reason,
            frame,
        })
    }

    fn receive_reject(
        &mut self,
        body: Vec<u8>,
    ) -> Result<FlowControlProgress, FlowControlError> {
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
        let active = self
            .registry
            .active_direction_len(FlowDirection::Inbound);
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
        if self.pending_inbound == Some(request.flow_id) {
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
        if registered.mode() == DeliveryMode::ReliableOrdered && reason == FlowTerminateReason::Normal
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
    owned_frame(ControlFrameType::FlowAccept, FlowAccept { flow_id }.encode())
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
        wire::{encode_varint, OpenFlow},
    };
    use runen_net::delivery::{
        DeliveryFlowHandle, OutboundPressureBehavior, ReceiverPressureBehavior,
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

    fn key(
        connection: ConnectionHandle,
        direction: FlowDirection,
        handle: u64,
    ) -> DeliveryFlowKey {
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

    #[test]
    fn outbound_preflight_failures_do_not_consume_flow_id_or_create_core_state() {
        let connection = ConnectionHandle::new(1);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 64);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let outbound = key(connection, FlowDirection::Outbound, 900);
        let reliable = policy(DeliveryMode::ReliableOrdered, 64);

        assert!(matches!(
            control.prepare_outbound_open(
                outbound,
                DeliveryMode::ReliableOrdered,
                reliable,
                nz(32),
                limits(8),
                None,
            ),
            Err(OutboundOpenError::StableMessageLimitMismatch { .. })
        ));
        assert!(matches!(
            control.prepare_outbound_open(
                outbound,
                DeliveryMode::ReliableOrdered,
                policy(DeliveryMode::ReliableOrdered, 128),
                nz(128),
                limits(8),
                None,
            ),
            Err(OutboundOpenError::PeerMessageLimit { .. })
        ));
        assert_eq!(endpoint.active_flows(), 0);
        assert_eq!(control.pending_outbound_len(), 0);

        let prepared = control
            .prepare_outbound_open(
                outbound,
                DeliveryMode::ReliableOrdered,
                reliable,
                nz(64),
                limits(8),
                None,
            )
            .unwrap();
        assert_eq!(prepared.flow.flow_id().sequence(), 0);
        assert_eq!(endpoint.active_flows(), 0);
        assert_eq!(endpoint.flow_contract(outbound), None);
    }

    #[test]
    fn sequenced_unreliable_open_uses_worst_case_sequence_envelope_before_consumption() {
        let connection = ConnectionHandle::new(2);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 256);
        let outbound = key(connection, FlowDirection::Outbound, 1);
        let p = policy(DeliveryMode::UnreliableSequenced, 64);

        assert!(matches!(
            control.prepare_outbound_open(
                outbound,
                DeliveryMode::UnreliableSequenced,
                p,
                nz(64),
                limits(8),
                Some(72),
            ),
            Err(OutboundOpenError::DatagramTooSmall {
                needed: 73,
                available: 72,
            })
        ));
        let prepared = control
            .prepare_outbound_open(
                outbound,
                DeliveryMode::UnreliableSequenced,
                p,
                nz(64),
                limits(8),
                Some(73),
            )
            .unwrap();
        assert_eq!(prepared.flow.flow_id().sequence(), 0);
    }

    #[test]
    fn peer_active_ceiling_bounds_pending_and_active_outbound_flows() {
        let connection = ConnectionHandle::new(3);
        let mut control = control(connection, WireSide::Client, 4, 256, 1, 256);
        let p = policy(DeliveryMode::ReliableOrdered, 64);
        let first = control
            .prepare_outbound_open(
                key(connection, FlowDirection::Outbound, 1),
                DeliveryMode::ReliableOrdered,
                p,
                nz(64),
                limits(8),
                None,
            )
            .unwrap();
        assert!(matches!(
            control.prepare_outbound_open(
                key(connection, FlowDirection::Outbound, 2),
                DeliveryMode::ReliableOrdered,
                p,
                nz(64),
                limits(8),
                None,
            ),
            Err(OutboundOpenError::PeerActiveFlowLimit { .. })
        ));

        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let reject = frame(
            ControlFrameType::FlowReject,
            FlowReject {
                flow_id: first.flow.flow_id(),
                reason: FlowRejectReason::ResourceLimit,
            }
            .encode(),
        );
        assert!(matches!(
            control.receive(&mut endpoint, reject).unwrap(),
            FlowControlProgress::OutboundRejected { .. }
        ));
        let second = control
            .prepare_outbound_open(
                key(connection, FlowDirection::Outbound, 2),
                DeliveryMode::ReliableOrdered,
                p,
                nz(64),
                limits(8),
                None,
            )
            .unwrap();
        assert_eq!(second.flow.flow_id().sequence(), 1);
    }

    #[test]
    fn outbound_flow_is_established_only_after_matching_accept() {
        let connection = ConnectionHandle::new(4);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let outbound = key(connection, FlowDirection::Outbound, 444);
        let prepared = control
            .prepare_outbound_open(
                outbound,
                DeliveryMode::ReliableOrdered,
                policy(DeliveryMode::ReliableOrdered, 64),
                nz(64),
                limits(8),
                None,
            )
            .unwrap();
        assert_eq!(endpoint.flow_contract(outbound), None);
        assert_eq!(control.registry().registered_flow(prepared.flow.flow_id()), None);

        let accept = frame(
            ControlFrameType::FlowAccept,
            FlowAccept {
                flow_id: prepared.flow.flow_id(),
            }
            .encode(),
        );
        let established = control.receive(&mut endpoint, accept).unwrap();
        assert_eq!(
            established,
            FlowControlProgress::OutboundEstablished(prepared.flow)
        );
        assert_eq!(
            endpoint.flow_contract(outbound),
            Some((DeliveryMode::ReliableOrdered, policy(DeliveryMode::ReliableOrdered, 64)))
        );
        assert_eq!(
            control.registry().registered_flow(prepared.flow.flow_id()).unwrap().key(),
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
                requested,
                DeliveryMode::ReliableOrdered,
                policy(DeliveryMode::ReliableOrdered, 64),
                nz(64),
                limits(1),
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
        let accept = frame(
            ControlFrameType::FlowAccept,
            FlowAccept {
                flow_id: prepared.flow.flow_id(),
            }
            .encode(),
        );
        let progress = control.receive(&mut endpoint, accept).unwrap();
        let FlowControlProgress::OutboundFailedAfterAccept {
            flow_id,
            key,
            reason,
            frame,
        } = progress
        else {
            panic!("expected resource termination after peer accept");
        };
        assert_eq!(flow_id, prepared.flow.flow_id());
        assert_eq!(key, requested);
        assert_eq!(reason, FlowTerminateReason::ResourceFailure);
        assert_eq!(frame.frame_type, ControlFrameType::FlowTerminate);
        assert_eq!(endpoint.flow_contract(requested), None);
        assert_eq!(control.registry().registered_flow(flow_id), None);
    }

    #[test]
    fn malformed_or_out_of_order_inbound_open_does_not_advance_peer_cursor() {
        let connection = ConnectionHandle::new(6);
        let mut control = control(connection, WireSide::Server, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));

        let malformed = ControlFrame {
            frame_type: ControlFrameType::OpenFlow,
            body: vec![0, 3, 1],
        };
        assert!(matches!(
            control.receive(&mut endpoint, malformed),
            Err(FlowControlError::Body(ControlBodyError::UnknownDeliveryMode(3)))
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
        assert_ne!(u64::from(flow.key().handle().get() == wire_flow.value()), 1);
        assert_eq!(flow.max_message_bytes(), 64);
        assert_eq!(frame.frame_type, ControlFrameType::FlowAccept);
        assert_eq!(endpoint.flow_contract(host_key).unwrap().1.max_message_bytes(), 128);
        assert_eq!(control.registry().registered_flow(wire_flow).unwrap().max_message_bytes(), 64);
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
    fn one_unresolved_inbound_request_bounds_host_admission_state() {
        let connection = ConnectionHandle::new(10);
        let mut control = control(connection, WireSide::Server, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let open = frame(
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
            control.receive(&mut endpoint, open).unwrap()
        else {
            panic!("expected request");
        };
        let accept = frame(
            ControlFrameType::FlowAccept,
            FlowAccept {
                flow_id: FlowId::new(WireSide::Server, 0).unwrap(),
            }
            .encode(),
        );
        assert!(matches!(
            control.receive(&mut endpoint, accept),
            Err(FlowControlError::InboundDecisionPending(_))
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

        let prepared = client
            .prepare_outbound_open(
                client_key,
                DeliveryMode::ReliableOrdered,
                policy(DeliveryMode::ReliableOrdered, 64),
                nz(64),
                limits(8),
                None,
            )
            .unwrap();
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
        let FlowControlProgress::OutboundEstablished(client_flow) = client
            .receive(&mut client_endpoint, accept)
            .unwrap()
        else {
            panic!("client must establish");
        };

        assert_eq!(client_flow.flow_id(), server_flow.flow_id());
        assert_eq!(client_flow.mode(), server_flow.mode());
        assert_eq!(client_flow.max_message_bytes(), server_flow.max_message_bytes());
        assert_ne!(client_flow.key().handle(), server_flow.key().handle());
        assert_eq!(client_endpoint.active_flows(), 1);
        assert_eq!(server_endpoint.active_flows(), 1);
    }

    #[test]
    fn remote_termination_retires_core_and_registry_and_datagram_cannot_recreate_flow() {
        let connection = ConnectionHandle::new(12);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let outbound = key(connection, FlowDirection::Outbound, 1);
        let prepared = control
            .prepare_outbound_open(
                outbound,
                DeliveryMode::UnreliableUnordered,
                policy(DeliveryMode::UnreliableUnordered, 64),
                nz(64),
                limits(8),
                Some(128),
            )
            .unwrap();
        let accept = frame(
            ControlFrameType::FlowAccept,
            FlowAccept {
                flow_id: prepared.flow.flow_id(),
            }
            .encode(),
        );
        control.receive(&mut endpoint, accept).unwrap();

        let terminate = frame(
            ControlFrameType::FlowTerminate,
            FlowTerminate {
                flow_id: prepared.flow.flow_id(),
                reason: FlowTerminateReason::ResourceFailure,
            }
            .encode(),
        );
        assert!(matches!(
            control.receive(&mut endpoint, terminate).unwrap(),
            FlowControlProgress::RemoteTerminated {
                reason: FlowTerminateReason::ResourceFailure,
                ..
            }
        ));
        assert_eq!(endpoint.flow_contract(outbound), None);
        assert_eq!(control.registry().registered_flow(prepared.flow.flow_id()), None);

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
    fn duplicate_unknown_and_wrong_side_control_responses_fail_closed() {
        let connection = ConnectionHandle::new(13);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let prepared = control
            .prepare_outbound_open(
                key(connection, FlowDirection::Outbound, 1),
                DeliveryMode::ReliableOrdered,
                policy(DeliveryMode::ReliableOrdered, 64),
                nz(64),
                limits(8),
                None,
            )
            .unwrap();
        let reject_body = FlowReject {
            flow_id: prepared.flow.flow_id(),
            reason: FlowRejectReason::ResourceLimit,
        }
        .encode();
        control
            .receive(
                &mut endpoint,
                frame(ControlFrameType::FlowReject, reject_body),
            )
            .unwrap();
        assert!(matches!(
            control.receive(
                &mut endpoint,
                frame(ControlFrameType::FlowReject, reject_body),
            ),
            Err(FlowControlError::UnknownPendingFlow(_))
        ));

        let wrong = frame(
            ControlFrameType::FlowAccept,
            FlowAccept {
                flow_id: FlowId::new(WireSide::Server, 0).unwrap(),
            }
            .encode(),
        );
        assert!(matches!(
            control.receive(&mut endpoint, wrong),
            Err(FlowControlError::WrongResponseSide { .. })
        ));
    }

    #[test]
    fn reliable_normal_control_termination_is_not_a_fin_substitute() {
        let connection = ConnectionHandle::new(14);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let prepared = control
            .prepare_outbound_open(
                key(connection, FlowDirection::Outbound, 1),
                DeliveryMode::ReliableOrdered,
                policy(DeliveryMode::ReliableOrdered, 64),
                nz(64),
                limits(8),
                None,
            )
            .unwrap();
        control
            .receive(
                &mut endpoint,
                frame(
                    ControlFrameType::FlowAccept,
                    FlowAccept {
                        flow_id: prepared.flow.flow_id(),
                    }
                    .encode(),
                ),
            )
            .unwrap();

        let normal = frame(
            ControlFrameType::FlowTerminate,
            FlowTerminate {
                flow_id: prepared.flow.flow_id(),
                reason: FlowTerminateReason::Normal,
            }
            .encode(),
        );
        assert!(matches!(
            control.receive(&mut endpoint, normal),
            Err(FlowControlError::ReliableNormalUsesFin(_))
        ));
        assert!(matches!(
            control.terminate_local(
                &mut endpoint,
                prepared.flow.flow_id(),
                FlowTerminateReason::Normal,
            ),
            Err(FlowControlError::ReliableNormalUsesFin(_))
        ));
        assert!(endpoint.flow_contract(prepared.flow.key()).is_some());
    }

    #[test]
    fn local_exceptional_termination_retires_active_mapping_once() {
        let connection = ConnectionHandle::new(15);
        let mut control = control(connection, WireSide::Client, 4, 256, 4, 256);
        let mut endpoint = DeliveryEndpoint::new(limits(8));
        let prepared = control
            .prepare_outbound_open(
                key(connection, FlowDirection::Outbound, 1),
                DeliveryMode::UnreliableUnordered,
                policy(DeliveryMode::UnreliableUnordered, 64),
                nz(64),
                limits(8),
                Some(128),
            )
            .unwrap();
        control
            .receive(
                &mut endpoint,
                frame(
                    ControlFrameType::FlowAccept,
                    FlowAccept {
                        flow_id: prepared.flow.flow_id(),
                    }
                    .encode(),
                ),
            )
            .unwrap();
        let terminated = control
            .terminate_local(
                &mut endpoint,
                prepared.flow.flow_id(),
                FlowTerminateReason::Normal,
            )
            .unwrap();
        assert_eq!(terminated.frame.frame_type, ControlFrameType::FlowTerminate);
        assert_eq!(endpoint.flow_contract(prepared.flow.key()), None);
        assert_eq!(control.registry().registered_flow(prepared.flow.flow_id()), None);
        assert!(matches!(
            control.terminate_local(
                &mut endpoint,
                prepared.flow.flow_id(),
                FlowTerminateReason::Normal,
            ),
            Err(FlowControlError::UnknownActiveFlow(_))
        ));
    }
}
