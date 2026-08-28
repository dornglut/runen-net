use std::{
    collections::TryReserveError,
    fmt,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};

use quinn::{Connection, ConnectionError, RecvStream, SendStream};
use runen_net::{
    delivery::{
        DeliveryEndpoint, DeliveryFlowKey, DeliveryMode, FlowDirection, FlowTermination,
    },
    protocol::NegotiationManager,
};

use crate::{
    flow_control::{EstablishedFlow, FlowControl},
    lifecycle::{
        ConnectionTeardown, EstablishedIoParts, FlowControlDriverParts, FlowControlledConnection,
    },
    quinn_binding::{
        InboundReliable, OutboundReliable, ReceiveError, ReceiveProgress, SendError, SendProgress,
    },
    wire::FlowId,
};

type OpenUniFuture =
    Pin<Box<dyn Future<Output = Result<SendStream, ConnectionError>> + Send + 'static>>;
type AcceptUniFuture =
    Pin<Box<dyn Future<Output = Result<RecvStream, ConnectionError>> + Send + 'static>>;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct ReliableReceiveLimits {
    scratch_bytes: NonZeroUsize,
    max_staging_bytes: NonZeroUsize,
}

struct PendingOutboundOpen {
    flow_id: FlowId,
    future: OpenUniFuture,
}

struct ActiveOutbound {
    flow_id: FlowId,
    binding: OutboundReliable<SendStream>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum ReliableIoStateError {
    NotReliable,
    WrongDirection,
    RegistryMismatch,
    DuplicateOutboundFlow,
    CapacityOverflow,
    InboundAcceptTerminated,
    OutboundAcquisitionPending,
    UnknownOutboundFlow,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum ReliableFailureContext {
    Unresolved,
    ResolvedReportable {
        flow_id: FlowId,
        key: DeliveryFlowKey,
        termination: Option<FlowTermination>,
    },
    ResolvedDetached {
        flow_id: FlowId,
        key: DeliveryFlowKey,
    },
}

#[derive(Debug)]
pub(super) enum ReliableIoError {
    State(ReliableIoStateError),
    Allocation(TryReserveError),
    Connection(ConnectionError),
    OutboundFinish {
        flow_id: FlowId,
        error: SendError,
    },
    OutboundAcquisitionBinding {
        flow_id: FlowId,
        error: SendError,
    },
    OutboundActiveBinding {
        context: ReliableFailureContext,
        error: SendError,
    },
    InboundConstruction(ReceiveError),
    InboundActiveBinding {
        context: ReliableFailureContext,
        error: ReceiveError,
    },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum OutboundAcquisitionProgress {
    Bound { flow_id: FlowId },
    Cancelled { flow_id: FlowId },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum ActiveReliableProgress {
    Outbound {
        flow_id: FlowId,
        progress: SendProgress,
    },
    Inbound(ReceiveProgress),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum OutboundFinishTarget {
    Pending,
    Active(usize),
    Unknown,
}

pub(super) struct ReliableConnectionIo {
    receive: ReliableReceiveLimits,
    accept_uni: Option<AcceptUniFuture>,
    pending_outbound: Vec<PendingOutboundOpen>,
    active_outbound: Vec<ActiveOutbound>,
    active_inbound: Vec<InboundReliable<RecvStream>>,
    outbound_open_cursor: usize,
    outbound_cursor: usize,
    inbound_cursor: usize,
}

impl fmt::Debug for ReliableConnectionIo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ReliableConnectionIo")
            .field("receive", &self.receive)
            .field("accept_uni_pending", &self.accept_uni.is_some())
            .field("pending_outbound", &self.pending_outbound.len())
            .field("active_outbound", &self.active_outbound.len())
            .field("active_inbound", &self.active_inbound.len())
            .finish_non_exhaustive()
    }
}

impl ReliableConnectionIo {
    fn new(
        connection: Connection,
        scratch_bytes: NonZeroUsize,
        max_staging_bytes: NonZeroUsize,
    ) -> Self {
        Self {
            receive: ReliableReceiveLimits {
                scratch_bytes,
                max_staging_bytes,
            },
            accept_uni: Some(accept_uni_owned(connection)),
            pending_outbound: Vec::new(),
            active_outbound: Vec::new(),
            active_inbound: Vec::new(),
            outbound_open_cursor: 0,
            outbound_cursor: 0,
            inbound_cursor: 0,
        }
    }

    pub(super) const fn max_staging_bytes(&self) -> NonZeroUsize {
        self.receive.max_staging_bytes
    }

    pub(super) fn outbound_flow_id(
        &self,
        flow_control: &FlowControl,
        key: DeliveryFlowKey,
    ) -> Option<FlowId> {
        self.pending_outbound
            .iter()
            .map(|pending| pending.flow_id)
            .chain(self.active_outbound.iter().map(|active| active.flow_id))
            .find(|flow_id| {
                flow_control
                    .registry()
                    .registered_flow(*flow_id)
                    .is_some_and(|registered| {
                        registered.key() == key
                            && registered.mode() == DeliveryMode::ReliableOrdered
                            && registered.key().direction() == FlowDirection::Outbound
                    })
            })
    }

    pub(super) fn request_outbound_finish_normal(
        &mut self,
        endpoint: &DeliveryEndpoint,
        flow_id: FlowId,
    ) -> Result<(), ReliableIoError> {
        let target = outbound_finish_target(
            flow_id,
            self.pending_outbound.iter().map(|pending| pending.flow_id),
            self.active_outbound.iter().map(|active| active.flow_id),
        );
        match target {
            OutboundFinishTarget::Pending => Err(ReliableIoError::State(
                ReliableIoStateError::OutboundAcquisitionPending,
            )),
            OutboundFinishTarget::Active(index) => self.active_outbound[index]
                .binding
                .request_finish_normal(endpoint)
                .map_err(|error| ReliableIoError::OutboundFinish { flow_id, error }),
            OutboundFinishTarget::Unknown => Err(ReliableIoError::State(
                ReliableIoStateError::UnknownOutboundFlow,
            )),
        }
    }

    pub(super) fn schedule_outbound(
        &mut self,
        connection: &Connection,
        flow_control: &FlowControl,
        flow: EstablishedFlow,
    ) -> Result<(), ReliableIoError> {
        if flow.mode() != DeliveryMode::ReliableOrdered {
            return Err(ReliableIoError::State(ReliableIoStateError::NotReliable));
        }
        if flow.key().direction() != FlowDirection::Outbound {
            return Err(ReliableIoError::State(ReliableIoStateError::WrongDirection));
        }

        let registered = flow_control
            .registry()
            .registered_flow(flow.flow_id())
            .ok_or(ReliableIoError::State(
                ReliableIoStateError::RegistryMismatch,
            ))?;
        if registered.key() != flow.key()
            || registered.mode() != flow.mode()
            || registered.max_message_bytes() != flow.max_message_bytes()
        {
            return Err(ReliableIoError::State(
                ReliableIoStateError::RegistryMismatch,
            ));
        }
        if contains_flow_id(
            flow.flow_id(),
            self.pending_outbound.iter().map(|pending| pending.flow_id),
            self.active_outbound.iter().map(|active| active.flow_id),
        ) {
            return Err(ReliableIoError::State(
                ReliableIoStateError::DuplicateOutboundFlow,
            ));
        }

        let target_active_capacity = self
            .active_outbound
            .len()
            .checked_add(self.pending_outbound.len())
            .and_then(|value| value.checked_add(1))
            .ok_or(ReliableIoError::State(
                ReliableIoStateError::CapacityOverflow,
            ))?;
        self.active_outbound
            .try_reserve(target_active_capacity - self.active_outbound.len())
            .map_err(ReliableIoError::Allocation)?;
        self.pending_outbound
            .try_reserve(1)
            .map_err(ReliableIoError::Allocation)?;
        self.pending_outbound.push(PendingOutboundOpen {
            flow_id: flow.flow_id(),
            future: open_uni_owned(connection.clone()),
        });
        debug_assert_eq!(
            self.outbound_flow_id(flow_control, flow.key()),
            Some(flow.flow_id())
        );
        Ok(())
    }

    pub(super) fn poll_outbound_acquisition(
        &mut self,
        cx: &mut Context<'_>,
        flow_control: &mut FlowControl,
    ) -> Poll<Result<OutboundAcquisitionProgress, ReliableIoError>> {
        let len = self.pending_outbound.len();
        if len == 0 {
            return Poll::Pending;
        }
        let start = self.outbound_open_cursor % len;
        for offset in 0..len {
            let index = (start + offset) % len;
            let flow_id = self.pending_outbound[index].flow_id;
            if !registered_outbound_is_live(flow_control, flow_id) {
                let _ = self.pending_outbound.swap_remove(index);
                self.outbound_open_cursor = cursor_after_remove(index, self.pending_outbound.len());
                return Poll::Ready(Ok(OutboundAcquisitionProgress::Cancelled { flow_id }));
            }

            match self.pending_outbound[index].future.as_mut().poll(cx) {
                Poll::Pending => {}
                Poll::Ready(Err(error)) => {
                    let _ = self.pending_outbound.swap_remove(index);
                    self.outbound_open_cursor =
                        cursor_after_remove(index, self.pending_outbound.len());
                    return Poll::Ready(Err(ReliableIoError::Connection(error)));
                }
                Poll::Ready(Ok(stream)) => {
                    let _ = self.pending_outbound.swap_remove(index);
                    self.outbound_open_cursor =
                        cursor_after_remove(index, self.pending_outbound.len());
                    let binding =
                        OutboundReliable::bind_quinn(flow_control.registry_mut(), flow_id, stream)
                            .map_err(|error| ReliableIoError::OutboundAcquisitionBinding {
                                flow_id,
                                error,
                            })?;
                    self.active_outbound
                        .push(ActiveOutbound { flow_id, binding });
                    return Poll::Ready(Ok(OutboundAcquisitionProgress::Bound { flow_id }));
                }
            }
        }
        self.outbound_open_cursor = (start + 1) % len;
        Poll::Pending
    }

    pub(super) fn poll_inbound_acquisition(
        &mut self,
        cx: &mut Context<'_>,
        connection: &Connection,
    ) -> Poll<Result<(), ReliableIoError>> {
        self.active_inbound
            .try_reserve(1)
            .map_err(ReliableIoError::Allocation)?;
        let polled = {
            let accept_uni = self.accept_uni.as_mut().ok_or(ReliableIoError::State(
                ReliableIoStateError::InboundAcceptTerminated,
            ))?;
            accept_uni.as_mut().poll(cx)
        };
        match polled {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => {
                self.accept_uni = None;
                Poll::Ready(Err(ReliableIoError::Connection(error)))
            }
            Poll::Ready(Ok(stream)) => {
                self.accept_uni = Some(accept_uni_owned(connection.clone()));
                let binding = InboundReliable::bind_quinn(
                    stream,
                    self.receive.scratch_bytes,
                    self.receive.max_staging_bytes,
                )
                .map_err(ReliableIoError::InboundConstruction)?;
                self.active_inbound.push(binding);
                Poll::Ready(Ok(()))
            }
        }
    }

    pub(super) fn poll_outbound_binding(
        &mut self,
        cx: &mut Context<'_>,
        endpoint: &mut DeliveryEndpoint,
        flow_control: &mut FlowControl,
    ) -> Poll<Result<ActiveReliableProgress, ReliableIoError>> {
        let len = self.active_outbound.len();
        if len == 0 {
            return Poll::Pending;
        }
        let start = self.outbound_cursor % len;
        for offset in 0..len {
            let index = (start + offset) % len;
            let flow_id = self.active_outbound[index].flow_id;
            let key = self.active_outbound[index].binding.flow_key();
            let registered_before = flow_control.registry().registered_flow(flow_id).is_some();
            match self.active_outbound[index].binding.poll_step(
                cx,
                endpoint,
                flow_control.registry_mut(),
            ) {
                Poll::Pending | Poll::Ready(Ok(SendProgress::Idle)) => {}
                Poll::Ready(Ok(progress @ SendProgress::Closed { .. })) => {
                    let _ = self.active_outbound.swap_remove(index);
                    self.outbound_cursor = cursor_after_remove(index, self.active_outbound.len());
                    return Poll::Ready(Ok(ActiveReliableProgress::Outbound { flow_id, progress }));
                }
                Poll::Ready(Ok(progress)) => {
                    self.outbound_cursor = (index + 1) % len;
                    return Poll::Ready(Ok(ActiveReliableProgress::Outbound { flow_id, progress }));
                }
                Poll::Ready(Err(error)) => {
                    let termination = self.active_outbound[index]
                        .binding
                        .take_failure_termination();
                    let context = resolved_failure_context(
                        flow_id,
                        key,
                        registered_before,
                        termination,
                    );
                    let _ = self.active_outbound.swap_remove(index);
                    self.outbound_cursor = cursor_after_remove(index, self.active_outbound.len());
                    return Poll::Ready(Err(ReliableIoError::OutboundActiveBinding {
                        context,
                        error,
                    }));
                }
            }
        }
        self.outbound_cursor = (start + 1) % len;
        Poll::Pending
    }

    pub(super) fn poll_inbound_binding(
        &mut self,
        cx: &mut Context<'_>,
        endpoint: &mut DeliveryEndpoint,
        flow_control: &mut FlowControl,
    ) -> Poll<Result<ActiveReliableProgress, ReliableIoError>> {
        let len = self.active_inbound.len();
        if len == 0 {
            return Poll::Pending;
        }
        let start = self.inbound_cursor % len;
        for offset in 0..len {
            let index = (start + offset) % len;
            let before_flow_id = self.active_inbound[index].resolved_flow_id();
            let before_key = self.active_inbound[index].resolved_flow_key();
            let before_registered = before_flow_id
                .is_some_and(|flow_id| flow_control.registry().registered_flow(flow_id).is_some());
            match self.active_inbound[index].poll_step(cx, endpoint, flow_control.registry_mut()) {
                Poll::Pending | Poll::Ready(Ok(ReceiveProgress::Draining { .. })) => {}
                Poll::Ready(Ok(progress @ ReceiveProgress::Closed { .. })) => {
                    let _ = self.active_inbound.swap_remove(index);
                    self.inbound_cursor = cursor_after_remove(index, self.active_inbound.len());
                    return Poll::Ready(Ok(ActiveReliableProgress::Inbound(progress)));
                }
                Poll::Ready(Ok(progress)) => {
                    self.inbound_cursor = (index + 1) % len;
                    return Poll::Ready(Ok(ActiveReliableProgress::Inbound(progress)));
                }
                Poll::Ready(Err(error)) => {
                    let after_flow_id = self.active_inbound[index].resolved_flow_id();
                    let after_key = self.active_inbound[index].resolved_flow_key();
                    let termination = self.active_inbound[index].take_failure_termination();
                    let context = inbound_failure_context(
                        before_flow_id,
                        before_key,
                        before_registered,
                        after_flow_id,
                        after_key,
                        termination,
                    );
                    let _ = self.active_inbound.swap_remove(index);
                    self.inbound_cursor = cursor_after_remove(index, self.active_inbound.len());
                    return Poll::Ready(Err(ReliableIoError::InboundActiveBinding {
                        context,
                        error,
                    }));
                }
            }
        }
        self.inbound_cursor = (start + 1) % len;
        Poll::Pending
    }
}

#[must_use = "reliable flow-controlled connection owns connection-local stream state"]
#[derive(Debug)]
pub(super) struct ReliableFlowControlledConnection {
    flow_controlled: FlowControlledConnection,
    reliable: ReliableConnectionIo,
}

#[must_use = "reliable established I/O ownership must be driven or synchronously torn down"]
#[derive(Debug)]
pub(super) struct ReliableEstablishedIoParts {
    pub(super) established: EstablishedIoParts,
    pub(super) reliable: ReliableConnectionIo,
}

#[must_use = "reliable driver parts borrow one connection-local reliable I/O owner"]
#[derive(Debug)]
pub(super) struct ReliableFlowDriverParts<'a> {
    pub(super) flow: FlowControlDriverParts<'a>,
    pub(super) reliable: &'a mut ReliableConnectionIo,
}

impl FlowControlledConnection {
    pub(super) fn into_reliable_io(
        mut self,
        scratch_bytes: NonZeroUsize,
        max_staging_bytes: NonZeroUsize,
    ) -> ReliableFlowControlledConnection {
        let connection = {
            let parts = self.driver_parts();
            parts.connection.clone()
        };
        ReliableFlowControlledConnection {
            flow_controlled: self,
            reliable: ReliableConnectionIo::new(connection, scratch_bytes, max_staging_bytes),
        }
    }
}

impl ReliableFlowControlledConnection {
    pub(super) fn driver_parts(&mut self) -> ReliableFlowDriverParts<'_> {
        ReliableFlowDriverParts {
            flow: self.flow_controlled.driver_parts(),
            reliable: &mut self.reliable,
        }
    }

    pub(super) fn into_established_io(self) -> ReliableEstablishedIoParts {
        let Self {
            flow_controlled,
            reliable,
        } = self;
        ReliableEstablishedIoParts {
            established: flow_controlled.into_established_io(),
            reliable,
        }
    }

    pub(super) fn teardown(
        self,
        manager: &mut NegotiationManager,
        delivery: &mut DeliveryEndpoint,
    ) -> ConnectionTeardown {
        let Self {
            flow_controlled,
            reliable: _reliable,
        } = self;
        flow_controlled.teardown(manager, delivery)
    }
}

fn open_uni_owned(connection: Connection) -> OpenUniFuture {
    Box::pin(async move { connection.open_uni().await })
}

fn accept_uni_owned(connection: Connection) -> AcceptUniFuture {
    Box::pin(async move { connection.accept_uni().await })
}

fn registered_outbound_is_live(flow_control: &FlowControl, flow_id: FlowId) -> bool {
    flow_control
        .registry()
        .registered_flow(flow_id)
        .is_some_and(|flow| {
            flow.mode() == DeliveryMode::ReliableOrdered
                && flow.key().direction() == FlowDirection::Outbound
        })
}

const fn resolved_failure_context(
    flow_id: FlowId,
    key: DeliveryFlowKey,
    registered_before: bool,
    termination: Option<FlowTermination>,
) -> ReliableFailureContext {
    if registered_before || termination.is_some() {
        ReliableFailureContext::ResolvedReportable {
            flow_id,
            key,
            termination,
        }
    } else {
        ReliableFailureContext::ResolvedDetached { flow_id, key }
    }
}

const fn inbound_failure_context(
    before_flow_id: Option<FlowId>,
    before_key: Option<DeliveryFlowKey>,
    before_registered: bool,
    after_flow_id: Option<FlowId>,
    after_key: Option<DeliveryFlowKey>,
    termination: Option<FlowTermination>,
) -> ReliableFailureContext {
    match (before_flow_id, before_key) {
        (Some(flow_id), Some(key)) => {
            resolved_failure_context(flow_id, key, before_registered, termination)
        }
        (None, None) => match (after_flow_id, after_key) {
            (Some(flow_id), Some(key)) => ReliableFailureContext::ResolvedReportable {
                flow_id,
                key,
                termination,
            },
            (None, None) => ReliableFailureContext::Unresolved,
            _ => ReliableFailureContext::Unresolved,
        },
        _ => ReliableFailureContext::Unresolved,
    }
}

fn contains_flow_id(
    flow_id: FlowId,
    pending: impl IntoIterator<Item = FlowId>,
    active: impl IntoIterator<Item = FlowId>,
) -> bool {
    pending
        .into_iter()
        .chain(active)
        .any(|item| item == flow_id)
}

fn outbound_finish_target(
    flow_id: FlowId,
    pending: impl IntoIterator<Item = FlowId>,
    active: impl IntoIterator<Item = FlowId>,
) -> OutboundFinishTarget {
    if pending.into_iter().any(|item| item == flow_id) {
        return OutboundFinishTarget::Pending;
    }
    active
        .into_iter()
        .position(|item| item == flow_id)
        .map_or(OutboundFinishTarget::Unknown, OutboundFinishTarget::Active)
}

const fn cursor_after_remove(index: usize, new_len: usize) -> usize {
    if new_len == 0 { 0 } else { index % new_len }
}

#[cfg(test)]
mod tests {
    use runen_net::{
        delivery::{DeliveryFlowHandle, FlowTerminationReason},
        identity::ConnectionHandle,
    };

    use super::*;
    use crate::wire::WireSide;

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    fn flow(side: WireSide, sequence: u64) -> FlowId {
        FlowId::new(side, sequence).unwrap()
    }

    fn key(direction: FlowDirection, handle: u64) -> DeliveryFlowKey {
        DeliveryFlowKey::new(
            ConnectionHandle::new(1),
            direction,
            DeliveryFlowHandle::new(handle),
        )
    }

    fn failure_termination(key: DeliveryFlowKey) -> FlowTermination {
        FlowTermination {
            key,
            reason: FlowTerminationReason::ReliableCustodyLost,
            pending_messages: 0,
            reliable_obligation_failed: true,
        }
    }

    fn assert_owned_send<T: Send + 'static>() {}
    fn assert_static<T: 'static>() {}

    #[test]
    fn acquisition_future_types_are_owned_and_send() {
        assert_owned_send::<OpenUniFuture>();
        assert_owned_send::<AcceptUniFuture>();
    }

    #[test]
    fn established_handoff_is_move_owned() {
        assert_static::<ReliableEstablishedIoParts>();
    }

    #[test]
    fn receive_limits_preserve_exact_staging_authority() {
        let limits = ReliableReceiveLimits {
            scratch_bytes: nz(8),
            max_staging_bytes: nz(64),
        };
        assert_eq!(limits.scratch_bytes, nz(8));
        assert_eq!(limits.max_staging_bytes, nz(64));
    }

    #[test]
    fn reliable_failure_context_preserves_key_evidence_and_reportability() {
        let flow_id = flow(WireSide::Server, 7);
        let key = key(FlowDirection::Inbound, 7);
        let termination = failure_termination(key);

        assert_eq!(
            inbound_failure_context(None, None, false, None, None, None),
            ReliableFailureContext::Unresolved
        );
        assert_eq!(
            inbound_failure_context(None, None, false, Some(flow_id), Some(key), Some(termination)),
            ReliableFailureContext::ResolvedReportable {
                flow_id,
                key,
                termination: Some(termination),
            }
        );
        assert_eq!(
            inbound_failure_context(
                Some(flow_id),
                Some(key),
                true,
                Some(flow_id),
                Some(key),
                Some(termination),
            ),
            ReliableFailureContext::ResolvedReportable {
                flow_id,
                key,
                termination: Some(termination),
            }
        );
        assert_eq!(
            inbound_failure_context(
                Some(flow_id),
                Some(key),
                false,
                Some(flow_id),
                Some(key),
                None,
            ),
            ReliableFailureContext::ResolvedDetached { flow_id, key }
        );
        assert_eq!(
            resolved_failure_context(flow_id, key, true, None),
            ReliableFailureContext::ResolvedReportable {
                flow_id,
                key,
                termination: None,
            }
        );
        assert_eq!(
            resolved_failure_context(flow_id, key, false, Some(termination)),
            ReliableFailureContext::ResolvedReportable {
                flow_id,
                key,
                termination: Some(termination),
            }
        );
        assert_eq!(
            resolved_failure_context(flow_id, key, false, None),
            ReliableFailureContext::ResolvedDetached { flow_id, key }
        );
    }

    #[test]
    fn duplicate_tracking_uses_transport_lists_not_a_second_registry() {
        let first = flow(WireSide::Client, 0);
        let second = flow(WireSide::Client, 1);
        let third = flow(WireSide::Client, 2);

        assert!(contains_flow_id(first, [first], [second]));
        assert!(contains_flow_id(second, [first], [second]));
        assert!(!contains_flow_id(third, [first], [second]));
    }

    #[test]
    fn normal_finish_target_is_explicitly_pending_active_or_unknown() {
        let pending = flow(WireSide::Client, 0);
        let active = flow(WireSide::Client, 1);
        let unknown = flow(WireSide::Client, 2);

        assert_eq!(
            outbound_finish_target(pending, [pending], [active]),
            OutboundFinishTarget::Pending
        );
        assert_eq!(
            outbound_finish_target(active, [pending], [active]),
            OutboundFinishTarget::Active(0)
        );
        assert_eq!(
            outbound_finish_target(unknown, [pending], [active]),
            OutboundFinishTarget::Unknown
        );
    }

    #[test]
    fn cursor_removal_stays_in_bounds() {
        assert_eq!(cursor_after_remove(0, 0), 0);
        assert_eq!(cursor_after_remove(0, 1), 0);
        assert_eq!(cursor_after_remove(2, 2), 0);
        assert_eq!(cursor_after_remove(1, 2), 1);
    }
}
