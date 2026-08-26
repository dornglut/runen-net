use std::{
    collections::TryReserveError,
    fmt,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    task::{Context, Poll},
};

use quinn::{Connection, ConnectionError, RecvStream, SendStream};
use runen_net::delivery::{DeliveryEndpoint, DeliveryMode, FlowDirection};

use crate::{
    flow_control::{EstablishedFlow, FlowControl},
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

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct ReliableIoCapacity {
    max_outbound: NonZeroUsize,
    max_inbound: NonZeroUsize,
}

impl ReliableIoCapacity {
    const fn outbound_at_capacity(self, pending: usize, active: usize) -> bool {
        match pending.checked_add(active) {
            Some(total) => total >= self.max_outbound.get(),
            None => true,
        }
    }

    const fn inbound_at_capacity(self, active: usize) -> bool {
        active >= self.max_inbound.get()
    }
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
    OutboundCapacityExceeded,
    InboundAcceptTerminated,
}

#[derive(Debug)]
pub(super) enum ReliableIoError {
    State(ReliableIoStateError),
    Allocation(TryReserveError),
    Connection(ConnectionError),
    OutboundBinding { flow_id: FlowId, error: SendError },
    InboundBinding(ReceiveError),
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

pub(super) struct ReliableConnectionIo {
    receive: ReliableReceiveLimits,
    capacity: ReliableIoCapacity,
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
            .field("capacity", &self.capacity)
            .field("accept_uni_pending", &self.accept_uni.is_some())
            .field("pending_outbound", &self.pending_outbound.len())
            .field("active_outbound", &self.active_outbound.len())
            .field("active_inbound", &self.active_inbound.len())
            .finish_non_exhaustive()
    }
}

impl ReliableConnectionIo {
    pub(super) fn new(
        connection: &Connection,
        scratch_bytes: NonZeroUsize,
        max_staging_bytes: NonZeroUsize,
        max_outbound: NonZeroUsize,
        max_inbound: NonZeroUsize,
    ) -> Self {
        Self {
            receive: ReliableReceiveLimits {
                scratch_bytes,
                max_staging_bytes,
            },
            capacity: ReliableIoCapacity {
                max_outbound,
                max_inbound,
            },
            accept_uni: Some(accept_uni_owned(connection.clone())),
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
            return Err(ReliableIoError::State(
                ReliableIoStateError::WrongDirection,
            ));
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
        if self
            .capacity
            .outbound_at_capacity(self.pending_outbound.len(), self.active_outbound.len())
        {
            return Err(ReliableIoError::State(
                ReliableIoStateError::OutboundCapacityExceeded,
            ));
        }

        let target_active_capacity = self
            .active_outbound
            .len()
            .checked_add(self.pending_outbound.len())
            .and_then(|value| value.checked_add(1))
            .ok_or(ReliableIoError::State(
                ReliableIoStateError::OutboundCapacityExceeded,
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

            let polled = self.pending_outbound[index].future.as_mut().poll(cx);
            match polled {
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
                    let binding = OutboundReliable::bind_quinn(
                        flow_control.registry_mut(),
                        flow_id,
                        stream,
                    )
                    .map_err(|error| ReliableIoError::OutboundBinding { flow_id, error })?;
                    self.active_outbound.push(ActiveOutbound { flow_id, binding });
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
        if self.capacity.inbound_at_capacity(self.active_inbound.len()) {
            return Poll::Pending;
        }
        self.active_inbound
            .try_reserve(1)
            .map_err(ReliableIoError::Allocation)?;
        let accept_uni = self.accept_uni.as_mut().ok_or(ReliableIoError::State(
            ReliableIoStateError::InboundAcceptTerminated,
        ))?;
        match accept_uni.as_mut().poll(cx) {
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
                .map_err(ReliableIoError::InboundBinding)?;
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
            let polled = self.active_outbound[index].binding.poll_step(
                cx,
                endpoint,
                flow_control.registry_mut(),
            );
            match polled {
                Poll::Pending | Poll::Ready(Ok(SendProgress::Idle)) => {}
                Poll::Ready(Ok(progress @ SendProgress::Closed)) => {
                    let _ = self.active_outbound.swap_remove(index);
                    self.outbound_cursor = cursor_after_remove(index, self.active_outbound.len());
                    return Poll::Ready(Ok(ActiveReliableProgress::Outbound {
                        flow_id,
                        progress,
                    }));
                }
                Poll::Ready(Ok(progress)) => {
                    self.outbound_cursor = (index + 1) % len;
                    return Poll::Ready(Ok(ActiveReliableProgress::Outbound {
                        flow_id,
                        progress,
                    }));
                }
                Poll::Ready(Err(error)) => {
                    let _ = self.active_outbound.swap_remove(index);
                    self.outbound_cursor = cursor_after_remove(index, self.active_outbound.len());
                    return Poll::Ready(Err(ReliableIoError::OutboundBinding {
                        flow_id,
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
            let polled = self.active_inbound[index].poll_step(
                cx,
                endpoint,
                flow_control.registry_mut(),
            );
            match polled {
                Poll::Pending | Poll::Ready(Ok(ReceiveProgress::Draining)) => {}
                Poll::Ready(Ok(progress @ ReceiveProgress::Closed)) => {
                    let _ = self.active_inbound.swap_remove(index);
                    self.inbound_cursor = cursor_after_remove(index, self.active_inbound.len());
                    return Poll::Ready(Ok(ActiveReliableProgress::Inbound(progress)));
                }
                Poll::Ready(Ok(progress)) => {
                    self.inbound_cursor = (index + 1) % len;
                    return Poll::Ready(Ok(ActiveReliableProgress::Inbound(progress)));
                }
                Poll::Ready(Err(error)) => {
                    let _ = self.active_inbound.swap_remove(index);
                    self.inbound_cursor = cursor_after_remove(index, self.active_inbound.len());
                    return Poll::Ready(Err(ReliableIoError::InboundBinding(error)));
                }
            }
        }
        self.inbound_cursor = (start + 1) % len;
        Poll::Pending
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

fn contains_flow_id(
    flow_id: FlowId,
    pending: impl IntoIterator<Item = FlowId>,
    active: impl IntoIterator<Item = FlowId>,
) -> bool {
    pending.into_iter().chain(active).any(|item| item == flow_id)
}

const fn cursor_after_remove(index: usize, new_len: usize) -> usize {
    if new_len == 0 { 0 } else { index % new_len }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::WireSide;

    fn nz(value: usize) -> NonZeroUsize {
        NonZeroUsize::new(value).unwrap()
    }

    fn flow(side: WireSide, sequence: u64) -> FlowId {
        FlowId::new(side, sequence).unwrap()
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
    fn capacity_is_directional_and_finite() {
        let capacity = ReliableIoCapacity {
            max_outbound: nz(2),
            max_inbound: nz(1),
        };
        assert!(!capacity.outbound_at_capacity(0, 0));
        assert!(!capacity.outbound_at_capacity(1, 0));
        assert!(capacity.outbound_at_capacity(1, 1));
        assert!(!capacity.inbound_at_capacity(0));
        assert!(capacity.inbound_at_capacity(1));
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
    fn cursor_removal_stays_in_bounds() {
        assert_eq!(cursor_after_remove(0, 0), 0);
        assert_eq!(cursor_after_remove(0, 1), 0);
        assert_eq!(cursor_after_remove(2, 2), 0);
        assert_eq!(cursor_after_remove(1, 2), 1);
    }
}
