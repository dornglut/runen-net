use std::{
    collections::TryReserveError,
    fmt,
    task::{Context, Poll},
};

use quinn::{Connection, ConnectionError};
use runen_net::delivery::{DeliveryEndpoint, DeliveryFlowKey, DeliveryMode, FlowDirection};

use crate::{
    datagram::{
        DatagramReceiveFailure, DatagramReceiveOutcome, DatagramSendError, DatagramSendProgress,
        DatagramSender, DatagramSubmissionError, DatagramSubmissionOutcome,
        OwnedDatagramReadFuture, read_quinn_datagram_owned, receive_datagram,
    },
    flow_control::{EstablishedFlow, FlowControl},
    wire::FlowId,
};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum DatagramIoStateError {
    NotUnreliable,
    WrongDirection(FlowDirection),
    RegistryMismatch,
    DuplicateOutboundFlow,
    ReadTerminated,
}

#[derive(Debug)]
pub(super) enum DatagramIoError {
    State(DatagramIoStateError),
    Allocation(TryReserveError),
    Connection(ConnectionError),
    Receive(DatagramReceiveFailure),
    Send {
        flow_id: FlowId,
        error: DatagramSendError,
    },
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(super) enum DatagramOutboundProgress {
    Cancelled {
        flow_id: FlowId,
    },
    Driven {
        flow_id: FlowId,
        progress: DatagramSendProgress,
    },
}

pub(super) struct DatagramConnectionIo {
    sender: DatagramSender<Connection>,
    receive: Option<OwnedDatagramReadFuture>,
    outbound: Vec<FlowId>,
    outbound_cursor: usize,
}

impl fmt::Debug for DatagramConnectionIo {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DatagramConnectionIo")
            .field("receive_pending", &self.receive.is_some())
            .field("outbound", &self.outbound.len())
            .field(
                "outbound_transport_drops",
                &self.sender.outbound_transport_drops(),
            )
            .finish_non_exhaustive()
    }
}

impl DatagramConnectionIo {
    pub(super) fn new(connection: Connection) -> Self {
        Self {
            sender: DatagramSender::new_quinn(connection.clone()),
            receive: Some(read_quinn_datagram_owned(connection)),
            outbound: Vec::new(),
            outbound_cursor: 0,
        }
    }

    pub(super) const fn outbound_transport_drops(&self) -> usize {
        self.sender.outbound_transport_drops()
    }

    pub(super) fn outbound_flow_id(
        &self,
        flow_control: &FlowControl,
        key: DeliveryFlowKey,
    ) -> Option<FlowId> {
        self.outbound.iter().copied().find(|flow_id| {
            flow_control
                .registry()
                .registered_flow(*flow_id)
                .is_some_and(|registered| {
                    registered.key() == key
                        && is_outbound_unreliable(
                            registered.key().direction(),
                            registered.mode(),
                        )
                })
        })
    }

    pub(super) fn submit(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        flow_control: &FlowControl,
        flow_id: FlowId,
        payload: Vec<u8>,
    ) -> Result<DatagramSubmissionOutcome, DatagramSubmissionError> {
        self.sender
            .submit(endpoint, flow_control.registry(), flow_id, payload)
    }

    pub(super) fn register_outbound(
        &mut self,
        flow_control: &FlowControl,
        flow: EstablishedFlow,
    ) -> Result<(), DatagramIoError> {
        retain_live_outbound(&mut self.outbound, &mut self.outbound_cursor, |flow_id| {
            registered_outbound_unreliable_is_live(flow_control, flow_id)
        });

        let registered =
            flow_control
                .registry()
                .registered_flow(flow.flow_id())
                .map(|registered| {
                    (
                        registered.key(),
                        registered.mode(),
                        registered.max_message_bytes(),
                    )
                });
        validate_outbound_registration(
            flow.key(),
            flow.mode(),
            flow.max_message_bytes(),
            registered,
            self.outbound.contains(&flow.flow_id()),
        )
        .map_err(DatagramIoError::State)?;

        self.outbound
            .try_reserve(1)
            .map_err(DatagramIoError::Allocation)?;
        self.outbound.push(flow.flow_id());
        debug_assert_eq!(self.outbound_flow_id(flow_control, flow.key()), Some(flow.flow_id()));
        Ok(())
    }

    pub(super) fn drive_outbound(
        &mut self,
        endpoint: &mut DeliveryEndpoint,
        flow_control: &FlowControl,
    ) -> Result<Option<DatagramOutboundProgress>, DatagramIoError> {
        let len = self.outbound.len();
        if len == 0 {
            return Ok(None);
        }

        let start = self.outbound_cursor % len;
        for offset in 0..len {
            let index = (start + offset) % len;
            let flow_id = self.outbound[index];
            if !registered_outbound_unreliable_is_live(flow_control, flow_id) {
                let _ = self.outbound.swap_remove(index);
                self.outbound_cursor = normalize_cursor(index, self.outbound.len());
                return Ok(Some(DatagramOutboundProgress::Cancelled { flow_id }));
            }

            match self
                .sender
                .drive_one(endpoint, flow_control.registry(), flow_id)
            {
                Ok(DatagramSendProgress::Idle) => {}
                Ok(progress) => {
                    self.outbound_cursor = (index + 1) % len;
                    return Ok(Some(DatagramOutboundProgress::Driven { flow_id, progress }));
                }
                Err(error) => {
                    self.outbound_cursor = (index + 1) % len;
                    return Err(DatagramIoError::Send { flow_id, error });
                }
            }
        }

        self.outbound_cursor = (start + 1) % len;
        Ok(None)
    }

    pub(super) fn poll_inbound(
        &mut self,
        cx: &mut Context<'_>,
        connection: &Connection,
        endpoint: &mut DeliveryEndpoint,
        flow_control: &FlowControl,
    ) -> Poll<Result<DatagramReceiveOutcome, DatagramIoError>> {
        let polled = {
            let receive = self
                .receive
                .as_mut()
                .ok_or(DatagramIoError::State(DatagramIoStateError::ReadTerminated))?;
            receive.as_mut().poll(cx)
        };

        match polled {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => {
                self.receive = None;
                Poll::Ready(Err(DatagramIoError::Connection(error)))
            }
            Poll::Ready(Ok(datagram)) => {
                self.receive = Some(read_quinn_datagram_owned(connection.clone()));
                Poll::Ready(
                    receive_datagram(endpoint, flow_control.registry(), datagram.as_ref())
                        .map_err(DatagramIoError::Receive),
                )
            }
        }
    }
}

fn validate_outbound_registration(
    key: DeliveryFlowKey,
    mode: DeliveryMode,
    max_message_bytes: usize,
    registered: Option<(DeliveryFlowKey, DeliveryMode, usize)>,
    duplicate: bool,
) -> Result<(), DatagramIoStateError> {
    if mode == DeliveryMode::ReliableOrdered {
        return Err(DatagramIoStateError::NotUnreliable);
    }
    if key.direction() != FlowDirection::Outbound {
        return Err(DatagramIoStateError::WrongDirection(key.direction()));
    }
    if registered != Some((key, mode, max_message_bytes)) {
        return Err(DatagramIoStateError::RegistryMismatch);
    }
    if duplicate {
        return Err(DatagramIoStateError::DuplicateOutboundFlow);
    }
    Ok(())
}

fn registered_outbound_unreliable_is_live(flow_control: &FlowControl, flow_id: FlowId) -> bool {
    flow_control
        .registry()
        .registered_flow(flow_id)
        .is_some_and(|registered| {
            is_outbound_unreliable(registered.key().direction(), registered.mode())
        })
}

fn is_outbound_unreliable(direction: FlowDirection, mode: DeliveryMode) -> bool {
    direction == FlowDirection::Outbound && mode != DeliveryMode::ReliableOrdered
}

fn retain_live_outbound(
    outbound: &mut Vec<FlowId>,
    cursor: &mut usize,
    mut is_live: impl FnMut(FlowId) -> bool,
) {
    outbound.retain(|flow_id| is_live(*flow_id));
    *cursor = normalize_cursor(*cursor, outbound.len());
}

const fn normalize_cursor(cursor: usize, len: usize) -> usize {
    if len == 0 { 0 } else { cursor % len }
}

#[cfg(test)]
mod tests {
    use runen_net::{delivery::DeliveryFlowHandle, identity::ConnectionHandle};

    use crate::wire::WireSide;

    use super::*;

    fn key(direction: FlowDirection, handle: u64) -> DeliveryFlowKey {
        DeliveryFlowKey::new(
            ConnectionHandle::new(1),
            direction,
            DeliveryFlowHandle::new(handle),
        )
    }

    fn flow(sequence: u64) -> FlowId {
        FlowId::new(WireSide::Client, sequence).unwrap()
    }

    fn assert_owned_send<T: Send + 'static>() {}

    #[test]
    fn connection_io_is_owned_and_send() {
        assert_owned_send::<DatagramConnectionIo>();
        assert_owned_send::<OwnedDatagramReadFuture>();
    }

    #[test]
    fn registration_requires_exact_outbound_unreliable_registry_contract() {
        let outbound = key(FlowDirection::Outbound, 1);
        let inbound = key(FlowDirection::Inbound, 2);
        let exact = Some((outbound, DeliveryMode::UnreliableSequenced, 64));

        assert_eq!(
            validate_outbound_registration(
                outbound,
                DeliveryMode::UnreliableSequenced,
                64,
                exact,
                false,
            ),
            Ok(())
        );
        assert_eq!(
            validate_outbound_registration(
                outbound,
                DeliveryMode::ReliableOrdered,
                64,
                Some((outbound, DeliveryMode::ReliableOrdered, 64)),
                false,
            ),
            Err(DatagramIoStateError::NotUnreliable)
        );
        assert_eq!(
            validate_outbound_registration(
                inbound,
                DeliveryMode::UnreliableUnordered,
                64,
                Some((inbound, DeliveryMode::UnreliableUnordered, 64)),
                false,
            ),
            Err(DatagramIoStateError::WrongDirection(FlowDirection::Inbound))
        );
        assert_eq!(
            validate_outbound_registration(
                outbound,
                DeliveryMode::UnreliableSequenced,
                64,
                Some((outbound, DeliveryMode::UnreliableSequenced, 63)),
                false,
            ),
            Err(DatagramIoStateError::RegistryMismatch)
        );
        assert_eq!(
            validate_outbound_registration(
                outbound,
                DeliveryMode::UnreliableSequenced,
                64,
                exact,
                true,
            ),
            Err(DatagramIoStateError::DuplicateOutboundFlow)
        );
    }

    #[test]
    fn transport_liveness_requires_outbound_unreliable_contract() {
        assert!(is_outbound_unreliable(
            FlowDirection::Outbound,
            DeliveryMode::UnreliableUnordered,
        ));
        assert!(is_outbound_unreliable(
            FlowDirection::Outbound,
            DeliveryMode::UnreliableSequenced,
        ));
        assert!(!is_outbound_unreliable(
            FlowDirection::Outbound,
            DeliveryMode::ReliableOrdered,
        ));
        assert!(!is_outbound_unreliable(
            FlowDirection::Inbound,
            DeliveryMode::UnreliableUnordered,
        ));
    }

    #[test]
    fn stale_compaction_bounds_tracking_and_normalizes_cursor() {
        let first = flow(0);
        let second = flow(1);
        let third = flow(2);
        let mut outbound = vec![first, second, third];
        let mut cursor = 5;

        retain_live_outbound(&mut outbound, &mut cursor, |flow_id| flow_id == second);

        assert_eq!(outbound, vec![second]);
        assert_eq!(cursor, 0);
    }

    #[test]
    fn cursor_normalization_stays_in_bounds() {
        assert_eq!(normalize_cursor(0, 0), 0);
        assert_eq!(normalize_cursor(0, 1), 0);
        assert_eq!(normalize_cursor(2, 2), 0);
        assert_eq!(normalize_cursor(1, 2), 1);
    }
}
