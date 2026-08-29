//! Advanced transport-realization boundary for Core delivery state.
//!
//! Ordinary applications do not need this module. Transport adapters use this contract to move
//! already-accepted outbound payloads into transport custody and to feed received transport
//! payloads back into the same [`super::DeliveryEndpoint`] authority.
//!
//! This module owns no delivery state. [`DeliveryTransportAdapter`] is implemented only by
//! [`super::DeliveryEndpoint`] so acceptance, ordering/sequencing, pressure accounting, exposure,
//! and termination remain owned by one Core endpoint.

use super::{
    DeliveryEndpoint, DeliveryFlowKey, DeliveryMode, DeliveryOperationError, FlowTermination,
    ReceiveOutcome,
};

mod sealed {
    pub trait Sealed {}

    impl Sealed for super::DeliveryEndpoint {}
}

/// Snapshot of one accepted Core delivery transfer exposed to a transport realization.
///
/// Cloning this value shares the accepted payload allocation; it does not transfer Core custody.
/// Custody changes only through [`DeliveryTransportAdapter::commit_outbound_custody`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveryTransfer(super::DeliveryTransfer);

impl DeliveryTransfer {
    pub const fn mode(&self) -> DeliveryMode {
        self.0.mode()
    }

    pub const fn accepted_index(&self) -> u64 {
        self.0.accepted_index()
    }

    pub fn payload(&self) -> &[u8] {
        self.0.payload()
    }

    pub fn payload_len(&self) -> usize {
        self.0.payload_len()
    }

    pub(super) fn into_inner(self) -> super::DeliveryTransfer {
        self.0
    }
}

/// Read-only metadata for the next accepted outbound transfer.
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

/// Failure while transferring an accepted outbound message out of Core custody.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CustodyCommitError {
    UnknownFlow,
    WrongDirection,
    NoPendingMessage,
    NotFront,
}

impl From<super::CustodyCommitError> for CustodyCommitError {
    fn from(error: super::CustodyCommitError) -> Self {
        match error {
            super::CustodyCommitError::UnknownFlow => Self::UnknownFlow,
            super::CustodyCommitError::WrongDirection => Self::WrongDirection,
            super::CustodyCommitError::NoPendingMessage => Self::NoPendingMessage,
            super::CustodyCommitError::NotFront => Self::NotFront,
        }
    }
}

/// Explicit advanced contract used by transport realizations of Core delivery semantics.
///
/// Importing this trait opts a consumer into transport custody/ingress mechanics. The contract is
/// sealed to [`DeliveryEndpoint`]: custom transports use the endpoint through this trait rather than
/// implementing another delivery-state owner. It deliberately does not define transport selection,
/// scheduling, wire identifiers, retries, or another delivery authority.
pub trait DeliveryTransportAdapter: sealed::Sealed {
    /// Inspect the sequence/index the next successful outbound submission would consume.
    ///
    /// This is read-only preflight evidence and does not reserve or accept a message.
    fn next_outbound_accepted_index(
        &self,
        key: DeliveryFlowKey,
    ) -> Result<Option<u64>, DeliveryOperationError>;

    /// Snapshot the accepted outbound transfer currently at the front of Core custody.
    fn peek_outbound(
        &self,
        key: DeliveryFlowKey,
    ) -> Result<Option<DeliveryTransfer>, DeliveryOperationError>;

    /// Inspect only the transport-relevant metadata of the current outbound transfer.
    fn peek_outbound_metadata(
        &self,
        key: DeliveryFlowKey,
    ) -> Result<Option<OutboundTransferMetadata>, DeliveryOperationError>;

    /// Commit the front accepted outbound transfer out of Core custody.
    fn commit_outbound_custody(
        &mut self,
        key: DeliveryFlowKey,
        accepted_index: u64,
    ) -> Result<DeliveryTransfer, CustodyCommitError>;

    /// Feed one transport-received payload into Core using the flow's established delivery mode.
    fn receive_transport_payload(
        &mut self,
        key: DeliveryFlowKey,
        accepted_index: u64,
        payload: Vec<u8>,
    ) -> Result<ReceiveOutcome, DeliveryOperationError>;

    /// Feed a previously snapshotted transport transfer into an inbound Core flow.
    fn receive_transfer(
        &mut self,
        key: DeliveryFlowKey,
        transfer: DeliveryTransfer,
    ) -> Result<ReceiveOutcome, DeliveryOperationError>;

    /// Report that accepted reliable custody can no longer satisfy its delivery guarantee.
    fn fail_reliable_custody(
        &mut self,
        key: DeliveryFlowKey,
    ) -> Result<FlowTermination, DeliveryOperationError>;
}

impl DeliveryTransportAdapter for DeliveryEndpoint {
    fn next_outbound_accepted_index(
        &self,
        key: DeliveryFlowKey,
    ) -> Result<Option<u64>, DeliveryOperationError> {
        DeliveryEndpoint::next_outbound_accepted_index(self, key)
    }

    fn peek_outbound(
        &self,
        key: DeliveryFlowKey,
    ) -> Result<Option<DeliveryTransfer>, DeliveryOperationError> {
        DeliveryEndpoint::peek_outbound(self, key).map(|transfer| transfer.map(DeliveryTransfer))
    }

    fn peek_outbound_metadata(
        &self,
        key: DeliveryFlowKey,
    ) -> Result<Option<OutboundTransferMetadata>, DeliveryOperationError> {
        DeliveryEndpoint::peek_outbound_metadata(self, key).map(|metadata| {
            metadata.map(|metadata| OutboundTransferMetadata {
                mode: metadata.mode(),
                accepted_index: metadata.accepted_index(),
                payload_len: metadata.payload_len(),
            })
        })
    }

    fn commit_outbound_custody(
        &mut self,
        key: DeliveryFlowKey,
        accepted_index: u64,
    ) -> Result<DeliveryTransfer, CustodyCommitError> {
        DeliveryEndpoint::commit_outbound_custody(self, key, accepted_index)
            .map(DeliveryTransfer)
            .map_err(CustodyCommitError::from)
    }

    fn receive_transport_payload(
        &mut self,
        key: DeliveryFlowKey,
        accepted_index: u64,
        payload: Vec<u8>,
    ) -> Result<ReceiveOutcome, DeliveryOperationError> {
        DeliveryEndpoint::receive_transport_payload(self, key, accepted_index, payload)
    }

    fn receive_transfer(
        &mut self,
        key: DeliveryFlowKey,
        transfer: DeliveryTransfer,
    ) -> Result<ReceiveOutcome, DeliveryOperationError> {
        DeliveryEndpoint::receive(self, key, transfer.into_inner())
    }

    fn fail_reliable_custody(
        &mut self,
        key: DeliveryFlowKey,
    ) -> Result<FlowTermination, DeliveryOperationError> {
        DeliveryEndpoint::fail_reliable_custody(self, key)
    }
}
