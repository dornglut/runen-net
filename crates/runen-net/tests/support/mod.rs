use std::collections::VecDeque;

use runen_net::delivery::{
    DeliveryEndpoint, DeliveryFlowKey, DeliveryTransfer, ReceiveOutcome,
};

#[derive(Debug)]
struct Staged {
    source: DeliveryFlowKey,
    target: DeliveryFlowKey,
    transfer: DeliveryTransfer,
}

/// Bounded deterministic delivery stage used only by repository conformance tests.
///
/// The stage acts below RunenNet delivery semantics: it can reorder, duplicate,
/// drop, or delay transfers already handed to it, but it never selects or
/// changes a delivery mode. Capacity is explicit in both message count and
/// payload bytes.
#[derive(Debug)]
pub(crate) struct FaultStage {
    max_messages: usize,
    max_bytes: usize,
    bytes: usize,
    queue: VecDeque<Staged>,
}

impl FaultStage {
    pub(crate) fn new(max_messages: usize, max_bytes: usize) -> Self {
        Self {
            max_messages,
            max_bytes,
            bytes: 0,
            queue: VecDeque::new(),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.queue.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub(crate) const fn payload_bytes(&self) -> usize {
        self.bytes
    }

    fn has_capacity(&self, bytes: usize) -> bool {
        self.queue.len() < self.max_messages
            && self
                .bytes
                .checked_add(bytes)
                .is_some_and(|total| total <= self.max_bytes)
    }

    /// Transfers outbound custody only when this bounded stage can retain it.
    pub(crate) fn take(
        &mut self,
        source: &mut DeliveryEndpoint,
        source_flow: DeliveryFlowKey,
        target_flow: DeliveryFlowKey,
    ) -> bool {
        let Some(preview) = source.peek_outbound(source_flow).unwrap() else {
            return false;
        };
        if !self.has_capacity(preview.payload_len()) {
            return false;
        }

        let transfer = source
            .commit_outbound_custody(source_flow, preview.accepted_index())
            .unwrap();
        self.bytes += transfer.payload_len();
        self.queue.push_back(Staged {
            source: source_flow,
            target: target_flow,
            transfer,
        });
        true
    }

    pub(crate) fn duplicate(&mut self, index: usize) -> bool {
        let Some(staged) = self.queue.get(index) else {
            return false;
        };
        if !self.has_capacity(staged.transfer.payload_len()) {
            return false;
        }

        let copy = Staged {
            source: staged.source,
            target: staged.target,
            transfer: staged.transfer.clone(),
        };
        self.bytes += copy.transfer.payload_len();
        self.queue.push_back(copy);
        true
    }

    pub(crate) fn swap(&mut self, first: usize, second: usize) {
        self.queue.swap(first, second);
    }

    /// Removes one staged transfer without exposing it, deterministically
    /// realizing transport/network loss below the selected delivery mode.
    pub(crate) fn drop_at(&mut self, index: usize) -> DeliveryTransfer {
        self.remove(index).transfer
    }

    pub(crate) fn deliver(
        &mut self,
        index: usize,
        target: &mut DeliveryEndpoint,
    ) -> ReceiveOutcome {
        let staged = self.remove(index);
        target.receive(staged.target, staged.transfer).unwrap()
    }

    fn remove(&mut self, index: usize) -> Staged {
        let staged = self.queue.remove(index).unwrap();
        self.bytes -= staged.transfer.payload_len();
        staged
    }
}
