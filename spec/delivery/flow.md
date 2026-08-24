# Delivery Flow Semantics

Status: **provisional incomplete normative**

This document owns the initial RunenNet delivery-flow lifetime, message submission/acceptance/exposure model, and delivery modes. Identity vocabulary is defined by [Core identity and time](../core/identity.md). Session membership and connection binding are defined by [Session and authority lifecycle](../session/lifecycle.md).

Resource limits and pressure outcomes are owned by [Delivery pressure and resource policy](pressure.md).

## Scope

This revision defines:

- unidirectional delivery flows;
- flow lifetime relative to one current participant connection binding;
- message submission, acceptance, rejection, and receiver exposure;
- `ReliableOrdered`, `UnreliableUnordered`, and `UnreliableSequenced` delivery modes;
- terminal flow failure when an accepted reliable guarantee can no longer be upheld;
- transport-realization requirements that prevent silent semantic downgrade.

Wire flow identifiers, serialization, packetization, fragmentation algorithms, bandwidth priority, deadlines, keyed supersession, replication policy, input buffering, and reconnect recovery are not defined by this revision.

## Message unit

For this delivery layer, a **message** is one finite opaque byte payload submitted as a single semantic unit.

A receiver either exposes a complete message payload or does not expose that message. Transport fragmentation, stream chunking, packet boundaries, and reassembly are realization details and MUST NOT become partial message exposure.

The maximum accepted message size is governed by the delivery pressure specification and by any stricter capability negotiated for the selected realization.

## Delivery flow

A **delivery flow** is one unidirectional semantic message-delivery domain within one SessionId and one current authorized participant connection binding.

In the initial client/server model, a flow direction is either:

- authority to one Bound participant; or
- one Bound participant to the authority.

A delivery flow is not a transport connection, QUIC stream, QUIC DATAGRAM context, socket, executor task, or engine queue. A realization MAY map one delivery flow onto one or more transport-native mechanisms only if the resulting behavior conforms to the flow's selected delivery mode.

Different delivery flows are independent ordering and sequencing domains. Ordering or sequencing in one flow does not constrain exposure in another flow.

The concrete representation used to distinguish delivery flows is not defined by this revision.

## Flow establishment and lifetime

A delivery flow exists only while its participant has the current authorized connection binding required by the session lifecycle specification.

The flow's direction and delivery mode are fixed for that flow lifetime. They MUST be selected before any message on the flow is accepted for delivery.

When that connection binding ends, every delivery flow scoped to that binding terminates. An authorized replacement connection creates new delivery-flow lifetimes even when the same retained ParticipantId is rebound.

Accepted messages from a terminated flow MUST NOT be silently transferred to a replacement flow. A later recovery specification may define explicit application-level recovery or resubmission without changing this rule.

A flow MAY also terminate before binding loss when its realization reports that the selected delivery contract can no longer be upheld.

Flow termination MUST be observable to the host. The concrete error type and public API representation are not defined by this revision.

## Submission, acceptance, rejection, and exposure

**Submission** is the act of offering one complete message to a delivery flow.

A submission has exactly one of these initial outcomes:

- **Accepted** — RunenNet has admitted the message under that flow's delivery mode; or
- **Rejected** — RunenNet has not admitted the message and makes no delivery guarantee for it.

A rejected submission MUST NOT consume the delivery sequence assigned to accepted `UnreliableSequenced` messages.

Rejection reasons required by resource or realization limits are defined by the delivery pressure specification or later transport profiles.

**Exposure** occurs when the receiving RunenNet delivery layer makes one complete accepted message available to the receiving host/session semantics.

Transport receipt, transport acknowledgement, packet acknowledgement, byte reassembly, and internal queue insertion are not by themselves exposure.

## Delivery modes

Each delivery flow has exactly one delivery mode for its lifetime.

### ReliableOrdered

For a `ReliableOrdered` flow, accepted messages have one total acceptance order within that flow.

While the flow remains operational, the receiver MUST expose every accepted message exactly once and in acceptance order.

The receiver MUST NOT expose a later accepted message before an earlier accepted message from the same flow.

An accepted `ReliableOrdered` message MUST NOT be intentionally discarded, evicted, superseded, or skipped while the flow remains operational.

If the implementation determines that it can no longer uphold these requirements, it MUST terminate the flow with an observable terminal failure. Such termination may leave accepted messages unexposed; those messages MUST NOT be represented as successfully delivered.

`ReliableOrdered` therefore does not promise successful eventual exposure across connection loss, process failure, session closure, or another terminal flow failure.

### UnreliableUnordered

For an `UnreliableUnordered` flow, an accepted message MAY be lost before exposure.

Accepted messages MAY be exposed in any order relative to other accepted messages on the same flow.

This revision does not guarantee duplicate suppression for `UnreliableUnordered`. A receiving realization MAY expose duplicate copies produced by the underlying network or transport unless a later profile adds stronger duplicate handling.

A transport or implementation MUST NOT claim successful reliable delivery merely because a particular unreliable message happened to be exposed.

### UnreliableSequenced

For an `UnreliableSequenced` flow, each accepted message is assigned one logical sequence value that is strictly greater than every sequence value previously assigned to an accepted message in that flow.

Sequence values are scoped to one delivery-flow lifetime. Their wire width, encoding, and wrap strategy are not defined by this revision.

An accepted message MAY be lost before exposure and messages MAY arrive at the receiver out of sequence.

The receiver maintains the sequence value of the most recently exposed message for that flow. A received message MUST be exposed only if its sequence value is greater than the most recently exposed sequence value.

When such a message is exposed, its sequence value becomes the new most recently exposed sequence value. A message whose sequence value is less than or equal to that value MUST NOT subsequently be exposed.

The receiver does not wait for missing sequence values. Exposure MAY skip any number of accepted-but-lost or otherwise unexposed messages.

A newer received message that is discarded before exposure because of permitted unreliable pressure or validation failure does not by itself advance the most recently exposed sequence value.

`UnreliableSequenced` defines receiver-side stale/duplicate rejection. It does not require sender-side coalescing, keyed latest-value replacement, or eviction of older accepted messages solely because a newer message was accepted.

## Delivery mode set

The three modes above are the complete initial RN1 delivery-mode set.

Reliable unordered delivery, reliable sequenced delivery, unreliable delivery with acknowledgements, partial reliability, deadlines, and application-keyed supersession are not defined by this revision.

A later specification may add another delivery mode only with independently defined semantics. Existing modes MUST NOT change meaning to accommodate a new mode.

## Transport realization

A transport/runtime adapter MUST receive or otherwise know the selected delivery-flow mode before it accepts a message for transport realization.

Payload size, current path MTU, stream availability, congestion state, queue occupancy, or adapter implementation convenience MUST NOT change the selected delivery mode.

If a realization cannot support the selected mode for a flow or message, it MUST reject establishment/submission before semantic acceptance where possible, or terminate the affected flow with an explicit failure if the inability is discovered after acceptance. It MUST NOT silently downgrade the mode.

A realization MAY provide behavior stronger than the minimum permitted by an unreliable mode, but that stronger observed behavior does not become a RunenNet guarantee and MUST NOT be exposed as though the flow had changed modes.

Transport-native ordering does not define RunenNet flow ordering. If a realization maps one `ReliableOrdered` flow across transport mechanisms that do not themselves provide the required single ordering domain, RunenNet ordering metadata or another conforming mechanism MUST preserve the flow semantics.

## Flow termination and connection replacement

Connection loss, connection closure, membership termination, or session closure terminates affected flows according to the session lifecycle specification.

For `ReliableOrdered`, termination with accepted but unexposed messages is an observable terminal delivery failure for those messages; they are not silently considered delivered.

For unreliable modes, flow termination may leave accepted messages unexposed without per-message delivery failure because loss is already permitted by the mode. The flow termination itself remains observable.

A new connection binding does not continue the sequence or acceptance order of a terminated flow. Any later recovery protocol that reconstructs higher-level state must do so explicitly above these flow lifetimes.
