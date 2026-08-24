# Delivery Pressure and Resource Policy

Status: **provisional incomplete normative**

This document owns the initial RunenNet delivery resource bounds and local pressure outcomes. It depends on the message, flow, acceptance, exposure, and delivery-mode semantics defined by [Delivery flow semantics](flow.md).

## Scope

This revision defines:

- finite message-size limits;
- finite per-flow and per-binding queue/storage limits;
- outbound submission behavior under local pressure;
- receiver behavior under local pressure;
- reliable versus unreliable pressure rules;
- minimum observable pressure outcomes required for conformance;
- adapter obligations around hidden buffering and unsupported size/capacity constraints.

Bandwidth priority, send cadence, deadlines, application-keyed supersession, congestion-control algorithms, packet MTU discovery, and public configuration API shape are not defined by this revision.

## Resource policy is explicit and finite

Every delivery flow MUST operate under an explicit finite resource policy.

At minimum, a conforming realization MUST bound:

- maximum bytes in one message presented to the delivery layer;
- maximum locally queued/pending message count for one flow;
- maximum locally queued/pending payload bytes for one flow.

A conforming realization that maintains multiple flows for one current participant connection binding MUST also impose finite aggregate bounds on locally queued/pending message count and payload bytes attributable to that binding.

Any additional RunenNet-owned staging queue, retry queue, reassembly buffer, transport-adapter queue, or pending-submission structure that can grow from local or remote message activity MUST have an explicit finite bound or be directly covered by one of the bounds above.

Exact numeric defaults are not defined by this revision.

## Message-size validation

A submission whose payload exceeds the flow's configured maximum message size MUST be rejected before delivery acceptance.

Payload size MUST NOT cause the delivery mode to change.

When an inbound framing or transport realization supplies a claimed message length, a conforming implementation MUST validate that length against the applicable finite limit before allocating storage proportional to the claimed length.

An inbound message that exceeds the applicable maximum MUST NOT be exposed. The later wire/protocol specification may define whether such input terminates a protocol session, terminates a flow, or is otherwise classified as a protocol violation; this document only requires bounded allocation and non-exposure.

## Outbound pressure before acceptance

When accepting a submitted message would exceed a per-flow or per-binding local queue/storage bound, the implementation MUST apply an explicit pressure outcome before accepting the new message, except where the selected unreliable eviction policy below deliberately frees capacity first.

Every flow MUST support **RejectNew** pressure behavior:

- the new submission is rejected;
- no delivery sequence is consumed for that rejected submission;
- previously accepted messages are unaffected.

A `ReliableOrdered` flow MUST NOT free capacity by discarding, evicting, superseding, or replacing an accepted reliable message. If capacity is unavailable, the new submission MUST remain unaccepted or be rejected according to the runtime's submission API; it MUST NOT be reported as accepted and then silently dropped because of local pressure.

This specification does not require a blocking or waiting submission API. If an implementation owns a queue of submissions waiting for capacity, that queue is itself subject to explicit finite bounds.

## Optional unreliable eviction policy

An unreliable flow MAY be configured with **EvictOldestUnreliable** instead of RejectNew as its local outbound queue-pressure policy.

When a new submission would exceed a local bound under this policy, the implementation MAY discard the oldest locally pending accepted messages from the same unreliable flow until the new message fits or until no permitted eviction can make it fit.

Each deliberate eviction MUST be classified as a local unreliable pressure drop. If sufficient capacity is obtained, the new submission may then be accepted normally. If sufficient capacity cannot be obtained, the new submission is rejected.

`EvictOldestUnreliable` is valid only for `UnreliableUnordered` and `UnreliableSequenced` flows. It MUST NOT be used for `ReliableOrdered`.

This pressure policy is FIFO queue eviction only. It does not define application-keyed latest-value supersession, semantic replacement, or cancellation.

For `UnreliableSequenced`, discarding a locally pending message does not alter the receiver's most-recently-exposed sequence value. Receiver sequence behavior remains owned by the delivery-flow specification.

## Local custody after reliable acceptance

After a `ReliableOrdered` message is accepted, every RunenNet-owned stage that has custody of that message MUST do one of the following:

- retain it within explicit finite bounds until it can transfer custody without loss;
- transfer it to another conforming stage that preserves the same reliable-flow obligation; or
- cause an observable terminal flow failure when the obligation can no longer be upheld.

A stage MUST NOT report successful transfer merely because bytes were copied into an unbounded or semantically lossy downstream queue.

This requirement does not imply that the sender can know remote application exposure before a flow ends. Reliable delivery remains conditional on an operational flow as defined by the delivery-flow specification.

## Receiver pressure

Receiver-side storage influenced by incoming traffic MUST remain within the finite policy bounds.

For `ReliableOrdered` traffic, receiver pressure MUST NOT silently discard an accepted/in-order message while leaving the flow operational. A conforming realization MUST instead apply bounded backpressure before accepting additional reliable data where the realization permits it, or terminate the affected flow/binding with an observable terminal pressure failure if the reliable obligation can no longer be preserved.

For unreliable traffic, the receiver MAY discard a complete incoming message when local pressure prevents bounded admission. Such a deliberate local discard MUST be classified as a local unreliable pressure drop and the message MUST NOT be exposed.

For `UnreliableSequenced`, a message discarded before exposure because of receiver pressure does not advance the most-recently-exposed sequence value.

The specific choice between dropping a newly arrived unreliable message and evicting an older buffered unreliable message is an implementation/configuration policy unless a later profile standardizes it. Whatever policy is selected MUST be bounded and MUST NOT be silently applied to reliable traffic.

## Aggregate pressure

A message that fits its per-flow limits may still exceed the aggregate resource budget for the current participant connection binding.

Aggregate pressure MUST apply the same reliability distinction as per-flow pressure:

- accepted reliable messages MUST NOT be silently discarded to satisfy the aggregate bound;
- new reliable submissions may remain unaccepted or be rejected;
- unreliable traffic may be rejected or deliberately dropped only according to an explicit bounded unreliable policy.

A realization MUST NOT evade aggregate bounds by creating additional delivery flows or transport-native streams for the same binding.

## Adapter and transport buffering

Transport adapters MUST configure or wrap transport-native queues and buffers so RunenNet-owned delivery behavior remains bounded.

A transport adapter MUST NOT use payload size or queue pressure to switch a message to a different delivery mode.

A transport-native API that can deliberately discard previously queued data under local pressure MUST NOT be used as an unobservable substitute for the configured RunenNet pressure policy.

For unreliable modes, transport/network loss that is intrinsic to the selected unreliable realization remains permitted by the delivery mode. Deliberate local RunenNet/adapter pressure drops that are under implementation control remain subject to the explicit pressure policy and observability requirements in this document.

For reliable mode, any transport or adapter condition that would require loss of an accepted message MUST instead preserve bounded backpressure or cause observable terminal flow failure.

## Required observable outcomes

A conforming implementation MUST make at least the following outcome classes distinguishable to its conformance/runtime inspection boundary:

- submission accepted;
- submission rejected because the message exceeds its size limit;
- submission rejected because of local pressure;
- accepted unreliable message deliberately dropped by local pressure;
- terminal flow failure because a reliable delivery obligation could not be preserved.

This revision does not require those outcomes to be public per-message events, wire messages, or a particular Rust enum. Aggregate diagnostics are sufficient where the individual message cannot be identified, provided deterministic conformance tests can distinguish the required behavior.

Network loss of an unreliable message is not required to be observable to the sender.

## Deferred scheduling semantics

Bandwidth priority, weighted scheduling, fairness between flows, send frequency, deadlines/expiration, and application-keyed supersession are not defined by this revision.

An implementation MAY use internal scheduling strategies only if they preserve all accepted delivery and pressure requirements. Internal scheduling behavior MUST NOT become implicit normative priority semantics.
