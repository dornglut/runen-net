# Delivery Pressure and Resource Policy

Status: **provisional incomplete normative**

This document owns the initial RunenNet delivery resource bounds and local pressure outcomes. It depends on the message, flow, acceptance, exposure, and delivery-mode semantics defined by [Delivery flow semantics](flow.md).

## Scope

This revision defines:

- finite message-size limits;
- finite per-flow, per-binding, and per-session delivery storage limits;
- finite active delivery-flow counts;
- outbound submission behavior under local pressure;
- receiver behavior under local pressure;
- reliable versus unreliable pressure rules;
- minimum observable pressure outcomes required for conformance;
- adapter obligations around hidden buffering and unsupported size/capacity constraints.

Bandwidth priority, send cadence, deadlines, application-keyed supersession, congestion-control algorithms, packet MTU discovery, participant-count/admission policy, and public configuration API shape are not defined by this revision.

## Pressure policy attachment

Before a delivery flow accepts its first message, the implementation MUST attach an explicit finite resource policy to that flow.

The policy MUST define:

- maximum message bytes;
- maximum locally queued/pending message count for the flow;
- maximum locally queued/pending payload bytes for the flow;
- the outbound pressure behavior for that flow;
- the receiver pressure behavior for that flow.

Those limits and pressure behaviors MUST remain semantically stable for the flow lifetime. A realization MAY reduce internal transport capacity dynamically, but such a change does not authorize silent weakening of the attached RunenNet policy or delivery mode.

Exact numeric defaults are not defined by this revision.

## Aggregate delivery bounds

Every current participant connection binding MUST have explicit finite aggregate bounds on:

- active delivery-flow count;
- locally queued/pending delivery message count across its flows;
- locally queued/pending delivery payload bytes across its flows.

Those aggregate bounds apply even when the binding currently has only one delivery flow.

Every Open session MUST also have explicit finite aggregate delivery bounds on:

- active delivery-flow count across all current bindings;
- locally queued/pending delivery message count across all current bindings;
- locally queued/pending delivery payload bytes across all current bindings.

These are delivery-resource bounds, not participant-count or admission-policy semantics. A session may use any admission policy that remains consistent with its finite delivery-resource budget.

A realization MUST NOT evade an aggregate bound by creating additional delivery flows, connection bindings, or transport-native streams.

Any additional RunenNet-owned staging queue, retry queue, reassembly buffer, transport-adapter queue, flow registry, or pending-submission structure that can grow from local or remote message activity MUST have an explicit finite bound or be directly covered by a flow/binding/session bound above.

## Message-size validation

A submission whose payload exceeds the flow's maximum message size MUST be rejected before delivery acceptance.

Payload size MUST NOT cause the delivery mode to change.

When inbound framing or transport input supplies a claimed message length, a conforming implementation MUST validate that length against the applicable finite limit before allocating storage proportional to the claimed length.

An inbound message that exceeds the applicable maximum MUST NOT be exposed. The later wire/protocol specification may define whether such input terminates a protocol session, terminates a flow, or is otherwise classified as a protocol violation; this document only requires bounded allocation and non-exposure.

## Outbound pressure behaviors

The initial outbound pressure behaviors are **RejectNew** and **EvictOldestUnreliable**.

### RejectNew

RejectNew is valid for every delivery mode.

When accepting a submitted message would exceed a per-flow, per-binding, or per-session local bound:

- the new submission is rejected;
- no delivery sequence is consumed for that rejected submission;
- previously accepted messages are unaffected.

A `ReliableOrdered` flow MUST use RejectNew when local pressure prevents immediate acceptance. It MUST NOT free capacity by discarding, evicting, superseding, or replacing an accepted reliable message.

This specification does not require a blocking or waiting submission API. A runtime MAY defer deciding a reliable submission until capacity exists, but any RunenNet-owned structure that stores deferred submissions is itself subject to explicit finite bounds and the message MUST remain unaccepted until the decision is made.

### EvictOldestUnreliable

EvictOldestUnreliable is valid only for `UnreliableUnordered` and `UnreliableSequenced` flows.

When a new submission would exceed a local bound under this policy, the implementation MAY discard the oldest locally pending accepted messages from the same unreliable flow until the new message fits or until no permitted eviction can make it fit.

Each deliberate eviction MUST be classified as a local unreliable pressure drop. If sufficient capacity is obtained, the new submission may then be accepted normally. If sufficient capacity cannot be obtained, the new submission is rejected.

EvictOldestUnreliable MUST NOT evict accepted reliable messages or messages owned by another flow merely to admit the new submission.

This behavior is FIFO queue eviction only. It does not define application-keyed latest-value supersession, semantic replacement, or cancellation.

For `UnreliableSequenced`, discarding a locally pending message does not alter the receiver's most-recently-exposed sequence value.

## Local custody after reliable acceptance

After a `ReliableOrdered` message is accepted, every RunenNet-owned stage that has custody of that message MUST do one of the following:

- retain it within explicit finite bounds until it can transfer custody without loss;
- transfer it to another conforming stage that preserves the same reliable-flow obligation; or
- cause an observable terminal flow failure when the obligation can no longer be upheld.

A stage MUST NOT report successful transfer merely because bytes were copied into an unbounded or semantically lossy downstream queue.

This requirement does not imply that the sender can know remote application exposure before a flow ends. Reliable delivery remains conditional on an operational flow as defined by the delivery-flow specification.

## Receiver pressure behaviors

The initial unreliable receiver pressure behaviors are **DropIncomingUnreliable** and **EvictOldestBufferedUnreliable**. A flow using an unreliable delivery mode MUST select one of them before accepting its first message.

Receiver-side storage influenced by incoming traffic MUST remain within the attached flow and aggregate binding/session bounds.

### Reliable receiver pressure

For `ReliableOrdered` traffic, receiver pressure MUST NOT silently discard a received message while leaving the flow operational.

A conforming realization MUST apply bounded backpressure before admitting additional reliable data where the realization permits it, or terminate the affected flow/binding with an observable terminal pressure failure if the reliable obligation can no longer be preserved.

Reliable receiver pressure MUST NOT use either unreliable receiver-drop behavior below.

### DropIncomingUnreliable

When a complete incoming unreliable message cannot be admitted without exceeding a local bound, the receiver discards that incoming message and does not expose it.

The discard MUST be classified as a local unreliable pressure drop.

### EvictOldestBufferedUnreliable

When a complete incoming unreliable message cannot be admitted without exceeding a local bound, the receiver MAY discard the oldest buffered, not-yet-exposed message from the same unreliable flow until the incoming message fits or until no permitted eviction can make it fit.

Each eviction MUST be classified as a local unreliable pressure drop. If sufficient capacity still cannot be obtained, the incoming message is also discarded and classified as a local unreliable pressure drop.

This behavior MUST NOT evict reliable messages or messages from another flow.

For `UnreliableSequenced`, a message discarded before exposure because of receiver pressure does not advance the most-recently-exposed sequence value.

## Aggregate pressure

A message that fits its per-flow limits may still exceed a binding-level or session-level aggregate delivery budget.

Aggregate pressure MUST preserve the same reliability distinction:

- accepted reliable messages MUST NOT be silently discarded to satisfy an aggregate bound;
- new reliable submissions may remain unaccepted or be rejected;
- unreliable submissions/received messages may be rejected or deliberately dropped only according to their selected explicit pressure behavior.

If a flow-local unreliable eviction cannot resolve aggregate pressure without evicting another flow's data, the new message MUST be rejected or dropped rather than implicitly changing another flow's policy.

If a new delivery flow would exceed a binding-level or session-level active-flow bound, that flow MUST NOT begin accepting messages.

## Adapter and transport buffering

Transport adapters MUST configure or wrap transport-native queues and buffers so RunenNet-owned delivery behavior remains bounded.

A transport adapter MUST NOT use payload size or queue pressure to switch a message to a different delivery mode.

A transport-native API that can deliberately discard previously queued data under local pressure MUST NOT be used as an unobservable substitute for the selected RunenNet pressure behavior.

For unreliable modes, transport/network loss intrinsic to the selected unreliable realization remains permitted by the delivery mode. Deliberate local RunenNet/adapter pressure drops under implementation control remain subject to the explicit pressure behavior and observability requirements in this document.

For reliable mode, any transport or adapter condition that would require loss of an accepted message MUST instead preserve bounded backpressure or cause observable terminal flow failure.

## Required observable outcomes

A conforming implementation MUST make at least the following outcome classes distinguishable to its conformance/runtime inspection boundary:

- submission accepted;
- submission rejected because the message exceeds its size limit;
- submission rejected because of local pressure;
- delivery-flow establishment rejected because an aggregate active-flow bound is exhausted;
- accepted unreliable message deliberately dropped by local pressure;
- received unreliable message deliberately dropped by local pressure;
- terminal flow failure because a reliable delivery obligation could not be preserved.

This revision does not require those outcomes to be public per-message events, wire messages, or a particular Rust enum. Aggregate diagnostics are sufficient where the individual message cannot be identified, provided deterministic conformance tests can distinguish the required behavior.

Network loss of an unreliable message is not required to be observable to the sender.

## Deferred scheduling semantics

Bandwidth priority, weighted scheduling, fairness between flows, send frequency, deadlines/expiration, and application-keyed supersession are not defined by this revision.

An implementation MAY use internal scheduling strategies only if they preserve all accepted delivery and pressure requirements. Internal scheduling behavior MUST NOT become implicit normative priority semantics.
