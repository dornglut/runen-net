# Core Identity and Time

Status: **provisional incomplete normative**

This document owns the identity and logical-time concepts required by the initial RunenNet semantic core. Session lifecycle and authority transitions are owned by [Session and authority lifecycle](../session/lifecycle.md).

## Scope

This revision defines:

- session identity;
- participant identity;
- network entity identity;
- authoritative simulation tick identity;
- the non-equivalence of those identities with transport connections and application authentication identities.

Wire widths, serialization, cryptographic generation algorithms, public Rust type names, and multi-authority/failover identity are not defined by this revision.

## Session identity

A **SessionId** identifies one authoritative RunenNet session lifetime.

A SessionId is opaque. Its semantic meaning does not derive from a process ID, socket address, transport connection identifier, filesystem state, ECS entity, or application-visible server name.

A conforming realization MUST ensure that data belonging to one session lifetime cannot be accepted as data for a distinct later session because the same SessionId was reused.

The mechanism used to satisfy that requirement is not defined by this revision. In particular, RunenNet does not require a process-global counter.

## Participant identity

A **ParticipantId** identifies one admitted participant membership within one session.

ParticipantId uniqueness is scoped by SessionId. Within a session, the authority MUST NOT assign the same ParticipantId to two distinct participant memberships, and a retired ParticipantId MUST NOT be reused before that session ends.

A ParticipantId is distinct from:

- a transport connection or transport connection identifier;
- a socket/network address;
- an authentication account, principal, ticket, or platform user ID;
- an ECS entity.

An application MAY use external authentication or account information when deciding which participant is being admitted, but that external identity does not become RunenNet ParticipantId authority merely by being supplied to admission.

ParticipantId lifetime follows participant-membership lifetime. Connection loss, retention, and authorized connection replacement are owned by [Session and authority lifecycle](../session/lifecycle.md).

## Network entity identity

A **NetworkEntityId** identifies one authority-owned replicated entity incarnation within one session.

NetworkEntityId uniqueness is scoped by SessionId. The authority owns assignment of NetworkEntityId values for authoritative replicated entities.

Once assigned within a session, a NetworkEntityId MUST NOT be reused for a different entity incarnation before the session ends. This prevents delayed or retained network state from becoming applicable to a later entity solely because an identifier was recycled.

A NetworkEntityId does not imply an ECS entity representation. Hosts MAY map it to ECS entities, object handles, database keys, or other local state through adapters outside the core semantics.

Cross-session persistent object identity is not defined by this revision.

## Authoritative simulation tick

A **SimulationTick** identifies an application simulation step on the authoritative session timeline.

The authoritative host supplies SimulationTick values. RunenNet MUST NOT infer them from wall-clock time, transport packet numbers, transport connection state, or executor scheduling.

Within one session, greater SimulationTick values denote later authoritative simulation steps. A conforming host MUST NOT reuse a smaller tick value to denote a later step in the same session.

Not every simulation step is required to produce network traffic.

SimulationTick is distinct from delivery sequence numbers and replication snapshot cursors. Those concepts order different semantic domains and are owned by their respective delivery and replication specifications.

## Connection identity is not protocol identity

A transport/runtime adapter may use an opaque local handle to refer to one transport-connection lifetime. That handle is not a SessionId, ParticipantId, or NetworkEntityId and is not by itself protocol identity.

A transport's native connection identifiers, including identifiers that may rotate during one transport connection, MUST NOT be treated as RunenNet session or participant identity solely because the transport exposes them.

Connection binding and replacement are owned by [Session and authority lifecycle](../session/lifecycle.md).

## Authority lifetime

The initial core does not define a separate authority-generation or authority-epoch identifier.

One SessionId identifies one authoritative session lifetime. Preserving the same logical session across replacement of its authority is not defined by this revision. A realization that starts a distinct authoritative session lifetime MUST use a SessionId that satisfies the non-confusion requirement above.

A future failover profile may introduce explicit authority-generation semantics without changing the meaning of SessionId defined here.
