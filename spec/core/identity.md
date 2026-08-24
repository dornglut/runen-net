# Core Identity and Time

Status: **provisional incomplete normative**

This document owns the identity and logical-time vocabulary required by the initial RunenNet semantic core. It does not define admission, participant membership, connection binding, session lifecycle, or authority transitions.

## Scope

This revision defines:

- session identity;
- participant identity;
- network entity identity;
- simulation tick identity;
- the non-equivalence of those identities with transport connections and application authentication identities.

Wire widths, serialization, cryptographic generation algorithms, public Rust type names, admission state, and multi-authority/failover identity are not defined by this revision.

## Session identity

A **SessionId** identifies one RunenNet session lifetime.

A SessionId is opaque. Its semantic meaning does not derive from a process ID, socket address, transport connection identifier, filesystem state, ECS entity, or application-visible server name.

A conforming realization MUST ensure that data belonging to one session lifetime cannot be accepted as data for a distinct later session because the same SessionId was reused.

The mechanism used to satisfy that requirement is not defined by this revision. In particular, RunenNet does not require a process-global counter.

## Participant identity

A **ParticipantId** identifies one participant identity within one session.

ParticipantId uniqueness is scoped by SessionId. A ParticipantId MUST NOT identify more than one participant within the same session, and once a ParticipantId has identified a participant it MUST NOT later identify a different participant before that session ends.

A ParticipantId is distinct from:

- a transport connection or transport connection identifier;
- a socket/network address;
- an authentication account, principal, ticket, or platform user ID;
- an ECS entity.

An application MAY associate external authentication or account information with a ParticipantId, but that external identity does not become the RunenNet ParticipantId merely because the association exists.

Whether the participant is admitted, bound to a transport connection, retained after connection loss, or removed is not identity semantics.

## Network entity identity

A **NetworkEntityId** identifies one replicated entity incarnation within one session.

NetworkEntityId uniqueness is scoped by SessionId. A NetworkEntityId MUST NOT identify more than one entity incarnation within the same session, and once assigned it MUST NOT later identify a different entity incarnation before that session ends.

This prevents delayed or retained network state from becoming applicable to a later entity solely because an identifier was recycled.

A NetworkEntityId does not imply an ECS entity representation. Hosts MAY map it to ECS entities, object handles, database keys, or other local state through adapters outside the core semantics.

Cross-session persistent object identity is not defined by this revision.

## Simulation tick

A **SimulationTick** identifies an application simulation step on one session's simulation timeline.

The host supplies SimulationTick values. RunenNet MUST NOT infer them from wall-clock time, transport packet numbers, transport connection state, or executor scheduling.

Within one session, greater SimulationTick values denote later simulation steps. A conforming host MUST NOT reuse a smaller tick value to denote a later step in the same session.

Not every simulation step is required to produce network traffic.

SimulationTick is distinct from delivery sequence numbers and replication snapshot cursors. Those concepts order different semantic domains and are not defined by this document.

## Transport connection identity is distinct

A transport/runtime adapter MAY use an opaque local handle to refer to one transport-connection lifetime. That handle is not a SessionId, ParticipantId, or NetworkEntityId and is not by itself protocol identity.

A transport's native connection identifiers, including identifiers that may rotate during one transport connection, MUST NOT be treated as RunenNet session or participant identity solely because the transport exposes them.

Connection binding and replacement are not defined by this document.

## Additional identity domains

The initial core does not define a separate authority-generation or authority-epoch identifier.

Authentication principals, persistent account identities, matchmaking identities, and cross-session persistent entity identities are also outside this identity model.

Preserving one SessionId across replacement of the authority responsible for that session is not defined by this revision.
