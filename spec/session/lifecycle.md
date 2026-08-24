# Session and Authority Lifecycle

Status: **provisional incomplete normative**

This document owns the initial RunenNet session, participant-membership, admission, connection-binding, and authority lifecycle semantics. The identity and logical-time vocabulary used here is defined by [Core identity and time](../core/identity.md).

## Initial authority model

This revision defines a single-authority client/server session model.

Each Open session has exactly one **authority**. For the lifecycle owned by this document, the authority owns:

- establishment and closure of the session;
- whether the session accepts admission;
- creation, retention, and explicit removal of participant membership;
- authorization of current participant connection bindings and replacements.

Authority responsibilities outside this lifecycle are not defined by this document.

Authority is a semantic role. It MUST NOT be inferred from a socket address, transport connection identifier, thread, process, ECS resource, or ownership of a transport adapter.

Peer-to-peer, multi-authority, authority handoff, and preservation of one SessionId across authority replacement are not defined by this revision.

## Identity allocation authority

When establishing a new session lifetime, the authority MUST supply the SessionId for that lifetime. The SessionId MUST satisfy the identity and non-confusion requirements defined by the core identity specification.

When creating a new participant membership, the authority MUST assign its ParticipantId. When assigning a NetworkEntityId for a session-scoped replicated entity, the authority MUST assign that identity. Both assignments MUST satisfy the applicable session-scoped uniqueness and non-reuse requirements defined by the core identity specification.

The generation algorithms and concrete representations of these identifiers are not defined by this document.

## Session lifecycle

A session is either **Open** or **Closed** in the initial core.

A new session lifetime begins Open when its authority establishes it with a SessionId.

An Open session MAY admit participants and maintain participant memberships and connection bindings.

Closing a session is terminal for that session lifetime. A Closed session MUST NOT become Open again under the same lifetime. When a session becomes Closed:

- all participant memberships in that session end;
- all current connection bindings to that session cease to authorize participant traffic;
- all session-scoped NetworkEntityId values cease to identify live entities in that session;
- later traffic or retained state from that SessionId MUST NOT be applied to another session lifetime.

Graceful drain/shutdown phases may be introduced by a later runtime/profile specification; they do not change the terminal meaning of Closed.

## Transport connection

A **transport connection** is one realized communication relationship supplied by a transport/runtime adapter.

Transport connection establishment does not create a RunenNet participant membership and does not by itself authorize application, input, replication, or authority traffic.

A transport adapter MAY expose a local opaque connection handle. Its representation and native transport identifiers are implementation concerns. The handle is used only to associate transport events with the semantic binding described below.

## Admission

Admission is the authority decision that either creates a participant membership or accepts a binding for an existing retained membership, and binds the accepted transport connection to that participant.

An admission attempt targets one Open session.

On **admission acceptance**:

- the admission MUST target the intended SessionId;
- a newly created membership MUST receive a ParticipantId not previously used for another participant in that session;
- an existing retained membership MUST keep its existing ParticipantId;
- the accepted transport connection MUST become the participant's current authorized binding.

On **admission rejection**, no new participant membership or authorized connection binding is created by that admission attempt.

Authentication, account identity, matchmaking, server discovery, connect-ticket issuance, lobby membership, roster policy, and game settings are outside this specification. A host MAY use such information to make the admission decision, but RunenNet does not standardize that policy here.

Any protocol or schema compatibility rules that constrain admission are owned by the applicable protocol specification.

## Participant membership

A **participant membership** associates one ParticipantId with one Open session.

The initial client/server profile permits at most one current transport connection binding for a participant at a time.

A participant with a current authorized binding is **Bound**. A retained membership without a current connection binding is **Unbound**.

Only a Bound participant may originate traffic that the session interprets as that participant's traffic.

A participant membership ends when:

- the authority explicitly removes it;
- the selected session policy ends it after connection loss or recovery expiry; or
- the session closes.

After membership ends, that membership MUST NOT be rebound. ParticipantId reuse remains governed by the core identity specification.

## Connection loss and bounded retention

Loss or closure of the currently bound transport connection MUST remove that connection binding.

Connection loss does not implicitly transfer authority and MUST NOT cause another connection to inherit the participant identity without an explicit authorized admission or rebind decision.

A session policy MUST select one of these membership outcomes after binding loss:

- **Terminate** — the participant membership ends when the binding is lost; or
- **RetainForRecovery** — the participant becomes Unbound under an explicit bounded retention policy.

RetainForRecovery MUST have a finite bound expressed by host/runtime policy. An Unbound participant cannot originate participant traffic and MUST NOT retain unbounded remotely influenced resources solely because the connection disappeared.

This revision defines membership retention only. The protocol for reconnect attempts, proof of continuity, state recovery, baseline recovery, and retry timing is not defined here.

## Authorized connection replacement

An Open session MAY bind a replacement transport connection to an existing Unbound participant only after an explicit authority-approved admission or rebind decision establishes that the replacement belongs to that participant.

The existing ParticipantId MUST remain unchanged across such an authorized replacement.

A replacement binding MUST NOT make traffic from the previous connection valid again. If the previous connection later produces delayed traffic, that traffic is not authorized as participant traffic after its binding has ceased.

Simultaneous multi-connection participant membership is not defined by this revision.

## Session scoping

Every semantic operation that acts on participant membership or session-owned network identity is scoped to exactly one SessionId.

A conforming realization MUST prevent an operation, retained message, or recovery artifact from one SessionId from being applied as though it belonged to a different SessionId.

The wire/protocol specification owns how SessionId scope is represented and validated in encoded protocol traffic.

## Observable outcomes

Later runtime APIs may expose more detailed operational states, but they MUST preserve at least these semantic distinctions:

- transport connection established but not admitted;
- admission accepted versus rejected;
- participant Bound versus Unbound;
- participant membership ended;
- session Open versus Closed.

Runtime-specific states such as DNS resolution, QUIC handshaking, retry backoff, socket setup, or executor task lifecycle are not RunenNet session states unless a later specification explicitly promotes them to semantic authority.
