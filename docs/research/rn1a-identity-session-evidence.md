# RN1A Identity and Session Evidence

Status: **non-normative**

This record supports [RN1A](https://github.com/dornglut/runen-net/issues/4). It compares Runenwerk migration evidence with external networking designs. It does not define RunenNet semantics.

Evidence snapshot:

- Runenwerk: `37a267e41e49317516d6513b02794f8fc480056a` (observed 2026-08-24)
- Lightyear: `0.29.0` documentation (observed 2026-08-24)
- QUIC: RFC 9000
- netcode: 1.02 standard on the observed upstream branch

## Questions under review

RN1A must distinguish the lifetime and ownership of:

- an authoritative RunenNet session;
- an admitted participant;
- a transient transport connection;
- a replicated network entity;
- application simulation time versus protocol ordering/cursors.

It must also decide whether a separate authority epoch is required in the initial core.

## Current Runenwerk evidence

### Simulation identity is coupled to the engine ECS

Runenwerk `net/engine_sim/src/identity.rs` colocates `SimulationTick`, `SimulationSessionId`, `ActorId`, and `NetEntityId`. `SimulationTick` and `SimulationSessionId` derive concrete `ecs::Component` and `ecs::Resource`, and `SimulationSessionId::new()` allocates from a process-global `AtomicU64`.

This is unsuitable as standalone RunenNet authority because semantic identity should not depend on a concrete ECS or process-global allocator.

Source: [Runenwerk simulation identity](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_sim/src/identity.rs).

### Session state uses a separate transport connection identity

Runenwerk `engine_net` separately stores a transport `ConnectionId` in client/server session state. The server has a session-level configuration plus a set of active connection IDs and a numeric next-connection allocator.

This demonstrates that connection lifetime and simulation/session vocabulary are already distinct implementation concerns, but the current public model does not provide a stable admitted-participant identity between them.

Source: [Runenwerk session state](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/session/ids.rs).

### Admission currently returns connection identity as the join identity

On an accepted join, current Runenwerk allocates a `ConnectionId` and returns its numeric value in `JoinAccepted`. The client then stores that connection ID when it becomes active.

That is convenient for the current runtime, but it risks making a transient transport connection the de facto application/session identity. It also makes later connection replacement or reconnect semantics harder to express without changing identity.

Sources:

- [Runenwerk admission](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/session/admission.rs)
- [Runenwerk client handoff](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/session/handoff.rs)

### Admission also carries game-specific state

`AuthoritativeJoinState` contains lobby IDs, roster player codes, player/AI limits, and JSON settings. Those are application/game concerns, not general session identity semantics, and should not migrate into the standalone core.

### Reconnect design already separates transport mechanics from authority semantics

The active Runenwerk reconnect design states that `engine_net` owns session/protocol authority while `engine_net_quic` owns reconnect attempts and backoff. It also states that reconnect must not change server authority.

This supports keeping connection replacement below the semantic session/participant identity boundary.

Source: [Runenwerk reconnect and history recovery design](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/docs-site/src/content/docs/design/active/net-reconnect-history-recovery.md).

## External evidence

### QUIC connection IDs are not application session IDs

RFC 9000 defines QUIC connection IDs as transport identifiers selected by endpoints. A single QUIC connection may use multiple connection IDs and migrate between network paths while remaining the same connection. Therefore a RunenNet application/session identity must not be defined by a Quinn/QUIC connection ID or socket address.

Source: [RFC 9000, Connection ID and Connection Migration](https://www.rfc-editor.org/rfc/rfc9000.html#name-connection-id).

### Long-lived peer identity over transient links is an established useful separation

Lightyear 0.29.0 documentation describes a connection layer with a long-running `PeerId` over lower-level links. This is not authority for RunenNet, but it demonstrates that a stable application-network participant identity can remain useful independently of the transport link that currently carries traffic.

Source: [Lightyear 0.29.0 crate documentation](https://docs.rs/lightyear/0.29.0/lightyear/).

### Secure connection protocols also separate client identity from endpoint mapping

The observed netcode 1.02 standard carries a client ID in authenticated connection-token data while maintaining separate endpoint/encryption mappings and packet sequence/replay state. This again illustrates that client/application identity need not be the network address or transient connection realization.

Source: [netcode standard](https://github.com/mas-bandwidth/netcode/blob/main/STANDARD.md).

## Resulting design pressure

The evidence supports the following minimal direction for normative review:

1. **Session identity** names one authoritative RunenNet session lifetime and must not depend on process-global allocation.
2. **Participant identity** is session-scoped and is the stable identity for one admitted membership rather than for one transport connection.
3. **Connection identity/handle** names only a transient transport-connection lifetime and is not a protocol/session identity. A transport adapter supplies it locally; it must not be equated with QUIC connection IDs.
4. **Network entity identity** is authority-owned and session-scoped. Reuse during the same session should be prohibited so delayed traffic cannot target a different entity incarnation.
5. **Simulation tick** is host-provided logical simulation time. It is distinct from delivery sequence numbers and snapshot cursors, which belong to their later semantic owners.
6. **Separate authority epoch** is not justified for the initial core if each distinct authoritative session lifetime has a non-confusable `SessionId`. A later failover model that preserves one logical session across authority replacement may add an authority-generation concept explicitly.
7. **Authentication principal, matchmaking/server discovery, lobby/roster state, and ticket policy** remain outside the core. Admission may consume an application decision without standardizing how that decision was produced.

## Resolved RN1A choices

The normative draft resolves the initial open questions as follows:

- SessionId generation is mechanism-independent, but reuse must not permit data from one session lifetime to be accepted by another.
- The initial client/server profile permits one current transport connection binding per participant.
- Connection loss always removes the binding. Membership either terminates or remains Unbound only under an explicit bounded recovery-retention policy.
- Authorized connection replacement may preserve ParticipantId only for an existing retained membership.
- Session lifecycle exposes semantic admission/binding/membership distinctions, not transport handshake or executor states.

## Deferred items

Exact integer widths, serialization, cryptographic generation, public Rust type names, QUIC mapping, multi-connection participants, peer-to-peer/multi-authority topology, and authority failover within one SessionId are not defined by RN1A.
