# Repository Architecture

This document owns the structure and dependency boundaries of the RunenNet repository. It does not define networking semantics.

## Product boundary

RunenNet is a standalone Rust networking framework. Runenwerk is a downstream consumer, not an architectural host.

Production RunenNet packages MUST NOT depend on:

- Runenwerk;
- a concrete ECS;
- rendering or UI frameworks;
- spatial/world-streaming frameworks;
- gameplay/application frameworks;
- engine plugin or scheduling APIs.

Consumer integrations may depend on RunenNet plus their host frameworks outside the RunenNet semantic core.

## Dependency law

Networking semantics are established before transport realization. Transport-independent semantic packages MUST NOT depend on a production transport, socket API, or executor-specific runtime API. Transport/runtime adapters may realize accepted contracts but MUST NOT silently alter any accepted RunenNet semantic contract.

Terms or feature families that are not yet normatively defined — including currently deferred priority, keyed supersession, advanced reconnect/history, prediction, and interest semantics — are not implementation authority merely because a future adapter or consumer needs them.

World/game policy such as spatial relevancy, ownership facts, and application simulation remains consumer-owned and enters RunenNet through explicit host-neutral contracts.

## Implementation packages

RN0 intentionally did not ratify a multi-crate product topology. Creating core, protocol, runtime, transport, macro, or adapter crates before their independent ownership was demonstrated would have made package shape precede semantic evidence.

RN1 established the initial semantic ownership boundaries, and RN2 introduced the minimum transport-independent implementation package required to realize them.

The currently ratified production package topology is:

- `crates/runen-net` — transport-independent RunenNet semantic/core implementation;
- `crates/runen-net-quic` — production QUIC transport adapter that realizes the accepted QUIC profile downstream of `runen-net`.

The permitted dependency direction is `runen-net-quic -> runen-net`. `runen-net` MUST NOT depend on `runen-net-quic`.

Production QUIC, TLS, socket, and executor-specific dependencies belong to `runen-net-quic` when an accepted RN5 implementation slice actually requires them. They MUST NOT enter `runen-net` merely because the adapter uses them.

The adapter package is implementation structure, not specification authority. QUIC wire and transport semantics remain owned by the normative specification; package modules and types MUST NOT create competing semantic contracts.

Additional package splits require separate architectural justification. Control, reliable-flow, DATAGRAM, TLS, and endpoint concerns are not separate crates merely because they are distinct implementation responsibilities.

## Top-level artifact areas

- `spec/` — normative specification artifacts;
- `docs/` — non-normative architecture, verification, decisions, research, and guides;
- `tools/` — repository tooling only;
- `crates/` — accepted product implementation packages;
- future `examples/` — consumer-facing examples and standalone proofs;
- future `conformance/` — executable conformance assets when required by the accepted verification design.

## Extraction boundary

Current Runenwerk networking source is migration evidence, not package authority. Extraction MUST separate reusable networking semantics from engine-specific realization instead of moving the current `net/` tree wholesale.

RunenNet may own transport-independent concerns such as sessions, delivery contracts, protocol identity, replication cursors/baselines, acknowledgements, resynchronization, network-oriented retention, prediction/input protocol contracts, generic interest vocabulary, recovery semantics, and transport interfaces when those concerns have accepted standalone semantic owners.

RunenNet does not automatically own general simulation, ECS world access, engine schedules/resources, spatial queries, rendering smoothing, gameplay policy, or archival/editor replay.

## Cutover rule

When Runenwerk adopts RunenNet, networking semantic authority moves to this repository. Old Runenwerk semantic crates and duplicate protocol state are removed rather than preserved through forwarding or compatibility layers.
