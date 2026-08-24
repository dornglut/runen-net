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

Networking semantics are established before transport realization. A transport adapter may realize delivery contracts but MUST NOT silently alter their reliability, ordering, sequencing, supersession, priority, recovery, or pressure semantics.

World/game policy such as spatial relevancy, ownership facts, and application simulation remains consumer-owned and enters RunenNet through explicit host-neutral contracts.

## Implementation packages

RN0 intentionally does not ratify a multi-crate implementation topology. Creating `runen-net-core`, protocol, runtime, transport, macro, or adapter crates before their independent ownership is demonstrated would make package shape precede semantic evidence.

RN2 may introduce the minimum package decomposition justified by accepted RN1 semantics and conformance needs.

## Top-level artifact areas

- `spec/` — normative specification artifacts;
- `docs/` — non-normative architecture, verification, decisions, research, and guides;
- future `crates/` — implementation packages only after package ownership is accepted;
- future `tools/` — repository tooling only;
- future `examples/` — consumer-facing examples and standalone proofs;
- future `conformance/` — executable conformance assets when required by the accepted verification design.

## Extraction boundary

Current Runenwerk networking source is migration evidence, not package authority. Extraction MUST separate reusable networking semantics from engine-specific realization instead of moving the current `net/` tree wholesale.

RunenNet may own transport-independent concerns such as sessions, delivery contracts, protocol identity, replication cursors/baselines, acknowledgements, resynchronization, network-oriented retention, prediction/input protocol contracts, generic interest vocabulary, recovery semantics, and transport interfaces.

RunenNet does not automatically own general simulation, ECS world access, engine schedules/resources, spatial queries, rendering smoothing, gameplay policy, or archival/editor replay.

## Cutover rule

When Runenwerk adopts RunenNet, networking semantic authority moves to this repository. Old Runenwerk semantic crates and duplicate protocol state are removed rather than preserved through forwarding or compatibility layers.
