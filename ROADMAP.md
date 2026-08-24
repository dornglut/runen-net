# RunenNet Roadmap

This document owns project sequencing and acceptance gates. It does not define networking semantics or repository dependency law.

## RN0 — Authority foundation

Establish the standalone product boundary, documentation authority, specification conventions, repository architecture, roadmap, and canonical validation contract. No Runenwerk networking implementation code is copied in this stage.

**Gate:** authority is unambiguous; RunenNet independence is explicit; RN1 can specify semantics without deriving them implicitly from existing engine code.

## RN1 — Semantic contracts

Specify the minimum transport-independent model required for implementation: identity, session/authority lifecycle, delivery contracts, replication baseline/acknowledgement/resynchronization semantics, resource-bound invariants, protocol/schema compatibility, and conformance claims.

**Gate:** implementation-critical invariants have normative owners; unresolved items are explicit rather than filled by implementation convenience.

## RN2 — Minimal semantic core

Implement the accepted RN1 state machines and contracts without Runenwerk, ECS, sockets, or production transport assumptions. Add a deterministic in-memory/fault transport and executable conformance coverage.

**Gate:** accepted semantic behavior, including required correctness and resource invariants, is testable independently of engine and production transport realization.

## RN3 — Standalone proof

Provide a plain-Rust authoritative client/server example using ordinary host-owned state and RunenNet contracts only.

**Gate:** the example requires no Runenwerk, concrete ECS, engine plugin/schedule system, or spatial/rendering framework.

## RN4 — Fault and adversarial hardening

Expand assurance around the accepted core under loss, reordering, duplication, saturation, malformed input, connection loss/replacement, retention pressure, and hostile resource claims. Harden implementation limits without postponing correctness rules that belong to RN1/RN2.

This stage hardens the conservative connection-replacement/recovery semantics already accepted by RN1. Advanced reconnect/history continuity that avoids a fresh baseline is not implied here and remains migration/extension work unless separately specified.

**Gate:** conformance and fault tests demonstrate bounded, deterministic recovery behavior across the supported failure model.

## RN5 — QUIC realization

Implement the production QUIC adapter with explicit mapping from RunenNet delivery semantics to QUIC streams/datagrams, plus framing limits, TLS, ALPN, connection limits, and transport backpressure.

**Gate:** QUIC passes the same semantic transport conformance expectations as the deterministic transport where applicable; payload size never silently changes delivery guarantees.

## RN6 — Public standalone framework surface

Refine the engine-independent user API around the proven core and production transport: client/server composition, protocol/schema registration, diagnostics, optional derive/macros where justified, and stable standalone examples/guides.

**Gate:** ordinary standalone use is coherent and documented without Runenwerk-specific concepts, while advanced integrations retain explicit host/transport escape hatches.

## RN7 — Migration semantic closure

Audit the networking behavior still relied on by Runenwerk after the standalone core is established and close every semantic capability required for a clean cutover through independent RunenNet authority. Expected candidates include prediction/reconciliation, interest/relevancy budgeting, advanced reconnect/history recovery, and related diagnostics, but the accepted migration inventory determines the actual scope.

**Gate:** every Runenwerk networking capability that must survive cutover either has an accepted RunenNet semantic owner and implementation or is explicitly retired by accepted Runenwerk product authority; no required behavior depends on keeping the old semantic core alive.

## RN8 — Runenwerk clean cutover

Implement downstream Runenwerk/RunenECS integration against the accepted standalone framework surface and migration-closed semantics, switch Runenwerk to RunenNet authority, and delete the old engine-owned networking semantic crates/state.

**Gate:** dependency direction is one-way from Runenwerk to RunenNet; the engine integration does not redefine framework semantics; no forwarding crate, duplicate protocol authority, compatibility shim, or required legacy semantic implementation remains.

## RN9 — Maturity and post-cutover extensions

Add fuzzing, benchmarks, compatibility/version policy, ecosystem hardening, lag compensation or other new semantics, and additional transports only through separately accepted work justified by independent framework demand.

**Gate:** pre-1.0 stability criteria are explicit and independently verifiable; extensions compose without weakening accepted core semantics or host independence.

## Sequencing rule

Stages are dependency gates, not a feature checklist. Later work does not bypass unresolved correctness or authority from an earlier gate. Parallel work is acceptable only where ownership and assumptions are independent.
