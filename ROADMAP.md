# RunenNet Roadmap

This document owns project sequencing and acceptance gates. It does not define networking semantics or repository dependency law.

## RN0 — Authority foundation

Establish the standalone product boundary, documentation authority, specification conventions, repository architecture, roadmap, and canonical validation contract. No Runenwerk networking implementation code is copied in this stage.

**Gate:** authority is unambiguous; RunenNet independence is explicit; RN1 can specify semantics without deriving them implicitly from existing engine code.

## RN1 — Semantic contracts

Specify the minimum transport-independent model required for implementation: identity, session/authority lifecycle, delivery contracts, replication baseline/acknowledgement/resynchronization semantics, resource-bound invariants, and conformance claims.

**Gate:** implementation-critical invariants have normative owners; unresolved items are explicit rather than filled by implementation convenience.

## RN2 — Minimal semantic core

Implement the accepted RN1 state machines and contracts without Runenwerk, ECS, sockets, or production transport assumptions. Add a deterministic in-memory/fault transport and executable conformance coverage.

**Gate:** accepted semantic behavior, including required correctness and resource invariants, is testable independently of engine and production transport realization.

## RN3 — Standalone proof

Provide a plain-Rust authoritative client/server example using ordinary host-owned state and RunenNet contracts only.

**Gate:** the example requires no Runenwerk, concrete ECS, engine plugin/schedule system, or spatial/rendering framework.

## RN4 — Fault and adversarial hardening

Expand assurance around the accepted core under loss, reordering, duplication, saturation, malformed input, reconnect, retention pressure, and hostile resource claims. Harden implementation limits without postponing correctness rules that belong to RN1/RN2.

**Gate:** conformance and fault tests demonstrate bounded, deterministic recovery behavior across the supported failure model.

## RN5 — QUIC realization

Implement the production QUIC adapter with explicit mapping from RunenNet delivery semantics to QUIC streams/datagrams, plus framing limits, TLS, ALPN, connection limits, and transport backpressure.

**Gate:** QUIC passes the same semantic transport conformance expectations as the deterministic transport where applicable; payload size never silently changes delivery guarantees.

## RN6 — Runenwerk clean cutover

Implement downstream Runenwerk/RunenECS integration, switch Runenwerk to RunenNet authority, and delete the old engine-owned networking semantic crates/state.

**Gate:** dependency direction is one-way from Runenwerk to RunenNet; no forwarding crate, duplicate protocol authority, or compatibility shim remains.

## RN7 — Public framework API

Refine standalone user ergonomics: client/server composition, registration, diagnostics, optional derive/macros where justified, and stable examples/guides.

**Gate:** ordinary framework use does not require expert transport/runtime hooks, while advanced integrations retain explicit escape hatches.

## RN8 — Advanced semantics

Add prediction/reconciliation, interest/relevancy budgeting, recovery/history features, lag compensation, or related capabilities only through separately accepted semantic work.

**Gate:** each feature composes with existing profiles without weakening core semantics or host independence.

## RN9 — Maturity

Add fuzzing, benchmarks, compatibility/version policy, ecosystem hardening, and additional transports only when independent demand demonstrates the need.

**Gate:** pre-1.0 stability criteria are explicit and independently verifiable.

## Sequencing rule

Stages are dependency gates, not a feature checklist. Later work does not bypass unresolved correctness or authority from an earlier gate. Parallel work is acceptable only where ownership and assumptions are independent.
