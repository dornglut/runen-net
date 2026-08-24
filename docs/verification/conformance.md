# RN2 Conformance Evidence

This document is **non-normative verification evidence**. The RunenNet specification remains the sole semantic authority.

## Evaluated claim

**RunenNet 0.1-provisional — AuthoritativeReplication**

`AuthoritativeReplication` includes `Core` by definition. The claim shape and profile composition are owned by the normative [conformance profile specification](../../spec/conformance/profiles.md).

This is a provisional semantic conformance claim for the requirements defined by the current specification revision. It is not a claim of production wire interoperability, API stability, or completeness of semantic areas the specification explicitly leaves undefined.

## Evidence map

| Normative owner | RN2 evidence |
| --- | --- |
| [Core identity and time](../../spec/core/identity.md) | Executable identity/non-reuse evidence in [`identity.rs`](../../crates/runen-net/src/identity.rs); session-scoped use is composed by [`rn2d_profiles.rs`](../../crates/runen-net/tests/rn2d_profiles.rs). |
| [Session and authority lifecycle](../../spec/session/lifecycle.md) | Executable lifecycle/retention/replacement evidence in [`session.rs`](../../crates/runen-net/src/session.rs); negotiation and delivery lifetime composition is exercised by [`rn2d_profiles.rs`](../../crates/runen-net/tests/rn2d_profiles.rs). |
| [Delivery flow semantics](../../spec/delivery/flow.md) | Mode/order/sequence/custody/fault evidence in [`rn2b_delivery.rs`](../../crates/runen-net/tests/rn2b_delivery.rs), with shared bounded fault support in [`tests/support`](../../crates/runen-net/tests/support/mod.rs); profile composition is exercised by [`rn2d_profiles.rs`](../../crates/runen-net/tests/rn2d_profiles.rs). |
| [Delivery pressure and resource policy](../../spec/delivery/pressure.md) | Flow/connection/session/aggregate pressure evidence in [`rn2b_delivery.rs`](../../crates/runen-net/tests/rn2b_delivery.rs) and [`rn2b_pressure_edges.rs`](../../crates/runen-net/tests/rn2b_pressure_edges.rs). |
| [Protocol, schema, codec, and capability identity](../../spec/protocol/identity.md) | Registration and exact-identity evidence in [`protocol.rs`](../../crates/runen-net/src/protocol.rs); exact selected schema/codec identity is inspected in [`rn2d_profiles.rs`](../../crates/runen-net/tests/rn2d_profiles.rs). |
| [Protocol and schema negotiation](../../spec/protocol/negotiation.md) | Offer bounds, exact selection, mutual validation, imposed requirements, connection lifetime, and termination evidence in [`protocol.rs`](../../crates/runen-net/src/protocol.rs); admission/binding composition is exercised by [`rn2d_profiles.rs`](../../crates/runen-net/tests/rn2d_profiles.rs). |
| [Authoritative replication consistency](../../spec/replication/consistency.md) | Exact-base reconstruction, atomic complete-target commit, emission/ACK classification, and lineage evidence in [`rn2c_replication.rs`](../../crates/runen-net/tests/rn2c_replication.rs) and [`rn2c_edges.rs`](../../crates/runen-net/tests/rn2c_edges.rs); delivery/session composition is exercised by [`rn2d_profiles.rs`](../../crates/runen-net/tests/rn2d_profiles.rs). |
| [Replication retention and full-snapshot recovery](../../spec/replication/recovery.md) | Bounded retention, recovery barriers/generations, eviction, replacement, and resource evidence in [`rn2c_replication.rs`](../../crates/runen-net/tests/rn2c_replication.rs) and [`rn2c_edges.rs`](../../crates/runen-net/tests/rn2c_edges.rs); current-generation recovery composition is exercised by [`rn2d_profiles.rs`](../../crates/runen-net/tests/rn2d_profiles.rs). |

Repository independence is structural evidence: [`crates/runen-net/Cargo.toml`](../../crates/runen-net/Cargo.toml) remains the single product-crate manifest and the canonical [`cargo validate`](../../TESTING.md) gate checks the complete workspace without requiring Runenwerk, an ECS, sockets, an async executor, or a production transport.

## Explicit RN2 nonclaims

The current specification leaves production realization mechanisms open or assigns them to later work. RN2 therefore does not claim standardized behavior for:

- concrete protocol/semantic ID wire widths or encodings;
- production bootstrap bytes or framing;
- production inbound claimed-length parsing/allocation before a wire parser exists;
- wire delivery-flow identifiers, packetization, fragmentation, or transport framing;
- one standardized byte codec or CodecId implementation;
- QUIC, TLS, ALPN, sockets, or another production transport mapping;
- prediction, interest/relevancy, lag compensation, archival replay, or advanced reconnect continuity;
- public Rust API stability.

Absence of those mechanisms is not filled by RN2 test-fixture behavior. The shared deterministic fault stage is repository test support only and does not define a production transport contract.

## Validation boundary

The evidence above is accepted only on a repository head that passes the canonical validation command documented in [`TESTING.md`](../../TESTING.md). Tests are evidence for normative owners; they do not become specification authority themselves.
