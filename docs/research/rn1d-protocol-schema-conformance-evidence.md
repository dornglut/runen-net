# RN1D Protocol, Schema, and Conformance Evidence

Status: **non-normative**

This record supports [RN1D](https://github.com/dornglut/runen-net/issues/10). It compares pinned Runenwerk migration evidence, accepted RunenNet semantics, the `dornglut/runen` conformance precedent, and selected external schema systems. It does not define RunenNet semantics.

Evidence snapshot:

- Runenwerk: `37a267e41e49317516d6513b02794f8fc480056a` (observed 2026-08-24)
- RunenNet RN1A–RN1C: accepted specification on the RN1D base revision
- `dornglut/runen` conformance profile model: observed 2026-08-24
- Cap'n Proto schema language: observed 2026-08-24
- Protocol Buffers Editions language/evolution guidance: observed 2026-08-24

## Current Runenwerk evidence

### One version object mixes unrelated compatibility domains

Runenwerk `ProtocolVersion` contains three integers:

- `protocol_version`;
- `game_content_version`;
- `schema_version`.

Compatibility is exact equality of all three fields.

Source: [Runenwerk protocol version](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/protocol/version.rs).

This conflates at least three independent concerns:

- the logical networking protocol contract;
- application/game content policy;
- data-schema compatibility.

A standalone framework should not make game-content revision part of RunenNet protocol identity, and one global schema integer cannot safely identify independently evolving payload contracts.

Crate/package SemVer is a fourth independent version domain and is not represented by this struct. It also must not become wire compatibility authority implicitly.

### Admission exact-matches the composite version

`Hello` and `JoinRequest` both carry the composite `ProtocolVersion`. Server admission rejects a join when `request.protocol.is_compatible_with(state.config.protocol)` returns false. The same admission path also handles `server_id` and tickets.

Sources:

- [Runenwerk control protocol](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/protocol/control.rs)
- [Runenwerk admission](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/session/admission.rs)

This is useful evidence that compatibility must be established before ordinary admitted traffic, but the current fields do not provide a clean standalone compatibility contract. Authentication tickets, server selection, lobby state, and content compatibility remain application policy outside the RunenNet core.

### Typed payload identity is stringly and codec identity is implicit

`TypedPayloadMessage` carries:

- `channel: String`;
- `type_name: String`;
- `schema_version: u16`;
- opaque payload bytes.

Its helper decodes the bytes directly as an arbitrary requested Rust/Serde type. There is no stable schema-family identity, exact contract identity, codec identity, or prior negotiated binding.

Source: [Runenwerk message envelope](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/protocol/envelope.rs).

Human-readable names are useful diagnostics, but names can be renamed, reorganized, collide, or differ across language bindings. They are not an adequate protocol identity boundary.

### Replicated component payloads have the same identity problem

Runenwerk replication payload entries identify component state by `component_name: String` plus opaque payload bytes. The replication registry is also keyed by component-name strings and is coupled to ECS metadata/macros.

Sources:

- [Runenwerk snapshot protocol](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/protocol/snapshot.rs)
- [Runenwerk replication registration](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/replication/registration.rs)

RN1C deliberately defines replication state images without choosing ECS/component encoding. RN1D therefore needs a host-neutral schema identity vocabulary that later wire/adapter work can use without making `component_name` or Rust type identity normative.

### Postcard and Rust enum layout are implementation details today

Runenwerk's envelope/payload helpers call postcard directly over Rust `Serialize`/`Deserialize` types, and protocol tests assert postcard round trips.

Source: [Runenwerk protocol module tests](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/protocol/mod.rs).

This is an implementation fact, not a suitable standalone specification. A future wire profile may standardize postcard or another encoding, but RN1D should identify the selected codec explicitly and keep the logical protocol independent from Rust enum representation.

## External schema-system comparison

### Stable numeric identities survive symbolic renames better than names

Cap'n Proto assigns unique IDs to schema files/types and documents that explicit IDs allow declarations to be renamed or moved while retaining protocol identity. Its documentation also notes collision and wire-size problems with symbolic global names.

Source: [Cap'n Proto schema language](https://capnproto.org/language.html).

RunenNet should not copy Cap'n Proto's 64-bit format or ID generation algorithm, but this supports a stable opaque `SchemaId` independent from diagnostic names and host-language organization.

### Compatibility is governed by explicit schema rules, not package version ordering

Protocol Buffers assigns stable field numbers that must not be reused or changed once in use. Its documented wire compatibility depends on explicit evolution rules; a package/runtime version number does not itself prove that two schemas are safely interchangeable.

Source: [Protocol Buffers Editions language guide](https://protobuf.dev/programming-guides/editions/).

RunenNet does not yet define an IDL or schema-evolution algebra. Therefore automatically treating revision N+1 as compatible with N would invent semantics without evidence. The conservative initial contract should negotiate one exact common schema contract and defer adapters/ranges until such rules are explicitly standardized.

## Runen family conformance precedent

`dornglut/runen/spec/conformance/profiles.md` separates profile claims from implementation details, requires every rule included by a claimed profile, prohibits extensions from silently weakening a profile, and keeps Core freestanding from hosted facilities such as networking/threads/filesystems.

Source: [Runen conformance profiles](https://github.com/dornglut/runen/blob/main/spec/conformance/profiles.md).

RunenNet should use the same governance discipline without copying Runen's language-specific profile taxonomy. A RunenNet Core claim should remain independent of sockets, an OS, an async executor, Quinn, an ECS, or an engine. Authoritative Replication can then compose on top of Core.

## Resulting design pressure

The evidence supports the following minimal RN1D direction:

1. **Specification version**, crate/package SemVer, protocol identity, schema identity, codec identity, negotiated capabilities, and application content compatibility are separate domains.
2. `ProtocolId` names one logical protocol family. `ProtocolRevision` names one exact immutable protocol contract; the initial model does not infer compatibility from ordering.
3. `SchemaId` names one stable semantic schema family independently from a Rust type/component/channel name.
4. One exact immutable `SchemaContractId` identifies a specific schema contract inside a SchemaId family. It is opaque and non-ordered; changing semantic contract requires a distinct contract identity.
5. The initial core does not need a separate globally meaningful integer `SchemaVersion`. Human/tooling version labels may exist outside protocol authority.
6. `CodecId` identifies the exact encoding/decoding contract used for a negotiated schema binding. Codec identity is explicit before decode.
7. `CapabilityId` identifies one semantic optional/required facility. Changes that alter its contract require a distinct capability identity or later explicit revision mechanism; integer ordering is not compatibility.
8. Human-readable labels are optional bounded diagnostics only. They never substitute for protocol/schema/capability identity.
9. Initial negotiation uses finite offers/manifests. A peer may advertise multiple exact protocol or schema contracts, but compatibility means selecting one exact common contract rather than applying implicit ranges.
10. For a selected SchemaId, negotiation establishes exactly one `(SchemaContractId, CodecId)` binding before any payload using that schema is interpreted.
11. Required capabilities/schemas must resolve; unknown or unsupported required entries make negotiation incompatible. Optional entries may be omitted from the negotiated result.
12. Duplicate exact manifest entries and contradictory entries are rejected rather than resolved by file/order/last-wins behavior.
13. Protocol/capability/schema negotiation is a bounded pre-admission control operation in the initial profile. Its semantic bootstrap representation is distinct from negotiated application schemas; the future wire profile owns its bytes/framing.
14. Application/game-content compatibility remains host admission policy. RunenNet may carry opaque application admission data later but does not define its semantics.
15. Postcard is not normative. A future codec/wire profile may choose it explicitly.
16. Initial conformance profiles should be only **Core** and **AuthoritativeReplication**. Prediction, interest, QUIC, advanced reconnect/history, and other profiles remain undefined until their semantics exist.
17. A conformance claim names the exact RunenNet specification version plus claimed profile names. This provisional revision does not introduce independently versioned profiles; each profile contract is the one defined by the claimed specification revision.
18. Runtime capability negotiation is not itself a conformance claim. Extensions cannot weaken or redefine rules included by a claimed profile.

## Proposed normative ownership

The evidence supports three one-way owners:

- `spec/protocol/identity.md` — non-equivalent protocol/schema/codec/capability/version identities;
- `spec/protocol/negotiation.md` — bounded exact-contract offers, selection, schema bindings, and failure/admission rules; depends on protocol identity plus existing session/delivery semantics;
- `spec/conformance/profiles.md` — Core and AuthoritativeReplication profile composition and claim rules; depends on the accepted semantic owners and does not restate them.

The intended dependency graph remains acyclic. Existing identity/session/delivery/replication semantics do not depend on protocol schema mechanics; protocol negotiation constrains when those semantics may be used over interoperating peers.