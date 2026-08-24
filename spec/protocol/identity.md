# Protocol, Schema, Codec, and Capability Identity

Status: **provisional incomplete normative**

This document owns the identity vocabulary used by the initial RunenNet compatibility model. It defines identity domains and their non-equivalence. It does not define negotiation, wire encoding, schema evolution adapters, application content compatibility, or conformance claims.

## Scope

This revision defines:

- RunenNet specification version as distinct from protocol/package identity;
- protocol family and exact protocol-contract identity;
- schema family and exact schema-contract identity;
- codec identity;
- capability identity;
- the non-authoritative role of human-readable names and host-language type identities.

Concrete integer widths, UUID formats, hash algorithms, byte encodings, registry file formats, crate type names, and public API representations are not defined by this revision.

## Specification version

A **RunenNet specification version** identifies one revision of the normative RunenNet specification.

The specification version is documentation/conformance identity. It MUST NOT by itself be interpreted as:

- a wire-protocol identifier;
- a schema identifier;
- a codec identifier;
- a crate/package version;
- proof that two peers can interoperate.

The current specification version is declared by the specification index.

## Package version

A **package version** identifies an implementation or distribution release such as a crate SemVer version.

Package versions are implementation/distribution metadata. Two implementations with different package versions MAY implement the same RunenNet protocol/schema contracts, and two packages with similar or ordered versions are not thereby protocol-compatible.

A conforming implementation MUST NOT infer peer protocol, schema, or codec compatibility solely from package-version equality, ordering, or SemVer compatibility.

## Protocol identity

A **ProtocolId** identifies one logical RunenNet protocol family.

A **ProtocolRevision** identifies one exact immutable protocol contract within one ProtocolId family.

ProtocolRevision is opaque and non-ordered for compatibility purposes. Numeric or lexical ordering of its concrete representation, if any, MUST NOT imply backwards, forwards, or mutual compatibility.

Changing the logical protocol contract in a way that requires different peer interpretation requires a distinct ProtocolRevision unless another normative specification explicitly defines compatibility between those contracts.

Changing only a separately negotiated SchemaContractId, CodecId, application content contract, or optional CapabilityId does **not** by itself require changing ProtocolRevision when the logical protocol contract remains unchanged. Those identity domains evolve independently.

Conversely, changing ProtocolRevision MUST NOT silently change the meaning of an existing SchemaId, SchemaContractId, CodecId, or CapabilityId. Any cross-domain dependency must be stated explicitly by the normative contract that introduces it.

The initial compatibility model recognizes interoperability only through an exact common ProtocolId and ProtocolRevision selected by negotiation.

This revision does not assign a concrete wire representation or globally reserved value to either identity.

## Protocol identity is not application content identity

ProtocolId/ProtocolRevision identify RunenNet protocol interpretation. They do not identify:

- a game build;
- downloadable content;
- a map/content pack;
- a matchmaking queue;
- an application build number;
- an application-specific save/content schema;
- a server instance.

A host MAY impose such application compatibility policy before admission, but that policy is not RunenNet protocol identity unless a later specification explicitly defines a RunenNet semantic contract for it.

## Schema identity

A **SchemaId** identifies one stable semantic data-schema family.

A SchemaId is protocol identity, not a display name. Its meaning MUST NOT depend on:

- a Rust type name or `TypeId`;
- an ECS component/resource name or ID;
- a module/package path;
- a source-language fully-qualified name;
- a typed-message channel string;
- declaration order;
- process-local allocation.

Renaming or relocating a host-language type MUST NOT require changing its SchemaId when the intended semantic schema family remains the same.

A SchemaId MAY have a bounded human-readable diagnostic label, but that label is not used to establish schema equality or compatibility.

## Exact schema contract identity

A **SchemaContractId** identifies one exact immutable semantic contract within a SchemaId family.

The pair `(SchemaId, SchemaContractId)` identifies one exact schema contract in the initial compatibility model.

SchemaContractId is opaque and non-ordered. Greater, newer-looking, or SemVer-like representations MUST NOT be interpreted as compatible with another SchemaContractId unless a later normative schema-evolution contract explicitly defines that relation.

A semantic schema change that can alter conforming interpretation requires a distinct SchemaContractId unless compatibility for that change is explicitly standardized elsewhere.

The initial RunenNet core does not define a globally meaningful integer `SchemaVersion`, schema-version range negotiation, or automatic schema migration.

## Schema contract assignment

RunenNet does not define an IDL, canonical schema-description language, hash algorithm, or automatic derivation algorithm in this revision.

A SchemaContractId MAY be explicitly assigned by schema tooling or derived from a collision-resistant canonical contract representation outside this revision. Whatever mechanism an implementation uses:

- the mapping from `(SchemaId, SchemaContractId)` to semantic schema contract MUST be stable for the lifetime of data/protocol interoperability that relies on it;
- the same pair MUST NOT intentionally identify two different semantic contracts;
- detecting a local duplicate registration of the same pair with contradictory contract metadata MUST be treated as a registration/configuration defect, not resolved by last-wins behavior.

A future schema/IDL profile may standardize canonical fingerprints without changing the identity separation defined here.

## Codec identity

A **CodecId** identifies one exact encoding/decoding contract for transforming a negotiated semantic payload to and from bytes or another transportable representation.

CodecId is opaque and non-ordered. A change that alters conforming encoded interpretation requires a distinct CodecId unless a later codec specification explicitly defines compatibility.

CodecId is distinct from SchemaId and SchemaContractId:

- one exact schema contract MAY be supported by multiple codecs;
- one codec MAY encode multiple schemas if that codec's contract permits it;
- schema equality does not imply codec equality;
- codec equality does not imply schema equality.

Postcard, Serde, Rust enum layout, a host ABI, and an ECS storage representation are not implicitly RunenNet codecs merely because an implementation uses them internally.

A codec becomes interoperable RunenNet protocol identity only when represented by an explicitly agreed CodecId under the negotiation rules.

## Capability identity

A **CapabilityId** identifies one immutable semantic capability contract that peers may advertise for compatibility negotiation.

CapabilityId is distinct from:

- a conformance profile claim;
- a crate feature flag;
- a cargo feature;
- a transport-native extension identifier;
- a human-readable feature name.

The initial revision does not define an ordered CapabilityVersion. A semantic change that would make two implementations interpret the capability differently requires a distinct CapabilityId unless a later normative compatibility mechanism is defined.

A CapabilityId MAY have a bounded diagnostic label. The label is not identity.

## Required and optional use are not identity

Whether a protocol/schema/capability is **required** or **optional** in one compatibility offer is a negotiation property. It does not change the identity or meaning of that ProtocolRevision, SchemaContractId, CodecId, or CapabilityId.

## Identity comparison

Identity equality in this specification means exact equality within the same identity domain.

An implementation MUST NOT establish equality by:

- case-folding or normalizing human-readable labels;
- comparing host-language type names;
- package-version proximity;
- numeric revision ordering;
- matching payload layouts heuristically;
- successful best-effort decode.

If two peers do not have an exact common identity required by the negotiation specification, compatibility fails rather than being inferred from resemblance.

## Deferred identity mechanisms

This revision does not define:

- canonical numeric widths or binary encodings for IDs;
- central/global registries;
- UUID/hash generation algorithms;
- automatic schema fingerprints;
- schema migration identity;
- compatibility ranges between ProtocolRevision values or SchemaContractId values;
- application/game-content version identity;
- transport-specific protocol selectors such as QUIC/TLS ALPN values.

Those mechanisms may later realize or extend the identity domains here, but MUST NOT collapse the domains or make human/implementation names authoritative.