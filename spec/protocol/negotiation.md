# Protocol and Schema Negotiation

Status: **provisional incomplete normative**

This document owns the initial RunenNet compatibility-offer, exact-contract selection, capability/schema negotiation, and negotiated-contract lifetime semantics. Protocol/schema/codec/capability identities are defined by [Protocol, schema, codec, and capability identity](identity.md).

Session admission and participant binding are defined by [Session and authority lifecycle](../session/lifecycle.md). Pre-admission delivery-flow semantics and resource pressure are defined by [Delivery flow semantics](../delivery/flow.md) and [Delivery pressure and resource policy](../delivery/pressure.md).

This document does not define production wire framing, transport/TLS negotiation, application authentication, game-content compatibility, schema migration, or public API shape.

## Scope

This revision defines:

- finite peer compatibility offers;
- exact common protocol selection;
- required and optional capability negotiation;
- exact schema-contract and codec bindings;
- malformed/incompatible negotiation outcomes;
- establishment and lifetime of one immutable negotiated contract per transport-connection lifetime;
- preconditions for participant admission and schema-dependent payload interpretation.

## Bootstrap negotiation boundary

Compatibility negotiation occurs before RunenNet participant admission in the initial profile.

RN1B permits semantic delivery flows before participant admission. A transport/runtime realization MAY use such pre-admission communication to carry negotiation control data.

The negotiation control representation is **bootstrap control data**. Bootstrap control data MUST be interpretable without relying on a schema or codec that is itself being negotiated by that data.

This revision defines the semantic bootstrap values and bounds but not their production byte encoding. A future wire/transport profile MUST provide a bounded bootstrap representation sufficient to realize this negotiation without circular dependency on an unnegotiated application schema.

Bootstrap negotiation does not create participant membership and does not authorize participant traffic. Successful negotiation is a compatibility prerequisite for later admission, not admission itself.

## Compatibility offer

Each endpoint has one finite **CompatibilityOffer** for the current transport-connection lifetime.

An offer contains:

- one or more supported protocol-contract alternatives `(ProtocolId, ProtocolRevision)`;
- zero or more capability entries;
- zero or more schema entries;
- optional bounded diagnostic labels that do not affect identity or selection.

The concrete data structure/API is not defined by this revision.

An offer is immutable once submitted for one negotiation attempt. Changing supported contracts requires a new negotiation attempt under a new connection or a later renegotiation profile; in-place mutation of the initial offer is not defined here.

## Protocol alternatives

A protocol alternative is exactly one `(ProtocolId, ProtocolRevision)` pair.

The two peers are protocol-compatible only if their offers contain at least one exactly equal protocol alternative.

The authority selects exactly one common protocol alternative for the negotiated contract.

If more than one exact common alternative exists, authority selection policy is implementation/application policy. The selected pair MUST be explicitly represented in the negotiation result; no ordering, “highest version,” or package-version rule is implied.

If no exact common protocol alternative exists, negotiation is **ProtocolIncompatible** and MUST NOT become Established.

## Capability entries

A capability entry contains:

- one CapabilityId supported by the endpoint; and
- one requirement level: **Required** or **Optional**.

An endpoint MUST NOT list the same CapabilityId more than once in one offer. A duplicate capability entry makes the offer Malformed.

For each capability:

- if either endpoint marks the CapabilityId Required, the other endpoint MUST advertise support for the same CapabilityId and the negotiated contract MUST enable it;
- if both endpoints advertise the same CapabilityId as Optional, the authority MAY enable or omit it;
- a CapabilityId advertised by only one endpoint as Optional is omitted;
- a CapabilityId required by one endpoint but unsupported by the other makes negotiation **RequiredCapabilityUnavailable**.

Unknown capability identities are therefore safe only when optional: an endpoint may ignore an unknown Optional capability because it cannot become enabled without mutual support. An unknown Required capability makes negotiation incompatible.

Enabling a capability does not authorize it to weaken or redefine the selected protocol contract or any RunenNet normative semantics.

## Schema entries

A schema entry contains:

- one SchemaId supported by the endpoint;
- requirement level Required or Optional;
- one or more exact schema-contract alternatives for that SchemaId.

Each schema-contract alternative contains:

- one SchemaContractId; and
- one or more supported CodecId values for that exact schema contract.

An offer MUST contain at most one top-level schema entry for a given SchemaId. Multiple exact SchemaContractId alternatives for that SchemaId are represented inside that one entry.

Within one schema entry:

- the same SchemaContractId MUST NOT appear more than once;
- one schema-contract alternative MUST NOT list the same CodecId more than once.

A violation makes the offer Malformed rather than invoking last-wins, first-wins, or declaration-order semantics.

## Exact schema compatibility

For one SchemaId, one exact schema binding is compatible only when both peers support:

- the same SchemaId;
- the same SchemaContractId alternative for that SchemaId; and
- at least one identical CodecId for that exact schema contract.

The authority selects at most one `(SchemaContractId, CodecId)` binding for each SchemaId.

If multiple exact common contracts or codecs exist, the authority MAY select any mutually supported alternative according to local policy. The result MUST explicitly identify the selected SchemaContractId and CodecId.

No compatibility is inferred from:

- SchemaContractId ordering;
- diagnostic names;
- successful decode attempts;
- similar field layouts;
- package versions;
- one endpoint supporting a different “newer” contract.

## Required and optional schema resolution

If either endpoint marks a SchemaId Required, negotiation MUST select one exact common schema binding for that SchemaId.

If no exact common `(SchemaContractId, CodecId)` binding exists for a required SchemaId, negotiation is **RequiredSchemaUnavailable**.

If both endpoints mark a SchemaId Optional:

- the authority MAY select one exact common binding when one exists; or
- omit the SchemaId from the negotiated contract.

If no common exact binding exists for an optional SchemaId, it is omitted and negotiation may otherwise succeed.

A SchemaId advertised by only one endpoint as Optional is omitted.

## Negotiated contract

A successful selection produces one **NegotiatedContract** containing exactly:

- the selected ProtocolId and ProtocolRevision;
- the set of enabled CapabilityId values;
- zero or more selected schema bindings, each mapping one SchemaId to exactly one `(SchemaContractId, CodecId)` pair.

The negotiated contract MUST contain no protocol alternative, capability, schema contract, or codec that was not supported by both endpoint offers.

Every Required capability and Required SchemaId from either offer MUST be satisfied in the negotiated contract.

A negotiated contract MUST NOT contain more than one selected binding for one SchemaId.

The authority's selection is not Established merely because the authority constructed it.

## Mutual validation and establishment

Negotiation becomes **Established** only after both endpoints have validated the same NegotiatedContract against their own immutable offers and the rules above.

The exact message exchange used to prove mutual validation is not defined by this revision. A conforming realization MUST nevertheless ensure that participant admission is not accepted while the endpoints can still disagree about the negotiated contract.

If an endpoint receives a proposed result that contains unsupported, omitted-required, contradictory, duplicated, or otherwise invalid selections, it MUST reject the negotiation rather than attempt best-effort interoperability.

Negotiation failure is observable to the host/runtime inspection boundary.

## Negotiation and session admission

Under the initial profile, RN1A participant admission acceptance requires an Established negotiated contract for the transport connection being admitted.

Authentication, tickets, server selection, matchmaking, application content/build compatibility, and other host policy may independently reject admission. Successful RunenNet compatibility negotiation does not override those policies.

Conversely, application authentication or ticket success MUST NOT cause RunenNet participant admission to bypass required RunenNet negotiation.

On authorized connection replacement for a retained participant, the replacement transport connection MUST establish a new NegotiatedContract before it becomes the participant's authorized replacement binding.

The prior connection's negotiated contract MUST NOT be silently transferred to the replacement connection.

## Negotiated-contract lifetime

One Established NegotiatedContract is immutable for its transport-connection lifetime in the initial profile.

It terminates when that transport connection terminates.

This revision does not define in-place renegotiation, post-admission schema additions, capability upgrades/downgrades, or negotiated-contract transfer across connections.

A later renegotiation profile may add those operations only with explicit transition and stale-message rules.

## Schema-dependent payload interpretation

A typed, replicated, or other schema-dependent payload associated with SchemaId S MUST NOT be interpreted under RunenNet schema semantics unless the current connection's Established negotiated contract contains a selected binding for S.

When S is selected, payload interpretation MUST use exactly the selected SchemaContractId and CodecId for S.

An implementation MUST NOT:

- choose a different locally registered schema revision because decode succeeds;
- fall back to another codec;
- infer schema from a Rust/ECS type name;
- interpret an unselected optional schema;
- treat a human-readable channel/type/component name as the selected schema identity.

A payload referring to an unselected/unknown SchemaId or requiring an unavailable selected binding MUST be rejected before schema-specific decode/application. The later wire/protocol profile may classify the violation more specifically or terminate the connection.

The wire representation may carry SchemaId and may carry the selected contract/codec explicitly or derive them from the Established negotiated binding; that representation choice is outside this revision.

## Codec use

CodecId selects an exact encoding/decoding contract. A payload MUST be decoded only under the CodecId selected for its SchemaId.

An implementation's internal use of postcard, Serde, JSON, Protobuf, Cap'n Proto, custom bit-packing, or another codec does not authorize using that codec for interoperating RunenNet payloads unless the current negotiated binding selects its CodecId.

A future standardized codec profile may assign concrete CodecId values and byte rules.

## Manifest and bootstrap resource policy

Each endpoint MUST enforce explicit finite limits for peer-controlled negotiation state before proportional allocation or registration.

At minimum the initial compatibility policy MUST bound:

- total bootstrap/offer/result bytes or equivalent accountable representation size;
- number of protocol alternatives per offer;
- capability-entry count;
- schema-entry count;
- SchemaContractId alternative count per SchemaId;
- CodecId count per schema-contract alternative;
- total selected schema-binding count;
- total diagnostic-label bytes and each individual diagnostic-label length.

Any additional RunenNet-owned unknown-entry, parsing, staging, retry, or negotiation-state structure influenced by peer input MUST be explicitly finite or covered by an aggregate bound above.

A claimed count/length from a future wire representation MUST be checked against its applicable finite limit before allocating storage proportional to that claim.

Exact numeric defaults are not defined by this revision.

The initial offer structure contains no unbounded opaque extension payload. A later extension mechanism MUST define its own skippability and bounds before unknown extension bodies can be accepted safely.

## Malformed offer outcomes

An offer is Malformed when it violates structural rules including duplicate identities, zero contract alternatives for a schema entry, zero codec alternatives for a schema contract, or configured finite limits.

Malformed peer input MUST NOT be normalized into a valid offer by dropping duplicates, truncating silently, or applying last-wins semantics.

A local application/configuration that attempts to construct such an offer MUST fail before the malformed offer is advertised as a valid compatibility offer.

## Compatibility outcome classes

A conforming implementation MUST make at least these initial negotiation outcomes distinguishable to its conformance/runtime inspection boundary:

- Established;
- MalformedOffer;
- ProtocolIncompatible;
- RequiredCapabilityUnavailable;
- RequiredSchemaUnavailable;
- ResourceLimitExceeded;
- InvalidSelection;
- NegotiationAborted because the underlying connection terminated.

The exact public error/event enum is not defined here.

## Extensions

Applications MAY define their own optional CapabilityId or SchemaId contracts outside the standardized RunenNet set.

Such extensions:

- remain subject to exact identity, negotiation, lifetime, and resource rules here;
- MUST NOT redefine the meaning of standardized RunenNet identities;
- MUST NOT weaken session, delivery, replication, resource, or conformance semantics;
- are not themselves evidence of RunenNet standards conformance.

## Deferred negotiation semantics

This revision does not define:

- protocol revision ranges or ordered version preference;
- schema migration/adaptation between different SchemaContractId values;
- in-place renegotiation;
- dynamic post-admission schema registration;
- codec transcoding during negotiation;
- application/game-content compatibility;
- authentication/authorization policy;
- production bootstrap bytes/framing;
- transport/TLS selectors such as ALPN.

Those features may later extend negotiation, but MUST NOT introduce best-effort decode or silently reinterpret an Established exact binding.