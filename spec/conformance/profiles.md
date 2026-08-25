# RunenNet Conformance Profiles

Status: **provisional normative**

This document owns the initial RunenNet conformance-profile taxonomy, composition, and claim rules. It does not restate networking semantics owned by other specification artifacts.

All normative artifacts included by a profile are interpreted according to [Specification conventions](../conventions.md). The conventions document governs specification interpretation/authority; it is not itself an implementation feature.

## Conformance claim

A **RunenNet conformance claim** states:

- the exact RunenNet specification version being claimed; and
- one or more claimable profile names defined by that specification version.

An implementation MUST satisfy every normative requirement addressed to conforming implementations by every profile it claims.

The current provisional revision does not define independently versioned profile contracts. A profile name identifies the profile contract defined by the claimed RunenNet specification version.

Future revisions MAY introduce independently versioned profiles only with an explicit compatibility/claim transition. Package/crate SemVer does not substitute for specification/profile identity.

## No partial profile claims

An implementation that implements only part of a profile's defined normative requirements MUST NOT claim conformance to that profile.

Terms such as “mostly Core,” “Core-compatible,” or “AuthoritativeReplication except recovery” are not standardized RunenNet conformance claims.

An implementation MAY describe partial or experimental support in ordinary documentation provided that it does not present that description as a standardized conformance claim.

## Provisional and incomplete specification scope

A conformance claim to this provisional specification covers only the normative requirements actually defined by the claimed specification revision and profiles.

If an included normative artifact is marked **incomplete**, its already-defined rules remain binding, but semantic items explicitly left open remain undefined according to [Specification conventions](../conventions.md).

A conforming implementation MUST NOT present its implementation choice for an open specification item as standardized RunenNet behavior merely because it satisfies the currently defined profile requirements.

A provisional conformance claim therefore does not imply that the profile is feature-complete or stable across future specification revisions. Wire interoperability is claimed only where an explicitly claimed wire/transport profile defines it.

## Core profile

**Core** is the base RunenNet conformance profile.

A Core claim includes the normative requirements addressed to conforming implementations by:

- [Core identity and time](../core/identity.md);
- [Session and authority lifecycle](../session/lifecycle.md);
- [Delivery flow semantics](../delivery/flow.md);
- [Delivery pressure and resource policy](../delivery/pressure.md);
- [Protocol, schema, codec, and capability identity](../protocol/identity.md);
- [Protocol and schema negotiation](../protocol/negotiation.md).

A Core implementation MUST preserve the authority boundaries and observable semantics of those specifications; implementing equivalent behavior through a different internal architecture does not permit changing their normative behavior.

### Core is freestanding

Claiming Core alone MUST NOT require:

- Runenwerk or another game/application engine;
- an ECS;
- a renderer or spatial framework;
- a filesystem;
- a real network interface or socket API;
- a particular operating system;
- threads;
- an async executor;
- Quinn/QUIC, UDP, TCP, WebTransport, or another production transport;
- a GPU or presentation system.

A Core implementation MAY use any of those facilities internally or through adapters. The claim means none of them is a semantic prerequisite for implementing and testing the Core contract.

An in-memory or deterministic fault transport can therefore be sufficient to exercise Core semantics.

## AuthoritativeReplication profile

**AuthoritativeReplication** extends Core with the initial single-authority replication consistency and recovery contract.

An AuthoritativeReplication claim includes all Core rules plus the normative requirements addressed to conforming implementations by:

- [Authoritative replication consistency](../replication/consistency.md);
- [Replication retention and full-snapshot recovery](../replication/recovery.md).

An implementation MUST NOT claim AuthoritativeReplication without satisfying Core.

AuthoritativeReplication does not require:

- an ECS or component storage model;
- a particular state-image representation;
- prediction or rollback;
- interpolation/presentation smoothing;
- interest/relevancy or spatial streaming;
- lag compensation;
- archival replay/history;
- connection recovery that preserves delta eligibility;
- a standardized production transport or codec.

Those concerns are outside the current profile unless a later normative profile explicitly adds them.

## QUIC profile

**QUIC** extends Core with the standardized RunenNet QUIC wire/transport realization.

A QUIC claim includes all Core rules plus the normative requirements addressed to conforming implementations by:

- [QUIC transport profile](../transport/quic.md).

An implementation MUST NOT claim QUIC without satisfying Core.

Requirements addressed specifically to the QUIC client or QUIC server apply to an implementation when it provides that endpoint role. A QUIC claim does not require one implementation unit to provide both roles; every role it does provide MUST satisfy all profile requirements addressed to that role.

A QUIC claim states that the implementation can establish and operate the QUIC wire profile defined by the claimed RunenNet specification revision for its implemented endpoint role(s), including its ALPN/bootstrap, compatibility-negotiation representation, delivery-flow realization, framing, resource, and failure rules.

A QUIC claim does not by itself claim AuthoritativeReplication. An implementation that satisfies both profiles MAY claim both `QUIC` and `AuthoritativeReplication`; neither profile includes the other beyond their common Core dependency.

The QUIC profile standardizes transport/bootstrap interoperability. Schema-dependent application payload interoperability still requires the exact mutually supported protocol/schema/codec contract required by the Core negotiation rules.

A QUIC claim does not require Quinn, Tokio, rustls, or another particular implementation library/runtime.

## Profile composition

Profiles compose only according to explicit normative inclusion or interaction rules.

AuthoritativeReplication includes Core by definition.

QUIC includes Core by definition.

QUIC and AuthoritativeReplication are orthogonal extensions of Core and MAY be claimed together. Neither claim weakens or replaces requirements of the other.

The absence of a profile for another feature does not authorize an implementation to infer standardized semantics for that feature from analogy, implementation behavior, or external frameworks.

No claimable Prediction, Interest, RecoveryHistory, WebTransport, Hosted, Realtime, or similar profile is defined by this revision.

## Profile requirements cannot be negotiated away

Runtime compatibility negotiation selects mutually supported contracts for one connection. It MUST NOT weaken a conformance profile's normative requirements.

If a standardized protocol/capability/schema identity is required to realize a claimed profile over an interoperating connection, the normative owner introducing that identity must define the requirement explicitly, and negotiation must satisfy it according to the protocol negotiation specification.

Marking such an identity Optional in a local offer does not change the profile requirement.

Core and AuthoritativeReplication do not standardize one required production CodecId or transport realization. QUIC standardizes its transport/bootstrap realization but does not thereby assign an application CodecId or replace application protocol/schema negotiation.

## Conformance is not runtime capability negotiation

Conformance profiles and runtime CapabilityId negotiation are different concepts.

A runtime capability advertisement states what one connection can negotiate under the protocol negotiation specification. It does not by itself constitute a standards conformance claim.

Likewise, an implementation's public conformance claim does not require every possible standardized or application-defined capability to be enabled on every connection.

A standardized capability that later depends on a particular conformance profile MUST define that dependency explicitly.

## Wire interoperability

A Core or AuthoritativeReplication conformance claim establishes semantic conformance to the claimed specification/profile. Neither claim by itself asserts byte-level interoperability over a production transport.

A QUIC claim adds the concrete bootstrap/control and delivery transport interoperability defined by the QUIC transport profile. It does not make arbitrary application payload bytes interoperable without the exact common application protocol/schema/codec contract required by negotiation.

A conforming QUIC client and conforming QUIC server can therefore establish the standardized RunenNet QUIC profile and perform the standardized compatibility bootstrap when their deployment/TLS policy permits the connection. They may interpret schema-dependent application payloads only after establishing the exact mutually supported negotiated contract required by Core.

Another future wire/transport profile may add a different production realization without redefining Core, AuthoritativeReplication, or the existing QUIC claim.

## Extensions

An implementation MAY provide application or vendor extensions.

Extensions MUST NOT:

- silently weaken or redefine rules included by a claimed profile;
- use implementation-specific behavior to fill a specification item explicitly left undefined;
- present an application CapabilityId or SchemaId as a standardized RunenNet identity unless that identity is actually standardized;
- treat package-version equality as proof of conformance or protocol compatibility.

If an extension conflicts with a claimed profile, the implementation is non-conforming for that claim.

## Claims across implementation architecture

Conformance is determined by the normative behavior of the claimed profile, not by crate topology or internal module names.

An implementation may be:

- a standalone Rust library;
- a component of another engine/application;
- implemented in another programming language;
- realized with static buffers or dynamic allocation;
- driven synchronously or asynchronously;

provided every rule of the claimed profile is preserved.

No Runenwerk, RunenECS, Quinn, Tokio, Serde, or postcard dependency is implied by a Core or AuthoritativeReplication claim. A QUIC claim requires QUIC behavior as specified by the QUIC profile but still does not require a particular QUIC library or executor.

## Claim documentation

A published standardized conformance claim MUST identify the claimed RunenNet specification version and profile names unambiguously.

For this revision, examples of structurally valid claim descriptions are:

- `RunenNet 0.1-provisional — Core`;
- `RunenNet 0.1-provisional — AuthoritativeReplication` (which includes Core);
- `RunenNet 0.1-provisional — QUIC` (which includes Core);
- `RunenNet 0.1-provisional — QUIC + AuthoritativeReplication`.

Listing Core in addition to a profile that includes Core is permitted but redundant.

A QUIC implementation SHOULD document whether it provides the client role, server role, or both. Endpoint-role support does not create a separate conformance-profile identity.

These examples define claim shape only; they are not evidence that any implementation has passed conformance.

An implementation SHOULD document the validation/conformance evidence used to support its claim, but the artifact format for such evidence is not defined by this revision.

## Future profiles

Future standardized profiles may add transport realizations, prediction, interest, schema evolution, security hardening, or other facilities only after their normative semantics have canonical owners.

A future profile MUST identify:

- the base profile(s) it requires;
- the normative owners it adds;
- any explicit interaction rules with existing profiles;
- its claim/version identity under the specification's then-current claim model.

New profiles MUST NOT retroactively change the meaning of an existing profile claim without an explicit specification revision.
