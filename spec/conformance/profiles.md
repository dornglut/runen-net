# RunenNet Conformance Profiles

Status: **provisional normative**

This document owns the initial RunenNet conformance-profile taxonomy, composition, and claim rules. It does not restate networking semantics owned by other specification artifacts.

## Conformance claim

A **RunenNet conformance claim** states:

- the exact RunenNet specification version being claimed; and
- one or more claimable profile names defined by that specification version.

An implementation MUST satisfy every normative rule included by every profile it claims.

The current provisional revision does not define independently versioned profile contracts. A profile name identifies the profile contract defined by the claimed RunenNet specification version.

Future revisions MAY introduce independently versioned profiles only with an explicit compatibility/claim transition. Package/crate SemVer does not substitute for specification/profile identity.

## No partial profile claims

An implementation that implements only part of a profile MUST NOT claim conformance to that profile.

Terms such as “mostly Core,” “Core-compatible,” or “AuthoritativeReplication except recovery” are not standardized RunenNet conformance claims.

An implementation MAY describe partial or experimental support in ordinary documentation provided that it does not present that description as a standardized conformance claim.

## Core profile

**Core** is the base RunenNet conformance profile.

A Core claim includes the normative rules owned by:

- [Specification conventions](../conventions.md);
- [Core identity and time](../core/identity.md);
- [Session and authority lifecycle](../session/lifecycle.md);
- [Delivery flow semantics](../delivery/flow.md);
- [Delivery pressure and resource policy](../delivery/pressure.md);
- [Protocol, schema, codec, and capability identity](../protocol/identity.md);
- [Protocol and schema negotiation](../protocol/negotiation.md).

A Core implementation MUST preserve the dependency and authority boundaries of those specifications; implementing equivalent behavior through a different internal architecture does not permit changing their observable semantics.

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

An AuthoritativeReplication claim includes all Core rules plus the normative rules owned by:

- [Authoritative replication consistency](../replication/consistency.md);
- [Replication retention and full-snapshot recovery](../replication/recovery.md).

An implementation MUST NOT claim AuthoritativeReplication without also satisfying Core.

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

## Profile composition

Profiles compose only according to explicit normative inclusion or interaction rules.

AuthoritativeReplication includes Core by definition.

The absence of a profile for another feature does not authorize an implementation to infer standardized semantics for that feature from analogy, implementation behavior, or external frameworks.

No claimable Prediction, Interest, RecoveryHistory, QUIC, WebTransport, Hosted, Realtime, or similar profile is defined by this revision.

## Conformance is not runtime capability negotiation

Conformance profiles and runtime CapabilityId negotiation are different concepts.

A runtime capability advertisement states what one connection can negotiate under the protocol negotiation specification. It does not by itself constitute a standards conformance claim.

Likewise, an implementation's public conformance claim does not require every possible standardized or application-defined capability to be enabled on every connection.

A standardized capability that later depends on a particular conformance profile MUST define that dependency explicitly.

## Conformance is not wire interoperability

A Core or AuthoritativeReplication conformance claim establishes semantic conformance to the claimed specification/profile. It does not by itself claim byte-level interoperability over a production transport.

This revision does not standardize:

- one production bootstrap wire encoding;
- one envelope framing format;
- one required CodecId;
- QUIC/TLS/ALPN realization;
- another concrete transport profile.

Two conforming implementations require an exact mutually supported protocol/schema/codec contract under the negotiation rules before they may interpret each other's schema-dependent payloads.

A future wire/transport profile may add a stronger interoperability claim without redefining Core or AuthoritativeReplication semantics.

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

No Runenwerk, RunenECS, Quinn, Tokio, Serde, or postcard dependency is implied by a conformance claim.

## Claim documentation

A published standardized conformance claim MUST identify the claimed RunenNet specification version and profile names unambiguously.

For this revision, examples of structurally valid claim descriptions are:

- `RunenNet 0.1-provisional — Core`;
- `RunenNet 0.1-provisional — Core + AuthoritativeReplication`.

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