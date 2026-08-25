# QUIC Transport Profile

Status: **provisional normative**

This document owns the RunenNet QUIC wire profile: transport-profile identity, bounded bootstrap encoding, QUIC delivery-flow realization, connection-scoped wire flow identity, framing, and QUIC-specific failure/resource rules. It does not redefine the transport-independent semantics owned by [Core identity and time](../core/identity.md), [Session and authority lifecycle](../session/lifecycle.md), [Delivery flow semantics](../delivery/flow.md), [Delivery pressure and resource policy](../delivery/pressure.md), [Protocol, schema, codec, and capability identity](../protocol/identity.md), or [Protocol and schema negotiation](../protocol/negotiation.md).

The profile is defined against QUIC version 1 (RFC 9000), QUIC-TLS (RFC 9001), and QUIC DATAGRAM (RFC 9221). QUIC version 2 (RFC 9369) is an optional additional transport version under the rules below. Those RFCs define QUIC/TLS mechanics; this document owns their RunenNet profile mapping.

## Scope

This revision defines:

- the initial RunenNet QUIC wire revision and ALPN protocol identifier;
- required QUIC transport-version and DATAGRAM support;
- a bounded connection-control stream and bootstrap negotiation encoding;
- concrete wire encodings for bootstrap protocol/schema/capability identities;
- connection-scoped delivery-flow identifiers and establishment;
- `ReliableOrdered` realization over persistent QUIC streams;
- `UnreliableUnordered` and `UnreliableSequenced` realization over QUIC DATAGRAM;
- bounded framing and pre-allocation length validation;
- transport/native-buffer obligations required to preserve RunenNet pressure semantics;
- TLS, 0-RTT, close, replacement, and QUIC-specific error boundaries.

This revision does not define application authentication, game/content compatibility, public API shape, executor choice, certificate issuance infrastructure, congestion-control policy, bandwidth priority, unreliable fragmentation/reassembly, prediction, interest/relevancy, or advanced reconnect/history continuity.

## Profile identity

The initial RunenNet QUIC wire revision is **1**.

A revision-1 connection MUST negotiate the exact ALPN protocol identifier:

```text
runennet/1
```

The ALPN value identifies this QUIC wire profile only. It is not a RunenNet specification version, `ProtocolId`, `ProtocolRevision`, `SchemaId`, `SchemaContractId`, `CodecId`, `CapabilityId`, game/content version, package version, SessionId, or ParticipantId.

An incompatible change to the transport bootstrap, control framing, flow wire representation, or delivery realization requires a distinct future ALPN value. A package release or application protocol revision does not by itself change the ALPN value.

### QUIC transport versions

A conforming implementation of this profile MUST support QUIC version 1 (`0x00000001`). This provides one common transport version for every implementation claiming the profile.

An implementation MAY additionally support QUIC version 2 (`0x6b3343cf`) with the same `runennet/1` ALPN value only when the RunenNet wire behavior defined here remains byte-for-byte and semantically identical above QUIC. Supporting QUIC version 2 does not create a different RunenNet profile or application protocol identity.

This revision does not permit an implementation to claim the profile while supporting QUIC version 2 but not version 1.

### QUIC DATAGRAM requirement

A profile connection MUST support QUIC DATAGRAM according to RFC 9221 in both directions. An endpoint MUST NOT declare the RunenNet QUIC profile ready when local DATAGRAM receive support is disabled or the peer did not advertise DATAGRAM support.

A connection without mutually usable QUIC DATAGRAM support MAY remain a valid QUIC connection for another application protocol, but it is not a usable `runennet/1` profile connection and MUST fail RunenNet profile bootstrap before any RunenNet delivery flow accepts a message.

## Layer and identity boundaries

QUIC connection IDs, stream IDs, packet numbers, TLS identities, socket addresses, and path identifiers are transport-native values. They MUST NOT be used as SessionId, ParticipantId, NetworkEntityId, RunenNet delivery FlowId, or application protocol/schema identity.

The QUIC client/server side determines only wire namespaces defined by this document. It MUST NOT be used to infer which endpoint is the RunenNet semantic authority. Authority role is host-supplied semantic state and is explicitly checked during profile bootstrap.

Application protocol/schema negotiation occurs inside the authenticated `runennet/1` connection after transport-profile bootstrap. The ALPN value MUST NOT be substituted for the exact `ProtocolId`/`ProtocolRevision` and schema/codec negotiation required by the Core specification.

## Common wire encoding

Unless this document defines a fixed-width field, non-negative integers are encoded as QUIC variable-length integers using the encoding from RFC 9000 Section 16.

A sender MUST use the shortest valid QUIC variable-length encoding for a value. A receiver MUST reject a non-minimal encoding as a profile protocol error. No profile integer may exceed the QUIC variable-length integer range.

The following identity domains use exactly 16 octets in network byte order, most-significant octet first:

- `ProtocolId`;
- `ProtocolRevision`;
- `SchemaId`;
- `SchemaContractId`;
- `CodecId`;
- `CapabilityId`.

The 16-octet representation is a wire representation of the existing opaque identity value. It does not make an identity ordered for compatibility or interchangeable with another identity domain.

Requirement levels use one octet:

- `0` — Optional;
- `1` — Required.

Any other requirement-level value is malformed bootstrap data.

Diagnostic labels from `CompatibilityOffer` are not transmitted by wire revision 1. They remain local diagnostic metadata and do not affect identity or selection. A received wire offer is therefore the peer's semantic offer projected onto the identity/requirement fields defined below, with no peer diagnostic label.

## Connection control stream

After the QUIC handshake is confirmed and `runennet/1` has been authenticated by ALPN, the QUIC client MUST open exactly one bidirectional QUIC stream as the RunenNet connection-control stream and MUST write its first control frame without waiting for server data.

The QUIC server MUST permit exactly one client-initiated bidirectional stream for `runennet/1` application use and MUST treat that stream as the connection-control stream. The QUIC client MUST permit zero server-initiated bidirectional application streams for this profile. No other bidirectional application stream is defined by wire revision 1.

A local attempt to open an additional bidirectional application stream under `runennet/1`, or receipt of one where the negotiated QUIC transport limits nevertheless permit it, is a profile protocol error.

The control stream remains connection-scoped. Its loss, reset, malformed framing, or premature end terminates the RunenNet profile connection and therefore every delivery flow on that connection.

### Control-frame framing

Each control frame is encoded as:

```text
frame_type: varint
body_length: varint
body: body_length octets
```

Before allocating storage proportional to `body_length`, the receiver MUST validate it against its locally selected finite `max_control_frame_bytes`. The local limit MUST exist before the first peer frame is parsed and MUST NOT be derived from a peer claim.

A frame body MUST be consumed exactly according to its frame definition; trailing or missing octets are a profile protocol error. Unknown frame types are profile protocol errors in wire revision 1. Extension framing that can be ignored safely is not defined by this revision; an incompatible required extension uses a future ALPN wire revision instead.

The initial frame types are:

| Type | Frame |
| ---: | --- |
| 0 | `SETTINGS` |
| 1 | `NEGOTIATION_OFFER` |
| 2 | `NEGOTIATION_PROPOSAL` |
| 3 | `NEGOTIATION_VALIDATED` |
| 4 | `NEGOTIATION_ESTABLISHED` |
| 5 | `NEGOTIATION_FAILED` |
| 6 | `OPEN_FLOW` |
| 7 | `FLOW_ACCEPT` |
| 8 | `FLOW_REJECT` |
| 9 | `FLOW_TERMINATE` |

## Profile settings and readiness

`SETTINGS` is the first control frame sent by each endpoint in its direction of the control stream.

Its body contains, in order:

```text
semantic_role: u8
max_control_frame_bytes: varint
max_negotiation_frame_bytes: varint
max_active_incoming_flows: varint
max_incoming_message_bytes: varint
```

`semantic_role` is:

- `0` — NonAuthority;
- `1` — Authority.

Every maximum MUST be non-zero and finite. `max_control_frame_bytes` MUST be large enough to parse the largest valid wire-revision-1 `SETTINGS` body, and `max_negotiation_frame_bytes` MUST be large enough to encode at least one protocol alternative with zero capabilities and zero schemas. `max_negotiation_frame_bytes` MUST NOT exceed `max_control_frame_bytes`.

Each value advertises the sender's own local receive/resource ceiling. Peer input MUST NOT raise that local ceiling or choose the sender's pressure behavior. A sender of later frames MUST respect the peer-advertised ceiling in addition to its own local limits.

In particular, an endpoint MUST NOT:

- send a control frame whose body exceeds the peer-advertised `max_control_frame_bytes`;
- send a negotiation frame whose body exceeds the peer-advertised `max_negotiation_frame_bytes`;
- request a new outgoing delivery flow when doing so would exceed the peer-advertised `max_active_incoming_flows` for currently active flows initiated by that endpoint;
- request `max_message_bytes` greater than the peer-advertised `max_incoming_message_bytes`.

These advertised ceilings do not guarantee later acceptance: receiver-local connection/session/aggregate pressure may still require a bounded `FLOW_REJECT`.

Exactly one endpoint MUST advertise Authority and exactly one MUST advertise NonAuthority. Each endpoint MUST compare the received role with its host-supplied expectation. A role disagreement is a profile protocol error; transport client/server side is not a substitute for this check.

For one endpoint, the connection becomes **ProfileReady** only after:

- the QUIC handshake is confirmed;
- ALPN is exactly `runennet/1`;
- mutually usable RFC 9221 DATAGRAM support is established;
- the endpoint has sent its valid `SETTINGS` frame; and
- the endpoint has received and validated the peer's single `SETTINGS` frame.

Before that endpoint is ProfileReady it MUST NOT send or process any non-`SETTINGS` RunenNet control frame. A duplicate or later `SETTINGS` frame is a profile protocol error. No delivery flow may be established before compatibility negotiation becomes Established.

## Bounded compatibility-negotiation wire representation

The negotiation state machine and compatibility rules remain owned by [Protocol and schema negotiation](../protocol/negotiation.md). This section defines only the production bootstrap byte representation and exchange required to realize that state machine without an unnegotiated application codec.

Each endpoint sends exactly one immutable `NEGOTIATION_OFFER` after it becomes ProfileReady. A receiver MUST validate both the control-frame body length and all semantic offer limits before allocating or retaining peer-controlled collections.

A `NEGOTIATION_OFFER` body is:

```text
protocol_count: varint
protocols[protocol_count]:
    protocol_id: 16 octets
    protocol_revision: 16 octets
capability_count: varint
capabilities[capability_count]:
    capability_id: 16 octets
    requirement_level: u8
schema_count: varint
schemas[schema_count]:
    schema_id: 16 octets
    requirement_level: u8
    contract_count: varint
    contracts[contract_count]:
        schema_contract_id: 16 octets
        codec_count: varint
        codecs[codec_count]: 16 octets each
```

The body MUST NOT exceed either endpoint's applicable `max_negotiation_frame_bytes`. Counts MUST be checked against the receiver's finite negotiation policy before collection allocation. Structural duplicates and empty collections are classified by the semantic negotiation owner and MUST NOT be normalized by the wire decoder.

The Authority endpoint selects the proposed contract only after both valid offers are available and after validating the proposal against its own immutable offer, the peer offer, and applicable profile requirements.

`NEGOTIATION_PROPOSAL` is sent only by the Authority and contains:

```text
protocol_id: 16 octets
protocol_revision: 16 octets
enabled_capability_count: varint
enabled_capabilities[enabled_capability_count]: 16 octets each
selected_schema_count: varint
selected_schemas[selected_schema_count]:
    schema_id: 16 octets
    schema_contract_id: 16 octets
    codec_id: 16 octets
```

Sending the proposal asserts that the Authority has locally validated that exact contract. The NonAuthority MUST validate the exact received contract against its immutable offer and the normative negotiation rules. If validation succeeds, it sends an empty `NEGOTIATION_VALIDATED` frame.

Only the NonAuthority may send `NEGOTIATION_VALIDATED`, and only for the single proposal it actually received and validated. The Authority considers negotiation Established only after receiving that frame for its proposal. It then sends an empty `NEGOTIATION_ESTABLISHED` frame. Only the Authority may send `NEGOTIATION_ESTABLISHED`. The NonAuthority considers negotiation Established only after receiving that final frame. This final acknowledgement realizes the Core requirement that participant admission cannot proceed while the endpoints can still disagree about whether mutual validation completed.

Duplicate, repeated, out-of-order, wrong-role, or state-inapplicable negotiation frames are profile protocol errors; wire revision 1 defines exactly one negotiation attempt per connection.

A negotiation failure SHOULD be reported with `NEGOTIATION_FAILED` before connection termination when the control stream remains usable. Its body is exactly:

```text
outcome: varint
```

Initial outcome values are:

| Code | Outcome |
| ---: | --- |
| 0 | MalformedOffer |
| 1 | ProtocolIncompatible |
| 2 | RequiredCapabilityUnavailable |
| 3 | RequiredSchemaUnavailable |
| 4 | ResourceLimitExceeded |
| 5 | InvalidSelection |

Any other value is a profile protocol error. After sending `NEGOTIATION_FAILED`, the sender terminates the profile connection with the `NEGOTIATION_FAILED` application error code. If the failure frame cannot be delivered, the remote endpoint may observe the semantic negotiation as aborted by connection termination.

No production `CodecId` is used to encode these bootstrap frames. The bootstrap representation exists specifically to avoid circular dependence on a codec that is still being negotiated.

## Delivery-flow wire identity

A **FlowId** is a QUIC variable-length integer scoped to exactly one `runennet/1` connection lifetime.

FlowId values are independent of QUIC stream IDs and every RunenNet session/participant/entity identity.

The low bit identifies the QUIC side that initiates and sends the unidirectional delivery flow:

- `0` — QUIC client initiated;
- `1` — QUIC server initiated.

For each side, local flow sequence starts at zero. The wire FlowId is:

```text
FlowId = (local_flow_sequence << 1) | side_bit
```

Each side MUST use its next sequence value exactly once and then increment it by one. FlowId values MUST NOT be reused or wrapped within a connection. A receiver can therefore validate the exact next FlowId for each side without retaining an unbounded set of retired identifiers.

Exhausting the representable local sequence ends the ability to establish another flow on that connection; wrap or reuse is a profile protocol error.

### Delivery-mode encoding

`OPEN_FLOW` uses these exact mode values:

| Value | Delivery mode |
| ---: | --- |
| 0 | `ReliableOrdered` |
| 1 | `UnreliableUnordered` |
| 2 | `UnreliableSequenced` |

No other delivery mode is defined by wire revision 1.

## Flow establishment

A sender MUST attach its own finite local flow resource/pressure policy before requesting establishment. Peer input does not select that policy.

`OPEN_FLOW` contains:

```text
flow_id: varint
delivery_mode: varint
max_message_bytes: varint
```

`max_message_bytes` is the sender's requested stable payload-size contract for the flow and MUST be non-zero. It MUST NOT exceed either the sender's applicable local flow maximum or the peer-advertised `max_incoming_message_bytes`.

For an unreliable mode, the sender MUST additionally ensure at establishment that the requested payload maximum plus the largest possible wire-revision-1 DATAGRAM envelope for that flow fits the currently usable QUIC application-datagram size. For `UnreliableSequenced`, that envelope calculation MUST allow the sequence field to grow to its largest valid encoding during the flow lifetime.

The receiver validates:

- that `flow_id` is the exact next identifier for the initiating side;
- that the mode is defined by this profile;
- that accepting the flow would remain within its local active-flow and aggregate bounds;
- that its own local receive policy can support the requested `max_message_bytes` without weakening that policy.

An invalid FlowId or undefined delivery-mode value is a profile protocol error rather than a negotiable rejection.

If all checks succeed, the receiver attaches its own finite receive pressure policy for the flow and sends `FLOW_ACCEPT` containing only `flow_id`. The requested `max_message_bytes` then becomes the stable profile payload ceiling for that flow lifetime at both endpoints; each endpoint's separately owned local resource bounds may still be stricter for admission at a given moment.

If the flow cannot be accepted, the receiver sends `FLOW_REJECT`:

```text
flow_id: varint
reason: varint
```

Initial rejection reasons are:

| Code | Reason |
| ---: | --- |
| 0 | ResourceLimit |
| 1 | MessageLimit |

The sender MUST NOT report the flow established or accept a message on it before receiving `FLOW_ACCEPT`. Once a syntactically valid `OPEN_FLOW` with the exact next FlowId is processed, that FlowId is consumed whether the receiver accepts or rejects the flow; both endpoints advance the corresponding next-flow sequence and MUST NOT retry by reusing the identifier.

A peer MUST NOT create receive-flow state from an unknown DATAGRAM or an unexpected QUIC stream. Flow state is created only by a valid `OPEN_FLOW` processed under local bounds.

A duplicate, unknown, wrong-side, or state-inapplicable `FLOW_ACCEPT`, `FLOW_REJECT`, or `FLOW_TERMINATE` control frame is a profile protocol error. Wire revision 1 does not define retransmission of control frames at the application layer because the control stream is reliable.

## ReliableOrdered realization

Each accepted `ReliableOrdered` flow maps to exactly one persistent unidirectional QUIC stream initiated by the flow sender.

The sender opens that stream only after `FLOW_ACCEPT`. The stream begins with the flow's `flow_id` encoded as a minimal QUIC variable-length integer. The receiver MUST reject an unknown, wrong-side, non-reliable, or duplicate stream association as a profile protocol error.

After the FlowId header, the stream contains zero or more message frames:

```text
payload_length: varint
payload: payload_length octets
```

`payload_length` counts only the opaque RunenNet delivery payload. Before allocating storage proportional to it, the receiver MUST validate it against the established flow `max_message_bytes` and every applicable local aggregate bound.

One complete framed payload maps to exactly one RunenNet delivery message. QUIC stream chunks are never partial RunenNet exposure.

The sender writes accepted messages to this stream in the flow's acceptance order. Opening one stream per message is not a conforming mapping for a single `ReliableOrdered` flow.

### Reliable custody and backpressure

Semantic message acceptance may occur only when the adapter has admitted the whole message under the selected RunenNet pressure policy and either:

- retains bounded local custody until reliable QUIC-stream custody can be transferred; or
- has transferred custody to a bounded reliable QUIC-stream stage that cannot intentionally discard the bytes while the flow remains operational.

QUIC flow control, stream send capacity, or executor scheduling MAY delay custody transfer. They MUST NOT cause an accepted reliable message to be discarded or moved to an unreliable mechanism. If the obligation cannot be preserved, the realization terminates the flow with the observable terminal reliable-failure outcome owned by the delivery specification.

A clean FIN received exactly on a message boundary terminates the sender-owned reliable flow after all preceding complete messages have been processed. A reset, stop, malformed/truncated frame, length violation, or connection loss terminates the flow; accepted but unexposed messages retain the failure semantics owned by the delivery specification.

The profile never selects DATAGRAM for a `ReliableOrdered` message.

## Unreliable DATAGRAM realization

Wire revision 1 does not fragment or reassemble an unreliable RunenNet message across multiple QUIC DATAGRAM frames. One accepted unreliable message is carried by at most one QUIC DATAGRAM application payload.

An `UnreliableUnordered` DATAGRAM payload is:

```text
flow_id: varint
payload: remaining octets
```

An `UnreliableSequenced` DATAGRAM payload is:

```text
flow_id: varint
sequence: varint
payload: remaining octets
```

The receiver determines the mode from the already-established FlowId. An unknown or terminated FlowId is discarded without creating flow state. A DATAGRAM naming a `ReliableOrdered` flow is a profile protocol error.

### Unreliable sequence values

The first accepted message on an `UnreliableSequenced` flow has sequence value zero. Each later accepted message consumes exactly the next integer. Rejected submissions do not consume a value.

Sequence values MUST NOT wrap. After the largest representable QUIC variable-length integer has been consumed, the flow cannot accept another message and MUST be terminated before another sequenced flow is used.

The receiver applies the stale/duplicate exposure rule owned by the delivery-flow specification to the decoded sequence value.

### DATAGRAM size and changing path limits

Before semantic acceptance of an unreliable submission, the sender MUST verify that the encoded profile DATAGRAM payload fits the currently usable QUIC application-datagram size as well as the flow's stable `max_message_bytes`.

If it does not fit, the submission is rejected before acceptance. Wire revision 1 provides no reliable-stream fallback and no multi-DATAGRAM fragmentation for that submission.

A later path-MTU reduction may make a previously acceptable datagram size temporarily unsendable. When that inability is known before semantic acceptance, the new submission MUST be rejected. If an already accepted unreliable message is subsequently lost because the unreliable QUIC realization can no longer transmit or deliver it, non-exposure remains permitted by the selected unreliable mode; an observable transport-drop diagnostic SHOULD be recorded when the implementation can distinguish the event.

A later path-MTU increase does not raise the flow's stable `max_message_bytes`.

## Unreliable pressure and native QUIC queues

QUIC DATAGRAM network/transport loss remains permitted by the two unreliable delivery modes. Deliberate local RunenNet/adapter pressure remains governed by the pressure policy attached to each flow.

An adapter MUST NOT rely on a transport-native queue's implicit eviction policy to implement `RejectNew`, `EvictOldestUnreliable`, `DropIncomingUnreliable`, or `EvictOldestBufferedUnreliable`.

In particular, a native queue shared by multiple flows MUST NOT be used in a way that intentionally evicts an older datagram from another flow merely because a new datagram is submitted. Adapter-owned admission and buffering MUST preserve the selected per-flow policy and applicable connection/session/aggregate bounds before handoff to a transport-native DATAGRAM queue.

A native DATAGRAM staging buffer MAY exist as a bounded transport stage, but its capacity and handoff behavior MUST be configured or wrapped so the implementation does not intentionally use hidden native eviction as RunenNet pressure policy. A dedicated receive/drain stage MAY treat loss below the RunenNet admission boundary as intrinsic unreliable transport loss, but once a complete datagram has entered RunenNet-owned receive custody, deliberate local drops and evictions are subject to the selected receiver pressure behavior and required observability.

## Flow termination

`FLOW_TERMINATE` contains:

```text
flow_id: varint
reason: varint
```

Initial termination reasons are:

| Code | Reason |
| ---: | --- |
| 0 | Normal |
| 1 | ResourceFailure |
| 2 | ProtocolFailure |
| 3 | ReliableDeliveryFailure |

For an unreliable flow, processing `FLOW_TERMINATE` ends the flow immediately; later DATAGRAMs for that FlowId are stale and discarded without state creation.

For a reliable flow, a normal sender close is represented by clean FIN on the persistent stream at a message boundary. `FLOW_TERMINATE` is used when either endpoint must report exceptional termination. A receiver-originated exceptional termination MUST cause the sender to cease writes and terminate/reset the corresponding reliable stream. Stream reset/stop details may use the application error codes below but MUST preserve the observable RunenNet terminal-failure class.

Flow termination never authorizes FlowId reuse.

## QUIC resource realization

Every implementation MUST configure explicit finite transport/adapter bounds consistent with the delivery and negotiation resource policies. At minimum this includes:

- concurrent transport connections per endpoint or higher-level host aggregate;
- bidirectional stream limits consistent with exactly one client-initiated control stream and no server-initiated bidirectional application stream;
- finite incoming/outgoing unidirectional stream limits compatible with the implementation's bounded set of accepted active reliable flows;
- per-stream receive windows;
- connection receive and send windows;
- reliable adapter custody/staging queues;
- QUIC DATAGRAM send and receive staging buffers;
- active FlowId registries;
- control/negotiation parsing state;
- reliable message framing/reassembly storage.

QUIC stream limits are transport resource controls. They MUST NOT be derived by equating QUIC streams with RunenNet sessions or participants.

An implementation MUST NOT accept a reliable flow if its configured stream/resource relationship can make that flow permanently incapable of obtaining its one persistent stream while the flow is otherwise considered operational. Temporary QUIC flow-control or stream-capacity blocking is permitted only when bounded custody/backpressure preserves the accepted delivery contract.

A peer-advertised stream count, flow count, frame length, message length, or DATAGRAM capability MUST NOT allocate proportional local state before the applicable local finite bound is checked.

The exact numeric production defaults are implementation policy, provided they satisfy this profile and the transport-independent resource owners. Conformance does not require one universal memory budget.

## TLS, trust, and 0-RTT

The profile uses the TLS 1.3 integration required by the negotiated QUIC transport version.

A client MUST authenticate the intended server according to an explicit host trust policy. Certificate issuance, CA choice, pinning, server-name policy, client certificates, and application account authentication are host/deployment concerns unless separately standardized. TLS peer identity MUST NOT be reused implicitly as RunenNet participant or application protocol identity.

RunenNet profile control frames, negotiation bootstrap data, and delivery-flow data MUST NOT be sent or accepted as QUIC 0-RTT application data in wire revision 1.

TLS session resumption MAY be used only when no RunenNet profile/application data is accepted before handshake confirmation and authenticated ALPN selection. A future replay-safe early-data profile requires separate normative authority.

## Application and stream error codes

Wire revision 1 reserves these QUIC application error codes for RunenNet profile termination/reset reporting:

| Code | Name |
| ---: | --- |
| 0 | `NO_ERROR` |
| 1 | `PROFILE_PROTOCOL_ERROR` |
| 2 | `CONTROL_FRAME_ERROR` |
| 3 | `RESOURCE_LIMIT_ERROR` |
| 4 | `NEGOTIATION_FAILED` |
| 5 | `FLOW_PROTOCOL_ERROR` |
| 6 | `RELIABLE_DELIVERY_FAILED` |

An implementation MAY include a bounded diagnostic reason phrase when the QUIC API permits one. Diagnostic text is not semantic identity and MUST NOT be parsed as a protocol contract.

A protocol error in connection-level bootstrap/control state terminates the profile connection. A failure isolated to one established delivery flow SHOULD terminate only that flow when the QUIC realization can do so without ambiguity or semantic leakage; connection termination remains permitted when safe isolation is impossible.

## Connection close and replacement

QUIC connection close or loss terminates the control stream, negotiation contract, and every delivery flow on that connection.

A replacement QUIC connection MUST repeat ALPN authentication, profile settings, DATAGRAM support checks, compatibility negotiation, and delivery-flow establishment from fresh connection-scoped state.

No prior FlowId, reliable stream, unreliable sequence, accepted-message custody, negotiation proposal, Established contract, QUIC stream ID, or native transport queue state transfers implicitly to the replacement connection. Any higher-level participant retention or replication recovery proceeds only through the separately owned session/replication semantics after the new connection satisfies their prerequisites.

## Conformance boundary

The `QUIC` conformance profile is defined by [Conformance profiles](../conformance/profiles.md). A `QUIC` claim proves this wire/transport realization in addition to Core; it does not claim any particular QUIC library, async executor, certificate provider, application `CodecId`, or engine integration.

A `QUIC` implementation may also claim `AuthoritativeReplication`. The transport profile carries those higher-level payloads without redefining replication semantics.
