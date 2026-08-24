# RN1B Delivery and Pressure Evidence

Status: **non-normative**

This record supports [RN1B](https://github.com/dornglut/runen-net/issues/6). It compares pinned Runenwerk migration evidence with transport standards and current framework behavior. It does not define RunenNet semantics.

Evidence snapshot:

- Runenwerk: `37a267e41e49317516d6513b02794f8fc480056a` (observed 2026-08-24)
- QUIC transport: RFC 9000
- QUIC DATAGRAM: RFC 9221
- Quinn: `0.11.11` documentation (observed 2026-08-24)
- Lightyear: `0.29.0` documentation (observed 2026-08-24)
- Renet: `2.0.0` documentation (observed 2026-08-24)

## Current Runenwerk evidence

### Delivery vocabulary exists before the adapter, but does not reach the runtime command boundary

Runenwerk `engine_net` defines `TransportLane` values for reliable, unreliable, unreliable-sequenced, and input-stream traffic. `DeliveryGuarantee` separately defines `ReliableOrdered`, `Unreliable`, `UnreliableSequenced`, and `InputSequenced`, while `LaneSemantics` also carries an `ordered` boolean.

This is useful evidence that delivery intent belongs above the transport, but the current vocabulary mixes generic delivery semantics with application purpose (`InputStream`) and duplicates ordering information.

Sources:

- [Runenwerk lane vocabulary](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/transport/lanes.rs)
- [Runenwerk lane semantics](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/transport/semantics.rs)

`SessionRuntimeCommand` carries a message and destination connection but no delivery mode or semantic flow. Therefore the selected delivery intent is lost before concrete transport sending.

Source: [Runenwerk session runtime command boundary](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/session/ids.rs).

### Delivery must exist before participant admission

Current Runenwerk begins a client session by sending `Hello` and `JoinRequest` while the client has no admitted `ConnectionId`. Only `JoinAccepted` transitions the client to active and records that identity.

That means a generic delivery abstraction cannot require an already-admitted participant membership. Pre-admission control traffic needs delivery semantics over the transport-connection lifetime without granting participant authority.

Source: [Runenwerk client session handoff](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/session/handoff.rs).

### Current QUIC realization changes guarantee according to payload size

`engine_net_quic::send_envelope` serializes an envelope and calls `send_datagram` whenever the encoded payload fits the current QUIC datagram limit. Otherwise it opens a new unidirectional stream.

This means payload size currently selects unreliable datagram versus reliable stream behavior. That is incompatible with RunenNet's repository law that delivery semantics are selected before the transport boundary.

Source: [Runenwerk QUIC message transport](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net_quic/src/runtime/message_transport.rs).

### Current engine queues are bounded, but overflow policy is not delivery-aware

Runenwerk configures network work queues with a capacity of 4096 messages. The generic enqueue helper logs that the newest message is being dropped when the queue reports backpressure.

A single drop-newest policy cannot preserve reliable/control semantics while also supporting lossy real-time traffic. It is useful migration evidence for the need to make pressure behavior part of the semantic delivery contract rather than a generic engine queue side effect.

Source: [Runenwerk engine networking resources](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/engine/src/plugins/net/resources.rs).

## Transport-standard evidence

### QUIC ordering exists within a stream, not across streams

RFC 9000 defines independent QUIC streams. QUIC does not provide a general application message ordering relation across distinct streams. Therefore opening a fresh stream for each reliable message cannot by itself realize one ordered RunenNet delivery domain.

Source: [RFC 9000](https://www.rfc-editor.org/rfc/rfc9000.html).

The transport realization may use one stream, multiple streams plus RunenNet ordering metadata, or another mechanism. The semantic ordering domain must exist independently of that choice.

### QUIC DATAGRAM is explicitly unreliable and may be dropped under congestion or receiver pressure

RFC 9221 defines DATAGRAM frames as unreliable. A receiver may drop them when it cannot commit processing or memory resources. Under congestion, a sender may delay or drop a datagram rather than transmit it.

Source: [RFC 9221](https://www.rfc-editor.org/rfc/rfc9221.html).

This supports treating loss as a permitted outcome of an unreliable delivery mode, while keeping local queue admission and deliberate local eviction explicit so they do not become accidental adapter policy.

### Quinn exposes different local datagram pressure policies

Quinn 0.11.11 documents `send_datagram` as unreliable and unordered and states that older queued unsent datagrams may be discarded to make space. `send_datagram_wait` instead waits for buffer space, effectively preferring older queued datagrams. Quinn also exposes the currently available datagram buffer space and a connection-dependent maximum datagram size.

Source: [Quinn 0.11.11 `Connection`](https://docs.rs/quinn/0.11.11/quinn/struct.Connection.html).

This is strong evidence that RunenNet cannot leave pressure policy implicit in the Quinn call selected by an adapter. The core contract must decide admission/eviction semantics first; the adapter must realize them or report that it cannot.

Quinn also documents that even reliable QUIC stream data cannot be promised to reach the remote application if the connection closes after the remote QUIC stack received it. This argues against defining reliable delivery as an unconditional eventual-delivery promise. Instead, accepted reliable messages need a conditional guarantee while the semantic flow remains operational plus explicit terminal failure when that guarantee can no longer be upheld.

## Framework comparison evidence

### Renet uses unidirectional message channels with explicit memory bounds

Renet 2.0.0 describes channels as unilateral/message-based and offers unreliable, reliable ordered, and reliable unordered send types. `ChannelConfig` contains a maximum memory usage bound; the documented overflow behavior drops new unreliable messages and disconnects on reliable-channel exhaustion.

Sources:

- [Renet 2.0.0 channels](https://docs.rs/renet/2.0.0/renet/struct.ChannelConfig.html)
- [Renet 2.0.0 send types](https://docs.rs/renet/2.0.0/renet/enum.SendType.html)

RunenNet should not copy that policy mechanically, but it is evidence that direction, guarantee, and memory pressure are distinct concerns and that reliable overflow must become an explicit failure/backpressure outcome rather than silent loss.

### Lightyear separates channel guarantee from scheduling pressure choices

Lightyear 0.29.0 `ChannelSettings` separates channel mode from priority, send frequency, and whether locally unsent unreliable messages are retried after bandwidth admission fails. Its current documentation also uses sequenced-unreliable delivery for cases where older received values should be ignored.

Sources:

- [Lightyear 0.29.0 channel settings](https://docs.rs/lightyear/0.29.0/lightyear/prelude/struct.ChannelSettings.html)
- [Lightyear 0.29.0 crate documentation](https://docs.rs/lightyear/0.29.0/lightyear/)

This supports keeping generic delivery guarantees separate from bandwidth priority and from application-specific input/replication policy.

## Resulting design pressure

The evidence supports a minimal RN1B model:

1. A **delivery flow** is a unidirectional semantic message domain established over one transport-connection lifetime. It is not the transport connection itself, a QUIC stream, or a datagram lane.
2. Delivery flows may exist before participant admission. Delivery acceptance/exposure does not grant participant authority; session lifecycle remains the owner of admission and connection binding.
3. A flow's delivery mode is fixed before message submission reaches a transport adapter.
4. The initial core needs three demonstrated modes: **ReliableOrdered**, **UnreliableUnordered**, and **UnreliableSequenced**.
5. Reliable unordered delivery is useful in other frameworks but is not required by current Runenwerk migration evidence for RN2; it can be added later without weakening the three initial modes.
6. `UnreliableSequenced` is receiver-side monotonic exposure: later sequence values may make older arrivals stale. It does not automatically mean sender-side keyed latest-value coalescing.
7. Message submission must distinguish **rejected before acceptance** from **accepted under a mode**. Reliable messages may be rejected under pressure before acceptance, but accepted reliable messages cannot then be silently evicted while the flow is still operational.
8. Reliable delivery must be conditional on an operational flow. Connection/flow failure can leave accepted messages unexposed, but that failure must be explicit rather than represented as successful reliable delivery.
9. Payload size may reject a submission or require same-mode fragmentation/realization; it must never switch the selected delivery mode.
10. Local pressure needs explicit finite message/byte/flow-count bounds at flow and connection scope, plus aggregate bounds that include pre-admission connections. Unreliable queues may deliberately evict only under an explicitly selected lossy policy; reliable queues may not.
11. Receiver pressure follows the same semantic distinction: reliable traffic requires backpressure or explicit terminal failure, while unreliable traffic may be dropped.
12. Priority, send cadence, application-keyed supersession, deadlines, input buffering, replication policy, and reconnect transfer are separate concerns and should remain undefined until independently justified.

## Proposed normative ownership

The evidence supports two one-way owners:

- `spec/delivery/flow.md` — delivery-flow lifetime, submission/acceptance/exposure, and delivery modes;
- `spec/delivery/pressure.md` — finite resource policy and pressure outcomes, depending on the flow semantics.

This dependency direction is acyclic: session lifecycle → delivery flow → delivery pressure. Identity remains a dependency of session lifecycle rather than being redefined by delivery.
