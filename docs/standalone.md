# Standalone RunenNet client/server guide

This is a non-normative usage guide for the public standalone RunenNet API. Networking semantics are owned by the [RunenNet specification](../spec/README.md), including [delivery flows](../spec/delivery/flow.md), [protocol negotiation](../spec/protocol/negotiation.md), and the [QUIC transport profile](../spec/transport/quic.md).

For a complete loopback program, see the [public QUIC example](../crates/runen-net-quic/examples/standalone.rs). The lower-level [Core-only authoritative counter](../crates/runen-net/examples/authoritative_counter.rs) remains the transport-independent example.

## Ownership model

RunenNet has two public layers:

- `runen-net` is the transport-independent Core. The application owns Core identity, negotiation, delivery, session, replication, and resource-policy state.
- `runen-net-quic` is the production QUIC adapter. It realizes the accepted QUIC profile without replacing Core authority.

The application owns `NegotiationManager` and `DeliveryEndpoint`. A public QUIC `Connection` borrows them only during synchronous commands or `poll` calls; it does not retain them in an async task, mutex, or command queue.

`ConnectionHandle` and `DeliveryFlowKey` are host/Core identities. Wire flow identifiers and Quinn objects are intentionally not part of the ordinary public API.

## 1. Configure finite resources and TLS explicitly

Create and validate `EndpointResourceLimits`, then derive a `ProfileConfig` from explicit `ProfileLimits`. Reliable receive staging is supplied separately through `ReliableReceiveLimits` when a ProfileReady connection is activated.

`ClientEndpoint` owns client-side transport setup and trust material. `ServerEndpoint` owns server-side transport setup and server identity material. The example generates a self-signed certificate only to make a single-process loopback program executable; production applications should provide their own certificate lifecycle and trust policy.

These resource numbers, timeouts, addresses, certificate choices, and pressure policies are application/demo policy. They are not normative RunenNet defaults.

## 2. Bootstrap transport asynchronously

`ClientEndpoint::connect` and `ServerEndpoint::accept` are async transport-bootstrap operations. They establish the invariant-preserving QUIC/TLS/ALPN/profile state and return `ProfileReadyConnection` values.

Async bootstrap is deliberately separated from Core progression: it does not hold mutable `NegotiationManager` or `DeliveryEndpoint` borrows across `.await`.

Semantic `Authority` is independent of QUIC client/server side. The loopback example assigns `SemanticRole::Authority` to the QUIC server, but a conforming application may put semantic Authority on the client side instead when its product policy requires that.

## 3. Declare compatibility with explicit stable identities

The host supplies protocol, capability, schema, contract, and codec identities. Do not derive them from Rust type names, layout, registration order, or process-local state. Identity and negotiation semantics are defined by the [protocol specification](../spec/protocol/identity.md).

RN6 declaration ergonomics build the existing Core values without adding a runtime registry or a second validation authority. For example:

```rust
let offer = CompatibilityOffer::builder()
    .protocol(ProtocolId::new(1), ProtocolRevision::new(1))
    .build();
```

The revision-1 QUIC profile does not encode `CompatibilityOffer` diagnostic labels, so an offer submitted through `runen-net-quic` must omit them. Keep any local diagnostic metadata outside the submitted compatibility offer.

For schemas, compose `SchemaOffer::builder(...)` and `SchemaContractOffer::builder(...)` with explicit `RequirementLevel`, contract IDs, and codec IDs. The builders preserve the declared values and ordering. Existing `CompatibilityOffer::validate` / `NegotiationManager::validate_offer` remain the validation and accounting paths.

## 4. Activate one durable public connection

The application activates each `ProfileReadyConnection` with:

- a host-supplied `ConnectionHandle`;
- its `CompatibilityOffer`;
- `NegotiationRequirements`;
- explicit `ReliableReceiveLimits`;
- a synchronous mutable borrow of its `NegotiationManager`.

Activation returns one move-owned `runen_net_quic::Connection`. That same public owner spans compatibility negotiation and established delivery; applications do not switch to a second public flow/runtime object after negotiation.

## 5. Drive with explicit polling and host decisions

After ProfileReady bootstrap, the primary driving boundary is:

```rust
connection.poll(cx, &mut negotiation_manager, &mut delivery_endpoint)
```

The application decides where that poll runs: a standalone executor loop, a custom scheduler, or an engine scheduler can all provide the `Context`. RunenNet does not require a hidden connection task.

When polling yields `ConnectionEvent::AuthoritySelectionRequired`, semantic Authority inspects the Core negotiation state and explicitly supplies a `NegotiatedContract` through `select_authority`. RunenNet does not choose a “best” proposal automatically.

When polling yields `ConnectionEvent::IncomingFlowRequested`, the host explicitly accepts or rejects the move-only request. Acceptance supplies the host-selected inbound `DeliveryFlowKey` and Core resource policy. This is application policy, not transport policy.

## 6. Open and use Core-keyed flows

Outbound flows are opened with `OutboundFlowConfig`, which includes:

- an outbound `DeliveryFlowKey`;
- a fixed `DeliveryMode`;
- `FlowResourcePolicy` and `DeliveryScopeLimits`.

The outbound message-size authority is specified once through `FlowResourcePolicy::max_message_bytes`. For the revision-1 QUIC facade, the adapter derives the peer-visible stable `OPEN_FLOW.max_message_bytes` contract from that Core policy value. The adapter still validates the derived contract against peer/profile and DATAGRAM constraints before establishment; callers do not maintain a second synchronized message-size field.

For incoming flows, the peer has already requested its stable maximum. The receiving host may choose a local `FlowResourcePolicy::max_message_bytes` that is larger than that requested contract; admission fails only when the peer request exceeds the local policy ceiling.

The adapter never silently changes reliable/unreliable semantics because of payload size, transport pressure, or DATAGRAM availability. See the normative [delivery](../spec/delivery/flow.md) and [pressure](../spec/delivery/pressure.md) specifications for the accepted behavior.

Use the same public `Connection::submit` operation for reliable and unreliable flows. Submission preserves the different accepted handoff laws internally while keeping the public flow identity Core-keyed.

## 7. Receive data from Core, not from a QUIC message queue

`ConnectionEvent::DataReady` tells the application which inbound `DeliveryFlowKey` has observable data. The event does not own or duplicate the payload.

Read the payload from the application-owned Core endpoint:

```rust
if let Some(message) = delivery.poll_exposure(inbound_key)? {
    let payload = message.payload();
    // application handling
}
```

This keeps received payload custody in `DeliveryEndpoint` for every transport realization.

## 8. Finish flows and tear down explicitly

`finish_outbound_flow_normal` requests the accepted normal sender finish for a Core-keyed flow. The QUIC adapter preserves the mode-specific realization: reliable normal finish uses its reliable FIN/ack path, while unreliable normal termination uses the accepted profile path. Applications observe durable termination through `ConnectionEvent::FlowTerminated`.

Connection teardown is consuming and explicit:

```rust
let teardown = connection.teardown(&mut negotiation_manager, &mut delivery_endpoint);
```

Teardown releases connection-local adapter ownership and returns Core flow-termination/cleanup evidence. Connection loss or teardown does **not** decide whether a higher-level session should be retained, removed, or replaced; that remains host/Core policy under the [session lifecycle specification](../spec/session/lifecycle.md).

Close the owning endpoint and await `wait_idle` when the application is finished with its QUIC endpoint.

## Advanced integration boundary

Advanced consumers do not need a separate networking stack. They can combine:

- direct `runen-net` Core APIs;
- explicit `Context/Poll` driving of the public QUIC connection;
- synchronous host-policy commands;
- application-owned session/world/scheduler state.

Raw mutable Quinn access is not the ordinary escape hatch because bypassing the accepted stream/DATAGRAM/control owners could violate the QUIC profile. If an integration requires behavior beyond the public boundary, treat it as a separately reviewed framework requirement rather than bypassing RunenNet semantics.

## Run the example

From the repository root:

```text
cargo run --locked -p runen-net-quic --example standalone
```

The example is intentionally one process on loopback. It demonstrates ownership and API composition, not production deployment, authentication, matchmaking, certificate operations, or world/session policy.
