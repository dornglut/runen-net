# RunenNet

RunenNet is a standalone, engine-independent Rust networking framework.

Runenwerk is a downstream consumer of RunenNet, not its architectural host. Existing Runenwerk networking code is migration evidence only unless its semantics are deliberately specified and accepted here.

## Mental model

RunenNet has two public layers:

- `runen-net` is the transport-independent semantic Core. Applications own its session, negotiation, delivery, replication, input/prediction, and resource-policy state.
- `runen-net-quic` is the production QUIC realization. It drives the accepted QUIC profile against application-owned Core state; it does not replace or duplicate Core authority.

The main Core concepts compose in this direction:

```text
identity + protocol negotiation
        -> session membership / authorization
        -> delivery flows
        -> replication
        -> input / prediction
```

This is a navigation map, not a requirement that every application use every subsystem. A simple byte-channel application can stop at delivery. Replication and prediction are opt-in higher-level contracts.

Keep these identities distinct:

- `SessionId` identifies one session;
- `ParticipantId` identifies one participant incarnation inside session policy;
- `ConnectionHandle` identifies one local transport-connection lifetime and has no wire meaning;
- `SimulationTick` is host simulation logical time.

Semantic Authority is also independent of QUIC client/server side.

## Where to start

- [Standalone client/server guide](docs/standalone.md) — ordinary production-QUIC lifecycle and the Core/QUIC ownership model.
- [Production-QUIC public API example](crates/runen-net-quic/examples/standalone.rs) — executable reliable and unreliable loopback flows.
- [Transport-independent Core example](crates/runen-net/examples/authoritative_counter.rs) — direct Core delivery realization plus authoritative replication. It intentionally uses the advanced `delivery::adapter` boundary and is not the ordinary QUIC application path.
- `runen-net` crate rustdoc — Core subsystem ownership, session recovery, and replication/prediction composition.
- `runen-net-quic` crate rustdoc — endpoint/ProfileReady/connection lifecycle and event-driving model.

## Repository authority

- [Specification](spec/README.md) — normative RunenNet semantics.
- [Repository architecture](ARCHITECTURE.md) — package and dependency boundaries.
- [Roadmap](ROADMAP.md) — sequencing and acceptance gates.
- [Repository testing](TESTING.md) — canonical mechanical validation.
- [Documentation architecture](docs/documentation-architecture.md) — documentation ownership and dependency direction.

## License

RunenNet is available under the [GNU General Public License v3.0 only](LICENSE). A separate commercial license may be available from copyright holder(s) authorized to grant it; see [LICENSING.md](LICENSING.md).
