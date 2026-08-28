# RunenNet

RunenNet is a standalone, engine-independent Rust networking framework.

Runenwerk is a downstream consumer of RunenNet, not its architectural host. Existing Runenwerk networking code is migration evidence only unless its semantics are deliberately specified and accepted here.

## Standalone use

- [Standalone client/server guide](docs/standalone.md)
- [Production-QUIC public API example](crates/runen-net-quic/examples/standalone.rs)
- [Transport-independent Core example](crates/runen-net/examples/authoritative_counter.rs)

## Repository authority

- [Specification](spec/README.md) — normative RunenNet semantics.
- [Repository architecture](ARCHITECTURE.md) — package and dependency boundaries.
- [Roadmap](ROADMAP.md) — sequencing and acceptance gates.
- [Repository testing](TESTING.md) — canonical mechanical validation.
- [Documentation architecture](docs/documentation-architecture.md) — documentation ownership and dependency direction.

## License

RunenNet is available under the [GNU General Public License v3.0 only](LICENSE). A separate commercial license may be available from copyright holder(s) authorized to grant it; see [LICENSING.md](LICENSING.md).
