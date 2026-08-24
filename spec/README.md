# RunenNet Specification

Version: **0.1-provisional**

The specification defines RunenNet semantics. Core networking semantics are host- and transport-independent. Optional profiles may define conforming host or transport realizations, but they do not redefine the core semantics they realize.

Implementation packages, Runenwerk behavior, roadmap items, and research records are non-normative unless incorporated through an explicit specification revision.

- [Specification conventions](conventions.md)
- [Core identity and time](core/identity.md)
- [Session and authority lifecycle](session/lifecycle.md)
- [Delivery flow semantics](delivery/flow.md)
- [Delivery pressure and resource policy](delivery/pressure.md)
- [Protocol, schema, codec, and capability identity](protocol/identity.md)
- [Protocol and schema negotiation](protocol/negotiation.md)
- [Authoritative replication consistency](replication/consistency.md)
- [Replication retention and full-snapshot recovery](replication/recovery.md)
- [Conformance profiles](conformance/profiles.md)

Semantic areas not linked from this index are not defined by this revision.
