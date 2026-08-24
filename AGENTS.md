# Agent Instructions

Automated contributors must follow the repository documentation authority defined in [docs/documentation-architecture.md](docs/documentation-architecture.md).

Before editing, inspect the canonical owner of the concern and its direct normative dependencies. Current Runenwerk networking implementation is evidence and migration input, not normative RunenNet authority.

For iterative continuation, re-establish current repository state and live issue authority before selecting work. An open specification item is not by itself permission to invent semantics or implementation.

Do not create compatibility aliases, duplicate authorities, speculative crate splits, engine/ECS dependencies, or transport-owned networking semantics unless accepted repository authority explicitly requires them.

RunenNet MUST remain independently usable without Runenwerk, a concrete ECS, engine plugins/schedules, rendering, or spatial frameworks.

Before proposing acceptance, run the canonical validation defined by [TESTING.md](TESTING.md) once that gate is established, and review the exact changed head.
