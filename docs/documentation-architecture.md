# Documentation Architecture

This document owns repository documentation boundaries and dependency direction. It does not define RunenNet semantics, project priorities, package topology, or verification policy.

## Artifact ownership

- `spec/` — normative RunenNet specification only;
- `ROADMAP.md` — project sequencing and acceptance gates only;
- `ARCHITECTURE.md` — repository package and dependency structure only;
- `TESTING.md` — mechanical repository validation only;
- `docs/architecture/` — non-normative implementation and realization design;
- `docs/verification/` — non-normative assurance and conformance strategy;
- `docs/decisions/` — historical design decisions and rationale;
- `docs/research/` — external research and migration evidence;
- `CONTRIBUTING.md` — contributor process when present;
- `AGENTS.md` — automation-specific contributor constraints;
- `README.md` and directory README files — navigation and orientation.

## Dependency direction

Normative specification artifacts may reference other normative specification artifacts. They MUST NOT depend on roadmap, repository implementation, verification documents, design decisions, research notes, Runenwerk source, or contributor workflow for their meaning.

Non-normative documents may reference normative specification owners. Roadmap and contributor documents may reference any artifact needed to identify work, but they do not acquire authority over the referenced concern.

## No duplicated authority

Each rule has one canonical owner. Documents may summarize their own scope and link to another owner, but SHOULD NOT duplicate another owner's detailed rules.

If a concept change requires editing multiple documents that each claim to define the same rule, the documentation decomposition is defective and ownership must be corrected.

## Implementation is not specification

Existing Runenwerk networking code, tests, historical documents, or transport behavior MAY be used as research and migration evidence. They MUST NOT become normative RunenNet semantics merely because they already exist.

## Growth rule

Split an artifact when responsibilities can evolve independently under different correctness or review obligations. Do not pre-create taxonomy or package-shaped documentation solely in anticipation of future implementation.
