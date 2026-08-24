# Repository Testing

This document owns the mechanical repository validation contract. Semantic assurance and conformance strategy belong under `docs/verification/` when introduced.

## Canonical gate

The intended repository acceptance command is:

```text
cargo validate
```

RN0 is not accepted until this command is implemented in repository-owned tooling and exercises the complete mechanical gate for the repository state.

At minimum the RN0 gate must verify:

1. locked Cargo metadata once a workspace exists;
2. Markdown link integrity;
3. normative `spec/` dependency-boundary rules;
4. workspace formatting once Rust code exists;
5. locked all-target tests once Rust targets exist;
6. Clippy with warnings denied once Rust targets exist;
7. Git diff hygiene and checkout-state preservation.

Focused checks may be used during development but do not replace the canonical gate before acceptance.

## Bootstrap constraint

Documentation-only bootstrap commits before the Cargo validation tool exists MUST NOT be described as satisfying the final RN0 validation contract. Establishing the runnable gate is remaining RN0 work, not an exception to it.
