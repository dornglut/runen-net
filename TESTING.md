# Repository Testing

This document owns the mechanical repository validation contract. Semantic assurance and conformance strategy belong under `docs/verification/` when introduced.

## Canonical gate

The repository acceptance command is:

```text
cargo validate
```

The command is repository-owned and currently verifies:

1. locked Cargo metadata;
2. Markdown link integrity;
3. normative `spec/` dependency-boundary rules;
4. workspace formatting;
5. locked all-target workspace tests;
6. execution of the public standalone QUIC loopback example;
7. Clippy with warnings denied;
8. Git diff hygiene;
9. before/after checkout-state preservation.

The standalone QUIC execution check is self-contained and loopback-only. It verifies that the advertised public client/server example completes successfully at runtime rather than merely compiling.

Focused checks may be used during development but do not replace `cargo validate` before acceptance.

GitHub Actions invokes the same repository-owned command through the pinned Dornglut reusable Rust validation workflow and validates the exact reviewed feature-head revision.
