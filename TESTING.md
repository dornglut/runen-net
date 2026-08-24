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
6. Clippy with warnings denied;
7. Git diff hygiene;
8. before/after checkout-state preservation.

Focused checks may be used during development but do not replace `cargo validate` before acceptance.

GitHub Actions invokes the same repository-owned command through the pinned Dornglut reusable Rust validation workflow and validates the exact reviewed feature-head revision.
