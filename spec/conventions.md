# Specification Conventions

Status: **provisional normative**

This document defines how normative RunenNet specification artifacts are interpreted. It does not define networking behavior.

## Requirement terms

Capitalized **MUST**, **MUST NOT**, **REQUIRED**, **SHOULD**, **SHOULD NOT**, **MAY**, and **OPTIONAL** express normative requirement levels.

Lowercase uses are ordinary English and do not carry those requirement levels.

## Document status

Authority classes are:

- **normative** — constrains conforming implementations;
- **illustrative** — communicates intent without defining required behavior;
- **non-normative** — does not define RunenNet behavior.

Defined qualifiers are:

- **provisional** — accepted pre-stability authority that may change only through explicit normative revision;
- **incomplete** — owns some established rules while explicitly leaving identified semantic items open.

`incomplete` does not weaken rules already defined by the artifact.

## Open specification items

Text stating that a rule is **not defined by this revision** marks an open specification item. It is not permission for an implementation, engine, transport, existing Runenwerk behavior, or test expectation to define RunenNet semantics implicitly.

Implementation-defined behavior exists only where normative text explicitly permits an implementation choice and requires that choice to be documented.

## Ownership and conflicts

Each normative rule has exactly one canonical owner.

A normative artifact MAY reference another normative owner or state relationships between separately owned concepts. It MUST NOT independently restate or redefine another owner's normative rule merely for local completeness.

If two normative artifacts appear to define the same semantic responsibility, that is a specification defect requiring explicit ownership correction. Implementation behavior and document order do not resolve such conflicts.

Non-normative material never overrides normative text.

## Host and transport independence

Unless a normative profile explicitly says otherwise, RunenNet semantics MUST NOT be inferred from a particular engine, ECS, operating system, async executor, serialization crate, or transport realization.
