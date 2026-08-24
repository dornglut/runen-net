# Replication Retention and Full-Snapshot Recovery

Status: **provisional incomplete normative**

This document owns the initial RunenNet live replication baseline-retention, baseline-usability, and full-snapshot-recovery semantics. It depends on [Authoritative replication consistency](consistency.md).

Session membership/connection replacement is defined by [Session and authority lifecycle](../session/lifecycle.md). RN1B delivery-flow lifetimes remain independent of replication lineage state.

## Scope

This revision defines:

- finite live replication state retention;
- retained committed baselines on client and authority;
- bounded emitted-snapshot evidence used for ACK verification;
- client persistent `FullSnapshotRequired` recovery state;
- authority delta eligibility versus full-snapshot-required state;
- recovery after missing baseline, malformed/unreconstructable delta, baseline eviction, and connection replacement;
- separation of live replication retention from prediction/interpolation and archival replay.

Wire recovery messages, advanced reconnect/history restoration, schema negotiation, prediction rollback, interest/view construction, archival checkpoint format, and public API shape are not defined by this revision.

## Live replication retention domain

**Live replication retention** is state kept specifically so current authoritative replication can:

- reconstruct incoming deltas on the client;
- construct outgoing deltas on the authority;
- verify acknowledgements of previously emitted snapshots; and
- recover deterministically when that state is unavailable.

Live replication retention is distinct from:

- prediction/rollback history;
- presentation/interpolation history;
- archival recording or replay history;
- editor/debug capture.

An implementation MAY use a shared storage mechanism internally only if ownership, bounds, and eviction effects required by this specification remain explicit. The existence of archival/history data does not automatically make a cursor an available live replication baseline.

## Baseline availability

A committed state image at cursor C is **BaselineAvailable** on one endpoint only when that endpoint can obtain the exact complete state image for C under its current bounded live replication retention contract.

The image may be stored directly or reconstructed through a deterministic bounded live-baseline provider. If exact reconstruction would depend on data outside the active retention contract, C is not BaselineAvailable for this protocol.

A baseline from another lineage MUST NOT satisfy BaselineAvailable even if its cursor representation or application contents appear equal.

## Required finite retention policy

Before a replication lineage becomes active, each endpoint MUST operate it under an explicit finite live-retention policy.

At minimum the policy MUST bound:

- maximum attributable bytes for one complete retained replication state image;
- maximum retained committed state-image count per lineage;
- maximum attributable retained committed state bytes per lineage;
- maximum in-progress candidate/reconstruction state bytes per lineage;
- maximum retained emitted-snapshot evidence entries per lineage on the authority.

The authority MUST additionally impose finite session-level aggregate bounds on:

- active/retained replication-lineage state;
- retained committed state-image count;
- retained committed state bytes;
- emitted-snapshot evidence entries.

The client MUST impose finite aggregate replication-state bounds across all active session/lineage state it can hold concurrently.

Any additional RunenNet-owned baseline cache, reconstruction cache, candidate-state buffer, emitted-cursor registry, or recovery queue that can grow from peer activity MUST have an explicit finite bound or be covered directly by a bound above.

Exact numeric defaults are not defined by this revision.

## State-image resource limit

A candidate full or reconstructed delta target whose complete state image cannot fit the endpoint's configured maximum state-image/candidate resource bound MUST NOT be committed.

The endpoint MUST report an observable replication resource failure. It MUST NOT partially install the oversized state or silently increase an unbounded resource limit.

A state-image resource failure is not automatically classified as recoverable by another full snapshot, because the same complete target state may exceed the same local bound. A host/profile may terminate replication or change policy through mechanisms outside this revision.

## Client retained committed history

While synchronized, the client MUST retain its current committed state image as BaselineAvailable.

The client MAY retain older committed state images so newer delta snapshots can reconstruct from the authority's latest ACKed baseline even after the client has advanced to a newer current cursor.

Historical committed state retention MUST remain within the configured finite count/byte policy.

The client MAY evict an older committed baseline at any time permitted by that policy. Eviction does not mutate the current committed state. If a later newer delta declares that evicted cursor as its base, the delta becomes MissingBase and recovery rules below apply.

A client therefore need not preserve arbitrary history indefinitely to make acknowledged-baseline deltas correct. Bounded eviction may cause a full recovery; it MUST NOT cause application against the wrong base.

## Authority retained state

The authority MAY retain complete emitted state images for each lineage under its finite live-retention policy.

For ordinary delta operation, the exact state image at the lineage's latest confirmed cursor must be BaselineAvailable on the authority.

If that latest confirmed state is not BaselineAvailable, the lineage is not delta-eligible even though the client confirmation remains valid.

The authority MAY evict historical/unconfirmed state images to remain within policy. If eviction removes the exact latest-confirmed baseline, authority recovery state MUST become FullSnapshotRequired as defined below.

A slow participant MUST NOT force the authority to violate session aggregate retention bounds. Full-snapshot recovery is the correctness fallback when live delta history is evicted.

## Emitted-snapshot evidence

The authority keeps bounded lightweight evidence sufficient to determine whether a higher acknowledgement cursor refers to a snapshot actually emitted for that lineage, as required by the consistency specification.

Emission evidence MUST distinguish at least:

- ReplicationCursor;
- whether the emitted snapshot was Full or Delta;
- its lineage;
- its emission order relative to the authority's current recovery state.

The authority MAY discard old evidence under finite policy once it is no longer required for confirmation handling.

If a higher non-future acknowledgement arrives after the authority has discarded the evidence needed to establish that it was emitted, the acknowledgement is UnverifiableConfirmation. It does not advance latest confirmed cursor.

UnverifiableConfirmation alone does not retroactively invalidate an already usable older confirmed baseline. A host may continue from that older confirmed baseline if it remains BaselineAvailable; if the client no longer retains that base, the client's MissingBase recovery path will request a full snapshot.

## Client recovery state

For each active lineage, the client recovery state is one of:

- **Synchronized** — the client has a usable current committed replication state and is allowed to process newer delta snapshots using retained declared baselines;
- **FullSnapshotRequired(reason)** — incremental delta processing is suspended until a valid newer full snapshot is committed.

A newly established lineage begins FullSnapshotRequired with reason **InitialBaseline**.

FullSnapshotRequired is persistent semantic state. Observing it, polling it, producing diagnostics, or attempting to communicate a recovery demand MUST NOT clear it.

A conforming runtime MUST make the recovery requirement repeatedly observable until it is resolved.

The concrete wire/control message used to tell the authority that a full snapshot is required is not defined by this revision.

## Client transitions to FullSnapshotRequired

While Synchronized, a newer delta causes FullSnapshotRequired when any of these occur:

- **MissingBase** — the exact declared committed baseline is not retained;
- **MalformedDelta** — the delta cannot be completely decoded/validated;
- **ReconstructionFailure** — applying the transform to the exact declared base fails to produce a valid complete target;
- **DeltaTickRegression** — the newer target violates the consistency tick-order rule;
- **DeltaCommitFailure** — required host integration cannot establish the reconstructed complete target as current without violating atomic commit.

In all cases the prior current committed state remains unchanged.

A Stale or DuplicateCurrent snapshot does not by itself require recovery because it cannot advance or corrupt current state.

A malformed/invalid newer full snapshot does not destroy an otherwise usable synchronized current baseline. It is rejected without commit. If the client was already FullSnapshotRequired, it remains FullSnapshotRequired.

## Behavior while FullSnapshotRequired

While the client is FullSnapshotRequired:

- it MUST NOT commit a delta snapshot, even if that delta happens to reference a retained historical baseline;
- stale/duplicate input MUST NOT clear recovery state;
- it MAY receive and evaluate newer full snapshots;
- only successful commit of a valid newer full snapshot clears the client state to Synchronized.

This makes “full required” an explicit recovery barrier rather than a hint that an arbitrary later delta may accidentally clear.

## Client connection-replacement recovery

When a retained participant membership receives an authorized replacement transport connection, the replication lineage identity may persist, but the initial recovery profile MUST place the client lineage in FullSnapshotRequired with reason **ConnectionReplacement** before replication on the new connection proceeds.

Previously retained committed state MAY remain available to the application for presentation or diagnostics, but it MUST NOT be used to commit new post-rebind deltas until a valid post-rebind full snapshot has been committed.

A later advanced recovery profile may define proof of baseline continuity across connection replacement without changing this conservative initial rule.

## Authority recovery state

For each active lineage, the authority replication send state is one of:

- **DeltaEligible(base_cursor)** — `base_cursor` is the latest confirmed cursor and its exact state image is BaselineAvailable under live retention;
- **FullSnapshotRequired(reason)** — the authority MUST establish a newly confirmed usable full baseline before emitting further deltas.

A newly established lineage begins FullSnapshotRequired with reason InitialBaseline.

The authority enters FullSnapshotRequired when:

- no confirmed cursor exists;
- the latest confirmed cursor is not BaselineAvailable;
- the participant/host communicates a valid full-snapshot recovery demand;
- an authorized replacement transport connection is bound to the retained participant under the initial recovery profile; or
- another rule in this specification explicitly requires full recovery.

## Authority behavior while FullSnapshotRequired

While authority state is FullSnapshotRequired:

- the authority MUST NOT emit a delta snapshot for that lineage;
- it MAY emit one or more newer full snapshots;
- emitting or RN1B-accepting a full snapshot MUST NOT by itself clear FullSnapshotRequired;
- delivery/exposure of a full snapshot without a Confirmed acknowledgement MUST NOT by itself clear FullSnapshotRequired.

A full snapshot emitted while the authority is FullSnapshotRequired is a **recovery full snapshot** for that recovery episode.

The authority may return to DeltaEligible only after:

1. it receives a Confirmed acknowledgement for a recovery full snapshot emitted during the current FullSnapshotRequired episode; and
2. the exact complete state image for that confirmed full cursor is still BaselineAvailable.

The confirmed full cursor then becomes the DeltaEligible base cursor.

If the recovery full state was evicted before its acknowledgement arrives, the acknowledgement can still become Confirmed according to consistency rules, but FullSnapshotRequired remains because the confirmed state is not BaselineAvailable. The authority must establish another usable full baseline.

This prevents “sent one full snapshot” from becoming a false recovery completion when that full snapshot was lost or its baseline cannot be used.

## Authority behavior while DeltaEligible

While DeltaEligible(base_cursor):

- `base_cursor` MUST equal the lineage's latest confirmed cursor;
- its exact state image MUST remain BaselineAvailable;
- every newly constructed delta MUST use that base cursor, as required by the consistency specification, until a newer acknowledgement becomes Confirmed.

Multiple newer delta snapshots MAY therefore be emitted against the same confirmed base while the authority waits for a newer ACK.

When a newer acknowledgement becomes Confirmed:

- if its exact state image is BaselineAvailable, DeltaEligible advances to that newer cursor;
- if its state image is not BaselineAvailable, authority state becomes FullSnapshotRequired with reason **ConfirmedBaselineUnavailable**.

If the current DeltaEligible baseline is later evicted, state immediately becomes FullSnapshotRequired with reason **BaselineEvicted**.

DuplicateConfirmation and StaleConfirmation do not change authority recovery state.

FutureConfirmation does not change authority recovery state.

UnverifiableConfirmation does not advance latest confirmed cursor and does not by itself require abandoning an already BaselineAvailable DeltaEligible cursor.

## Missing-base recovery is per lineage

A client MissingBase or authority BaselineEvicted condition affects only that replication lineage.

It MUST NOT globally invalidate another participant's confirmed baseline or force unrelated lineages to full snapshots.

Session-wide resource pressure may independently evict multiple lineages under the configured aggregate retention policy, in which case each affected lineage transitions explicitly.

## Connection replacement does not transfer delivery state

RN1B delivery flows terminate with their transport connection. A replacement connection creates new delivery-flow lifetimes.

Replication lineage state may remain associated with the retained ParticipantId, but queued/accepted RN1B messages from the old connection MUST NOT be transferred to new flows.

The initial recovery rule requiring a new full snapshot after rebind is the explicit bridge between persistent participant/replication identity and non-persistent delivery-flow state.

## Recovery completion is not observation

Neither client nor authority recovery state may be cleared merely because:

- a recovery flag/state was read;
- a recovery request was queued;
- a full snapshot was constructed;
- a full snapshot was submitted to delivery;
- a full snapshot was exposed but not committed/confirmed as required above.

Only the semantic recovery transitions defined in this document clear FullSnapshotRequired.

## Diagnostics and conformance outcomes

A conforming implementation MUST make at least these replication-recovery outcomes distinguishable to its conformance/runtime inspection boundary:

- lineage Synchronized / DeltaEligible;
- client FullSnapshotRequired and its reason class;
- authority FullSnapshotRequired and its reason class;
- MissingBase;
- malformed/reconstruction/commit delta failure;
- baseline eviction;
- recovery full snapshot emitted;
- recovery full snapshot committed on client;
- recovery full acknowledgement Confirmed;
- Confirmed acknowledgement whose baseline is unavailable;
- state-image resource failure.

The exact public event/enum representation is not defined here.

## Deferred recovery semantics

The following are not defined by this revision:

- reconstructing a baseline from archival replay/checkpoints;
- preserving delta eligibility across connection replacement through proof of continuity;
- transferring in-flight delivery messages across connections;
- prediction rollback history;
- interpolation history;
- lag compensation history.

Those features may later consume the lineage/cursor/commit semantics here but MUST NOT weaken bounded live retention or exact-base reconstruction.
