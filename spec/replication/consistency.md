# Authoritative Replication Consistency

Status: **provisional incomplete normative**

This document owns the initial RunenNet authoritative replication lineage, cursor, full-state, delta, commit, and acknowledgement semantics.

Identity and participant lifetime are defined by [Core identity and time](../core/identity.md) and [Session and authority lifecycle](../session/lifecycle.md). Message delivery acceptance/exposure is defined by [Delivery flow semantics](../delivery/flow.md).

Live baseline retention and full-snapshot recovery policy are not defined by this document.

## Scope

This revision defines:

- replication lineage scope;
- replication cursor ordering;
- complete authoritative state images;
- full snapshot and delta snapshot meaning;
- client applicability and atomic commit;
- acknowledgement meaning and authority-side confirmation handling;
- separation of replication cursor, SimulationTick, and delivery sequence/order.

Component/schema encoding, ECS operations, interest/view construction, prediction/rollback, live-retention limits, reconnect history restoration, archival replay, and transport mapping are not defined by this revision.

## Initial authority model

This revision defines authority-to-participant state replication for the single-authority client/server session model.

Only the session authority establishes authoritative replication state for a lineage. A participant acknowledgement confirms receipt/commit of authority state; it does not make the participant authoritative for that state.

## Replication lineage

A **replication lineage** is the ordered authoritative state history presented to one admitted participant incarnation.

The initial client/server profile permits at most one active authoritative replication lineage for one ParticipantId.

A lineage begins no earlier than creation of that participant membership. It may remain semantically associated with the same retained participant while the membership is temporarily Unbound. It ends when that participant membership ends or the session closes.

A replacement transport connection does not create a new participant incarnation and therefore does not by itself create a new replication lineage. Whether the prior baseline is usable after connection replacement is a recovery concern, not lineage identity.

Different participant incarnations have different replication lineages even if they correspond to the same external account or later observe identical application state.

This revision defines no separate LineageId. The ParticipantId scopes the one initial lineage.

## Replication state image

A **replication state image** is the complete authoritative replication state for one lineage at one replication cursor and one SimulationTick.

“Complete” means complete for the state view owned by that lineage. The construction of that view, including later interest/relevancy policy, is outside this revision.

A state image is semantically independent of ECS representation. Its encoding, component schema, and host storage representation are not defined here.

## Replication cursor

A **ReplicationCursor** identifies one authoritative state revision inside one replication lineage.

ReplicationCursor values are opaque except for ordering within their lineage.

For snapshots emitted for one lineage, each newer emitted state revision MUST use a ReplicationCursor greater than every previously emitted state revision in that lineage.

Cursor values need not be contiguous. A cursor from one lineage MUST NOT be ordered, compared, or treated as identifying the same state solely because it has the same representation as a cursor from another lineage.

ReplicationCursor is distinct from:

- SimulationTick;
- RN1B delivery-flow acceptance order;
- RN1B `UnreliableSequenced` sequence values;
- transport packet or stream sequence numbers.

## Simulation tick relationship

Each emitted authoritative state revision has one SimulationTick describing the host simulation step represented by that state.

Within one lineage, a newer ReplicationCursor MUST NOT represent a SimulationTick earlier than the SimulationTick of an older emitted ReplicationCursor.

Multiple ReplicationCursor values MAY represent the same SimulationTick. Not every SimulationTick requires a replication state revision.

## Snapshot emission

For acknowledgement purposes, a snapshot is **emitted** only when the complete message carrying that snapshot has been Accepted into its selected RN1B delivery flow.

Building, serializing, staging, or attempting to submit a snapshot does not by itself make its cursor acknowledgement-eligible.

The selected RN1B delivery mode is outside this specification. Replication semantics MUST remain correct for any delivery mode that a later profile declares valid for the snapshot kind.

## Full snapshot

A **full snapshot** declares:

- one target ReplicationCursor;
- one target SimulationTick;
- one complete authoritative replication state image for that target.

A full snapshot is baseline-independent. Its validity and meaning MUST NOT depend on possession of an earlier replication cursor or state image.

A newer valid full snapshot may therefore establish or replace the client's current committed baseline directly.

A full snapshot MUST NOT encode a semantic requirement that some previous cursor was applied first. Diagnostic provenance may be carried by a later wire format, but it does not become a baseline precondition.

## Delta snapshot

A **delta snapshot** declares:

- exactly one base ReplicationCursor;
- one target ReplicationCursor greater than that base cursor;
- one target SimulationTick;
- a deterministic transform from the complete state image at the declared base cursor to the complete authoritative state image at the target cursor.

The delta's semantics are defined only relative to its declared base state image. Applying the same transform to any other current state is not conforming delta application.

A delta does not require its target cursor to be the numerically immediate successor of its base. Cursor gaps are allowed.

## Client current baseline

For one lineage, the client has either:

- no current committed replication baseline; or
- exactly one current committed tuple `(ReplicationCursor, SimulationTick, replication state image)`.

Historical application, prediction, interpolation, or replay state may exist outside this protocol state, but such state is not an additional current replication baseline.

## Candidate classification

Let `current` be the client's current committed cursor when one exists.

For either full or delta snapshots:

- a target cursor less than `current` is **Stale** and MUST NOT mutate the current baseline;
- a target cursor equal to `current` is **DuplicateCurrent** and MUST NOT reapply or mutate the current baseline;
- a target cursor greater than `current`, or any target when no current baseline exists, is a newer candidate subject to the rules below.

A client MAY emit a repeat acknowledgement for DuplicateCurrent without recommitting the state.

A newer snapshot whose SimulationTick is earlier than the current committed SimulationTick is **TickRegression** and MUST NOT mutate the current baseline.

### Full snapshot applicability

A newer full snapshot is baseline-applicable without reference to the current cursor.

It becomes committable only after its complete state image and all required host integration validation have succeeded.

### Delta snapshot applicability

A delta is baseline-applicable only when the client currently has a committed baseline and the delta's base ReplicationCursor is exactly equal to that current committed cursor.

A newer delta received with no current baseline is **MissingBase**.

A newer delta whose declared base is not exactly the current committed cursor is **BaseMismatch**.

The existence of some older retained or reconstructable state with the declared base cursor MUST NOT make such a delta applicable to a different current baseline.

After baseline applicability is established, the delta transform must successfully produce the complete target state image and all required host integration validation must succeed before commit.

## Atomic commit

**Commit** is the protocol transition that makes one newer authoritative state image the lineage's current replication baseline.

Commit MUST be atomic from the replication protocol perspective:

1. the complete snapshot/delta is validated;
2. for a delta, the exact current base is established and the transform succeeds;
3. the resulting complete target state and required host integration are successfully established;
4. only then do current ReplicationCursor and SimulationTick advance together to the target.

A failed validation, decode, transform, or host-integration attempt MUST NOT advance the current ReplicationCursor or SimulationTick and MUST NOT be reported as a committed state.

A host adapter that mutates external state while applying a candidate MUST provide staging, rollback, replacement, or another mechanism sufficient to avoid reporting a partial/failed mutation as a committed replication baseline.

The concrete host transaction mechanism is outside this specification.

## Acknowledgement meaning

A **replication acknowledgement** confirms that the participant has committed the identified ReplicationCursor as its current authoritative replication baseline for this lineage.

An acknowledgement is application-level replication confirmation. It is not:

- packet receipt;
- transport acknowledgement;
- byte reassembly;
- snapshot decode success without commit;
- rendering/presentation completion.

A client MUST NOT originate an acknowledgement for a cursor that it has not committed as its current baseline.

A client MAY repeat an acknowledgement for its current committed cursor.

When an acknowledgement is itself emitted through RunenNet delivery, its message is considered emitted only according to RN1B delivery acceptance. This document does not select its delivery mode.

## Authority acknowledgement state

For each replication lineage, the authority maintains at most one **latest confirmed cursor**, initially absent.

An incoming acknowledgement is interpreted only after session lifecycle has authorized the sender as the participant owning that lineage.

Relative to `latest confirmed cursor`:

- if the acknowledged cursor is equal, the acknowledgement is **DuplicateConfirmation** and is an idempotent no-op;
- if it is lower, the acknowledgement is **StaleConfirmation** and MUST NOT regress confirmation state;
- if it is higher, the authority MUST verify that this cursor identifies a snapshot previously emitted for this lineage before advancing confirmation.

If a higher cursor is verified as previously emitted for the lineage, the acknowledgement is **Confirmed** and becomes the new latest confirmed cursor.

If a higher cursor is greater than every cursor the authority has emitted for the lineage, it is **FutureConfirmation** and MUST NOT advance confirmation state.

If the authority cannot establish that a higher non-future cursor was actually emitted for the lineage, it is **UnverifiableConfirmation** and MUST NOT advance confirmation state.

The mechanism and retention of emission evidence are recovery/retention concerns and are not defined here.

## Confirmation is not baseline availability

A Confirmed acknowledgement records a fact about the participant's committed state. It does not assert that the authority still retains or can reconstruct the corresponding state image.

Therefore loss of authority-side baseline state MUST NOT retroactively turn a previously truthful Confirmed acknowledgement into a false acknowledgement.

Whether the latest confirmed cursor is currently usable as a delta base is a separate recovery/retention decision.

## ACK skipping

A participant MAY acknowledge a newer committed cursor without separately acknowledging every older emitted cursor.

If that newer acknowledgement is Confirmed, the authority may advance its latest confirmed cursor directly to it. Older later-arriving acknowledgements then become StaleConfirmation or DuplicateConfirmation as applicable.

This supports full snapshots and other valid state transitions that can supersede unacknowledged older states without making cursors contiguous.

## Invalid/stale input does not mutate baseline

Stale, DuplicateCurrent, TickRegression, MissingBase, BaseMismatch, malformed/un-decodable candidates, failed delta transforms, and failed host integration MUST NOT mutate the client's current committed baseline.

This document classifies those outcomes only. Which outcomes require persistent full-snapshot recovery is defined by the replication recovery specification.

Likewise, DuplicateConfirmation, StaleConfirmation, FutureConfirmation, and UnverifiableConfirmation MUST NOT regress or otherwise poison the authority's latest confirmed cursor.
