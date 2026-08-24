# Authoritative Replication Consistency

Status: **provisional incomplete normative**

This document owns the initial RunenNet authoritative replication lineage, cursor, full-state, delta-reconstruction, commit, and acknowledgement semantics.

Identity and participant lifetime are defined by [Core identity and time](../core/identity.md) and [Session and authority lifecycle](../session/lifecycle.md). Message delivery acceptance/exposure is defined by [Delivery flow semantics](../delivery/flow.md).

Live baseline retention and full-snapshot recovery policy are not defined by this document.

## Scope

This revision defines:

- replication lineage scope;
- replication cursor ordering;
- complete authoritative state images;
- full snapshot and delta snapshot meaning;
- historical-baseline reconstruction and atomic target commit;
- acknowledgement meaning and authority-side confirmation handling;
- authority delta-base selection from client confirmation;
- separation of ReplicationCursor, SimulationTick, and delivery sequence/order.

Component/schema encoding, ECS operation format, interest/view construction, prediction/rollback, live-retention limits, reconnect history restoration, archival replay, and transport mapping are not defined by this revision.

## Initial authority model

This revision defines authority-to-participant state replication for the single-authority client/server session model.

Only the session authority establishes authoritative replication state for a lineage. A participant acknowledgement confirms commit of authority state; it does not make the participant authoritative for that state.

## Replication lineage

A **replication lineage** is the ordered authoritative state history presented to one admitted participant incarnation.

The initial client/server profile permits at most one active authoritative replication lineage for one ParticipantId.

A lineage begins no earlier than creation of that participant membership. It may remain semantically associated with the same retained participant while the membership is temporarily Unbound. It ends when that participant membership ends or the session closes.

A replacement transport connection does not create a new participant incarnation and therefore does not by itself create a new replication lineage. Whether prior baseline state is usable after connection replacement is outside this consistency document.

Different participant incarnations have different replication lineages even if they correspond to the same external account or observe identical application state.

This revision defines no separate LineageId. The ParticipantId scopes the one initial lineage.

## Replication state image

A **replication state image** is the complete authoritative replication state for one lineage at one ReplicationCursor and one SimulationTick.

“Complete” means complete for the state view owned by that lineage. Construction of that view, including later interest/relevancy policy, is outside this revision.

A state image is semantically independent of ECS representation. Its encoding, component schema, and host storage representation are not defined here.

A client may retain multiple previously committed state images as historical delta baselines, but exactly one committed state image is **current** at any instant.

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

A full snapshot is baseline-independent. Its validity and meaning MUST NOT depend on possession of an earlier ReplicationCursor or state image.

A newer valid full snapshot may therefore establish or replace the client's current committed state directly.

A full snapshot MUST NOT encode a semantic requirement that some previous cursor was applied first. Diagnostic provenance may be carried by a later wire format, but it does not become a baseline precondition.

## Delta snapshot

A **delta snapshot** declares:

- exactly one base ReplicationCursor;
- one target ReplicationCursor greater than that base cursor;
- one target SimulationTick;
- a deterministic transform from the complete committed state image at the declared base cursor to the complete authoritative state image at the target cursor.

The delta's semantics are defined only relative to its declared base state image. The transform MUST NOT be interpreted as a patch to an arbitrary current host state.

A delta does not require its target cursor to be the numerically immediate successor of its base. Cursor gaps are allowed.

Multiple emitted delta snapshots MAY share the same base cursor while the authority has not yet received confirmation of a newer committed cursor.

## Authority delta-base selection

The authority's **latest confirmed cursor** is defined by the acknowledgement rules below.

When the authority emits a delta snapshot, that delta's base cursor MUST equal the latest confirmed cursor for the lineage at the time the delta is constructed.

The authority MUST NOT choose an older confirmed cursor merely because it is retained when a newer confirmed cursor exists.

Whether the exact state image for the latest confirmed cursor is still available for delta construction is a retention/recovery concern. If it is not available, this consistency document does not permit substituting another older base.

When no confirmed cursor exists, the authority cannot construct a delta under this initial model. A baseline-independent full snapshot is required to establish a confirmable state first.

## Client committed state history

For one lineage, the client has:

- zero or one **current committed** state image; and
- zero or more **historical committed** state images retained as possible delta-reconstruction baselines.

Every retained committed state image is identified by its ReplicationCursor and SimulationTick.

Historical committed state is protocol baseline material only. Its existence does not make it current and does not authorize applying later delta operations directly to that historical or current host state.

The amount and eviction policy of retained committed history are not defined by this document.

## Candidate classification

Let `current` be the client's current committed cursor when one exists.

For either full or delta snapshots:

- a target cursor less than `current` is **Stale** and MUST NOT mutate the current state;
- a target cursor equal to `current` is **DuplicateCurrent** and MUST NOT reapply or mutate the current state;
- a target cursor greater than `current`, or any target when no current state exists, is a newer candidate subject to the rules below.

A client MAY emit a repeat acknowledgement for DuplicateCurrent without recommitting the state.

A newer snapshot whose SimulationTick is earlier than the current committed SimulationTick is **TickRegression** and MUST NOT mutate the current state.

### Full snapshot applicability

A newer full snapshot is baseline-applicable without reference to any retained historical cursor.

It becomes committable only after its complete state image and all required host-integration validation have succeeded.

### Delta reconstruction applicability

For a newer delta, the client MUST locate the committed state image identified by the delta's declared base ReplicationCursor in the same lineage.

If no such committed state image is retained, the delta is **MissingBase** and MUST NOT mutate the current state.

If the base state is retained, the client MUST apply the delta transform to exactly that state image to reconstruct one complete target state image.

The declared base need not equal the current cursor. A retained older committed base remains valid reconstruction input for a newer target.

The target SimulationTick MUST NOT be earlier than either:

- the SimulationTick of the declared base state image; or
- the SimulationTick of the client's current committed state, when current exists.

Violation is TickRegression and MUST NOT mutate current state.

After exact-base reconstruction succeeds, the complete reconstructed target and all required host-integration validation must succeed before commit.

## Reconstructed target replaces protocol state

A successfully reconstructed delta target is a complete authoritative state image. Commit establishes that complete target as current; it does not mean “apply these delta operations to whatever host state is currently installed.”

A host adapter MAY implement commit by replacement, reconciliation, diffing current host state against the reconstructed target, transactional mutation, or another strategy. Whatever strategy is used, the resulting committed replication state MUST be semantically equal to the reconstructed complete target image.

This rule is what permits multiple newer deltas to share one older acknowledged baseline without applying historical-base operations to an unrelated newer host state.

## Atomic commit

**Commit** is the protocol transition that makes one newer authoritative state image the lineage's current replication state.

Commit MUST be atomic from the replication protocol perspective:

1. the complete snapshot/delta is validated;
2. for a delta, the exact declared committed base is located and target reconstruction succeeds;
3. the resulting complete target state and required host integration are successfully established;
4. only then do current ReplicationCursor and SimulationTick advance together to the target.

A failed validation, decode, base lookup, transform, reconstruction, or host-integration attempt MUST NOT advance the current ReplicationCursor or SimulationTick and MUST NOT be reported as committed.

A host adapter that mutates external state while applying a candidate MUST provide staging, rollback, replacement, reconciliation, or another mechanism sufficient to avoid reporting a partial/failed mutation as a committed replication state.

After commit, the prior current state may remain as historical committed baseline material under the live retention policy.

## Acknowledgement meaning

A **replication acknowledgement** confirms that the participant committed the identified ReplicationCursor as its current authoritative replication state for this lineage at the time the acknowledgement was originated.

An acknowledgement is application-level replication confirmation. It is not:

- packet receipt;
- transport acknowledgement;
- byte reassembly;
- snapshot decode success without commit;
- rendering/presentation completion.

A client MUST NOT originate an acknowledgement for a cursor that it has not committed as current.

A client MAY repeat an acknowledgement for its current committed cursor.

A later client state may advance after an acknowledgement is originated; that does not make the earlier acknowledgement false.

When an acknowledgement is itself emitted through RunenNet delivery, its message is considered emitted only according to RN1B delivery acceptance. This document does not select its delivery mode.

## Authority acknowledgement state

For each replication lineage, the authority maintains at most one **latest confirmed cursor**, initially absent.

An incoming acknowledgement is interpreted only after session lifecycle has authorized the sender as the participant owning that lineage.

Relative to latest confirmed cursor:

- if the acknowledged cursor is equal, the acknowledgement is **DuplicateConfirmation** and is an idempotent no-op;
- if it is lower, the acknowledgement is **StaleConfirmation** and MUST NOT regress confirmation state;
- if it is higher, the authority MUST verify that the cursor identifies a snapshot previously emitted for this lineage before advancing confirmation.

If a higher cursor is verified as previously emitted for the lineage, the acknowledgement is **Confirmed** and becomes the new latest confirmed cursor.

If a higher cursor is greater than every cursor the authority has emitted for the lineage, it is **FutureConfirmation** and MUST NOT advance confirmation state.

If the authority cannot establish that a higher non-future cursor was actually emitted for the lineage, it is **UnverifiableConfirmation** and MUST NOT advance confirmation state.

The mechanism and retention of emission evidence are not defined by this document.

## Confirmation is not baseline availability

A Confirmed acknowledgement records a fact about the participant's committed state. It does not assert that the authority still retains or can reconstruct the corresponding state image.

Loss of authority-side baseline state MUST NOT retroactively turn a previously truthful Confirmed acknowledgement into a false acknowledgement.

Whether latest confirmed cursor is currently usable as the next delta base is a separate retention/recovery decision.

## ACK skipping

A participant MAY acknowledge a newer committed cursor without separately acknowledging every older emitted cursor.

If that newer acknowledgement is Confirmed, the authority advances latest confirmed cursor directly to it. Older later-arriving acknowledgements then become StaleConfirmation or DuplicateConfirmation as applicable.

This permits a client to receive and commit newer authoritative snapshots despite loss/reordering of older snapshots.

## Invalid/stale input does not mutate state

Stale, DuplicateCurrent, TickRegression, MissingBase, malformed/un-decodable candidates, failed delta transforms/reconstruction, and failed host integration MUST NOT mutate the client's current committed state.

This document classifies consistency outcomes only; it does not define which failures require persistent full-snapshot recovery.

Likewise, DuplicateConfirmation, StaleConfirmation, FutureConfirmation, and UnverifiableConfirmation MUST NOT regress or otherwise poison the authority's latest confirmed cursor.
