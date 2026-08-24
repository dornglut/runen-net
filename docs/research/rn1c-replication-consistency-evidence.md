# RN1C Replication Consistency Evidence

Status: **non-normative**

This record supports [RN1C](https://github.com/dornglut/runen-net/issues/8). It compares pinned Runenwerk migration evidence with current external replication designs. It does not define RunenNet semantics.

Evidence snapshot:

- Runenwerk: `37a267e41e49317516d6513b02794f8fc480056a` (observed 2026-08-24)
- Lightyear replication: `0.29.0` documentation (observed 2026-08-24)
- RN1A and RN1B: accepted RunenNet normative specification on the RN1C base revision

## Current Runenwerk evidence

### Active design and client implementation disagree on strict delta applicability

The active Runenwerk authoritative replication design states that a client delta must advance from the client's current cursor and claims strict delta-base validation is implemented.

Source: [Runenwerk authoritative replication design](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/docs-site/src/content/docs/design/active/net-authoritative-replication-protocol.md).

The implementation does not enforce that invariant. `ClientReplicationRuntime::apply_delta_snapshot` rejects a non-advancing target cursor, then accepts any `delta.base` that remains present in its `snapshots` map. It does not require `delta.base == last_cursor`.

Source: [Runenwerk client replication runtime](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/runtime/client.rs).

This matters because the runtime merges the delta against the retained historical base but emits an apply plan containing only the incoming delta actions. Those actions are then intended to mutate the host's current state. If the current state is newer than the declared base, the transform is being applied to a different state than the one it was derived from. Entity lifecycle operations make this particularly unsafe.

The standalone semantics therefore need one unambiguous rule: an incremental delta may update the current client baseline only when its declared base is exactly the current committed cursor.

### Recovery need is currently a destructive boolean

On a missing delta base, the client sets `needs_full_resync = true`. `take_resync_request()` returns the boolean and immediately clears it. A failed downstream enqueue/send can therefore erase the recovery requirement without a successful full snapshot.

Malformed delta decode returns `DecodeError` without setting that recovery flag, despite the active design saying malformed deltas require full resynchronization.

Source: [Runenwerk client replication runtime](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/runtime/client.rs).

This supports a persistent semantic recovery state that is cleared only by committing a valid full snapshot, not by observing or attempting to report the state.

### Client live baseline retention is unbounded by protocol policy

`ClientReplicationRuntime` stores every decoded/merged snapshot in a `BTreeMap<SnapshotCursor, SnapshotPayload>` until reset. Under a strict-current delta model, arbitrary historical client baselines are not required for correctness.

Source: [Runenwerk client replication runtime](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/runtime/client.rs).

This supports retaining one current committed replication state plus explicitly bounded staging in the minimal core. Prediction history and archival replay are separate domains.

### Server ACK validation improved but still conflates confirmation with retention

`AuthoritativeServerRuntime` now rejects stale, future, unsent, and pruned ACKs before changing `last_acknowledged`. This is stronger than the older active design's partial-contract description.

Source: [Runenwerk authoritative server runtime](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/runtime/server.rs).

However, a pruned cursor is rejected as though the client's acknowledgement were semantically invalid. These are different facts:

- the client may truthfully have committed a cursor that the authority emitted;
- the authority may no longer retain the exact state needed to use that cursor as a delta base.

A standalone protocol should preserve the first fact while treating the second as loss of delta eligibility requiring a full snapshot.

The current implementation also treats an ACK equal to the last acknowledged cursor as stale rejection. Idempotent duplicate acknowledgement is safer for retransmission/reordering: equal confirmation can be a no-op without poisoning state, while a lower cursor is stale.

### Baseline/checkpoint authority is duplicated across layers

`AuthoritativeServerRuntime` owns per-connection `last_acknowledged`, `force_full_snapshot`, and `sent_cursors` plus a snapshot timeline.

The engine plugin independently owns `ConnectionBaselineCheckpoint` with `last_ack_cursor`, `last_sent_cursor`, `last_full_snapshot_cursor`, `needs_full_resync`, and bounded `sent_cursors`, plus global and per-connection snapshot histories.

Sources:

- [Runenwerk authoritative server runtime](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/runtime/server.rs)
- [Runenwerk engine networking replication state](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/engine/src/plugins/net/resources.rs)

This is evidence for one RunenNet-owned semantic replication state per consumer lineage. Engine adapters may project/inspect that state but should not independently redefine ACK/baseline/recovery authority.

### Timeline retention is explicit but not governed by one bounded policy

`SnapshotTimeline` stores complete snapshots in a map and only releases them when `prune_before` is called. The engine plugin has additional snapshot-history maps and a separately bounded sent-cursor set.

Sources:

- [Runenwerk snapshot timeline](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/replication/timeline.rs)
- [Runenwerk engine networking replication state](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/engine/src/plugins/net/resources.rs)

A slow or disconnected consumer must not force unbounded live-state retention. Evicting its latest confirmed baseline is allowed under a finite policy, but that lineage then requires a new full snapshot before deltas resume.

### Current cursor scope follows implementation mechanics, not a demonstrated semantic boundary

`SnapshotTimeline` allocates cursors from one timeline while server APIs build snapshots for individual connections. Engine integration also keeps per-connection histories because different consumers can receive different payloads.

Sources:

- [Runenwerk snapshot timeline](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/replication/timeline.rs)
- [Runenwerk engine networking replication state](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/engine/src/plugins/net/resources.rs)

Because later interest/relevancy can make each participant's authoritative projection different, bare cursors should not acquire accidental global comparability. A participant-scoped replication lineage gives the initial core a clear baseline domain while remaining independent of transport connections.

### Snapshot cursor and simulation tick are already separate concepts

Runenwerk snapshot and delta messages carry both `SimulationTick` and `SnapshotCursor`. ACKs also carry a cursor and a last-received tick.

Sources:

- [Runenwerk snapshot protocol](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/protocol/snapshot.rs)
- [Runenwerk ACK protocol](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/protocol/ack.rs)

That separation is correct. A replication cursor orders committed authoritative state revisions in its own lineage; a simulation tick names host logical simulation time; RN1B delivery sequence/order belongs to another domain again.

## External comparison evidence

### Lightyear keeps delta state as replication-specific retained state

Lightyear 0.29.0 exposes a `DeltaManager` whose purpose is to keep old diffable component state so senders can compute deltas. Its world replication, prediction, interpolation, and input facilities are separate modules/features.

Sources:

- [Lightyear 0.29.0 replication crate](https://docs.rs/lightyear_replication/0.29.0/lightyear_replication/)
- [Lightyear 0.29.0 DeltaManager](https://docs.rs/lightyear/0.29.0/lightyear/prelude/struct.DeltaManager.html)

This is not authority for RunenNet, but it reinforces two useful separations: delta-baseline history is a replication concern, and prediction/interpolation history should not be conflated with it.

## Resulting design pressure

The evidence supports the following minimal direction for normative review:

1. One **replication lineage** belongs to one admitted ParticipantId in the initial client/server profile. It survives temporary Unbound membership but ends with the participant incarnation.
2. `ReplicationCursor` is monotonic only inside one lineage and is not globally comparable with another lineage.
3. A full snapshot is a baseline-independent complete authoritative state image for that lineage at one cursor/tick.
4. A delta declares exactly one base cursor and one newer target cursor and denotes a transform from that exact base image to the complete target image.
5. The client may commit a delta only when `delta.base` equals its current committed cursor. A newer delta with another base cannot be applied to the current state.
6. Commit is atomic from the replication protocol perspective: validation/decoding/delta application/host acceptance succeed before the current baseline changes.
7. ACK means the client committed that cursor as its current authoritative baseline. It is not transport receipt or decode acknowledgement.
8. Duplicate ACK of the currently confirmed cursor should be idempotent; lower ACK is stale; future/unverifiable ACK cannot advance confirmation.
9. A valid client confirmation and server baseline availability are separate. If the confirmed state was evicted, the ACK can remain true while delta eligibility is false.
10. The client needs one current committed baseline plus bounded staging for the initial strict-current model, not arbitrary historical snapshots.
11. Server live-baseline retention is finite by count/bytes. Eviction of a consumer's latest confirmed baseline moves that lineage to full-snapshot-required recovery.
12. Full-snapshot-required recovery is persistent until a valid full snapshot is committed by the client and confirmed to the authority as a currently usable retained baseline.
13. A replacement connection does not inherit RN1B flow state. The initial replication profile conservatively requires a new full snapshot after rebind even if the ParticipantId/lineage persists.
14. Prediction, interest/view construction, schema identity, and archival replay remain separate future owners.

## Proposed normative ownership

The evidence supports two one-way owners:

- `spec/replication/consistency.md` — lineage/cursor, full/delta, commit, and acknowledgement semantics;
- `spec/replication/recovery.md` — live retention, baseline usability, connection-replacement recovery, and persistent full-snapshot-required state, depending on consistency semantics.

The intended dependency direction is acyclic: identity → session lifecycle → delivery → replication consistency → replication recovery.
