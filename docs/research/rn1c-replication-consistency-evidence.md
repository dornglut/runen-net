# RN1C Replication Consistency Evidence

Status: **non-normative**

This record supports [RN1C](https://github.com/dornglut/runen-net/issues/8). It compares pinned Runenwerk migration evidence with established snapshot-replication designs. It does not define RunenNet semantics.

Evidence snapshot:

- Runenwerk: `37a267e41e49317516d6513b02794f8fc480056a` (observed 2026-08-24)
- Lightyear replication: `0.29.0` documentation (observed 2026-08-24)
- Valve Source multiplayer networking documentation (observed 2026-08-24)
- Gaffer on Games, “Snapshot Compression” (2015)
- RN1A and RN1B: accepted RunenNet normative specification on the RN1C base revision

## Current Runenwerk evidence

### The active design's strict-current claim conflicts with the server's acknowledged-baseline strategy

The active Runenwerk authoritative replication design says a client delta must advance from the client's current cursor and claims strict delta-base validation is implemented.

Source: [Runenwerk authoritative replication design](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/docs-site/src/content/docs/design/active/net-authoritative-replication-protocol.md).

The client implementation does not enforce `delta.base == last_cursor`. It accepts any declared base that remains in its retained `snapshots` map, reconstructs a merged target state from that base, and then emits an apply plan containing only the incoming delta operations.

Source: [Runenwerk client replication runtime](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/runtime/client.rs).

At first glance this looks like a missing strict-current check. Deeper review shows that conclusion is incomplete because the authority deliberately constructs deltas from the latest client-ACKed cursor.

Source: [Runenwerk authoritative server runtime](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/runtime/server.rs).

Suppose cursor 1 is the authority's last received ACK. The authority can send delta `1→2`; before ACK 2 returns, it can legitimately send another current delta `1→3`. Requiring `base == current` would make `1→3` unusable after the client commits `1→2`, unless the sender waits an RTT for every delta or switches to a fragile unacknowledged chaining model.

The actual Runenwerk correctness defect is therefore more specific: the client reconstructs target 3 from historical base 1, but its host apply plan contains only the `1→3` delta operations and applies those operations to whatever host state is currently installed. That mixes **historical-base reconstruction** with **current-state patch application**.

The standalone semantics should instead make a delta a transform from exactly its declared retained base to one complete target state image. Once that target is reconstructed, committing it means atomically establishing/reconciling the complete target as current state. The delta operation list must never be blindly applied as though it had been derived from the current host state.

### Acknowledged-baseline deltas are established practice

Valve's Source networking documentation describes world updates as delta-compressed against the last acknowledged update, with full snapshots used for initial/recovery cases.

Source: [Source Multiplayer Networking](https://developer.valvesoftware.com/wiki/Source_Multiplayer_Networking).

Gaffer on Games describes the same core pattern: the sender encodes a new snapshot relative to a baseline the receiver has acknowledged, and updates that baseline when a newer ACK arrives. This lets the sender continue transmitting snapshots while tolerating loss and RTT.

Source: [Snapshot Compression](https://gafferongames.com/post/snapshot_compression/).

These are comparison evidence, not RunenNet authority. They demonstrate why acknowledged-baseline reconstruction is useful and why strict-current-only deltas would be an unnecessary protocol restriction.

### Recovery need is currently a destructive boolean

On a missing delta base, the client sets `needs_full_resync = true`. `take_resync_request()` returns the boolean and immediately clears it. A failed downstream enqueue/send can therefore erase the recovery requirement without a successful full snapshot.

Malformed delta decode returns `DecodeError` without setting that recovery flag, despite the active design saying malformed deltas require full resynchronization.

Source: [Runenwerk client replication runtime](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/runtime/client.rs).

This supports persistent semantic recovery state that is cleared by a successful recovery transition, not by observation.

### Client baseline history is useful, but it is unbounded by protocol policy

Because acknowledged-baseline deltas may refer to an older committed cursor, the client does need bounded historical committed state. The current `ClientReplicationRuntime` retains every decoded/merged snapshot in a `BTreeMap` until reset, with no protocol-level count/byte policy.

Source: [Runenwerk client replication runtime](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/runtime/client.rs).

The correct conclusion is not “retain only current”; it is “retain only a finite live replication baseline history.” If an incoming delta references a baseline already evicted under that policy, the client cannot reconstruct it and must recover with a full snapshot.

Prediction/interpolation history and archival replay remain separate retention domains.

### Server ACK validation improved but still conflates confirmation with retention

`AuthoritativeServerRuntime` now rejects stale, future, unsent, and pruned ACKs before changing `last_acknowledged`. This is stronger than the older active design's partial-contract description.

Source: [Runenwerk authoritative server runtime](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/runtime/server.rs).

However, a pruned cursor is rejected as though the client's acknowledgement were semantically false. These are separate facts:

- the client may truthfully have committed a cursor that the authority emitted;
- the authority may no longer retain the exact state needed to use that cursor as a delta-compression baseline.

A standalone protocol should preserve the first fact while treating the second as loss of delta eligibility requiring a full snapshot.

The current implementation also treats an ACK equal to the last acknowledged cursor as stale rejection. Idempotent duplicate acknowledgement is safer: equal confirmation can be a no-op, while lower confirmation is stale.

### Baseline/checkpoint authority is duplicated across layers

`AuthoritativeServerRuntime` owns per-connection `last_acknowledged`, `force_full_snapshot`, and `sent_cursors` plus a snapshot timeline.

The engine plugin independently owns `ConnectionBaselineCheckpoint` with `last_ack_cursor`, `last_sent_cursor`, `last_full_snapshot_cursor`, `needs_full_resync`, and bounded `sent_cursors`, plus global and per-connection snapshot histories.

Sources:

- [Runenwerk authoritative server runtime](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/runtime/server.rs)
- [Runenwerk engine networking replication state](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/engine/src/plugins/net/resources.rs)

This supports one RunenNet-owned semantic replication state per participant lineage. Engine adapters may project or inspect that state but should not independently redefine ACK, baseline, or recovery authority.

### Timeline retention is explicit but not governed by one bounded policy

`SnapshotTimeline` stores complete snapshots in a map and only releases them when `prune_before` is called. The engine plugin has additional snapshot-history maps and a separately bounded sent-cursor set.

Sources:

- [Runenwerk snapshot timeline](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/replication/timeline.rs)
- [Runenwerk engine networking replication state](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/engine/src/plugins/net/resources.rs)

A slow or disconnected consumer must not force unbounded live-state retention. Both sides may evict historical baseline state under finite policy. Eviction is a recoverable compression-state loss, not permission to apply a delta to the wrong base.

### Current cursor scope follows implementation mechanics, not a demonstrated semantic boundary

`SnapshotTimeline` allocates cursors from one timeline while server APIs build snapshots for individual connections. Engine integration also keeps per-connection histories because different consumers can receive different payloads.

Sources:

- [Runenwerk snapshot timeline](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/replication/timeline.rs)
- [Runenwerk engine networking replication state](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/engine/src/plugins/net/resources.rs)

Because later interest/relevancy can make each participant's authoritative projection different, bare cursors should not acquire accidental global comparability. A ParticipantId-scoped replication lineage gives the initial core a clear baseline domain while remaining independent of transport connections.

### Snapshot cursor and simulation tick are already separate concepts

Runenwerk snapshot and delta messages carry both `SimulationTick` and `SnapshotCursor`. ACKs also carry a cursor and a last-received tick.

Sources:

- [Runenwerk snapshot protocol](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/protocol/snapshot.rs)
- [Runenwerk ACK protocol](https://github.com/dornglut/runenwerk/blob/37a267e41e49317516d6513b02794f8fc480056a/net/engine_net/src/protocol/ack.rs)

That separation is correct. A replication cursor orders authoritative state revisions in its lineage; a simulation tick names host logical simulation time; RN1B delivery sequence/order is a third domain.

## External framework comparison

### Lightyear keeps delta state as replication-specific retained state

Lightyear 0.29.0 exposes a `DeltaManager` whose purpose is to keep old diffable component state so senders can compute deltas. Its world replication, prediction, interpolation, and input facilities are separate modules/features.

Sources:

- [Lightyear 0.29.0 replication crate](https://docs.rs/lightyear_replication/0.29.0/lightyear_replication/)
- [Lightyear 0.29.0 DeltaManager](https://docs.rs/lightyear/0.29.0/lightyear/prelude/struct.DeltaManager.html)

This reinforces two useful separations: delta-baseline history is a replication concern, and prediction/interpolation history should not be conflated with it.

## Additional recovery-state review

The initial recovery draft also needed a stale-ack boundary that Runenwerk's boolean model does not provide. A full snapshot sent during one recovery attempt must not be able to clear a later recovery episode after connection replacement or another explicit recovery boundary.

That leads to a local authority-side **recovery generation**: qualifying recovery full snapshots are created/designated after the generation begins, are newer than its start cursor watermark, and remain associated with that generation in emitted-snapshot evidence. An ACK for an older recovery full can still be a truthful replication confirmation, but it cannot satisfy a newer recovery generation.

The same boundary must prevent not-yet-emitted delta work from crossing from `DeltaEligible` into `FullSnapshotRequired`. Already emitted deltas remain historical facts; queued candidates that have not yet reached RN1B acceptance must be invalidated.

These requirements are state-machine correctness constraints derived from RN1A/RN1B and the replication model; they are not imported from an external framework.

## Resulting design pressure

The evidence supports the following minimal direction for normative review:

1. One **replication lineage** belongs to one admitted ParticipantId in the initial client/server profile. It survives temporary Unbound membership but ends with the participant incarnation.
2. `ReplicationCursor` is monotonic only inside one lineage and is not globally comparable with another lineage.
3. A full snapshot is a baseline-independent complete authoritative state image for that lineage at one cursor/tick.
4. A delta declares exactly one base cursor and one newer target cursor and denotes a deterministic transform from that exact complete base image to a complete target image.
5. The authority uses its latest client-confirmed cursor as the delta-compression baseline while that exact state remains available. Multiple newer snapshots may legitimately share that ACKed base until a newer confirmation arrives.
6. For a delta whose target is newer than the client's current state, the client may reconstruct from any retained committed base named by the delta. The base need not equal current.
7. Reconstruction and host commit are separate: after reconstructing the complete target from the exact historical base, the client atomically establishes/reconciles the complete target as current. Delta operations are never blindly applied against an unrelated current host state.
8. If the declared baseline is no longer retained, the delta cannot be reconstructed and persistent full-snapshot recovery is required.
9. ACK means the client committed that cursor as its current authoritative state. It is not transport receipt or decode acknowledgement.
10. Duplicate ACK of the currently confirmed cursor is idempotent; lower ACK is stale; future or unverifiable ACK cannot advance confirmation.
11. A valid client confirmation and authority baseline availability are separate. If the confirmed state was evicted, confirmation remains true while delta eligibility becomes false.
12. Client and authority live-baseline histories are finite by count and bytes. Aggregate lineage counts and bytes are also finite. Eviction may increase full-snapshot recovery frequency but never weakens correctness.
13. Client `FullSnapshotRequired` persists until the client successfully commits a valid newer full snapshot. Reading or reporting recovery state never clears it.
14. Authority `FullSnapshotRequired` persists until a qualifying full snapshot from the current recovery generation is Confirmed and its exact state remains BaselineAvailable.
15. Recovery generations prevent delayed ACKs, re-tagged older full snapshots, and connection-replacement races from falsely restoring delta eligibility.
16. Crossing into authority recovery invalidates not-yet-emitted delta work for that lineage; already emitted delivery remains historical and is handled by client candidate/recovery rules.
17. A replacement connection does not inherit RN1B flow state. The initial replication profile conservatively starts a fresh full-snapshot recovery generation after rebind even if the ParticipantId and replication lineage persist.
18. Prediction, interest/view construction, schema identity, and archival replay remain separate future owners.

## Proposed normative ownership

The evidence supports two one-way owners:

- `spec/replication/consistency.md` — lineage/cursor, full/delta reconstruction, commit, and acknowledgement semantics;
- `spec/replication/recovery.md` — live retention, baseline usability, recovery generation, connection-replacement recovery, and persistent full-snapshot-required state, depending on consistency semantics.

The intended dependency direction is acyclic: identity → session lifecycle → delivery → replication consistency → replication recovery.
