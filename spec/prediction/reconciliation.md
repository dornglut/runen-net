# Participant Input Prediction and Authoritative Reconciliation

Status: **provisional incomplete normative**

This document owns the initial RunenNet semantics for participant-originated simulation input, bounded local prediction state, and reconciliation of predicted host state after authoritative replication commits.

Participant and simulation-tick identity are defined by [Core identity and time](../core/identity.md). Participant admission, binding, removal, and connection replacement are defined by [Session and authority lifecycle](../session/lifecycle.md). Message acceptance and exposure are defined by [Delivery flow semantics](../delivery/flow.md), with delivery resource policy defined by [Delivery pressure and resource policy](../delivery/pressure.md). Authoritative replication commit is defined by [Authoritative replication consistency](../replication/consistency.md), and full-snapshot recovery barriers are defined by [Replication retention and full-snapshot recovery](../replication/recovery.md).

## Scope

This revision defines:

- one initial participant-input model for authoritative client/server simulation;
- participant input batches targeted at host-supplied `SimulationTick` values;
- semantic duplicate/conflict classification without a separate input sequence domain;
- monotonic authority-side admissible input windows and finite input-accounting requirements;
- deterministic authority input classification precedence;
- bounded participant-side pending prediction state;
- prediction continuity and invalidation;
- an authoritative reconciliation frontier for tracked local prediction;
- authoritative-commit-before-replay ordering;
- retirement of predicted input covered by authoritative progression;
- ordered replay of still-pending later input;
- reconciliation failure behavior and observable outcome classes;
- conservative prediction behavior across replication recovery and connection replacement.

Wire encoding, concrete message types, public Rust API shape, gameplay command meaning, ECS integration, engine scheduling, input sampling, simulation implementation, interpolation, presentation smoothing, lag compensation, archival replay, general rollback, deterministic lockstep, and transport-flow selection are not defined by this revision.

## Initial authority model

This revision applies to the existing single-authority client/server session model.

A participant may originate input describing requested simulation intent. The session authority remains authoritative over resulting replicated state.

Participant input does not grant authority over replicated state. Locally predicted application of input is speculative host behavior whose validity is always subordinate to later authoritative replication state.

## Participant input batch

A **participant input batch** is one finite opaque host-defined input value associated with:

- one admitted `ParticipantId` in one session; and
- one target `SimulationTick`.

RunenNet does not interpret the gameplay meaning of the batch. One batch may contain zero or more host commands internally, but that internal command structure is outside this specification.

The initial input model permits at most one distinct participant input batch for one participant at one target tick.

The semantic key of a participant input batch is therefore the tuple:

```text
(ParticipantId, target SimulationTick)
```

within one `SessionId` lifetime.

No separate RunenNet input sequence number is defined by this revision.

A participant may omit a batch for a tick. Tick values need not be contiguous across submitted batches.

## Batch immutability and duplicate classification

For one participant input key, the batch content is immutable.

After one endpoint has accepted or retained a batch for a participant/tick key:

- another observation of the same key with the same complete input value is **DuplicateInput**;
- another observation of the same key with different input content is **ConflictingInput**.

DuplicateInput MUST NOT cause the host simulation to apply the same semantic batch a second time.

ConflictingInput MUST NOT replace, merge with, or mutate the already accepted batch for that key and MUST NOT be exposed as an additional applicable input batch.

A realization MAY avoid storing complete batch contents if it retains bounded evidence sufficient to classify same-key duplicates and conflicts correctly. The evidence mechanism and concrete equality/fingerprint representation are implementation-defined, but collisions or truncation MUST NOT permit two distinct retained batch values to be treated as the same accepted input.

This semantic classification is independent of delivery duplicate handling. A delivery mode that already suppresses duplicate message exposure does not remove the input layer's same-key immutability rule.

## Authorization

A received participant input batch may be interpreted only after the session lifecycle authorizes the sending connection as the current binding for that `ParticipantId`.

Input received from an unadmitted connection, a connection not currently bound to that participant, a removed participant, or a closed session is **UnauthorizedInput** and MUST NOT become applicable participant input.

Delivery exposure by itself is not participant-input authorization.

## Authority input window

For each participant whose input may currently affect simulation, the authority maintains an explicit finite **input acceptance window** over `SimulationTick` values.

The window has:

- a minimum admissible target tick; and
- a maximum admissible target tick not less than the minimum.

The host supplies or advances this window according to its simulation lifecycle. RunenNet does not define when a particular game executes a tick.

For one participant incarnation, both the minimum and maximum admissible target ticks MUST be monotonically nondecreasing. Updating either bound MUST preserve `minimum <= maximum`.

The host MUST advance the minimum beyond a tick only after that tick is closed to ordinary participant input under this initial profile. Once a target tick becomes lower than the minimum, that target tick MUST NOT later become admissible again for the same participant incarnation.

The maximum MAY advance before the minimum as the host opens additional future simulation ticks for input, but it MUST NOT move backward to revoke a tick that was already inside the acceptance window. Resource or gameplay policy that needs to stop accepting otherwise admissible input must use an explicit rejection or lifecycle mechanism rather than silently redefining a previously open tick as never having been admissible.

For an authorized batch whose target tick is considered against the current window:

- a target tick lower than the minimum admissible tick is **StaleInput**;
- a target tick greater than the maximum admissible tick is **FutureInputOutsideWindow**;
- a target tick inside the window is an admissible candidate subject to duplicate/conflict and resource checks.

StaleInput and FutureInputOutsideWindow MUST NOT become applicable host input under this initial profile.

The authority MUST NOT keep an unbounded future-input horizon. The configured input window and all storage attributable to accepted/not-yet-expired input MUST remain finite.

This input window is not lag compensation. A future extension may define a different late-input or rollback policy without changing the meaning of this initial profile.

## Authority input classification order

For one newly exposed participant-input candidate, the authority MUST classify it in this order:

1. verify current participant/session authorization; otherwise classify UnauthorizedInput;
2. if the target tick is below the current minimum admissible tick, classify StaleInput;
3. if retained evidence already exists for the same participant/tick key, classify DuplicateInput for the same complete value or ConflictingInput for different content;
4. if the target tick is above the current maximum admissible tick, classify FutureInputOutsideWindow;
5. otherwise perform required resource admission and classify InputAccepted or InputResourceRejected.

Only InputAccepted creates a newly applicable host input batch.

This precedence means that once the monotonic minimum advances past a key, a later observation of that key is StaleInput even if duplicate/conflict evidence happens to remain retained. DuplicateInput and ConflictingInput describe repeated observations only while the key is still inside the current admissible window.

A batch rejected as FutureInputOutsideWindow does not reserve that participant/tick key. If the monotonic maximum later advances to include the target tick, a later observation may then be classified under the current window and accepted normally.

## Authority input acceptance

An admissible authorized batch becomes **InputAccepted** only after all RunenNet-owned resource admission required for correct duplicate/conflict and lifetime handling has succeeded.

InputAccepted means the input layer has accepted the participant/tick batch as applicable host input. It does not mean:

- the transport delivered it reliably;
- the host simulation has executed the target tick;
- the input changed gameplay state;
- the input will later be represented by an independent acknowledgement.

After InputAccepted, the realization MUST expose the batch to the host at most once as applicable input for that participant/tick key.

The host owns how an accepted batch is buffered, scheduled, or applied to simulation, provided later RunenNet semantic outcomes are reported truthfully.

If required RunenNet-owned accounting cannot admit the batch within configured bounds, the batch is InputResourceRejected and MUST NOT be reported as InputAccepted.

## Finite input resources

Before participant input becomes active, the realization MUST operate under explicit finite resource policy sufficient to bound every RunenNet-owned structure whose growth can be driven by participant input.

At minimum the policy MUST bound:

- maximum complete input-batch size attributable to one participant/tick key;
- maximum retained input keys/evidence per participant;
- maximum retained input bytes/evidence cost per participant;
- maximum future tick distance admitted by the authority input window;
- aggregate retained input keys/evidence across the session; and
- aggregate retained input bytes/evidence cost across the session.

If a realization retains complete batches for authority-side scheduling, those bytes count toward the corresponding bounds.

The realization MAY discard duplicate/conflict evidence for a tick once that tick is lower than the participant's monotonic minimum admissible input tick, because every later observation of that key is necessarily StaleInput and cannot become applicable again in that participant incarnation.

Resource pressure MUST NOT cause an already accepted participant/tick key to become silently reusable for different content while that key remains inside the admissible window.

## Local prediction

A participant endpoint MAY locally apply its own participant input speculatively before the corresponding authoritative result is observed.

Such application is **local prediction**. It does not change protocol authority and does not make the locally predicted state an authoritative replication state image.

A host MUST enable RunenNet-tracked prediction only for state that it can re-establish from the participant lineage's committed authoritative replication state and then advance again using the retained pending-input timeline. If the committed lineage state is insufficient to restore the prediction-relevant host state, that host state is not eligible for this prediction contract.

A participant input batch MUST NOT be applied as tracked RunenNet prediction unless the endpoint has first admitted that batch into its bounded pending-prediction state.

If pending-prediction resource admission fails, the endpoint MUST NOT apply the batch as RunenNet-tracked prediction and then forget the information required to reconcile it later.

A host MAY choose not to predict an otherwise valid local input batch.

## Prediction continuity state

For one active participant replication lineage, local prediction continuity is one of:

- **PredictionActive** — new local batches may be retained for prediction and later replay under this specification; or
- **PredictionInvalidated(reason)** — previously predicted continuity cannot be trusted and pending predicted input from the invalidated continuity MUST NOT later be replayed.

Prediction continuity is subordinate to the participant membership and replication lineage. This revision defines no separate wire-visible prediction epoch identifier.

A newly admitted participant begins PredictionInvalidated until a valid authoritative full replication state establishing the initial synchronized baseline has been committed for that lineage.

When that initial authoritative full state commits at `SimulationTick T`, the participant establishes its reconciliation frontier at T and may enter PredictionActive.

While PredictionActive, the reconciliation frontier is the latest successfully committed authoritative `SimulationTick` for the lineage. It MUST be monotonically nondecreasing. A newer authoritative commit at the same tick leaves the frontier unchanged; a commit at a later tick advances it.

## Pending predicted input

While PredictionActive, a locally predicted participant batch remains **pending predicted input** until this specification retires or invalidates it.

A new batch is eligible to enter pending prediction only when its target tick is strictly greater than the current reconciliation frontier. A local batch whose target tick is less than or equal to the frontier is **PredictionInputNotNewerThanFrontier** and MUST NOT enter pending prediction or be applied as RunenNet-tracked local prediction.

Pending predicted input is keyed by target `SimulationTick`. Because the initial model permits at most one distinct batch per participant/tick key, pending replay order is the ascending target-tick order.

For a new tracked-prediction candidate while PredictionActive, the participant endpoint MUST classify it in this order:

1. if its target tick is less than or equal to the current reconciliation frontier, classify PredictionInputNotNewerThanFrontier;
2. if a pending batch already exists for the same participant/tick key, classify DuplicateInput for the same complete value or ConflictingInput for different content;
3. otherwise perform pending-prediction resource admission and either retain the immutable batch as pending predicted input or classify PendingPredictionResourceRejected.

Only a newly retained pending batch may then be applied as tracked local prediction.

The participant endpoint MUST impose finite bounds on:

- maximum pending predicted batch count;
- maximum pending predicted batch bytes/accounted cost; and
- maximum target-tick distance above the current reconciliation frontier represented by pending predicted batches.

Exact numeric defaults are not defined by this revision.

A locally predicted batch may remain pending whether its current delivery submission is Accepted, Rejected, lost, or awaiting retry. Delivery state does not by itself prove whether the authority applied the input.

If a host retries submission, it MUST retry the same immutable participant/tick batch value. It MUST NOT submit different input under the same participant/tick key.

## Authoritative reconciliation frontier

A successfully committed authoritative replication state at `SimulationTick T` establishes or updates the reconciliation frontier at T for the participant lineage according to the monotonic rule above.

The frontier means that local predicted input targeted at tick T or earlier is no longer eligible to be replayed on top of that authoritative state.

This is not an assertion that every such local batch was received or executed by the authority. The authoritative state is final for replay purposes regardless of whether an individual earlier local input was applied, lost, rejected, or superseded by host simulation policy.

The reconciliation frontier is therefore not an independent input acknowledgement.

A participant MUST NOT infer per-input receipt or execution merely because authoritative replication advanced to or beyond an input's target tick.

## Commit-before-replay ordering

Prediction reconciliation occurs only after a newer authoritative replication candidate has successfully completed the commit rules owned by the authoritative replication specification.

For a successful authoritative commit at target tick T, the participant-side reconciliation order is:

1. the complete authoritative replication target is validated/reconstructed and atomically committed under the replication specification;
2. the reconciliation frontier is established or advanced to T;
3. pending predicted batches whose target tick is less than or equal to T are retired without replay;
4. remaining pending predicted batches with target ticks greater than T are replayed in ascending target-tick order;
5. only after required replay succeeds may the predicted host state be reported as reconciled under the current prediction continuity.

A failed authoritative validation, reconstruction, or commit MUST NOT advance the reconciliation frontier, retire pending predicted input, or trigger replay as though the candidate had committed.

This document does not change replication acknowledgement meaning. A replication acknowledgement may truthfully confirm the authoritative commit even when later local prediction replay fails, because prediction replay does not retroactively uncommit authoritative replication state.

## Replay semantics

**Replay** is the host operation that advances speculative prediction again from the newly committed authoritative state using the still-pending local participant input timeline.

RunenNet defines which batches remain eligible and the ascending target-tick order in which they are presented to the host. The host owns the simulation mechanics required to realize that replay, including application of the opaque input, execution of any required intervening simulation steps, and reconstruction of other host-local prediction state.

RunenNet does not require replay to be implemented as direct command reapplication, ECS mutation, saved-world rollback, or any particular simulation algorithm.

The host MUST begin replay from state semantically consistent with the newly committed authoritative target, not from a partially retained pre-correction predicted state.

For every pending batch presented for replay, the host MUST preserve that batch's target-tick meaning. A batch targeted at a later tick MUST NOT be applied as though it belonged to an earlier simulation step merely to simplify replay implementation.

A replayed batch remains pending after successful replay. It is retired only when a later authoritative reconciliation frontier reaches its target tick or prediction continuity is invalidated.

## Replay failure

Replay of all required still-pending batches is one prediction-reconciliation operation.

If replay of any required batch or required intervening prediction step fails, the participant MUST NOT report the predicted host state as successfully reconciled.

The host integration MUST provide staging, restoration, re-establishment from the committed authoritative state, or another mechanism sufficient to prevent a partially replayed predicted state from being treated as valid current prediction.

After replay failure:

- the authoritative replication commit and resulting reconciliation frontier remain valid;
- prediction continuity becomes PredictionInvalidated with a replay-failure reason;
- pending predicted input from that invalidated continuity MUST NOT later be replayed; and
- the endpoint MUST return to a known authoritative host state before prediction is re-enabled.

A realization MAY re-enable PredictionActive from the already committed authoritative state once the host has explicitly re-established that state and cleared the invalid pending-prediction continuity. It need not request a new network full snapshot solely because local replay failed, unless another replication rule independently requires full recovery.

## Reconciliation outcomes

A conforming realization MUST make at least these prediction/input outcomes distinguishable to its conformance or host-integration boundary:

Authority-side input outcomes:

- InputAccepted;
- DuplicateInput;
- ConflictingInput;
- StaleInput;
- FutureInputOutsideWindow;
- InputResourceRejected;
- UnauthorizedInput.

Participant-side prediction outcomes/states:

- PredictionActive;
- PredictionInvalidated with a reason class;
- PredictionInputNotNewerThanFrontier;
- PendingPredictionResourceRejected;
- authoritative commit with no pending replay required;
- authoritative commit with one or more still-pending batches replayed successfully;
- replay failure after authoritative commit.

Concrete public enum/type names are not defined by this revision.

Diagnostics presentation, counters, logs, traces, and UI are outside this specification.

## Replication recovery barrier

When the replication lineage enters client `FullSnapshotRequired`, local prediction continuity for that lineage MUST become PredictionInvalidated before any later recovery reconciliation occurs.

All pending predicted input from the invalidated continuity MUST be discarded from RunenNet replay eligibility.

While the lineage remains FullSnapshotRequired:

- the endpoint MUST NOT replay invalidated pre-recovery predicted input;
- the endpoint MUST NOT treat retained pre-recovery prediction state as a valid base for new correction;
- RunenNet-tracked local prediction under this initial profile is suspended.

After a valid newer full snapshot commits and clears the replication recovery barrier, the participant establishes a fresh reconciliation frontier at that full snapshot's `SimulationTick` and may establish a fresh PredictionActive continuity from that authoritative state.

The host may independently collect user intent while recovery is in progress, but such collection is not pending RunenNet prediction under this revision unless a later specification defines how it becomes safe post-recovery input.

## Connection loss and replacement

When the currently bound transport connection is lost and the participant membership becomes unbound, participant-side prediction continuity becomes PredictionInvalidated.

An authorized replacement connection does not restore the prior prediction continuity.

The existing replication recovery specification requires a fresh qualifying full authoritative baseline after replacement. Only after that baseline is committed and the recovery barrier clears may a new PredictionActive continuity begin.

Accepted/unexposed delivery messages from the old connection are not transferred to the replacement connection, as defined by the delivery specification. Pre-replacement pending predicted input MUST NOT be replayed after the replacement full baseline under this initial profile.

Authority-side input-window bounds and accepted-key evidence are scoped to the retained participant incarnation, not to one transport connection. Temporary unbinding or authorized replacement MUST NOT move an authority input-window bound backward or reset still-required accepted-key evidence in a way that permits a participant/tick key to become newly applicable a second time.

This conservative rule deliberately does not define advanced reconnect input continuity.

## Participant removal and session close

When a participant membership ends, all participant-input duplicate/conflict evidence and pending local prediction state scoped solely to that participant MAY be released immediately and MUST NOT be reused for a later participant incarnation.

When the session closes, all session-scoped input and prediction state terminates.

A later participant incarnation is a distinct identity even if it corresponds to the same external account or local player.

## Delivery relationship

This specification does not select a delivery mode for participant input.

A conforming profile or host may choose an accepted delivery mode only if the composed behavior still satisfies this input specification.

In particular:

- `UnreliableUnordered` may expose transport/network duplicates, which the participant/tick duplicate rule prevents from becoming duplicate host application;
- `UnreliableSequenced` may suppress older input messages after a newer message is exposed, which is permitted only if the host accepts the possibility that those older inputs are lost and later authoritative state corrects prediction;
- `ReliableOrdered` provides stronger delivery but does not change participant-input identity or authoritative reconciliation semantics.

Changing delivery flows, packet numbers, transport sequence values, or connection identifiers MUST NOT change the participant/tick input key.

## No independent input acknowledgement in the initial model

This revision defines no independent input acknowledgement channel, input-confirmation cursor, or input-delivery receipt.

The authoritative replication reconciliation frontier controls retirement from local prediction replay, but it does not prove per-input receipt or execution.

A future extension may define explicit input acknowledgements only if an independent requirement demonstrates that authoritative state progression is insufficient. Such an extension MUST use its own normative owner and MUST NOT change the truth conditions of existing replication acknowledgements.

## No general rollback contract

This revision does not require the authority to rewind simulation for late input and does not define deterministic rollback history.

Stale input is rejected by the initial authority input window rather than retroactively inserted into an already closed simulation tick.

Client-side replay of still-pending local input after authoritative correction is prediction reconciliation, not a general rollback/history system.

Lag compensation, server rewind, deterministic lockstep, archival replay, and checkpoint restoration remain outside this revision.

## Required conformance cases

A realization claiming this semantic area MUST be testable for at least the following cases:

1. one accepted participant/tick batch is exposed to the authority host at most once despite a same-value duplicate;
2. conflicting content for an already accepted participant/tick key is rejected without replacing the accepted batch;
3. input below the minimum admissible tick is StaleInput even when old duplicate evidence remains;
4. authority input-window bounds never move backward, and a key below the advanced minimum cannot later become admissible again in the same participant incarnation;
5. input beyond the finite future window is FutureInputOutsideWindow and may become admissible only after the monotonic maximum advances to include it;
6. input/resource saturation fails explicitly without unbounded growth or silent key reuse;
7. an unauthorized connection cannot create applicable participant input;
8. a locally predicted batch cannot be applied as tracked prediction when pending-prediction admission failed;
9. a local tracked-prediction candidate at or before the reconciliation frontier is PredictionInputNotNewerThanFrontier and is not applied;
10. same-key local pending prediction is classified deterministically as DuplicateInput or ConflictingInput while newer than the frontier;
11. authoritative commit at tick T advances/establishes the frontier and retires all pending predicted batches at ticks less than or equal to T;
12. after that commit, pending batches later than T replay exactly once for that reconciliation in ascending target-tick order with their target-tick meaning preserved;
13. an authoritative candidate that fails before commit does not advance the frontier, retire, or replay pending prediction;
14. replay or intervening prediction-step failure leaves the authoritative commit/frontier valid but invalidates prediction continuity and does not expose a partially replayed state as valid prediction;
15. entering FullSnapshotRequired invalidates and clears pre-recovery replay eligibility;
16. authorized connection replacement does not replay pre-replacement pending prediction after the required replacement full baseline and does not reset authority input-window/key identity;
17. participant removal and session close prevent old input/prediction state from becoming applicable to a later participant/session lifetime.

## Open items

Not defined by this revision:

- concrete wire representation for participant input;
- public API/type names;
- a default input delivery mode;
- exact numeric resource defaults;
- independent input acknowledgements;
- multiple distinct semantic input batches for one participant at one tick;
- authority rollback or acceptance of stale input;
- advanced reconnect prediction/input continuity;
- prediction across multiple simultaneous authorities;
- deterministic simulation requirements;
- interpolation or presentation correction policy.
