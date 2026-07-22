# FleetRollout Reconcile Loop — Design Document

**Status:** Draft for MVP implementation
**Scope:** `FleetRollout` controller (`fleet.fleetrollout.io/v1alpha1`), kubebuilder v4 / controller-runtime
**Audience:** implementers of the reconcile loop

---

## 1. The central design decision: how a "wave" maps to Kubernetes objects

The spec says "roll out `image` to a wave of *Nodes*". Nodes don't run images; workloads do. So the real question is: **which Kubernetes object carries the image onto a node, and how does the controller scope it to a wave?**

### Option A — Managed DaemonSet, node affinity/selector widened per wave

The controller owns a single DaemonSet (`RollingUpdate` disabled is irrelevant here; the DS simply never targets un-promoted nodes). Each promoted node gets a label `fleet.fleetrollout.io/wave-approved: <rollout-name>` (or the DS `nodeAffinity` lists node names per wave), and the DaemonSet's node selector matches only approved nodes. Promotion = label more nodes (or patch the affinity term list).

* **Pros:** one owned object; DS controller handles pod lifecycle, restarts, node reboots; naturally level-based (DS spec is the desired state).
* **Cons:**
  * If promotion is done by **labeling Nodes**, the controller mutates cluster-scoped objects it does not own. Owner references are impossible (Node outlives the rollout), cleanup requires finalizer bookkeeping, and two FleetRollouts targeting overlapping nodes fight over labels.
  * If promotion is done by **patching a giant `nodeAffinity` matchExpressions/matchFields list**, the DS spec grows O(fleet) and every wave rewrites it — noisy diffs, and node-name-based affinity (`metadata.name In [...]`) has practical size limits on large fleets.
  * Rollback semantics are awkward: shrinking the selector *removes* the workload from nodes rather than reverting them to the old image. If a previous version must keep running (typical for edge agents), you need a *second* DaemonSet anyway.

### Option B — Two DaemonSets is overkill; **one DaemonSet with `updateStrategy: OnDelete`, operator deletes pods per wave** (RECOMMENDED)

The controller owns exactly one DaemonSet:

* `spec.template.spec.containers[0].image = spec.image` (the *new* image),
* `spec.selector`/`template` node affinity = the resolved `targetSelector` (all target nodes, always — the DS covers the whole fleet from day one),
* `spec.updateStrategy.type = OnDelete`.

With `OnDelete`, updating the DS template does **not** restart any pod. A pod only picks up the new template when it is deleted. So:

* **"Roll out to a wave" = delete the DS pods on that wave's nodes.** The DaemonSet controller immediately recreates them from the current (new-image) template.
* **"A node is updated" = its DS pod's template hash / image matches the current DS template and the pod is Ready.** This is directly observable — perfect for level-based reconciliation. The controller never stores "I already did wave 2" as the source of truth; it *derives* progress from pod state on every reconcile.
* **"Rollback" = set the DS template image back to the last-good image, then delete the already-updated pods** so they recreate on the old image. Same primitive, opposite direction.

**Pros:**

* Single owned object with a clean ownerRef → garbage collection deletes everything when the FleetRollout is deleted.
* No mutation of Nodes or any object the controller doesn't own.
* Idempotent by construction: deleting an already-recreated pod is prevented by checking the pod's template hash first; "delete pods on wave-N nodes whose hash ≠ current" is safe to repeat.
* Uses the DS controller for the hard parts (scheduling, restarts, node drain/reboot recovery, tolerations).
* This is exactly the mechanism kubectl-era operators (and OpenKruise's Advanced DaemonSet, Kubernetes' own historical "manual DS rollout" guidance) use — well-trodden.

**Cons / accepted costs:**

* ~~First deployment: the DS schedules the image to **all** target nodes at once, bypassing waves.~~ **Closed in v0.3 (C4).** The DS template carries a `fleetrollout.fleet.fleetrollout.io/wave` pod `schedulingGate`, so every DS-created pod is born SchedulingGated (unscheduled, zero node footprint); the controller removes the gate per wave, after the prior wave's health gate passes. Scheduling is therefore wave- and gate-bounded from the very first deploy, and a node that joins mid-rollout stays gated (running nothing) until it is included in a fresh plan — no more "new node gets the ungated new image immediately" bypass.
* Update granularity is pod-level, so we identify "wave membership" by pod→node mapping at delete time.

### Option C — Controller creates bare per-node Pods (or per-wave Jobs) itself

The controller lists target nodes and creates one Pod per node (nodeName pinned), per wave.

* **Pros:** total control over pacing; wave = "the set of pods I created this round"; no DS quirks.
* **Cons:** the controller re-implements the DaemonSet controller — pod restarts on failure, node reboot recovery, eviction handling, stuck-terminating pods, garbage collection races. Bare pods are not recreated when a node reboots. This is a large, bug-prone surface for zero MVP benefit.

### Decision

> **Option B: one controller-owned DaemonSet with `updateStrategy: OnDelete`; the operator advances a wave by deleting that wave's stale pods.**
> Owner reference: `FleetRollout → DaemonSet` (controller ref). Pods are owned by the DaemonSet as usual.
> The controller's per-node "generation" signal is the DS `pod-template-generation` label / template hash on each pod, compared against the DS's current template — a purely observed, level-based signal.

---

## 2. State machine

```
                       ┌────────────────────────────────────────────┐
                       │                                            │
        spec created   ▼                                            │ spec.image changed
      ┌──────────► Progressing ──────────────────────────────► (restart rollout:
      │                │  ▲                                     currentWave=0, stays
      │                │  │ user clears paused /                Progressing)
      │                │  │ healthGate passes after retry
      │                │  │
      │   health gate  │  └──────────── Paused ◄── spec.paused=true (later; MVP: N/A)
      │   timeout AND  │                  ▲
      │   rollbackPolicy=Never ───────────┘   (gate timeout + Never ⇒ Paused,
      │                │                       waiting for human)
      │                │
      │   health gate timeout AND rollbackPolicy=OnFailure
      │                │
      │                ▼
      │           RollingBack ──── all pods back on last-good image ───► RolledBack   (terminal*)
      │                                                                     │
      │   all waves updated+ready, last gate passed                         │ spec.image changed
      ▼                                                                     ▼
     Done  (terminal*)                                          (new rollout: Progressing)
```

`*terminal`: `Done` and `RolledBack` are terminal **for the current `spec.image` + observed spec generation**. Any spec change that alters the effective rollout (new image, new selector) re-enters `Progressing` from wave 0. `RollingBack` is not a spec phase enum value in v1alpha1 — it is represented as `phase: Progressing` + condition `RollingBack=True` during the transition, then `phase: RolledBack` when complete. (Alternative: add it to the enum later; MVP maps it onto conditions to keep the published enum as specified.)

**Transition triggers (all evaluated inside Reconcile — level-triggered):**

| From | To | Trigger (observed condition, not event) |
|---|---|---|
| (none) | Progressing | FleetRollout exists, DS reconciled, work remains (some target pod hash ≠ current template) |
| Progressing | Progressing (wave++) | Current wave fully updated + Ready **and** health gate passed |
| Progressing | Paused | Health gate timed out and `rollbackPolicy: Never` |
| Progressing | RolledBack (via RollingBack condition) | Health gate timed out and `rollbackPolicy: OnFailure`; rollback completes when all previously-updated pods observe the last-good template |
| Progressing | Done | Every target node's pod matches current template and is Ready; final gate passed |
| Done / RolledBack / Paused | Progressing | Spec changed (`metadata.generation ≠ status.observedGeneration` with a different effective image) |

---

## 3. Reconcile pseudocode (level-triggered, no blocking sleeps)

```text
Reconcile(ctx, req):
    fr = Get(FleetRollout, req)                     // NotFound → return, GC handles DS
    if fr.DeletionTimestamp != nil: return          // ownerRef GC cleans DS; no finalizer needed in MVP

    // ---- 1. Ensure the owned DaemonSet matches spec (idempotent apply) ----
    lastGood = fr.annotations["fleet.fleetrollout.io/last-good-image"]   // recorded before starting a new image
    desiredImage = fr.spec.image
    if rollbackInProgress(fr):                       // condition RollingBack=True
        desiredImage = lastGood

    ds = CreateOrUpdate(ownedDaemonSet(fr, desiredImage))    // OnDelete strategy, targetSelector affinity
    // CreateOrUpdate is a no-op if already in desired shape → idempotent

    // ---- 2. Observe (never trust memory; derive everything) ----
    nodes   = List(Nodes, matching fr.spec.targetSelector)   // only Ready nodes count as actionable
    order   = stableSort(nodes)                              // §4
    waves   = partition(order, fr.spec.waveSize)             // §4
    pods    = List(Pods, owned by ds)                        // index by spec.nodeName
    updated(node) := pod on node has current DS template hash AND pod.Ready
    stale(node)   := pod exists with old hash, or hash matches but not yet Ready

    // ---- 3. Detect spec change mid-flight ----
    if fr.status.observedImage != fr.spec.image and not rollbackInProgress:
        record lastGood = fr.status.observedImage (annotation, only if previous rollout was Done)
        reset: status.currentWave = 0, phase = Progressing, clear gate-start annotation
        // fall through — the DS update above already carries the new image

    // ---- 4. Terminal checks (derived, not remembered) ----
    if all target nodes updated and Ready:
        if rollbackInProgress:
            phase = RolledBack; set conditions; update status; return (no requeue)
        else:
            phase = Done; set conditions; update status; return (no requeue)

    // ---- 5. Find the current wave = first wave containing a non-updated node ----
    w = first index i where any node in waves[i] is not updated(node)
    status.currentWave = w; status.totalWaves = len(waves)
    status.updatedNodes = count(updated)

    // ---- 6. Is the current wave still converging? ----
    if any node in waves[w] has a pod with current hash but not Ready:
        // DS controller is recreating pods; just wait — watch on Pods will retrigger us,
        // but requeue as a backstop for missed events / offline nodes
        set condition WaveReady=False (reason=PodsPending)
        update status; return RequeueAfter(15s)

    if any node in waves[w] has a stale pod:
        // ---- act: advance the wave by deleting stale pods ----
        for node in waves[w] where stale(node):
            Delete(pod(node))            // idempotent: only stale-hash pods are ever deleted
        update status; return RequeueAfter(10s)

    // ---- 7. Wave w is fully updated + Ready → health gate before promotion ----
    if fr.spec.healthGate != nil and not rollbackInProgress:
        gateStart = annotation "gate-start-wave-<w>" (set now if absent)  // persisted, restart-safe
        result = evaluatePromQL(fr.spec.healthGate)   // one bounded HTTP call, ~5s client timeout
        if result == HEALTHY:
            set condition HealthGatePassed=True (wave w)
            // loop continues: next reconcile sees wave w fully updated → w+1 becomes current
            update status; return Requeue (immediate)
        if now - gateStart < timeoutSeconds:
            set condition HealthGatePassed=False (reason=Evaluating)
            update status; return RequeueAfter(gatePollInterval)   // e.g. 15s
        // ---- gate timed out ----
        if fr.spec.rollbackPolicy == OnFailure:
            set condition RollingBack=True (reason=HealthGateTimeout)
            update status; return Requeue     // next pass flips DS image to lastGood (§6)
        else:  // Never
            phase = Paused
            set condition HealthGatePassed=False (reason=Timeout)
            update status; return (no requeue; human edits spec to resume)

    // no gate, or rollback path: promotion is implicit — next reconcile computes w+1
    update status; return Requeue (immediate)
```

**Requeue strategy summary**

| Situation | Return |
|---|---|
| Waiting for DS pods to recreate/become Ready | `RequeueAfter(15s)` (watches on owned pods also trigger) |
| Just deleted a wave's pods | `RequeueAfter(10s)` |
| Health gate evaluating, not yet timed out | `RequeueAfter(15s)` — the *only* place we poll an external system; never `time.Sleep` |
| Done / RolledBack / Paused | no requeue (spec-change watch re-triggers) |
| Transient API error | return err (controller-runtime backoff) |

**Watches:** primary `FleetRollout`; `Owns(&DaemonSet{})`; `Watches(&Pod{}, ownedByOurDS)` (or rely on DS status changes); `Watches(&Node{}, mapToRolloutsSelecting(node))` so node add/remove re-triggers affected rollouts.

**Status updates:** always via the status subresource (`Status().Update`), once per reconcile at the end; compare-before-write to avoid hot loops.

---

## 4. Wave partitioning — deterministic and restart-stable

Requirements: a controller restart, cache resync, or node-list reorder must **not** reshuffle which wave a node belongs to.

* **Ordering key:** sort target nodes by `metadata.name` (lexicographic). Node names are unique and stable. Optionally hash-prefix later for spreading; name-sort is the MVP.
  * Why not creationTimestamp: ties are possible; names never tie.
* **`waveSize` resolution** (against N = number of currently-matching target nodes):
  * Integer `5` → `size = 5`.
  * Percentage `"20%"` → `size = ceil(N * 20 / 100)` — `ceil`, so `"20%"` of 3 nodes is 1, never 0. Use `intstr.GetScaledValueFromIntOrPercent(waveSize, N, /*roundUp=*/true)`.
  * Clamp: `size = max(1, min(size, N))`.
* **Partition:** `waves[i] = sortedNodes[i*size : min((i+1)*size, N)]`; `totalWaves = ceil(N / size)`.
* **Stability under membership change:** partitioning is recomputed from the live node list every reconcile (level-based). A node added mid-rollout slots into its name-sorted position. Because "current wave" is *derived* as "first wave with a non-updated node" (§3 step 5), an inserted node that lands in an already-completed wave simply makes that wave current again and gets updated next — deterministic and self-healing, no stored wave assignments to migrate. `status.currentWave`/`totalWaves` are informational projections, never inputs.

---

## 5. Health gate flow

* **When:** evaluated only when the current wave is *fully updated and Ready* — never earlier (metrics from half-rolled pods are noise).
* **How:** one HTTP GET to `spec.healthGate.prometheusURL` `/api/v1/query?query=<PromQL>` with a short client timeout (5s). Runs inline in Reconcile — a single bounded call is acceptable; there is no polling loop inside Reconcile. Poll cadence comes from `RequeueAfter`.
* **Pass criterion (MVP convention, documented in API docs):** the query returns at least one sample and every returned sample value is `> 0` → HEALTHY. Empty result or any `<= 0` sample → not (yet) healthy. (Later: explicit comparison operator in the CRD.)
* **Timeout:** the first time the gate is consulted for wave *w*, persist `gate-start-wave-<w>: <RFC3339 now>` as a FleetRollout annotation (survives controller restarts). Each reconcile: if healthy → pass; else if `now - gateStart >= timeoutSeconds` → gate failed → rollback or pause per `rollbackPolicy`; else `RequeueAfter(15s)`.
* **Flap handling:** a gate that fails then passes *within the timeout window* simply passes — the timeout is the debounce. Once a wave's gate has passed (condition recorded with the wave number), it is **not** re-evaluated for that wave; passing is latched via the `HealthGatePassed` condition + wave annotation, so a later metric dip does not retroactively fail an earlier wave (see §8).
* **Prometheus unreachable:** treated the same as "not yet healthy" (retry until timeout), with condition reason `PrometheusUnreachable` so operators can tell the difference.

---

## 6. Rollback (concrete semantics under Option B)

Trigger: health gate timeout with `rollbackPolicy: OnFailure`.

1. Set condition `RollingBack=True` and persist. (`phase` stays `Progressing` until rollback completes; see §2 note.)
2. Next reconcile: `desiredImage = lastGood` → the `CreateOrUpdate` in step 1 patches the **DS template back to the last-good image**. Because the strategy is `OnDelete`, nothing restarts yet.
3. The same wave loop now runs in reverse *automatically*: every pod running the *new* (bad) image now has a stale template hash relative to the DS. The reconciler deletes stale pods — **all at once, not wave-by-wave** (rolling back slowly to a known-bad image helps no one; MVP deletes all stale pods in one pass, still bounded by DS recreation).
4. Health gate is **skipped** during rollback (we are returning to a known-good state).
5. When every target node's pod matches the last-good template and is Ready → `phase: RolledBack`, `RollingBack=False (reason=Completed)`. Terminal until spec changes.

`last-good-image` bookkeeping: when a rollout reaches `Done`, record `spec.image` into the `last-good-image` annotation. When a *new* image appears in spec, the annotation already holds the previous successful image. First-ever rollout with no last-good: `OnFailure` degrades to `Paused` with condition reason `NoKnownGoodImage` (nothing to roll back to).

---

## 7. Status & conditions

Status fields set every reconcile (status subresource, compare-before-write):

* `phase`, `currentWave` (0-based internally, surfaced 1-based for humans — pick one and document; MVP: 0-based), `totalWaves`, `updatedNodes`, plus `observedGeneration`-style tracking via `conditions[].observedGeneration`.

Condition types (`metav1.Condition`, standard reasons in parentheses):

| Type | True when | Key reasons |
|---|---|---|
| `Ready` | phase is Done | `RolloutComplete`, `Progressing`, `RolledBack`, `Paused` |
| `Progressing` | actively converging a wave or awaiting a gate | `AdvancingWave`, `WaitingForPods`, `EvaluatingHealthGate` |
| `WaveReady` | current wave fully updated + pods Ready | `PodsPending`, `WaveConverged` |
| `HealthGatePassed` | gate for the most recent completed wave passed | `Evaluating`, `Timeout`, `PrometheusUnreachable`, `Passed`, `NoGateConfigured` |
| `RollingBack` | rollback in flight | `HealthGateTimeout`, `Completed` |
| `Degraded` | terminal-ish trouble needing a human | `NoKnownGoodImage`, `NoTargetNodes`, `RolledBack` |

Emit Kubernetes Events (`recorder.Event`) at: wave promotion, gate pass/fail, rollback start/complete, done.

---

## 8. Edge cases

| Case | Behavior (all fall out of level-based derivation) |
|---|---|
| **Node added mid-rollout** | Node watch triggers reconcile; DS schedules a pod on it (with the *current DS template* — new image). If it name-sorts into a "completed" wave, that wave becomes current again (§4) and the node is verified/counted; since the DS already gave it the new image, there is nothing to delete — it just needs to become Ready. Caveat: a brand-new node gets the new image immediately (DS behavior), effectively skipping the gate for that one node. Accepted for MVP; documented. |
| **Node removed mid-rollout** | Its pod disappears with it; recomputed partition shrinks; `totalWaves`/`updatedNodes` adjust. If it was the only non-updated node, rollout completes. |
| **Spec image changed mid-rollout** | `status.observedImage ≠ spec.image` → treat as a **new rollout**: DS template updated, `currentWave` derives back to the first wave containing a stale pod (likely wave 0), gate annotations for the old image cleared (namespace gate annotations by image hash: `gate-start-<imagehash>-wave-<w>`). Nodes already on the *old* new image are now stale again and get re-rolled. `last-good` is only advanced on `Done`, so rollback target remains the last *successful* image, not the abandoned one. |
| **Controller restart mid-rollout** | Nothing is in memory. On restart, reconcile re-derives: DS exists (CreateOrUpdate no-op), pods' template hashes show exactly which nodes are updated, current wave is re-derived, gate start time is read from the annotation. Worst case: a duplicate no-op pod-hash check or a re-run PromQL query. No duplicate deletes (only stale-hash pods are deleted). |
| **Health gate flapping** | Within a wave's timeout window: transient failure then success → pass (timeout is the debounce). After a wave's gate passes it is latched (condition + annotation) — later flaps affect only the *current* wave's gate. A dip during wave N+1's window correctly blocks N+1. |
| **Offline / NotReady nodes** | DS pods on unreachable nodes stay stale or unready indefinitely, which would wedge the wave. MVP rule: nodes whose `Ready` condition ≠ True are **excluded from the actionable target set** (not counted in N, waves, or `updatedNodes`; surfaced via a `skippedNodes`-style condition message). When the node returns, the Node watch re-triggers and it slots into the derivation. This keeps one dead edge box from blocking the fleet. |
| **Stuck terminating pod** (deleted but node can't confirm) | The wave shows "stale pod exists" forever. MVP: surface via `WaveReady=False` + requeue; no force-delete (dangerous on edge). Operator intervenes. Later: configurable stuck-timeout. |
| **Two FleetRollouts selecting overlapping nodes** | Each owns its own DS, so they don't fight over objects — but two DS pods per node may not be desired. MVP: document as unsupported; later: admission webhook / conflict condition. |

---

## 9. MVP cut line

### MVP — build first

1. Owned DaemonSet with `OnDelete`, CreateOrUpdate idempotent apply, ownerRef GC.
2. Node resolution (Ready nodes only), name-sorted deterministic partitioning, count + percent `waveSize` (`intstr` scaled value, roundUp, clamp ≥1).
3. Level-derived progress: template-hash comparison, derived `currentWave`, stale-pod deletion per wave.
4. Health gate: single PromQL instant query, `>0` convention, annotation-persisted gate start, timeout, `RequeueAfter` polling.
5. Rollback `OnFailure`: DS image → last-good, delete-all-stale, `RolledBack`. `Never` → `Paused`.
6. Status subresource + the six conditions in §7 + Events.
7. Watches: FleetRollout, owned DaemonSet, owned Pods, Nodes (mapped).
8. Edge behavior: restart-safety, node add/remove, spec-change-restarts-rollout, offline-node exclusion.

### Later — explicitly deferred

* `spec.paused` field and a real user-facing pause/resume (MVP `Paused` is only the gate-failure-with-`Never` parking state).
* Gating the *initial* deployment (first rollout onto empty nodes goes fleet-wide at once under Option B; fix via "create DS with pause-image/last-good first" or per-node scheduling gates).
* Rich health gate: comparison operators, thresholds, multiple queries, per-wave queries with label templating (`{{.wave}}`), Prometheus auth/TLS.
* Wave-by-wave rollback, partial rollback, `maxUnavailable` within a wave, canary hold times / soak duration between waves.
* Handling stuck-terminating pods (force policy), per-node timeout/skip budget (`maxSkewedNodes`).
* ~~Multiple containers / full pod template in spec.~~ **Done (v0.3):** `spec.template` accepts a full `PodTemplateSpec`; `spec.image` remains as shorthand for a single-container template. Rollout identity is a hash of the rendered base template, so any field change re-rolls.
* Overlapping-rollout admission webhook; metrics (`fleetrollout_wave_duration_seconds` etc.); `kubectl` printer columns beyond the basics.
* `RollingBack` as a first-class phase enum value (v1alpha2).

---

## Appendix: key invariants (checklist for review)

1. Reconcile never sleeps; all waiting is `RequeueAfter`.
2. No decision depends on in-memory state; everything derives from: DS template, pod hashes/readiness, node list, FleetRollout spec + annotations.
3. The only mutations are: CreateOrUpdate the owned DS, delete stale-hash pods, update own status/annotations. Nodes are never mutated.
4. Deleting a pod is only ever done when its template hash ≠ DS current hash → repeat-safe.
5. `last-good-image` only advances on `Done`.
