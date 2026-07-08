# Progressive rollouts to edge fleets, the boring (correct) way

*Building a Kubernetes operator that upgrades a fleet of edge nodes wave by wave — and why the least clever design won.*

Kubernetes' built-in `Deployment` rollout is great in a datacenter: homogeneous nodes, stable networking, and a rollback that costs nothing because a bad pod is just rescheduled somewhere healthy. Edge and robot/IoT fleets violate every one of those assumptions. Nodes are heterogeneous, connectivity is intermittent, and "roll back" can mean a technician physically driving to a device. In that world you want three things that Kubernetes doesn't give you out of the box: **staged rollout**, **health-gated promotion**, and **automatic rollback**.

I built [FleetRollout](https://github.com/timo-kang/fleetrollout), a small operator, to explore doing this correctly. The interesting part wasn't the feature list — it was the central design decision, where the *least clever* option turned out to be the right one.

## The question: how does a "wave" map to Kubernetes objects?

The CRD says "roll out `image` to a wave of *nodes*." But nodes don't run images — workloads do. So the real question is: which object carries the image onto a node, and how do you scope it to a wave? I considered three options.

**Option A — one DaemonSet, widen its node affinity per wave.** Start it targeting nothing, then promote nodes by labeling them (or growing a node-name affinity list). Tempting, but it mutates cluster-scoped `Node` objects the operator doesn't own — no owner references, cleanup needs finalizer bookkeeping, and two rollouts targeting overlapping nodes fight over labels. Rollback is also awkward: shrinking the selector *removes* the workload instead of reverting it.

**Option B — one DaemonSet with `updateStrategy: OnDelete`, delete pods per wave.** The DaemonSet covers the whole fleet from day one, its template already carries the new image, but `OnDelete` means updating the template restarts nothing. To roll out to a wave, you **delete that wave's pods**; the DaemonSet recreates them on the new image.

**Option C — the operator creates bare per-node Pods itself.** Maximum control, but you re-implement the DaemonSet controller: restart-on-failure, node-reboot recovery, eviction handling, stuck-terminating pods. A large, bug-prone surface for zero benefit.

I chose **B**, and the reasons are all about *what you don't have to build*.

## Why "delete a pod" is the whole trick

With `OnDelete`, the DaemonSet controller still does the hard parts — scheduling, restarts after node reboots, tolerations. The operator's only job is to decide *which* pods to delete, and when. That yields three properties almost for free:

- **Level-based progress.** "A node is updated" is directly observable: its pod runs the desired image and is Ready. The operator never stores "I already did wave 2." It *derives* progress from pod state on every reconcile. A controller restart mid-rollout is a non-event — it re-derives everything from the cluster.
- **Idempotency by construction.** The only mutation is "delete pods in the current wave whose image ≠ desired." Deleting an already-recreated pod can't happen (its image matches); deleting a pod that's already terminating is skipped. Repeating the action is a no-op.
- **Rollback is the same primitive, reversed.** Roll back = set the template image back to the last-good image and delete the now-stale pods. No separate rollback code path — just a different `desiredImage`.

That last point is my favorite. When rollback fell out of the design as "the forward loop with `desiredImage = lastGood`," I knew the object model was right. Good primitives make the hard features cheap.

## Level-based reconciliation, concretely

The reconcile loop never sleeps and never trusts memory. Every pass:

1. `CreateOrUpdate` the owned DaemonSet (idempotent).
2. List Ready target nodes, sort by name (stable across restarts), partition into waves. `waveSize` is a count or a percentage resolved with ceil and clamped to ≥1.
3. Derive `updated(node)` from pod image + readiness; the **current wave** is simply *the first wave containing a non-updated node*.
4. Act on the current wave: delete stale pods, or wait for pods to become Ready.
5. Gate promotion, then update status and `RequeueAfter`.

Because the current wave is *derived*, edge cases resolve themselves. A node added mid-rollout name-sorts into position; if it lands in an already-completed wave, that wave simply becomes current again and the node converges. A node that goes NotReady is excluded from the actionable set, so one dead edge box can't wedge the fleet.

## Health gates and rollback

Promotion from wave *w* to *w+1* is blocked until wave *w*'s health gate passes. The gate is a single PromQL instant query (`healthy = ≥1 sample, all values > 0`), evaluated only once the wave is fully updated and Ready. The gate's start time is persisted as an annotation keyed by image + wave, so the timeout survives controller restarts, and a pass is latched so a later metric dip can't retroactively fail an earlier wave.

On timeout, `rollbackPolicy` decides: `OnFailure` with a known-good image reverts the DaemonSet template to `last-good` and deletes the bad-image pods (all at once — rolling back slowly to a known-good image helps no one); `Never` parks in `Paused` for a human. A `RolledBack` rollout is sticky until someone pushes a new `spec.image`, at which point the rollback is abandoned and the fleet rolls forward again.

## The takeaway

The clever option (widen affinity, track wave assignments, hand-manage pods) would have meant more code and more state to get wrong. The boring option — lean on a DaemonSet, derive everything from observed state, make one small decision per reconcile — is shorter, restart-safe, and idempotent, and it made rollback nearly free.

For systems that run where you can't easily reach them, "boring and correct" is the whole point.

---

*Code, design doc, and kind-based demo: [github.com/timo-kang/fleetrollout](https://github.com/timo-kang/fleetrollout).*
