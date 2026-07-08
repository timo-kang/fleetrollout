# FleetRollout — Production-Readiness Review & Roadmap Brainstorm

**Status:** Review of MVP + slice 2 (as of `main`)
**Reviewed:** `internal/controller/fleetrollout_controller.go`, `api/v1alpha1/fleetrollout_types.go`, `docs/reconcile-design.md`, RBAC (`config/rbac/role.yaml`), CI workflows, test suites, Helm chart.
**Bar:** "Would I ship a robot-fleet agent update through this on a real field deployment?"

---

## Verdict up front

The core design is genuinely sound: level-based derivation (never trusting memory), the OnDelete-DaemonSet + delete-per-wave primitive, deterministic name-sorted partitioning, restart-safe gate latching keyed by image, and an unusually honest design doc that documents its own cut lines. That is the hard 20% and it is done right.

But the current state is a **correct prototype, not a field tool**. The gaps cluster into four themes:

1. **The failure modes of the *safety mechanisms themselves* are unsafe** (monitoring outage → rollback; rollback = fleet-wide simultaneous restart; gate latches keyed by unstable wave indices).
2. **The workload model is too narrow** (single container, no pod template) for any real agent.
3. **State lives in annotations on a user-owned object**, which GitOps will destroy.
4. **The one thing the operator exists to do — wave/gate/rollback behavior — has no automated e2e coverage.**

None of these are fatal; all are fixable in roughly two release cycles.

---

# Part A — Gap Analysis

Priority legend: **P0** = will cause a field incident or blocks any real adoption. **P1** = blocks serious evaluation / beta. **P2** = needed for v1.0 polish.

## A.1 Correctness & robustness

| # | Pri | Gap | Detail |
|---|-----|-----|--------|
| C1 | **P0** | **Monitoring outage triggers rollback.** | `gate()` starts the timeout clock the first time the gate is consulted and `evalPromQL` treats *unreachable Prometheus* and *HTTP 5xx* identically to "not yet healthy" (controller.go ~L351–393). A 5-minute Prometheus outage during any wave therefore **times out the gate and mass-deletes pods fleet-wide** (rollback path, L181–224). On an edge fleet, where the monitoring link is the *least* reliable component, this converts a monitoring blip into a fleet-wide agent restart. The `PrometheusUnreachable` condition reason exists but does not change the decision. Fix: distinguish "signal says unhealthy" (→ rollback) from "no signal" (→ hold/pause, separate `unreachableTimeout`, never auto-rollback on absence of data). |
| C2 | **P0** | **Gate latches are keyed by wave *index*, but wave boundaries are not stable.** | `gate-ok-<imgHash>-w<N>` latches promotion for wave N, but N's *membership* is recomputed every reconcile from the live Ready-node list (`nodeNames` → `size` → slicing). A node going NotReady (excluded from N), a node joining, or a percent `waveSize` recomputing against a changed N **shifts every wave boundary**. A latched `gate-ok-w1` then silently authorizes promotion over a *different set of nodes* than the one that was actually health-checked. Same instability affects the final gate (`totalWaves-1`). On flapping edge nodes this is the common case, not the corner case. Fix: latch gates against a *snapshot* of the partition (persist the node→wave assignment at rollout start in status, reconcile against it, only re-partition on spec change), or key latches by "highest updated-node count gated" rather than index. |
| C3 | **P0** | **Rollback is a fleet-wide simultaneous restart.** | The rollback path deletes *all* stale pods in one pass (L190–196) with no rate limit and no waves. On a 500-node fleet that is 500 concurrent agent restarts — image pulls saturating edge uplinks, and a window where the entire fleet's agent is down at once. The design doc's rationale ("rolling back slowly to a known-bad image helps no one") is half right: the *image* is bad, but the *restart* is still an availability event. Field reality: the "bad" image usually fails a *metric*, not hard-crashes; a maxUnavailable-bounded rollback is strictly safer. Fix: reuse the wave loop in reverse with a (larger) rollback wave size, or at minimum a token-bucket on deletes. |
| C4 | **P0 (documented)** | **First deploy bypasses waves entirely.** | Documented Option-B cost (design doc §9): a fresh FleetRollout on empty nodes schedules the image fleet-wide at once, gate never consulted. Also applies to every *node added mid-rollout* (design doc §8 caveat) — on an edge fleet where nodes rejoin constantly, "new node gets ungated new image immediately" is a recurring gate bypass, not a day-one-only issue. Fix: pod `schedulingGates` (stable since k8s 1.27) — DS template carries a scheduling gate; the controller ungates per wave. This closes both first-deploy and node-join bypass with the same mechanism and keeps Option B intact. |
| C5 | **P1** | **Single-container, bare-bones pod template.** | The DS template is exactly one container with only a name and image (L122). No env, volumes (device access! `/dev`, hostPath for robot buses), resources, securityContext, tolerations, hostNetwork, imagePullSecrets, initContainers, or sidecars. No real robot/edge agent is deployable this way — this is the biggest *adoption* blocker even before the safety issues. Also `updated()` checks `Containers[0].Image` only. Fix: `spec.template` (full `PodTemplateSpec`) with `spec.image` (or a named target container) as the rolled field; derive updatedness from the DS pod-template-generation label instead of image string comparison — which the design doc already describes but the code does not do. |
| C6 | **P1** | **Controller state in annotations on a user-owned object.** | `last-good-image`, `rolling-back`, `rollback-from`, and all gate latches live in `metadata.annotations` of the FleetRollout the *user* authors. `kubectl apply` from a manifest without them, or ArgoCD/Flux with pruning/self-heal — the normal GitOps posture for fleets — strips them: rollback target lost, gate latches lost (gates re-run), in-flight rollback silently abandoned. Also unbounded growth: `gate-ok-<hash>-w<N>` keys accumulate per image forever. Fix: move to `status` (it's a subresource precisely so controllers own it): `status.lastGoodImage`, `status.rollback`, `status.waveGates`. Annotations were a fine MVP shortcut; they cannot survive GitOps. |
| C7 | **P1** | **Stuck-terminating pods wedge a wave forever (documented, but no visibility budget).** | Design doc §8 accepts this, and "no force-delete on edge" is the *right* default. But there is no stuck-detection, no condition, no event, no per-node skip budget. A single dead node with a pod stuck in Terminating halts the entire fleet rollout indefinitely and the status just says `WaveReady=False / PodsPending`. Fix: `stuckTimeoutSeconds` → surface `Degraded` with the node name; optional `maxSkippedNodes` budget to proceed past N stuck nodes (never force-delete by default). |
| C8 | **P1** | **NotReady-node exclusion interacts badly with progress accounting.** | Excluding NotReady nodes from N is the right call for not wedging, but: (a) it feeds C2 (boundary shift); (b) a node that was updated, then went NotReady, then rejoins *counts as updated* even if its agent crash-looped offline; (c) `updatedNodes` silently shrinks/grows, so `Done` can be declared over a fleet where 30% of nodes were offline and never got the image — with no record of who was skipped. Fix: `status.skippedNodes` (names or count + condition), and treat "Done with skipped nodes" as `Ready=True` + `Degraded=True(reason=NodesSkipped)` so a human can see it. |
| C9 | **P2** | Overlapping rollouts targeting the same nodes: two DSes per node, two rollouts fighting over the same host resources. Documented as unsupported; needs at least a *detected* conflict condition (cheap: compare selectors across FleetRollouts at reconcile) before webhooks. |
| C10 | **P2** | `finish()` does `Update` (annotations) then `Status().Update` unconditionally every reconcile — no compare-before-write despite the design doc requiring it (§3). At 15s requeues across many CRs this is write amplification on the API server, and conflict-prone against concurrent user edits (no retry/patch; plain Update on possibly-stale object). Use `Patch` with optimistic-lock or server-side apply, and skip no-op status writes. |
| C11 | **P2** | Pod watch missing: design doc §3 specifies `Watches(&Pod{})`; `SetupWithManager` only has FleetRollout/DaemonSet/Node. Pod readiness flips are picked up via DS status updates + 15s polling — works, but promotion latency is polling-bound and the doc/code disagree. Either add the (filtered!) pod watch or fix the doc. |

## A.2 API maturity

| # | Pri | Gap |
|---|-----|-----|
| API1 | **P1** | **No validation beyond one enum.** `image` accepts any string (empty after trim, no tag/digest check), `prometheusURL` accepts any URL (see SEC2), `waveSize: "abc%"` / `"0%"` / negatives silently clamp to 1, `timeoutSeconds` accepts negatives (→ instant timeout → instant rollback). Add CEL validation rules on the CRD first (`x-kubernetes-validations` — no webhook infra needed): non-empty image, waveSize pattern, timeout ≥ min, healthGate URL scheme allowlist. Webhooks only when CEL can't express it (cross-object overlap checks). |
| API2 | **P1** | **No `observedGeneration`.** Neither `status.observedGeneration` nor per-condition `observedGeneration` is set (conditions carry 0). Consumers (Argo health checks, `kubectl wait`, humans) cannot tell whether `Done` refers to the current spec or the previous one. This is table stakes for any status API. |
| API3 | **P1** | `status.phase` is a free string with no enum markers, and `RollingBack` is a condition-only state while `Paused`/`RolledBack` are phases — inconsistent surface. Define the enum in the CRD, add `spec.paused` (a rollout tool without an operator-facing pause is not usable in an incident), and record `status.observedImage` + wave assignment snapshot (feeds C2/C6). |
| API4 | **P2** | Printer columns are decent (Phase/Wave/Updated/Age) but missing `TotalWaves` ("2 of ?") and a `Message`/`Ready` column; `kubectl get` should tell the whole story on one line for a field operator over a bad link. |
| API5 | **P2** | v1alpha1→v1beta1 path: the C5 (`spec.template`) and C6 (status-based state) changes are breaking. Do them **now**, in v1alpha1, before anyone depends on it — alpha is exactly the license to break. Plan conversion webhooks only at beta. |

## A.3 Operability & observability

| # | Pri | Gap |
|---|-----|-----|
| O1 | **P1** | **Zero custom metrics.** Only default controller-runtime metrics exist. An operator that *gates on Prometheus* but exposes nothing about itself to Prometheus is an irony users will notice. Minimum set: `fleetrollout_wave_duration_seconds` (histogram), `fleetrollout_gate_evaluations_total{result}`, `fleetrollout_rollbacks_total`, `fleetrollout_nodes{state=updated\|pending\|skipped}`, `fleetrollout_phase` (gauge per CR). |
| O2 | **P1** | **No Events.** Design doc §7 promises `recorder.Event` at wave promotion / gate pass-fail / rollback / done; the reconciler has no `EventRecorder` at all. `kubectl describe fleetrollout` is silent during the most important transitions. This is a half-day fix with outsized field value. |
| O3 | **P2** | No rollout *history*: after a rollback, nothing records which image failed, which wave, when, or the failing gate value. An `status.history[]` ring (last N transitions) or Events with retention guidance. Field debugging happens days later. |
| O4 | **P2** | Leader election exists (flag, default off; enabled in manager config) — fine. But no readiness gating on cache sync issues, no documented HA guidance, and `rolloutsForNode` lists **all** FleetRollouts on **every node event** — on a large flapping fleet this is a reconcile storm amplifier (see S1). |

## A.4 Health gate realism

| # | Pri | Gap |
|---|-----|-----|
| H1 | **P0** | = C1 (unreachable/5xx counts toward rollback). The single most dangerous behavior in the codebase. |
| H2 | **P1** | **No auth/TLS.** Plain `http.Client`, no bearer token, no mTLS, no CA bundle, no basic auth. Virtually every managed/prod Prometheus (Thanos, Mimir, Grafana Cloud, Amazon AMP) requires auth. `secretRef` for credentials + TLS config block. Also: no response-size limit on the JSON decode. |
| H3 | **P1** | **Hardcoded ">0 on all samples" semantics, one query.** No operator/threshold (`< 0.01` error-rate style queries need inversion gymnastics), no multiple queries (AND), no per-wave scoping — the gate for wave 2 measures the *whole fleet's* metric including the not-yet-updated 80%, which dilutes exactly the signal you're gating on. Needs: `queries[] {query, op, threshold}` + template vars (`{{ .waveNodes }}`, `{{ .image }}`) so a wave's gate can select its own nodes. |
| H4 | **P1** | **No soak/bake time.** The gate can pass on the first evaluation seconds after pods go Ready — before the agent has done anything worth measuring. For robots this is the whole ballgame: the failure you're screening for appears after minutes of operation. `minSoakSeconds` (gate cannot pass before wave-ready + soak) is ~20 lines and transforms real-world safety. |
| H5 | **P2** | Prometheus-only. Fine for now — but design the gate as a discriminated union (`prometheus:`, later `job:`/`webhook:`) so adding a "run this synthetic-check Job on the wave's nodes" gate (extremely edge-relevant: metrics pipelines from field sites lag) isn't a breaking change. |

## A.5 Rollback realism

| # | Pri | Gap |
|---|-----|-----|
| R1 | **P0** | = C3 (all-at-once rollback restart). |
| R2 | **P1** | No `maxUnavailable` within a *forward* wave either: advancing a wave deletes every stale pod in it simultaneously — a 20% wave = 20% of the fleet's agents down at once during recreate + image pull (slow on edge links). Bound in-wave concurrency. |
| R3 | **P1** | First-rollout-no-last-good → `Paused` with `NoKnownGoodImage` is handled and correct. But there is no way to *seed* a known-good (`spec.rollbackImage` or annotation bootstrap) for fleets adopting the operator over an already-running agent — their real last-good exists, the operator just doesn't know it. |
| R4 | **P2** | Rollback skips the gate (reasonable) but also skips *verification beyond readiness* — a last-good image that no longer works (expired certs, migrated backend API) rolls "back" into a second failure with `phase: RolledBack, Ready=False` and no further automation. At minimum document; later, evaluate the gate in observe-only mode during rollback and surface it. |

## A.6 Scale & performance

| # | Pri | Gap |
|---|-----|-----|
| S1 | **P1** | `rolloutsForNode` does an uncached-pattern full `List` of all FleetRollouts per node event; node condition churn (heartbeats touch Node objects; edge nodes flap) × fleet size = constant reconcile pressure. Add an event filter (only requeue on label/Ready-condition *changes*), and note the map function misses nodes that *stop* matching (only new labels are evaluated). |
| S2 | **P1** | Serial deletes in-reconcile: a 5000-node fleet at 20% = 1000 sequential API deletes inside one Reconcile call, blocking the (default 1) worker for the duration. Batch with client-side rate limiting and return early. |
| S3 | **P2** | Cache footprint: the manager caches **all pods** cluster-wide-per-namespace and **all nodes**. For the pod cache, add a label selector (`ownerLabel` exists!) via `cache.Options.ByObject` — one-line-ish, large memory win on big clusters. |
| S4 | **P2** | Multi-tenancy: nothing prevents two teams' rollouts from colliding (C9); no per-namespace scoping story for the node list (nodes are cluster-scoped — document the trust model). |

## A.7 Security

| # | Pri | Gap |
|---|-----|-----|
| SEC1 | **P1** | RBAC is actually *close* to least-privilege for the design (nodes: read-only ✓, pods: get/list/watch/delete — no create ✓). The real issues: **cluster-wide pod delete** (any pod in any namespace, since the ClusterRole isn't namespace-bound) and **daemonsets create/delete cluster-wide**. Mitigate: document a namespaced-Role deployment mode for single-namespace use; longer term, validate at admission that the operator only touches label-selected pods. |
| SEC2 | **P1** | **SSRF via `prometheusURL`.** Anyone who can create a FleetRollout can make the controller (with its serviceaccount network identity, inside the cluster network) GET an arbitrary URL with an arbitrary query string, and observe reachability/status through conditions. CEL-validate scheme, optionally restrict to an operator-level allowlist flag. |
| SEC3 | **P2** | No image policy hooks: `image` is any string; no digest pinning encouragement, no cosign/policy-controller integration note. For fleets, at least document "pin by digest; the rollback target is a string, and `:latest` makes last-good meaningless." |
| SEC4 | **P2** | DS pod template sets no `securityContext` (runAsNonRoot etc.) and can't (C5 — no template field). Once `spec.template` lands, ship secure-by-default sample + PSA-compatible docs. |

## A.8 Testing

| # | Pri | Gap |
|---|-----|-----|
| T1 | **P0** | **The core behavior is untested in CI.** Verified: the envtest suite has exactly one `It` (DS creation — correct scope, since envtest has no kubelet: DS pods never schedule, so waves/gates/rollback *cannot* run there), and the CI e2e (`test/e2e/e2e_test.go`) asserts only "manager runs" + "metrics endpoint serves." The wave→gate→promote→rollback lifecycle — the entire reason this project exists — is exercised only by manual kind demos. This is the single highest-leverage engineering investment: a CI e2e on kind (multi-worker, the `hack/kind.yaml` topology already exists) with a stub Prometheus (10-line httptest server in a pod, or a controller flag pointing at a fixture) driving: happy-path waves, gate-timeout→rollback, gate-timeout→Paused(Never), node-join mid-rollout, controller restart mid-rollout. |
| T2 | **P1** | Alternative/complement: fake-client unit tests that simulate the DS controller (create pod on delete) can cover the wave/gate state machine at unit speed — the reconciler's inputs are all listable objects, so this is very testable code. Do this *and* T1 (unit for logic breadth, e2e for integration truth). |
| T3 | **P2** | No chaos/soak: kill the controller mid-wave in e2e (restart-safety is *claimed* by design — prove it in CI), flap a node during a gate window (would have caught C2), stuck-terminating pod simulation (finalizer on a pod). |

## A.9 Release & versioning

| # | Pri | Gap |
|---|-----|-----|
| REL1 | **P1** | No published artifacts: no container image on a public registry (GHCR), no tagged releases, no semver. `make docker-push` exists but nothing runs it. Nobody can try this without building from source. A `release.yml` (goreleaser or plain: tag → build multi-arch **including arm64 — it's an edge tool** → push GHCR → attach manifests) is a day of work. |
| REL2 | **P2** | Helm chart is the kubebuilder-generated one in `dist/chart`, unversioned, unpublished. Publish via GH Pages/OCI. OLM: skip until there's demand — Helm + raw manifests cover the actual edge audience (k3s users don't run OLM). |
| REL3 | **P2** | No upgrade-path statement (CRD changes between alphas: document "delete/recreate" honestly), no CHANGELOG, no compat matrix (k8s versions tested — matters for `schedulingGates` in C4-fix). |

---

# Part B — Roadmap to Field-Grade

## Ruthless prioritization: what actually matters on a robot fleet

A field operator's questions, in order: *(1) Can it deploy my actual agent?* (C5) *(2) Will the safety mechanism itself hurt me?* (C1/C2/C3) *(3) Can I see what it's doing and stop it?* (O2, API3-pause) *(4) Can I install it?* (REL1) *(5) Do I believe your tests?* (T1). Everything else — OLM, webhooks, multi-query gates, history — is nice-to-have until those five are yes.

Explicitly **deprioritized** (defensible cut lines): OLM packaging, conversion webhooks, non-Prometheus gates (design the union, don't build it), multi-tenancy admission control, force-delete policies.

### v0.2 — "Safe to point at a real fleet" (the safety release)

| Item | Fixes | Size | Status |
|---|---|---|---|
| Gate: distinguish no-data from unhealthy; never auto-rollback on unreachable/5xx (separate hold behavior + condition) | C1/H1 | S | ✅ pure `decideGate` (no-data→hold, `MonitoringUnavailable`); 9 unit + kind e2e |
| Snapshot wave assignment in status; latch gates against the snapshot, not live indices | C2 | M | ✅ `status.plan` freezes node set + absolute `waveSize`; gate latch = `plan.gatedWaves` high-water inside the plan; fake-client regression test (node-join can't shift boundaries) |
| Wave-bounded rollback (reverse waves or delete rate-limit) + in-wave `maxUnavailable` | C3/R2 | M | ✅ rollback deletes ≤`size`/pass (in-wave `maxUnavailable` still TODO) |
| Move all controller state annotations → `status` (lastGoodImage, rollback, gate latches); add `observedGeneration` everywhere; phase enum; `spec.paused` | C6/API2/API3 | M | ✅ state fully in `status` (`lastGoodImage`, `rollback`, `plan`); `observedGeneration` on status + every condition; `FleetRolloutPhase` enum; one-time legacy-annotation strip. `spec.paused` deferred |
| `minSoakSeconds` on the gate | H4 | S | ⬜ |
| Events (EventRecorder at every transition) | O2 | S | ⬜ |
| **CI e2e on kind covering waves/gate/rollback/restart** (+ fake-DS unit tests) | T1/T2 | L — do it first; it protects every other change in this table | 🔄 fake-client state-machine unit tests landed; full stub-Prometheus lifecycle e2e next |

### v0.3 — "Deploys real agents, installable" (the adoption release)

| Item | Fixes |
|---|---|
| `spec.template` (full PodTemplateSpec) + rolled-image field; updatedness via pod-template-generation | C5 |
| First-deploy + node-join gating via pod `schedulingGates` ungated per wave | C4 |
| Gate auth/TLS (`secretRef`, CA bundle) + operators/thresholds + per-wave query templating | H2/H3 |
| CEL validation (image, waveSize, timeout, URL scheme) | API1/SEC2 |
| Prometheus metrics for the operator itself + Grafana dashboard JSON in-repo | O1 |
| GHCR multi-arch (amd64+arm64) images, tagged releases, published Helm chart | REL1/REL2 |
| Stuck-pod detection condition + `skippedNodes` surfacing | C7/C8 |
| Node-event filtering; pod cache label-scoping | S1/S3 |

### v1.0 — "Trustworthy" (the credibility release)

Rollout history in status; seedable last-good (R3); rollback-health observation (R4); overlap-detection condition (C9); chaos e2e in CI (controller kill, node flap, stuck terminating — T3); namespaced-RBAC mode docs (SEC1); compare-before-write/SSA status handling (C10); k8s compat matrix + upgrade docs (REL3); v1beta1 with conversion story.

## The 2–3 highest-credibility moves (field + portfolio/CNCF signal)

1. **The safety-failure-mode fix + a CI e2e that proves it (C1 + C2 + T1 together).** "Our gate distinguishes *no data* from *bad data*, wave membership is snapshotted so flapping nodes can't shift what a passed gate authorized — and here is the CI run demonstrating a Prometheus outage that does *not* roll back the fleet, and a genuine failure that does." This is exactly the reasoning that separates toy operators from Argo-Rollouts-class tools, it's testable, and it makes a superb design-blog chapter. Nothing else on this list signals staff-level operator engineering as strongly.
2. **`spec.template` + schedulingGates-based first-deploy/node-join gating (C5 + C4).** Real workloads plus closing the two documented gate bypasses with one modern-Kubernetes mechanism. schedulingGates is a current, slightly-underused primitive — using it correctly for progressive edge delivery is a genuinely novel, citable design point.
3. **A reproducible "flaky edge fleet" demo + release artifacts (REL1 + a kind-based chaos demo).** One `make demo-chaos`: kind cluster, nodes that flap, a Prometheus stub you can toggle, watch the rollout pause-not-panic, then roll back wave-by-wave. Recorded as an asciinema in the README. Installable via `helm install oci://ghcr.io/...`. This is what makes strangers *try* it — and trying it is the entire funnel for an OSS operator.

## What's genuinely good (keep, and say so publicly)

Level-based derivation with zero in-memory state; deterministic partitioning with self-healing wave derivation; gate latching keyed by image (spec-change-restarts-rollout falls out for free); Option B's honest trade-off analysis; the invariants checklist in the design doc. The bones are right — the work above is muscle, not surgery.
