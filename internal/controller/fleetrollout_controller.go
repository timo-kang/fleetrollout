/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fleetv1alpha1 "github.com/timo-kang/fleetrollout/api/v1alpha1"
)

const (
	agentContainer = "agent"
	ownerLabel     = "fleetrollout.fleet.fleetrollout.io/owner"
	hashLabel      = "fleetrollout.fleet.fleetrollout.io/template-hash"
	waveGateName   = "fleetrollout.fleet.fleetrollout.io/wave"
	annPrefix      = "fleetrollout.fleet.fleetrollout.io/"

	// objectNameField is the field key the DaemonSet controller uses in a pod's node affinity to
	// bind it to a node before scheduling (metav1.ObjectNameField). Used to find the target node of
	// a still-SchedulingGated pod, which has no spec.nodeName yet.
	objectNameField = "metadata.name"

	requeueConverging = 15 * time.Second
	requeueActed      = 10 * time.Second
	requeueGate       = 15 * time.Second
	promQLTimeout     = 5 * time.Second
	defaultTimeout    = 300 * time.Second

	// condition-type string constants
	strProgressing = "Progressing"
	strRolledBack  = "RolledBack"
	strRollingBack = "RollingBack"
	strHealthGate  = "HealthGatePassed"
)

// FleetRolloutReconciler reconciles a FleetRollout object
type FleetRolloutReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	// HTTP is used to evaluate PromQL health gates; overridable in tests.
	HTTP *http.Client
}

// +kubebuilder:rbac:groups=fleet.fleetrollout.io,resources=fleetrollouts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=fleet.fleetrollout.io,resources=fleetrollouts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fleet.fleetrollout.io,resources=fleetrollouts/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;update;patch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// Reconcile implements the level-triggered rollout loop. See docs/reconcile-design.md.
// The rolled artifact is a full pod template identified by a hash; scheduling is wave-bounded via
// a pod schedulingGate the controller removes per wave (so first-deploy and node-join can't bypass
// the waves/gate). All controller state lives in the status subresource.
func (r *FleetRolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var fr fleetv1alpha1.FleetRollout
	if err := r.Get(ctx, req.NamespacedName, &fr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !fr.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}

	// One-time migration: strip legacy controller-state annotations (state now lives in status).
	if stripLegacyAnnotations(&fr) {
		if err := r.Update(ctx, &fr); err != nil {
			return ctrl.Result{}, err
		}
	}

	// This reconciler always re-derives from the current spec, so status always reflects fr.Generation.
	fr.Status.ObservedGeneration = fr.Generation

	// ---- Resolve desired artifact (forward vs rollback). Identity is the template hash. ----
	base := renderBaseTemplate(&fr)
	currentHash := computeTemplateHash(base)

	rb := fr.Status.Rollback
	if rb != nil && currentHash != rb.FromHash {
		fr.Status.Rollback, rb = nil, nil // spec rolled forward to a new template → abandon rollback
	}
	if rb != nil && fr.Status.LastGood == nil {
		fr.Status.Rollback, rb = nil, nil // nothing to roll back to; fall through to normal path
	}
	desiredBase, desiredHash := base, currentHash
	if rb != nil {
		desiredBase, desiredHash = fr.Status.LastGood.Template, fr.Status.LastGood.TemplateHash
	}

	// ---- 1. Ensure the owned DaemonSet matches the desired template (idempotent) ----
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: fr.Name, Namespace: fr.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, ds, func() error {
		dsLabels := map[string]string{ownerLabel: fr.Name}
		ds.Labels = dsLabels
		ds.Spec.Selector = &metav1.LabelSelector{MatchLabels: dsLabels}
		ds.Spec.UpdateStrategy = appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType}
		ds.Spec.Template = renderDSTemplate(desiredBase, desiredHash, &fr)
		return controllerutil.SetControllerReference(&fr, ds, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, err
	}

	// ---- 2. Observe target nodes (Ready only), deterministically ordered ----
	sel, err := metav1.LabelSelectorAsSelector(&fr.Spec.TargetSelector)
	if err != nil {
		return ctrl.Result{}, err
	}
	var nodeList corev1.NodeList
	if err := r.List(ctx, &nodeList, client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return ctrl.Result{}, err
	}
	var nodeNames []string
	for i := range nodeList.Items {
		if nodeReady(&nodeList.Items[i]) {
			nodeNames = append(nodeNames, nodeList.Items[i].Name)
		}
	}
	slices.Sort(nodeNames)
	n := len(nodeNames)
	if n == 0 {
		setCond(&fr, metav1.Condition{
			Type: "Degraded", Status: metav1.ConditionTrue, Reason: "NoTargetNodes",
			Message: "no Ready nodes match spec.targetSelector",
		})
		fr.Status.Phase = fleetv1alpha1.PhaseProgressing
		return r.finish(ctx, &fr, ctrl.Result{RequeueAfter: requeueConverging})
	}

	// ---- 3. Pods owned by the DS, indexed by target node (scheduled OR still gated) ----
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(fr.Namespace),
		client.MatchingLabels{ownerLabel: fr.Name}); err != nil {
		return ctrl.Result{}, err
	}
	podByNode := make(map[string]*corev1.Pod, len(podList.Items))
	for i := range podList.Items {
		if node := podTargetNode(&podList.Items[i]); node != "" {
			podByNode[node] = &podList.Items[i]
		}
	}

	// Predicates keyed on the desired template hash.
	updated := func(node string) bool {
		p := podByNode[node]
		return p != nil && p.DeletionTimestamp.IsZero() &&
			p.Labels[hashLabel] == desiredHash && p.Spec.NodeName != "" && podReady(p)
	}
	stale := func(p *corev1.Pod) bool { return p.Labels[hashLabel] != desiredHash }

	// ---- 4. Rule A: delete gated+stale pods fleet-wide (unscheduled → zero workload impact) ----
	deletedStaleGated := 0
	for i := range podList.Items {
		p := &podList.Items[i]
		if p.DeletionTimestamp.IsZero() && stale(p) && podGated(p) {
			if err := r.Delete(ctx, p); err != nil && client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}
			deletedStaleGated++
		}
	}
	if deletedStaleGated > 0 {
		log.Info("deleted gated stale pods (Rule A)", "count", deletedStaleGated)
		fr.Status.Phase = fleetv1alpha1.PhaseProgressing
		return r.finish(ctx, &fr, ctrl.Result{RequeueAfter: requeueActed})
	}

	if rb != nil {
		return r.reconcileRollback(ctx, log, &fr, nodeNames, podByNode, updated, stale, desiredHash, desiredBase)
	}
	return r.reconcileForward(ctx, log, &fr, nodeNames, podByNode, updated, stale, currentHash, base)
}

// reconcileForward drives the wave-by-wave forward rollout against the immutable plan snapshot:
// ensure the plan, derive live progress, gate-then-act on the current wave (delete scheduled-stale
// pods, then ungate current-hash pods to release them for scheduling), and declare Done.
func (r *FleetRolloutReconciler) reconcileForward(ctx context.Context, log logr, fr *fleetv1alpha1.FleetRollout,
	nodeNames []string, podByNode map[string]*corev1.Pod, updated func(string) bool, stale func(*corev1.Pod) bool,
	currentHash string, base corev1.PodTemplateSpec) (ctrl.Result, error) {
	n := len(nodeNames)

	// ---- Ensure the wave-assignment snapshot (plan) for the current (hash, generation) ----
	plan := fr.Status.Plan
	if plan == nil || plan.TemplateHash != currentHash || plan.Generation != fr.Generation {
		size := resolveWaveSize(fr.Spec.WaveSize, n)
		plan = &fleetv1alpha1.RolloutPlan{
			TemplateHash: currentHash,
			Image:        templateImage(fr, base),
			Generation:   fr.Generation,
			WaveSize:     int32(size),
			Nodes:        slices.Clone(nodeNames),
		}
		fr.Status.Plan = plan
		total := (len(plan.Nodes) + size - 1) / size
		setCond(fr, metav1.Condition{
			Type: strProgressing, Status: metav1.ConditionTrue, Reason: "Planned",
			Message: fmt.Sprintf("planned %d nodes in %d waves for %s", len(plan.Nodes), total, plan.Image),
		})
	}
	size := int(plan.WaveSize)
	total := (len(plan.Nodes) + size - 1) / size
	fr.Status.TotalWaves = int32(total)

	readySet := make(map[string]bool, n)
	for _, name := range nodeNames {
		readySet[name] = true
	}
	eligible := func(node string) bool { return readySet[node] }

	updatedCount, pendingCount := 0, 0
	for _, node := range plan.Nodes {
		switch {
		case updated(node):
			updatedCount++
		case eligible(node):
			pendingCount++
		}
	}
	fr.Status.UpdatedNodes = int32(updatedCount)

	waveFullyUpdated := func(w int) bool {
		start, end := w*size, min((w+1)*size, len(plan.Nodes))
		for _, node := range plan.Nodes[start:end] {
			if eligible(node) && !updated(node) {
				return false
			}
		}
		return true
	}

	// ---- Terminal: no eligible pending nodes → (final health gate) → Done ----
	if pendingCount == 0 {
		if fr.Spec.HealthGate != nil {
			d := r.gate(ctx, log, fr, total-1)
			if !d.promote {
				return r.onGateHold(ctx, fr, d)
			}
		}

		// Steady-state adoption: the rollout is complete and the current template has fully passed
		// the gate, so a node that joined after the plan was frozen is safe to release. Ungate any
		// gated-on-current pod on a Ready target node (planned nodes are never gated here — a gated
		// pod is unscheduled, so it would have kept pendingCount > 0 and we would not be at Done).
		adopted := 0
		for node, p := range podByNode {
			if eligible(node) && podGatedOn(p, currentHash) {
				if err := r.ungate(ctx, p); err != nil {
					return ctrl.Result{}, err
				}
				adopted++
			}
		}
		if adopted > 0 {
			log.Info("adopted nodes that joined after rollout completion", "count", adopted)
		}

		fr.Status.Phase = fleetv1alpha1.PhaseDone
		fr.Status.CurrentWave = int32(total)
		fr.Status.LastGood = &fleetv1alpha1.LastGood{
			TemplateHash: currentHash, Image: templateImage(fr, base), Template: base,
		}
		skipped := len(plan.Nodes) - updatedCount
		msg := "all target nodes updated and Ready"
		if skipped > 0 {
			msg = fmt.Sprintf("all reachable planned nodes updated (%d of %d skipped: NotReady)", skipped, len(plan.Nodes))
		}
		setCond(fr, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: "RolloutComplete", Message: msg,
		})
		setCond(fr, metav1.Condition{
			Type: strProgressing, Status: metav1.ConditionFalse, Reason: "RolloutComplete",
		})
		return r.finish(ctx, fr, ctrl.Result{})
	}

	// ---- Current wave = first wave with an eligible non-updated node ----
	curWave := 0
	for w := range total {
		if !waveFullyUpdated(w) {
			curWave = w
			break
		}
	}
	fr.Status.CurrentWave = int32(curWave)
	fr.Status.Phase = fleetv1alpha1.PhaseProgressing

	// ---- Promotion from wave (curWave-1) must be gate-approved before we touch curWave ----
	if curWave > 0 && fr.Spec.HealthGate != nil {
		d := r.gate(ctx, log, fr, curWave-1)
		if !d.promote {
			return r.onGateHold(ctx, fr, d)
		}
	}

	// ---- Act on the current wave: delete scheduled-stale pods, then ungate current-hash pods ----
	start, end := curWave*size, min((curWave+1)*size, len(plan.Nodes))
	var stalePods, toUngate []*corev1.Pod
	for _, node := range plan.Nodes[start:end] {
		if !eligible(node) {
			continue // planned node currently NotReady → skip in place (boundaries never move)
		}
		p := podByNode[node]
		switch {
		case p == nil, !p.DeletionTimestamp.IsZero():
			// DS will (re)create the pod gated on the current template; wait
		case stale(p) && p.Spec.NodeName != "":
			stalePods = append(stalePods, p) // scheduled on the old template → delete, DS recreates gated
		case podGatedOn(p, currentHash):
			toUngate = append(toUngate, p) // born gated on the current template → release into this wave
		}
	}

	if len(stalePods) > 0 {
		for _, p := range stalePods {
			if err := r.Delete(ctx, p); err != nil && client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}
		}
		log.Info("advanced wave: deleted stale pods", "wave", curWave, "count", len(stalePods))
		setCond(fr, metav1.Condition{
			Type: strProgressing, Status: metav1.ConditionTrue, Reason: "AdvancingWave",
			Message: fmt.Sprintf("rolling wave %d (%d pods)", curWave, len(stalePods)),
		})
		return r.finish(ctx, fr, ctrl.Result{RequeueAfter: requeueActed})
	}

	if len(toUngate) > 0 {
		for _, p := range toUngate {
			if err := r.ungate(ctx, p); err != nil {
				return ctrl.Result{}, err
			}
		}
		log.Info("advanced wave: ungated pods for scheduling", "wave", curWave, "count", len(toUngate))
		setCond(fr, metav1.Condition{
			Type: strProgressing, Status: metav1.ConditionTrue, Reason: "AdvancingWave",
			Message: fmt.Sprintf("released wave %d for scheduling (%d pods)", curWave, len(toUngate)),
		})
		return r.finish(ctx, fr, ctrl.Result{RequeueAfter: requeueActed})
	}

	setCond(fr, metav1.Condition{
		Type: "WaveReady", Status: metav1.ConditionFalse, Reason: "PodsPending",
		Message: fmt.Sprintf("waiting for wave %d pods to schedule and become Ready", curWave),
	})
	return r.finish(ctx, fr, ctrl.Result{RequeueAfter: requeueConverging})
}

// reconcileRollback reverts to the last-good template against the LIVE node list. It ungates
// last-good pods immediately (last-good is trusted — no gate) and deletes stale pods wave-bounded
// (≤ size per pass) to cap simultaneous restarts. Rollback never consults the plan snapshot.
func (r *FleetRolloutReconciler) reconcileRollback(ctx context.Context, log logr, fr *fleetv1alpha1.FleetRollout,
	nodeNames []string, podByNode map[string]*corev1.Pod, updated func(string) bool, stale func(*corev1.Pod) bool,
	desiredHash string, _ corev1.PodTemplateSpec) (ctrl.Result, error) {
	n := len(nodeNames)
	size := resolveWaveSize(fr.Spec.WaveSize, n)
	fr.Status.TotalWaves = int32((n + size - 1) / size)
	fr.Status.Phase = fleetv1alpha1.PhaseRollingBack

	// Ungate any last-good pods (unbounded — trusted image, holding them Pending only extends outage).
	ungated := 0
	for _, node := range nodeNames {
		if p := podByNode[node]; p != nil && podGatedOn(p, desiredHash) {
			if err := r.ungate(ctx, p); err != nil {
				return ctrl.Result{}, err
			}
			ungated++
		}
	}

	// Delete stale (bad-image) pods, wave-bounded.
	deleted := 0
	for _, node := range nodeNames {
		if deleted >= size {
			break
		}
		p := podByNode[node]
		if p != nil && p.DeletionTimestamp.IsZero() && stale(p) && p.Spec.NodeName != "" {
			if err := r.Delete(ctx, p); err != nil && client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}
			deleted++
		}
	}

	updatedCount := 0
	for _, node := range nodeNames {
		if updated(node) {
			updatedCount++
		}
	}
	fr.Status.UpdatedNodes = int32(updatedCount)

	if ungated > 0 || deleted > 0 {
		log.Info("rolling back to last-good", "ungated", ungated, "deleted", deleted, "size", size)
		setCond(fr, metav1.Condition{
			Type: strRollingBack, Status: metav1.ConditionTrue, Reason: "InProgress",
			Message: fmt.Sprintf("rolling back to last-good template (≤%d deletes/pass)", size),
		})
		return r.finish(ctx, fr, ctrl.Result{RequeueAfter: requeueActed})
	}
	if updatedCount == n {
		fr.Status.Phase = fleetv1alpha1.PhaseRolledBack // sticky until spec changes (Rollback stays set)
		setCond(fr, metav1.Condition{
			Type: strRollingBack, Status: metav1.ConditionFalse, Reason: "Completed",
			Message: "rolled back to last-good template",
		})
		setCond(fr, metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: strRolledBack})
		setCond(fr, metav1.Condition{
			Type: "Degraded", Status: metav1.ConditionTrue, Reason: strRolledBack,
			Message: "rolled back after health-gate failure; edit spec to retry",
		})
		return r.finish(ctx, fr, ctrl.Result{})
	}
	setCond(fr, metav1.Condition{
		Type: strRollingBack, Status: metav1.ConditionTrue, Reason: "InProgress",
		Message: "waiting for rolled-back pods to become Ready",
	})
	return r.finish(ctx, fr, ctrl.Result{RequeueAfter: requeueConverging})
}

// gateDecision is the outcome of evaluating a wave's health gate.
type gateDecision struct {
	promote  bool // gate passed (or already latched) → proceed
	paused   bool // gate timed out, rollbackPolicy=Never (or no known-good) → park in Paused
	rollback bool // gate timed out, rollbackPolicy=OnFailure with a known-good image → roll back
	res      ctrl.Result
}

// gate evaluates (and latches) the health gate for a completed wave against the plan snapshot.
// A passed gate is recorded as plan.GatedWaves (high-water mark), so the latch is bound to the
// exact node set the plan froze — flapping nodes cannot re-authorize a different set (C2).
func (r *FleetRolloutReconciler) gate(ctx context.Context, log logr, fr *fleetv1alpha1.FleetRollout, wave int) gateDecision {
	plan := fr.Status.Plan
	if plan.GatedWaves >= int32(wave)+1 {
		return gateDecision{promote: true} // already latched; never re-evaluated
	}
	gate := fr.Spec.HealthGate

	if plan.EvaluatingWave == nil || *plan.EvaluatingWave != int32(wave) {
		w := int32(wave)
		now := metav1.Now()
		plan.EvaluatingWave = &w
		plan.GateStartedAt = &now
	}

	healthy, reachable := r.evalPromQL(ctx, gate.PrometheusURL, gate.Query)
	timeout := time.Duration(gate.TimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = defaultTimeout
	}
	timedOut := time.Since(plan.GateStartedAt.Time) >= timeout
	hasLastGood := fr.Status.LastGood != nil && fr.Status.LastGood.TemplateHash != plan.TemplateHash
	onFailure := fr.Spec.RollbackPolicy == fleetv1alpha1.RollbackOnFailure

	switch decideGate(reachable, healthy, timedOut, onFailure, hasLastGood) {
	case gatePass:
		plan.GatedWaves = int32(wave) + 1
		plan.EvaluatingWave, plan.GateStartedAt = nil, nil
		setCond(fr, metav1.Condition{
			Type: strHealthGate, Status: metav1.ConditionTrue, Reason: "Passed",
			Message: fmt.Sprintf("wave %d health gate passed", wave),
		})
		log.Info("health gate passed", "wave", wave)
		return gateDecision{promote: true, res: ctrl.Result{Requeue: true}}
	case gateRollback:
		log.Info("health gate unhealthy past timeout → rolling back", "wave", wave)
		return gateDecision{rollback: true}
	case gatePauseTimeout:
		reason, msg := "Timeout", fmt.Sprintf("wave %d gate unhealthy past %s", wave, timeout)
		if onFailure && !hasLastGood {
			reason, msg = "NoKnownGoodImage", "gate unhealthy past timeout and no known-good template to roll back to"
		}
		fr.Status.Phase = fleetv1alpha1.PhasePaused
		setCond(fr, metav1.Condition{
			Type: strHealthGate, Status: metav1.ConditionFalse, Reason: reason, Message: msg,
		})
		log.Info("health gate unhealthy past timeout → paused", "wave", wave)
		return gateDecision{paused: true}
	default: // gateWait — includes unreachable, which NEVER escalates (no data ≠ unhealthy)
		reason, msg := "Evaluating", fmt.Sprintf("wave %d health gate not yet passed", wave)
		if !reachable {
			reason, msg = "MonitoringUnavailable", "Prometheus unreachable — holding, never rolling back on missing data"
		}
		setCond(fr, metav1.Condition{
			Type: strHealthGate, Status: metav1.ConditionFalse, Reason: reason, Message: msg,
		})
		return gateDecision{res: ctrl.Result{RequeueAfter: requeueGate}}
	}
}

// gateAction is the pure outcome of a health-gate observation.
type gateAction int

const (
	gatePass gateAction = iota
	gateWait
	gateRollback
	gatePauseTimeout
)

// decideGate is the pure, unit-tested safety decision for a health gate.
// Key invariant: missing data (Prometheus unreachable) NEVER escalates to rollback or pause —
// only a reachable, definitively-unhealthy signal that persists past the timeout does.
// This prevents a monitoring outage from triggering a fleet-wide rollback.
func decideGate(reachable, healthy, timedOut, onFailure, hasLastGood bool) gateAction {
	if healthy {
		return gatePass
	}
	if !reachable {
		return gateWait // no data ≠ unhealthy: hold and retry, do not escalate
	}
	if !timedOut {
		return gateWait
	}
	if onFailure && hasLastGood {
		return gateRollback
	}
	return gatePauseTimeout
}

// evalPromQL runs an instant query; healthy = >=1 sample and every sample value > 0.
func (r *FleetRolloutReconciler) evalPromQL(ctx context.Context, base, query string) (healthy, reachable bool) {
	cl := r.HTTP
	if cl == nil {
		cl = &http.Client{Timeout: promQLTimeout}
	}
	u := fmt.Sprintf("%s/api/v1/query?query=%s", base, url.QueryEscape(query))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return false, false
	}
	resp, err := cl.Do(req)
	if err != nil {
		return false, false
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return false, true
	}
	var pr struct {
		Status string `json:"status"`
		Data   struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return false, true
	}
	if pr.Status != "success" || len(pr.Data.Result) == 0 {
		return false, true
	}
	for _, s := range pr.Data.Result {
		if len(s.Value) != 2 {
			return false, true
		}
		str, ok := s.Value[1].(string)
		if !ok {
			return false, true
		}
		v, err := strconv.ParseFloat(str, 64)
		if err != nil || v <= 0 {
			return false, true
		}
	}
	return true, true
}

// onGateHold handles a non-promoting gate decision: trigger rollback (OnFailure) or wait (evaluating/Paused).
func (r *FleetRolloutReconciler) onGateHold(ctx context.Context, fr *fleetv1alpha1.FleetRollout, d gateDecision) (ctrl.Result, error) {
	if d.rollback {
		fr.Status.Rollback = &fleetv1alpha1.RollbackStatus{
			FromHash: fr.Status.Plan.TemplateHash, FromImage: fr.Status.Plan.Image, StartedAt: metav1.Now(),
		}
		fr.Status.Phase = fleetv1alpha1.PhaseRollingBack
		setCond(fr, metav1.Condition{
			Type: strRollingBack, Status: metav1.ConditionTrue, Reason: "HealthGateTimeout",
			Message: "health gate failed; rolling back to last-good template",
		})
		return r.finish(ctx, fr, ctrl.Result{Requeue: true})
	}
	return r.finish(ctx, fr, d.res)
}

// finish persists the status subresource. Controller state lives entirely in status now.
func (r *FleetRolloutReconciler) finish(ctx context.Context, fr *fleetv1alpha1.FleetRollout, res ctrl.Result) (ctrl.Result, error) {
	if err := r.Status().Update(ctx, fr); err != nil {
		return ctrl.Result{}, err
	}
	return res, nil
}

// ungate removes the wave scheduling gate from a pod so the scheduler can bind it. Removing an
// already-absent gate is a no-op (idempotent across controller restarts / requeues).
func (r *FleetRolloutReconciler) ungate(ctx context.Context, pod *corev1.Pod) error {
	gates := pod.Spec.SchedulingGates[:0:0]
	changed := false
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == waveGateName {
			changed = true
			continue
		}
		gates = append(gates, g)
	}
	if !changed {
		return nil
	}
	pod.Spec.SchedulingGates = gates
	return client.IgnoreNotFound(r.Update(ctx, pod))
}

// setCond stamps the current generation onto the condition, then upserts it.
func setCond(fr *fleetv1alpha1.FleetRollout, c metav1.Condition) {
	c.ObservedGeneration = fr.Generation
	apimeta.SetStatusCondition(&fr.Status.Conditions, c)
}

// stripLegacyAnnotations removes any pre-status controller-state annotations (one-time migration).
func stripLegacyAnnotations(fr *fleetv1alpha1.FleetRollout) bool {
	changed := false
	for k := range fr.Annotations {
		if strings.HasPrefix(k, annPrefix) {
			delete(fr.Annotations, k)
			changed = true
		}
	}
	return changed
}

// resolveWaveSize turns spec.waveSize (count or percent, default "20%") into an absolute node
// count in [1, n].
func resolveWaveSize(ws intstr.IntOrString, n int) int {
	if ws.StrVal == "" && ws.IntVal == 0 {
		ws = intstr.FromString("20%")
	}
	size, err := intstr.GetScaledValueFromIntOrPercent(&ws, n, true)
	if err != nil || size < 1 {
		size = 1
	}
	if size > n {
		size = n
	}
	return size
}

// podGated reports whether the pod still carries the wave scheduling gate.
func podGated(pod *corev1.Pod) bool {
	for _, g := range pod.Spec.SchedulingGates {
		if g.Name == waveGateName {
			return true
		}
	}
	return false
}

// podGatedOn reports whether the pod is gated AND carries the given (current) template hash — i.e.
// it is ready to be released into its wave (never ungate a stale pod).
func podGatedOn(pod *corev1.Pod, hash string) bool {
	return podGated(pod) && pod.Labels[hashLabel] == hash
}

// podTargetNode returns the node a pod targets: its spec.nodeName once scheduled, otherwise the
// node-name term the DaemonSet controller injects into node affinity for a not-yet-scheduled
// (e.g. SchedulingGated) pod.
func podTargetNode(pod *corev1.Pod) string {
	if pod.Spec.NodeName != "" {
		return pod.Spec.NodeName
	}
	aff := pod.Spec.Affinity
	if aff == nil || aff.NodeAffinity == nil || aff.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution == nil {
		return ""
	}
	for _, term := range aff.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
		for _, f := range term.MatchFields {
			if f.Key == objectNameField && len(f.Values) == 1 {
				return f.Values[0]
			}
		}
	}
	return ""
}

func nodeReady(node *corev1.Node) bool {
	for _, c := range node.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func podReady(pod *corev1.Pod) bool {
	if pod.Status.Phase != corev1.PodRunning {
		return false
	}
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

// rolloutsForNode maps a Node event to the FleetRollouts whose selector matches it.
func (r *FleetRolloutReconciler) rolloutsForNode(ctx context.Context, obj client.Object) []reconcile.Request {
	var list fleetv1alpha1.FleetRolloutList
	if err := r.List(ctx, &list); err != nil {
		return nil
	}
	nodeLabels := labels.Set(obj.GetLabels())
	var reqs []reconcile.Request
	for i := range list.Items {
		fr := &list.Items[i]
		s, err := metav1.LabelSelectorAsSelector(&fr.Spec.TargetSelector)
		if err != nil {
			continue
		}
		if s.Matches(nodeLabels) {
			reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{Name: fr.Name, Namespace: fr.Namespace}})
		}
	}
	return reqs
}

// logr is the minimal logging interface used by gate (satisfied by logr.Logger).
type logr = interface {
	Info(msg string, keysAndValues ...any)
}

// SetupWithManager sets up the controller with the Manager.
func (r *FleetRolloutReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fleetv1alpha1.FleetRollout{}).
		Owns(&appsv1.DaemonSet{}).
		Watches(&corev1.Node{}, handler.EnqueueRequestsFromMapFunc(r.rolloutsForNode)).
		Named("fleetrollout").
		Complete(r)
}
