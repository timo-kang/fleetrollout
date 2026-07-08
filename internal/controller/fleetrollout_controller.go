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
	annPrefix      = "fleetrollout.fleet.fleetrollout.io/"

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
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// Reconcile implements the level-triggered rollout loop. See docs/reconcile-design.md.
// All controller state (plan snapshot, gate latches, rollback, last-good image) lives in
// status — a controller-owned subresource GitOps pruning cannot strip.
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

	// ---- Resolve rollback state (a new spec.image supersedes an in-flight/finished rollback) ----
	rb := fr.Status.Rollback
	if rb != nil && fr.Spec.Image != rb.FromImage {
		fr.Status.Rollback, rb = nil, nil // rolled forward to a new image → abandon rollback
	}
	if rb != nil && fr.Status.LastGoodImage == "" {
		fr.Status.Rollback, rb = nil, nil // nothing to roll back to; fall through to normal path
	}
	desiredImage := fr.Spec.Image
	if rb != nil {
		desiredImage = fr.Status.LastGoodImage
	}

	// ---- 1. Ensure the owned DaemonSet matches spec (idempotent) ----
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: fr.Name, Namespace: fr.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, ds, func() error {
		dsLabels := map[string]string{ownerLabel: fr.Name}
		ds.Labels = dsLabels
		ds.Spec.Selector = &metav1.LabelSelector{MatchLabels: dsLabels}
		ds.Spec.UpdateStrategy = appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType}
		ds.Spec.Template.Labels = dsLabels
		ds.Spec.Template.Spec.NodeSelector = fr.Spec.TargetSelector.MatchLabels
		ds.Spec.Template.Spec.Containers = []corev1.Container{{Name: agentContainer, Image: desiredImage}}
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
		// No Ready targets: hold. An existing plan is preserved (not cleared) so it resumes on return.
		setCond(&fr, metav1.Condition{
			Type: "Degraded", Status: metav1.ConditionTrue, Reason: "NoTargetNodes",
			Message: "no Ready nodes match spec.targetSelector",
		})
		fr.Status.Phase = fleetv1alpha1.PhaseProgressing
		return r.finish(ctx, &fr, ctrl.Result{RequeueAfter: requeueConverging})
	}

	// ---- 3. Pods owned by the DS, indexed by node ----
	var podList corev1.PodList
	if err := r.List(ctx, &podList, client.InNamespace(fr.Namespace),
		client.MatchingLabels{ownerLabel: fr.Name}); err != nil {
		return ctrl.Result{}, err
	}
	podByNode := make(map[string]*corev1.Pod, len(podList.Items))
	for i := range podList.Items {
		if p := &podList.Items[i]; p.Spec.NodeName != "" {
			podByNode[p.Spec.NodeName] = p
		}
	}
	updated := func(node string) bool {
		p := podByNode[node]
		return p != nil && p.DeletionTimestamp.IsZero() && len(p.Spec.Containers) > 0 &&
			p.Spec.Containers[0].Image == desiredImage && podReady(p)
	}

	// ---- Rollback path: revert to last-good against LIVE nodes, wave-bounded (≤size/pass) ----
	if rb != nil {
		return r.reconcileRollback(ctx, log, &fr, nodeNames, podByNode, updated, desiredImage)
	}

	// ---- 4. Ensure the wave-assignment snapshot (plan) for the current (image, generation) ----
	plan := fr.Status.Plan
	if plan == nil || plan.Image != fr.Spec.Image || plan.Generation != fr.Generation {
		size := resolveWaveSize(fr.Spec.WaveSize, n)
		plan = &fleetv1alpha1.RolloutPlan{
			Image:      fr.Spec.Image,
			Generation: fr.Generation,
			WaveSize:   int32(size),
			Nodes:      slices.Clone(nodeNames),
		}
		fr.Status.Plan = plan
		total := (len(plan.Nodes) + size - 1) / size
		setCond(&fr, metav1.Condition{
			Type: strProgressing, Status: metav1.ConditionTrue, Reason: "Planned",
			Message: fmt.Sprintf("planned %d nodes in %d waves for image %s", len(plan.Nodes), total, plan.Image),
		})
	}
	size := int(plan.WaveSize)
	total := (len(plan.Nodes) + size - 1) / size
	fr.Status.TotalWaves = int32(total)

	// Live eligibility: a planned node is only actionable while it is currently Ready.
	readySet := make(map[string]bool, n)
	for _, name := range nodeNames {
		readySet[name] = true
	}
	eligible := func(node string) bool { return readySet[node] }

	// Progress is derived LIVE every reconcile (level-based); only the assignment is snapshotted.
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

	// ---- 5. Terminal: no eligible pending nodes → (final health gate) → Done ----
	if pendingCount == 0 {
		if fr.Spec.HealthGate != nil {
			d := r.gate(ctx, log, &fr, total-1)
			if !d.promote {
				return r.onGateHold(ctx, &fr, d)
			}
		}
		fr.Status.Phase = fleetv1alpha1.PhaseDone
		fr.Status.CurrentWave = int32(total)
		fr.Status.LastGoodImage = desiredImage
		skipped := len(plan.Nodes) - updatedCount
		msg := "all target nodes updated and Ready"
		if skipped > 0 {
			msg = fmt.Sprintf("all reachable planned nodes updated (%d of %d skipped: NotReady)", skipped, len(plan.Nodes))
		}
		setCond(&fr, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: "RolloutComplete", Message: msg,
		})
		setCond(&fr, metav1.Condition{
			Type: strProgressing, Status: metav1.ConditionFalse, Reason: "RolloutComplete",
		})
		return r.finish(ctx, &fr, ctrl.Result{})
	}

	// ---- 6. Current wave = first wave with an eligible non-updated node ----
	curWave := 0
	for w := range total {
		if !waveFullyUpdated(w) {
			curWave = w
			break
		}
	}
	fr.Status.CurrentWave = int32(curWave)
	fr.Status.Phase = fleetv1alpha1.PhaseProgressing

	// ---- 6b. Promotion from wave (curWave-1) must be gate-approved before we touch curWave ----
	if curWave > 0 && fr.Spec.HealthGate != nil {
		d := r.gate(ctx, log, &fr, curWave-1)
		if !d.promote {
			return r.onGateHold(ctx, &fr, d)
		}
	}

	// ---- 7. Act on the current wave: delete stale pods among its eligible nodes ----
	start, end := curWave*size, min((curWave+1)*size, len(plan.Nodes))
	var stalePods []*corev1.Pod
	for _, node := range plan.Nodes[start:end] {
		if !eligible(node) {
			continue // planned node currently NotReady → skip in place (boundaries never move)
		}
		p := podByNode[node]
		switch {
		case p == nil:
			// DS will schedule with current template (desiredImage); wait
		case !p.DeletionTimestamp.IsZero():
			// already terminating; DS is recreating it
		case len(p.Spec.Containers) > 0 && p.Spec.Containers[0].Image != desiredImage:
			stalePods = append(stalePods, p)
		}
	}

	if len(stalePods) > 0 {
		for _, p := range stalePods {
			if err := r.Delete(ctx, p); err != nil && client.IgnoreNotFound(err) != nil {
				return ctrl.Result{}, err
			}
		}
		log.Info("advanced wave: deleted stale pods", "wave", curWave, "count", len(stalePods))
		setCond(&fr, metav1.Condition{
			Type: strProgressing, Status: metav1.ConditionTrue, Reason: "AdvancingWave",
			Message: fmt.Sprintf("rolling wave %d (%d pods)", curWave, len(stalePods)),
		})
		return r.finish(ctx, &fr, ctrl.Result{RequeueAfter: requeueActed})
	}

	setCond(&fr, metav1.Condition{
		Type: "WaveReady", Status: metav1.ConditionFalse, Reason: "PodsPending",
		Message: fmt.Sprintf("waiting for wave %d pods to become Ready", curWave),
	})
	return r.finish(ctx, &fr, ctrl.Result{RequeueAfter: requeueConverging})
}

// reconcileRollback reverts stale pods to the last-good image against the LIVE node list,
// wave-bounded (≤ size deletions per pass). Rollback never consults the plan snapshot: the
// target is by definition an already-verified image, so partition stability buys nothing.
func (r *FleetRolloutReconciler) reconcileRollback(ctx context.Context, log logr, fr *fleetv1alpha1.FleetRollout,
	nodeNames []string, podByNode map[string]*corev1.Pod, updated func(string) bool, desiredImage string) (ctrl.Result, error) {
	n := len(nodeNames)
	size := resolveWaveSize(fr.Spec.WaveSize, n)
	fr.Status.TotalWaves = int32((n + size - 1) / size)

	deleted := 0
	for _, node := range nodeNames {
		if deleted >= size {
			break
		}
		p := podByNode[node]
		if p != nil && p.DeletionTimestamp.IsZero() && len(p.Spec.Containers) > 0 &&
			p.Spec.Containers[0].Image != desiredImage {
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
	fr.Status.Phase = fleetv1alpha1.PhaseRollingBack

	if deleted > 0 {
		log.Info("rolling back: deleted bad-image pods", "count", deleted, "to", desiredImage)
		setCond(fr, metav1.Condition{
			Type: strRollingBack, Status: metav1.ConditionTrue, Reason: "InProgress",
			Message: fmt.Sprintf("rolling back to last-good image %s (≤%d/wave)", desiredImage, size),
		})
		return r.finish(ctx, fr, ctrl.Result{RequeueAfter: requeueActed})
	}
	if updatedCount == n {
		fr.Status.Phase = fleetv1alpha1.PhaseRolledBack // sticky until spec.image changes (Rollback stays set)
		setCond(fr, metav1.Condition{
			Type: strRollingBack, Status: metav1.ConditionFalse, Reason: "Completed",
			Message: "rolled back to last-good image",
		})
		setCond(fr, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionFalse, Reason: strRolledBack,
		})
		setCond(fr, metav1.Condition{
			Type: "Degraded", Status: metav1.ConditionTrue, Reason: strRolledBack,
			Message: "rolled back after health-gate failure; edit spec.image to retry",
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

	// Timeout anchor, persisted in status → a controller restart resumes (not restarts) the window.
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
	lg := fr.Status.LastGoodImage
	onFailure := fr.Spec.RollbackPolicy == fleetv1alpha1.RollbackOnFailure
	hasLastGood := lg != "" && lg != fr.Spec.Image

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
		log.Info("health gate unhealthy past timeout → rolling back", "wave", wave, "lastGood", lg)
		return gateDecision{rollback: true}
	case gatePauseTimeout:
		reason, msg := "Timeout", fmt.Sprintf("wave %d gate unhealthy past %s", wave, timeout)
		if onFailure && !hasLastGood {
			reason, msg = "NoKnownGoodImage", "gate unhealthy past timeout and no known-good image to roll back to"
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
		fr.Status.Rollback = &fleetv1alpha1.RollbackStatus{FromImage: fr.Spec.Image, StartedAt: metav1.Now()}
		fr.Status.Phase = fleetv1alpha1.PhaseRollingBack
		setCond(fr, metav1.Condition{
			Type: strRollingBack, Status: metav1.ConditionTrue, Reason: "HealthGateTimeout",
			Message: "health gate failed; rolling back to last-good image",
		})
		return r.finish(ctx, fr, ctrl.Result{Requeue: true})
	}
	return r.finish(ctx, fr, d.res)
}

// finish persists the status subresource. Controller state lives entirely in status now, so
// there is no separate annotation write on the acting path.
func (r *FleetRolloutReconciler) finish(ctx context.Context, fr *fleetv1alpha1.FleetRollout, res ctrl.Result) (ctrl.Result, error) {
	if err := r.Status().Update(ctx, fr); err != nil {
		return ctrl.Result{}, err
	}
	return res, nil
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
