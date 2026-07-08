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
	"hash/fnv"
	"net/http"
	"net/url"
	"slices"
	"strconv"
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

	// phase / condition-type string constants
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
// Slice 2a: adds PromQL health-gated wave promotion + Node watch. (Rollback: slice 2b.)
func (r *FleetRolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var fr fleetv1alpha1.FleetRollout
	if err := r.Get(ctx, req.NamespacedName, &fr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !fr.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil
	}
	annChanged := false
	rollingBack := fr.Annotations[annPrefix+"rolling-back"] == "1"
	rollbackFrom := fr.Annotations[annPrefix+"rollback-from"]
	lastGood := fr.Annotations[annPrefix+"last-good-image"]

	// A new spec.image supersedes an in-flight or finished rollback → abandon it and roll forward.
	if rollingBack && fr.Spec.Image != rollbackFrom {
		delAnn(&fr, "rolling-back", &annChanged)
		delAnn(&fr, "rollback-from", &annChanged)
		rollingBack = false
	}
	if rollingBack && lastGood == "" {
		rollingBack = false // nothing to roll back to; fall through to normal path
	}

	desiredImage := fr.Spec.Image
	if rollingBack {
		desiredImage = lastGood
	}
	imgKey := shortHash(desiredImage)

	// ---- 1. Ensure the owned DaemonSet matches spec (idempotent) ----
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: fr.Name, Namespace: fr.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, ds, func() error {
		labels := map[string]string{ownerLabel: fr.Name}
		ds.Labels = labels
		ds.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		ds.Spec.UpdateStrategy = appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType}
		ds.Spec.Template.Labels = labels
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
		apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
			Type: "Degraded", Status: metav1.ConditionTrue, Reason: "NoTargetNodes",
			Message: "no Ready nodes match spec.targetSelector",
		})
		fr.Status.Phase = strProgressing
		return r.finish(ctx, &fr, annChanged, ctrl.Result{RequeueAfter: requeueConverging})
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

	updatedCount := 0
	for _, node := range nodeNames {
		if updated(node) {
			updatedCount++
		}
	}
	fr.Status.UpdatedNodes = int32(updatedCount)

	// ---- Rollback path: revert to last-good image, all at once (no waves, no gate) ----
	if rollingBack {
		var stale []*corev1.Pod
		for _, node := range nodeNames {
			p := podByNode[node]
			if p != nil && p.DeletionTimestamp.IsZero() && len(p.Spec.Containers) > 0 &&
				p.Spec.Containers[0].Image != desiredImage {
				stale = append(stale, p)
			}
		}
		if len(stale) > 0 {
			for _, p := range stale {
				if err := r.Delete(ctx, p); err != nil && client.IgnoreNotFound(err) != nil {
					return ctrl.Result{}, err
				}
			}
			log.Info("rolling back: deleted bad-image pods", "count", len(stale), "to", desiredImage)
			fr.Status.Phase = strProgressing
			apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
				Type: strRollingBack, Status: metav1.ConditionTrue, Reason: "InProgress",
				Message: fmt.Sprintf("rolling back to last-good image %s", desiredImage),
			})
			return r.finish(ctx, &fr, annChanged, ctrl.Result{RequeueAfter: requeueActed})
		}
		if updatedCount == n {
			fr.Status.Phase = strRolledBack
			apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
				Type: strRollingBack, Status: metav1.ConditionFalse, Reason: "Completed",
				Message: "rolled back to last-good image",
			})
			apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
				Type: "Ready", Status: metav1.ConditionFalse, Reason: strRolledBack,
			})
			apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
				Type: "Degraded", Status: metav1.ConditionTrue, Reason: strRolledBack,
				Message: "rolled back after health-gate failure; edit spec.image to retry",
			})
			return r.finish(ctx, &fr, annChanged, ctrl.Result{}) // sticky until spec.image changes
		}
		apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
			Type: strRollingBack, Status: metav1.ConditionTrue, Reason: "InProgress",
			Message: "waiting for rolled-back pods to become Ready",
		})
		return r.finish(ctx, &fr, annChanged, ctrl.Result{RequeueAfter: requeueConverging})
	}

	// waveSize resolution (count/percent, ceil, clamp >= 1)
	ws := fr.Spec.WaveSize
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
	totalWaves := (n + size - 1) / size
	fr.Status.TotalWaves = int32(totalWaves)

	waveFullyUpdated := func(w int) bool {
		start, end := w*size, min((w+1)*size, n)
		for _, node := range nodeNames[start:end] {
			if !updated(node) {
				return false
			}
		}
		return true
	}

	// ---- 4. Terminal: everything updated → (final health gate) → Done ----
	if updatedCount == n {
		if fr.Spec.HealthGate != nil {
			d := r.gate(ctx, log, &fr, totalWaves-1, imgKey, &annChanged)
			if !d.promote {
				return r.onGateHold(ctx, &fr, annChanged, d)
			}
		}
		fr.Status.Phase = "Done"
		fr.Status.CurrentWave = int32(totalWaves)
		if fr.Annotations[annPrefix+"last-good-image"] != desiredImage {
			setAnn(&fr, "last-good-image", desiredImage, &annChanged)
		}
		apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: "RolloutComplete",
			Message: "all target nodes updated and Ready",
		})
		apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
			Type: strProgressing, Status: metav1.ConditionFalse, Reason: "RolloutComplete",
		})
		return r.finish(ctx, &fr, annChanged, ctrl.Result{})
	}

	// ---- 5. Current wave = first wave with a non-updated node ----
	curWave := 0
	for w := range totalWaves {
		if !waveFullyUpdated(w) {
			curWave = w
			break
		}
	}
	fr.Status.CurrentWave = int32(curWave)
	fr.Status.Phase = strProgressing

	// ---- 5b. Health gate: promotion from wave (curWave-1) must be approved before we touch curWave ----
	if curWave > 0 && fr.Spec.HealthGate != nil {
		d := r.gate(ctx, log, &fr, curWave-1, imgKey, &annChanged)
		if !d.promote {
			return r.onGateHold(ctx, &fr, annChanged, d)
		}
	}

	// ---- 6. Act on the current wave ----
	start, end := curWave*size, min((curWave+1)*size, n)
	var stalePods []*corev1.Pod
	for _, node := range nodeNames[start:end] {
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
		apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
			Type: strProgressing, Status: metav1.ConditionTrue, Reason: "AdvancingWave",
			Message: fmt.Sprintf("rolling wave %d (%d pods)", curWave, len(stalePods)),
		})
		return r.finish(ctx, &fr, annChanged, ctrl.Result{RequeueAfter: requeueActed})
	}

	apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
		Type: "WaveReady", Status: metav1.ConditionFalse, Reason: "PodsPending",
		Message: fmt.Sprintf("waiting for wave %d pods to become Ready", curWave),
	})
	return r.finish(ctx, &fr, annChanged, ctrl.Result{RequeueAfter: requeueConverging})
}

// gateDecision is the outcome of evaluating a wave's health gate.
type gateDecision struct {
	promote  bool // gate passed (or already latched) → proceed
	paused   bool // gate timed out, rollbackPolicy=Never (or no known-good) → park in Paused
	rollback bool // gate timed out, rollbackPolicy=OnFailure with a known-good image → roll back
	res      ctrl.Result
}

// gate evaluates (and latches) the health gate for a completed wave, keyed by image+wave.
func (r *FleetRolloutReconciler) gate(ctx context.Context, log logr, fr *fleetv1alpha1.FleetRollout, wave int, imgKey string, annChanged *bool) gateDecision {
	okKey := fmt.Sprintf("gate-ok-%s-w%d", imgKey, wave)
	if fr.Annotations[annPrefix+okKey] == "1" {
		return gateDecision{promote: true}
	}
	gate := fr.Spec.HealthGate
	startKey := fmt.Sprintf("gate-start-%s-w%d", imgKey, wave)
	startStr := fr.Annotations[annPrefix+startKey]
	if startStr == "" {
		startStr = time.Now().UTC().Format(time.RFC3339)
		setAnn(fr, startKey, startStr, annChanged)
	}

	healthy, reachable := r.evalPromQL(ctx, gate.PrometheusURL, gate.Query)
	if healthy {
		setAnn(fr, okKey, "1", annChanged)
		apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
			Type: strHealthGate, Status: metav1.ConditionTrue, Reason: "Passed",
			Message: fmt.Sprintf("wave %d health gate passed", wave),
		})
		log.Info("health gate passed", "wave", wave)
		return gateDecision{promote: true, res: ctrl.Result{Requeue: true}}
	}

	start, _ := time.Parse(time.RFC3339, startStr)
	timeout := time.Duration(gate.TimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = 300 * time.Second
	}
	if time.Since(start) >= timeout {
		lg := fr.Annotations[annPrefix+"last-good-image"]
		if fr.Spec.RollbackPolicy == fleetv1alpha1.RollbackOnFailure && lg != "" && lg != fr.Spec.Image {
			log.Info("health gate timed out → rolling back", "wave", wave, "lastGood", lg)
			return gateDecision{rollback: true}
		}
		fr.Status.Phase = "Paused"
		reason, msg := "Timeout", fmt.Sprintf("wave %d health gate timed out after %s", wave, timeout)
		if fr.Spec.RollbackPolicy == fleetv1alpha1.RollbackOnFailure && lg == "" {
			reason, msg = "NoKnownGoodImage", "health gate timed out and no known-good image to roll back to"
		}
		apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
			Type: strHealthGate, Status: metav1.ConditionFalse, Reason: reason, Message: msg,
		})
		log.Info("health gate timed out → paused", "wave", wave)
		return gateDecision{paused: true}
	}

	reason := "Evaluating"
	if !reachable {
		reason = "PrometheusUnreachable"
	}
	apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
		Type: strHealthGate, Status: metav1.ConditionFalse, Reason: reason,
		Message: fmt.Sprintf("wave %d health gate not yet passed", wave),
	})
	return gateDecision{res: ctrl.Result{RequeueAfter: requeueGate}}
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
func (r *FleetRolloutReconciler) onGateHold(ctx context.Context, fr *fleetv1alpha1.FleetRollout, annChanged bool, d gateDecision) (ctrl.Result, error) {
	if d.rollback {
		setAnn(fr, "rolling-back", "1", &annChanged)
		setAnn(fr, "rollback-from", fr.Spec.Image, &annChanged)
		apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
			Type: strRollingBack, Status: metav1.ConditionTrue, Reason: "HealthGateTimeout",
			Message: "health gate failed; rolling back to last-good image",
		})
		fr.Status.Phase = strProgressing
		return r.finish(ctx, fr, annChanged, ctrl.Result{Requeue: true})
	}
	return r.finish(ctx, fr, annChanged, d.res)
}

// finish persists annotations (if changed) then the status subresource.
func (r *FleetRolloutReconciler) finish(ctx context.Context, fr *fleetv1alpha1.FleetRollout, annChanged bool, res ctrl.Result) (ctrl.Result, error) {
	if annChanged {
		if err := r.Update(ctx, fr); err != nil {
			return ctrl.Result{}, err
		}
	}
	if err := r.Status().Update(ctx, fr); err != nil {
		return ctrl.Result{}, err
	}
	return res, nil
}

func setAnn(fr *fleetv1alpha1.FleetRollout, key, val string, changed *bool) {
	if fr.Annotations == nil {
		fr.Annotations = map[string]string{}
	}
	full := annPrefix + key
	if fr.Annotations[full] != val {
		fr.Annotations[full] = val
		*changed = true
	}
}

func delAnn(fr *fleetv1alpha1.FleetRollout, key string, changed *bool) {
	full := annPrefix + key
	if fr.Annotations != nil {
		if _, ok := fr.Annotations[full]; ok {
			delete(fr.Annotations, full)
			*changed = true
		}
	}
}

func shortHash(s string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return strconv.FormatUint(uint64(h.Sum32()), 16)
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
