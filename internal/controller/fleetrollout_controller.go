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
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	fleetv1alpha1 "github.com/timo-kang/fleetrollout/api/v1alpha1"
)

const (
	// agentContainer is the single container the MVP controller manages in the owned DaemonSet.
	agentContainer = "agent"
	// ownerLabel ties DaemonSet pods back to their FleetRollout.
	ownerLabel = "fleetrollout.fleet.fleetrollout.io/owner"

	requeueConverging = 15 * time.Second // waiting for DS pods to recreate / become Ready
	requeueActed      = 10 * time.Second // just deleted a wave's stale pods
)

// FleetRolloutReconciler reconciles a FleetRollout object
type FleetRolloutReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=fleet.fleetrollout.io,resources=fleetrollouts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=fleet.fleetrollout.io,resources=fleetrollouts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=fleet.fleetrollout.io,resources=fleetrollouts/finalizers,verbs=update
// +kubebuilder:rbac:groups=apps,resources=daemonsets,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;delete
// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch

// Reconcile implements the level-triggered rollout loop. See docs/reconcile-design.md.
// MVP slice: owned DaemonSet (OnDelete) + deterministic wave partitioning + image-based
// updated/stale derivation + per-wave stale-pod deletion + Done. Health gate & rollback: TODO.
func (r *FleetRolloutReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var fr fleetv1alpha1.FleetRollout
	if err := r.Get(ctx, req.NamespacedName, &fr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !fr.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, nil // ownerRef GC removes the DaemonSet; no finalizer in MVP
	}

	desiredImage := fr.Spec.Image

	// ---- 1. Ensure the owned DaemonSet matches spec (idempotent) ----
	ds := &appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: fr.Name, Namespace: fr.Namespace}}
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, ds, func() error {
		labels := map[string]string{ownerLabel: fr.Name}
		ds.Labels = labels
		ds.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels}
		ds.Spec.UpdateStrategy = appsv1.DaemonSetUpdateStrategy{Type: appsv1.OnDeleteDaemonSetStrategyType}
		ds.Spec.Template.ObjectMeta.Labels = labels
		ds.Spec.Template.Spec.NodeSelector = fr.Spec.TargetSelector.MatchLabels
		ds.Spec.Template.Spec.Containers = []corev1.Container{{
			Name:  agentContainer,
			Image: desiredImage,
		}}
		return controllerutil.SetControllerReference(&fr, ds, r.Scheme)
	}); err != nil {
		return ctrl.Result{}, err
	}

	// ---- 2. Observe: resolve target nodes (Ready only), deterministically partitioned ----
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
	sort.Strings(nodeNames) // stable, restart-safe ordering
	n := len(nodeNames)

	if n == 0 {
		apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
			Type: "Degraded", Status: metav1.ConditionTrue, Reason: "NoTargetNodes",
			Message: "no Ready nodes match spec.targetSelector",
		})
		fr.Status.Phase = "Progressing"
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
		p := &podList.Items[i]
		if p.Spec.NodeName != "" {
			podByNode[p.Spec.NodeName] = p
		}
	}
	// updated(node): pod exists, runs desiredImage, and is Ready.
	updated := func(node string) bool {
		p := podByNode[node]
		return p != nil && len(p.Spec.Containers) > 0 &&
			p.Spec.Containers[0].Image == desiredImage && podReady(p)
	}

	updatedCount := 0
	for _, node := range nodeNames {
		if updated(node) {
			updatedCount++
		}
	}
	fr.Status.UpdatedNodes = int32(updatedCount)

	// waveSize resolution (count or percent), ceil, clamp >= 1.
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

	// ---- 4. Terminal: everything updated ----
	if updatedCount == n {
		fr.Status.Phase = "Done"
		fr.Status.CurrentWave = int32(totalWaves)
		apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
			Type: "Ready", Status: metav1.ConditionTrue, Reason: "RolloutComplete",
			Message: "all target nodes updated and Ready",
		})
		apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
			Type: "Progressing", Status: metav1.ConditionFalse, Reason: "RolloutComplete",
		})
		return r.finish(ctx, &fr, ctrl.Result{})
	}

	// ---- 5. Derive current wave = first wave containing a non-updated node ----
	curWave := 0
	for w := 0; w < totalWaves; w++ {
		start, end := w*size, min((w+1)*size, n)
		done := true
		for _, node := range nodeNames[start:end] {
			if !updated(node) {
				done = false
				break
			}
		}
		if !done {
			curWave = w
			break
		}
	}
	fr.Status.CurrentWave = int32(curWave)
	fr.Status.Phase = "Progressing"

	// ---- 6. Act on the current wave ----
	start, end := curWave*size, min((curWave+1)*size, n)
	waveNodes := nodeNames[start:end]

	converging := false // pod already on desiredImage but not yet Ready
	var stalePods []*corev1.Pod
	for _, node := range waveNodes {
		p := podByNode[node]
		switch {
		case p == nil:
			converging = true // DS will schedule a pod (current template = desiredImage); wait
		case len(p.Spec.Containers) > 0 && p.Spec.Containers[0].Image != desiredImage:
			stalePods = append(stalePods, p) // old image → delete so DS recreates on new image
		case !podReady(p):
			converging = true // right image, not Ready yet
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
			Type: "Progressing", Status: metav1.ConditionTrue, Reason: "AdvancingWave",
			Message: "deleting stale pods in current wave to trigger update",
		})
		return r.finish(ctx, &fr, ctrl.Result{RequeueAfter: requeueActed})
	}

	// converging (pods recreating / becoming Ready)
	apimeta.SetStatusCondition(&fr.Status.Conditions, metav1.Condition{
		Type: "WaveReady", Status: metav1.ConditionFalse, Reason: "PodsPending",
		Message: "waiting for current wave pods to become Ready",
	})
	_ = converging
	// NOTE: health gate + promotion + rollback land in slice 2 (docs/reconcile-design.md §5–6).
	return r.finish(ctx, &fr, ctrl.Result{RequeueAfter: requeueConverging})
}

// finish writes the status subresource and returns the given result.
func (r *FleetRolloutReconciler) finish(ctx context.Context, fr *fleetv1alpha1.FleetRollout, res ctrl.Result) (ctrl.Result, error) {
	if err := r.Status().Update(ctx, fr); err != nil {
		return ctrl.Result{}, err
	}
	return res, nil
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

// SetupWithManager sets up the controller with the Manager.
func (r *FleetRolloutReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&fleetv1alpha1.FleetRollout{}).
		Owns(&appsv1.DaemonSet{}).
		Named("fleetrollout").
		Complete(r)
	// TODO(slice 2): Watches(&corev1.Node{}, mapNodeToRollouts) so node add/remove re-triggers.
}
