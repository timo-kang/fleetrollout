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
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	fleetv1alpha1 "github.com/timo-kang/fleetrollout/api/v1alpha1"
)

const (
	frName        = "fr"
	nsDefault     = "default"
	frImage       = "img:v1"
	lastGoodImg   = "img:v0"
	fleetGroupKey = "fleet-group"
	fleetGroupVal = "field-robots"
)

// planTestScheme builds a scheme with the FleetRollout + core/apps types registered.
func planTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := fleetv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func readyNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{fleetGroupKey: fleetGroupVal}},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}},
		},
	}
}

func newFleetRollout(waveSize string) *fleetv1alpha1.FleetRollout {
	return &fleetv1alpha1.FleetRollout{
		ObjectMeta: metav1.ObjectMeta{Name: frName, Namespace: nsDefault, Generation: 1},
		Spec: fleetv1alpha1.FleetRolloutSpec{
			TargetSelector: metav1.LabelSelector{MatchLabels: map[string]string{fleetGroupKey: fleetGroupVal}},
			Image:          frImage,
			WaveSize:       intstr.FromString(waveSize),
		},
	}
}

// reconcileOnce runs one Reconcile pass and returns the refreshed FleetRollout.
func reconcileOnce(t *testing.T, c client.Client) *fleetv1alpha1.FleetRollout {
	t.Helper()
	r := &FleetRolloutReconciler{Client: c, Scheme: c.Scheme()}
	key := types.NamespacedName{Name: frName, Namespace: nsDefault}
	if _, err := r.Reconcile(context.Background(), reconcile.Request{NamespacedName: key}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	got := &fleetv1alpha1.FleetRollout{}
	if err := c.Get(context.Background(), key, got); err != nil {
		t.Fatalf("Get after reconcile: %v", err)
	}
	return got
}

// TestPlanSnapshotCreatedOnFirstReconcile: the first reconcile freezes the wave partition into status.
func TestPlanSnapshotCreatedOnFirstReconcile(t *testing.T) {
	s := planTestScheme(t)
	fr := newFleetRollout("25%")
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&fleetv1alpha1.FleetRollout{}).
		WithObjects(fr, readyNode("n1"), readyNode("n2"), readyNode("n3"), readyNode("n4")).
		Build()

	got := reconcileOnce(t, c)

	if got.Status.Plan == nil {
		t.Fatal("expected status.plan to be set after first reconcile")
	}
	if got.Status.Plan.Image != "img:v1" {
		t.Errorf("plan.image = %q, want img:v1", got.Status.Plan.Image)
	}
	if got.Status.Plan.WaveSize != 1 {
		t.Errorf("plan.waveSize = %d, want 1 (ceil(4*0.25))", got.Status.Plan.WaveSize)
	}
	if len(got.Status.Plan.Nodes) != 4 {
		t.Errorf("plan.nodes len = %d, want 4", len(got.Status.Plan.Nodes))
	}
	if got.Status.TotalWaves != 4 {
		t.Errorf("totalWaves = %d, want 4", got.Status.TotalWaves)
	}
	if got.Status.Plan.Generation != 1 {
		t.Errorf("plan.generation = %d, want 1", got.Status.Plan.Generation)
	}
	if got.Status.ObservedGeneration != 1 {
		t.Errorf("observedGeneration = %d, want 1", got.Status.ObservedGeneration)
	}
}

// TestPlanStableWhenNodeJoins is the C2 regression guard: a node joining mid-rollout must NOT
// shift the frozen wave boundaries. Under the old wave-index scheme, live n would grow to 5 and
// re-slice every wave; with a snapshot, the plan's node set and wave size are immutable.
func TestPlanStableWhenNodeJoins(t *testing.T) {
	s := planTestScheme(t)
	fr := newFleetRollout("25%")
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&fleetv1alpha1.FleetRollout{}).
		WithObjects(fr, readyNode("n1"), readyNode("n2"), readyNode("n3"), readyNode("n4")).
		Build()

	reconcileOnce(t, c) // freezes plan over 4 nodes

	// A 5th node joins the fleet, Ready.
	if err := c.Create(context.Background(), readyNode("n5")); err != nil {
		t.Fatalf("create n5: %v", err)
	}
	got := reconcileOnce(t, c)

	if l := len(got.Status.Plan.Nodes); l != 4 {
		t.Errorf("plan.nodes len = %d after node join, want 4 (snapshot must not absorb new nodes)", l)
	}
	for _, n := range got.Status.Plan.Nodes {
		if n == "n5" {
			t.Errorf("plan.nodes must not contain the newly-joined node n5: %v", got.Status.Plan.Nodes)
		}
	}
	if got.Status.Plan.WaveSize != 1 {
		t.Errorf("plan.waveSize = %d after node join, want 1 (frozen)", got.Status.Plan.WaveSize)
	}
	if got.Status.TotalWaves != 4 {
		t.Errorf("totalWaves = %d after node join, want 4 (frozen)", got.Status.TotalWaves)
	}
}

// TestPlanReplacedOnImageChange: a new spec.image invalidates the plan and resets gate latches.
func TestPlanReplacedOnImageChange(t *testing.T) {
	s := planTestScheme(t)
	fr := newFleetRollout("50%")
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&fleetv1alpha1.FleetRollout{}).
		WithObjects(fr, readyNode("n1"), readyNode("n2")).
		Build()

	got := reconcileOnce(t, c)
	// Simulate a gate having latched on the v1 plan.
	got.Status.Plan.GatedWaves = 1
	if err := c.Status().Update(context.Background(), got); err != nil {
		t.Fatalf("seed gatedWaves: %v", err)
	}

	// User rolls a new image (generation bumps).
	got.Spec.Image = "img:v2"
	got.Generation = 2
	if err := c.Update(context.Background(), got); err != nil {
		t.Fatalf("update image: %v", err)
	}
	got = reconcileOnce(t, c)

	if got.Status.Plan.Image != "img:v2" {
		t.Errorf("plan.image = %q, want img:v2 (plan must be replaced)", got.Status.Plan.Image)
	}
	if got.Status.Plan.GatedWaves != 0 {
		t.Errorf("plan.gatedWaves = %d, want 0 (latches reset on new plan)", got.Status.Plan.GatedWaves)
	}
	if got.Status.Plan.Generation != 2 {
		t.Errorf("plan.generation = %d, want 2", got.Status.Plan.Generation)
	}
}

// TestOldAnnotationsStrippedOnReconcile: legacy controller-state annotations are removed once
// (GitOps hygiene — they would otherwise be permanent diff noise and mislead future readers).
func TestOldAnnotationsStrippedOnReconcile(t *testing.T) {
	s := planTestScheme(t)
	fr := newFleetRollout("50%")
	fr.Annotations = map[string]string{
		annPrefix + "last-good-image": lastGoodImg,
		annPrefix + "gate-ok-abc-w0":  "1",
		"unrelated/keep-me":           "yes",
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&fleetv1alpha1.FleetRollout{}).
		WithObjects(fr, readyNode("n1"), readyNode("n2")).
		Build()

	got := reconcileOnce(t, c)

	for k := range got.Annotations {
		if len(k) >= len(annPrefix) && k[:len(annPrefix)] == annPrefix {
			t.Errorf("legacy annotation %q should have been stripped", k)
		}
	}
	if got.Annotations["unrelated/keep-me"] != "yes" {
		t.Errorf("unrelated annotation must be preserved, got %v", got.Annotations)
	}
}

// ownedPod builds a DS-owned pod on a node running the given image, Ready.
func ownedPod(node, image string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "pod-" + node, Namespace: nsDefault,
			Labels: map[string]string{ownerLabel: frName},
		},
		Spec: corev1.PodSpec{
			NodeName:   node,
			Containers: []corev1.Container{{Name: agentContainer, Image: image}},
		},
		Status: corev1.PodStatus{
			Phase:      corev1.PodRunning,
			Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
		},
	}
}

// TestRollbackSupersededByNewImage: a spec.image different from rollback.fromImage abandons the
// in-flight rollback and rolls forward (plan is rebuilt for the new image).
func TestRollbackSupersededByNewImage(t *testing.T) {
	s := planTestScheme(t)
	fr := newFleetRollout("50%") // spec.image = frImage ("img:v1")
	fr.Status = fleetv1alpha1.FleetRolloutStatus{
		LastGoodImage: lastGoodImg,
		Rollback:      &fleetv1alpha1.RollbackStatus{FromImage: "img:vFAILED"}, // != spec.image
	}
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&fleetv1alpha1.FleetRollout{}).
		WithObjects(fr, readyNode("n1"), readyNode("n2")).
		Build()

	got := reconcileOnce(t, c)

	if got.Status.Rollback != nil {
		t.Errorf("rollback should be superseded (cleared) when spec.image != fromImage, got %+v", got.Status.Rollback)
	}
	if got.Status.Plan == nil || got.Status.Plan.Image != frImage {
		t.Errorf("expected a forward plan for the new spec.image %q, got %+v", frImage, got.Status.Plan)
	}
}

// TestRollbackDeletesStalePod: while rolling back, a pod still running the failed image is deleted
// so the DS recreates it on the last-good image; the plan is never consulted.
func TestRollbackDeletesStalePod(t *testing.T) {
	s := planTestScheme(t)
	fr := newFleetRollout("50%") // spec.image = "img:v1" (the failed image)
	fr.Status = fleetv1alpha1.FleetRolloutStatus{
		LastGoodImage: lastGoodImg,
		Rollback:      &fleetv1alpha1.RollbackStatus{FromImage: frImage}, // == spec.image → active rollback
	}
	stale := ownedPod("n1", frImage)    // running the failed image, must be deleted
	good := ownedPod("n2", lastGoodImg) // already on last-good, must be left alone
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&fleetv1alpha1.FleetRollout{}).
		WithObjects(fr, readyNode("n1"), readyNode("n2"), stale, good).
		Build()

	got := reconcileOnce(t, c)

	if got.Status.Phase != fleetv1alpha1.PhaseRollingBack {
		t.Errorf("phase = %q, want RollingBack", got.Status.Phase)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "pod-n1", Namespace: nsDefault}, &corev1.Pod{}); err == nil {
		t.Error("stale pod-n1 (failed image) should have been deleted during rollback")
	}
	if err := c.Get(context.Background(), types.NamespacedName{Name: "pod-n2", Namespace: nsDefault}, &corev1.Pod{}); err != nil {
		t.Errorf("last-good pod-n2 must be left alone during rollback, got %v", err)
	}
}
