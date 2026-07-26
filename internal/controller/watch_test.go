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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

func nodeWith(ready corev1.ConditionStatus, lbls map[string]string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "n", Labels: lbls},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: ready}}},
	}
}

// TestNodePredicate_FiltersHeartbeats: a heartbeat-only update (same labels + Ready) is dropped;
// a label change or a Ready flip is enqueued. This is the S1 reconcile-storm guard.
func TestNodePredicate_FiltersHeartbeats(t *testing.T) {
	p := nodePredicate()
	fleet := map[string]string{fleetGroupKey: fleetGroupVal}

	cases := []struct {
		name       string
		old, updNw *corev1.Node
		want       bool
	}{
		{"heartbeat only (no change)", nodeWith(corev1.ConditionTrue, fleet), nodeWith(corev1.ConditionTrue, fleet), false},
		{"ready flips", nodeWith(corev1.ConditionTrue, fleet), nodeWith(corev1.ConditionFalse, fleet), true},
		{"labels change", nodeWith(corev1.ConditionTrue, fleet), nodeWith(corev1.ConditionTrue, map[string]string{"other": "x"}), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := p.Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.updNw})
			if got != tc.want {
				t.Errorf("Update predicate = %v, want %v", got, tc.want)
			}
		})
	}

	// Create/Delete always enqueue (a node appearing or leaving matters).
	if !p.Create(event.CreateEvent{Object: nodeWith(corev1.ConditionTrue, fleet)}) {
		t.Error("Create should enqueue")
	}
	if !p.Delete(event.DeleteEvent{Object: nodeWith(corev1.ConditionTrue, fleet)}) {
		t.Error("Delete should enqueue")
	}
}

// TestOwnerLabelSelector: the cache-scoping selector matches owner-labeled pods and nothing else.
func TestOwnerLabelSelector(t *testing.T) {
	sel := ownerLabelSelector()
	if !sel.Matches(labels.Set{ownerLabel: "my-rollout"}) {
		t.Error("selector must match a pod carrying the owner label")
	}
	if sel.Matches(labels.Set{"app": "unrelated"}) {
		t.Error("selector must not match a pod without the owner label")
	}
}
