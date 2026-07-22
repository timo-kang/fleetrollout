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

	fleetv1alpha1 "github.com/timo-kang/fleetrollout/api/v1alpha1"
)

func specWithImage(image string) *fleetv1alpha1.FleetRollout {
	return &fleetv1alpha1.FleetRollout{Spec: fleetv1alpha1.FleetRolloutSpec{Image: image}}
}

// TestRenderBaseTemplate_Shorthand: spec.image expands to a canonical single "agent" container.
func TestRenderBaseTemplate_Shorthand(t *testing.T) {
	base := renderBaseTemplate(specWithImage(frImage))
	if len(base.Spec.Containers) != 1 {
		t.Fatalf("expected 1 container, got %d", len(base.Spec.Containers))
	}
	if base.Spec.Containers[0].Name != agentContainer {
		t.Errorf("container name = %q, want %q", base.Spec.Containers[0].Name, agentContainer)
	}
	if base.Spec.Containers[0].Image != frImage {
		t.Errorf("container image = %q, want img:v1", base.Spec.Containers[0].Image)
	}
}

// TestComputeTemplateHash_Deterministic: same input → same hash, different input → different hash.
func TestComputeTemplateHash_Deterministic(t *testing.T) {
	a1 := computeTemplateHash(renderBaseTemplate(specWithImage(frImage)))
	a2 := computeTemplateHash(renderBaseTemplate(specWithImage(frImage)))
	b := computeTemplateHash(renderBaseTemplate(specWithImage(imgV2)))
	if a1 == "" {
		t.Fatal("hash is empty")
	}
	if a1 != a2 {
		t.Errorf("hash not deterministic: %q vs %q", a1, a2)
	}
	if a1 == b {
		t.Errorf("different images hashed the same: %q", a1)
	}
}

// TestComputeTemplateHash_TemplateFieldChange: a non-image field change (env) changes the hash — C5:
// the rolled artifact is the whole template, not just the image string.
func TestComputeTemplateHash_TemplateFieldChange(t *testing.T) {
	withEnv := func(v string) corev1.PodTemplateSpec {
		return corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: agentContainer, Image: frImage,
			Env: []corev1.EnvVar{{Name: "LOG_LEVEL", Value: v}},
		}}}}
	}
	if computeTemplateHash(withEnv("info")) == computeTemplateHash(withEnv("debug")) {
		t.Error("env change must change the template hash (any template field is part of the rolled artifact)")
	}
}

// TestRenderDSTemplate_Injection: the controller injects nodeSelector, owner + hash labels, and the
// wave scheduling gate — none of which are part of the hash.
func TestRenderDSTemplate_Injection(t *testing.T) {
	fr := &fleetv1alpha1.FleetRollout{
		ObjectMeta: metav1.ObjectMeta{Name: "fr"},
		Spec: fleetv1alpha1.FleetRolloutSpec{
			Image:          frImage,
			TargetSelector: metav1.LabelSelector{MatchLabels: map[string]string{fleetGroupKey: fleetGroupVal}},
		},
	}
	base := renderBaseTemplate(fr)
	hash := computeTemplateHash(base)
	dsTmpl := renderDSTemplate(base, hash, fr)

	if dsTmpl.Spec.NodeSelector[fleetGroupKey] != fleetGroupVal {
		t.Errorf("nodeSelector not injected from targetSelector: %v", dsTmpl.Spec.NodeSelector)
	}
	if dsTmpl.Labels[ownerLabel] != "fr" {
		t.Errorf("owner label not injected: %v", dsTmpl.Labels)
	}
	if dsTmpl.Labels[hashLabel] != hash {
		t.Errorf("hash label = %q, want %q", dsTmpl.Labels[hashLabel], hash)
	}
	gates := dsTmpl.Spec.SchedulingGates
	if len(gates) != 1 || gates[0].Name != waveGateName {
		t.Errorf("wave scheduling gate not injected: %v", gates)
	}

	// The hash is a pure function of the base template — injection must not perturb it.
	if computeTemplateHash(base) != hash {
		t.Error("hash must be computed from the base template, independent of injection")
	}
}
