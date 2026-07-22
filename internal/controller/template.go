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
	"encoding/json"
	"fmt"
	"hash/fnv"
	"maps"

	corev1 "k8s.io/api/core/v1"

	fleetv1alpha1 "github.com/timo-kang/fleetrollout/api/v1alpha1"
)

// renderBaseTemplate returns the user's pod template, or the canonical single-container expansion
// of the spec.image shorthand. This is the BASE template: what the rolled artifact's hash is
// computed over, BEFORE the controller injects nodeSelector, labels, or the scheduling gate.
func renderBaseTemplate(fr *fleetv1alpha1.FleetRollout) corev1.PodTemplateSpec {
	if fr.Spec.Template != nil {
		return *fr.Spec.Template.DeepCopy()
	}
	return corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: agentContainer, Image: fr.Spec.Image}},
		},
	}
}

// computeTemplateHash is a deterministic, label-safe hash of the base pod template. Any change to
// any template field (image, env, resources, volumes, ...) changes the hash and thus triggers a
// new rollout. JSON marshaling is stable for a given struct definition (fields in declaration
// order, map keys sorted), which is sufficient for a controller-build-stable identity.
func computeTemplateHash(base corev1.PodTemplateSpec) string {
	b, err := json.Marshal(base)
	if err != nil {
		// A PodTemplateSpec always marshals; fall back to a constant so callers stay total.
		b = []byte("unmarshalable")
	}
	h := fnv.New32a()
	_, _ = h.Write(b)
	return fmt.Sprintf("%08x", h.Sum32())
}

// templateImage returns the image to surface for display: the shorthand image, or the first
// container's image of a full template.
func templateImage(fr *fleetv1alpha1.FleetRollout, base corev1.PodTemplateSpec) string {
	if fr.Spec.Image != "" {
		return fr.Spec.Image
	}
	if len(base.Spec.Containers) > 0 {
		return base.Spec.Containers[0].Image
	}
	return ""
}

// renderDSTemplate injects the controller-owned fields onto a copy of the base template:
// nodeSelector (from targetSelector), owner + template-hash labels, and the wave scheduling gate.
// Injection is unconditional (defense in depth): reserved fields are overwritten regardless of
// what the base carried.
func renderDSTemplate(base corev1.PodTemplateSpec, hash string, fr *fleetv1alpha1.FleetRollout) corev1.PodTemplateSpec {
	t := *base.DeepCopy()

	if t.Labels == nil {
		t.Labels = map[string]string{}
	} else {
		t.Labels = maps.Clone(t.Labels)
	}
	t.Labels[ownerLabel] = fr.Name
	t.Labels[hashLabel] = hash

	t.Spec.NodeSelector = maps.Clone(fr.Spec.TargetSelector.MatchLabels)
	t.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: waveGateName}}
	return t
}
