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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// FleetRolloutSpec defines the desired state of FleetRollout.
type FleetRolloutSpec struct {
	// targetSelector selects the fleet's target nodes (e.g. edge/robot nodes) by label.
	// +required
	TargetSelector metav1.LabelSelector `json:"targetSelector"`

	// image is the container image to progressively roll out across the fleet.
	// +required
	Image string `json:"image"`

	// waveSize is the number of nodes updated per wave — a count (e.g. 5) or a
	// percentage of the selected fleet (e.g. "20%").
	// +kubebuilder:default="20%"
	// +optional
	WaveSize intstr.IntOrString `json:"waveSize,omitempty"`

	// healthGate optionally gates promotion to the next wave on a PromQL check.
	// +optional
	HealthGate *HealthGate `json:"healthGate,omitempty"`

	// rollbackPolicy controls whether a failed wave triggers automatic rollback.
	// +kubebuilder:default=OnFailure
	// +optional
	RollbackPolicy RollbackPolicy `json:"rollbackPolicy,omitempty"`
}

// HealthGate is a promotion gate evaluated between waves.
type HealthGate struct {
	// prometheusURL is the base URL of the Prometheus server to query.
	// +required
	PrometheusURL string `json:"prometheusURL"`

	// query is a PromQL expression; the wave is healthy when it returns a truthy, non-empty result.
	// +required
	Query string `json:"query"`

	// timeoutSeconds is how long to wait for the gate to pass before failing the wave.
	// +kubebuilder:default=300
	// +optional
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`
}

// RollbackPolicy controls automatic rollback behavior on wave failure.
// +kubebuilder:validation:Enum=OnFailure;Never
type RollbackPolicy string

const (
	RollbackOnFailure RollbackPolicy = "OnFailure"
	RollbackNever     RollbackPolicy = "Never"
)

// FleetRolloutStatus defines the observed state of FleetRollout.
type FleetRolloutStatus struct {
	// phase is a high-level summary: Progressing, Paused, RolledBack, or Done.
	// +optional
	Phase string `json:"phase,omitempty"`

	// currentWave is the 0-based index of the wave currently being processed.
	// +optional
	CurrentWave int32 `json:"currentWave,omitempty"`

	// totalWaves is the total number of waves planned for this rollout.
	// +optional
	TotalWaves int32 `json:"totalWaves,omitempty"`

	// updatedNodes is the number of nodes successfully updated so far.
	// +optional
	UpdatedNodes int32 `json:"updatedNodes,omitempty"`

	// conditions represent the current state of the FleetRollout resource
	// (e.g. Progressing, Degraded). Status is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Wave",type=integer,JSONPath=`.status.currentWave`
// +kubebuilder:printcolumn:name="Updated",type=integer,JSONPath=`.status.updatedNodes`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// FleetRollout is the Schema for the fleetrollouts API
type FleetRollout struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of FleetRollout
	// +required
	Spec FleetRolloutSpec `json:"spec"`

	// status defines the observed state of FleetRollout
	// +optional
	Status FleetRolloutStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// FleetRolloutList contains a list of FleetRollout
type FleetRolloutList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []FleetRollout `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &FleetRollout{}, &FleetRolloutList{})
		return nil
	})
}
