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

// FleetRolloutPhase is a high-level summary of rollout state.
// +kubebuilder:validation:Enum=Progressing;Paused;RollingBack;RolledBack;Done
type FleetRolloutPhase string

const (
	PhaseProgressing FleetRolloutPhase = "Progressing"
	PhasePaused      FleetRolloutPhase = "Paused"
	PhaseRollingBack FleetRolloutPhase = "RollingBack"
	PhaseRolledBack  FleetRolloutPhase = "RolledBack"
	PhaseDone        FleetRolloutPhase = "Done"
)

// RolloutPlan is an immutable snapshot of the wave partition for one (image, generation).
// Wave w is the node slice Nodes[w*WaveSize : min((w+1)*WaveSize, len(Nodes))]. Gate latches
// (GatedWaves) live inside the plan so replacing the plan atomically clears them — a passed
// gate can never authorize promotion over a different node set than the one it verified (C2).
type RolloutPlan struct {
	// image this plan rolls out; the plan is stale if it differs from the current desired image.
	// +required
	Image string `json:"image"`

	// generation of the spec this plan was computed from; a spec change re-plans and resets gates.
	// +required
	Generation int64 `json:"generation"`

	// waveSize is the ABSOLUTE per-wave node count, resolved once from spec.waveSize (percent already
	// applied against len(nodes) at plan time); never re-resolved, so live fleet-size changes cannot
	// shift wave boundaries.
	// +kubebuilder:validation:Minimum=1
	// +required
	WaveSize int32 `json:"waveSize"`

	// nodes is the frozen, name-sorted list of Ready target node names captured at plan time.
	// +listType=atomic
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=5000
	// +required
	Nodes []string `json:"nodes"`

	// gatedWaves is the high-water mark: health gates for waves [0, gatedWaves) of THIS plan have
	// passed. Promotion into wave w requires gatedWaves >= w. Monotonic for the plan's lifetime.
	// +optional
	GatedWaves int32 `json:"gatedWaves,omitempty"`

	// evaluatingWave is the wave whose gate is currently being evaluated (timeout anchor).
	// +optional
	EvaluatingWave *int32 `json:"evaluatingWave,omitempty"`

	// gateStartedAt is when the gate for evaluatingWave first started evaluating; the timeout base.
	// Persisted in status so a controller restart resumes (not restarts) the timeout window.
	// +optional
	GateStartedAt *metav1.Time `json:"gateStartedAt,omitempty"`
}

// RollbackStatus records an in-flight (or completed-and-sticky) rollback to the last-good image.
type RollbackStatus struct {
	// fromImage is the spec.image that failed its gate; a spec.image different from this value
	// supersedes and abandons the rollback (roll forward to the new image instead).
	// +required
	FromImage string `json:"fromImage"`

	// startedAt is when the rollback was triggered.
	// +optional
	StartedAt metav1.Time `json:"startedAt,omitempty"`
}

// FleetRolloutStatus defines the observed state of FleetRollout.
type FleetRolloutStatus struct {
	// phase is a high-level summary of the rollout.
	// +optional
	Phase FleetRolloutPhase `json:"phase,omitempty"`

	// observedGeneration is the spec generation this status reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// currentWave is the 0-based index of the wave currently being processed.
	// +optional
	CurrentWave int32 `json:"currentWave,omitempty"`

	// totalWaves is the total number of waves planned for this rollout.
	// +optional
	TotalWaves int32 `json:"totalWaves,omitempty"`

	// updatedNodes is the number of planned nodes running the desired image and Ready.
	// +optional
	UpdatedNodes int32 `json:"updatedNodes,omitempty"`

	// lastGoodImage is the most recent image that completed a rollout (reached Done); the
	// rollback target. Controller-owned in status so GitOps pruning cannot strip it.
	// +optional
	LastGoodImage string `json:"lastGoodImage,omitempty"`

	// rollback is non-nil while a rollback is in flight or sticky-completed (phase RolledBack).
	// +optional
	Rollback *RollbackStatus `json:"rollback,omitempty"`

	// plan is the immutable wave-assignment snapshot the forward rollout reconciles against.
	// +optional
	Plan *RolloutPlan `json:"plan,omitempty"`

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
