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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// FleetRolloutSpec defines the desired state of FleetRollout.
// Exactly one of image or template must be set.
// +kubebuilder:validation:XValidation:rule="has(self.image) != has(self.template)",message="exactly one of spec.image and spec.template must be set"
type FleetRolloutSpec struct {
	// targetSelector selects the fleet's target nodes (e.g. edge/robot nodes) by label.
	// +required
	TargetSelector metav1.LabelSelector `json:"targetSelector"`

	// image is shorthand for a minimal single-container pod template (container name "agent")
	// running this image. Mutually exclusive with template — use template for real agents that
	// need volumes, env, resources, securityContext, etc.
	// +kubebuilder:validation:MinLength=1
	// +optional
	Image string `json:"image,omitempty"`

	// template is the full pod template to roll progressively across the fleet. The controller
	// injects nodeSelector (from targetSelector), owner/template-hash labels, and a scheduling
	// gate; those fields are reserved and must not be set by the user (see validation rules).
	// A change to ANY template field (image, env, resources, ...) is a new rollout.
	// +kubebuilder:validation:XValidation:rule="!has(self.spec.schedulingGates) || self.spec.schedulingGates.size() == 0",message="spec.template.spec.schedulingGates is managed by the controller"
	// +kubebuilder:validation:XValidation:rule="!has(self.spec.nodeSelector)",message="use spec.targetSelector; template.spec.nodeSelector is injected by the controller"
	// +kubebuilder:validation:XValidation:rule="self.spec.containers.size() >= 1",message="template must define at least one container"
	// +optional
	Template *corev1.PodTemplateSpec `json:"template,omitempty"`

	// waveSize is the number of nodes updated per wave — a positive count (e.g. 5) or a
	// positive percentage of the selected fleet (e.g. "20%"). An integer 0 means "unset" (the
	// controller uses the 20% default); a "0%"/"abc%" string is rejected.
	// +kubebuilder:default="20%"
	// +kubebuilder:validation:XValidation:rule="type(self) == int ? self >= 0 : self.matches('^[1-9][0-9]*%$')",message="waveSize must be a positive integer or a positive percentage like \"20%\""
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

// HealthGate is a promotion gate evaluated between waves. Provide exactly one of query (a single
// PromQL check) or queries (several checks, all of which must pass).
// +kubebuilder:validation:XValidation:rule="has(self.query) != has(self.queries)",message="exactly one of healthGate.query or healthGate.queries must be set"
type HealthGate struct {
	// prometheusURL is the base URL of the Prometheus server to query. Must be http(s) — the scheme
	// is allowlisted to blunt SSRF (a FleetRollout author would otherwise make the controller GET an
	// arbitrary in-cluster URL from its serviceaccount identity).
	// +kubebuilder:validation:XValidation:rule="self.startsWith('http://') || self.startsWith('https://')",message="prometheusURL must start with http:// or https://"
	// +required
	PrometheusURL string `json:"prometheusURL"`

	// query is a single PromQL check — shorthand for queries: [{query: <q>, op: gt, threshold: "0"}]
	// (healthy when every returned sample is > 0). Mutually exclusive with queries.
	// +kubebuilder:validation:MinLength=1
	// +optional
	Query string `json:"query,omitempty"`

	// queries is a set of PromQL checks; the gate is healthy only when ALL of them pass, and holds
	// (never rolls back) if ANY of them cannot be definitively answered. Mutually exclusive with query.
	// +kubebuilder:validation:MinItems=1
	// +kubebuilder:validation:MaxItems=8
	// +listType=atomic
	// +optional
	Queries []GateQuery `json:"queries,omitempty"`

	// timeoutSeconds is how long to wait for the gate to pass before failing the wave (0 = default
	// 300s). Negative values are rejected — they would make the gate time out instantly and, under
	// rollbackPolicy=OnFailure, trigger an immediate rollback.
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=0
	// +optional
	TimeoutSeconds int32 `json:"timeoutSeconds,omitempty"`

	// auth optionally authenticates to Prometheus (bearer token or basic auth from a Secret).
	// +optional
	Auth *GateAuth `json:"auth,omitempty"`

	// tls optionally configures TLS for the Prometheus connection (custom CA, server name).
	// +optional
	TLS *GateTLS `json:"tls,omitempty"`
}

// GateAuth authenticates the controller's Prometheus requests using credentials from a Secret in
// the FleetRollout's namespace.
type GateAuth struct {
	// type selects the scheme: "bearer" reads Secret key "token"; "basic" reads "username" + "password".
	// +kubebuilder:validation:Enum=bearer;basic
	// +required
	Type string `json:"type"`

	// secretRef names the Secret (in the FleetRollout's namespace) holding the credentials.
	// +required
	SecretRef corev1.LocalObjectReference `json:"secretRef"`
}

// GateTLS configures TLS for the Prometheus connection.
// +kubebuilder:validation:XValidation:rule="!(has(self.caRef) && self.insecureSkipVerify)",message="tls.caRef and tls.insecureSkipVerify are mutually exclusive"
type GateTLS struct {
	// caRef optionally supplies a CA bundle (key "ca.crt") to verify the server certificate.
	// Mutually exclusive with insecureSkipVerify.
	// +optional
	CARef *CASourceReference `json:"caRef,omitempty"`

	// insecureSkipVerify disables server-certificate verification (discouraged; last resort).
	// +optional
	InsecureSkipVerify bool `json:"insecureSkipVerify,omitempty"`

	// serverName overrides the SNI / certificate server name used for verification.
	// +optional
	ServerName string `json:"serverName,omitempty"`
}

// CASourceReference references a CA bundle in a Secret or ConfigMap (fixed key "ca.crt") in the
// FleetRollout's namespace.
type CASourceReference struct {
	// kind is the source object type: Secret or ConfigMap.
	// +kubebuilder:validation:Enum=Secret;ConfigMap
	// +kubebuilder:default=ConfigMap
	// +optional
	Kind string `json:"kind,omitempty"`

	// name is the Secret/ConfigMap name.
	// +required
	Name string `json:"name"`
}

// GateQuery is one PromQL check: the wave is healthy for this query when it returns at least one
// sample and EVERY sample value satisfies `value <op> threshold`.
type GateQuery struct {
	// query is the PromQL expression to evaluate as an instant query.
	// +kubebuilder:validation:MinLength=1
	// +required
	Query string `json:"query"`

	// op is the comparison every sample value must satisfy against threshold:
	// gt (>), ge (>=), lt (<), le (<=), eq (==), ne (!=). Defaults to gt.
	// +kubebuilder:validation:Enum=gt;ge;lt;le;eq;ne
	// +kubebuilder:default=gt
	// +optional
	Op string `json:"op,omitempty"`

	// threshold is the value each sample is compared against (a decimal quantity, e.g. "0", "0.95",
	// "-1"). Defaults to "0", so with the default op gt this is the classic "every sample > 0".
	// +kubebuilder:default="0"
	// +optional
	Threshold *resource.Quantity `json:"threshold,omitempty"`

	// name is an optional label for this query, surfaced in conditions/events; defaults to its index.
	// +kubebuilder:validation:MaxLength=63
	// +optional
	Name string `json:"name,omitempty"`
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

// RolloutPlan is an immutable snapshot of the wave partition for one (templateHash, generation).
// Wave w is the node slice Nodes[w*WaveSize : min((w+1)*WaveSize, len(Nodes))]. Gate latches
// (GatedWaves) live inside the plan so replacing the plan atomically clears them — a passed
// gate can never authorize promotion over a different node set than the one it verified (C2).
type RolloutPlan struct {
	// templateHash identifies the rolled artifact (hash of the rendered base pod template); the
	// plan is stale if it differs from the current desired hash. Any template field change
	// (image, env, resources, ...) changes the hash and triggers a re-plan.
	// +required
	TemplateHash string `json:"templateHash"`

	// image is the rolled image (shorthand image, or the first container's image), for display only.
	// +optional
	Image string `json:"image,omitempty"`

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

// RollbackStatus records an in-flight (or completed-and-sticky) rollback to the last-good template.
type RollbackStatus struct {
	// fromHash is the desired template hash that failed its gate; a current desired hash different
	// from this value supersedes and abandons the rollback (roll forward to the new template instead).
	// +required
	FromHash string `json:"fromHash"`

	// fromImage is the image that failed its gate, for display only.
	// +optional
	FromImage string `json:"fromImage,omitempty"`

	// startedAt is when the rollback was triggered.
	// +optional
	StartedAt metav1.Time `json:"startedAt,omitempty"`
}

// LastGood is the most recent template that completed a rollout (reached Done) — the rollback
// target. The rendered base template is stored so a rollback can re-render the DaemonSet from it.
type LastGood struct {
	// templateHash of the last-good rendered base template.
	// +required
	TemplateHash string `json:"templateHash"`

	// image of the last-good template, for display only.
	// +optional
	Image string `json:"image,omitempty"`

	// template is the rendered base pod template (pre-injection) to roll back to.
	// +required
	Template corev1.PodTemplateSpec `json:"template"`
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

	// skippedNodes are planned nodes currently NotReady and not yet updated — excluded from wave
	// progress so one dead edge box can't wedge the fleet. A rollout can reach Done with skipped
	// nodes (surfaced as Degraded=True, reason NodesSkipped).
	// +listType=atomic
	// +optional
	SkippedNodes []string `json:"skippedNodes,omitempty"`

	// lastGood is the most recent template that completed a rollout (reached Done); the rollback
	// target. Controller-owned in status so GitOps pruning cannot strip it.
	// +optional
	LastGood *LastGood `json:"lastGood,omitempty"`

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
// +kubebuilder:printcolumn:name="Skipped",type=string,JSONPath=`.status.skippedNodes[*]`,priority=1
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
