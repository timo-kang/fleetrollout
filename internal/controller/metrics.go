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
	"github.com/prometheus/client_golang/prometheus"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	fleetv1alpha1 "github.com/timo-kang/fleetrollout/api/v1alpha1"
)

// Per-CR metric label keys.
const (
	labelNamespace    = "namespace"
	labelFleetRollout = "fleetrollout"
)

// Custom operator metrics, exposed on the same controller-runtime /metrics endpoint. An operator
// that gates rollouts on Prometheus should itself be observable in Prometheus.
var (
	gateEvaluationsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "fleetrollout_gate_evaluations_total",
		Help: "Health-gate evaluations by outcome (pass, rollback, pause, wait).",
	}, []string{"result"})

	wavePromotionsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "fleetrollout_wave_promotions_total",
		Help: "Wave promotions authorized by a passing health gate.",
	})

	rollbacksTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "fleetrollout_rollbacks_total",
		Help: "Automatic rollbacks triggered by a failed health gate.",
	})

	updatedNodesGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fleetrollout_updated_nodes",
		Help: "Planned nodes updated to the desired template and Ready.",
	}, []string{labelNamespace, labelFleetRollout})

	totalWavesGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fleetrollout_total_waves",
		Help: "Total planned waves for the current rollout.",
	}, []string{labelNamespace, labelFleetRollout})

	phaseGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "fleetrollout_phase",
		Help: "Current rollout phase (1 for the active phase, 0 otherwise).",
	}, []string{labelNamespace, labelFleetRollout, "phase"})
)

var allPhases = []fleetv1alpha1.FleetRolloutPhase{
	fleetv1alpha1.PhaseProgressing,
	fleetv1alpha1.PhasePaused,
	fleetv1alpha1.PhaseRollingBack,
	fleetv1alpha1.PhaseRolledBack,
	fleetv1alpha1.PhaseDone,
}

func init() {
	ctrlmetrics.Registry.MustRegister(
		gateEvaluationsTotal, wavePromotionsTotal, rollbacksTotal,
		updatedNodesGauge, totalWavesGauge, phaseGauge,
	)
}

// recordStatusMetrics reflects a FleetRollout's status into the per-CR gauges.
func recordStatusMetrics(fr *fleetv1alpha1.FleetRollout) {
	updatedNodesGauge.WithLabelValues(fr.Namespace, fr.Name).Set(float64(fr.Status.UpdatedNodes))
	totalWavesGauge.WithLabelValues(fr.Namespace, fr.Name).Set(float64(fr.Status.TotalWaves))
	for _, p := range allPhases {
		v := 0.0
		if p == fr.Status.Phase {
			v = 1
		}
		phaseGauge.WithLabelValues(fr.Namespace, fr.Name, string(p)).Set(v)
	}
}

// forgetCRMetrics drops a deleted FleetRollout's per-CR gauge series so they don't linger.
func forgetCRMetrics(namespace, name string) {
	updatedNodesGauge.DeleteLabelValues(namespace, name)
	totalWavesGauge.DeleteLabelValues(namespace, name)
	for _, p := range allPhases {
		phaseGauge.DeleteLabelValues(namespace, name, string(p))
	}
}
