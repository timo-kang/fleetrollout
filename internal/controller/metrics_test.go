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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	fleetv1alpha1 "github.com/timo-kang/fleetrollout/api/v1alpha1"
)

// TestMetricsGaugesReflectStatus: after a rollout reaches Done, the per-CR gauges reflect its
// status (updated nodes, total waves, phase=Done). Deleting the CR forgets the series.
func TestMetricsGaugesReflectStatus(t *testing.T) {
	s := planTestScheme(t)
	fr := newFleetRollout("50%")
	hash := frTemplateHash(fr)
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&fleetv1alpha1.FleetRollout{}).
		WithObjects(fr, readyNode("n1"), readyNode("n2"),
			ownedPod("n1", hash), ownedPod("n2", hash)).
		Build()

	got := reconcileOnce(t, c)
	if got.Status.Phase != fleetv1alpha1.PhaseDone {
		t.Fatalf("phase = %q, want Done", got.Status.Phase)
	}

	if v := testutil.ToFloat64(updatedNodesGauge.WithLabelValues(nsDefault, frName)); v != 2 {
		t.Errorf("fleetrollout_updated_nodes = %v, want 2", v)
	}
	if v := testutil.ToFloat64(totalWavesGauge.WithLabelValues(nsDefault, frName)); v != 2 {
		t.Errorf("fleetrollout_total_waves = %v, want 2", v)
	}
	if v := testutil.ToFloat64(phaseGauge.WithLabelValues(nsDefault, frName, string(fleetv1alpha1.PhaseDone))); v != 1 {
		t.Errorf("fleetrollout_phase{Done} = %v, want 1", v)
	}
	if v := testutil.ToFloat64(phaseGauge.WithLabelValues(nsDefault, frName, string(fleetv1alpha1.PhaseProgressing))); v != 0 {
		t.Errorf("fleetrollout_phase{Progressing} = %v, want 0 (only the active phase is 1)", v)
	}

	forgetCRMetrics(nsDefault, frName)
	if c := testutil.CollectAndCount(updatedNodesGauge); c != 0 {
		t.Errorf("updated_nodes series count = %d after forget, want 0", c)
	}
}

// TestMetricsGatePassCounter: a passing final health gate increments the pass + promotion counters.
func TestMetricsGatePassCounter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(promSuccess("1"))) // healthy
	}))
	defer srv.Close()

	s := planTestScheme(t)
	fr := newFleetRollout("50%")
	fr.Spec.HealthGate = &fleetv1alpha1.HealthGate{PrometheusURL: srv.URL, Query: "up", TimeoutSeconds: 60}
	hash := frTemplateHash(fr)
	c := fake.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&fleetv1alpha1.FleetRollout{}).
		WithObjects(fr, readyNode("n1"), readyNode("n2"),
			ownedPod("n1", hash), ownedPod("n2", hash)). // all updated → terminal → final gate
		Build()

	passBefore := testutil.ToFloat64(gateEvaluationsTotal.WithLabelValues("pass"))
	promoBefore := testutil.ToFloat64(wavePromotionsTotal)

	r := &FleetRolloutReconciler{Client: c, Scheme: c.Scheme(), HTTP: srv.Client()}
	if _, err := r.Reconcile(context.Background(), reconcileReq()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if d := testutil.ToFloat64(gateEvaluationsTotal.WithLabelValues("pass")) - passBefore; d != 1 {
		t.Errorf("gate_evaluations_total{pass} delta = %v, want 1", d)
	}
	if d := testutil.ToFloat64(wavePromotionsTotal) - promoBefore; d != 1 {
		t.Errorf("wave_promotions_total delta = %v, want 1", d)
	}
}
