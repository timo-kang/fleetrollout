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
	"strings"
	"testing"
)

// promSuccess builds a Prometheus /api/v1/query success body with one sample per value.
func promSuccess(values ...string) string {
	samples := make([]string, len(values))
	for i, v := range values {
		samples[i] = `{"metric":{},"value":[1700000000,"` + v + `"]}`
	}
	return `{"status":"success","data":{"resultType":"vector","result":[` + strings.Join(samples, ",") + `]}}`
}

// gt0 is the classic single ">0 on every sample" gate (bare-query equivalent).
func gt0(q string) []resolvedQuery {
	return []resolvedQuery{{query: q, op: "gt", threshold: 0, name: q}}
}

func TestEvalPromQL(t *testing.T) {
	tests := []struct {
		name          string
		status        int
		body          string
		wantHealthy   bool
		wantReachable bool
	}{
		{"healthy single >0", 200, promSuccess("1"), true, true},
		{"healthy multi all >0", 200, promSuccess("1", "3", "0.5"), true, true},
		{"unhealthy one is 0", 200, promSuccess("1", "0"), false, true},
		{"unhealthy negative", 200, promSuccess("-1"), false, true},
		// Safety fix: empty vector / non-success / non-200 are NO DATA → unreachable → HOLD,
		// not "unhealthy" (which could roll back on timeout).
		{"empty result → no data", 200, `{"status":"success","data":{"resultType":"vector","result":[]}}`, false, false},
		{"error status → no data", 200, `{"status":"error","data":{"result":[]}}`, false, false},
		{"non-200 → no data", 503, `boom`, false, false},
		{"malformed json → no data", 200, `not-json`, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			r := &FleetRolloutReconciler{HTTP: srv.Client()}
			healthy, reachable := r.evalPromQL(context.Background(), srv.URL, gt0(`up == 1`))
			if healthy != tt.wantHealthy || reachable != tt.wantReachable {
				t.Fatalf("evalPromQL = (healthy=%v, reachable=%v), want (healthy=%v, reachable=%v)",
					healthy, reachable, tt.wantHealthy, tt.wantReachable)
			}
		})
	}
}

// TestEvalPromQL_OpThreshold: an error-rate style gate (value < threshold) — expressible now
// without inversion gymnastics.
func TestEvalPromQL_OpThreshold(t *testing.T) {
	cases := []struct {
		name        string
		value       string
		op          string
		threshold   float64
		wantHealthy bool
	}{
		{"lt passes below threshold", "0.005", "lt", 0.01, true},
		{"lt fails above threshold", "0.5", "lt", 0.01, false},
		{"ge passes at threshold", "0.95", "ge", 0.95, true},
		{"le fails above threshold", "1.2", "le", 1.0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(promSuccess(c.value)))
			}))
			defer srv.Close()
			r := &FleetRolloutReconciler{HTTP: srv.Client()}
			q := []resolvedQuery{{query: "err", op: c.op, threshold: c.threshold, name: "err"}}
			healthy, reachable := r.evalPromQL(context.Background(), srv.URL, q)
			if !reachable {
				t.Fatal("expected reachable")
			}
			if healthy != c.wantHealthy {
				t.Errorf("healthy = %v, want %v", healthy, c.wantHealthy)
			}
		})
	}
}

// TestEvalPromQL_MultiQuery: the gate ANDs queries; one unhealthy → unhealthy(reachable);
// one unreachable → HOLD (reachable=false) regardless of the others.
func TestEvalPromQL_MultiQuery(t *testing.T) {
	healthySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(promSuccess("1")))
	}))
	defer healthySrv.Close()

	// Both healthy → healthy.
	r := &FleetRolloutReconciler{HTTP: healthySrv.Client()}
	h, reach := r.evalPromQL(context.Background(), healthySrv.URL,
		[]resolvedQuery{{query: "a", op: "gt", name: "a"}, {query: "b", op: "gt", name: "b"}})
	if !h || !reach {
		t.Errorf("both healthy: got (healthy=%v, reachable=%v), want (true,true)", h, reach)
	}

	// One query unreachable (bad host) → HOLD, even though the other is healthy.
	// Use a base URL that fails to connect for one query is not possible per-query with one base,
	// so simulate: point at a server that 503s → that query is unanswered → whole gate unreachable.
	mixedSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if strings.Contains(req.URL.RawQuery, "bad") {
			w.WriteHeader(503)
			return
		}
		_, _ = w.Write([]byte(promSuccess("1")))
	}))
	defer mixedSrv.Close()
	r = &FleetRolloutReconciler{HTTP: mixedSrv.Client()}
	h, reach = r.evalPromQL(context.Background(), mixedSrv.URL,
		[]resolvedQuery{{query: "good", op: "gt", name: "good"}, {query: "bad", op: "gt", name: "bad"}})
	if h || reach {
		t.Errorf("one unreachable query: got (healthy=%v, reachable=%v), want (false,false) HOLD", h, reach)
	}
}

// TestEvalPromQL_OversizedBody: a huge response is capped and treated as no data (HOLD), not OOM.
func TestEvalPromQL_OversizedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"status":"success","data":{"resultType":"vector","result":[`))
		blob := strings.Repeat("x", 2<<20) // 2 MiB > 1 MiB cap
		_, _ = w.Write([]byte(`{"metric":{"j":"` + blob + `"},"value":[1,"1"]}]}}`))
	}))
	defer srv.Close()
	r := &FleetRolloutReconciler{HTTP: srv.Client()}
	healthy, reachable := r.evalPromQL(context.Background(), srv.URL, gt0("up"))
	if healthy || reachable {
		t.Errorf("oversized body: got (healthy=%v, reachable=%v), want (false,false) HOLD", healthy, reachable)
	}
}

func TestEvalPromQL_Unreachable(t *testing.T) {
	r := &FleetRolloutReconciler{HTTP: &http.Client{}}
	healthy, reachable := r.evalPromQL(context.Background(), "http://127.0.0.1:1", gt0(`up`))
	if healthy || reachable {
		t.Fatalf("unreachable target: got (healthy=%v, reachable=%v), want (false,false)", healthy, reachable)
	}
}

func TestDecideGate(t *testing.T) {
	// reachable, healthy, timedOut, onFailure, hasLastGood → want
	cases := []struct {
		name                                        string
		reachable, healthy, timedOut, onFail, hasLG bool
		want                                        gateAction
	}{
		{"healthy promotes", true, true, false, true, true, gatePass},
		{"healthy promotes even past timeout", true, true, true, true, true, gatePass},
		{"reachable unhealthy, not timed out → wait", true, false, false, true, true, gateWait},
		{"reachable unhealthy, timed out, OnFailure+lastGood → rollback", true, false, true, true, true, gateRollback},
		{"reachable unhealthy, timed out, Never → pause", true, false, true, false, true, gatePauseTimeout},
		{"reachable unhealthy, timed out, OnFailure no lastGood → pause", true, false, true, true, false, gatePauseTimeout},
		// The safety-critical rows: unreachable must NEVER rollback/pause, regardless of timeout/policy.
		{"unreachable, not timed out → wait", false, false, false, true, true, gateWait},
		{"unreachable, timed out, OnFailure+lastGood → still wait (no rollback on no-data)", false, false, true, true, true, gateWait},
		{"unreachable, timed out, Never → still wait", false, false, true, false, true, gateWait},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := decideGate(c.reachable, c.healthy, c.timedOut, c.onFail, c.hasLG); got != c.want {
				t.Fatalf("decideGate(reachable=%v,healthy=%v,timedOut=%v,onFail=%v,hasLG=%v) = %v, want %v",
					c.reachable, c.healthy, c.timedOut, c.onFail, c.hasLG, got, c.want)
			}
		})
	}
}
