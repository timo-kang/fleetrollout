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
)

// promResult builds a Prometheus /api/v1/query success body with one scalar-ish sample per value.
func promSuccess(values ...string) string {
	body := `{"status":"success","data":{"resultType":"vector","result":[`
	for i, v := range values {
		if i > 0 {
			body += ","
		}
		body += `{"metric":{},"value":[1700000000,"` + v + `"]}`
	}
	return body + `]}}`
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
		{"empty result", 200, `{"status":"success","data":{"resultType":"vector","result":[]}}`, false, true},
		{"error status", 200, `{"status":"error","data":{"result":[]}}`, false, true},
		{"non-200", 503, `boom`, false, true},
		{"malformed json", 200, `not-json`, false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = w.Write([]byte(tt.body))
			}))
			defer srv.Close()

			r := &FleetRolloutReconciler{HTTP: srv.Client()}
			healthy, reachable := r.evalPromQL(context.Background(), srv.URL, `up == 1`)
			if healthy != tt.wantHealthy || reachable != tt.wantReachable {
				t.Fatalf("evalPromQL = (healthy=%v, reachable=%v), want (healthy=%v, reachable=%v)",
					healthy, reachable, tt.wantHealthy, tt.wantReachable)
			}
		})
	}
}

func TestEvalPromQL_Unreachable(t *testing.T) {
	r := &FleetRolloutReconciler{HTTP: &http.Client{}}
	healthy, reachable := r.evalPromQL(context.Background(), "http://127.0.0.1:1", `up`)
	if healthy || reachable {
		t.Fatalf("unreachable target: got (healthy=%v, reachable=%v), want (false,false)", healthy, reachable)
	}
}

func TestShortHash(t *testing.T) {
	a1 := shortHash("registry/img:v1")
	a2 := shortHash("registry/img:v1")
	b := shortHash("registry/img:v2")
	if a1 != a2 {
		t.Fatalf("shortHash not deterministic: %q vs %q", a1, a2)
	}
	if a1 == b {
		t.Fatalf("shortHash collision for different images: both %q", a1)
	}
}
