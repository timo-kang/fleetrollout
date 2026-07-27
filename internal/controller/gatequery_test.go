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
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"

	fleetv1alpha1 "github.com/timo-kang/fleetrollout/api/v1alpha1"
)

func TestCompare(t *testing.T) {
	cases := []struct {
		op   string
		v, t float64
		want bool
	}{
		{"gt", 1, 0, true}, {"gt", 0, 0, false},
		{"ge", 0, 0, true}, {"ge", -1, 0, false},
		{"lt", 0.005, 0.01, true}, {"lt", 0.5, 0.01, false},
		{"le", 1, 1, true}, {"le", 1.1, 1, false},
		{"eq", 1, 1, true}, {"eq", 1, 0, false},
		{"ne", 1, 0, true}, {"ne", 0, 0, false},
		{"", 1, 0, true},       // empty op defaults to gt
		{opBogus, 1, 0, true},  // unknown op defaults to gt (never silently passes unhealthy)
		{opBogus, 0, 0, false}, //
	}
	for _, c := range cases {
		if got := compare(c.v, c.op, c.t); got != c.want {
			t.Errorf("compare(%v, %q, %v) = %v, want %v", c.v, c.op, c.t, got, c.want)
		}
	}
}

func TestNormalizeQueries(t *testing.T) {
	// Bare query → single gt/0 check.
	got := normalizeQueries(&fleetv1alpha1.HealthGate{Query: "up"})
	if len(got) != 1 || got[0].query != "up" || got[0].op != "gt" || got[0].threshold != 0 {
		t.Fatalf("bare query normalize = %+v, want one gt/0 check", got)
	}

	// Explicit queries: defaults filled (op empty→gt, nil threshold→0), names default to query.
	thr := resource.MustParse("0.95")
	gate := &fleetv1alpha1.HealthGate{Queries: []fleetv1alpha1.GateQuery{
		{Query: "a"},                            // op/threshold default
		{Query: "b", Op: "lt", Threshold: &thr}, // explicit
	}}
	got = normalizeQueries(gate)
	if len(got) != 2 {
		t.Fatalf("want 2 resolved queries, got %d", len(got))
	}
	if got[0].op != "gt" || got[0].threshold != 0 || got[0].name != "a" {
		t.Errorf("q0 defaults wrong: %+v", got[0])
	}
	// Quantity → float is approximate (0.95 ≈ 0.9500000000000001); compare with tolerance.
	if got[1].op != "lt" || got[1].threshold < 0.9499 || got[1].threshold > 0.9501 {
		t.Errorf("q1 explicit wrong: op=%q threshold=%v (want lt/~0.95)", got[1].op, got[1].threshold)
	}
}

func TestWaveNodesRegex(t *testing.T) {
	nodes := []string{"n1.edge", "n2", "n3", "n4"}
	// waveSize 2 → wave 0 = n1,n2; wave 1 = n3,n4. Dots are escaped (regex-inert).
	if got := waveNodesRegex(nodes, 2, 0); got != `n1\.edge|n2` {
		t.Errorf("wave 0 = %q, want n1\\.edge|n2", got)
	}
	if got := waveNodesRegex(nodes, 2, 1); got != "n3|n4" {
		t.Errorf("wave 1 = %q, want n3|n4", got)
	}
	// Out-of-range wave → empty.
	if got := waveNodesRegex(nodes, 2, 9); got != "" {
		t.Errorf("out-of-range wave = %q, want empty", got)
	}
}

func TestRenderQueries(t *testing.T) {
	tc := gateTemplateContext{WaveNodes: "n1|n2", Wave: 1, Image: "img:v2", TemplateHash: "abc"}

	// Happy path: variables expand; plain text passes through.
	out, err := renderQueries([]resolvedQuery{
		{query: `up{node=~"({{.WaveNodes}})"}`, name: "a"},
		{query: `rate(err{wave="{{.Wave}}"}[5m])`, name: "b"},
		{query: `plain_no_template`, name: "c"},
	}, tc)
	if err != nil {
		t.Fatal(err)
	}
	if out[0].query != `up{node=~"(n1|n2)"}` {
		t.Errorf("q0 = %q", out[0].query)
	}
	if out[1].query != `rate(err{wave="1"}[5m])` {
		t.Errorf("q1 = %q", out[1].query)
	}
	if out[2].query != `plain_no_template` {
		t.Errorf("q2 = %q", out[2].query)
	}

	// Unknown variable → config error (not a hold).
	_, err = renderQueries([]resolvedQuery{{query: `{{.Nope}}`, name: "x"}}, tc)
	var ce *gateConfigError
	if !errors.As(err, &ce) || ce.reason != "TemplateInvalid" {
		t.Errorf("unknown var: want TemplateInvalid config error, got %v", err)
	}

	// Malformed template → config error.
	_, err = renderQueries([]resolvedQuery{{query: `{{.WaveNodes`, name: "y"}}, tc)
	if !errors.As(err, &ce) || ce.reason != "TemplateInvalid" {
		t.Errorf("malformed: want TemplateInvalid config error, got %v", err)
	}
}
