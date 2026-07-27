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
	"regexp"
	"strings"
	"text/template"

	fleetv1alpha1 "github.com/timo-kang/fleetrollout/api/v1alpha1"
)

// resolvedQuery is a GateQuery with its op/threshold reduced to concrete values for evaluation.
type resolvedQuery struct {
	query     string
	op        string
	threshold float64
	name      string
}

// compare reports whether a sample value satisfies `value <op> threshold`. An unknown op is treated
// as gt (the default), so a malformed op can never silently pass an unhealthy sample.
func compare(value float64, op string, threshold float64) bool {
	switch op {
	case "ge":
		return value >= threshold
	case "lt":
		return value < threshold
	case "le":
		return value <= threshold
	case "eq":
		return value == threshold
	case "ne":
		return value != threshold
	default: // "gt" and anything unrecognized
		return value > threshold
	}
}

// gateTemplateContext is the variable set available to per-wave gate query templates. It lets a
// wave's gate select only its own nodes (so it isn't diluted by the not-yet-updated fleet).
type gateTemplateContext struct {
	WaveNodes    string // current wave's node names as a regex alternation "n1|n2" (for PromQL =~)
	Wave         int    // current wave index (0-based)
	Image        string // image under rollout
	TemplateHash string // current template hash
}

// waveNodesRegex renders the given wave's node names as a regex alternation, each name escaped so
// dots etc. are literal. Node names are DNS-1123 (no quotes), so PromQL string quoting is safe.
func waveNodesRegex(nodes []string, waveSize, wave int) string {
	if waveSize < 1 {
		waveSize = 1
	}
	start := wave * waveSize
	end := min(start+waveSize, len(nodes))
	if start >= len(nodes) || start < 0 {
		return ""
	}
	escaped := make([]string, 0, end-start)
	for _, n := range nodes[start:end] {
		escaped = append(escaped, regexp.QuoteMeta(n))
	}
	return strings.Join(escaped, "|")
}

// renderQueries expands each query as a Go text/template against tc, before URL-escaping. A
// malformed template or unknown variable is a config error (surfaced as Degraded, never a hold on
// bad data). Plain queries with no {{ }} pass through unchanged.
func renderQueries(queries []resolvedQuery, tc gateTemplateContext) ([]resolvedQuery, error) {
	out := make([]resolvedQuery, len(queries))
	for i, q := range queries {
		tmpl, err := template.New("gate").Option("missingkey=error").Parse(q.query)
		if err != nil {
			return nil, configErr("TemplateInvalid", "gate query %q: %v", q.name, err)
		}
		var b strings.Builder
		if err := tmpl.Execute(&b, tc); err != nil {
			return nil, configErr("TemplateInvalid", "gate query %q: %v", q.name, err)
		}
		q.query = b.String()
		out[i] = q
	}
	return out, nil
}

// normalizeQueries reduces a HealthGate to a flat list of resolvedQuery, applying defaults. A bare
// gate.Query is shorthand for a single "> 0 on every sample" check; gate.Queries carries explicit
// op/threshold (defaults op=gt, threshold=0 when a field is unset, matching the CRD defaults).
func normalizeQueries(gate *fleetv1alpha1.HealthGate) []resolvedQuery {
	if gate.Query != "" {
		return []resolvedQuery{{query: gate.Query, op: "gt", threshold: 0, name: "query"}}
	}
	out := make([]resolvedQuery, 0, len(gate.Queries))
	for i := range gate.Queries {
		q := &gate.Queries[i]
		op := q.Op
		if op == "" {
			op = "gt"
		}
		threshold := 0.0
		if q.Threshold != nil {
			threshold = q.Threshold.AsApproximateFloat64()
		}
		name := q.Name
		if name == "" {
			name = q.Query
		}
		out = append(out, resolvedQuery{query: q.Query, op: op, threshold: threshold, name: name})
	}
	return out
}
