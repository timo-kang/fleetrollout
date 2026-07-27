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
