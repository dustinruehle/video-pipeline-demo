// Package planexec is the DAG executor shared by Pattern V and Pattern D.
// Plans differ in where they come from (Go function vs YAML); execution is
// identical: validate, group steps by topological level, run each level in
// parallel via futures, wait, advance.
package planexec

import (
	"fmt"
	"sort"

	"github.com/temporal-community/video-pipeline-demo/internal/types"
)

// validate enforces the three plan invariants:
//  1. Every Kind is a known DerivativeKind constant.
//  2. Every DependsOn entry references a Kind present in the same plan.
//  3. The dependency graph contains no cycles.
//
// Validation errors are converted to non-retryable application errors by the
// caller — a malformed plan can never succeed on retry.
func validate(steps []types.DerivativeStep) error {
	if len(steps) == 0 {
		return fmt.Errorf("plan has no steps")
	}

	kindSet := make(map[types.DerivativeKind]bool, len(steps))
	for _, s := range steps {
		if !types.KnownDerivativeKinds[s.Kind] {
			return fmt.Errorf("unknown derivative kind: %q", s.Kind)
		}
		if kindSet[s.Kind] {
			return fmt.Errorf("duplicate derivative kind: %q", s.Kind)
		}
		kindSet[s.Kind] = true
	}

	for _, s := range steps {
		for _, dep := range s.DependsOn {
			if !kindSet[dep] {
				return fmt.Errorf("step %q depends on missing kind %q", s.Kind, dep)
			}
			if dep == s.Kind {
				return fmt.Errorf("step %q depends on itself", s.Kind)
			}
		}
	}

	if cyc := detectCycle(steps); cyc != nil {
		return fmt.Errorf("dependency cycle detected: %v", cyc)
	}
	return nil
}

// detectCycle returns nil if the graph is acyclic. Otherwise it returns one
// concrete cycle (slice of Kinds in traversal order) for the error message.
func detectCycle(steps []types.DerivativeStep) []types.DerivativeKind {
	adj := make(map[types.DerivativeKind][]types.DerivativeKind, len(steps))
	order := make([]types.DerivativeKind, 0, len(steps))
	for _, s := range steps {
		adj[s.Kind] = append([]types.DerivativeKind(nil), s.DependsOn...)
		order = append(order, s.Kind)
	}

	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[types.DerivativeKind]int, len(steps))

	var stack []types.DerivativeKind
	var dfs func(types.DerivativeKind) []types.DerivativeKind
	dfs = func(node types.DerivativeKind) []types.DerivativeKind {
		color[node] = gray
		stack = append(stack, node)
		for _, dep := range adj[node] {
			switch color[dep] {
			case gray:
				cycle := append([]types.DerivativeKind(nil), stack...)
				return append(cycle, dep)
			case white:
				if cyc := dfs(dep); cyc != nil {
					return cyc
				}
			}
		}
		color[node] = black
		stack = stack[:len(stack)-1]
		return nil
	}

	for _, node := range order {
		if color[node] == white {
			if cyc := dfs(node); cyc != nil {
				return cyc
			}
		}
	}
	return nil
}

// groupByLevel returns a slice-of-slices where each inner slice is the set
// of steps with all dependencies satisfied at that round (Kahn's algorithm).
// Within each level, steps are sorted alphabetically by Kind so the order
// is deterministic across runs — replay-stable.
func groupByLevel(steps []types.DerivativeStep) [][]types.DerivativeStep {
	indeg := make(map[types.DerivativeKind]int, len(steps))
	stepByKind := make(map[types.DerivativeKind]types.DerivativeStep, len(steps))
	for _, s := range steps {
		stepByKind[s.Kind] = s
		indeg[s.Kind] = len(s.DependsOn)
	}

	// reverse adjacency: who depends on me
	rev := make(map[types.DerivativeKind][]types.DerivativeKind, len(steps))
	for _, s := range steps {
		for _, dep := range s.DependsOn {
			rev[dep] = append(rev[dep], s.Kind)
		}
	}

	// To keep determinism, drain in alphabetical order at every level.
	remaining := make([]types.DerivativeKind, 0, len(steps))
	for _, s := range steps {
		remaining = append(remaining, s.Kind)
	}
	sort.Slice(remaining, func(i, j int) bool { return remaining[i] < remaining[j] })

	var levels [][]types.DerivativeStep
	for len(remaining) > 0 {
		var current []types.DerivativeStep
		var leftover []types.DerivativeKind
		for _, k := range remaining {
			if indeg[k] == 0 {
				current = append(current, stepByKind[k])
			} else {
				leftover = append(leftover, k)
			}
		}
		if len(current) == 0 {
			// Should be unreachable: validate() has already rejected cycles.
			return levels
		}
		for _, s := range current {
			for _, child := range rev[s.Kind] {
				indeg[child]--
			}
		}
		levels = append(levels, current)
		remaining = leftover
	}
	return levels
}
