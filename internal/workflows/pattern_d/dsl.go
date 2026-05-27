// Package pattern_d hosts the "Pattern D" workflow — dynamic, plan-in-YAML.
// The DSL is parsed and validated client-side by the starter, then passed to
// the workflow as a typed struct. This keeps the YAML library out of the
// workflow path entirely (replay-safe across yaml library versions).
package pattern_d

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"

	"github.com/temporal-community/video-pipeline-demo/internal/types"
)

// Plan is the typed equivalent of a YAML file under dsl/. The workflow
// receives this struct, not raw YAML bytes — YAML is unmarshaled by the
// starter.
type Plan struct {
	Name        string           `yaml:"name" json:"name"`
	Version     int              `yaml:"version" json:"version"`
	Description string           `yaml:"description,omitempty" json:"description,omitempty"`
	Derivatives []DerivativeSpec `yaml:"derivatives" json:"derivatives"`
}

type DerivativeSpec struct {
	Kind      types.DerivativeKind   `yaml:"kind" json:"kind"`
	DependsOn []types.DerivativeKind `yaml:"depends_on,omitempty" json:"depends_on,omitempty"`
	Config    map[string]any         `yaml:"config,omitempty" json:"config,omitempty"`
}

// Parse decodes a YAML byte slice into a Plan. Does NOT validate; call
// Validate() afterwards.
func Parse(b []byte) (Plan, error) {
	var p Plan
	if err := yaml.Unmarshal(b, &p); err != nil {
		return Plan{}, fmt.Errorf("yaml unmarshal: %w", err)
	}
	return p, nil
}

// ParseFile reads a YAML file and decodes it.
func ParseFile(path string) (Plan, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return Plan{}, fmt.Errorf("read %s: %w", path, err)
	}
	return Parse(b)
}

// Validate enforces:
//  1. name is non-empty
//  2. version is positive
//  3. derivatives is non-empty
//  4. every kind is in types.KnownDerivativeKinds
//  5. every depends_on entry references a kind present in the same plan
//  6. no cycles in the DAG
func (p Plan) Validate() error {
	if p.Name == "" {
		return fmt.Errorf("plan name is required")
	}
	if p.Version <= 0 {
		return fmt.Errorf("plan version must be positive (got %d)", p.Version)
	}
	if len(p.Derivatives) == 0 {
		return fmt.Errorf("plan has no derivatives")
	}

	known := make(map[types.DerivativeKind]bool, len(p.Derivatives))
	for _, d := range p.Derivatives {
		if !types.KnownDerivativeKinds[d.Kind] {
			return fmt.Errorf("unknown derivative kind: %q", d.Kind)
		}
		if known[d.Kind] {
			return fmt.Errorf("duplicate derivative kind: %q", d.Kind)
		}
		known[d.Kind] = true
	}
	for _, d := range p.Derivatives {
		for _, dep := range d.DependsOn {
			if !known[dep] {
				return fmt.Errorf("derivative %q depends on missing kind %q", d.Kind, dep)
			}
			if dep == d.Kind {
				return fmt.Errorf("derivative %q depends on itself", d.Kind)
			}
		}
	}

	// Cycle detection via DFS (white/gray/black). Mirrors planexec.detectCycle
	// but operates on DerivativeSpec ordering so we can return the offending kinds.
	color := make(map[types.DerivativeKind]int, len(p.Derivatives))
	adj := make(map[types.DerivativeKind][]types.DerivativeKind, len(p.Derivatives))
	for _, d := range p.Derivatives {
		adj[d.Kind] = append([]types.DerivativeKind(nil), d.DependsOn...)
	}
	var stack []types.DerivativeKind
	var dfs func(types.DerivativeKind) error
	dfs = func(node types.DerivativeKind) error {
		color[node] = 1
		stack = append(stack, node)
		for _, dep := range adj[node] {
			switch color[dep] {
			case 1:
				return fmt.Errorf("dependency cycle: %v -> %v", stack, dep)
			case 0:
				if err := dfs(dep); err != nil {
					return err
				}
			}
		}
		color[node] = 2
		stack = stack[:len(stack)-1]
		return nil
	}
	for _, d := range p.Derivatives {
		if color[d.Kind] == 0 {
			if err := dfs(d.Kind); err != nil {
				return err
			}
		}
	}
	return nil
}

// ToSteps converts a validated Plan into the executor's wire format.
// Order matches the YAML source order (no map iteration), so the conversion
// is deterministic across runs.
func (p Plan) ToSteps() []types.DerivativeStep {
	out := make([]types.DerivativeStep, 0, len(p.Derivatives))
	for _, d := range p.Derivatives {
		deps := append([]types.DerivativeKind(nil), d.DependsOn...)
		var cfg map[string]any
		if len(d.Config) > 0 {
			cfg = make(map[string]any, len(d.Config))
			for k, v := range d.Config {
				cfg[k] = v
			}
		}
		out = append(out, types.DerivativeStep{
			Kind:      d.Kind,
			DependsOn: deps,
			Config:    cfg,
		})
	}
	return out
}
