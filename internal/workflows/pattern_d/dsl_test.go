package pattern_d

import (
	"reflect"
	"strings"
	"testing"

	"github.com/temporal-community/video-pipeline-demo/internal/types"
)

func TestParse_AllSampleFiles(t *testing.T) {
	cases := []struct {
		path       string
		wantSteps  int
		wantName   string
		wantConfig bool
	}{
		{"../../../dsl/short_clip.yaml", 4, "short_clip", false},
		{"../../../dsl/standard_hd.yaml", 5, "standard_hd", false},
		{"../../../dsl/spherical_chaptered.yaml", 12, "spherical_chaptered", true},
	}
	for _, tc := range cases {
		t.Run(tc.path, func(t *testing.T) {
			p, err := ParseFile(tc.path)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			if err := p.Validate(); err != nil {
				t.Fatalf("validate: %v", err)
			}
			if p.Name != tc.wantName {
				t.Errorf("name: want %q got %q", tc.wantName, p.Name)
			}
			if len(p.Derivatives) != tc.wantSteps {
				t.Errorf("step count: want %d got %d", tc.wantSteps, len(p.Derivatives))
			}
			if tc.wantConfig {
				found := false
				for _, d := range p.Derivatives {
					if d.Config != nil {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected at least one derivative with config")
				}
			}
		})
	}
}

func TestValidate_RejectsCycle(t *testing.T) {
	src := []byte(`
name: bad_cycle
version: 1
derivatives:
  - kind: metadata
    depends_on: [publish]
  - kind: publish
    depends_on: [metadata]
`)
	p, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}
	err = p.Validate()
	if err == nil || !strings.Contains(err.Error(), "cycle") {
		t.Fatalf("want cycle error, got %v", err)
	}
}

func TestValidate_RejectsUnknownKind(t *testing.T) {
	src := []byte(`
name: unknown
version: 1
derivatives:
  - kind: not_real
    depends_on: []
`)
	p, _ := Parse(src)
	err := p.Validate()
	if err == nil || !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("want unknown-kind error, got %v", err)
	}
}

func TestValidate_RejectsMissingDependency(t *testing.T) {
	src := []byte(`
name: missing
version: 1
derivatives:
  - kind: mpv
    depends_on: [metadata]
`)
	p, _ := Parse(src)
	err := p.Validate()
	if err == nil || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("want missing-dep error, got %v", err)
	}
}

func TestValidate_RejectsMissingName(t *testing.T) {
	src := []byte(`
version: 1
derivatives:
  - kind: metadata
`)
	p, _ := Parse(src)
	err := p.Validate()
	if err == nil {
		t.Fatal("expected name-required error")
	}
}

func TestValidate_RejectsZeroVersion(t *testing.T) {
	src := []byte(`
name: x
version: 0
derivatives:
  - kind: metadata
`)
	p, _ := Parse(src)
	err := p.Validate()
	if err == nil || !strings.Contains(err.Error(), "version") {
		t.Fatalf("want version error, got %v", err)
	}
}

func TestToSteps_Deterministic(t *testing.T) {
	p, err := ParseFile("../../../dsl/spherical_chaptered.yaml")
	if err != nil {
		t.Fatal(err)
	}
	first := p.ToSteps()
	for i := 0; i < 100; i++ {
		got := p.ToSteps()
		if !reflect.DeepEqual(got, first) {
			t.Fatalf("ToSteps not deterministic on iteration %d", i)
		}
	}
}

func TestToSteps_PreservesDependencyOrder(t *testing.T) {
	p, err := ParseFile("../../../dsl/spherical_chaptered.yaml")
	if err != nil {
		t.Fatal(err)
	}
	steps := p.ToSteps()
	// publish has 6 parents in a specific YAML order
	for _, s := range steps {
		if s.Kind != types.DerivativePublish {
			continue
		}
		want := []types.DerivativeKind{
			types.DerivativeMetadata,
			types.DerivativeHLS,
			types.DerivativeEditProxy,
			types.DerivativeThumbnail,
			types.DerivativeBitrateLow,
			types.DerivativeBitrateHigh,
		}
		if !reflect.DeepEqual(s.DependsOn, want) {
			t.Fatalf("publish deps order: want %v got %v", want, s.DependsOn)
		}
		return
	}
	t.Fatal("publish step not found")
}

func TestToSteps_PreservesConfig(t *testing.T) {
	p, err := ParseFile("../../../dsl/spherical_chaptered.yaml")
	if err != nil {
		t.Fatal(err)
	}
	steps := p.ToSteps()
	for _, s := range steps {
		if s.Kind == types.DerivativeCustomEncode {
			if v, ok := s.Config["sleep_ms"]; !ok || v == nil {
				t.Fatalf("expected sleep_ms config on custom_encode, got %v", s.Config)
			}
			return
		}
	}
	t.Fatal("custom_encode step not found")
}
