package pattern_d

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/testsuite"

	"github.com/temporal-community/video-pipeline-demo/internal/activities"
	"github.com/temporal-community/video-pipeline-demo/internal/types"
)

func runPlan(t *testing.T, planPath string, expected int) {
	t.Helper()
	plan, err := ParseFile(planPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := plan.Validate(); err != nil {
		t.Fatal(err)
	}

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(activities.ProduceDerivative)

	calls := 0
	env.OnActivity(activities.ProduceDerivative, mock.Anything, mock.Anything).
		Return(func(_ context.Context, in types.ProduceDerivativeInput) (types.DerivativeOutput, error) {
			calls++
			return types.DerivativeOutput{Kind: in.Kind, Path: "x"}, nil
		})

	env.ExecuteWorkflow(DynamicMediaWorkflow, PatternDInput{
		Req: types.MediaIngestRequest{
			MediaID:     "vid_dsl_test",
			MediaType:   types.MediaType(plan.Name),
			CameraModel: types.CameraMobile,
		},
		Plan: plan,
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}
	if calls != expected {
		t.Fatalf("expected %d activity invocations, got %d", expected, calls)
	}
}

func TestWorkflow_ShortClip_FourActivities(t *testing.T) {
	runPlan(t, "../../../dsl/short_clip.yaml", 4)
}
func TestWorkflow_StandardHD_FiveActivities(t *testing.T) {
	runPlan(t, "../../../dsl/standard_hd.yaml", 5)
}
func TestWorkflow_Spherical_TwelveActivities(t *testing.T) {
	runPlan(t, "../../../dsl/spherical_chaptered.yaml", 12)
}

func TestAddDerivativeSignal_RunsExtraActivity(t *testing.T) {
	plan, err := ParseFile("../../../dsl/short_clip.yaml")
	if err != nil {
		t.Fatal(err)
	}

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(activities.ProduceDerivative)

	called := map[types.DerivativeKind]int{}
	env.OnActivity(activities.ProduceDerivative, mock.Anything, mock.Anything).
		Return(func(_ context.Context, in types.ProduceDerivativeInput) (types.DerivativeOutput, error) {
			called[in.Kind]++
			return types.DerivativeOutput{Kind: in.Kind, Path: "x"}, nil
		})

	// Pre-queue the signal at workflow start so it's drained after the main plan.
	env.RegisterDelayedCallback(func() {
		env.SignalWorkflow(types.SignalAddDerivative, types.DerivativeStep{
			Kind:      types.DerivativeThumbnail,
			DependsOn: nil,
		})
	}, 0)

	env.ExecuteWorkflow(DynamicMediaWorkflow, PatternDInput{
		Req: types.MediaIngestRequest{
			MediaID:     "vid_signal_test",
			MediaType:   types.MediaType(plan.Name),
			CameraModel: types.CameraMobile,
		},
		Plan: plan,
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("workflow did not complete")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("workflow error: %v", err)
	}

	total := 0
	for _, n := range called {
		total += n
	}
	if total != 5 {
		t.Fatalf("expected 5 invocations (4 plan + 1 signal-added thumbnail), got %d: %v", total, called)
	}
	if called[types.DerivativeThumbnail] != 1 {
		t.Fatalf("expected exactly 1 thumbnail call, got %d", called[types.DerivativeThumbnail])
	}
}

func TestWorkflow_Cycle_FailsImmediately(t *testing.T) {
	// Bypass the starter: hand a Plan with a cycle directly to the workflow.
	src := []byte(`
name: bad_cycle
version: 1
derivatives:
  - kind: metadata
    depends_on: [publish]
  - kind: publish
    depends_on: [metadata]
`)
	plan, err := Parse(src)
	if err != nil {
		t.Fatal(err)
	}

	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(activities.ProduceDerivative)

	called := false
	env.OnActivity(activities.ProduceDerivative, mock.Anything, mock.Anything).
		Return(func(_ context.Context, _ types.ProduceDerivativeInput) (types.DerivativeOutput, error) {
			called = true
			return types.DerivativeOutput{}, nil
		})

	env.ExecuteWorkflow(DynamicMediaWorkflow, PatternDInput{
		Req:  types.MediaIngestRequest{MediaID: "x", MediaType: "bad_cycle"},
		Plan: plan,
	})

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected completion")
	}
	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("expected InvalidPlan error")
	}
	if called {
		t.Fatal("activity should not be invoked when plan is invalid")
	}
}
