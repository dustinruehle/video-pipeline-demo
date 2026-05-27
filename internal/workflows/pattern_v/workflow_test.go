package pattern_v

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"

	"github.com/temporal-community/video-pipeline-demo/internal/activities"
	"github.com/temporal-community/video-pipeline-demo/internal/types"
)

func newReq(mt types.MediaType, camera types.CameraModel) types.MediaIngestRequest {
	return types.MediaIngestRequest{
		MediaID:     "vid_test",
		MediaType:   mt,
		CameraModel: camera,
		InputPath:   "samples/sample.mp4",
	}
}

func TestIPhoneBasicPlan_ExecutesFourSteps(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(activities.ProduceDerivative)

	called := map[types.DerivativeKind]int{}
	env.OnActivity(activities.ProduceDerivative, mock.Anything, mock.Anything).
		Return(func(_ context.Context, in types.ProduceDerivativeInput) (types.DerivativeOutput, error) {
			called[in.Kind]++
			return types.DerivativeOutput{Kind: in.Kind, Path: "x"}, nil
		})

	env.ExecuteWorkflow(MediaProcessingWorkflow, newReq(types.MediaTypeShortClip, types.CameraMobile))

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected workflow completion")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []types.DerivativeKind{
		types.DerivativeMetadata, types.DerivativeFFmpegEncode,
		types.DerivativeMPV, types.DerivativePublish,
	}
	if total := totalCalls(called); total != len(want) {
		t.Fatalf("expected %d invocations, got %d: %v", len(want), total, called)
	}
	for _, k := range want {
		if called[k] != 1 {
			t.Fatalf("missing call for %s (got %v)", k, called)
		}
	}
}

func TestSphericalPlan_ExecutesTwelveSteps(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(activities.ProduceDerivative)

	called := map[types.DerivativeKind]int{}
	env.OnActivity(activities.ProduceDerivative, mock.Anything, mock.Anything).
		Return(func(_ context.Context, in types.ProduceDerivativeInput) (types.DerivativeOutput, error) {
			called[in.Kind]++
			return types.DerivativeOutput{Kind: in.Kind, Path: "x"}, nil
		})

	env.ExecuteWorkflow(MediaProcessingWorkflow, newReq(types.MediaTypeSpherical, types.CameraSpherical))

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected completion")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if total := totalCalls(called); total != 12 {
		t.Fatalf("expected 12 invocations, got %d: %v", total, called)
	}
}

func TestSphericalPlan_DependenciesPassedAsInputs(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(activities.ProduceDerivative)

	// Track inputs per kind, asserting hls's input list contains mpv output.
	inputs := map[types.DerivativeKind][]types.DerivativeKind{}
	env.OnActivity(activities.ProduceDerivative, mock.Anything, mock.Anything).
		Return(func(_ context.Context, in types.ProduceDerivativeInput) (types.DerivativeOutput, error) {
			parents := make([]types.DerivativeKind, 0, len(in.Inputs))
			for _, p := range in.Inputs {
				parents = append(parents, p.Kind)
			}
			inputs[in.Kind] = parents
			return types.DerivativeOutput{Kind: in.Kind, Path: "x"}, nil
		})

	env.ExecuteWorkflow(MediaProcessingWorkflow, newReq(types.MediaTypeSpherical, types.CameraSpherical))

	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("workflow did not complete cleanly: err=%v", env.GetWorkflowError())
	}

	// HLS depends on MPV
	if got := inputs[types.DerivativeHLS]; len(got) != 1 || got[0] != types.DerivativeMPV {
		t.Fatalf("hls inputs: want [mpv], got %v", got)
	}
	// MPV depends on projection
	if got := inputs[types.DerivativeMPV]; len(got) != 1 || got[0] != types.DerivativeProjection {
		t.Fatalf("mpv inputs: want [projection], got %v", got)
	}
	// Publish depends on six parents (order matches PlanFor)
	pubParents := inputs[types.DerivativePublish]
	if len(pubParents) != 6 {
		t.Fatalf("publish should have 6 parents, got %d: %v", len(pubParents), pubParents)
	}
}

func TestMisconfigured_FailsCleanlyInBoundedTime(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(activities.ProduceDerivative)

	env.OnActivity(activities.ProduceDerivative, mock.Anything, mock.Anything).
		Return(func(_ context.Context, in types.ProduceDerivativeInput) (types.DerivativeOutput, error) {
			if in.Kind == types.DerivativeCustomEncode {
				return types.DerivativeOutput{}, errors.New("custom encoder misconfigured for this media type (demo)")
			}
			return types.DerivativeOutput{Kind: in.Kind, Path: "x"}, nil
		})

	env.ExecuteWorkflow(MediaProcessingWorkflow, newReq(types.MediaTypeMisconfigured, types.CameraActionStandard))

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected completion (with failure)")
	}
	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("expected workflow error, got nil")
	}
}

func TestActivityRetry_TransientFailure(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(activities.ProduceDerivative)

	// The first two invocations of `metadata` fail; thereafter the activity
	// — and any other kind — returns a synthetic success.
	attempts := 0
	env.OnActivity(activities.ProduceDerivative, mock.Anything, mock.Anything).
		Return(func(_ context.Context, in types.ProduceDerivativeInput) (types.DerivativeOutput, error) {
			if in.Kind == types.DerivativeMetadata {
				attempts++
				if attempts <= 2 {
					return types.DerivativeOutput{}, errors.New("transient")
				}
			}
			return types.DerivativeOutput{Kind: in.Kind, Path: "x"}, nil
		})

	env.ExecuteWorkflow(MediaProcessingWorkflow, newReq(types.MediaTypeShortClip, types.CameraMobile))

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected completion")
	}
	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("expected success after transient failures, got %v", err)
	}
	if attempts < 3 {
		t.Fatalf("expected at least 3 metadata attempts, got %d", attempts)
	}
}

func TestStandardHD_FiveSteps(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(activities.ProduceDerivative)

	called := map[types.DerivativeKind]int{}
	env.OnActivity(activities.ProduceDerivative, mock.Anything, mock.Anything).
		Return(func(_ context.Context, in types.ProduceDerivativeInput) (types.DerivativeOutput, error) {
			called[in.Kind]++
			return types.DerivativeOutput{Kind: in.Kind, Path: "x"}, nil
		})

	env.ExecuteWorkflow(MediaProcessingWorkflow, newReq(types.MediaTypeStandardHD, types.CameraActionStandard))

	if !env.IsWorkflowCompleted() || env.GetWorkflowError() != nil {
		t.Fatalf("unexpected: err=%v", env.GetWorkflowError())
	}
	if total := totalCalls(called); total != 5 {
		t.Fatalf("expected 5 invocations, got %d: %v", total, called)
	}
}

func TestCancellation_RunsCleanupActivity(t *testing.T) {
	var ts testsuite.WorkflowTestSuite
	env := ts.NewTestWorkflowEnvironment()
	env.RegisterActivity(activities.ProduceDerivative)
	env.RegisterActivity(activities.MarkCancelled)

	// ProduceDerivative honors cancellation so the workflow can be canceled
	// mid-plan.
	env.OnActivity(activities.ProduceDerivative, mock.Anything, mock.Anything).
		Return(func(ctx context.Context, in types.ProduceDerivativeInput) (types.DerivativeOutput, error) {
			select {
			case <-ctx.Done():
				return types.DerivativeOutput{}, ctx.Err()
			case <-time.After(50 * time.Millisecond):
				return types.DerivativeOutput{Kind: in.Kind, Path: "x"}, nil
			}
		})

	cleanupCalls := 0
	env.OnActivity(activities.MarkCancelled, mock.Anything, mock.Anything).
		Return(func(_ context.Context, _ types.MediaIngestRequest) error {
			cleanupCalls++
			return nil
		})

	env.RegisterDelayedCallback(func() {
		env.CancelWorkflow()
	}, 10*time.Millisecond)

	env.ExecuteWorkflow(MediaProcessingWorkflow, newReq(types.MediaTypeSpherical, types.CameraSpherical))

	if !env.IsWorkflowCompleted() {
		t.Fatal("expected workflow to terminate")
	}
	err := env.GetWorkflowError()
	if err == nil {
		t.Fatal("expected CanceledError, got nil")
	}
	var canceled *temporal.CanceledError
	if !errors.As(err, &canceled) {
		t.Fatalf("expected CanceledError, got %T: %v", err, err)
	}
	if cleanupCalls != 1 {
		t.Fatalf("expected MarkCancelled to be called once, got %d", cleanupCalls)
	}
}

func totalCalls(m map[types.DerivativeKind]int) int {
	n := 0
	for _, v := range m {
		n += v
	}
	return n
}
