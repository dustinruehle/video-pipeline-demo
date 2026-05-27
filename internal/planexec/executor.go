package planexec

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/temporal-community/video-pipeline-demo/internal/activities"
	"github.com/temporal-community/video-pipeline-demo/internal/types"
)

// Execute runs the steps as a DAG against the calling workflow context. Levels
// are processed sequentially; steps within a level run in parallel via
// concurrent futures and are awaited before the next level starts.
//
// On any per-step error the executor returns immediately — the outer workflow
// surfaces a clean failure rather than continuing partial work.
func Execute(ctx workflow.Context, steps []types.DerivativeStep, req types.MediaIngestRequest) (map[types.DerivativeKind]types.DerivativeOutput, error) {
	if err := validate(steps); err != nil {
		return nil, temporal.NewNonRetryableApplicationError("invalid plan", "InvalidPlan", err)
	}
	levels := groupByLevel(steps)
	completed := make(map[types.DerivativeKind]types.DerivativeOutput, len(steps))

	actOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute,
		HeartbeatTimeout:    10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    5,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, actOpts)

	type pending struct {
		kind types.DerivativeKind
		f    workflow.Future
	}

	for _, level := range levels {
		futures := make([]pending, 0, len(level))
		for _, step := range level {
			inputs := gatherInputs(step, completed)
			f := workflow.ExecuteActivity(ctx, activities.ProduceDerivative,
				types.ProduceDerivativeInput{
					MediaID: req.MediaID,
					Kind:    step.Kind,
					Inputs:  inputs,
					Req:     req,
					Config:  step.Config,
				})
			futures = append(futures, pending{step.Kind, f})
		}
		for _, p := range futures {
			var out types.DerivativeOutput
			if err := p.f.Get(ctx, &out); err != nil {
				return nil, fmt.Errorf("step %s: %w", p.kind, err)
			}
			completed[p.kind] = out
		}
	}
	return completed, nil
}

// gatherInputs iterates DependsOn (a deterministic slice), NOT the completed
// map. Map iteration order is non-deterministic in Go and would break replay.
func gatherInputs(step types.DerivativeStep, completed map[types.DerivativeKind]types.DerivativeOutput) []types.DerivativeOutput {
	out := make([]types.DerivativeOutput, 0, len(step.DependsOn))
	for _, dep := range step.DependsOn {
		out = append(out, completed[dep])
	}
	return out
}
