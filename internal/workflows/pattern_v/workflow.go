// Package pattern_v hosts the "Pattern V" workflow — versioned, plan-in-code.
// The plan for each media type is hard-coded in plan.go; changing a plan
// means a Go PR and a new worker build.
package pattern_v

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/temporal-community/video-pipeline-demo/internal/activities"
	"github.com/temporal-community/video-pipeline-demo/internal/planexec"
	"github.com/temporal-community/video-pipeline-demo/internal/searchattrs"
	"github.com/temporal-community/video-pipeline-demo/internal/types"
)

// MediaProcessingWorkflow is the single workflow that handles every media
// type. The plan (4–12 steps) is selected by PlanFor(MediaType) and executed
// as a DAG by planexec.Execute.
func MediaProcessingWorkflow(ctx workflow.Context, req types.MediaIngestRequest) error {
	if err := searchattrs.UpsertForWorkflow(ctx, req); err != nil {
		return err
	}

	// Cleanup-on-cancel: runs MarkCancelled on a disconnected context so the
	// manifest write still happens after the workflow's main ctx is canceled.
	defer func() {
		if ctx.Err() == nil {
			return
		}
		dctx, _ := workflow.NewDisconnectedContext(ctx)
		dctx = workflow.WithActivityOptions(dctx, workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		_ = workflow.ExecuteActivity(dctx, activities.MarkCancelled, req).Get(dctx, nil)
	}()

	plan := PlanFor(req.MediaType)

	// === DEMO TOGGLE: REPLAY-REGRESSION DEMONSTRATION ===
	// To show the replay test catching a workflow-code regression during the
	// live demo, uncomment the next line. The rehearsal script does this with
	// sed, re-runs the replay test (which fails because captured histories
	// don't expect the extra step), then re-comments. Do NOT use
	// workflow.SideEffect for this — SideEffect caches its result in history
	// and would not produce the failure during replay against an old history.
	//
	// plan.Steps = append(plan.Steps, types.DerivativeStep{Kind: types.DerivativeThumbnail, DependsOn: []types.DerivativeKind{types.DerivativeMPV}})
	// === END DEMO TOGGLE ===

	_, err := planexec.Execute(ctx, plan.Steps, req)
	return err
}
