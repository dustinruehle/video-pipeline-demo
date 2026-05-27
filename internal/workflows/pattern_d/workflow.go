package pattern_d

import (
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/temporal-community/video-pipeline-demo/internal/planexec"
	"github.com/temporal-community/video-pipeline-demo/internal/searchattrs"
	"github.com/temporal-community/video-pipeline-demo/internal/types"
)

// PatternDInput is what enters workflow history. The Plan struct has already
// been parsed and validated by the starter — but we re-validate inside the
// workflow as defense-in-depth so tests that bypass the starter still see a
// clean InvalidPlan failure on bad input.
type PatternDInput struct {
	Req  types.MediaIngestRequest
	Plan Plan
}

// DynamicMediaWorkflow is the Pattern D workflow. Same executor as Pattern V;
// only the plan source differs (YAML-parsed Plan vs Go function).
//
// In addition to the YAML-driven main plan, the workflow drains a signal
// channel named SignalAddDerivative after the main plan completes. Any steps
// received via that signal are executed through the same planexec.Execute —
// proving runtime mutability of a running workflow (complement to the
// design-time flexibility of editing the YAML file).
func DynamicMediaWorkflow(ctx workflow.Context, in PatternDInput) error {
	if err := searchattrs.UpsertForWorkflow(ctx, in.Req); err != nil {
		return err
	}
	if err := in.Plan.Validate(); err != nil {
		return temporal.NewNonRetryableApplicationError(
			"plan validation failed", "InvalidPlan", err)
	}
	if _, err := planexec.Execute(ctx, in.Plan.ToSteps(), in.Req); err != nil {
		return err
	}

	ch := workflow.GetSignalChannel(ctx, types.SignalAddDerivative)
	var extras []types.DerivativeStep
	for {
		var s types.DerivativeStep
		if !ch.ReceiveAsync(&s) {
			break
		}
		extras = append(extras, s)
	}
	if len(extras) > 0 {
		if _, err := planexec.Execute(ctx, extras, in.Req); err != nil {
			return err
		}
	}
	return nil
}
