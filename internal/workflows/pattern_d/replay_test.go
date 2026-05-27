package pattern_d

import (
	"errors"
	"os"
	"testing"

	"go.temporal.io/sdk/worker"
)

func TestReplay_12Step(t *testing.T) {
	const path = "../../../testdata/histories/pattern_d_12step.json"
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		t.Skip("no captured history; run `make capture-histories` first")
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(DynamicMediaWorkflow)
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path); err != nil {
		t.Fatalf("replay failed: %v", err)
	}
}
