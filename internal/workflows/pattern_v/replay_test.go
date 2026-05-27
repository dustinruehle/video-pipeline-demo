package pattern_v

import (
	"errors"
	"os"
	"testing"

	"go.temporal.io/sdk/worker"
)

// Replay tests skip cleanly if a captured history is missing. Run
// `make capture-histories` after the worker is healthy to generate them.

func TestReplay_4Step(t *testing.T) {
	const path = "../../../testdata/histories/pattern_v_4step.json"
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		t.Skip("no captured history; run `make capture-histories` first")
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(MediaProcessingWorkflow)
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path); err != nil {
		t.Fatalf("replay failed: %v", err)
	}
}

func TestReplay_12Step(t *testing.T) {
	const path = "../../../testdata/histories/pattern_v_12step.json"
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		t.Skip("no captured history; run `make capture-histories` first")
	}
	replayer := worker.NewWorkflowReplayer()
	replayer.RegisterWorkflow(MediaProcessingWorkflow)
	if err := replayer.ReplayWorkflowHistoryFromJSONFile(nil, path); err != nil {
		t.Fatalf("replay failed: %v", err)
	}
}
