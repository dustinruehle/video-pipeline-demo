package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.temporal.io/sdk/activity"

	"github.com/temporal-community/video-pipeline-demo/internal/types"
)

// MarkCancelled is invoked from a workflow's cancellation-cleanup defer block
// (see pattern_v/workflow.go) via a disconnected context, so it runs even
// after the workflow's main ctx has been canceled. It writes a single
// cancelled.json manifest into the workflow's output directory.
func MarkCancelled(ctx context.Context, req types.MediaIngestRequest) error {
	activity.GetLogger(ctx).Info("mark cancelled", "mediaID", req.MediaID)

	outDir := req.OutputDir
	if outDir == "" {
		outDir = filepath.Join("tmp", "output", req.MediaID)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return fmt.Errorf("mkdir outdir: %w", err)
	}

	manifest := map[string]any{
		"media_id":     req.MediaID,
		"cancelled_at": time.Now().Format(time.RFC3339),
		"reason":       "workflow_cancelled",
	}
	body, _ := json.MarshalIndent(manifest, "", "  ")
	dest := filepath.Join(outDir, "cancelled.json")
	if err := os.WriteFile(dest, body, 0o644); err != nil {
		return fmt.Errorf("write cancelled manifest: %w", err)
	}
	return nil
}
