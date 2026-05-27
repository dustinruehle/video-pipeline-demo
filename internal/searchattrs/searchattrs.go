// Package searchattrs defines the three custom search attributes the demos
// rely on for headline observability moments. Registration itself happens
// out-of-band in scripts/register-search-attrs.sh; this package owns the
// typed keys and the workflow-side upsert call.
package searchattrs

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/temporal-community/video-pipeline-demo/internal/types"
)

var (
	MediaID     = temporal.NewSearchAttributeKeyKeyword("MediaID")
	CameraModel = temporal.NewSearchAttributeKeyKeyword("CameraModel")
	MediaType   = temporal.NewSearchAttributeKeyKeyword("MediaType")
)

// UpsertForWorkflow stamps the three search attributes onto the running
// workflow execution. Call this as the FIRST action of every workflow so the
// execution is searchable in the UI before any activity runs.
func UpsertForWorkflow(ctx workflow.Context, req types.MediaIngestRequest) error {
	return workflow.UpsertTypedSearchAttributes(ctx,
		MediaID.ValueSet(req.MediaID),
		CameraModel.ValueSet(string(req.CameraModel)),
		MediaType.ValueSet(string(req.MediaType)),
	)
}

// EnsureRegistered is a marker called by worker main.go to make the
// dependency between the worker and the attribute registration explicit.
// The actual registration is performed by scripts/register-search-attrs.sh
// during `make setup`; this is intentionally a no-op so workers don't need
// admin privileges at runtime.
func EnsureRegistered(_ client.Client) error { return nil }
