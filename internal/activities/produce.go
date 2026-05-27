// Package activities holds the single registered activity (ProduceDerivative)
// and its internal subroutines. Sub-routines are package-private on purpose:
// only ONE activity is registered with the worker, and it dispatches on
// DerivativeKind. This keeps the activity registration surface tight and
// matches the upstream "one activity, one job" mental model.
package activities

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/temporal-community/video-pipeline-demo/internal/types"
)

// ProduceDerivative is the single activity registered with both workers. It
// dispatches on Kind to internal subroutines. Validation of unknown kinds
// returns a non-retryable error so the workflow fails fast.
func ProduceDerivative(ctx context.Context, in types.ProduceDerivativeInput) (types.DerivativeOutput, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("produce derivative", "mediaID", in.MediaID, "kind", in.Kind)

	if in.Req.DemoMode {
		if err := sleepWithHeartbeat(ctx, 2*time.Second); err != nil {
			return types.DerivativeOutput{}, err
		}
	}

	// Misconfigured-input guard: only custom_encode for misconfigured media
	// fails. Retry policy caps at 5 attempts → workflow fails in ~15 seconds
	// (1 + 2 + 4 + 8 = 15s of backoff between 5 instantaneous attempts).
	if in.Req.MediaType == types.MediaTypeMisconfigured && in.Kind == types.DerivativeCustomEncode {
		return types.DerivativeOutput{}, errors.New("custom encoder misconfigured for this media type (demo)")
	}

	outDir := in.Req.OutputDir
	if outDir == "" {
		outDir = filepath.Join("tmp", "output", in.MediaID)
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return types.DerivativeOutput{}, fmt.Errorf("mkdir outdir: %w", err)
	}

	switch in.Kind {
	case types.DerivativeMetadata:
		return extractMetadata(ctx, in, outDir)
	case types.DerivativeFFmpegEncode, types.DerivativeMPV, types.DerivativeHLS,
		types.DerivativeBitrateLow, types.DerivativeBitrateHigh, types.DerivativeEditProxy,
		types.DerivativeConcat, types.DerivativeThumbnail:
		return transcodeFFmpeg(ctx, in, outDir)
	case types.DerivativeCustomEncode, types.DerivativeStabilize, types.DerivativeProjection:
		return transcodeCustom(ctx, in, outDir)
	case types.DerivativePublish:
		return publishMedia(ctx, in, outDir)
	default:
		return types.DerivativeOutput{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("unknown derivative kind: %s", in.Kind),
			"UnknownDerivativeKind", nil)
	}
}
