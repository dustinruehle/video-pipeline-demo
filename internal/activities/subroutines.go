// Stand-in for a proprietary in-house transcoding engine. Real implementation
// is customer-specific and not part of this demo.
//
// This file holds all of ProduceDerivative's internal helpers — none of them
// are registered as Temporal activities. The single registered activity is
// produce.go:ProduceDerivative, which dispatches here based on Kind.

package activities

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"

	"github.com/temporal-community/video-pipeline-demo/internal/types"
)

// FFmpeg stderr regexes — adapted verbatim from
// temporal-community/temporal-ffmpeg-pipeline (MIT, see NOTICES.md).
var (
	progressRegex = regexp.MustCompile(`time=(\d+:\d+:\d+\.\d+)`)
	durationRegex = regexp.MustCompile(`Duration: (\d+:\d+:\d+\.\d+)`)
)

// sleepWithHeartbeat sleeps in 1-second chunks, calling RecordHeartbeat
// between each chunk so a 10s heartbeat timeout is never approached.
func sleepWithHeartbeat(ctx context.Context, total time.Duration) error {
	remaining := total
	for remaining > 0 {
		step := time.Second
		if remaining < step {
			step = remaining
		}
		select {
		case <-time.After(step):
		case <-ctx.Done():
			return ctx.Err()
		}
		activity.RecordHeartbeat(ctx, map[string]any{"elapsed_ms": (total - remaining + step).Milliseconds()})
		remaining -= step
	}
	return nil
}

// extractMetadata shells out to ffprobe and parses the JSON output. Real
// activity: produces a metadata.json file in outDir.
func extractMetadata(ctx context.Context, in types.ProduceDerivativeInput, outDir string) (types.DerivativeOutput, error) {
	probe := exec.CommandContext(ctx, "ffprobe",
		"-v", "error",
		"-show_format",
		"-show_streams",
		"-of", "json",
		in.Req.InputPath,
	)
	out, err := probe.Output()
	if err != nil {
		return types.DerivativeOutput{}, fmt.Errorf("ffprobe: %w", err)
	}

	var probed struct {
		Format struct {
			Duration string `json:"duration"`
		} `json:"format"`
	}
	if jerr := json.Unmarshal(out, &probed); jerr != nil {
		return types.DerivativeOutput{}, fmt.Errorf("ffprobe json: %w", jerr)
	}

	durMs := int64(0)
	if probed.Format.Duration != "" {
		if secs, perr := strconv.ParseFloat(probed.Format.Duration, 64); perr == nil {
			durMs = int64(secs * 1000)
		}
	}

	dest := filepath.Join(outDir, "metadata.json")
	if werr := os.WriteFile(dest, out, 0o644); werr != nil {
		return types.DerivativeOutput{}, fmt.Errorf("write metadata: %w", werr)
	}
	st, _ := os.Stat(dest)
	return types.DerivativeOutput{
		Kind:       in.Kind,
		Path:       dest,
		Bytes:      st.Size(),
		DurationMs: durMs,
	}, nil
}

// transcodeFFmpeg runs ffmpeg with kind-specific args. Heartbeats every
// 5 seconds from a goroutine; progress is parsed from stderr.
func transcodeFFmpeg(ctx context.Context, in types.ProduceDerivativeInput, outDir string) (types.DerivativeOutput, error) {
	args, outPath := ffmpegArgsFor(in.Kind, in.Req.InputPath, outDir, in.MediaID)
	cmd := exec.CommandContext(ctx, "ffmpeg", args...)

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return types.DerivativeOutput{}, fmt.Errorf("stderr pipe: %w", err)
	}
	if err := cmd.Start(); err != nil {
		return types.DerivativeOutput{}, fmt.Errorf("ffmpeg start: %w", err)
	}

	var totalDurationMs, currentMs int64

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	done := make(chan struct{})
	defer close(done)

	go func() {
		for {
			select {
			case <-ticker.C:
				activity.RecordHeartbeat(ctx, map[string]any{
					"progress_ms": currentMs,
					"total_ms":    totalDurationMs,
				})
			case <-done:
				return
			}
		}
	}()

	scanner := bufio.NewScanner(io.LimitReader(stderr, 10*1024*1024))
	for scanner.Scan() {
		line := scanner.Text()
		if totalDurationMs == 0 {
			if m := durationRegex.FindStringSubmatch(line); len(m) > 1 {
				totalDurationMs = timeToMilliseconds(m[1])
			}
		}
		if m := progressRegex.FindStringSubmatch(line); len(m) > 1 {
			currentMs = timeToMilliseconds(m[1])
		}
	}

	if err := cmd.Wait(); err != nil {
		return types.DerivativeOutput{}, fmt.Errorf("ffmpeg %s: %w", in.Kind, err)
	}

	st, _ := os.Stat(outPath)
	bytes := int64(0)
	if st != nil {
		bytes = st.Size()
	}
	return types.DerivativeOutput{
		Kind:       in.Kind,
		Path:       outPath,
		Bytes:      bytes,
		DurationMs: totalDurationMs,
	}, nil
}

// ffmpegArgsFor returns kind-specific ffmpeg args + the produced output path.
// Args are intentionally minimal — we transcode the same 10-second sample
// many times during a rehearsal and need each step under ~2 seconds.
func ffmpegArgsFor(kind types.DerivativeKind, input, outDir, mediaID string) ([]string, string) {
	base := []string{"-y", "-loglevel", "info", "-i", input}
	switch kind {
	case types.DerivativeFFmpegEncode, types.DerivativeMPV:
		out := filepath.Join(outDir, fmt.Sprintf("%s_%s.mp4", kind, mediaID))
		return append(base, "-c:v", "libx264", "-preset", "ultrafast", "-crf", "30", "-t", "2", out), out
	case types.DerivativeHLS:
		out := filepath.Join(outDir, fmt.Sprintf("hls_%s.m3u8", mediaID))
		return append(base, "-c:v", "libx264", "-preset", "ultrafast", "-crf", "32", "-t", "2",
			"-hls_time", "1", "-hls_list_size", "0", "-f", "hls", out), out
	case types.DerivativeBitrateLow:
		out := filepath.Join(outDir, fmt.Sprintf("bitrate_low_%s.mp4", mediaID))
		return append(base, "-c:v", "libx264", "-preset", "ultrafast", "-b:v", "200k", "-t", "2", out), out
	case types.DerivativeBitrateHigh:
		out := filepath.Join(outDir, fmt.Sprintf("bitrate_high_%s.mp4", mediaID))
		return append(base, "-c:v", "libx264", "-preset", "ultrafast", "-b:v", "2000k", "-t", "2", out), out
	case types.DerivativeEditProxy:
		out := filepath.Join(outDir, fmt.Sprintf("edit_proxy_%s.mp4", mediaID))
		return append(base, "-c:v", "libx264", "-preset", "ultrafast", "-vf", "scale=640:-2", "-t", "2", out), out
	case types.DerivativeConcat:
		// Standalone "concat" stand-in: re-encode the same input as if it
		// were a concatenation of chaptered segments. Cheap and visible.
		out := filepath.Join(outDir, fmt.Sprintf("concat_%s.mp4", mediaID))
		return append(base, "-c:v", "libx264", "-preset", "ultrafast", "-crf", "30", "-t", "2", out), out
	case types.DerivativeThumbnail:
		out := filepath.Join(outDir, fmt.Sprintf("thumb_%s.jpg", mediaID))
		return append(base, "-frames:v", "1", "-q:v", "5", out), out
	default:
		// Fallback that produces _something_, so the switch in produce.go
		// stays a simple table. Kinds not listed here should never reach
		// transcodeFFmpeg in practice.
		out := filepath.Join(outDir, fmt.Sprintf("%s_%s.mp4", kind, mediaID))
		return append(base, "-c:v", "libx264", "-preset", "ultrafast", "-crf", "30", "-t", "1", out), out
	}
}

// transcodeCustom is the stand-in for the proprietary in-house encoder. It
// sleeps 2-4 seconds (or whatever sleep_ms config says), heartbeats, writes
// a marker file. Real implementation is customer-specific.
func transcodeCustom(ctx context.Context, in types.ProduceDerivativeInput, outDir string) (types.DerivativeOutput, error) {
	sleep := 2*time.Second + time.Duration(rand.Int63n(int64(2*time.Second)))
	if in.Config != nil {
		if v, ok := in.Config["sleep_ms"]; ok {
			switch n := v.(type) {
			case int:
				sleep = time.Duration(n) * time.Millisecond
			case int64:
				sleep = time.Duration(n) * time.Millisecond
			case float64:
				sleep = time.Duration(n) * time.Millisecond
			}
		}
	}
	if err := sleepWithHeartbeat(ctx, sleep); err != nil {
		return types.DerivativeOutput{}, err
	}

	out := filepath.Join(outDir, fmt.Sprintf("custom_%s_%s.bin", in.MediaID, in.Kind))
	if err := os.WriteFile(out, []byte(fmt.Sprintf("stand-in %s\n", in.Kind)), 0o644); err != nil {
		return types.DerivativeOutput{}, fmt.Errorf("custom marker: %w", err)
	}
	st, _ := os.Stat(out)
	return types.DerivativeOutput{Kind: in.Kind, Path: out, Bytes: st.Size()}, nil
}

// publishMedia is the terminal step. Writes a manifest listing every
// upstream derivative path. Stub — a real publish would push to a CDN.
func publishMedia(_ context.Context, in types.ProduceDerivativeInput, outDir string) (types.DerivativeOutput, error) {
	paths := make([]string, 0, len(in.Inputs))
	for _, dep := range in.Inputs {
		if dep.Path != "" {
			paths = append(paths, dep.Path)
		}
	}
	manifest := map[string]any{
		"media_id":    in.MediaID,
		"camera":      in.Req.CameraModel,
		"media_type":  in.Req.MediaType,
		"derivatives": paths,
		"published":   time.Now().Format(time.RFC3339),
	}
	dest := filepath.Join(outDir, fmt.Sprintf("published_%s.json", in.MediaID))
	body, _ := json.MarshalIndent(manifest, "", "  ")
	if err := os.WriteFile(dest, body, 0o644); err != nil {
		return types.DerivativeOutput{}, fmt.Errorf("write manifest: %w", err)
	}
	st, _ := os.Stat(dest)
	return types.DerivativeOutput{Kind: in.Kind, Path: dest, Bytes: st.Size()}, nil
}

// timeToMilliseconds converts ffmpeg HH:MM:SS.MS to int64 ms.
// Adapted verbatim from upstream (MIT, see NOTICES.md).
func timeToMilliseconds(timeStr string) int64 {
	parts := strings.Split(timeStr, ":")
	if len(parts) != 3 {
		return 0
	}
	hours, _ := strconv.ParseInt(parts[0], 10, 64)
	minutes, _ := strconv.ParseInt(parts[1], 10, 64)
	secondsParts := strings.Split(parts[2], ".")
	seconds, _ := strconv.ParseInt(secondsParts[0], 10, 64)
	var milliseconds int64
	if len(secondsParts) > 1 {
		msStr := secondsParts[1]
		if len(msStr) > 3 {
			msStr = msStr[:3]
		} else if len(msStr) < 3 {
			msStr = msStr + strings.Repeat("0", 3-len(msStr))
		}
		milliseconds, _ = strconv.ParseInt(msStr, 10, 64)
	}
	return (hours*3600+minutes*60+seconds)*1000 + milliseconds
}
