# DEMO_A.md — Pattern V (Versioned + Code-Branched)

**Goal:** Single Temporal Go workflow handles a variable-length, DAG-shaped video processing pipeline with first-class observability, durability across worker failures, and bounded execution that cannot loop endlessly. Centerpiece of the 5/21 customer call.

**Build time:** 4–6 hours including tests.
**Demo runtime:** 8 minutes.
**Build target:** dev server. **Demo target:** Cloud (`./demo-a-cloud.sh`).

---

## Source material to fork

**Prerequisite:** `INSPECTION.md` exists (per CLAUDE.md §Inspect before build).

Upstream: `https://github.com/temporal-community/temporal-ffmpeg-pipeline`, cloned into `vendor-reference/`. Lift FFmpeg invocation pattern, heartbeat cadence, retry tuning. Replace workflow shape (linear → DAG) and type system (inline → centralized).

After Demo A's done definition is met, `rm -rf vendor-reference/`.

---

## Type system (`internal/types/types.go`)

```go
type MediaType string

const (
    MediaTypeShortClip        MediaType = "short_clip"
    MediaTypeStandardHD          MediaType = "standard_hd"
    MediaTypeSpherical MediaType = "spherical"
    MediaTypeMisconfigured      MediaType = "misconfigured"
)

type CameraModel string

const (
    CameraMobile      CameraModel = "mobile"
    CameraActionStandard      CameraModel = "action_standard"
    CameraActionPro CameraModel = "action_pro"
    CameraSpherical        CameraModel = "spherical"
)

type Source string

const (
    SourceWiFiSync   Source = "wifi_sync"
    SourceMobileApp  Source = "mobile_app"
    SourceWebLibrary Source = "web_library"
)

type MediaIngestRequest struct {
    MediaID     string
    CameraModel CameraModel
    MediaType   MediaType
    Source      Source
    InputPath   string
    OutputDir   string // defaults to tmp/output/<MediaID>/ in starter
    DemoMode    bool   // pad activities with sleep + heartbeats for visible pause-resume
}

type DerivativeKind string

const (
    DerivativeMetadata     DerivativeKind = "metadata"
    DerivativeCustomEncode   DerivativeKind = "custom_encode"
    DerivativeFFmpegEncode DerivativeKind = "ffmpeg_encode"
    DerivativeHLS          DerivativeKind = "hls"
    DerivativeEditProxy    DerivativeKind = "edit_proxy"
    DerivativeStabilize    DerivativeKind = "stabilize"
    DerivativeConcat       DerivativeKind = "concat"
    DerivativeMPV          DerivativeKind = "mpv"
    DerivativeThumbnail    DerivativeKind = "thumbnail"
    DerivativeProjection   DerivativeKind = "projection"
    DerivativeBitrateLow   DerivativeKind = "bitrate_low"
    DerivativeBitrateHigh  DerivativeKind = "bitrate_high"
    DerivativePublish      DerivativeKind = "publish"
)

type DerivativeStep struct {
    Kind      DerivativeKind
    DependsOn []DerivativeKind
}

type MediaPlan struct {
    MediaType MediaType
    Steps     []DerivativeStep
}

type DerivativeOutput struct {
    Kind       DerivativeKind
    Path       string
    Bytes      int64
    DurationMs int64
}

type ProduceDerivativeInput struct {
    MediaID string
    Kind    DerivativeKind
    Inputs  []DerivativeOutput // upstream outputs in the same workflow execution
    Req     MediaIngestRequest
    Config  map[string]any // optional per-step config (used by Pattern D)
}
```

---

## Search attributes (`internal/searchattrs/searchattrs.go`)

Three custom search attributes registered idempotently via `scripts/register-search-attrs.sh`. Script handles both dev (`temporal operator search-attribute create`) and cloud (`tcld namespace search-attributes add`).

| Name | Type | Purpose |
|---|---|---|
| `MediaID` | Keyword | Headline observability moment. |
| `CameraModel` | Keyword | Filter by camera. |
| `MediaType` | Keyword | Filter by pipeline shape. |

```go
package searchattrs

import (
    "go.temporal.io/sdk/temporal"
    "go.temporal.io/sdk/workflow"
)

var (
    MediaID     = temporal.NewSearchAttributeKeyString("MediaID")
    CameraModel = temporal.NewSearchAttributeKeyString("CameraModel")
    MediaType   = temporal.NewSearchAttributeKeyString("MediaType")
)

func UpsertForWorkflow(ctx workflow.Context, req types.MediaIngestRequest) error {
    return workflow.UpsertTypedSearchAttributes(ctx,
        MediaID.ValueSet(req.MediaID),
        CameraModel.ValueSet(string(req.CameraModel)),
        MediaType.ValueSet(string(req.MediaType)),
    )
}

// EnsureRegistered is a no-op marker called by worker main.go to make
// the dependency explicit. The actual registration is done by the
// scripts/register-search-attrs.sh setup step, not from Go code.
func EnsureRegistered(_ client.Client) error { return nil }
```

Workflow calls `UpsertForWorkflow` as its first action so the workflow is searchable before any activity runs.

---

## Shared executor (`internal/planexec/`)

**Used by BOTH Pattern V and Pattern D.** DAG-aware level-by-level executor: groups steps into dependency levels, executes each level in parallel via futures, waits before advancing. Same parallelism behavior for both patterns — they differ only in plan source.

`internal/planexec/plan.go`:

```go
func validate(steps []types.DerivativeStep) error {
    // 1. All Kind values are known DerivativeKind constants.
    // 2. All DependsOn references point to a Kind present in the slice.
    // 3. No cycles (DFS).
    // Return descriptive errors; these become non-retryable.
}

// groupByLevel returns slice-of-slices. Within each level, sorted by string(Kind) ascending.
// No map iteration. Deterministic across runs.
func groupByLevel(steps []types.DerivativeStep) [][]types.DerivativeStep {
    // Topological sort by Kahn's algorithm.
    // Each "level" = the set of steps with all dependencies satisfied at that round.
    // Sort each level alphabetically by Kind before returning.
}
```

`internal/planexec/executor.go`:

```go
func Execute(ctx workflow.Context, steps []types.DerivativeStep, req types.MediaIngestRequest) (map[types.DerivativeKind]types.DerivativeOutput, error) {
    if err := validate(steps); err != nil {
        return nil, temporal.NewNonRetryableApplicationError("invalid plan", "InvalidPlan", err)
    }
    levels := groupByLevel(steps)
    completed := map[types.DerivativeKind]types.DerivativeOutput{}

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

    for _, level := range levels {
        type pending struct {
            kind types.DerivativeKind
            f    workflow.Future
        }
        futures := make([]pending, 0, len(level))
        for _, step := range level {
            inputs := gatherInputs(step, completed)
            f := workflow.ExecuteActivity(ctx, activities.ProduceDerivative,
                types.ProduceDerivativeInput{
                    MediaID: req.MediaID,
                    Kind:    step.Kind,
                    Inputs:  inputs,
                    Req:     req,
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

func gatherInputs(step types.DerivativeStep, completed map[types.DerivativeKind]types.DerivativeOutput) []types.DerivativeOutput {
    // Build inputs in the order DependsOn lists them (slice, not map iteration).
    out := make([]types.DerivativeOutput, 0, len(step.DependsOn))
    for _, dep := range step.DependsOn {
        out = append(out, completed[dep])
    }
    return out
}
```

Note `gatherInputs` iterates `step.DependsOn` (slice — deterministic), not `completed` (map — would be non-deterministic).

---

## Activities (`internal/activities/`)

**ONE registered activity: `ProduceDerivative`.** Sub-routines are internal to the package, never separately registered. This eliminates the inconsistency where some "activities" were table entries but the workflow only called one.

`internal/activities/produce.go`:

```go
func ProduceDerivative(ctx context.Context, in types.ProduceDerivativeInput) (types.DerivativeOutput, error) {
    logger := activity.GetLogger(ctx)
    logger.Info("produce derivative", "mediaID", in.MediaID, "kind", in.Kind)

    if in.Req.DemoMode {
        // Pad with a brief sleep + heartbeat so pause-resume is visible to the human eye.
        if err := sleepWithHeartbeat(ctx, 2*time.Second); err != nil {
            return types.DerivativeOutput{}, err
        }
    }

    // Misconfigured-input guard: only custom_encode for misconfigured media fails.
    // Retry policy caps attempts at 5 → workflow fails cleanly in ~15 seconds.
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
        return transcodeCustom(ctx, in, outDir) // stand-in for a proprietary in-house encoder
    case types.DerivativePublish:
        return publishMedia(ctx, in, outDir)
    default:
        return types.DerivativeOutput{}, temporal.NewNonRetryableApplicationError(
            fmt.Sprintf("unknown derivative kind: %s", in.Kind),
            "UnknownDerivativeKind", nil)
    }
}
```

`internal/activities/subroutines.go` — internal helpers (not exported, not registered as activities):

| Sub-routine | What it does | Stub or real? |
|---|---|---|
| `extractMetadata` | `ffprobe -v error -show_format -show_streams -of json <input>` → parse + return DurationMs/codec | Real |
| `transcodeFFmpeg` | Shells out to `ffmpeg` with kind-specific args; parses `frame=N` from stderr to heartbeat | Real |
| `transcodeCustom` | STUB. Sleeps 2-4s with heartbeats, writes marker file `custom_<mediaid>_<kind>.bin`. Top-of-file comment: "Stand-in for a proprietary in-house transcoding engine. Real implementation is customer-specific and not part of this demo." | Stub |
| `publishMedia` | STUB. Writes `published_<mediaid>.json` with list of derivative paths from inputs | Stub |
| `sleepWithHeartbeat` | Helper. Sleeps in 1s chunks calling `activity.RecordHeartbeat` between them | — |

**Misconfigured timing math (re-verified):** retry policy is InitialInterval=1s, BackoffCoefficient=2.0, MaximumInterval=10s, MaximumAttempts=5. Activity returns instantly (error path, no sleep). Total time = sum of 4 backoff waits between 5 attempts: 1 + 2 + 4 + 8 = **15 seconds**. Workflow fails after attempt 5. Narration says "~15 seconds."

---

## Plan derivation (`internal/workflows/pattern_v/plan.go`)

```go
func PlanFor(mt types.MediaType) types.MediaPlan {
    switch mt {
    case types.MediaTypeShortClip:
        // 4 steps
        return types.MediaPlan{MediaType: mt, Steps: []types.DerivativeStep{
            {Kind: types.DerivativeMetadata},
            {Kind: types.DerivativeFFmpegEncode, DependsOn: []types.DerivativeKind{types.DerivativeMetadata}},
            {Kind: types.DerivativeMPV, DependsOn: []types.DerivativeKind{types.DerivativeFFmpegEncode}},
            {Kind: types.DerivativePublish, DependsOn: []types.DerivativeKind{types.DerivativeMetadata, types.DerivativeMPV}},
        }}
    case types.MediaTypeStandardHD:
        // 5 steps: add hls after mpv
        // ...
    case types.MediaTypeSpherical:
        // 12 steps:
        // metadata → concat → custom_encode → stabilize → projection
        // projection → mpv → hls → bitrate_low, bitrate_high
        // projection → edit_proxy
        // projection → thumbnail
        // publish depends on metadata, hls, edit_proxy, thumbnail, bitrate_low, bitrate_high
        // ...
    case types.MediaTypeMisconfigured:
        // Includes custom_encode → activity fails every attempt → retries exhaust at 5 → workflow fails in ~15s.
        return types.MediaPlan{MediaType: mt, Steps: []types.DerivativeStep{
            {Kind: types.DerivativeMetadata},
            {Kind: types.DerivativeCustomEncode, DependsOn: []types.DerivativeKind{types.DerivativeMetadata}},
            {Kind: types.DerivativeFFmpegEncode, DependsOn: []types.DerivativeKind{types.DerivativeMetadata}},
            {Kind: types.DerivativeMPV, DependsOn: []types.DerivativeKind{types.DerivativeFFmpegEncode}},
            {Kind: types.DerivativePublish, DependsOn: []types.DerivativeKind{types.DerivativeCustomEncode, types.DerivativeMPV}},
        }}
    default:
        panic("unknown media type")
    }
}
```

---

## Workflow (`internal/workflows/pattern_v/workflow.go`)

```go
func MediaProcessingWorkflow(ctx workflow.Context, req types.MediaIngestRequest) error {
    if err := searchattrs.UpsertForWorkflow(ctx, req); err != nil {
        return err
    }
    plan := PlanFor(req.MediaType)

    // === DEMO TOGGLE: REPLAY-REGRESSION DEMONSTRATION ===
    // To show the replay test catching a workflow-code regression during the live
    // demo, uncomment the next line. The rehearsal script does this with sed,
    // re-runs the replay test (which fails because captured histories don't expect
    // the extra step), then re-comments. Do NOT use workflow.SideEffect for this —
    // SideEffect caches its result in history and would not produce the failure
    // during replay against an old history.
    //
    // plan.Steps = append(plan.Steps, types.DerivativeStep{Kind: types.DerivativeThumbnail, DependsOn: []types.DerivativeKind{types.DerivativeMPV}})
    // === END DEMO TOGGLE ===

    _, err := planexec.Execute(ctx, plan.Steps, req)
    return err
}
```

The exact comment marker — `// === DEMO TOGGLE: REPLAY-REGRESSION DEMONSTRATION ===` — must appear verbatim so the rehearsal script's `sed` can find and toggle the line below it reliably.

Workflow execution timeout (set by starter): 5 minutes. Outer bound.

---

## Starter (`cmd/starter-a/main.go`)

Reads `.env` (via `os.Getenv` directly — no library needed since `tclient` handles defaults). Builds client via `tclient.New()`. Submits, **blocks on `run.Get()`** with 90s context timeout, prints status + UI URL. Exits 0 even on workflow failure (rehearsal scripts continue).

```go
opts := client.StartWorkflowOptions{
    ID:                       fmt.Sprintf("video-pipeline-a-%s", req.MediaID),
    TaskQueue:                "video-pipeline-a",
    WorkflowExecutionTimeout: 5 * time.Minute,
}
run, err := c.ExecuteWorkflow(ctx, opts, pattern_v.MediaProcessingWorkflow, req)
if err != nil {
    log.Fatalf("execute workflow: %v", err) // hard error: connection/auth issue
}

waitCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
defer cancel()
runErr := run.Get(waitCtx, nil)

desc, _ := c.DescribeWorkflowExecution(context.Background(), run.GetID(), run.GetRunID())
fmt.Printf("Workflow %s ended with status %s\n", run.GetID(), desc.WorkflowExecutionInfo.Status)
fmt.Printf("View: %s/%s\n", uiBaseURL(), run.GetID())
if runErr != nil {
    fmt.Printf("(workflow returned error: %v)\n", runErr)
}
```

`uiBaseURL()` returns `http://localhost:8233/namespaces/default/workflows` on dev or `https://cloud.temporal.io/namespaces/<ns>/workflows` on cloud, mirroring `lib.sh ui_url()`.

CLI flags: `--media-id`, `--media-type`, `--camera`, `--source`, `--input`, `--demo-mode` (set true in rehearsal scripts).

---

## Tests

### `internal/activities/activities_test.go`

Each sub-routine: happy-path + failure-mode. `transcodeFFmpeg` uses a 2-second `testdata/fixtures/tiny.mp4` so the suite stays sub-second per test. Override `OutputDir` to `t.TempDir()` so tests don't pollute `tmp/output/`.

### `internal/planexec/executor_test.go`

This is where the parallelism + structural assertions live. Workflow tests don't try to assert on real concurrency (testsuite serializes).

1. `TestValidate_RejectsCycle`
2. `TestValidate_RejectsUnknownKind`
3. `TestValidate_RejectsMissingDependency`
4. `TestGroupByLevel_FlatPlan` — linear chain → N levels of 1.
5. `TestGroupByLevel_ParallelLeaves` — three deps on one root → 2 levels (root, then triple).
6. `TestGroupByLevel_DAGOrdering` — for spherical: assert exact level contents.
7. `TestGroupByLevel_Deterministic` — same input 100× → identical output.

### `internal/workflows/pattern_v/workflow_test.go`

Uses `testsuite.WorkflowTestSuite` with mocked `ProduceDerivative`.

1. `TestIPhoneBasicPlan_ExecutesFourSteps` — assert exactly 4 invocations of ProduceDerivative with correct Kind values.
2. `TestSphericalChapteredPlan_ExecutesTwelveSteps`.
3. `TestSphericalChaptered_DependenciesPassedAsInputs` — assert that the `Inputs` field on HLS's ProduceDerivative call contains MPV's output, MPV's contains projection's, etc. Structural, not timing.
4. `TestMisconfigured_FailsCleanlyInBoundedTime` — mock ProduceDerivative to return error for custom_encode; assert workflow fails (not hangs) with a clear error message; the test should complete well under the workflow execution timeout (the testsuite advances simulated time).
5. `TestActivityRetry_TransientFailure` — `env.OnActivity(activities.ProduceDerivative, ...).Return(...).Times(2)` to fail twice, then success; assert workflow completes.
6. `TestSearchAttributesStamped` — assert MediaID/CameraModel/MediaType upserted before first activity.

### `internal/workflows/pattern_v/replay_test.go`

**Important pattern:** skip cleanly if histories don't exist yet (avoids the chicken-and-egg with `make test` on a fresh clone).

```go
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
```

Same shape for `TestReplay_12Step`. Histories are produced by `make capture-histories` (see CLAUDE.md).

**Why no env-var toggle for the demo failure?** `workflow.SideEffect` caches its result in workflow history. On replay against an old history, the cached value is returned regardless of the current env var. The env-var-driven "break replay" mechanism would not actually produce a replay failure — it would silently pass. The honest mechanism is changing the workflow code itself, which is also a better demo narrative (audience sees the code change land).

### `make test-integration`

Pre-flight check (dev server up). Submit `iphone_basic`, assert COMPLETED within 60s, confirm searchable by MediaID. Submit `misconfigured`, assert FAILED within 60s. Cap at 10 workflows per invocation.

---

## Rehearsal script (`scripts/demo-a-walkthrough.sh`)

Used by both `./demo-a.sh` (sets target dev) and `./demo-a-cloud.sh` (sets target cloud). Reads `TEMPORAL_TARGET`, defaults to dev. PID files. SIGSTOP/SIGCONT for pause-resume. Uses pre-compiled `./bin/` binaries.

```bash
#!/usr/bin/env bash
set -euo pipefail
source scripts/lib.sh
mkdir -p tmp

TARGET="${TEMPORAL_TARGET:-dev}"
say "=== Demo A: Pattern V on TEMPORAL_TARGET=$TARGET ==="
say "UI: $(ui_url)"
echo

# On dev, ensure server + setup are in place.
if [ "$TARGET" = "dev" ]; then
    ./scripts/start-dev-server.sh
    ./scripts/register-search-attrs.sh
    ./scripts/ensure-sample-video.sh
fi

# Pre-compile if binaries missing.
[ -x bin/worker-a ] || make build

./bin/worker-a >tmp/worker-a.log 2>&1 &
echo $! > tmp/worker-a.pid
wait_for_worker "$(cat tmp/worker-a.pid)" || die "worker-a failed to start (see tmp/worker-a.log)"

# IMPORTANT: cleanup stops worker only — leaves dev server running for human UI verification.
cleanup() {
    [ -f tmp/worker-a.pid ] && kill "$(cat tmp/worker-a.pid)" 2>/dev/null || true
    rm -f tmp/worker-a.pid
}
trap cleanup EXIT

# [1/6] iPhone basic (4 steps)
MID_1=$(new_media_id vid_short)
say "[1/6] iPhone basic — 4-step plan"
./bin/starter-a --media-id "$MID_1" --media-type short_clip \
    --camera mobile --input samples/sample.mp4 --demo-mode
wait_for_indexing
say "    → UI: search MediaID = $MID_1 — 4 activities, clean execution"
echo

# [2/6] Spherical chaptered (12 steps, DAG)
MID_2=$(new_media_id vid_spherical)
say "[2/6] Spherical chaptered — 12-step plan, DAG dependencies"
./bin/starter-a --media-id "$MID_2" --media-type spherical \
    --camera spherical --input samples/sample.mp4 --demo-mode
wait_for_indexing
say "    → Same workflow code. 12 activities. HLS waited on MPV waited on projection."
say "    → edit_proxy, thumbnail, hls ran in parallel where DAG allows."
echo

# [3/6] Pause-resume durability — SIGSTOP/SIGCONT for instant visual resume
MID_3=$(new_media_id vid_pause)
say "[3/6] Pause-resume durability"
./bin/starter-a --media-id "$MID_3" --media-type spherical \
    --camera spherical --input samples/sample.mp4 --demo-mode &
STARTER_PID=$!
sleep 6
say "    → SIGSTOP worker"
kill -STOP "$(cat tmp/worker-a.pid)"
sleep 8
say "    → SIGCONT worker — resumes from last completed activity, no work redone"
kill -CONT "$(cat tmp/worker-a.pid)"
wait "$STARTER_PID" || true
wait_for_indexing
say "    → Activity-level checkpointing in action."
echo

# [4/6] Endless-loop guard
MID_4=$(new_media_id vid_bad)
say "[4/6] Misconfigured media — endless-loop guard"
./bin/starter-a --media-id "$MID_4" --media-type misconfigured \
    --camera action_standard --input samples/sample.mp4 --demo-mode || true
wait_for_indexing
say "    → Retry cap of 5 → workflow failed in ~15 seconds."
say "    → No infinite loop. No system lockup."
echo

# [5/6] Replay test — pass, then break via sed, then revert
say "[5/6] Replay test — passes today"
go test ./internal/workflows/pattern_v/ -run TestReplay -v
say "    → Now uncomment the DEMO TOGGLE line in workflow.go and re-run..."
WF_FILE="internal/workflows/pattern_v/workflow.go"
# Uncomment the line below DEMO TOGGLE marker
sed -i.bak '/DEMO TOGGLE: REPLAY-REGRESSION DEMONSTRATION/,/END DEMO TOGGLE/ {
    s|^    // plan.Steps = append|    plan.Steps = append|
}' "$WF_FILE"
go test ./internal/workflows/pattern_v/ -run TestReplay -v || true
say "    → Replay failed: captured history doesn't include the new step."
say "    → This is how you catch workflow-code regressions before prod."
# Restore
mv "${WF_FILE}.bak" "$WF_FILE"
go test ./internal/workflows/pattern_v/ -run TestReplay -v >/dev/null
say "    → Restored. Replay passes again."
echo

# [6/6] UI walkthrough
say "[6/6] Open the UI ($(ui_url)) and search these MediaIDs:"
say "    - $MID_1   (4 steps, clean)"
say "    - $MID_2   (12 steps with DAG)"
say "    - $MID_3   (paused, resumed)"
say "    - $MID_4   (misconfigured, clean failure)"
say "=== Demo A complete ==="
```

The `sed` UID toggle relies on the exact `// === DEMO TOGGLE: REPLAY-REGRESSION DEMONSTRATION ===` marker in `workflow.go` and the prefix `    // plan.Steps = append`. If you change either, update the sed expression to match.

---

## Verification before declaring Demo A done

- [ ] `make test` passes after `make capture-histories` (replay tests included)
- [ ] `make test-integration` passes against dev
- [ ] `./demo-a.sh` runs end-to-end with no human intervention
- [ ] UI shows each MediaID via search after `wait_for_indexing`
- [ ] Pause-resume produces a COMPLETED workflow (no failure)
- [ ] Misconfigured fails in 13–17 seconds (target ~15)
- [ ] Replay-break sed toggle: passes, breaks, restores — all three transitions clean
- [ ] `./demo-a-cloud.sh` exists, is executable, sources `.env.cloud` correctly (NOT actually run against Cloud during build)
- [ ] `./demo-a.sh` total runtime 6–10 minutes cold. Record in README.md.
