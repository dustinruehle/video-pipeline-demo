# DEMO_B.md — Pattern D (Dynamic DSL)

**Goal:** Same Temporal workflow code executes any pipeline shape, driven by a human-readable YAML derivatives map. New media type = config change, not code change.

**Build time:** 3–4 hours after Demo A.
**Demo runtime:** 5 minutes.
**Build target:** dev server. **Demo target:** Cloud (`./demo-b-cloud.sh`).
**Dependency:** Demo A must be done first. Pattern D reuses `internal/activities/` and `internal/planexec/` verbatim.

---

## What's shared with Pattern V (do not duplicate)

| Concern | Lives in | Notes |
|---|---|---|
| Activities | `internal/activities/` | Same `ProduceDerivative`. |
| Plan execution | `internal/planexec/` | Same DAG executor. |
| Search attributes | `internal/searchattrs/` | Same three. |
| Type system | `internal/types/` | Same types. |
| Client | `internal/tclient/` | Same dev/cloud helper. |

What's new for Pattern D: `internal/workflows/pattern_d/dsl.go` (YAML parser + validator), `workflow.go` (takes a typed `dsl.Plan` as input), and a new starter that parses YAML on the client side and submits the typed plan.

---

## YAML schema (`dsl/README.md` is the public spec)

```yaml
name: spherical_chaptered
description: Spherical 360° video shot in chaptered segments, with stabilization
version: 1
derivatives:
  - kind: metadata
    depends_on: []
  - kind: concat
    depends_on: [metadata]
  - kind: custom_encode
    depends_on: [concat]
    config:
      sleep_ms: 3000
  - kind: stabilize
    depends_on: [custom_encode]
  - kind: projection
    depends_on: [stabilize]
  - kind: mpv
    depends_on: [projection]
  - kind: hls
    depends_on: [mpv]
  - kind: edit_proxy
    depends_on: [projection]
  - kind: thumbnail
    depends_on: [projection]
  - kind: bitrate_low
    depends_on: [hls]
  - kind: bitrate_high
    depends_on: [hls]
  - kind: publish
    depends_on:
      - metadata
      - hls
      - edit_proxy
      - thumbnail
      - bitrate_low
      - bitrate_high
```

**Schema rules** (enforced in `internal/workflows/pattern_d/dsl.go`):

1. `name` required; uniquely identifies the file.
2. `version` integer; bump on semantics change.
3. Every `kind` must be in `types.DerivativeKind`.
4. Every `depends_on` entry references a `kind` present in the same file.
5. Graph must be a DAG.
6. `config` is optional `map[string]any` passed through to activity input.

```go
// internal/workflows/pattern_d/dsl.go

type Plan struct {
    Name        string           `yaml:"name" json:"name"`
    Version     int              `yaml:"version" json:"version"`
    Description string           `yaml:"description,omitempty" json:"description,omitempty"`
    Derivatives []DerivativeSpec `yaml:"derivatives" json:"derivatives"`
}

type DerivativeSpec struct {
    Kind      types.DerivativeKind   `yaml:"kind" json:"kind"`
    DependsOn []types.DerivativeKind `yaml:"depends_on,omitempty" json:"depends_on,omitempty"`
    Config    map[string]any         `yaml:"config,omitempty" json:"config,omitempty"`
}

func Parse(b []byte) (Plan, error)
func ParseFile(path string) (Plan, error)
func (p Plan) Validate() error                // cycles, unknown kinds, missing deps
func (p Plan) ToSteps() []types.DerivativeStep // deterministic conversion
```

**Critical architectural choice:** the workflow takes a typed `Plan` struct as input, NOT raw YAML bytes. The starter parses + validates YAML client-side before submitting; the typed Plan is what enters workflow history (as JSON via the SDK's data converter). This keeps:
- YAML library version out of the workflow path (no replay risk if yaml.v3 → v4)
- Validation errors surfaced at submit time, before any worker resource is consumed
- The workflow code minimal and easily reviewable

---

## Sample DSL files (`dsl/`)

`dsl/short_clip.yaml` (4 steps):

```yaml
name: short_clip
version: 1
description: Simple iPhone clip via mobile app
derivatives:
  - kind: metadata
    depends_on: []
  - kind: ffmpeg_encode
    depends_on: [metadata]
  - kind: mpv
    depends_on: [ffmpeg_encode]
  - kind: publish
    depends_on: [metadata, mpv]
```

`dsl/standard_hd.yaml` (5 steps): same shape, plus `hls` depending on `mpv`.

`dsl/spherical_chaptered.yaml`: the 12-step plan shown above.

---

## Workflow (`internal/workflows/pattern_d/workflow.go`)

```go
type PatternDInput struct {
    Req  types.MediaIngestRequest
    Plan dsl.Plan
}

func DynamicMediaWorkflow(ctx workflow.Context, in PatternDInput) error {
    if err := searchattrs.UpsertForWorkflow(ctx, in.Req); err != nil {
        return err
    }
    // Validate again inside the workflow as defense-in-depth.
    // The starter has already validated; this catches any future bug where
    // the starter is bypassed (e.g., direct ExecuteWorkflow calls from tests).
    if err := in.Plan.Validate(); err != nil {
        return temporal.NewNonRetryableApplicationError(
            "plan validation failed", "InvalidPlan", err)
    }
    _, err := planexec.Execute(ctx, in.Plan.ToSteps(), in.Req)
    return err
}
```

No YAML parsing inside the workflow. The `Plan` struct is serialized by the SDK's data converter on submission and replay-stable across YAML library versions.

**Live config reload:** the starter reads `--plan-file` at submit time, parses + validates, submits the typed Plan. Editing the YAML between submissions changes the next workflow's plan. Running workflows keep the plan they were started with.

---

## Starter (`cmd/starter-b/main.go`)

CLI flags: `--media-id`, `--plan-file`, `--camera`, `--source`, `--input`, `--demo-mode`.

```go
plan, err := dsl.ParseFile(*planFile)
if err != nil {
    die("plan-file YAML is invalid: %v", err)
}
if err := plan.Validate(); err != nil {
    die("plan-file failed validation: %v", err)
}

opts := client.StartWorkflowOptions{
    ID:                       fmt.Sprintf("video-pipeline-b-%s", req.MediaID),
    TaskQueue:                "video-pipeline-b",
    WorkflowExecutionTimeout: 5 * time.Minute,
}
run, err := c.ExecuteWorkflow(ctx, opts, pattern_d.DynamicMediaWorkflow, pattern_d.PatternDInput{
    Req:  req,
    Plan: plan,
})
// block on run.Get() with 90s timeout, print status + UI URL (same pattern as starter-a)
```

The client-side parse + validate is the Pattern D endless-loop guard: a malformed YAML never reaches the worker.

---

## Tests

### `internal/workflows/pattern_d/dsl_test.go`

1. `TestParse_AllSampleFiles` — three YAMLs parse, step counts 4/5/12.
2. `TestValidate_RejectsCycle` — `A → B → A`.
3. `TestValidate_RejectsUnknownKind` — `kind: not_real`.
4. `TestValidate_RejectsMissingDependency` — dep on undefined step.
5. `TestToSteps_Deterministic` — 100× same plan, identical output.
6. `TestToSteps_PreservesDependencyOrder` — for each step, `DependsOn` slice order matches YAML source.

### `internal/workflows/pattern_d/workflow_test.go`

Mocked `ProduceDerivative` via `testsuite`. Executor concurrency is covered in `planexec/executor_test.go`.

1. `TestWorkflow_IPhoneSimple_FourActivities` — submit parsed iphone_simple Plan, assert 4 invocations.
2. `TestWorkflow_HeroBasic_FiveActivities`.
3. `TestWorkflow_SphericalChaptered_TwelveActivities` — assert step count + structural dependency assertion (HLS's input contains MPV's output).
4. `TestWorkflow_Cycle_FailsImmediately` — bypass the starter, submit a Plan with a cycle directly to the workflow, assert non-retryable `InvalidPlan` before any activity runs.

### `internal/workflows/pattern_d/replay_test.go`

Same skip-if-missing pattern as Pattern V:

```go
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
```

### `make test-integration`

Pre-flight: dev server up. Cap 10 workflows.

1. Submit `short_clip.yaml` → 4 activities, COMPLETED.
2. Submit `spherical_chaptered.yaml` → 12 activities, COMPLETED.
3. Write a runtime-edited YAML to `tmp/edited.yaml` (add a thumbnail step), submit, assert step count is 5.
4. Submit a YAML with a cycle, assert starter exits non-zero with "failed validation" message (workflow never starts).

---

## Rehearsal script (`scripts/demo-b-walkthrough.sh`)

Used by `./demo-b.sh` (dev) and `./demo-b-cloud.sh` (cloud). Reads `TEMPORAL_TARGET`, defaults to dev. Uses `./bin/` binaries and `lib.sh` helpers.

```bash
#!/usr/bin/env bash
set -euo pipefail
source scripts/lib.sh
mkdir -p tmp

TARGET="${TEMPORAL_TARGET:-dev}"
say "=== Demo B: Pattern D on TEMPORAL_TARGET=$TARGET ==="
say "UI: $(ui_url)"
echo

if [ "$TARGET" = "dev" ]; then
    ./scripts/start-dev-server.sh
    ./scripts/register-search-attrs.sh
    ./scripts/ensure-sample-video.sh
fi

[ -x bin/worker-b ] || make build

./bin/worker-b >tmp/worker-b.log 2>&1 &
echo $! > tmp/worker-b.pid
wait_for_worker "$(cat tmp/worker-b.pid)" || die "worker-b failed to start"

cleanup() {
    [ -f tmp/worker-b.pid ] && kill "$(cat tmp/worker-b.pid)" 2>/dev/null || true
    rm -f tmp/worker-b.pid
}
trap cleanup EXIT

# [1/4] Four-step from YAML
MID_1=$(new_media_id vid_dsl_short)
say "[1/4] Four-step plan from YAML"
cat dsl/short_clip.yaml
./bin/starter-b --media-id "$MID_1" --camera mobile \
    --plan-file dsl/short_clip.yaml --input samples/sample.mp4 --demo-mode
wait_for_indexing
say "    → 4 activities. Plan was a YAML file, not Go code."
echo

# [2/4] Twelve-step from YAML, same workflow code
MID_2=$(new_media_id vid_dsl_spherical)
say "[2/4] Twelve-step plan, same workflow code"
cat dsl/spherical_chaptered.yaml
./bin/starter-b --media-id "$MID_2" --camera spherical \
    --plan-file dsl/spherical_chaptered.yaml --input samples/sample.mp4 --demo-mode
wait_for_indexing
say "    → 12 activities. Parallel where the DAG allows."
echo

# [3/4] Add a derivative WITHOUT code change
say "[3/4] Adding thumbnail derivative via YAML edit"
cat > tmp/short_with_thumb.yaml <<'EOF'
name: short_with_thumb
version: 2
derivatives:
  - kind: metadata
    depends_on: []
  - kind: ffmpeg_encode
    depends_on: [metadata]
  - kind: mpv
    depends_on: [ffmpeg_encode]
  - kind: thumbnail
    depends_on: [mpv]
  - kind: publish
    depends_on: [metadata, mpv, thumbnail]
EOF
MID_3=$(new_media_id vid_dsl_thumb)
./bin/starter-b --media-id "$MID_3" --camera mobile \
    --plan-file tmp/short_with_thumb.yaml --input samples/sample.mp4 --demo-mode
wait_for_indexing
say "    → 5 activities. Worker never restarted. No code change."
echo

# [4/4] Cycle rejected pre-flight
say "[4/4] Cycle in YAML — rejected at submit time"
cat > tmp/bad_cycle.yaml <<'EOF'
name: bad_cycle
version: 1
derivatives:
  - kind: metadata
    depends_on: [publish]
  - kind: publish
    depends_on: [metadata]
EOF
MID_4=$(new_media_id vid_dsl_bad)
./bin/starter-b --media-id "$MID_4" --camera mobile \
    --plan-file tmp/bad_cycle.yaml --input samples/sample.mp4 --demo-mode || true
say "    → Cycle detected at submit. Workflow never started."
say "    → Pattern D's endless-loop guard."
echo

say "=== Demo B complete ==="
say "Search the UI ($(ui_url)):"
say "    - $MID_1   (4 steps)"
say "    - $MID_2   (12 steps)"
say "    - $MID_3   (5 steps, thumbnail added)"
```

---

## Verification before declaring Demo B done

- [ ] DSL parser + validator tests pass
- [ ] Workflow tests pass against all three sample YAMLs
- [ ] Replay test passes (after `make capture-histories`)
- [ ] Integration test passes (including runtime-edit and cycle-rejection cases)
- [ ] `./demo-b.sh` runs end-to-end against dev with no human intervention
- [ ] UI shows each MediaID via search
- [ ] Cycle-in-YAML case exits the starter non-zero with a clear "failed validation" message
- [ ] `./demo-b-cloud.sh` exists, is executable, sources `.env.cloud` correctly
- [ ] Activities + planexec are byte-for-byte identical to Demo A (no duplication)
- [ ] `./demo-b.sh` total runtime 4–6 minutes cold. Record in README.md.
