# video-pipeline-demo

Two Temporal Go SDK demos showing how a branching, DAG-shaped video pipeline
can be expressed durably and observably:

- **Pattern V** (`internal/workflows/pattern_v/`) — versioned, plan-in-code.
- **Pattern D** (`internal/workflows/pattern_d/`) — dynamic, plan-in-YAML.

Both patterns share the same executor (`internal/planexec/`) and activity
implementation (`internal/activities/`). The only thing that differs is
where the plan comes from.

## Six demoable behaviors

1. Search for a media ID in the UI and land on the workflow.
2. Pause the worker mid-pipeline (SIGSTOP) and watch it resume from the last
   completed activity (SIGCONT).
3. Submit a misconfigured media item; the workflow fails cleanly in ~15
   seconds (5 retry attempts with exponential backoff), never loops.
4. Run the same workflow code against a 4-step short clip and a 12-step
   spherical chaptered pipeline.
5. Catch a non-determinism bug via `WorkflowReplayer` against a captured
   history (Demo A only — the rehearsal script toggles the change).
6. Hand-edit a YAML plan and watch the next run pick it up — no code
   change, no worker restart (Demo B only).

## Setup

```bash
make setup
```

`make setup` runs `scripts/install-prereqs.sh` first (installs `ffmpeg` and
the Temporal CLI via Homebrew on macOS; verifies presence on Linux), then
downloads Go modules, starts the local Temporal dev server, registers the
three search attributes, generates a synthetic sample clip, and
pre-compiles all four binaries.

Prerequisites you provide:

- macOS or Linux (macOS is primary; Linux is best-effort).
- Go 1.22+. `go version` must succeed.
- On macOS, Homebrew must be installed.
- The Temporal Web UI runs at <http://localhost:8233>.

To stop everything: `make teardown`.

## Running the demos against dev

```bash
./demo-a.sh           # Pattern V (versioned + code-branched) — non-stop, ~2 min
./demo-b.sh           # Pattern D (dynamic DSL)              — non-stop, ~1 min

./demo-a.sh --step    # Pause between moments — for live narration
./demo-b.sh --step    # (same)
```

Both scripts:

- Start the dev server if not already up.
- Register search attributes idempotently.
- Generate a sample video if `samples/sample.mp4` is missing.
- Spin up the corresponding worker; tear it down on exit.
- **Leave the dev server running** so a human can verify in the UI.

**Live walkthrough mode (`--step`)** prints a yellow prompt before each narrated moment (and before the most interesting sub-moments inside the pause-resume and replay-test steps). Press **Enter** to advance. Use this when presenting to a customer — you control pacing and can talk to whatever just happened, or set up what's about to happen, before continuing.

## Running the demos on Temporal Cloud

1. Copy `.env.cloud.example` to `.env.cloud` and fill in the four required
   values (`TEMPORAL_ADDRESS`, `TEMPORAL_NAMESPACE`, `TEMPORAL_API_KEY`).
2. Make sure `tcld` is installed (the registration script calls it to
   register the three search attributes against the Cloud namespace).
3. Run:

   ```bash
   ./demo-a-cloud.sh
   ./demo-b-cloud.sh
   ```

The cloud launchers refuse to run against any namespace whose name contains
`prod`, `production`, `prd`, `live`, or `staging`. Override with care.

## Tests

```bash
make test               # unit + replay tests against captured histories
make capture-histories  # produce testdata/histories/*.json (committed)
make test-integration   # run a small number of workflows against the dev server
make lint               # gofmt -l . + go vet
```

Replay tests (`internal/workflows/pattern_v/replay_test.go` and
`internal/workflows/pattern_d/replay_test.go`) skip cleanly if the
corresponding history JSON is missing. Run `make capture-histories` once
after a fresh clone to produce them.

## Adding a new media type

- **Pattern V:** edit `internal/workflows/pattern_v/plan.go`, add a new
  case to `PlanFor`, add a constant in `internal/types/types.go`, write a
  test, ship a new worker.
- **Pattern D:** drop a new `<media_type>.yaml` into `dsl/`, pass
  `--plan-file dsl/<media_type>.yaml` to `starter-b`. No code change.

## Repository layout

```
cmd/worker-{a,b}     # registers activities, runs forever
cmd/starter-{a,b}    # submits one workflow, blocks for result, prints UI URL
internal/types       # shared structs and DerivativeKind constants
internal/searchattrs # typed search attribute keys + UpsertForWorkflow
internal/tclient     # dev/cloud-aware client builder
internal/planexec    # DAG executor used by both patterns
internal/activities  # single registered activity + internal subroutines
internal/workflows/pattern_v  # versioned, plan-in-code
internal/workflows/pattern_d  # dynamic, plan-in-YAML
dsl                  # YAML plans consumed by Pattern D
samples              # demo input video lives here
scripts              # lifecycle + rehearsal helpers
testdata/histories   # captured histories used by replay tests
testdata/fixtures    # tiny clips for activity unit tests
deck                 # customer deck (built independently of the code)
```

See `CLAUDE.md`, `DEMO_A.md`, `DEMO_B.md`, `DECK.md` for the build spec and
`INSPECTION.md` for the upstream-reference inspection.

## Acknowledgements

Portions of `internal/activities/subroutines.go` (FFmpeg stderr regexes,
`timeToMilliseconds`, heartbeat-via-ticker pattern) are adapted from
`temporal-community/temporal-ffmpeg-pipeline` (MIT). See `NOTICES.md` for
the full upstream attribution.
