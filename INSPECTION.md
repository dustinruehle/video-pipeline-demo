# INSPECTION.md — Upstream reference inspection

## Upstream snapshot

- Repo: https://github.com/temporal-community/temporal-ffmpeg-pipeline
- Commit SHA: `51f45040dcec28f9993857ce05d99bb3f2ff9ec1`
- Files (Go): 4 (`activity.go`, `workflow.go`, `worker/main.go`, `start/main.go`)
- Lines of Go: ~330
- SDK version: `go.temporal.io/sdk v1.33.0` (Go 1.24.0 module)
- License: MIT, Copyright (c) 2025 Temporal Community

## Reuse verbatim

- **FFmpeg progress / duration regex.** `time=(\d+:\d+:\d+\.\d+)` and `Duration: (\d+:\d+:\d+\.\d+)`. Battle-tested against multiple ffmpeg versions.
- **`timeToMilliseconds` helper.** HH:MM:SS.MS → int64 ms. Identical use case in our `extractMetadata`/`transcodeFFmpeg`.
- **Heartbeat-from-ticker pattern.** Goroutine with `time.NewTicker(5 * time.Second)` + `done` channel for cancellation. Cleanly stopped via `defer close(done)`.
- **`bufio.Scanner` over `io.LimitReader`** to bound stderr memory at 10 MiB. We adopt this verbatim.

## Adapt then reuse

- **Activity retry policy.** Upstream: `MaximumAttempts: 3`, `MaximumInterval: 10 * time.Minute`. Ours: `MaximumAttempts: 5`, `MaximumInterval: 10 * time.Second`. The 5-second heartbeat cadence is kept; the timeouts are tightened to make the misconfigured-input demo fail in ~15 seconds.
- **Activity options block.** Same shape — `StartToCloseTimeout`, `HeartbeatTimeout`, `RetryPolicy` — but values shrunk to demo-friendly defaults (2 min / 10 s / above retry tuning).
- **Worker registration pattern.** `worker.New(c, taskQueue, worker.Options{})`, `RegisterWorkflow`/`RegisterActivity`, `w.Run(worker.InterruptCh())`. Kept; task queue name centralized in a constant per worker binary.
- **Starter shape.** `client.Dial` → `ExecuteWorkflow` → `run.Get(ctx, nil)`. Kept; replaced `client.Dial(client.Options{})` with our `tclient.New()` helper that handles dev/cloud targets and API key auth.

## Replace entirely

- **Workflow shape.** Upstream is one linear activity. Ours is a DAG executed level-by-level via `internal/planexec.Execute`, with 4–12 steps depending on `MediaType`.
- **Type system.** Upstream defines `FFmpegProcessingParams`/`FFmpegProcessingResult` inline in `activity.go`. Ours centralizes types in `internal/types/types.go` and is shared across patterns.
- **Single activity → dispatch-on-Kind activity.** Upstream registers exactly one activity. We also register exactly one (`ProduceDerivative`), but it dispatches to internal subroutines based on `DerivativeKind`. Sub-routines (`extractMetadata`, `transcodeFFmpeg`, `transcodeCustom`, `publishMedia`) are package-private — never registered separately.
- **No search attributes upstream.** We add `MediaID`, `CameraModel`, `MediaType` and call `workflow.UpsertTypedSearchAttributes` as the first workflow action.
- **No tests upstream.** We add `executor_test.go`, `activities_test.go`, `workflow_test.go`, `dsl_test.go`, and `replay_test.go` (skipping on missing histories).
- **No DSL pattern upstream.** Pattern D (YAML-driven plan) is entirely new.

## Discard

- **Output path as `--output` flag.** Upstream takes one output path. Ours produces N derivatives into `tmp/output/<MediaID>/`. Single `--output` flag is irrelevant.
- **`--ffmpeg-args` flag.** Upstream lets the caller pass arbitrary ffmpeg args. Ours has kind-specific args baked into `transcodeFFmpeg` based on the derivative being produced.
- **Workflow ID with timestamp.** Upstream uses `video-processing-YYYYMMDD-HHMMSS`. Ours uses `video-pipeline-{a,b}-<MediaID>` so the workflow is searchable by the same MediaID an operations team would query.
- **`activity.RecordHeartbeat` from inside the stderr scanner loop.** Upstream heartbeats from both the ticker and the per-line scanner. The per-line version is redundant given the 5-second ticker and the 10-second heartbeat timeout. We keep only the ticker version for clarity.

## SDK version decision

Upgrade to **latest stable `go.temporal.io/sdk`** (CLAUDE.md default). At inspection time upstream is on `v1.33.0`; we run `go get go.temporal.io/sdk@latest` and let go.mod resolve. The API surface used (`workflow.ExecuteActivity`, `workflow.Future`, `temporal.RetryPolicy`, `worker.New`, `worker.NewWorkflowReplayer`) has been stable for several versions; no migration risk expected.

## Cloud-specific deviations

- Upstream's `client.Dial(client.Options{})` assumes insecure localhost. Our `internal/tclient/tclient.go` reads `TEMPORAL_TARGET`:
  - `dev` (default) → no auth, address from `TEMPORAL_ADDRESS` or `localhost:7233`.
  - `cloud` → `client.Options{HostPort, Namespace, Credentials: client.NewAPIKeyStaticCredentials(...), ConnectionOptions: ConnectionOptions{TLS: &tls.Config{}}}`.
- mTLS path is **not built** (per CLAUDE.md §Tooling). Documented in NOTES.md.

## License and attribution

Upstream is MIT licensed. We are not copying files verbatim, but we lift specific regex patterns and helper functions. Attribution lives in `NOTICES.md` at repo root, with upstream commit SHA and the full upstream LICENSE text.

## Open questions

None. Spec is internally consistent.
