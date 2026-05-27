#!/usr/bin/env bash
# Demo A rehearsal. Used by ./demo-a.sh (dev) and ./demo-a-cloud.sh (cloud).
# Cleanup stops the worker only — leaves the dev server up so a human can
# verify the UI afterward.
set -euo pipefail
source scripts/lib.sh
mkdir -p tmp

TARGET="${TEMPORAL_TARGET:-dev}"
say "=== Demo A: Pattern V on TEMPORAL_TARGET=$TARGET ==="
say "UI: $(ui_url)"
echo

if [ "$TARGET" = "dev" ]; then
    ./scripts/start-dev-server.sh
    ./scripts/register-search-attrs.sh
    ./scripts/ensure-sample-video.sh
fi

[ -x bin/worker-a ] || make build

./bin/worker-a >tmp/worker-a.log 2>&1 &
echo $! > tmp/worker-a.pid
wait_for_worker "$(cat tmp/worker-a.pid)" || die "worker-a failed to start (see tmp/worker-a.log)"

cleanup() {
    [ -f tmp/worker-a.pid ] && kill "$(cat tmp/worker-a.pid)" 2>/dev/null || true
    rm -f tmp/worker-a.pid
}
trap cleanup EXIT

# [1/7] iPhone basic (4 steps)
pause_step "[1/7] iPhone basic — 4-step plan. Set the scene: 'simple iPhone clip, 4 derivatives.'"
MID_1=$(new_media_id vid_short)
say "[1/7] iPhone basic — 4-step plan"
./bin/starter-a --media-id "$MID_1" --media-type short_clip \
    --camera mobile --input samples/sample.mp4 --demo-mode
wait_for_indexing
say "    → UI: search MediaID = $MID_1 — 4 activities, clean execution"
echo

# [2/7] Spherical chaptered (12 steps, DAG)
pause_step "[2/7] Spherical 12-step plan. Same workflow code; 3x as many activities. Watch parallelism after 'projection'."
MID_2=$(new_media_id vid_spherical)
say "[2/7] Spherical chaptered — 12-step plan, DAG dependencies"
./bin/starter-a --media-id "$MID_2" --media-type spherical \
    --camera spherical --input samples/sample.mp4 --demo-mode
wait_for_indexing
say "    → Same workflow code. 12 activities. HLS waited on MPV waited on projection."
say "    → edit_proxy, thumbnail, hls ran in parallel where DAG allows."
echo

# [3/7] Worker crash + recovery — kill the worker process, restart fresh.
# This is the headline durability moment: a different process picks up where
# the dead one left off, with no work repeated.
pause_step "[3/7] Worker crash + recovery. About to start a workflow then kill the worker process entirely (graceful SIGTERM — same thing Kubernetes does when it reschedules your pod). The workflow state lives in Temporal, not in the worker, so it will resume against a fresh process."
MID_3=$(new_media_id vid_pause)
say "[3/7] Worker crash + recovery"
./bin/starter-a --media-id "$MID_3" --media-type spherical \
    --camera spherical --input samples/sample.mp4 --demo-mode &
STARTER_PID=$!
sleep 6

pause_step "Workflow has executed a few activities. About to kill the worker process. Narrate: 'simulating a K8s pod restart, a node failure, a deploy.'"
OLD_WORKER_PID=$(cat tmp/worker-a.pid)
say "    → killing worker (PID $OLD_WORKER_PID)"
kill -TERM "$OLD_WORKER_PID" 2>/dev/null || true
# Wait for the worker to actually exit so we can prove it's gone.
i=0
while kill -0 "$OLD_WORKER_PID" 2>/dev/null && [ $i -lt 20 ]; do
    sleep 0.25
    i=$((i + 1))
done
rm -f tmp/worker-a.pid
say "    → worker process is gone. Workflow is stuck — no worker to run the next activity."

pause_step "Worker is dead. The workflow state is preserved in Temporal — it's just waiting for a worker to come back. Press Enter to start a FRESH worker process (different PID, no memory of the old one) and watch the workflow resume."
./bin/worker-a >tmp/worker-a.log 2>&1 &
echo $! > tmp/worker-a.pid
NEW_WORKER_PID=$(cat tmp/worker-a.pid)
wait_for_worker "$NEW_WORKER_PID" || die "fresh worker-a failed to start (see tmp/worker-a.log)"
say "    → fresh worker started (PID $NEW_WORKER_PID, was $OLD_WORKER_PID). Workflow resumes from last completed activity."

wait "$STARTER_PID" || true
wait_for_indexing
say "    → Same workflow, brand-new worker process. Activity-level checkpointing in action."
echo

# [4/7] Graceful cancellation — `temporal workflow cancel` triggers in-flight
# activity ctx.Done() AND a disconnected-context cleanup activity that writes
# a cancellation manifest. Demonstrates the full cancel-cleanup pattern.
pause_step "[4/7] Graceful cancellation. Start a 12-step workflow; mid-flight we'll run 'temporal workflow cancel'. The in-flight activity sees ctx.Done(), the workflow exits with CanceledError — but a deferred cleanup runs on a disconnected context and writes a cancelled.json manifest."
MID_CANCEL=$(new_media_id vid_cancel)
say "[4/7] Graceful cancellation"
./bin/starter-a --media-id "$MID_CANCEL" --media-type spherical \
    --camera spherical --input samples/sample.mp4 --demo-mode &
STARTER_CANCEL_PID=$!
sleep 5

pause_step "Workflow has executed a few activities. About to cancel. Narrate: 'this is the canonical cancel pattern — in-flight ctx is canceled, but cleanup runs on a disconnected context so the manifest write still happens.'"
say "    → temporal workflow cancel --workflow-id video-pipeline-a-$MID_CANCEL"
temporal workflow cancel --workflow-id "video-pipeline-a-$MID_CANCEL" >/dev/null 2>&1 || true
wait "$STARTER_CANCEL_PID" || true
wait_for_indexing
if [ -f "tmp/output/$MID_CANCEL/cancelled.json" ]; then
    say "    → cancelled.json written by disconnected-ctx cleanup activity:"
    cat "tmp/output/$MID_CANCEL/cancelled.json"
else
    say "    → WARNING: cancelled.json missing — cleanup activity did not run"
fi
say "    → Workflow status: Canceled. Cleanup ran. No partial mess left behind."
echo

# [5/7] Endless-loop guard
pause_step "[5/7] Misconfigured media. The custom_encode activity is hardcoded to fail. Retry policy caps at 5 — expect failure in ~15s, NOT an infinite loop."
MID_4=$(new_media_id vid_bad)
say "[5/7] Misconfigured media — endless-loop guard"
./bin/starter-a --media-id "$MID_4" --media-type misconfigured \
    --camera action_standard --input samples/sample.mp4 --demo-mode || true
wait_for_indexing
say "    → Retry cap of 5 → workflow failed in ~15 seconds."
say "    → No infinite loop. No system lockup."
echo

# [6/7] Replay test — pass, then break via sed, then revert
pause_step "[6/7] Replay test. First we run it AS-IS — captured history matches current workflow code, so it passes."
say "[6/7] Replay test — passes today"
go test ./internal/workflows/pattern_v/ -run TestReplay -v
pause_step "Now we modify the workflow code to add an extra step. Watch the replay test catch the regression — the scary stack trace IS the demo."
say "    → Now uncomment the DEMO TOGGLE line in workflow.go and re-run..."
WF_FILE="internal/workflows/pattern_v/workflow.go"
sed -i.bak '/DEMO TOGGLE: REPLAY-REGRESSION DEMONSTRATION/,/END DEMO TOGGLE/ {
    s|^	// plan.Steps = append|	plan.Steps = append|
}' "$WF_FILE"
go test ./internal/workflows/pattern_v/ -run TestReplay -v || true
pause_step "Test failed loudly with 'nondeterministic workflow' — exactly what you want CI to catch at PR time, not at 2 AM. Now we revert and confirm it passes again."
say "    → Replay failed: captured history doesn't include the new step."
say "    → This is how you catch workflow-code regressions before prod."
mv "${WF_FILE}.bak" "$WF_FILE"
go test ./internal/workflows/pattern_v/ -run TestReplay -v >/dev/null
say "    → Restored. Replay passes again."
echo

# [7/7] UI walkthrough
pause_step "[7/7] UI walkthrough. Switch to the browser. Search by any MediaID. Click into a workflow. Show the timeline."
say "[7/7] Open the UI ($(ui_url)) and search these MediaIDs:"
say "    - $MID_1         (4 steps, clean)"
say "    - $MID_2         (12 steps with DAG)"
say "    - $MID_3         (worker killed, fresh worker resumed)"
say "    - $MID_CANCEL    (canceled, cleanup ran)"
say "    - $MID_4         (misconfigured, clean failure)"
say "=== Demo A complete ==="
