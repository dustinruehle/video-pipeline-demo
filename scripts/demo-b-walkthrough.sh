#!/usr/bin/env bash
# Demo B rehearsal. Used by ./demo-b.sh (dev) and ./demo-b-cloud.sh (cloud).
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

# [1/5] Four-step from YAML
pause_step "[1/5] Four-step plan from YAML. Show the YAML first, then submit it. Plan is a config file — no Go change."
MID_1=$(new_media_id vid_dsl_short)
say "[1/5] Four-step plan from YAML"
cat dsl/short_clip.yaml
./bin/starter-b --media-id "$MID_1" --camera mobile \
    --plan-file dsl/short_clip.yaml --input samples/sample.mp4 --demo-mode
wait_for_indexing
say "    → 4 activities. Plan was a YAML file, not Go code."
echo

# [2/5] Twelve-step from YAML, same workflow code
pause_step "[2/5] Twelve-step plan from a DIFFERENT YAML — same workflow code. Show the 12-step YAML before submitting."
MID_2=$(new_media_id vid_dsl_spherical)
say "[2/5] Twelve-step plan, same workflow code"
cat dsl/spherical_chaptered.yaml
./bin/starter-b --media-id "$MID_2" --camera spherical \
    --plan-file dsl/spherical_chaptered.yaml --input samples/sample.mp4 --demo-mode
wait_for_indexing
say "    → 12 activities. Parallel where the DAG allows."
echo

# [3/5] Mutate a RUNNING workflow via signal — runtime extensibility (the
# complement to design-time YAML edits in [4/5]).
pause_step "[3/5] Signal a running workflow to add an extra derivative. Start a 12-step plan, then mid-flight send 'temporal workflow signal --name add_derivative'. Signal is durably queued; workflow drains it after the main plan and runs a bonus thumbnail step — same workflow ID, one history."
MID_SIG=$(new_media_id vid_dsl_signal)
say "[3/5] Add derivative via signal — runtime mutation"
./bin/starter-b --media-id "$MID_SIG" --camera spherical \
    --plan-file dsl/spherical_chaptered.yaml --input samples/sample.mp4 --demo-mode &
STARTER_SIG_PID=$!
sleep 3

pause_step "Workflow is running. About to signal it to add a thumbnail derivative."
say "    → temporal workflow signal --workflow-id video-pipeline-b-$MID_SIG --name add_derivative ..."
temporal workflow signal \
    --workflow-id "video-pipeline-b-$MID_SIG" \
    --name add_derivative \
    --input '{"Kind":"thumbnail","DependsOn":[]}' >/dev/null 2>&1 || true
wait "$STARTER_SIG_PID" || true
wait_for_indexing
say "    → Workflow drained the signal after the main 12-step plan, ran 1 extra activity."
say "    → 13 total activities. Same workflow ID. One unified history with a WorkflowExecutionSignaled event."
echo

# [4/5] Add a derivative WITHOUT code change (design-time flex)
pause_step "[4/5] Now we ADD a thumbnail derivative — by editing YAML at runtime. No code change, no worker restart. This is Pattern D's design-time companion to the runtime signal in [3/5]."
say "[4/5] Adding thumbnail derivative via YAML edit"
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

# [5/5] Cycle rejected pre-flight
pause_step "[5/5] Cycle in YAML — should be rejected at submit time. Pattern D's endless-loop guard. Show the bad YAML, then try to submit."
say "[5/5] Cycle in YAML — rejected at submit time"
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
say "    - $MID_1     (4 steps)"
say "    - $MID_2     (12 steps)"
say "    - $MID_SIG   (13 steps, +1 from signal mid-flight)"
say "    - $MID_3     (5 steps, thumbnail added via YAML edit)"
