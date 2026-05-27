#!/usr/bin/env bash
# Captures workflow histories used by replay tests. Assumes:
#   - dev server is up
#   - search attributes registered
#   - bin/* binaries built
# Output: testdata/histories/*.json (committed).
set -euo pipefail
source scripts/lib.sh
mkdir -p tmp testdata/histories

./scripts/start-dev-server.sh
./scripts/register-search-attrs.sh
./scripts/ensure-sample-video.sh
[ -x bin/worker-a ] || make build

cleanup() {
    [ -f tmp/worker-a.pid ] && kill "$(cat tmp/worker-a.pid)" 2>/dev/null || true
    [ -f tmp/worker-b.pid ] && kill "$(cat tmp/worker-b.pid)" 2>/dev/null || true
    rm -f tmp/worker-a.pid tmp/worker-b.pid
}
trap cleanup EXIT

./bin/worker-a >tmp/worker-a.log 2>&1 &
echo $! > tmp/worker-a.pid
wait_for_worker "$(cat tmp/worker-a.pid)" || die "worker-a failed to start"

# Pattern V — 4 step
ID4=$(new_media_id capture4)
./bin/starter-a --media-id "$ID4" --media-type short_clip --camera mobile --input samples/sample.mp4
temporal workflow show --workflow-id "video-pipeline-a-$ID4" --output json > testdata/histories/pattern_v_4step.json
echo "captured pattern_v_4step.json ($(wc -c < testdata/histories/pattern_v_4step.json) bytes)"

# Pattern V — 12 step
ID12=$(new_media_id capture12)
./bin/starter-a --media-id "$ID12" --media-type spherical --camera spherical --input samples/sample.mp4
temporal workflow show --workflow-id "video-pipeline-a-$ID12" --output json > testdata/histories/pattern_v_12step.json
echo "captured pattern_v_12step.json ($(wc -c < testdata/histories/pattern_v_12step.json) bytes)"

kill "$(cat tmp/worker-a.pid)" 2>/dev/null || true
rm -f tmp/worker-a.pid

# Pattern D — 12 step
./bin/worker-b >tmp/worker-b.log 2>&1 &
echo $! > tmp/worker-b.pid
wait_for_worker "$(cat tmp/worker-b.pid)" || die "worker-b failed to start"

IDD=$(new_media_id captured)
./bin/starter-b --media-id "$IDD" --camera spherical --plan-file dsl/spherical_chaptered.yaml --input samples/sample.mp4
temporal workflow show --workflow-id "video-pipeline-b-$IDD" --output json > testdata/histories/pattern_d_12step.json
echo "captured pattern_d_12step.json ($(wc -c < testdata/histories/pattern_d_12step.json) bytes)"

echo "Done."
