#!/usr/bin/env bash
# Generate a synthetic 10-second test clip ONLY if samples/sample.mp4 is missing.
# Demo-day flow: the human places their own clip; this script must never overwrite.
set -euo pipefail

if [ -f samples/sample.mp4 ]; then
    echo "samples/sample.mp4 already exists; not overwriting."
    exit 0
fi

if ! command -v ffmpeg >/dev/null 2>&1; then
    echo "ERROR: ffmpeg not on PATH; cannot generate sample." >&2
    exit 1
fi

mkdir -p samples
ffmpeg -f lavfi -i testsrc=duration=10:size=1920x1080:rate=30 \
       -f lavfi -i sine=frequency=440 \
       -shortest -y samples/sample.mp4 >/dev/null 2>&1

echo "Generated synthetic samples/sample.mp4 (10s, 1920x1080)."
