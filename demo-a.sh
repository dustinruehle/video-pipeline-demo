#!/usr/bin/env bash
# Demo A — dev launcher. Drives scripts/demo-a-walkthrough.sh against the
# local Temporal dev server.
#
# Flags:
#   --step | -s   Pause between demo moments and wait for Enter.
#                 Use this for live, narrated walkthroughs.
set -euo pipefail
export TEMPORAL_TARGET=dev

case "${1:-}" in
    --step|-s) export DEMO_STEP=1 ;;
    "") ;;
    *) echo "unknown flag: $1 (use --step for narrated walkthrough)" >&2; exit 2 ;;
esac

exec ./scripts/demo-a-walkthrough.sh
