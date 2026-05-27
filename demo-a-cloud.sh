#!/usr/bin/env bash
# Demo A — Temporal Cloud launcher. Requires a populated .env.cloud.
#
# Flags:
#   --step | -s   Pause between demo moments and wait for Enter.
set -euo pipefail

[ -f .env.cloud ] || { echo "ERROR: .env.cloud not found. Copy .env.cloud.example and fill in your values."; exit 1; }
set -a; . ./.env.cloud; set +a

: "${TEMPORAL_ADDRESS:?required in .env.cloud}"
: "${TEMPORAL_NAMESPACE:?required in .env.cloud}"
: "${TEMPORAL_API_KEY:?required in .env.cloud}"
export TEMPORAL_TARGET=cloud

case "${1:-}" in
    --step|-s) export DEMO_STEP=1 ;;
    "") ;;
    *) echo "unknown flag: $1 (use --step for narrated walkthrough)" >&2; exit 2 ;;
esac

case "$TEMPORAL_NAMESPACE" in
    *prod*|*production*|*prd*|*live*|*staging*)
        echo "ERROR: TEMPORAL_NAMESPACE='$TEMPORAL_NAMESPACE' looks production-ish. Aborting." >&2
        exit 1 ;;
esac

echo "=== Demo A on Temporal Cloud ==="
echo "Namespace: $TEMPORAL_NAMESPACE"
echo "UI: https://cloud.temporal.io/namespaces/$TEMPORAL_NAMESPACE/workflows"
echo

./scripts/register-search-attrs.sh
exec ./scripts/demo-a-walkthrough.sh
