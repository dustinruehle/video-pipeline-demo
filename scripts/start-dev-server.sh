#!/usr/bin/env bash
# Idempotent start of the Temporal dev server. Safe to call repeatedly.
set -euo pipefail

mkdir -p tmp

# Already up?
if temporal operator namespace describe default >/dev/null 2>&1; then
    echo "Dev server already up."
    exit 0
fi

# Detect --db-filename support; if absent, fall back to in-memory.
DB_FLAGS=""
if temporal server start-dev --help 2>&1 | grep -q -- '--db-filename'; then
    DB_FLAGS="--db-filename ./tmp/temporal.db"
fi

echo "Starting Temporal dev server (logs: tmp/dev-server.log)..."
# shellcheck disable=SC2086
nohup temporal server start-dev $DB_FLAGS --log-level error >tmp/dev-server.log 2>&1 &
echo $! > tmp/dev-server.pid

# Health check via gRPC frontend (real check, not nc).
i=0
while [ $i -lt 60 ]; do
    if temporal operator namespace describe default >/dev/null 2>&1; then
        echo "Dev server is up (pid $(cat tmp/dev-server.pid))."
        exit 0
    fi
    sleep 0.5
    i=$((i + 1))
done

echo "ERROR: dev server failed to come up within 30 seconds. See tmp/dev-server.log" >&2
exit 1
