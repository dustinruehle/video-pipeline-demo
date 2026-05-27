#!/usr/bin/env bash
# Stop the Temporal dev server. Only called by `make teardown`.
set -euo pipefail

if [ -f tmp/dev-server.pid ]; then
    pid=$(cat tmp/dev-server.pid)
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid"
        echo "Stopped dev server (pid $pid)."
    fi
    rm -f tmp/dev-server.pid
fi
