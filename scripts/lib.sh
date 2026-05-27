#!/usr/bin/env bash
# scripts/lib.sh — sourced by all rehearsal scripts.

# say: colored narration line for the audience
say() {
    if [ -t 1 ] && command -v tput >/dev/null 2>&1; then
        printf '\033[34m%s\033[0m\n' "$*"
    else
        printf '%s\n' "$*"
    fi
}

# die: print error to stderr and exit non-zero
die() {
    printf '\033[31mERROR: %s\033[0m\n' "$*" >&2
    exit 1
}

# new_media_id <prefix>: cross-platform short unique ID (uuidgen works on macOS and Linux)
new_media_id() {
    local prefix="$1"
    local short
    short=$(uuidgen | tr 'A-Z' 'a-z' | tr -d '-' | cut -c1-8)
    printf '%s_%s' "$prefix" "$short"
}

# ui_url: workflow UI URL for the current target
ui_url() {
    local target="${TEMPORAL_TARGET:-dev}"
    if [ "$target" = "cloud" ]; then
        printf 'https://cloud.temporal.io/namespaces/%s/workflows' "${TEMPORAL_NAMESPACE:?TEMPORAL_NAMESPACE required for cloud}"
    else
        printf 'http://localhost:8233/namespaces/default/workflows'
    fi
}

# wait_for_worker <pid>: poll worker log for SDK start marker (max 15s)
wait_for_worker() {
    local pid="$1"
    local i=0
    while [ $i -lt 30 ]; do
        if ! kill -0 "$pid" 2>/dev/null; then
            return 1
        fi
        if grep -q "worker started" tmp/worker-*.log 2>/dev/null; then
            return 0
        fi
        sleep 0.5
        i=$((i + 1))
    done
    return 1
}

# wait_for_indexing: pause so search attributes are indexed before UI lookup
# Dev: nearly instant. Cloud: can be 1-3 seconds.
wait_for_indexing() {
    if [ "${TEMPORAL_TARGET:-dev}" = "cloud" ]; then
        sleep 3
    else
        sleep 1
    fi
}

# pause_step <message>: live-walkthrough pause point.
# Activated by DEMO_STEP=1 (set by ./demo-{a,b}.sh --step). When unset, no-op.
# Prints a visible yellow prompt and waits for Enter from /dev/tty so it works
# even if the script's stdin is redirected. Skips silently if no controlling tty.
pause_step() {
    if [ "${DEMO_STEP:-0}" != "1" ]; then
        return 0
    fi
    # Need a real terminal for stdout (so the prompt is visible) AND for input
    # (so read can actually wait). If either is missing, skip the pause.
    if [ ! -t 1 ] || [ ! -r /dev/tty ]; then
        return 0
    fi
    echo
    printf '\033[33m═══════════════════════════════════════════════════════════════\033[0m\n'
    printf '\033[33m▶ %s\033[0m\n' "$*"
    printf '\033[33m  Press Enter to continue (Ctrl-C to abort)\033[0m\n'
    printf '\033[33m═══════════════════════════════════════════════════════════════\033[0m\n'
    { read -r _ </dev/tty; } 2>/dev/null || true
}
