#!/usr/bin/env bash
# Idempotent registration of three custom search attributes.
# Dev:   temporal operator search-attribute create  (silently no-ops if present)
# Cloud: tcld namespace search-attributes add       (human must have tcld installed)
set -euo pipefail

TARGET="${TEMPORAL_TARGET:-dev}"

create_dev() {
    local name="$1"
    local type="$2"
    if temporal operator search-attribute create --name "$name" --type "$type" >/dev/null 2>&1; then
        echo "  registered $name ($type)"
    else
        # Most common reason: already exists. Treat as success.
        echo "  $name already present (or registration no-op)"
    fi
}

create_cloud() {
    local name="$1"
    local type="$2"
    if ! command -v tcld >/dev/null 2>&1; then
        echo "ERROR: tcld not on PATH. Install per https://docs.temporal.io/cloud/tcld" >&2
        return 1
    fi
    : "${TEMPORAL_NAMESPACE:?TEMPORAL_NAMESPACE required for cloud}"
    tcld namespace search-attributes add \
        --namespace "$TEMPORAL_NAMESPACE" \
        --search-attribute "$name=$type" || true
    echo "  ensured $name ($type) on cloud namespace $TEMPORAL_NAMESPACE"
}

echo "Registering search attributes (target=$TARGET):"
if [ "$TARGET" = "cloud" ]; then
    create_cloud MediaID Keyword
    create_cloud CameraModel Keyword
    create_cloud MediaType Keyword
else
    create_dev MediaID Keyword
    create_dev CameraModel Keyword
    create_dev MediaType Keyword
fi
echo "Done."
