#!/usr/bin/env bash
# Idempotent prerequisite installer. Called by `make install-prereqs` and `make setup`.
set -euo pipefail

OS=$(uname -s)

if ! command -v go >/dev/null 2>&1; then
    echo "ERROR: Go is not installed. Install Go 1.22+ from https://go.dev/dl/ and re-run." >&2
    exit 1
fi
GO_VERSION=$(go version | awk '{print $3}' | sed 's/go//')
echo "Found Go $GO_VERSION"

if [ "$OS" = "Darwin" ]; then
    if ! command -v brew >/dev/null 2>&1; then
        echo "ERROR: Homebrew is not installed. Install from https://brew.sh, then re-run." >&2
        exit 1
    fi

    for pkg in ffmpeg temporal; do
        if brew list --formula "$pkg" >/dev/null 2>&1; then
            echo "$pkg already installed."
        else
            echo "Installing $pkg via brew..."
            brew install "$pkg"
        fi
    done

elif [ "$OS" = "Linux" ]; then
    MISSING=""
    command -v ffmpeg >/dev/null 2>&1 || MISSING="$MISSING ffmpeg"
    command -v ffprobe >/dev/null 2>&1 || MISSING="$MISSING ffprobe"
    command -v temporal >/dev/null 2>&1 || MISSING="$MISSING temporal"
    if [ -n "$MISSING" ]; then
        echo "ERROR: Missing prerequisites on Linux:$MISSING" >&2
        echo "Install via: sudo apt-get install ffmpeg  (and download temporal CLI from https://docs.temporal.io/cli)" >&2
        exit 1
    fi
else
    echo "ERROR: Unsupported OS: $OS. This build targets macOS (primary) or Linux (best-effort)." >&2
    exit 1
fi

for bin in ffmpeg ffprobe temporal go git make uuidgen; do
    if ! command -v "$bin" >/dev/null 2>&1; then
        echo "ERROR: $bin not found on PATH after install." >&2
        exit 1
    fi
done

echo "All prerequisites installed and verified."
