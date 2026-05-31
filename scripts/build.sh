#!/usr/bin/env bash
# Build the workspace in release mode (in-memory bus; no Kafka).
set -euo pipefail

cargo build --release
echo "Build complete."
