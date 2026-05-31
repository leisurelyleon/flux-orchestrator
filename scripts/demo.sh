#!/usr/bin/env bash
# Run the end-to-end demo on the in-memory bus. No broker required.
set -euo pipefail

cargo run -p flux-cli -- demo
