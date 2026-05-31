#!/usr/bin/env bash
# OPTIONAL: start a single-node Kafka broker for the live Kafka backend.
# Requires Docker. Not needed for build, test, or the in-memory demo.
set -euo pipefail

docker compose up -d
echo "Kafka starting on localhost:9092. Use the 'kafka' feature to connect."
