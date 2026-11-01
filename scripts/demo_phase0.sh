#!/bin/sh
# Phase 0 demo: generate a stream, replay it, show the summary.
# Usage: scripts/demo_phase0.sh [build_dir]
set -eu
BUILD="${1:-build}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT

echo "== generate: gap profile (1% fault rate, seed 7) =="
"$ROOT/$BUILD/feed_gen_events" --profile gap --seed 7 --count 5000 --out "$TMP/gap.fdr"
echo "== replay at maximum speed =="
"$ROOT/$BUILD/feed_replay" --input "$TMP/gap.fdr"
echo "== replay timed at 4x (5000 msgs @ 100k/s => ~12 ms) =="
"$ROOT/$BUILD/feed_replay" --input "$TMP/gap.fdr" --mode timed --speed 4
