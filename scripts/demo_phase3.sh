#!/bin/sh
# Phase 3 demo: the same stream through the two-thread pipeline under each queue-full policy.
# Usage: scripts/demo_phase3.sh [build_dir]
set -eu
BUILD="${1:-build}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
B="$ROOT/$BUILD"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
pick() { tr ',{}' '\n\n\n' | grep -E '^ *"(rx_frames|capacity|high_water|queue_overflow|stalled_events|processed|skipped_tickets|missing_total|balanced)"' | tr -s ' ' | tr '\n' ' '; echo; }

"$B/feed_gen_events" --profile normal --seed 7 --count 20000 --out "$TMP/clean.fdr" >/dev/null
echo "== block, slow consumer (300 ns/event), capacity 8: backpressure, nothing lost =="
"$B/feed_pipeline" --input "$TMP/clean.fdr" --queue-capacity 8 --consumer-work-ns 300 | pick
echo; echo "== drop, slower consumer (3 us/event), capacity 16: loss is counted, and is NOT network loss =="
"$B/feed_pipeline" --input "$TMP/clean.fdr" --queue-capacity 16 --full-policy drop --consumer-work-ns 3000 | pick
echo; echo "== fail-fast, capacity 4: stops at the first overflow (exit status 3) =="
"$B/feed_pipeline" --input "$TMP/clean.fdr" --queue-capacity 4 --full-policy fail --consumer-work-ns 5000 | pick || echo "(exit status $?)"
