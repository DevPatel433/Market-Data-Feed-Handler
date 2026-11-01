#!/bin/sh
# Phase 2 demo: inject faults into a clean stream and compare the two gap policies.
# Usage: scripts/demo_phase2.sh [build_dir]
set -eu
BUILD="${1:-build}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
B="$ROOT/$BUILD"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
pick() { grep -E "\"(frames|gap_events|missing_total|duplicates|out_of_order|outstanding_missing|forwarded|paused_discarded|late_discarded|balanced|state)\"" | tr -s ' ' | tr -d '\n'; echo; }

"$B/feed_gen_events" --profile normal --seed 7 --count 20000 --out "$TMP/clean.fdr" >/dev/null
echo "== inject: 0.5% drop, 0.5% duplicate, 0.5% reorder =="
"$B/feed_fault_inject" --in "$TMP/clean.fdr" --out "$TMP/faulty.fdr" --seed 7 \
    --drop-ppm 5000 --dup-ppm 5000 --reorder-ppm 5000 | tr -s ' ' | tr -d '\n'; echo
echo; echo "== replay, gap-policy degrade (default) =="
"$B/feed_replay" --input "$TMP/faulty.fdr" --event-log "$TMP/events.jsonl" | pick
echo "first events:"; head -4 "$TMP/events.jsonl"
echo; echo "== replay, gap-policy pause =="
"$B/feed_replay" --input "$TMP/faulty.fdr" --gap-policy pause | pick
echo; echo "== replay, late-policy forward (late arrivals delivered) =="
"$B/feed_replay" --input "$TMP/faulty.fdr" --late-policy forward | pick
