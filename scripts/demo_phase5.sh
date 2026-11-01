#!/bin/sh
# Phase 5 demo: policy trade-offs, state corruption vs recovery, and overload repaired downstream.
# Usage: scripts/demo_phase5.sh [build_dir]
set -eu
BUILD="${1:-build}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; B="$ROOT/$BUILD"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
num() { sed -n "s/.*\"$1\": *\([0-9][0-9]*\).*/\1/p" "$2" | head -n 1; }
str() { sed -n "s/.*\"$1\": *\"\([a-z_]*\)\".*/\1/p" "$2" | head -n 1; }
flag() { sed -n "s/.*\"$1\": *\(true\|false\).*/\1/p" "$2" | head -n 1; }

echo "=== 1. Queue-full policies under bursty overload (deterministic model) ==="
"$B/feed_overload" --format table --capacities 16,1024,2048 --policies drop,block
echo
echo "Read it like this: with 2048 slots nothing is lost but events wait ~7 ms behind a burst; with 16 slots waits stay at 64 us"
echo "but 87% is lost (drop) or the SOURCE is held back ~7 ms (block). Lossless delay is the same wherever it is paid."

echo; echo "=== 2. Downstream state: what each recovery strategy does with the same faulty feed ==="
"$ROOT/scripts/compare_recovery.sh" "$BUILD" | sed -n '3,$p'

echo; echo "=== 3. Overload in the queue, repaired downstream ==="
"$B/feed_gen_events" --profile normal --seed 17 --count 1500 --out "$TMP/clean.fdr" >/dev/null
for s in naive hybrid; do
  "$B/feed_pipeline" --input "$TMP/clean.fdr" --mode rate --rate 20000 --queue-capacity 8 --full-policy drop --consumer-work-ns 150000 \
      --late-policy forward --recovery "$s" --authoritative "$TMP/clean.fdr" --retention 100000 --snapshot-interval 250 > "$TMP/$s.json"
  printf '%-7s queue_overflow=%-5s network_gaps=%-2s engine_gaps=%-3s state=%-8s exact=%-5s unknown_cancels=%s\n' "$s" \
      "$(num queue_overflow "$TMP/$s.json")" "$(num missing_total "$TMP/$s.json")" "$(num gaps_opened "$TMP/$s.json")" \
      "$(str sync_state "$TMP/$s.json")" "$(flag matches_reference "$TMP/$s.json")" "$(num unknown_cancels "$TMP/$s.json")"
done
echo "(a slow consumer drops about two thirds of the events in the queue; the network lost nothing. naive diverges; hybrid ends exact.)"
