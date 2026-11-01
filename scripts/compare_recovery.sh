#!/bin/sh
# Compare every recovery strategy on the same faulty feed (file replay, so the counts are deterministic).
# Usage: scripts/compare_recovery.sh [build_dir]
set -eu
BUILD="${1:-build}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; B="$ROOT/$BUILD"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
num() { sed -n "s/.*\"$1\": *\([0-9][0-9]*\).*/\1/p" "$2" | head -n 1; }
str() { sed -n "s/.*\"$1\": *\"\([a-z_]*\)\".*/\1/p" "$2" | head -n 1; }
flag() { sed -n "s/.*\"$1\": *\(true\|false\).*/\1/p" "$2" | head -n 1; }

"$B/feed_gen_events" --profile normal --seed 5 --count 6000 --out "$TMP/clean.fdr" >/dev/null
"$B/feed_fault_inject" --in "$TMP/clean.fdr" --out "$TMP/faulty.fdr" --seed 5 --drop-ppm 8000 --dup-ppm 8000 --reorder-ppm 8000 | tr -s ' ' | tr -d '\n'; echo
echo
echo "6000 messages; log window 3 messages and a 30-event round trip (a slow, short-memory recovery service); snapshots every 100."
printf '%-11s %-12s %-9s %-11s %-9s %-6s %-8s %-9s %-9s\n' strategy state exact unk_cancel next_apply gaps retx snapshot max_wait
for s in naive none retransmit snapshot hybrid; do
  "$B/feed_pipeline" --input "$TMP/faulty.fdr" --late-policy forward --queue-capacity 256 --recovery "$s" --authoritative "$TMP/clean.fdr" \
      --retention 3 --recovery-latency 30 --snapshot-interval 100 > "$TMP/$s.json"
  printf '%-11s %-12s %-9s %-11s %-9s %-6s %-8s %-9s %-9s\n' "$s" "$(str sync_state "$TMP/$s.json")" "$(flag matches_reference "$TMP/$s.json")" \
      "$(num unknown_cancels "$TMP/$s.json")" "$(num next_apply "$TMP/$s.json")" "$(num gaps_opened "$TMP/$s.json")" \
      "$(num recoveries_retransmit "$TMP/$s.json")" "$(num recoveries_snapshot "$TMP/$s.json")" "$(num episode_events_max "$TMP/$s.json")"
done
echo
echo "exact = the book equals the true state at next_apply-1.  max_wait = longest gap episode, in events, until the book was current again."
