#!/bin/sh
# End-to-end check of the two-thread pipeline CLI and its exit codes.
# Usage: smoke_pipeline.sh <feed_gen_events> <feed_fault_inject> <feed_pipeline>
set -eu
GEN="$1"; INJ="$2"; PIPE="$3"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
num() { sed -n "s/.*\"$1\": *\([0-9][0-9]*\).*/\1/p" "$2" | head -n 1; }

"$GEN" --profile normal --seed 8 --count 20000 --out "$TMP/clean.fdr" >/dev/null
"$INJ" --in "$TMP/clean.fdr" --out "$TMP/faulty.fdr" --seed 8 --drop-ppm 5000 --dup-ppm 5000 --reorder-ppm 5000 >/dev/null

# block: backpressure, nothing lost, both digests identical on repeat runs
"$PIPE" --input "$TMP/faulty.fdr" --queue-capacity 8 --consumer-work-ns 300 > "$TMP/block1.json"
"$PIPE" --input "$TMP/faulty.fdr" --queue-capacity 8 --consumer-work-ns 300 > "$TMP/block2.json"
grep -q '"balanced": true' "$TMP/block1.json"
[ "$(num queue_overflow "$TMP/block1.json")" = "0" ]
[ "$(grep '"consumer"' "$TMP/block1.json")" = "$(grep '"consumer"' "$TMP/block2.json")" ]
[ "$(num processed "$TMP/block1.json")" = "$(num forwarded_to_queue "$TMP/block1.json")" ]

# drop: a slow consumer must lose events, and every one must be accounted for as queue_overflow
"$PIPE" --input "$TMP/clean.fdr" --queue-capacity 8 --full-policy drop --consumer-work-ns 3000 > "$TMP/drop.json"
grep -q '"balanced": true' "$TMP/drop.json"
dropped="$(num queue_overflow "$TMP/drop.json")"
[ "$dropped" -gt 0 ]
[ "$(( $(num processed "$TMP/drop.json") + dropped ))" = "20000" ]
[ "$(num missing_total "$TMP/drop.json")" = "0" ]     # queue overflow is not network loss

# fail: stops at the first overflow, exit status 3, and the report is still consistent
rc=0; "$PIPE" --input "$TMP/clean.fdr" --queue-capacity 4 --full-policy fail --consumer-work-ns 5000 > "$TMP/fail.json" || rc=$?
[ "$rc" -eq 3 ]
grep -q '"balanced": true' "$TMP/fail.json"
grep -q '"replay_stopped_early": true' "$TMP/fail.json"

# usage / input errors
rc=0; "$PIPE" --input "$TMP/clean.fdr" --full-policy nope >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 2 ]
rc=0; "$PIPE" --input "$TMP/clean.fdr" --queue-capacity 0 >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 2 ]
rc=0; "$PIPE" --input "$TMP/none.fdr" >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 1 ]
echo "pipeline ok"
