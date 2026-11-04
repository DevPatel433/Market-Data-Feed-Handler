#!/bin/sh
# End-to-end: generate -> inject faults -> replay, and check that the sequencer's
# counters are exactly what the injector's report predicts.
# Usage: smoke_faults.sh <feed_gen_events> <feed_fault_inject> <feed_replay>
set -eu
GEN="$1"; INJ="$2"; REPLAY="$3"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
num() { sed -n "s/.*\"$1\": *\([0-9][0-9]*\).*/\1/p" "$2" | head -n 1; }
str() { sed -n "s/.*\"$1\": *\"\([a-z_]*\)\".*/\1/p" "$2" | head -n 1; }

"$GEN" --profile normal --seed 21 --count 20000 --out "$TMP/clean.fdr" >/dev/null

# --- duplicates only: sequencer duplicates == injected duplicates, stream stays synced
"$INJ" --in "$TMP/clean.fdr" --out "$TMP/dup.fdr" --seed 4 --dup-ppm 20000 --manifest "$TMP/dup.m" >/dev/null
"$REPLAY" --input "$TMP/dup.fdr" > "$TMP/dup.json"
[ "$(num duplicates "$TMP/dup.json")" = "$(num duplicated "$TMP/dup.m")" ]
[ "$(num missing_total "$TMP/dup.json")" = "0" ]
[ "$(str state "$TMP/dup.json")" = "synced" ]
grep -q '"balanced": true' "$TMP/dup.json"

# --- drops only: loss is detected, never hidden (tail loss after the last frame is undetectable)
"$INJ" --in "$TMP/clean.fdr" --out "$TMP/drop.fdr" --seed 4 --drop-ppm 10000 --manifest "$TMP/drop.m" >/dev/null
"$REPLAY" --input "$TMP/drop.fdr" --event-log "$TMP/drop.log" > "$TMP/drop.json"
missing="$(num missing_total "$TMP/drop.json")"; dropped="$(num dropped "$TMP/drop.m")"
[ "$missing" -gt 0 ] && [ "$missing" -le "$dropped" ] && [ "$((dropped - missing))" -le 3 ]
[ "$(str state "$TMP/drop.json")" = "degraded" ]
[ "$(grep -c '"event":"gap"' "$TMP/drop.log")" = "$(num gap_events "$TMP/drop.json")" ]

# --- reorder only: one gap + one late arrival per delayed frame
"$INJ" --in "$TMP/clean.fdr" --out "$TMP/re.fdr" --seed 4 --reorder-ppm 20000 --manifest "$TMP/re.m" >/dev/null
"$REPLAY" --input "$TMP/re.fdr" > "$TMP/re.json"
[ "$(num out_of_order "$TMP/re.json")" = "$(num reordered "$TMP/re.m")" ]
[ "$(num gap_events "$TMP/re.json")" = "$(num reordered "$TMP/re.m")" ]
[ "$(num outstanding_missing "$TMP/re.json")" = "0" ]

# --- pause policy: nothing is forwarded after the first gap
"$REPLAY" --input "$TMP/drop.fdr" --gap-policy pause > "$TMP/pause.json"
[ "$(str state "$TMP/pause.json")" = "paused" ]
[ "$(num forwarded "$TMP/pause.json")" -lt "$(num forwarded "$TMP/drop.json")" ]
grep -q '"balanced": true' "$TMP/pause.json"

# --- determinism: same input + same config => same consumer digest, same injector output
"$INJ" --in "$TMP/clean.fdr" --out "$TMP/drop2.fdr" --seed 4 --drop-ppm 10000 >/dev/null
cmp "$TMP/drop.fdr" "$TMP/drop2.fdr"
"$REPLAY" --input "$TMP/drop.fdr" > "$TMP/drop_b.json"
[ "$(grep '"consumer"' "$TMP/drop.json")" = "$(grep '"consumer"' "$TMP/drop_b.json")" ]

# --- rate mode paces by --rate (2000 frames at 100k/s ~ 20 ms) and still counts everything
"$REPLAY" --input "$TMP/clean.fdr" --mode rate --rate 1000000 > "$TMP/rate.json"
[ "$(num frames "$TMP/rate.json")" = "20000" ]

# --- usage errors exit 2, bad input exits 1
rc=0; "$INJ" --bogus >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 2 ]
rc=0; "$REPLAY" --input "$TMP/clean.fdr" --gap-policy nope >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 2 ]
rc=0; "$REPLAY" --input "$TMP/none.fdr" >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 1 ]
echo "faults ok"
