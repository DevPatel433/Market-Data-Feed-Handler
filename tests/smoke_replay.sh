#!/bin/sh
# End-to-end smoke test: generate -> replay, twice, and require identical results,
# and require the decoder's reject count to match the generator's manifest.
# Usage: smoke_replay.sh <feed_gen_events> <feed_replay>
set -eu
GEN="$1"; REPLAY="$2"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT

num() { sed -n "s/.*\"$1\": *\([0-9][0-9]*\).*/\1/p" "$2" | head -n 1; }

for profile in normal bursty malformed duplicate gap reorder; do
  "$GEN" --profile "$profile" --seed 5 --count 3000 --out "$TMP/a.fdr" --manifest "$TMP/m.json" >/dev/null
  "$GEN" --profile "$profile" --seed 5 --count 3000 --out "$TMP/b.fdr" >/dev/null
  cmp "$TMP/a.fdr" "$TMP/b.fdr"                      # byte-identical generation

  "$REPLAY" --input "$TMP/a.fdr" > "$TMP/a.json"
  "$REPLAY" --input "$TMP/b.fdr" > "$TMP/b.json"
  da="$(grep '"digest"' "$TMP/a.json")"; db="$(grep '"digest"' "$TMP/b.json")"
  [ "$da" = "$db" ]                                  # identical replay digest

  frames="$(num frames "$TMP/a.json")"; decoded="$(num decoded "$TMP/a.json")"
  rejected="$(num rejected_total "$TMP/a.json")"
  [ "$frames" = "$(num frames_out "$TMP/m.json")" ]  # replayed everything generated
  [ "$((decoded + rejected))" -eq "$frames" ]        # every frame accounted for
  [ "$rejected" = "$(num malformed "$TMP/m.json")" ] # decoder agrees with ground truth
done

# Usage errors must exit 2, missing input must exit 1.
if "$GEN" --bogus >/dev/null 2>&1; then echo "gen accepted bogus flag"; exit 1; fi
rc=0; "$REPLAY" --input "$TMP/missing.fdr" >/dev/null 2>&1 || rc=$?
[ "$rc" -eq 1 ]
echo "smoke ok"
