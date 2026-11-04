#!/bin/sh
# Export frames from every profile as a fuzz corpus and push them through the fuzz oracle.
# Usage: smoke_corpus.sh <feed_gen_events> <feed_fuzz_replay>
set -eu
GEN="$1"; FUZZ="$2"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
for profile in normal malformed gap; do
  mkdir "$TMP/$profile"
  "$GEN" --profile "$profile" --seed 3 --count 500 --out "$TMP/x.fdr" --frames-dir "$TMP/$profile" --frames-max 500 >/dev/null
  "$FUZZ" "$TMP/$profile"/frame_*.bin
done
echo "corpus ok"
