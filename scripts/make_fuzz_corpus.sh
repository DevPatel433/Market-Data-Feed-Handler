#!/bin/sh
# Build a seed corpus for the libFuzzer target from generated traffic.
# Usage: scripts/make_fuzz_corpus.sh [build_dir] [corpus_dir]
set -eu
BUILD="${1:-build}"; CORPUS="${2:-fuzz-corpus}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$CORPUS"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
for profile in normal malformed; do
  "$ROOT/$BUILD/feed_gen_events" --profile "$profile" --seed 11 --count 400 --fault-ppm 100000 \
      --out "$TMP/x.fdr" --frames-dir "$CORPUS" --frames-max 400 >/dev/null
  # keep both profiles: rename so the second pass does not overwrite the first
  for f in "$CORPUS"/frame_*.bin; do mv "$f" "$CORPUS/${profile}_$(basename "$f")"; done
done
echo "corpus in $CORPUS: $(ls "$CORPUS" | wc -l) files"
