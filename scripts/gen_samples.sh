#!/bin/sh
# Regenerate data/sample/*.fdr and their manifests. Deterministic: same binary
# logic + same seeds => byte-identical files (CI verifies this).
# Usage: scripts/gen_samples.sh [build_dir]   (default: build)
set -eu
BUILD="${1:-build}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
GEN="$ROOT/$BUILD/feed_gen_events"
OUT="$ROOT/data/sample"
[ -x "$GEN" ] || { echo "missing $GEN (build first)" >&2; exit 1; }

for profile in normal bursty malformed duplicate gap reorder; do
  "$GEN" --profile "$profile" --seed 1 --count 1000 --rate 100000 \
         --out "$OUT/$profile.fdr" --manifest "$OUT/$profile.manifest.json" >/dev/null
  echo "wrote $OUT/$profile.fdr"
done
