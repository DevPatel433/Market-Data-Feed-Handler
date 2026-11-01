#!/bin/sh
# Policy x capacity sweep from the deterministic model. Same arguments => byte-identical output.
# Usage: scripts/sweep_overload.sh [build_dir] [extra feed_overload args...]
#   e.g. scripts/sweep_overload.sh build --burst-size 500 --service-ns 6000
set -eu
BUILD="${1:-build}"; [ $# -gt 0 ] && shift
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
"$ROOT/$BUILD/feed_overload" --format table "$@"
