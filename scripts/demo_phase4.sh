#!/bin/sh
# Phase 4 demo: faulty traffic over real UDP sockets, then a flood that overflows the kernel buffer.
# Usage: scripts/demo_phase4.sh [build_dir]
set -eu
BUILD="${1:-build}"
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
B="$ROOT/$BUILD"
TMP="$(mktemp -d)"; PID=""
trap '[ -z "$PID" ] || kill "$PID" 2>/dev/null || true; rm -rf "$TMP"' EXIT
pick() { tr ',{}' '\n\n\n' | grep -E '^ *"(shutdown|datagrams|kernel_drops|rcvbuf_effective|missing_total|gap_events|duplicates|out_of_order|rejected|queue_overflow|processed|ticket_out_of_order|balanced)"' | tr -s ' ' | tr '\n' ' '; echo; }
start() { rm -f "$TMP/port"; "$B/feed_udp_recv" --port-file "$TMP/port" "$@" > "$TMP/recv.json" 2>/dev/null & PID=$!
          while [ ! -s "$TMP/port" ]; do sleep 0.05; done; PORT="$(cat "$TMP/port")"; }

"$B/feed_gen_events" --profile normal --seed 7 --count 6000 --out "$TMP/clean.fdr" >/dev/null
"$B/feed_fault_inject" --in "$TMP/clean.fdr" --out "$TMP/faulty.fdr" --seed 7 --drop-ppm 5000 --dup-ppm 5000 --reorder-ppm 5000 | tr -s ' ' | tr -d '\n'; echo
echo; echo "== faulty traffic at 20k datagrams/s: injected faults are detected, nothing else is lost =="
start --idle-exit-ms 300
"$B/feed_udp_send" --input "$TMP/faulty.fdr" --port "$PORT" --rate 20000 --burst-size 8 | tr -s ' ' | tr -d '\n'; echo
wait "$PID"; PID=""; pick < "$TMP/recv.json"

echo; echo "== flood into a tiny socket buffer: the kernel drops datagrams, and says so =="
start --idle-exit-ms 300 --rcvbuf 2048
"$B/feed_udp_send" --input "$TMP/clean.fdr" --port "$PORT" --mode max | tr -s ' ' | tr -d '\n'; echo
wait "$PID"; PID=""; pick < "$TMP/recv.json"
echo "(sent = datagrams + kernel_drops exactly. Sequencing inferred most of the loss from gaps, but datagrams"
echo " dropped at the very tail are followed by nothing, so only the kernel counter can see those.)"
