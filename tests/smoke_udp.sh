#!/bin/sh
# Two-process end-to-end tests of feed_udp_send / feed_udp_recv over loopback, including signals and exit codes.
# Usage: smoke_udp.sh <feed_gen_events> <feed_fault_inject> <feed_udp_send> <feed_udp_recv>
set -eu
GEN="$1"; INJ="$2"; SEND="$3"; RECV="$4"
TMP="$(mktemp -d)"
PIDS=""
cleanup() { for p in $PIDS; do kill "$p" 2>/dev/null || true; done; rm -rf "$TMP"; }
trap cleanup EXIT
num() { sed -n "s/.*\"$1\": *\([0-9][0-9]*\).*/\1/p" "$2" | head -n 1; }
str() { sed -n "s/.*\"$1\": *\"\([a-z_]*\)\".*/\1/p" "$2" | head -n 1; }

# start_recv <name> [recv args...]: launches a receiver in the background, waits for its port file.
start_recv() {
  name="$1"; shift
  rm -f "$TMP/$name.port"
  "$RECV" --port-file "$TMP/$name.port" "$@" > "$TMP/$name.json" 2> "$TMP/$name.err" &
  RECV_PID=$!; PIDS="$PIDS $RECV_PID"
  i=0; while [ ! -s "$TMP/$name.port" ]; do
    i=$((i + 1)); [ "$i" -le 100 ] || { echo "receiver '$name' did not start"; cat "$TMP/$name.err"; exit 1; }
    kill -0 "$RECV_PID" 2>/dev/null || { echo "receiver '$name' died"; cat "$TMP/$name.err"; exit 1; }
    sleep 0.1
  done
  RECV_PORT="$(cat "$TMP/$name.port")"
}
finish_recv() { rc=0; wait "$1" || rc=$?; RECV_RC=$rc; }

"$GEN" --profile normal --seed 31 --count 6000 --out "$TMP/clean.fdr" >/dev/null

# 1. clean run: everything sent is received, nothing dropped, balanced, idle shutdown, exit 0
start_recv clean --idle-exit-ms 250
"$SEND" --input "$TMP/clean.fdr" --port "$RECV_PORT" --rate 20000 --burst-size 8 > "$TMP/clean.send"
finish_recv "$RECV_PID"
[ "$RECV_RC" -eq 0 ]
[ "$(num sent "$TMP/clean.send")" = "6000" ]
[ "$(num datagrams "$TMP/clean.json")" = "6000" ]
[ "$(num kernel_drops "$TMP/clean.json")" = "0" ]
[ "$(num missing_total "$TMP/clean.json")" = "0" ]
[ "$(num processed "$TMP/clean.json")" = "6000" ]
[ "$(str shutdown "$TMP/clean.json")" = "idle" ]
grep -q '"balanced": true' "$TMP/clean.json"

# 2. injected drops are detected, not hidden (tail loss after the last frame is undetectable by sequencing)
"$INJ" --in "$TMP/clean.fdr" --out "$TMP/drop.fdr" --seed 5 --drop-ppm 10000 --manifest "$TMP/drop.m" >/dev/null
start_recv drop --idle-exit-ms 250 --event-log "$TMP/drop.events"
"$SEND" --input "$TMP/drop.fdr" --port "$RECV_PORT" --rate 20000 --burst-size 8 > "$TMP/drop.send"
finish_recv "$RECV_PID"
[ "$RECV_RC" -eq 0 ]
dropped="$(num dropped "$TMP/drop.m")"; missing="$(num missing_total "$TMP/drop.json")"
[ "$missing" -gt 0 ] && [ "$missing" -le "$dropped" ] && [ "$((dropped - missing))" -le 3 ]
[ "$(grep -c '"event":"gap"' "$TMP/drop.events")" = "$(num gap_events "$TMP/drop.json")" ]
grep -q '"state": "degraded"' "$TMP/drop.json" || grep -q '"gap_events": [1-9]' "$TMP/drop.json"

# 3. junk datagrams: classified by size, never create gaps
start_recv junk --idle-exit-ms 250 --packet-log "$TMP/junk.packets" --packet-log-max 50
"$SEND" --input "$TMP/clean.fdr" --port "$RECV_PORT" --rate 20000 --burst-size 8 --junk-ppm 50000 --junk-size 64 > "$TMP/junk.send"
finish_recv "$RECV_PID"
[ "$RECV_RC" -eq 0 ]
junk="$(num junk_sent "$TMP/junk.send")"; [ "$junk" -gt 0 ]
[ "$(num bad_version "$TMP/junk.json")" = "$junk" ]
[ "$(num rejected "$TMP/junk.json")" = "$junk" ]
[ "$(num missing_total "$TMP/junk.json")" = "0" ]
[ "$(wc -l < "$TMP/junk.packets")" -eq 50 ]
grep -q '"result":"rejected:bad_version"' "$TMP/junk.packets" || grep -q 'in_order:forward' "$TMP/junk.packets"

# 4. graceful shutdown on SIGTERM while running: report is still printed, balanced, exit 0
start_recv term
"$SEND" --input "$TMP/clean.fdr" --port "$RECV_PORT" --rate 50000 --burst-size 16 > "$TMP/term.send"
sleep 0.3
kill -TERM "$RECV_PID"
finish_recv "$RECV_PID"
[ "$RECV_RC" -eq 0 ]
[ "$(str shutdown "$TMP/term.json")" = "stop_requested" ]
[ "$(num datagrams "$TMP/term.json")" = "$(num sent "$TMP/term.send")" ]
grep -q '"balanced": true' "$TMP/term.json"

# 5. duration limit with no traffic at all
start_recv dur --duration-ms 300
finish_recv "$RECV_PID"
[ "$RECV_RC" -eq 0 ]
[ "$(str shutdown "$TMP/dur.json")" = "duration" ]
[ "$(num datagrams "$TMP/dur.json")" = "0" ]

# 6. busy-poll mode gives the same answer
start_recv busy --idle-exit-ms 250 --recv-mode busy
"$SEND" --input "$TMP/clean.fdr" --port "$RECV_PORT" --rate 20000 --burst-size 8 > "$TMP/busy.send"
finish_recv "$RECV_PID"
[ "$RECV_RC" -eq 0 ]
[ "$(num processed "$TMP/busy.json")" = "6000" ]
grep -q '"balanced": true' "$TMP/busy.json"

# 7. fail-fast overflow: exit status 3, books still balance
start_recv ff --queue-capacity 4 --full-policy fail --consumer-work-ns 20000
"$SEND" --input "$TMP/clean.fdr" --port "$RECV_PORT" --mode max > "$TMP/ff.send"
finish_recv "$RECV_PID"
[ "$RECV_RC" -eq 3 ]
[ "$(str shutdown "$TMP/ff.json")" = "fail_fast" ]
grep -q '"balanced": true' "$TMP/ff.json"

# 8. refusals and usage errors
rc=0; "$RECV" --full-policy block >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 2 ]          # block is not allowed for a network source
rc=0; "$RECV" --bogus >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 2 ]
rc=0; "$RECV" --host 8.8.8.8 --duration-ms 100 >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 1 ]   # non-loopback refused
rc=0; "$SEND" --input "$TMP/clean.fdr" --port 9 --host 8.8.8.8 >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 1 ]
rc=0; "$SEND" --input "$TMP/clean.fdr" >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 2 ]      # missing --port
rc=0; "$SEND" --input "$TMP/none.fdr" --port 9 >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 1 ]

# 9. port already in use
start_recv first --duration-ms 1500
rc=0; "$RECV" --port "$RECV_PORT" --duration-ms 100 >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 1 ]
finish_recv "$RECV_PID"
echo "udp ok"
