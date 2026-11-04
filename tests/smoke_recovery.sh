#!/bin/sh
# Recovery through the real CLIs: file pipeline and the UDP receiver, with exit codes and error handling.
# Usage: smoke_recovery.sh <feed_gen_events> <feed_fault_inject> <feed_pipeline> <feed_udp_send> <feed_udp_recv>
set -eu
GEN="$1"; INJ="$2"; PIPE="$3"; SEND="$4"; RECV="$5"
TMP="$(mktemp -d)"; PIDS=""
cleanup() { for p in $PIDS; do kill "$p" 2>/dev/null || true; done; rm -rf "$TMP"; }
trap cleanup EXIT
num() { sed -n "s/.*\"$1\": *\([0-9][0-9]*\).*/\1/p" "$2" | head -n 1; }
str() { sed -n "s/.*\"$1\": *\"\([a-z_]*\)\".*/\1/p" "$2" | head -n 1; }

"$GEN" --profile normal --seed 5 --count 6000 --out "$TMP/clean.fdr" >/dev/null
"$INJ" --in "$TMP/clean.fdr" --out "$TMP/faulty.fdr" --seed 5 --drop-ppm 8000 --dup-ppm 8000 --reorder-ppm 8000 >/dev/null
FAULTY="$TMP/faulty.fdr"; AUTH="$TMP/clean.fdr"
P="--late-policy forward --queue-capacity 256"

# naive control: claims to be synced, silently wrong, some damage visible
"$PIPE" --input "$FAULTY" $P --recovery naive --authoritative "$AUTH" > "$TMP/naive.json"
[ "$(str sync_state "$TMP/naive.json")" = "synced" ]
grep -q '"matches_reference": false' "$TMP/naive.json"
[ "$(num unknown_cancels "$TMP/naive.json")" -gt 0 ]

# none: reports unavailable, and what it holds is exactly right as of its last good point
"$PIPE" --input "$FAULTY" $P --recovery none --authoritative "$AUTH" > "$TMP/none.json"
[ "$(str sync_state "$TMP/none.json")" = "unavailable" ]
grep -q '"matches_reference": true' "$TMP/none.json"
[ "$(num unknown_cancels "$TMP/none.json")" = "0" ]

# retransmit with a window too small for a slow answer: unavailable, still exact
"$PIPE" --input "$FAULTY" $P --recovery retransmit --authoritative "$AUTH" --retention 3 --recovery-latency 30 > "$TMP/retx.json"
[ "$(str sync_state "$TMP/retx.json")" = "unavailable" ]
grep -q '"matches_reference": true' "$TMP/retx.json"

# snapshot and hybrid recover; hybrid survives the same tiny window by falling back to snapshots
for s in snapshot hybrid; do
  "$PIPE" --input "$FAULTY" $P --recovery $s --authoritative "$AUTH" --retention 3 --recovery-latency 30 --snapshot-interval 100 > "$TMP/$s.json"
  [ "$(str sync_state "$TMP/$s.json")" = "synced" ]
  grep -q '"matches_reference": true' "$TMP/$s.json"
  [ "$(num next_apply "$TMP/$s.json")" -gt 5900 ]
  [ "$(num unknown_cancels "$TMP/$s.json")" = "0" ]
  grep -q '"balanced": true' "$TMP/$s.json"
done
[ "$(num recoveries_snapshot "$TMP/hybrid.json")" -gt 0 ]

# lag and timeline outputs
"$PIPE" --input "$AUTH" --queue-capacity 16 --consumer-work-ns 3000 --measure-lag --timeline-out "$TMP/tl.csv" > "$TMP/lag.json"
[ "$(num count "$TMP/lag.json")" = "$(num processed "$TMP/lag.json")" ]
grep -q '"lag_ns"' "$TMP/lag.json"
[ "$(head -n 1 "$TMP/tl.csv")" = "t_ns,occupancy" ]
[ "$(wc -l < "$TMP/tl.csv")" -ge 3 ]

# configuration errors: recovery needing a source, and recovery on a feed that pause would starve
rc=0; "$PIPE" --input "$FAULTY" --recovery hybrid >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 2 ]
rc=0; "$PIPE" --input "$FAULTY" --recovery hybrid --authoritative "$AUTH" --gap-policy pause >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 2 ]
rc=0; "$PIPE" --input "$FAULTY" --recovery bogus >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 2 ]
rc=0; "$PIPE" --input "$FAULTY" --recovery hybrid --authoritative "$TMP/none.fdr" >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 1 ]
rc=0; "$PIPE" --input "$FAULTY" --recovery hybrid --authoritative "$FAULTY" >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 1 ]   # a faulty file is not authoritative

# the same recovery over real UDP sockets
rm -f "$TMP/port"
"$RECV" --port-file "$TMP/port" --idle-exit-ms 300 --late-policy forward --recovery hybrid --authoritative "$AUTH" > "$TMP/udp.json" 2>/dev/null &
RP=$!; PIDS="$PIDS $RP"
i=0; while [ ! -s "$TMP/port" ]; do i=$((i + 1)); [ "$i" -le 100 ] || { echo "receiver did not start"; exit 1; }; sleep 0.1; done
"$SEND" --input "$FAULTY" --port "$(cat "$TMP/port")" --rate 20000 --burst-size 8 >/dev/null
rc=0; wait "$RP" || rc=$?; [ "$rc" -eq 0 ]
[ "$(num kernel_drops "$TMP/udp.json")" = "0" ]
[ "$(str sync_state "$TMP/udp.json")" = "synced" ]
grep -q '"matches_reference": true' "$TMP/udp.json"
grep -q '"balanced": true' "$TMP/udp.json"
echo "recovery ok"
