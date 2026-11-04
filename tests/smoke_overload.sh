#!/bin/sh
# feed_overload: deterministic model sweep, formats, real runs and argument handling.
# Usage: smoke_overload.sh <feed_overload>
set -eu
OV="$1"
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT

# the model is deterministic: byte-identical output on every run
"$OV" --format csv --capacities 16,256,4096 > "$TMP/a.csv"
"$OV" --format csv --capacities 16,256,4096 > "$TMP/b.csv"
cmp "$TMP/a.csv" "$TMP/b.csv"
[ "$(wc -l < "$TMP/a.csv")" -eq 10 ]   # header + 3 policies x 3 capacities

# columns: policy,capacity,memory,offered,enqueued,dropped,not_offered,loss_pct,processed,high_water,...
cell() { awk -F, -v p="$1" -v c="$2" -v col="$3" '$1==p && $2==c {print $col}' "$TMP/a.csv"; }
[ "$(cell drop 4096 6)" = "0" ]                                   # enough memory: complete
awk -v v="$(cell drop 16 8)" 'BEGIN{exit !(v > 80)}'              # tiny queue: heavy loss
[ "$(cell block 16 6)" = "0" ]                                    # block never loses...
[ "$(cell block 256 6)" = "0" ]
awk -v v="$(cell block 16 16)" 'BEGIN{exit !(v > 1000000)}'       # ...but holds the source back (>1 ms worst delay)
awk -v v="$(cell fail 16 7)" 'BEGIN{exit !(v > 0)}'               # fail-fast leaves events unsent
[ "$(cell drop 16 10)" -le 16 ]                                   # high water never exceeds capacity
awk -v a="$(cell drop 16 13)" -v b="$(cell drop 4096 13)" 'BEGIN{exit !(a < b)}'   # smaller queue => smaller worst wait

"$OV" --format table --capacities 16,4096 --policies drop,block | grep -q 'burst overload'
"$OV" --format json --capacities 64 --policies drop | grep -q '"mean_utilisation"'

# timeline export
"$OV" --capacities 256 --policies drop --timeline-out "$TMP/tl.csv" --timeline-capacity 256 --timeline-policy drop >/dev/null
[ "$(head -n 1 "$TMP/tl.csv")" = "t_ns,occupancy" ]
[ "$(wc -l < "$TMP/tl.csv")" -gt 100 ]

# real pipeline per cell (small, fast); every real run must balance
"$OV" --count 3000 --burst-size 300 --rate 50000 --service-ns 8000 --capacities 8,1024 --policies drop,block --real > "$TMP/real.json"
[ "$(grep -c '"balanced": true' "$TMP/real.json")" -eq 4 ]
grep -q '"hardware_threads"' "$TMP/real.json"

# argument errors
rc=0; "$OV" --bogus >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 2 ]
rc=0; "$OV" --policies nope >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 2 ]
rc=0; "$OV" --capacities 0 >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 2 ]
rc=0; "$OV" --format xml >/dev/null 2>&1 || rc=$?; [ "$rc" -eq 2 ]
echo "overload ok"
