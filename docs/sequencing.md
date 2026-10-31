# Sequence validation (Phase 2)

Code: `include/feed/sequencer.hpp`, `src/sequencer.cpp` (classification and state),
`include/feed/ingest.hpp`, `src/ingest.cpp` (decode -> sequence -> deliver),
`include/feed/fault_injector.hpp` (fault injection). The sequencer is transport-independent: it sees
`(stream_id, seq)` pairs from messages that already passed `feed::decode`.

## 1. Classification

Each stream keeps `expected`, the next sequence number it wants (initially 1, or the first seq seen when
`join_mid_stream` is on), plus the set of **missing** numbers it has declared. Every valid message is
exactly one of:

| Class | Condition | Effect |
|-------|-----------|--------|
| `InOrder`    | `seq == expected` | `expected++` |
| `Gap`        | `seq > expected` | numbers `expected .. seq-1` become missing; `expected = seq+1` |
| `OutOfOrder` | `seq < expected` **and** `seq` is currently missing | removed from the missing set (a late arrival) |
| `Duplicate`  | `seq < expected` and not missing | nothing |
| `StreamLimit`| a new stream would exceed `max_streams` | discarded, counted |

Transport sequence numbers are a per-stream counter, entirely separate from order IDs and trade IDs.

## 2. Policy decisions (the Phase 2 questions)

| Question | Decision | Why |
|----------|----------|-----|
| What happens on a gap? | **Configurable.** `degrade` (default): keep forwarding, mark the stream `Degraded`. `pause`: mark `Paused`, forward nothing (including the message that exposed the gap) until `resync()`. | Both are legitimate. Degrade favours availability and lets the consumer decide; pause favours correctness of state built from the feed. Sequence tracking continues while paused, so loss accounting stays exact. |
| Is there automatic recovery? | **No.** `resync(stream, next_expected)` is the only way back to `Synced` from `Paused`, and the hook Phase 5's simulated snapshot/retransmit will call. | Recovery needs a data source (log or snapshot) that does not exist yet. |
| Does a late arrival get delivered? | **Configurable.** `discard` (default) or `forward`. | Delivering `seq 5` after `seq 6` can corrupt an order book (a cancel applied before its add). Default is safe; `forward` is for consumers that tolerate reordering. |
| Does a late arrival repair a degraded stream? | Only if it was **forwarded**, nothing is still outstanding, and nothing was given up on (evicted). | A discarded late message never reached the consumer, so the damage is still there. |
| What does a rejected (malformed) frame do to sequencing? | **Nothing.** It never reaches the sequencer; its sequence number is untrusted. The number the sender consumed shows up as a gap when the next valid message arrives. | Using fields from a frame that failed validation would let corrupt input steer state. `metrics.md` §4 reports `rejected` and `missing` side by side. |
| Does the first message have to be seq 1? | By default yes (a later first seq is a gap from 1). `--join-mid-stream` accepts the first message seen as the start. | A replay starts at 1; a live joiner may not. |
| Duplicate of a message that was *rejected* or evicted? | A seq that is not in the tracked missing set is a `Duplicate`. This is unavoidable for evicted ranges (below) and is counted in `seqs_evicted`. | See memory bounds. |

## 3. Stream state machine

```
                 gap (degrade)                      resync()
   Synced ───────────────────────▶ Degraded ─────────────────────▶ Synced
      ▲                               │  late arrival FORWARDED,
      │                               │  nothing outstanding or evicted
      │                               └──────────────────────────▶ Synced
      │         gap (pause)                              resync()
      └─────────────────────────── Paused ──────────────────────▶ Synced
```

`Degraded` does not heal on its own just because later messages arrive in order: a known hole is still a
hole. `Duplicate` and in-order traffic never change state.

## 4. Memory bounds

- **Missing numbers are stored as ranges** (`map<start,end>`), so a gap of a billion numbers costs one entry.
  A late arrival in the middle of a range splits it.
- **`max_missing_ranges` (default 4096) per stream.** Over the cap, the *oldest* ranges are evicted and counted
  as permanently lost (`ranges_evicted`, `seqs_evicted`); the stream cannot return to `Synced` by late arrivals
  afterwards. Without this, a sender that skips every other number would grow memory without bound
  (tested). **Consequence:** an evicted number that later arrives is classified `Duplicate`, not
  `OutOfOrder`; the ambiguity is measurable via `seqs_evicted`.
- **`max_streams` (default 1024).** `stream_id` is attacker-controlled, so unknown streams past the cap are
  discarded and counted (`stream_limit`).
- `seq == UINT64_MAX` is handled without wraparound (tested).

The in-order path does no allocation; gap handling allocates map nodes. That is acceptable for Phase 2 and is
a candidate for a pooled structure if Phase 6 profiling shows it matters.

## 5. Accounting identities (asserted in tests and at the end of every `feed_replay` run)

```
rx_frames = rejected + decoded
decoded   = forwarded + duplicate_discarded + late_discarded + paused_discarded + stream_limit_discarded
observed  = in_order + gap_events + duplicates + out_of_order            (messages routed to a stream)
missing_total = out_of_order + outstanding + seqs_evicted + resync_cleared_seqs
```

The last line means every number ever declared missing is accounted for exactly once: it arrived late, is still
outstanding, was given up on, or was cleared by a resync. `feed_replay` prints `"balanced": true` for the first two.

## 6. Limits of detection

- **Tail loss is invisible.** If the last frames of a stream are lost, nothing later reveals it. Only a
  subsequent message (a heartbeat in a live feed) exposes it. Tests account for this explicitly.
- A gap says *how many* numbers are missing, not whether they were lost, delayed, or will arrive. That is why
  gaps and late arrivals are separate events, and why a reordered frame produces one `Gap` followed by one
  `OutOfOrder`, not "no loss".

## 7. Fault injection

`feed_fault_inject --in A.fdr --out B.fdr [--drop-ppm N --drop-burst-max N --dup-ppm N --reorder-ppm N --reorder-max N]`

- **Drop:** removes runs of 1..`drop-burst-max` frames; the first frame is protected unless `--allow-drop-first`.
- **Duplicate:** a copy is sent immediately after the original with the same offset.
- **Reorder:** a frame is delayed 1..`reorder-max` positions; **offsets stay with their slots**, so the file stays valid and
  timing stays non-decreasing. Each frame is displaced at most once.
- Each fault type has its own RNG stream and passes run in the order drop, duplicate, reorder. Enabling a *later* pass
  therefore never changes what an *earlier* one chose (tested); an earlier pass does change what later ones see, because
  they index into its output. Output is a pure function of (input, config).

Expected sequencer behaviour per fault (each verified by a test or smoke check):

| Injected | Sequencer must report |
|----------|-----------------------|
| `duplicated = D` | `duplicates == D`, `missing_total == 0`, stream stays `Synced` |
| `dropped = X` | `missing_total == X` minus any undetectable tail loss; stream `Degraded` |
| `reordered = R` | `gap_events == R`, `out_of_order == R`, `outstanding == 0` |

For arbitrary combinations, the sequencer is compared arrival by arrival with a naive reference model that tracks
missing numbers one at a time (`tests/seq_oracle.hpp`).

## 8. Event log

`feed_replay --event-log FILE` writes one JSON object per anomaly (normal in-order traffic is not logged):

```json
{"arrival":3,"event":"gap","stream":1,"seq":5,"first_missing":3,"count":2,"action":"forward","state":"degraded"}
{"arrival":24,"event":"out_of_order","stream":1,"seq":24,"action":"discard","state":"degraded"}
{"arrival":87,"event":"rejected","reason":"bad_version"}
```

Events: `gap`, `duplicate`, `out_of_order`, `paused_discard`, `stream_limit`, `rejected`. `arrival` is the 1-based
frame index in the replay.
