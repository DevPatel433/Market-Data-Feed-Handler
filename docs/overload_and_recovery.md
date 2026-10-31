# Backpressure, overload and recovery (Phase 5)

Code: `include/feed/overload_model.hpp` (deterministic model), `lag_histogram.hpp`, `book_state.hpp`, `recovery.hpp`
(authoritative source, recovery engine), `pipeline.hpp` (lag, occupancy timeline, state), tools `feed_overload` and the recovery
flags of `feed_pipeline` / `feed_udp_recv`. Reproduce every table below with `scripts/sweep_overload.sh` and `scripts/compare_recovery.sh`.

Three questions, three answers up front:

1. **What does each queue-full policy cost?** Memory buys completeness *or* bounded delay, never both for free; and for a lossless system the end-to-end
   delay is the same wherever the waiting happens (§1.3).
2. **What happens to downstream state when events are lost?** Applying what arrives silently corrupts it. Applying strictly in order makes it *stale but never wrong*, and
   then it can be repaired or honestly reported unavailable (§4).
3. **Can a UDP sender be backpressured?** No. Only a source that can be paused can be (§7).

---

## 1. Overload: policies, metrics and the model

### 1.1 Definitions

| Term | Definition |
|------|------------|
| **Occupancy** | events waiting in the queue (`pushed - popped`); the event the consumer is working on is not in the queue |
| **Backlog peak / high-water** | the highest occupancy seen |
| **Queue wait** | time an event spent in the queue: dequeue - enqueue |
| **Source delay** | enqueue - scheduled arrival: how long backpressure held the producer back (`block` only) |
| **End-to-end** | completion - scheduled arrival (= source delay + queue wait + service) |
| **Loss** | events discarded by the queue (`queue_overflow`). Always attributed to the queue, never to the network (`metrics.md` §4) |
| **Lag (real pipeline)** | dequeue time - the moment the producer *offered* the event (includes any time it was blocked, but **not** time spent waiting for its scheduled slot) |
| **Time to steady state** | measured from the last moment the backlog was at its peak until the consumer has caught up (<= 1 event waiting). "Congested" means a backlog actually formed (peak >= 2); capacity does not hide a deep backlog in a large queue |

### 1.2 Why a model, and how it is trusted

Real producer/consumer timing depends on scheduling and core count, so two runs disagree. `simulate_overload` takes the scheduled arrival times, a fixed service time and a queue
capacity, and computes the exact consequence of each policy, deterministically. Its semantics match `SpscQueue`: the consumer *pops* (freeing the slot at once) and then works for the service time.

It is trusted because it is **differentially tested against an independent brute-force simulation** that steps time one tick at a time instead of jumping between events: 3,000 random
cases x 3 policies, comparing offered/enqueued/dropped, completion time, high-water, producer stall and the full queue-wait / source-delay / end-to-end distributions, all identical. Hand-computed
cases, conservation laws (`offered = enqueued + dropped`, queue wait <= `capacity x service`, work conservation), and "more memory never loses more" are also asserted.

### 1.3 The scenario and the results

Bursty traffic: 20,000 events, **100,000/s on average, in bursts of 2,000 at 20x that rate** (2M/s for 1 ms), against a consumer that processes **250,000/s** (4 us per event).
Mean utilisation is 0.40 (comfortable), but each burst overloads the consumer 8x, leaving a backlog of about `2000 x (1 - 1/8) = 1750` events at the end of the burst, which takes about 7 ms to drain. Each queue slot
is 88 bytes (`sizeof(QueuedEvent)`). Model output (`scripts/sweep_overload.sh`):

| policy | capacity | memory | loss | queue wait max | source delay max | producer stalled | steady in |
|--------|---------:|-------:|-----:|---------------:|-----------------:|-----------------:|----------:|
| drop  | 16    | 1.4 KiB   | **86.7%** | 64 us    | 0       | 0        | 60 us |
| drop  | 256   | 22 KiB    | 74.7%     | 1,024 us | 0       | 0        | 1.0 ms |
| drop  | 1024  | 88 KiB    | 36.3%     | 4,096 us | 0       | 0        | 4.1 ms |
| drop  | 2048  | 176 KiB   | **0%**    | 6,997 us | 0       | 0        | 7.0 ms |
| block | 16    | 1.4 KiB   | 0%        | 64 us    | 6,933 us | 79 ms    | 60 us |
| block | 256   | 22 KiB    | 0%        | 1,024 us | 5,973 us | 68 ms    | 1.0 ms |
| block | 1024  | 88 KiB    | 0%        | 4,096 us | 2,901 us | 33 ms    | 4.1 ms |
| block | 2048  | 176 KiB   | 0%        | 6,997 us | 0        | 0        | 7.0 ms |
| fail  | 16    | 1.4 KiB   | 99.9% (stops at the first overflow) | | | | |
| fail  | 2048  | 176 KiB   | 0%        | 6,997 us | 0       | 0        | 7.0 ms |

(Capacities are powers of two; the loss in `fail` rows is events never sent because the run stopped.)

### 1.4 What the numbers say

1. **Loss has a cliff, at the backlog peak.** Loss falls from 87% to 36% as the queue grows from 16 to 1,024 slots, then to **zero** at 2,048 (the first size above the 1,750 peak). A queue smaller than
   the burst's backlog loses roughly the excess no matter how it is tuned. A rule of thumb the sweep is consistent with (zero loss at 2,048 slots, 36% at 1,024): **lossless needs `capacity >= burst_size x (1 - 1/overload_factor)`**.
2. **Drop buys bounded delay with completeness.** Queue wait can never exceed `capacity x service_time` (64 us at 16 slots, 7 ms at 2,048): a small bounded queue is a *staleness bound*. Whatever survives is fresh; the rest is gone (and
   must be recovered downstream, §4, or accepted as loss).
3. **For a lossless system, the delay is the same wherever you wait.** Every lossless row (block at any capacity, drop at 2,048+) has the same worst-case **end-to-end** delay: **p99 6,930 us, max 7,000 us**. A 16-slot queue with
   `block` keeps queue wait at 64 us but holds the source back 6.9 ms; a 4,096-slot queue never holds the source back but lets events wait 7 ms. **Memory does not reduce lossless latency; it only moves where the waiting happens.**
   What memory does buy under `block` is *not stalling the source*: total stall time falls from 79 ms (16 slots) to 0 (2,048).
4. **Block is only available where the source can wait.** It is the right choice for file replay, where the producer can simply be paused. It is not available for a network source (§7).
5. **Fail-fast is a detector, not a policy.** It answers "is this deployment sized correctly?" with a loud stop. Its 99.9% "loss" is not data loss; it is the run ending at the first overflow.
6. **Time to steady state tracks backlog depth.** The 16-slot queue is back to steady in 60 us because it threw the burst away; the 2,048-slot queue takes 7 ms because it kept it.

**Choosing:** if completeness matters and the source can pause, `block`. If freshness matters more than completeness and loss can be repaired downstream, a small `drop` queue. If neither, size the queue to the backlog peak
and accept the latency. Memory is cheap here (176 KiB); the trade-off is not memory but whether 7 ms of delay or 15% of events is the lesser harm.

### 1.5 The real pipeline

`feed_pipeline --measure-lag --sample-interval-us N --timeline-out FILE` records per-event lag (histogram, percentiles are upper bounds within 12.5%, `lag_histogram.hpp`), samples queue occupancy, and derives time to steady
state from the samples (resolution = the sampling interval). `feed_overload --real` runs the real pipeline for each (policy, capacity) cell next to the model.

On the build machine (**one hardware thread**) the real runs agree with the model in direction (small drop queue loses heavily, 2,048 slots loses nothing, block loses nothing but stalls the source: 130 ms of stalls
vs 79 ms modelled) but not in magnitude (95.7% real loss vs 86.7% modelled at 16 slots, real backlog peak 2,000 vs 1,750, queue lag several ms where the model says 64 us). That is expected: with one core the producer and the
spinning consumer time-slice, so the producer runs through much of a burst before the consumer gets the CPU. `feed_overload --real` prints `valid_for_comparison: false` below two hardware threads. **Treat the model as the
quantitative result and the real runs as a qualitative cross-check.** Real `lag` also measures dequeue minus *offer* time, so for `block` it excludes the delay the source spent waiting for its schedule; the model's
`source_delay`/`end_to_end` (relative to the *scheduled* time) have no real counterpart.

## 2. Loss attribution, end to end

| Where | Counter | Seen by |
|-------|---------|---------|
| network / sender | `missing_total`, `gap_events` | sequencer (inferred from gaps) |
| kernel socket buffer | `kernel_drops` | `/proc/net/udp` (observed) |
| queue full | `queue_overflow` | the queue policy |
| invalid | `rejected` by reason | decoder |

Queue overflow leaves the sequencer's `missing_total` at 0 (tested: 1,000+ events dropped in the queue, no network gap). But the **downstream state engine sees every one of these as a gap** in the sequence it receives, because
it does its own gap tracking on what actually arrives. That is why a single recovery mechanism repairs network loss and queue overflow alike (§4.4, test `pipeline_overload_loss_in_the_queue_is_repaired_by_recovery_downstream`).

## 3. Downstream state: `BookState`

A minimal order book per stream: `Add` inserts a resting order, `Cancel` removes it, `Trade` updates per-instrument trade statistics (the protocol's trades carry no order id, so they do not reduce resting quantity; a
deliberate simplification), `Heartbeat` is a no-op. Damage from lost messages comes in two forms:

- **Visible** anomalies: a cancel for an unknown order (its Add was lost), an Add for an existing one.
- **Silent** divergence: a lost *Cancel* leaves a stale order. No local check can see it.

Only comparison with a reference is complete, so the book keeps an **order-independent 64-bit digest**, maintained incrementally (O(1) per message). `AuthoritativeData` precomputes the true digest after every sequence number, so
"is this state correct?" is a single comparison. Tests cover add/cancel/trade, anomalies, incremental == from-scratch, snapshot/restore round trips and catch-up from a snapshot.

## 4. Recovery

### 4.1 Design: strict order, stale but never wrong

`RecoveryEngine` applies messages **strictly in sequence order**. A message that arrives ahead of a hole waits in a bounded buffer. The consequence, asserted after **every event** across 240 randomised fault runs:

> **At every moment, in every state, the book's digest equals the true digest at `next_apply - 1`.**

The book can be *stale* (behind) but is never *wrong*: it cannot contain the effect of a message applied out of order, so it never shows the unknown-cancel anomalies or silent divergence of an apply-as-received consumer.
`RecoveryStrategy::Naive` is the control that applies messages as they arrive; the same invariant check fails for it in 15+ of 20 random runs, so the check is meaningful.

### 4.2 States and what they mean

| State | Meaning |
|-------|---------|
| **Synced** | no hole: every message through `next_apply - 1` is applied and nothing is outstanding |
| **Recovering** | a hole is open; the book is a consistent *stale* prefix and repair is in progress (live messages keep arriving into the buffer) |
| **Unavailable** | repair is impossible with this strategy; the book is **frozen** at its last consistent prefix and says so. Later messages are counted and discarded. Terminal |

"Restores known state or clearly reports unavailable state" is therefore a property of the report: `sync_state` is `synced` with `matches_reference: true`, or `unavailable` with `matches_reference: true` *as of `next_apply - 1`*. It is never
"synced and wrong" (except for the naive control, which is exactly that).

### 4.3 Strategies and the recovery services

| Strategy | Behaviour |
|----------|-----------|
| `naive` | control: apply in arrival order, ignore gaps. Always reports `synced`; silently diverges |
| `none` | strict order; if a gap does not heal by itself, go `unavailable` |
| `retransmit` | fetch the missing messages from a **retained log** (the newest `--retention` messages). If the hole has aged out of the window: `unavailable` |
| `snapshot` | wait for a **published snapshot** that covers the newest missing number, load it, drop the buffered messages it makes redundant, drain the rest |
| `hybrid` | retransmit if the hole is still in the window, otherwise snapshot |

The services are simulated by `AuthoritativeView`, built from a complete clean replay file: it advances with the highest sequence number the receiver has seen, publishes a snapshot every `--snapshot-interval` messages (keeping the
newest `--max-snapshots`), and serves retransmissions from the retention window. Two timing knobs model the real world in *events* (so results are deterministic): `--recovery-latency N` is the round trip (the response arrives after N
more events, during which live traffic keeps buffering) and `--reorder-wait N` is how long to wait for a late arrival to heal the gap on its own before asking (late arrivals reach the engine with `--late-policy forward`).
`--recovery-buffer` bounds the out-of-order buffer; messages beyond it are dropped, and recovery fetches them like any other hole.

**Requirements** (enforced, exit code 2 otherwise): `retransmit`/`snapshot`/`hybrid` need `--authoritative FILE`; recovery needs `--gap-policy degrade` because the engine does its own gap handling and `pause` would stop the feed it needs.
An authoritative file must be complete and in order (a faulty file is refused).

### 4.4 Comparison on one faulty feed

6,000 messages with injected drops (91), duplicates (43) and reordering (43); a recovery service with a **3-message log window and a 30-event round trip** (deliberately poor) and snapshots every 100 messages
(`scripts/compare_recovery.sh`):

| strategy | final state | exact? | unknown cancels | reached | gaps | via log | via snapshot | longest wait |
|----------|-------------|:------:|----------------:|--------:|-----:|--------:|-------------:|-------------:|
| naive      | synced (claims) | **no** | 19 | 6001 | n/a | | | |
| none       | **unavailable** | yes, as of seq 9 | 0 | 10 | 1 | | | |
| retransmit | **unavailable** | yes, as of seq 9 | 0 | 10 | 1 | 0 | | |
| snapshot   | synced | yes | 0 | 6001 | 60 | 0 | 34 | 107 events |
| hybrid     | synced | yes | 0 | 6001 | 60 | 0 | 34 | 107 events |

`naive` finishes "synced" and is wrong, with 19 visible anomalies and an unknown number of silent ones. `none` and `retransmit` stop at the first gap (seq 10) and say so; what they hold is exactly right. `snapshot` and `hybrid` recover all 60 gaps (the other 26
of the 60 healed on their own from late arrivals: `self_healed` = 26): 34 by loading a snapshot. `hybrid` equals `snapshot` here because the log window is too small to ever help; with a larger window it would use the log and never wait for a snapshot.

Recovery also repairs **queue overflow** (§2): with a paced feed and a slow consumer dropping about two thirds of events in the queue, `hybrid` opens ~60 gaps, repairs them all, and ends with the exact state; `naive` under the same overload does not.

### 4.5 Time to steady state, memory, latency, completeness

Each gap episode records how many events (and how much wall time) passed between the hole opening and the book being current again (`episode_events_max`, `episode_ns_max`).

| Dimension | Lever | Effect |
|-----------|-------|--------|
| **Latency** | recovery round trip, snapshot interval | `retransmit` recovers `latency` events after the gap; `snapshot` waits for the next snapshot that covers the hole (the longest episode above was 107 events with a snapshot every 100 messages and a 30-event round trip). The book is stale for exactly that long |
| **Memory** | retention window; snapshots x book size; reorder buffer | the log costs `retention x message size`; each snapshot copies the book (here up to ~4,096 live orders), so `max-snapshots x book size`; the buffer is bounded by `--recovery-buffer`, and overflow only delays recovery, it never corrupts |
| **Completeness** | strategy and service reach | `none`/`retransmit` trade completeness for simplicity and report `unavailable`; `snapshot`/`hybrid` always eventually recover but may stay stale for a snapshot interval; a gap with **no** service at all makes `snapshot` wait forever (stale, correct) and `retransmit` give up at once |

## 5. Verification

- **Tests added in Phase 5: 56** (177 in total, all passing under ASan+UBSan and under TSan). Highlights: the stale-but-never-wrong invariant after every event over 240 random runs and the check failing for the naive control; every strategy's deterministic scenarios (aged-out log, snapshot wait,
  bounded buffer, reorder wait, latency, no service); fate-of-every-message identities; the model vs the brute-force oracle; histogram percentiles within 12.5% of exact over 100k samples; pipeline refusal of configurations that cannot work; recovery of queue overflow;
  the sender not being backpressured over real UDP; recovery over real sockets.
- **Mutation testing**: 53 planted bugs in the book, authoritative source, recovery engine, model and pipeline additions, plus 8 in the histogram and queue-policy headers. Initial survivors were genuine test gaps and are fixed: the naive control's stale handling, the
  `events_unavailable` split, the final timeline point, and a histogram-merge minimum, plus one invalid (equivalent) mutant of mine that I replaced. All are now killed.
- **Findings during development** that changed the code: a 100%-loss "overload" test that passed vacuously on one core (root cause: `replayer.cpp` spun without yielding, starving the consumer; fixed, and the test is now paced); a time-to-steady-state definition tied to capacity that hid a deep backlog in a large queue (redefined, §1.1).

## 6. Limits

- **One machine, one hardware thread.** The model is the quantitative result; real-run comparisons are qualitative (§1.5). No multi-core measurement was possible.
- The model has one consumer with a **fixed** service time and ignores producer-side cost; real service times vary.
- The recovery services are **simulated, instantaneous, perfect and in-process**: the authoritative source is assumed complete and correct, availability is modelled by retention and snapshot cadence, and recovery traffic does not compete with the live feed. A real retransmission server has its own
  failure modes, rate limits and latency distribution.
- The book is deliberately simple (trades do not consume resting orders); one book per stream; no persistence across restarts.
- **Tail loss is invisible**: if the last messages of a stream are lost, nothing later exposes the gap, so the engine reports `synced` at an earlier point (exactly correct as of there). Only a later message or heartbeat can reveal it.
- Recovery needs `gap-policy degrade`; combining it with `pause` is refused.

## 7. A UDP sender cannot be backpressured

The roadmap warns against implying otherwise, and a test asserts it: a receiver with a 300 us-per-event consumer and an 8-slot queue is fed 1,500 datagrams at 20,000/s. The sender sends all 1,500 with zero errors and finishes in the same ~75 ms it would
for a fast receiver; the overload appears entirely as `queue_overflow` on the receiving side (hundreds of events), never as a slowed sender. UDP has no flow control, so `block` is refused for a network source (`udp_transport.md` §6). Backpressure on a network feed needs a
protocol-level mechanism (windowing, credits, a rate request), which this protocol does not have; the only options at the receiver are to drop (and recover downstream, §4) or to fail loudly.
