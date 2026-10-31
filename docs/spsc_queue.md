# Bounded SPSC ring buffer and the two-thread pipeline (Phase 3)

Code: `include/feed/spsc_queue.hpp` (queue), `include/feed/queue_policy.hpp` (what to do when full),
`include/feed/consumer_loop.hpp` (shutdown protocol), `include/feed/pipeline.hpp` + `src/pipeline.cpp`
(producer/consumer pipeline), `benchmarks/bench_spsc_layout.cpp` (layout benchmark).

## 1. Contract

`SpscQueue<T>` is a fixed-capacity FIFO for **exactly one producer thread and exactly one consumer thread**.

- Only one thread ever calls `try_push`, only one ever calls `try_pop`. A second producer or consumer is
  **undefined behaviour** (a data race), not an error the queue can detect. See §7 for what to use instead.
- `stats()` and `size_approx()` may be called from any thread at any time.
- After construction the queue never allocates, blocks, or throws.
- `T` must be default-constructible and nothrow-move-assignable. Slots are constructed up front and assigned into,
  which avoids placement-new lifetime questions. Memory is `capacity * sizeof(T)`, allocated once.
- Capacity is rounded **up to a power of two** (index masking) and is exact: a queue reporting `capacity() == N` holds N
  elements, not N-1. Indices are 64-bit and increase monotonically (never reduced modulo the capacity), so
  "full" is `tail - head == capacity` and "empty" is `tail == head`; there is no ambiguous-full-equals-empty state.

## 2. Full and empty behaviour

| Situation | Result |
|-----------|--------|
| `try_push` when `capacity()` elements are queued | returns `false`; the argument is **untouched** (retry-safe, even for move-only types) |
| `try_pop` when empty | returns `false`; `out` is untouched |
| everything else | `true` |

The queue itself never drops, waits or fails loudly. That decision belongs to the caller (§4).

**Occupancy and high-water mark.** `stats()` reports `pushed`, `popped`, `occupancy` (= pushed - popped),
`full_rejects`, `empty_polls` and `high_water`.
- `pushed`/`popped` come straight from the indices, so they cost nothing.
- `high_water` is the peak occupancy observed by the producer right after a push. It uses a relaxed read of the
  consumer's index, so it is an **upper bound** that can overstate by pops completing during the push itself, never
  exceeds `capacity()`, and is exact when the consumer is idle. It costs one extra load per push and can be switched
  off (`track_high_water = false`; it then reads 0).
- `full_rejects` counts failed push *attempts*: a producer that spins on a full queue increments it on every spin.
- `stats()` is a best-effort snapshot, not an atomic one: each field is individually valid but they are read at slightly different instants.

## 3. Memory-ordering argument

Two threads share the slot array and two indices. `tail` (next index to write) is written only by the producer;
`head` (next index to read) only by the consumer.

| Operation | Order | Why |
|-----------|-------|-----|
| producer reads its own `tail` | relaxed | it is the only writer, so it always sees its own latest value |
| producer writes slot, then `tail.store(t+1)` | **release** | publishes the slot write |
| consumer `tail.load` | **acquire** | pairs with the release above: if it sees `t+1`, the slot write *happens-before* the consumer's read, so the data is fully visible |
| consumer reads its own `head` | relaxed | sole writer |
| consumer reads slot, then `head.store(h+1)` | **release** | publishes "I am finished with this slot" |
| producer `head.load` (full check) | **acquire** | pairs with the release above: the consumer's read of that slot *happens-before* the producer's later overwrite, so the overwrite cannot race with the read |
| statistics counters | relaxed | single writer, plain load+store (no read-modify-write); readers tolerate staleness |

Both pairs are needed. Dropping the first lets the consumer read a half-written slot; dropping the second lets the
producer overwrite a slot while the consumer is still reading it.

**Cached remote index** (optional): each side remembers the other's last-seen index and reloads (acquire) only when that
stale value says "full" or "empty". Indices only ever increase, so a stale value is always *conservative*: a stale `head`
makes the queue look fuller than it is, a stale `tail` makes it look emptier. The worst outcome is a needless reload, never an
unsafe push or pop.

**Shutdown.** The producer pushes everything, then `done.store(true, release)`. The consumer pops until empty, then reads
`done` (acquire); if set, it drains once more and stops (`consume_until_done`). The second drain closes a real window: the
consumer sees "empty", the producer pushes its last events and sets `done`, the consumer reads `done == true`. Without the
second drain those events are lost. `done` needs release/acquire because of the happens-before it creates between the last
push and the final drain; a relaxed read would be formally allowed to miss it on weakly-ordered hardware (§6).

## 4. Queue-full policies (`queue_policy.hpp`)

| Policy | Behaviour | Accounting | Use when |
|--------|-----------|------------|----------|
| `drop` (`DropAndCount`) | never waits; the **new** event is discarded | `queue_overflow` += 1 | the source cannot be paused (live UDP: the sender cannot be told to slow down, so something must give) |
| `block` | spin/yield until space appears (backpressure) | `stalled_events`, `stall_spins` | the source *can* be paused: file replay. Latency of the producer grows instead of completeness shrinking |
| `fail` (`FailFast`) | stop at the first overflow, report it, exit status 3 | `queue_overflow` >= 1 | overflow means the system is mis-sized and silent loss is unacceptable. A source may still deliver frames after the failure (the rest of a socket batch); they are offered like any other event, never silently skipped, so the books balance. The UDP receiver stops between datagrams, so exactly 1 |

Always `offered == enqueued + dropped + aborted`. Drop keeps the *oldest* events (the queue's contents) and sheds the newest,
so survivors are an in-order subsequence. The quantitative comparison of the three policies under bursty overload (loss, queue wait, source delay, time to steady state, memory) is in `overload_and_recovery.md` §1. Block must never be used where the producer is a network receive loop: a blocked
receiver does not slow the sender, it just overflows the kernel socket buffer, which is loss you cannot count. Phase 4/5 builds on this.

## 5. The pipeline and its self-checks

`run_pipeline` runs `replay -> decode -> sequence -> enqueue` on the calling thread and `dequeue -> verify -> count` on a second
thread. Each event crossing the queue carries a **ticket** (1, 2, 3, ... per event *offered* to the queue, independent of protocol
sequence numbers) and a **check** value derived from its fields.

`ConsumerChecker` therefore detects, independently of the queue implementation:
- *skipped tickets*: events dropped before the queue (expected under `drop`);
- *out-of-order*: ticket not greater than the previous one (reordered **or** duplicated delivery);
- *corrupt*: `check` mismatch (a torn or stale read);
- a *digest* of everything delivered, compared with the producer's digest of everything it enqueued.

`PipelineReport::balanced()` asserts, after the consumer has drained:

```
frames_accounted:   rx_frames = rejected + duplicate_discarded + late_discarded + paused_discarded
                              + stream_limit_discarded + queue_overflow + processed         (metrics.md §4, now complete)
queue_consistent:   forwarded_by_sequencer = offered = enqueued + queue_overflow (+ aborted)
                    enqueued = processed,  queue empty,  producer_digest == consumer_digest
                    skipped_tickets + trailing_undelivered_tickets = queue_overflow
order_and_integrity: out_of_order = 0, corrupt = 0
```

Queue overflow is attributed to the queue, never to the network: a run that drops events in the queue still shows
`missing_total == 0` in the sequencer. Under `block` the consumer's digest is a pure function of the input and the
sequencing policy (tests compare it with a single-threaded reference); under `drop` it depends on timing by nature.

CLI: `feed_pipeline --input F --queue-capacity N --full-policy block|drop|fail --consumer-work-ns N [sequencing and replay flags]`.
Exit codes: 0 ok, 1 input error, 2 usage error, 3 stopped by `fail`, 4 internal inconsistency (a bug).

## 6. Layout: padding and index caching (decision pending measurement)

`SpscOptions` selects, at compile time: `pad_indices` (producer-owned and consumer-owned state on separate 64-byte lines to avoid
false sharing), `cache_remote_index` (§3), and `track_high_water`. They change performance and reporting, never correctness; all
combinations pass the same tests. The roadmap says to adopt cache-line separation **only after measuring**, so the default is
the simplest layout (`kDefaultSpscOptions`: no padding, no caching, high-water on).

**I could not measure it.** The only machine available while building this had a single hardware thread. False sharing and
cache-line ping-pong need two threads on two cores, so any benchmark there measures the scheduler, not the layout. The harness
detects this and prints `"valid_for_layout_decision": false`; on that machine it reported ~200 M ops/s with padded variants
sometimes *slower* than plain, which is noise from time-slicing and carries no information. I have deliberately recorded no
conclusion.

To decide on real hardware:

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build --target feed_bench_spsc
build/feed_bench_spsc --pin --messages 20000000 --repeats 9      # need >= 2 physical cores, quiet machine
```

Adopt `pad_indices` and/or `cache_remote_index` (change `kDefaultSpscOptions`) only if the result is `valid_for_layout_decision: true`
and the median gain is clearly outside the min/max spread across repeats, for the event size you actually queue (`QueuedEvent`
is larger than one cache line, which changes the picture compared with an 8-byte payload). Also compare `*_no_high_water` to see what
the statistic costs. Record the machine, kernel, compiler and numbers with the decision.

## 7. When SPSC is not enough

- **Multiple producers** (several feed handlers into one book): an SPSC queue cannot be shared. Options: one SPSC queue per
  producer plus a merging consumer (keeps every hot path wait-free; the merge defines ordering), or a bounded MPSC/MPMC queue with
  per-slot sequence numbers and a CAS on the claim index (e.g. Vyukov's design), which costs contended atomics and gives up the
  wait-free property.
- **Multiple consumers**: shard by key (instrument or stream) onto separate SPSC queues so each consumer still has a single producer, or use MPMC.
- **Blocking instead of spinning**: spinning burns a core and, on a loaded machine, hurts everyone; a condition variable or futex
  adds microseconds of wake-up latency. The right answer depends on whether the core is dedicated.
- **A real exchange feed**: the queue is not the bottleneck there. Expect kernel-bypass networking, busy-polled receive loops, thread pinning,
  NUMA-aware allocation, batching, hardware timestamps, and a retransmission/snapshot channel for gap recovery.

## 8. Verification

- **Unit tests** (`tests/test_spsc_queue.cpp`): exact capacity and rounding, FIFO, wraparound over thousands of cycles, 64-bit index wraparound (a
  start-index test hook begins 5 below 2^64), failed push leaves a move-only argument intact, high-water semantics, padded layout really separates the sides.
- **Stress**: 5 layout variants x 7 speed profiles (ping-pong capacity 1, capacity 2, fast/fast, slow consumer, slow producer, bursty producer), each
  checking nothing missing, duplicated, reordered or torn (self-checking 64-byte payloads), counters, and that a slow consumer really fills the queue.
  `FEED_STRESS_SCALE=N` multiplies iteration counts for soak runs.
- **ThreadSanitizer**: the whole suite is clean under TSan, including a 4-5x soak. `FEED_SANITIZE=thread` selects it; CI runs it with gcc and clang.
- **Does TSan actually catch ordering bugs?** I weakened each barrier in turn to `relaxed` (producer publish, consumer free, both acquire loads, and both
  cached-index refreshes). **All six were flagged by TSan, and four were invisible to a normal optimized run of the same tests.** x86's strong ordering
  hides these bugs from functional tests, so the TSan result is the evidence for the orderings above, not the stress tests.
- **Mutation testing** (30 planted bugs across queue, policy helper, consumer loop and pipeline): 27 caught. The three survivors:
  1. `size_approx` reading tail before head: stays inside the documented "racy snapshot within [0, capacity]" contract, so no deterministic test can fail it.
  2. `done` read with relaxed ordering: a logic race, not a data race. TSan cannot flag it and x86 cannot exhibit it. Justified by the argument in §3 only.
  3. (Found and fixed during this phase) the missing final drain initially **survived** because the lost-tail window is nanoseconds wide. I added the
     `after_empty_poll` seam in `consume_until_done` so a test reproduces the exact interleaving; the mutant is now killed by it.
- **Pipeline tests**: block delivers everything in order and matches a single-threaded reference digest across capacities 1/8/4096 and both late policies on a
  faulty, partly malformed stream; drop quantifies loss under a slow consumer and keeps overflow separate from network loss; fail-fast stops at exactly the first overflow;
  empty and all-rejected input; 100 start/stop cycles; the consumer's detectors are exercised with crafted duplicates, reordering, gaps and corrupted payloads.

### Limits of this verification

- Everything ran on x86-64 (total store order) on one core. **Not tested on weakly-ordered hardware (ARM, POWER).** TSan's happens-before reasoning
  covers data races on any architecture, but only a run on such hardware would exercise reorderings x86 never produces. Run the TSan job and stress soak on an ARM
  machine before relying on this elsewhere.
- No multi-core run, so no contention behaviour has been observed and no performance claim is made.
- Block's spin-then-yield wait is for correctness on any core count, not a latency-optimised wait strategy.
