# Metric definitions

Fixed in Phase 0 so that every later measurement means the same thing.
Code-level form: `include/feed/metrics_defs.hpp`.

## 1. Clock

All timestamps come from `std::chrono::steady_clock` (monotonic; `CLOCK_MONOTONIC` on Linux), stored as nanoseconds. Wall-clock time is never used for latency. On Linux `CLOCK_MONOTONIC` is system-wide, so timestamps taken by a sender and a receiver *process on the same host* are comparable; across hosts they are not.

## 2. Timestamp placement

| Name | Taken | Notes |
|------|-------|-------|
| `t_sched`   | Scheduled emission time of the frame (replay `offset_ns`, or sender clock when it sends) | Source side |
| `t_rx`      | Immediately after the bytes are in user memory (file replay: just before the sink call; UDP: right after `recvfrom` returns) | **Start of the pipeline** |
| `t_decoded` | After decode and sequence check succeed | |
| `t_enqueued`| After a successful queue push | |
| `t_dequeued`| After a successful queue pop | |
| `t_done`    | After the consumer has applied/logged the event | **End of the pipeline** |

## 3. Derived latencies

| Metric | Definition | Answers |
|--------|------------|---------|
| `decode_ns`     | `t_decoded - t_rx`    | Parsing + sequencing cost |
| `queue_ns`      | `t_dequeued - t_enqueued` | Time waiting in the ring buffer |
| `process_ns`    | `t_done - t_dequeued` | Consumer work |
| `end_to_end_ns` | `t_done - t_rx`       | Pipeline latency (headline number) |
| `lateness_ns`   | actual emit time - scheduled emit time | Replayer/sender accuracy (reported by `feed_replay --mode timed`) |

`end_to_end_ns` deliberately excludes the network and the kernel's receive path: it starts when user space has the bytes. Kernel/NIC latency needs hardware timestamps or a sender-embedded timestamp and is out of scope until Phase 4 decides on it. The pipeline must never claim a number it did not measure.

## 4. Loss accounting

Every frame that does not reach the consumer is attributed to **exactly one** category. Network loss and queue overflow are never merged.

| Counter | Meaning |
|---------|---------|
| `rx_frames`            | Frames handed to the decoder |
| `rejected`             | Failed decode/validation (`protocol.md` §6) |
| `duplicate_discarded`  | `seq` already seen; intentionally not forwarded |
| `late_discarded`       | Filled a declared gap, but `late_policy = discard` |
| `paused_discarded`     | Stream paused after a gap (`gap_policy = pause`) |
| `stream_limit_discarded` | More distinct streams than `max_streams` |
| `queue_overflow`       | Decoded fine, but the bounded queue was full (drop-and-count policy; Phase 3) |
| `processed`            | Consumer finished the event |

Identity that tests must assert at shutdown:

```
rx_frames = rejected + duplicate_discarded + late_discarded + paused_discarded + stream_limit_discarded
          + queue_overflow + processed + residual_in_queue
```

`feed_replay` (no queue) reports the part up to `forwarded`; `feed_pipeline` reports the complete identity, with `residual_in_queue`
always 0 because the consumer drains before the report is built. Sequencing adds its own identity (`sequencing.md` §5).

**Queue occupancy** is `pushed - popped`; **high-water** is the peak occupancy seen by the producer (an upper bound, exact when the consumer is
idle; `spsc_queue.md` §2). **Queue overflow is never reported as network loss**: a run that drops events in the queue still has `missing_total == 0`.
**UDP counters.** `socket.datagrams` (every datagram read; all are handed to the pipeline, so it must equal `rx_frames`), `truncated`, `kernel_drops`
(datagrams the kernel dropped because the socket buffer was full, read from `/proc/net/udp`; they never reach user space and so are *not* in `rx_frames`) and
`discarded_at_shutdown` (fail-fast only). Wire identity: `sent + junk_sent == datagrams + discarded_at_shutdown + kernel_drops`. `kernel_drops` is **observed**, whereas `network_gap` (`missing_total`) is **inferred** from sequence numbers;
tail loss is visible only to the former (`udp_transport.md` §7). `t_rx` for UDP is taken immediately after `recvmsg` returns.
**Lag and occupancy (Phase 5).** With `--measure-lag` the producer stamps each event when it *offers* it to the queue (`t_offered`, taken immediately before the enqueue attempt) and the consumer records `dequeue - t_offered` in a fixed-memory histogram
(`lag_histogram.hpp`; percentiles are upper bounds within 12.5% of exact). For `drop` this is `queue_ns`; for `block` it also includes the time the producer was held back, but not the time it waited for its scheduled slot. Occupancy is sampled every `--sample-interval-us`
(`t_ns, occupancy`; `--timeline-out` writes CSV). **Time to steady state** = from the last moment occupancy was at its peak until at most one event is waiting. Phase 6 adds the remaining timestamps (decode, enqueue) for the full breakdown.

**Downstream-state counters (Phase 5).** Per stream: `events = applied_direct + buffered + stale_ignored + overflow_dropped + discarded_unavailable`, and `buffered = applied_from_buffer + covered_discarded + dropped_on_unavailable + still_buffered`;
the sum of `events` over streams equals the consumer's `processed`. `matches_reference` compares the book digest with the true digest at `next_apply - 1`. See `overload_and_recovery.md` §3-§4.

`network_gap` is separate: it counts sequence numbers **never seen in a valid frame** (inferred from gaps, not observed). In the sequencer it is `missing_total`, split into late arrivals, still-outstanding, evicted and resync-cleared. Because a rejected frame's `seq` is also unseen, it shows up in gap accounting as well; the two counters answer different questions (what arrived broken vs. what is missing) and are reported side by side, not summed.

## 5. Percentiles and sample size

Report median, p95, p99, p99.9 only when the sample supports them: `n >= 10 / (1 - p)`, i.e. p95 needs 200 samples, p99 needs 1,000, p99.9 needs 10,000. Otherwise print `n/a`. Discard a configurable warm-up (default: first 10% of events) before computing statistics, and repeat runs to show run-to-run variation rather than quoting one number.

## 6. Run metadata (recorded with every benchmark result, Phase 6)

Machine/CPU model and core pinning, kernel version, compiler and version, build type and flags, message size, event rate, queue capacity, workload profile + seed, warm-up size, sample count, git commit. Machine-readable output (JSON) goes to `benchmarks/results/` (git-ignored), not into source directories.
