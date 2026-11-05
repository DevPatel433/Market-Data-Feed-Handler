# Low-Latency Market Data Feed Handler & Event Pipeline

A phased C++20 systems project: a simulated pipeline that replays or receives
exchange-like messages, validates sequence numbers, parses events, hands them
through bounded queues, and measures behaviour under normal and bursty load.

An independent portfolio project using only synthetic data. It is not a production
exchange feed, and nothing here claims production-grade performance.

**Current status: Phases 0-5 complete** (specification, workloads, replay, safe validating
decoder, sequence validation with fault injection, bounded SPSC queue, verified two-thread pipeline,
a loopback UDP sender/receiver, and overload measurement with downstream state and gap recovery).
Next: Phase 6, benchmarking, profiling and optimisation.
See `docs/architecture.md`.

## Build and test

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure

# With sanitizers (Debug recommended)
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DFEED_SANITIZE=address,undefined
cmake --build build-asan --parallel && ctest --test-dir build-asan --output-on-failure
```

Requires CMake >= 3.20 and a C++20 compiler (GCC 11+/Clang 14+), Linux. No third-party dependencies.
Warnings are errors by default; pass `-DFEED_WERROR=OFF` to relax.

## Try it

```sh
scripts/demo_phase0.sh build                       # generate -> replay -> summary

build/feed_gen_events --profile gap --seed 7 --count 5000 --out /tmp/gap.fdr
build/feed_replay --input /tmp/gap.fdr                          # max speed
build/feed_replay --input /tmp/gap.fdr --mode timed --speed 4   # honour offsets, 4x
```

`feed_replay` decodes every frame and prints a JSON summary: decoded count, rejects broken
down by reason, per-type counts, sequence range, and a content `digest` (two replays of the
same file always produce the same digest). Profiles: `normal bursty malformed duplicate gap reorder`.
Exit codes: 0 ok, 1 I/O or input error, 2 usage error.

## Overload and recovery

```sh
# 1. Compare queue-full policies under bursty overload: a deterministic model, exact and reproducible
build/feed_overload --format table                         # drop / block / fail x queue sizes; loss, wait, source delay, time to steady state
build/feed_overload --real --format table                  # also run the real pipeline per cell (needs >= 2 cores to be comparable)

# 2. What happens to downstream state when events are lost, and how it is repaired
scripts/compare_recovery.sh build                          # naive vs none vs retransmit vs snapshot vs hybrid on one faulty feed
build/feed_pipeline --input /tmp/faulty.fdr --late-policy forward --recovery hybrid --authoritative /tmp/clean.fdr \
    --retention 1000 --snapshot-interval 200 --measure-lag --timeline-out /tmp/occupancy.csv
```

The key results (`docs/overload_and_recovery.md`): a lossless system has the same worst-case end-to-end delay wherever it waits, so queue memory only decides *where* events wait;
applying messages as they arrive silently corrupts downstream state, while applying them strictly in order keeps it **stale but never wrong** and lets it be repaired from a retained log or snapshots, or reported **unavailable**;
and a UDP sender cannot be backpressured, so overload there is loss at the receiver. `scripts/demo_phase5.sh` shows all three.

## UDP transport (loopback only)

```sh
build/feed_gen_events --profile normal --seed 7 --count 20000 --out /tmp/clean.fdr
build/feed_fault_inject --in /tmp/clean.fdr --out /tmp/faulty.fdr --drop-ppm 5000 --dup-ppm 5000 --reorder-ppm 5000

build/feed_udp_recv --port-file /tmp/port --idle-exit-ms 300 --event-log /tmp/events.jsonl &   # prints a JSON report on exit
build/feed_udp_send --input /tmp/faulty.fdr --port "$(cat /tmp/port)" --rate 20000 --burst-size 8
wait

# other knobs: --recv-mode busy, --junk-ppm/--junk-size (invalid datagrams), --packet-log FILE, --full-policy drop|fail
# graceful shutdown any time: kill -TERM <pid>   (report is still printed; exit status 0)
```

The receiver reports `datagrams`, `kernel_drops` (from `/proc/net/udp`), rejected-by-reason, sequencing counters, queue overflow and
`"balanced"`. Injected gaps are detected, never hidden; loss in the kernel socket buffer and loss in the queue are reported separately from network gaps. Both tools refuse non-loopback addresses.
Design, shutdown protocol, loss attribution and verification: `docs/udp_transport.md`.

## Queue and pipeline

```sh
build/feed_pipeline --input /tmp/clean.fdr --queue-capacity 64                          # block: backpressure, nothing lost
build/feed_pipeline --input /tmp/clean.fdr --queue-capacity 16 --full-policy drop --consumer-work-ns 3000   # slow consumer: loss is counted
build/feed_pipeline --input /tmp/clean.fdr --queue-capacity 4 --full-policy fail --consumer-work-ns 5000    # stops at first overflow (exit 3)
```

A producer thread runs replay -> decode -> sequence -> bounded SPSC queue; a consumer thread verifies order and
integrity. The JSON report ends with `"balanced"`: every frame is accounted for, producer and consumer agree on what
crossed the queue, and nothing was reordered, duplicated or torn. Queue overflow is reported separately from network
loss. Contract, memory-ordering argument, policies and verification: `docs/spsc_queue.md`.

Thread-safety checks (ThreadSanitizer cannot be combined with ASan, so it is its own build):

```sh
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=RelWithDebInfo -DFEED_SANITIZE=thread
cmake --build build-tsan --parallel
FEED_STRESS_SCALE=3 TSAN_OPTIONS=halt_on_error=1 ctest --test-dir build-tsan --output-on-failure
```

The queue's layout options (cache-line padding, index caching) are **not yet decided**: they need a measurement on a machine
with at least two cores. `build/feed_bench_spsc --pin` prints whether its own result is valid for that decision.

## Sequence validation and fault injection

```sh
build/feed_gen_events --profile normal --seed 3 --count 20000 --out /tmp/clean.fdr
build/feed_fault_inject --in /tmp/clean.fdr --out /tmp/faulty.fdr --drop-ppm 5000 --dup-ppm 5000 --reorder-ppm 5000
build/feed_replay --input /tmp/faulty.fdr --event-log /tmp/events.jsonl            # gap policy: degrade
build/feed_replay --input /tmp/faulty.fdr --gap-policy pause --late-policy forward  # alternative policies
build/feed_replay --input /tmp/clean.fdr --mode rate --rate 50000                   # fixed-rate replay
```

Every valid message is classified in-order, gap, duplicate or out-of-order per stream; the JSON summary
reports the counters, per-stream state (`synced`/`degraded`/`paused`) and a `balanced` accounting check.
Policies, state machine, memory bounds and the reasoning behind each decision: `docs/sequencing.md`.

## Decoder and fuzzing

```cpp
#include "feed/decoder.hpp"
feed::Message msg;
if (feed::decode(datagram_bytes, msg) == feed::DecodeError::None) { /* use msg */ }
```

The decoder never allocates, throws or reads out of bounds, and accepts only canonical
encodings. Details and test strategy: `docs/decoder.md`. Coverage-guided fuzzing (Clang):

```sh
cmake -S . -B build-fuzz -DCMAKE_CXX_COMPILER=clang++ -DFEED_FUZZ=ON -DFEED_SANITIZE="address,undefined"
cmake --build build-fuzz --target feed_fuzz_decoder feed_gen_events
scripts/make_fuzz_corpus.sh build-fuzz fuzz-corpus
build-fuzz/feed_fuzz_decoder -max_total_time=60 fuzz-corpus
```

`feed_fuzz_replay FILE...` (built everywhere) re-runs individual inputs, e.g. a crash artifact.

## Layout

```
include/feed/   protocol, decoder, sequencer, ingest, SPSC queue, pipeline, UDP socket/sender/receiver, overload model, book state, recovery, fault injector, replay file, workload, replayer, metric definitions
src/            library implementation + feed_replay CLI
tools/          feed_gen_events (generator, corpus export), feed_fault_inject, fuzz_decoder (fuzz target)
                (CLIs in src/: feed_replay, feed_pipeline, feed_overload, feed_udp_send, feed_udp_recv)
tests/          unit tests (own tiny harness) + CLI smoke test
data/sample/    small synthetic streams + manifests (regenerate: scripts/gen_samples.sh)
docs/           protocol.md, decoder.md, sequencing.md, spsc_queue.md, udp_transport.md, overload_and_recovery.md, metrics.md, workloads.md, architecture.md
benchmarks/     SPSC layout benchmark (more in Phase 6)
.github/        CI: gcc+clang, Release and ASan/UBSan, sample-reproducibility check
```

## Documents

- `docs/overload_and_recovery.md`: overload model and policy trade-offs, downstream state, recovery strategies, results and limits
- `docs/udp_transport.md`: UDP design, receive modes, shutdown, loss attribution, verification
- `docs/spsc_queue.md`: queue contract, memory-ordering argument, full policies, layout decision, verification
- `docs/sequencing.md`: classification, gap/late policies, state machine, fault injection, event log
- `docs/decoder.md`: decoder guarantees, test strategy, fuzzing
- `docs/protocol.md`: wire format, hex examples, invalid-message contract, replay file format
- `docs/metrics.md`: clock, timestamp placement, latency and loss definitions, percentile rules
- `docs/workloads.md`: profiles, determinism guarantees, known caveats
