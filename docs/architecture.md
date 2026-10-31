# Architecture

```
Source -> Decoder -> Sequencer -> SPSC ring buffer -> Consumer
 (file |    bytes->     gap/dup/      bounded           apply/log
  UDP)      message      ooo state     handoff
                 \___________ Metrics (timestamps, counters) ___________/
```

Each stage is independently testable and benchmarkable; sockets never appear in decoder or sequencer code.

## Status

| Stage | Phase | State |
|-------|-------|-------|
| Protocol constants, encoder, header peek | 0 | done (`protocol.hpp`) |
| Replay file format, reader/writer | 0 | done (`replay_file.hpp`) |
| Deterministic workloads + fault profiles | 0 | done (`workload.hpp`) |
| Replayer (max speed, timed) | 0 | done (`replayer.hpp`) |
| Metric definitions | 0 | done (`metrics_defs.hpp`, `metrics.md`) |
| Full validating decoder + fuzz/mutation tests | 1 | done (`decoder.hpp`, `docs/decoder.md`) |
| Sequencer, ingest stage, fault injector, rate replay | 2 | done (`sequencer.hpp`, `ingest.hpp`, `fault_injector.hpp`, `docs/sequencing.md`) |
| SPSC ring buffer, full policies, two-thread pipeline, TSan | 3 | done (`spsc_queue.hpp`, `queue_policy.hpp`, `pipeline.hpp`, `docs/spsc_queue.md`); layout choice pending a multi-core measurement |
| UDP sender/receiver, loopback only, graceful shutdown, kernel-drop attribution | 4 | done (`udp_socket.hpp`, `udp_sender.hpp`, `udp_receiver.hpp`, `docs/udp_transport.md`) |
| Overload model, lag/occupancy measurement, downstream book, gap recovery (retransmit / snapshot / hybrid) | 5 | done (`overload_model.hpp`, `book_state.hpp`, `recovery.hpp`, `docs/overload_and_recovery.md`) |
| Benchmarks | 6 | to do |

## Deferred decisions (and where they get made)

| Decision | Phase |
|----------|-------|
| ~~On a gap: pause, mark stream degraded, or trigger simulated recovery?~~ | decided in Phase 2: configurable degrade/pause; recovery via `resync()` hook (`sequencing.md` §2) |
| ~~Does a rejected frame advance the expected sequence?~~ | decided in Phase 2: no, it becomes a gap |
| ~~Out-of-order policy~~ | decided in Phase 2: classify, then discard (default) or forward; no reorder buffer |
| ~~Queue-full policy contract~~ | decided in Phase 3: queue reports "full", caller applies drop / block / fail (`spsc_queue.md` §4); quantitative comparison done in Phase 5 (`overload_and_recovery.md` §1) |
| Padding / index caching in the queue | needs a >= 2-core measurement (`spsc_queue.md` §6) |
| ~~UDP receive: blocking vs. non-blocking vs. polling~~ | decided in Phase 4: non-blocking + poll(2) + eventfd wake-up + drain (default), busy-poll optional (`udp_transport.md` §4) |
| ~~Sender-embedded timestamp for network latency~~ | decided in Phase 4: not added (needs a protocol version bump); kernel receive timestamps (`SO_TIMESTAMPNS`) in Phase 6 |
| Recovery source: retained event log vs. snapshot | 5 |

## Non-goals (v1)

No real exchange connectivity, no proprietary data, no multi-producer queue, no claim of production-grade or ultra-low-latency performance without measurements.
