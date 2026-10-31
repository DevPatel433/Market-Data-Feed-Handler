# Local UDP transport (Phase 4)

Code: `include/feed/udp_socket.hpp` + `src/udp_socket.cpp` (socket layer), `udp_sender.hpp/.cpp`, `udp_receiver.hpp/.cpp`,
CLIs `feed_udp_send` / `feed_udp_recv`. File replay (Phases 0-3) stays the deterministic reference mode; UDP is a second *source*
for the same pipeline (`run_pipeline_source`), so everything downstream of the socket is the code that was already tested.

## 1. Scope and safety

Loopback only. `UdpSocket` refuses any address outside `127.0.0.0/8` (and `localhost`) for both bind and send, with no override flag, so a mistyped
`--host` cannot aim a rate-controlled sender at a real machine. IPv4 unicast only. Linux only (`recvmsg`, `SO_RXQ_OVFL`, `eventfd`, `/proc/net/udp`).

## 2. Separation of concerns

```
feed_udp_send:  replay file --> pacing/bursts/junk --> UdpSocket::send
feed_udp_recv:  UdpSocket::recv --> receive loop --> FrameIntake --> decode --> sequence --> SPSC queue --> verifying consumer
                (bytes only)       (timers, shutdown)  (pipeline, unchanged Phase 1-3 code)
```

`UdpSocket` knows nothing about the protocol: bytes in, bytes out, plus metadata (truncation, kernel counters). The receive loop owns timing and
shutdown. Decoding and sequencing are reached only through `FrameIntake`, the same interface file replay uses.

## 3. Datagram handling

- **One message per datagram** (protocol v1; `protocol.md` §1). Each datagram is a bounded byte buffer of `--max-datagram` bytes (default 2048, must exceed
  the 80-byte maximum message so "too large" is distinguishable from "valid").
- Length checks happen in `decode()` before any field is read; the receive path never indexes into a datagram itself.
- **Zero-length datagrams** are legal UDP: they arrive as size 0 and are rejected `too_short`.
- **Oversize datagrams** larger than the buffer are truncated by the kernel; `MSG_TRUNC` is detected and the datagram is counted as `truncated` and rejected
  `too_large` **without decoding** (a truncated prefix must never be mistaken for a valid frame).
- **"Packet size" knob.** Protocol v1 has one message per datagram and rejects padding (`payload_len` must equal the frame size), so a valid datagram is always 16-41 bytes
  and cannot be sized freely. The sender therefore varies datagram size with **junk datagrams** (`--junk-ppm`, `--junk-size`): extra datagrams of random bytes whose first
  byte is forced to `0xFF` (never a valid version), which exercise the too-short / bad-version / too-large / truncation paths. Junk never replaces a real frame, so it never
  creates a sequence gap. The receiver's side of the knob is `--max-datagram`.

## 4. Blocking, non-blocking or polling?

| Option | Verdict |
|--------|---------|
| Blocking `recv` (+ `SO_RCVTIMEO`) | Rejected. A blocked thread cannot be woken promptly for shutdown (only by timeout or a signal that interrupts the syscall), one syscall per datagram, and deadlines need timeouts anyway. |
| Non-blocking + `poll(2)` + drain until `EAGAIN` | **Default (`--recv-mode poll`).** Sleeps when idle (no CPU), wakes on data *or* on an `eventfd`, so `request_stop()` and signals interrupt it immediately. Draining up to 256 datagrams per wake amortises the poll cost under load, and the batch cap keeps deadlines and stop checks responsive. `poll` waits at most 50 ms so duration/idle deadlines are honoured even with no traffic. |
| Non-blocking busy-poll | Available (`--recv-mode busy`). Lowest wake-up latency, burns a core, yields every 32 empty polls so it never starves the sender on a single-core box. For latency work in Phase 6, not the default. |

`recvmmsg` (batch syscall) is not used yet; it is a measured optimisation for Phase 6, not a correctness feature.

## 5. Shutdown

Triggers: `SIGINT`/`SIGTERM`, `request_stop()` (any thread), `--duration-ms`, `--idle-exit-ms` (the idle timer starts only once the first datagram has arrived, so a
receiver started before its sender does not quit early), or fail-fast overflow. The signal handler only calls `request_stop()`, which does an atomic store and a `write(2)` to the
eventfd: both async-signal-safe.

Sequence: stop reading -> **final drain** of what the kernel already queued (processed normally, budget 100 ms so a still-flooding sender cannot stall exit) ->
producer-done -> queue drained and consumer joined -> socket drop counter read -> report printed. Under `--full-policy fail` the remainder is instead read and *discarded
and counted* (`discarded_at_shutdown`): fail-fast means stop now, but the sender/receiver books must still close.

Exit codes (`feed_udp_recv`): 0 ok, 1 setup/socket error, 2 usage error, 3 stopped by fail-fast overflow, 4 internal inconsistency (always a bug).
(`feed_udp_send`): 0 ok, 1 input/setup error, 2 usage error, 3 some datagrams failed to send.

## 6. Queue-full policy for a network source

`block` is **refused** (config error / usage error): a blocked receiver does not slow the sender, it just lets the kernel socket buffer overflow, which is loss this process
cannot account for in-line. Use `drop` (default; queue overflow counted) or `fail`. Phase 5 compares the policies under overload and adds downstream recovery that repairs the loss `drop` causes (`overload_and_recovery.md` §1, §4); a test shows that a slow receiver does not slow the sender (§7 there).

## 7. Where can a datagram be lost, and who sees it?

| Where | Seen by | Counter | Notes |
|-------|---------|---------|-------|
| In the "network" (here: injected by `feed_fault_inject` before sending) | sequencer, by inference | `missing_total`, `gap_events` | cannot tell injected loss from real loss without the sender's manifest |
| Kernel socket buffer full | the kernel | `kernel_drops` (from `/proc/net/udp`) | `SO_RXQ_OVFL` ancillary data is also read, but is only attached to *later* datagrams, so it misses loss at the tail; `/proc` is authoritative |
| Bounded SPSC queue full | the pipeline | `queue_overflow` | attributed to the queue, never to the network |
| Corrupt/invalid | decoder | `rejected` (by reason) | also shows up as a gap when the next valid frame arrives |

**Tail loss is invisible to sequencing.** If the kernel drops everything after the first N datagrams, nothing later arrives to expose the gap, so `missing_total == 0` while
thousands were lost. A test builds exactly this case (receiver not reading while the sender floods): sequencing reports zero gaps and `kernel_drops` accounts for every lost datagram.
This is why the kernel counter exists alongside sequence tracking.

Accounting identities (asserted in tests and printed as `udp_checks.balanced`):

```
receiver:  socket.datagrams == ingest.rx_frames                       (every datagram read is in exactly one bucket)
           + the pipeline identities from metrics.md §4 and spsc_queue.md §5
wire:      sender.sent + sender.junk_sent == socket.datagrams + socket.discarded_at_shutdown + socket.kernel_drops
```

The wire identity needs both reports (the receiver cannot know what was sent). It holds exactly on loopback because delivery into the socket buffer is synchronous with `sendto`.

## 8. Sizing and rate notes

- `--rcvbuf` is a request: Linux doubles it for bookkeeping and caps it at `net.core.rmem_max`. The report prints `rcvbuf_effective`. In a probe on the build machine (8 MiB effective) 5,000
  datagrams of 41 bytes occupied about 4.1 MB, i.e. **roughly 830 bytes of buffer per 41-byte datagram**, so buffer capacity in datagrams is much lower than bytes/41.
- Sender pacing sleeps for the bulk of a wait and yield-spins the rest. A plain spin would starve the receiver on a single core and *cause* drops. `--mode max` (no pacing) floods
  and is expected to overflow a slow receiver; it is useful precisely for demonstrating kernel-drop attribution.
- `--burst-size N` sends N datagrams back-to-back, then waits for the next slot, keeping the average rate. A test observes it through receive timestamps (burst 50 at 1,000/s produces ~7 silences of 50 ms; burst 1 produces none).

## 9. Diagnostics

- `--event-log FILE`: one JSON line per anomaly (rejection, gap, duplicate, late arrival, paused discard).
- `--packet-log FILE [--packet-log-max N]`: one JSON line per datagram: arrival number, receive timestamp (`CLOCK_MONOTONIC` ns), size, truncation flag, first 48 bytes in hex, and the verdict.
- Packet capture: `tcpdump -i lo -n -X udp port <port>` (the port is in `--port-file` or the startup message). Compare the capture with the packet log to separate "never sent" from "sent but dropped".
- `socket.max_batch`, `poll_timeouts`, `wakeups` and `empty_polls` show how the receive loop spent its time.

## 10. Verification

- **Unit/integration tests** (`tests/test_udp.cpp`, 19 tests): loopback-only enforcement; socket round trip, zero-length and truncated datagrams; deterministic junk generation; pacing, burst, duration and stop; configuration
  rejection (block policy, bad buffer, non-loopback, port in use); end-to-end at low rate (poll *and* busy mode, digest equal to an in-process reference), moderate rate (200k datagrams/s offered, every datagram accounted for), **injected gaps, duplicates and
  reordering compared with an independent reference model**, junk classified by size, oversize truncation, kernel-drop attribution, observer ordering, and shutdown by request/duration/idle/fail-fast plus 25 start-stop cycles checking file descriptors do not leak.
- **Two-process smoke test** (`tests/smoke_udp.sh`): real processes, real `SIGTERM`, port files, exit codes, port conflict, refusal of non-loopback hosts, junk, drops, fail-fast.
- **Sanitizers**: whole suite and the smoke test (with TSan-built tools) are clean under ASan+UBSan and TSan; the suite passed 12 consecutive runs and the smoke test 5, with no flakiness.
- **Mutation testing**: 27 planted bugs across socket, sender, receiver, ingest and pipeline: **26 killed**. The survivor was dead code (a "force drop after failure" line that behaved identically, since `enqueue` always tries to push first); it was removed. Testing also found real
  bugs during development: fail-fast accounting broke when a source delivered frames after the overflow (fixed), and the receiver leaked its `eventfd` (fixed; the fd-count test fails without the fix).
- **Limits**: one machine, loopback only. No real NIC, so no real reordering/duplication/loss from the network (those are injected), no multicast (production feeds use it, with A/B line arbitration), no IPv6, no hardware or kernel timestamps, **no throughput or latency claims** (single hardware thread, sanitizer builds). A sender-embedded timestamp for network
  latency would need a protocol version bump, so it is deliberately out of scope; kernel receive timestamps (`SO_TIMESTAMPNS`) need no protocol change and are the planned route in Phase 6.
