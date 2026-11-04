#include <dirent.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "check.hpp"
#include "feed/fault_injector.hpp"
#include "feed/udp_receiver.hpp"
#include "feed/udp_sender.hpp"
#include "feed/workload.hpp"
#include "seq_oracle.hpp"

using namespace feed;
using namespace std::chrono_literals;

namespace {

bool udp_available() {
  UdpSocket s;
  std::string e;
  return s.open(e) && s.bind_loopback("127.0.0.1", 0, e);
}

// Sandboxes without loopback UDP skip these tests instead of failing them.
#define REQUIRE_UDP()                                                \
  do {                                                               \
    if (!udp_available()) {                                          \
      std::fprintf(stderr, "    SKIP: loopback UDP not available\n"); \
      return;                                                        \
    }                                                                \
  } while (0)

std::vector<ReplayRecord> workload(Profile p, std::uint64_t count, std::uint64_t seed = 5) {
  WorkloadConfig c;
  c.profile = p;
  c.count = count;
  c.seed = seed;
  return generate_workload(c).records;
}

UdpReceiverConfig rx_config() {
  UdpReceiverConfig c;
  c.pipeline.queue_capacity = 16'384;  // larger than any test run: queue overflow only where a test asks for it
  c.pipeline.full_policy = QueueFullPolicy::DropAndCount;
  c.idle_exit_ms = 150;
  c.duration_ms = 20'000;  // safety net: a bug must fail the test, not hang it
  return c;
}

UdpSenderConfig tx_config(std::uint16_t port) {
  UdpSenderConfig c;
  c.port = port;
  c.mode = SendMode::Rate;
  c.rate_per_sec = 20'000;
  c.burst_size = 8;
  return c;
}

struct E2E {
  UdpReport rx;
  UdpSendReport tx;
  bool opened = false;
};

// Receiver on its own thread, sender on the calling thread; returns after the receiver has shut down.
E2E run_e2e(const std::vector<ReplayRecord>& records, UdpSenderConfig tx, UdpReceiverConfig rxc) {
  E2E out;
  UdpReceiver receiver(rxc);
  std::string error;
  out.opened = receiver.open(error);
  if (!out.opened) {
    std::fprintf(stderr, "    receiver.open failed: %s\n", error.c_str());
    return out;
  }
  tx.port = receiver.port();
  std::thread t([&] { out.rx = receiver.run(); });
  CHECK(send_records(records, tx, out.tx, error));
  t.join();
  return out;
}

// What the consumer should have seen, computed single-threaded from the frames that were sent.
std::uint64_t reference_digest(const std::vector<ReplayRecord>& recs) {
  Fnv d;
  std::uint64_t ticket = 0;
  Ingest in({}, [&](const Message& m, StreamState st) {
    QueuedEvent ev;
    ev.msg = m;
    ev.state = st;
    ev.ticket = ++ticket;
    digest_event(d, ev);
  });
  for (const auto& r : recs) in.on_frame(r.frame);
  return d.h;
}

// sent == received + kernel drops (+ anything deliberately discarded at shutdown): nothing unaccounted for.
void check_wire_accounting(const E2E& e) {
  const auto& s = e.rx.socket;
  CHECK_EQ(s.recv_errors, 0U);  // EAGAIN is "nothing to read", never an error
  CHECK(s.kernel_drops_from_proc);
  CHECK_EQ(e.tx.sent + e.tx.junk_sent, s.datagrams + s.discarded_at_shutdown + s.kernel_drops);
}

std::size_t open_fd_count() {
  std::size_t n = 0;
  if (DIR* d = ::opendir("/proc/self/fd")) {
    while (::readdir(d) != nullptr) ++n;
    ::closedir(d);
  }
  return n;
}

}  // namespace

// ------------------------------------------------------------- socket layer ----

TEST(udp_only_loopback_addresses_are_accepted) {
  std::uint32_t a = 0;
  for (const char* ok : {"127.0.0.1", "127.255.255.254", "127.1.2.3", "localhost"}) CHECK(parse_loopback_ipv4(ok, a));
  for (const char* bad : {"0.0.0.0", "8.8.8.8", "192.168.1.1", "10.0.0.1", "::1", "", "127.0.0.1x", "126.255.255.255", "128.0.0.1"}) {
    CHECK(!parse_loopback_ipv4(bad, a));
  }
  REQUIRE_UDP();
  UdpSocket s;
  std::string e;
  CHECK(s.open(e));
  CHECK(!s.bind_loopback("8.8.8.8", 0, e));
  CHECK(e.find("loopback") != std::string::npos);
  CHECK(!s.set_peer("192.168.0.9", 9, e));

  UdpSenderConfig cfg;
  cfg.host = "10.1.2.3";  // a typo must never aim the sender at a real host
  cfg.port = 9999;
  UdpSendReport rep;
  CHECK(!send_records(workload(Profile::Normal, 10), cfg, rep, e));
  CHECK_EQ(rep.sent, 0U);
}

TEST(udp_socket_round_trip_and_edge_cases) {
  REQUIRE_UDP();
  UdpSocket rx, tx;
  std::string e;
  CHECK(rx.open(e) && rx.bind_loopback("127.0.0.1", 0, e) && rx.set_nonblocking(e));
  CHECK(rx.local_port() != 0);
  CHECK(tx.open(e) && tx.set_peer("127.0.0.1", rx.local_port(), e));

  std::uint8_t buf[64];
  RecvInfo info;
  int err = 0;
  CHECK(rx.recv(buf, info, err) == RecvStatus::WouldBlock);  // empty

  const std::uint8_t five[5] = {1, 2, 3, 4, 5};
  CHECK(tx.send(five, err) == SendStatus::Sent);
  CHECK(tx.send(std::span<const std::uint8_t>{}, err) == SendStatus::Sent);  // a zero-length datagram is legal UDP
  CHECK(rx.recv(buf, info, err) == RecvStatus::Datagram);
  CHECK_EQ(info.size, 5U);
  CHECK(!info.truncated);
  CHECK_EQ(buf[4], 5);
  CHECK(rx.recv(buf, info, err) == RecvStatus::Datagram);
  CHECK_EQ(info.size, 0U);
  CHECK(rx.recv(buf, info, err) == RecvStatus::WouldBlock);

  const std::vector<std::uint8_t> big(100, 0xAB);  // larger than the 8-byte buffer below
  CHECK(tx.send(big, err) == SendStatus::Sent);
  CHECK(rx.recv(std::span<std::uint8_t>(buf, 8), info, err) == RecvStatus::Datagram);
  CHECK(info.truncated);
  CHECK_EQ(info.size, 8U);
  CHECK(rx.recv(buf, info, err) == RecvStatus::WouldBlock);  // the truncated remainder is gone, not re-delivered

  UdpSocket moved = std::move(rx);  // ownership transfer leaves exactly one live fd
  CHECK(moved.is_open());
  CHECK(!rx.is_open());
}

TEST(udp_sender_junk_is_deterministic_and_never_a_valid_frame) {
  REQUIRE_UDP();
  const auto records = workload(Profile::Normal, 400);
  std::vector<std::vector<std::uint8_t>> runs[2];
  std::uint64_t junk_sent[2] = {0, 0};
  for (int run = 0; run < 2; ++run) {
    UdpSocket sink;
    std::string e;
    CHECK(sink.open(e) && sink.bind_loopback("127.0.0.1", 0, e) && sink.set_nonblocking(e));
    int eff = 0;
    CHECK(sink.set_rcvbuf(4 << 20, eff, e));
    UdpSenderConfig cfg;
    cfg.port = sink.local_port();
    cfg.mode = SendMode::MaxSpeed;
    cfg.junk_ppm = 300'000;
    cfg.junk_size = 40;
    cfg.seed = 99;
    UdpSendReport rep;
    CHECK(send_records(records, cfg, rep, e));
    junk_sent[run] = rep.junk_sent;
    CHECK_EQ(rep.sent, 400U);
    std::uint8_t buf[256];
    RecvInfo info;
    int err = 0;
    while (sink.recv(buf, info, err) == RecvStatus::Datagram) runs[run].emplace_back(buf, buf + info.size);
  }
  CHECK(junk_sent[0] > 0);
  CHECK_EQ(junk_sent[0], junk_sent[1]);
  CHECK(runs[0] == runs[1]);  // same seed => the same bytes in the same order
  std::uint64_t junk_seen = 0;
  for (const auto& d : runs[0]) {
    Message m;
    if (d.size() == 40 && d[0] == 0xFF) {
      ++junk_seen;
      CHECK(decode(d, m) == DecodeError::BadVersion);
    }
  }
  CHECK_EQ(junk_seen, junk_sent[0]);
}

TEST(udp_sender_pacing_bursts_duration_and_stop) {
  REQUIRE_UDP();
  UdpSocket sink;
  std::string e;
  CHECK(sink.open(e) && sink.bind_loopback("127.0.0.1", 0, e));
  int eff = 0;
  CHECK(sink.set_rcvbuf(4 << 20, eff, e));
  const auto records = workload(Profile::Normal, 200);

  UdpSenderConfig cfg = tx_config(sink.local_port());
  cfg.rate_per_sec = 2000;
  cfg.burst_size = 10;  // 200 frames = 20 bursts; the last burst is scheduled at 19 * 10 / 2000 s = 95 ms
  UdpSendReport rep;
  CHECK(send_records(records, cfg, rep, e));
  CHECK_EQ(rep.sent, 200U);
  CHECK(rep.wall_ns >= 90'000'000ULL);  // paced, not flooded (sleeps never return early)
  CHECK(rep.wall_ns < 5'000'000'000ULL);

  cfg.rate_per_sec = 1000;
  cfg.burst_size = 1;
  cfg.duration_ms = 60;  // 200 frames would take 200 ms
  CHECK(send_records(records, cfg, rep, e));
  CHECK(rep.stopped_by_duration);
  CHECK(rep.sent > 0 && rep.sent < 200);
  CHECK_EQ(rep.sent + rep.not_sent, 200U);

  std::atomic<bool> stop{true};
  cfg.duration_ms = 0;
  CHECK(send_records(records, cfg, rep, e, &stop));
  CHECK(rep.stopped_by_request);
  CHECK_EQ(rep.sent, 0U);
  CHECK_EQ(rep.not_sent, 200U);
}

TEST(udp_receiver_rejects_bad_configuration) {
  REQUIRE_UDP();
  std::string e;
  {
    UdpReceiverConfig c = rx_config();
    c.pipeline.full_policy = QueueFullPolicy::Block;  // a blocked receiver cannot slow a UDP sender
    UdpReceiver r(c);
    CHECK(!r.open(e));
    CHECK(e.find("block") != std::string::npos);
  }
  {
    UdpReceiverConfig c = rx_config();
    c.max_datagram = kMaxMessageSize;  // must leave room to tell "too large" from "valid"
    CHECK(!UdpReceiver(c).open(e));
  }
  {
    UdpReceiverConfig c = rx_config();
    c.host = "8.8.8.8";
    CHECK(!UdpReceiver(c).open(e));
  }
  {
    UdpReceiverConfig c = rx_config();
    c.pipeline.queue_capacity = 0;
    CHECK(!UdpReceiver(c).open(e));
  }
  UdpReceiver first(rx_config());
  CHECK(first.open(e));
  UdpReceiverConfig c = rx_config();
  c.port = first.port();  // already bound
  UdpReceiver second(c);
  CHECK(!second.open(e));
}

// ------------------------------------------------------------- end to end ----

TEST(udp_e2e_low_rate_delivers_everything_in_order) {
  REQUIRE_UDP();
  const auto records = workload(Profile::Normal, 2000);
  for (const RecvMode mode : {RecvMode::Poll, RecvMode::BusyPoll}) {
    UdpReceiverConfig c = rx_config();
    c.mode = mode;
    const E2E r = run_e2e(records, tx_config(0), c);
    CHECK(r.opened);
    CHECK_EQ(r.tx.sent, 2000U);
    CHECK_EQ(r.tx.send_errors, 0U);
    CHECK_EQ(r.rx.socket.datagrams, 2000U);
    CHECK_EQ(r.rx.socket.kernel_drops, 0U);
    CHECK(r.rx.reason == ShutdownReason::Idle);
    CHECK(r.rx.balanced());
    CHECK_EQ(r.rx.pipeline.consumer.processed, 2000U);
    CHECK_EQ(r.rx.pipeline.sequencing.missing_total, 0U);
    CHECK_EQ(r.rx.pipeline.consumer.digest, reference_digest(records));  // same events, same order as an in-process run
    check_wire_accounting(r);
  }
}

TEST(udp_e2e_moderate_rate_accounts_for_every_datagram) {
  REQUIRE_UDP();
  const auto records = workload(Profile::Normal, 20'000);
  UdpSenderConfig tx = tx_config(0);
  tx.rate_per_sec = 200'000;
  tx.burst_size = 64;
  const E2E r = run_e2e(records, tx, rx_config());
  CHECK(r.opened);
  CHECK_EQ(r.tx.sent, 20'000U);
  CHECK(r.rx.socket.datagrams > 0);
  CHECK(r.rx.balanced());
  check_wire_accounting(r);  // whatever was lost (if anything) is attributed to the kernel buffer, not hidden
  CHECK_EQ(r.rx.pipeline.consumer.processed + r.rx.pipeline.overflow.dropped, r.rx.pipeline.ingest.forwarded);
}

TEST(udp_e2e_injected_gaps_duplicates_and_reordering_are_detected_not_hidden) {
  REQUIRE_UDP();
  const auto clean = workload(Profile::Normal, 6000, 17);
  FaultConfig f;
  f.seed = 17;
  f.drop_ppm = f.duplicate_ppm = f.reorder_ppm = 15'000;
  FaultReport fr;
  const auto faulted = inject_faults(clean, f, fr);
  CHECK(fr.dropped > 0 && fr.duplicated > 0 && fr.reordered > 0);

  // Independent expectation: replay the faulted sequence through the naive reference model.
  feedtest::Oracle oracle;
  std::uint64_t exp_gap_events = 0, exp_missing = 0, exp_dups = 0, exp_ooo = 0;
  for (const auto& rec : faulted) {
    Message m;
    CHECK(decode(rec.frame, m) == DecodeError::None);
    std::uint64_t first = 0, count = 0;
    switch (oracle.classify(m.seq, first, count)) {
      case Classification::Gap: ++exp_gap_events; exp_missing += count; break;
      case Classification::Duplicate: ++exp_dups; break;
      case Classification::OutOfOrder: ++exp_ooo; break;
      default: break;
    }
  }

  const E2E r = run_e2e(faulted, tx_config(0), rx_config());
  CHECK(r.opened);
  CHECK_EQ(r.rx.socket.kernel_drops, 0U);  // 6000 small datagrams fit in the socket buffer: the only loss is the injected loss
  CHECK_EQ(r.rx.socket.datagrams, faulted.size());
  const auto& s = r.rx.pipeline.sequencing;
  CHECK_EQ(s.gap_events, exp_gap_events);
  CHECK_EQ(s.missing_total, exp_missing);
  CHECK_EQ(s.duplicates, exp_dups);
  CHECK_EQ(s.duplicates, fr.duplicated);
  CHECK_EQ(s.out_of_order, exp_ooo);
  CHECK(s.gap_events > 0);  // detected, not hidden
  CHECK(r.rx.pipeline.streams.front().state == StreamState::Degraded);
  CHECK(r.rx.balanced());
  check_wire_accounting(r);
}

TEST(udp_e2e_burst_size_changes_the_arrival_pattern_not_the_average_rate) {
  REQUIRE_UDP();
  const auto records = workload(Profile::Normal, 400);
  const auto big_gaps = [&](std::uint32_t burst) {
    UdpSenderConfig tx = tx_config(0);
    tx.rate_per_sec = 1000;  // 400 datagrams over ~400 ms in both cases
    tx.burst_size = burst;
    UdpReceiverConfig c = rx_config();
    std::uint64_t last = 0, gaps = 0;
    c.observer = [&](std::uint64_t, std::uint64_t t_rx, std::span<const std::uint8_t>, std::size_t, const IngestResult&, bool) {
      if (last != 0 && t_rx - last > 20'000'000ULL) ++gaps;  // > 20 ms between consecutive arrivals
      last = t_rx;
    };
    const E2E r = run_e2e(records, tx, c);
    CHECK_EQ(r.rx.socket.datagrams, 400U);
    return gaps;
  };
  // burst 1: one datagram per millisecond, so no 20 ms silences. burst 50: 8 bursts 50 ms apart, so ~7 silences.
  CHECK(big_gaps(1) <= 2);
  CHECK(big_gaps(50) >= 5);
}

TEST(udp_e2e_junk_datagrams_are_classified_by_size_and_never_create_gaps) {
  REQUIRE_UDP();
  const auto records = workload(Profile::Normal, 1500);
  struct Case {
    std::size_t size;
    DecodeError expect;
  };
  const Case cases[] = {{0, DecodeError::TooShort},   // a zero-length datagram is legal UDP
                        {8, DecodeError::TooShort},
                        {64, DecodeError::BadVersion},
                        {300, DecodeError::TooLarge}};
  for (const Case& cs : cases) {
    UdpSenderConfig tx = tx_config(0);
    tx.junk_ppm = 100'000;
    tx.junk_size = cs.size;
    const E2E r = run_e2e(records, tx, rx_config());
    CHECK(r.opened);
    CHECK(r.tx.junk_sent > 0);
    const auto& ing = r.rx.pipeline.ingest;
    CHECK_EQ(ing.rejected, r.tx.junk_sent);
    CHECK_EQ(ing.rejected_by[static_cast<std::size_t>(cs.expect)], r.tx.junk_sent);
    CHECK_EQ(ing.forwarded, 1500U);  // every real frame still got through
    CHECK_EQ(r.rx.pipeline.sequencing.missing_total, 0U);  // junk is extra traffic: it never consumes a sequence number
    CHECK(r.rx.balanced());
    check_wire_accounting(r);
  }
}

TEST(udp_e2e_oversize_datagrams_are_truncated_and_rejected) {
  REQUIRE_UDP();
  const auto records = workload(Profile::Normal, 800);
  UdpSenderConfig tx = tx_config(0);
  tx.junk_ppm = 150'000;
  tx.junk_size = 600;
  UdpReceiverConfig c = rx_config();
  c.max_datagram = 128;  // smaller than the junk, so the kernel truncates it
  const E2E r = run_e2e(records, tx, c);
  CHECK(r.opened);
  CHECK(r.tx.junk_sent > 0);
  CHECK_EQ(r.rx.socket.truncated, r.tx.junk_sent);
  CHECK_EQ(r.rx.pipeline.ingest.rejected_by[static_cast<std::size_t>(DecodeError::TooLarge)], r.tx.junk_sent);
  CHECK_EQ(r.rx.pipeline.ingest.forwarded, 800U);
  CHECK(r.rx.balanced());
}

TEST(udp_kernel_drops_are_counted_separately_and_tail_loss_is_invisible_to_sequencing) {
  REQUIRE_UDP();
  const auto records = workload(Profile::Normal, 5000);
  UdpReceiverConfig c = rx_config();
  c.rcvbuf_bytes = 1024;  // the kernel raises this to its minimum (a couple of KB): room for a handful of datagrams
  UdpReceiver receiver(c);
  std::string e;
  CHECK(receiver.open(e));

  // Nobody is reading yet, so the buffer fills with the first datagrams and everything after is dropped by the kernel.
  UdpSenderConfig tx = tx_config(receiver.port());
  tx.mode = SendMode::MaxSpeed;
  UdpSendReport sent;
  CHECK(send_records(records, tx, sent, e));
  const UdpReport rep = receiver.run();

  CHECK_EQ(sent.sent, 5000U);
  CHECK(rep.socket.kernel_drops_from_proc);
  CHECK(rep.socket.kernel_drops > 4000);
  CHECK(rep.socket.datagrams > 0);
  CHECK_EQ(sent.sent, rep.socket.datagrams + rep.socket.kernel_drops);  // sent == received + dropped by the kernel
  CHECK_EQ(rep.pipeline.overflow.dropped, 0U);                          // and none of it is queue overflow
  // The surviving datagrams are the first N in order, and nothing arrives after the loss, so sequence
  // tracking has nothing to compare against: the loss is invisible to it and visible only to the kernel counter.
  CHECK_EQ(rep.pipeline.sequencing.missing_total, 0U);
  CHECK(rep.balanced());
}

TEST(udp_observer_sees_every_datagram_in_order_with_monotonic_timestamps) {
  REQUIRE_UDP();
  const auto records = workload(Profile::Normal, 600);
  UdpSenderConfig tx = tx_config(0);
  tx.junk_ppm = 100'000;
  tx.junk_size = 64;
  UdpReceiverConfig c = rx_config();
  std::uint64_t seen = 0, last_arrival = 0, last_t = 0, ordering_errors = 0, rejected_seen = 0;
  c.observer = [&](std::uint64_t arrival, std::uint64_t t_rx, std::span<const std::uint8_t> bytes, std::size_t size,
                   const IngestResult& res, bool truncated) {
    ++seen;
    if (arrival != last_arrival + 1) ++ordering_errors;
    if (t_rx < last_t) ++ordering_errors;
    if (bytes.size() != size || truncated) ++ordering_errors;
    if (res.error != DecodeError::None) ++rejected_seen;
    last_arrival = arrival;
    last_t = t_rx;
  };
  const E2E r = run_e2e(records, tx, c);
  CHECK_EQ(seen, r.rx.socket.datagrams);
  CHECK_EQ(ordering_errors, 0U);
  CHECK_EQ(rejected_seen, r.tx.junk_sent);
}

// ---------------------------------------------------------------- shutdown ----

TEST(udp_shutdown_on_request_is_prompt_even_when_idle) {
  REQUIRE_UDP();
  UdpReceiverConfig c = rx_config();
  c.idle_exit_ms = 0;
  c.duration_ms = 0;  // would run forever
  UdpReceiver receiver(c);
  std::string e;
  CHECK(receiver.open(e));
  UdpReport rep;
  std::thread t([&] { rep = receiver.run(); });
  std::this_thread::sleep_for(100ms);
  const auto asked = std::chrono::steady_clock::now();
  receiver.request_stop();
  t.join();
  const auto took = std::chrono::steady_clock::now() - asked;
  CHECK(took < 1s);  // the wake-up fd interrupts poll(2) immediately; no waiting for a timeout
  CHECK(rep.reason == ShutdownReason::StopRequested);
  CHECK(rep.socket.wakeups >= 1);
  CHECK(rep.balanced());
  CHECK_EQ(rep.socket.datagrams, 0U);
}

TEST(udp_shutdown_with_datagrams_still_queued_processes_them_all) {
  REQUIRE_UDP();
  const auto records = workload(Profile::Normal, 3000);
  UdpReceiverConfig c = rx_config();
  c.idle_exit_ms = 0;
  UdpReceiver receiver(c);
  std::string e;
  CHECK(receiver.open(e));
  UdpSenderConfig tx = tx_config(receiver.port());
  tx.mode = SendMode::MaxSpeed;
  UdpSendReport sent;
  CHECK(send_records(records, tx, sent, e));  // 3000 datagrams are now sitting in the kernel buffer
  receiver.request_stop();                    // stop BEFORE run() has read any of them
  const UdpReport rep = receiver.run();
  CHECK(rep.reason == ShutdownReason::StopRequested);
  CHECK_EQ(rep.socket.datagrams + rep.socket.kernel_drops, 3000U);  // the final drain read what the kernel held
  CHECK_EQ(rep.pipeline.consumer.processed + rep.pipeline.overflow.dropped, rep.pipeline.ingest.forwarded);
  CHECK(rep.balanced());
}

TEST(udp_shutdown_on_duration_limit) {
  REQUIRE_UDP();
  UdpReceiverConfig c = rx_config();
  c.idle_exit_ms = 0;
  c.duration_ms = 200;
  UdpReceiver receiver(c);
  std::string e;
  CHECK(receiver.open(e));
  const UdpReport rep = receiver.run();
  CHECK(rep.reason == ShutdownReason::Duration);
  CHECK(rep.elapsed_ns >= 190'000'000ULL);
  CHECK(rep.elapsed_ns < 3'000'000'000ULL);
  CHECK(rep.balanced());
}

TEST(udp_idle_timer_only_starts_after_the_first_datagram) {
  REQUIRE_UDP();
  UdpReceiverConfig c = rx_config();
  c.idle_exit_ms = 50;
  c.duration_ms = 400;  // nothing ever arrives: the idle rule must NOT fire, the duration limit must
  UdpReceiver receiver(c);
  std::string e;
  CHECK(receiver.open(e));
  const UdpReport rep = receiver.run();
  CHECK(rep.reason == ShutdownReason::Duration);
  CHECK(rep.elapsed_ns >= 390'000'000ULL);
}

TEST(udp_fail_fast_stops_at_overflow_and_accounts_for_the_rest) {
  REQUIRE_UDP();
  const auto records = workload(Profile::Normal, 4000);
  UdpReceiverConfig c = rx_config();
  c.idle_exit_ms = 0;
  c.pipeline.queue_capacity = 4;
  c.pipeline.full_policy = QueueFullPolicy::FailFast;
  c.pipeline.consumer_work_ns = 20'000;
  UdpReceiver receiver(c);
  std::string e;
  CHECK(receiver.open(e));
  UdpSenderConfig tx = tx_config(receiver.port());
  tx.mode = SendMode::MaxSpeed;
  UdpSendReport sent;
  CHECK(send_records(records, tx, sent, e));
  const UdpReport rep = receiver.run();
  CHECK(rep.reason == ShutdownReason::FailFast);
  CHECK(rep.pipeline.aborted);
  CHECK_EQ(rep.pipeline.overflow.dropped, 1U);  // exactly the event that did not fit: reading stops between datagrams
  CHECK(rep.balanced());
  // Datagrams after the failure were read only to be counted, so the sender/receiver books still close.
  CHECK_EQ(sent.sent, rep.socket.datagrams + rep.socket.discarded_at_shutdown + rep.socket.kernel_drops);
  CHECK(rep.socket.discarded_at_shutdown > 0);
}

TEST(udp_repeated_open_run_stop_cycles_do_not_leak_or_hang) {
  REQUIRE_UDP();
  const auto records = workload(Profile::Normal, 50);
  const std::size_t before = open_fd_count();
  for (int i = 0; i < 25; ++i) {
    UdpReceiverConfig c = rx_config();
    c.idle_exit_ms = 0;
    c.mode = (i % 2 == 0) ? RecvMode::Poll : RecvMode::BusyPoll;
    UdpReceiver receiver(c);
    std::string e;
    CHECK(receiver.open(e));
    UdpReport rep;
    std::thread t([&] { rep = receiver.run(); });
    UdpSendReport sent;
    CHECK(send_records(records, tx_config(receiver.port()), sent, e));
    std::this_thread::sleep_for(5ms);
    receiver.request_stop();
    t.join();
    CHECK(rep.balanced());
    CHECK_EQ(rep.socket.datagrams + rep.socket.kernel_drops, sent.sent);
  }
  CHECK_EQ(open_fd_count(), before);  // sockets and eventfds are all closed again
}

// ------------------------------------------------- Phase 5: overload over UDP ----

TEST(udp_a_slow_receiver_cannot_backpressure_the_sender) {
  REQUIRE_UDP();
  // UDP has no flow control. The sender keeps its own pace however slow the receiver is; the overload shows up as
  // loss on the receiving side, never as a slowed sender. (File replay can be paused, a socket peer cannot.)
  const auto records = workload(Profile::Normal, 1500);
  UdpSenderConfig tx = tx_config(0);
  tx.rate_per_sec = 20'000;  // 1500 datagrams: 75 ms nominal
  tx.burst_size = 1;

  UdpReceiverConfig fast = rx_config();
  const E2E a = run_e2e(records, tx, fast);

  UdpReceiverConfig slow = rx_config();
  slow.pipeline.queue_capacity = 8;
  slow.pipeline.consumer_work_ns = 300'000;  // 6x slower than the arrival interval: sustained overload
  const E2E b = run_e2e(records, tx, slow);

  CHECK(a.opened && b.opened);
  CHECK_EQ(a.tx.sent, 1500U);
  CHECK_EQ(b.tx.sent, 1500U);  // the sender sent everything...
  CHECK_EQ(b.tx.send_errors, 0U);
  CHECK(b.tx.wall_ns >= 70'000'000ULL);   // ...at its own pace...
  CHECK(b.tx.wall_ns < 3 * 75'000'000ULL);  // ...and was not slowed by the receiver's overload
  CHECK(b.rx.pipeline.overflow.dropped > 500);  // the loss is on the receiver's side, counted as queue overflow
  CHECK_EQ(a.rx.pipeline.overflow.dropped, 0U);
  CHECK(b.rx.balanced());
  check_wire_accounting(b);
}

TEST(udp_recovery_downstream_repairs_injected_loss_end_to_end) {
  REQUIRE_UDP();
  const auto clean = workload(Profile::Normal, 4000, 41);
  FaultConfig f;
  f.seed = 41;
  f.drop_ppm = f.duplicate_ppm = f.reorder_ppm = 10'000;
  FaultReport fr;
  auto faulty = inject_faults(std::vector<ReplayRecord>(clean.begin(), clean.begin() + 3700), f, fr);
  faulty.insert(faulty.end(), clean.begin() + 3700, clean.end());
  CHECK(fr.dropped > 10);

  auto data = std::make_shared<AuthoritativeData>();
  std::string e;
  CHECK(AuthoritativeData::from_records(clean, *data, e));

  for (const RecoveryStrategy s : {RecoveryStrategy::Naive, RecoveryStrategy::Hybrid}) {
    UdpReceiverConfig c = rx_config();
    c.pipeline.sequencer.late_policy = LatePolicy::Forward;
    c.pipeline.state.enabled = true;
    c.pipeline.state.recovery.strategy = s;
    c.pipeline.state.recovery.source.retention = 100'000;
    c.pipeline.state.recovery.source.snapshot_interval = 200;
    c.pipeline.state.authoritative = data;
    const E2E r = run_e2e(faulty, tx_config(0), c);
    CHECK(r.opened);
    CHECK_EQ(r.rx.socket.kernel_drops, 0U);  // the only loss is the injected loss
    CHECK(r.rx.balanced());
    CHECK_EQ(r.rx.pipeline.state.size(), 1U);
    const auto& st = r.rx.pipeline.state[0];
    if (s == RecoveryStrategy::Hybrid) {
      CHECK(st.state == SyncState::Synced);
      CHECK_EQ(st.next_apply, 4001U);
      CHECK(st.matches_reference);  // exactly the true final state, rebuilt from packets that really crossed a socket
      CHECK_EQ(st.unknown_cancels, 0U);
      CHECK(st.counters.recoveries_retransmit > 0);
    } else {
      CHECK(!st.matches_reference);  // the control silently diverges
    }
  }
}
