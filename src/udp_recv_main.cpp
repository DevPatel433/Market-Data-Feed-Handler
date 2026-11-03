// feed_udp_recv: receive UDP datagrams on loopback and run them through
// decode -> sequence -> bounded SPSC queue -> verifying consumer. Prints a JSON report on exit.
// Shuts down gracefully on SIGINT/SIGTERM, --duration-ms, or --idle-exit-ms.
// Exit codes: 0 ok, 1 setup/socket error, 2 usage error,
//             3 stopped by --full-policy fail (queue overflow),
//             4 internal consistency failure (accounting, ordering, integrity): always a bug.
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "cli_state_flags.hpp"
#include "feed/event_log.hpp"
#include "feed/report_json.hpp"
#include "feed/udp_receiver.hpp"

namespace {

feed::UdpReceiver* g_receiver = nullptr;

extern "C" void on_signal(int) {
  if (g_receiver != nullptr) g_receiver->request_stop();  // async-signal-safe: atomic store + write(2)
}

void usage(const char* argv0) {
  std::fprintf(stderr,
               "Usage: %s [options]\n"
               "Socket:\n"
               "  --host H               loopback address to bind (default 127.0.0.1; non-loopback is refused)\n"
               "  --port N               UDP port, 0 = ephemeral (default 0)\n"
               "  --port-file FILE       write the bound port to FILE once listening\n"
               "  --recv-mode poll|busy  poll: poll(2) + drain (default); busy: spin on recv (burns a core)\n"
               "  --rcvbuf BYTES         requested SO_RCVBUF (default 4194304; the kernel clamps it)\n"
               "  --max-datagram N       receive buffer size, > 80 (default 2048); larger datagrams are truncated and rejected\n"
               "Shutdown (SIGINT/SIGTERM always work):\n"
               "  --duration-ms N        stop after N ms\n"
               "  --idle-exit-ms N       stop after N ms without a datagram (counted once the first one has arrived)\n"
               "Queue / sequencing:\n"
               "  --queue-capacity N  --full-policy drop|fail  (block is refused for a network source)\n"
               "  --consumer-work-ns N  --gap-policy degrade|pause  --late-policy discard|forward  --join-mid-stream\n"
               "Diagnostics:\n"
               "  --event-log FILE       JSON Lines: one line per anomaly (rejection, gap, duplicate, late, ...)\n"
               "  --packet-log FILE      JSON Lines: one line per datagram with a hex prefix\n"
               "  --packet-log-max N     stop packet logging after N datagrams (default 1000)\n"
               "%s",
               argv0, feedcli::state_flags_usage());
}

bool parse_u64(const char* s, std::uint64_t& out) {
  if (s == nullptr || *s == '\0' || *s == '-') return false;
  char* end = nullptr;
  out = std::strtoull(s, &end, 10);
  return *end == '\0';
}

}  // namespace

int main(int argc, char** argv) {
  feed::UdpReceiverConfig cfg;
  cfg.pipeline.full_policy = feed::QueueFullPolicy::DropAndCount;
  std::string port_file, event_log_path, packet_log_path;
  std::uint64_t packet_log_max = 1000;
  feedcli::StateCli scli;

  for (int i = 1; i < argc; ++i) {
    const char* a = argv[i];
    const char* v = (i + 1 < argc) ? argv[i + 1] : nullptr;
    std::uint64_t n = 0;
    bool ok = true;
    if (const auto fr = feedcli::parse_state_flag(argc, argv, i, cfg.pipeline, scli); fr != feedcli::Flag::NotMine) {
      if (fr == feedcli::Flag::Bad) { std::fprintf(stderr, "error: bad or incomplete argument '%s'\n", a); usage(argv[0]); return 2; }
      continue;
    }
    if (std::strcmp(a, "--join-mid-stream") == 0) { cfg.pipeline.sequencer.join_mid_stream = true; continue; }
    if (v == nullptr) { std::fprintf(stderr, "error: '%s' needs a value\n", a); usage(argv[0]); return 2; }
    ++i;
    if (std::strcmp(a, "--host") == 0) cfg.host = v;
    else if (std::strcmp(a, "--port") == 0) { ok = parse_u64(v, n) && n <= 65535; cfg.port = static_cast<std::uint16_t>(n); }
    else if (std::strcmp(a, "--port-file") == 0) port_file = v;
    else if (std::strcmp(a, "--recv-mode") == 0) {
      if (std::strcmp(v, "poll") == 0) cfg.mode = feed::RecvMode::Poll;
      else if (std::strcmp(v, "busy") == 0) cfg.mode = feed::RecvMode::BusyPoll;
      else ok = false;
    }
    else if (std::strcmp(a, "--rcvbuf") == 0) { ok = parse_u64(v, n) && n >= 1 && n <= 1'000'000'000ULL; cfg.rcvbuf_bytes = static_cast<int>(n); }
    else if (std::strcmp(a, "--max-datagram") == 0) { ok = parse_u64(v, n) && n >= 1 && n <= 65535; cfg.max_datagram = static_cast<std::size_t>(n); }
    else if (std::strcmp(a, "--duration-ms") == 0) { ok = parse_u64(v, n); cfg.duration_ms = n; }
    else if (std::strcmp(a, "--idle-exit-ms") == 0) { ok = parse_u64(v, n); cfg.idle_exit_ms = n; }
    else if (std::strcmp(a, "--queue-capacity") == 0) { ok = parse_u64(v, n) && n >= 1 && n <= feed::SpscQueue<feed::QueuedEvent>::kMaxCapacity; cfg.pipeline.queue_capacity = static_cast<std::size_t>(n); }
    else if (std::strcmp(a, "--full-policy") == 0) {
      if (std::strcmp(v, "drop") == 0) cfg.pipeline.full_policy = feed::QueueFullPolicy::DropAndCount;
      else if (std::strcmp(v, "fail") == 0) cfg.pipeline.full_policy = feed::QueueFullPolicy::FailFast;
      else { std::fprintf(stderr, "error: --full-policy must be drop or fail for a network source\n"); return 2; }
    }
    else if (std::strcmp(a, "--consumer-work-ns") == 0) { ok = parse_u64(v, n); cfg.pipeline.consumer_work_ns = n; }
    else if (std::strcmp(a, "--gap-policy") == 0) {
      if (std::strcmp(v, "degrade") == 0) cfg.pipeline.sequencer.gap_policy = feed::GapPolicy::MarkDegraded;
      else if (std::strcmp(v, "pause") == 0) cfg.pipeline.sequencer.gap_policy = feed::GapPolicy::Pause;
      else ok = false;
    }
    else if (std::strcmp(a, "--late-policy") == 0) {
      if (std::strcmp(v, "discard") == 0) cfg.pipeline.sequencer.late_policy = feed::LatePolicy::Discard;
      else if (std::strcmp(v, "forward") == 0) cfg.pipeline.sequencer.late_policy = feed::LatePolicy::Forward;
      else ok = false;
    }
    else if (std::strcmp(a, "--event-log") == 0) event_log_path = v;
    else if (std::strcmp(a, "--packet-log") == 0) packet_log_path = v;
    else if (std::strcmp(a, "--packet-log-max") == 0) ok = parse_u64(v, packet_log_max);
    else ok = false;
    if (!ok) { std::fprintf(stderr, "error: bad argument '%s %s'\n", a, v); usage(argv[0]); return 2; }
  }

  std::string state_error;
  if (!feedcli::finalize_state(cfg.pipeline, scli, state_error)) { std::fprintf(stderr, "error: %s\n", state_error.c_str()); return 1; }
  if (std::string verr; !cfg.pipeline.validate(verr)) { std::fprintf(stderr, "error: %s\n", verr.c_str()); return 2; }

  std::FILE* event_log = nullptr;
  std::FILE* packet_log = nullptr;
  if (!event_log_path.empty() && (event_log = std::fopen(event_log_path.c_str(), "w")) == nullptr) {
    std::fprintf(stderr, "error: cannot write '%s'\n", event_log_path.c_str());
    return 1;
  }
  if (!packet_log_path.empty() && (packet_log = std::fopen(packet_log_path.c_str(), "w")) == nullptr) {
    std::fprintf(stderr, "error: cannot write '%s'\n", packet_log_path.c_str());
    return 1;
  }
  if (event_log != nullptr || packet_log != nullptr) {
    cfg.observer = [&](std::uint64_t arrival, std::uint64_t t_rx, std::span<const std::uint8_t> bytes, std::size_t size,
                       const feed::IngestResult& r, bool truncated) {
      if (event_log != nullptr) feed::log_event(event_log, arrival, r);
      if (packet_log != nullptr && arrival <= packet_log_max) feed::log_packet(packet_log, arrival, t_rx, bytes, size, r, truncated);
    };
  }

  feed::UdpReceiver receiver(cfg);
  std::string error;
  if (!receiver.open(error)) {
    std::fprintf(stderr, "error: %s\n", error.c_str());
    return 1;
  }
  if (!port_file.empty()) {
    std::FILE* pf = std::fopen(port_file.c_str(), "w");
    if (pf == nullptr || std::fprintf(pf, "%u\n", static_cast<unsigned>(receiver.port())) < 0 || std::fclose(pf) != 0) {
      std::fprintf(stderr, "error: cannot write '%s'\n", port_file.c_str());
      return 1;
    }
  }
  std::fprintf(stderr, "listening on 127.0.0.1:%u (mode %s); SIGINT/SIGTERM for graceful shutdown\n",
               static_cast<unsigned>(receiver.port()), feed::to_string(cfg.mode));

  g_receiver = &receiver;
  struct sigaction sa {};
  sa.sa_handler = on_signal;
  sigemptyset(&sa.sa_mask);
  sigaction(SIGINT, &sa, nullptr);
  sigaction(SIGTERM, &sa, nullptr);

  const feed::UdpReport r = receiver.run();
  g_receiver = nullptr;
  if (event_log != nullptr) std::fclose(event_log);
  if (packet_log != nullptr) std::fclose(packet_log);
  if (!scli.timeline_out.empty() && !feed::write_timeline_csv(scli.timeline_out.c_str(), r.pipeline.timeline)) {
    std::fprintf(stderr, "error: cannot write '%s'\n", scli.timeline_out.c_str());
    return 1;
  }

  const auto& s = r.socket;
  std::printf("{\n");
  std::printf("  \"port\": %u,\n  \"recv_mode\": \"%s\",\n  \"shutdown\": \"%s\",\n  \"elapsed_ns\": %llu,\n", static_cast<unsigned>(r.port),
              feed::to_string(cfg.mode), feed::to_string(r.reason), static_cast<unsigned long long>(r.elapsed_ns));
  std::printf("  \"policy\": {\"gap\": \"%s\", \"late\": \"%s\", \"queue_full\": \"%s\", \"consumer_work_ns\": %llu},\n",
              feed::to_string(cfg.pipeline.sequencer.gap_policy), feed::to_string(cfg.pipeline.sequencer.late_policy),
              feed::to_string(cfg.pipeline.full_policy), static_cast<unsigned long long>(cfg.pipeline.consumer_work_ns));
  std::printf("  \"socket\": {\"datagrams\": %llu, \"bytes\": %llu, \"truncated\": %llu, \"recv_errors\": %llu, "
              "\"discarded_at_shutdown\": %llu, \"kernel_drops\": %llu, \"kernel_drops_source\": \"%s\", \"rcvbuf_effective\": %d, \"max_batch\": %llu, "
              "\"poll_timeouts\": %llu, \"wakeups\": %llu, \"empty_polls\": %llu},\n",
              static_cast<unsigned long long>(s.datagrams), static_cast<unsigned long long>(s.bytes),
              static_cast<unsigned long long>(s.truncated), static_cast<unsigned long long>(s.recv_errors),
              static_cast<unsigned long long>(s.discarded_at_shutdown), static_cast<unsigned long long>(s.kernel_drops), s.kernel_drops_from_proc ? "proc_net_udp" : "so_rxq_ovfl", s.rcvbuf_effective,
              static_cast<unsigned long long>(s.max_batch), static_cast<unsigned long long>(s.poll_timeouts),
              static_cast<unsigned long long>(s.wakeups), static_cast<unsigned long long>(s.empty_polls));
  feed::print_pipeline_sections(stdout, r.pipeline);
  std::printf("  \"udp_checks\": {\"socket_consistent\": %s, \"balanced\": %s}\n}\n", r.socket_consistent() ? "true" : "false",
              r.balanced() ? "true" : "false");

  if (!r.balanced()) return 4;
  if (r.reason == feed::ShutdownReason::FailFast) return 3;
  return 0;
}
