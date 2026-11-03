// feed_udp_send: send the frames of a replay file as UDP datagrams to a LOOPBACK receiver.
// Exit codes: 0 ok, 1 input/setup error, 2 usage error, 3 some datagrams failed to send.
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "feed/replay_file.hpp"
#include "feed/udp_sender.hpp"

namespace {

void usage(const char* argv0) {
  std::fprintf(stderr,
               "Usage: %s --input FILE --port N [options]\n"
               "  --host H             loopback address (default 127.0.0.1; non-loopback is refused)\n"
               "  --mode rate|timed|max  rate: fixed --rate (default); timed: recorded offsets / --speed; max: no pacing\n"
               "  --rate N             datagrams per second in rate mode (default 10000)\n"
               "  --burst-size N       rate mode: send N datagrams back-to-back, then wait for the next slot (default 1)\n"
               "  --speed X            timed mode multiplier (default 1)\n"
               "  --duration-ms N      stop sending after N ms (default: send the whole file)\n"
               "  --junk-ppm N         probability per frame (parts per million) of an EXTRA invalid datagram first\n"
               "  --junk-size N        size in bytes of each junk datagram (default 64; 0 = empty datagram)\n"
               "  --seed N             PRNG seed for junk (default 1)\n"
               "  --sndbuf BYTES       requested SO_SNDBUF\n",
               argv0);
}

bool parse_u64(const char* s, std::uint64_t& out) {
  if (s == nullptr || *s == '\0' || *s == '-') return false;
  char* end = nullptr;
  out = std::strtoull(s, &end, 10);
  return *end == '\0';
}

}  // namespace

int main(int argc, char** argv) {
  feed::UdpSenderConfig cfg;
  std::string input;
  for (int i = 1; i < argc; ++i) {
    const char* a = argv[i];
    const char* v = (i + 1 < argc) ? argv[i + 1] : nullptr;
    std::uint64_t n = 0;
    bool ok = true;
    if (v == nullptr) { std::fprintf(stderr, "error: '%s' needs a value\n", a); usage(argv[0]); return 2; }
    ++i;
    if (std::strcmp(a, "--input") == 0) input = v;
    else if (std::strcmp(a, "--host") == 0) cfg.host = v;
    else if (std::strcmp(a, "--port") == 0) { ok = parse_u64(v, n) && n >= 1 && n <= 65535; cfg.port = static_cast<std::uint16_t>(n); }
    else if (std::strcmp(a, "--mode") == 0) {
      if (std::strcmp(v, "rate") == 0) cfg.mode = feed::SendMode::Rate;
      else if (std::strcmp(v, "timed") == 0) cfg.mode = feed::SendMode::Timed;
      else if (std::strcmp(v, "max") == 0) cfg.mode = feed::SendMode::MaxSpeed;
      else ok = false;
    }
    else if (std::strcmp(a, "--rate") == 0) { ok = parse_u64(v, n) && n >= 1; cfg.rate_per_sec = n; }
    else if (std::strcmp(a, "--burst-size") == 0) { ok = parse_u64(v, n) && n >= 1 && n <= 1'000'000; cfg.burst_size = static_cast<std::uint32_t>(n); }
    else if (std::strcmp(a, "--speed") == 0) { char* end = nullptr; cfg.speed = std::strtod(v, &end); ok = end != v && *end == '\0' && cfg.speed > 0.0; }
    else if (std::strcmp(a, "--duration-ms") == 0) { ok = parse_u64(v, n); cfg.duration_ms = n; }
    else if (std::strcmp(a, "--junk-ppm") == 0) { ok = parse_u64(v, n) && n <= 1'000'000; cfg.junk_ppm = static_cast<std::uint32_t>(n); }
    else if (std::strcmp(a, "--junk-size") == 0) { ok = parse_u64(v, n) && n <= 65'507; cfg.junk_size = static_cast<std::size_t>(n); }
    else if (std::strcmp(a, "--seed") == 0) { ok = parse_u64(v, n); cfg.seed = n; }
    else if (std::strcmp(a, "--sndbuf") == 0) { ok = parse_u64(v, n) && n >= 1 && n <= 1'000'000'000ULL; cfg.sndbuf_bytes = static_cast<int>(n); }
    else ok = false;
    if (!ok) { std::fprintf(stderr, "error: bad argument '%s %s'\n", a, v); usage(argv[0]); return 2; }
  }
  if (input.empty() || cfg.port == 0) { usage(argv[0]); return 2; }

  std::vector<std::uint8_t> image;
  if (!feed::read_file(input, image)) {
    std::fprintf(stderr, "error: cannot read '%s'\n", input.c_str());
    return 1;
  }
  std::vector<feed::ReplayRecord> records;
  if (const auto err = feed::parse_replay(image, records); err != feed::ReplayError::None) {
    std::fprintf(stderr, "error: invalid replay file '%s': %s\n", input.c_str(), feed::to_string(err));
    return 1;
  }

  feed::UdpSendReport r;
  std::string error;
  if (!feed::send_records(records, cfg, r, error)) {
    std::fprintf(stderr, "error: %s\n", error.c_str());
    return 1;
  }
  const double secs = static_cast<double>(r.wall_ns) / 1e9;
  const char* mode = cfg.mode == feed::SendMode::Rate ? "rate" : cfg.mode == feed::SendMode::Timed ? "timed" : "max";
  std::printf("{\n  \"destination\": \"%s:%u\",\n  \"mode\": \"%s\",\n", cfg.host.c_str(), static_cast<unsigned>(cfg.port), mode);
  std::printf("  \"frames_in_file\": %" PRIu64 ",\n  \"sent\": %" PRIu64 ",\n  \"bytes\": %" PRIu64 ",\n  \"junk_sent\": %" PRIu64 ",\n", r.frames_in_file,
              r.sent, r.bytes, r.junk_sent);
  std::printf("  \"send_errors\": %" PRIu64 ",\n  \"not_sent\": %" PRIu64 ",\n  \"stopped_by_duration\": %s,\n", r.send_errors, r.not_sent,
              r.stopped_by_duration ? "true" : "false");
  std::printf("  \"wall_ns\": %" PRIu64 ",\n  \"max_lateness_ns\": %" PRIu64 ",\n  \"achieved_datagrams_per_sec\": %.0f\n}\n", r.wall_ns,
              r.max_lateness_ns, secs > 0.0 ? static_cast<double>(r.sent + r.junk_sent) / secs : 0.0);
  if (r.send_errors > 0) {
    std::fprintf(stderr, "warning: %" PRIu64 " send error(s); first: %s\n", r.send_errors, r.first_error.c_str());
    return 3;
  }
  return 0;
}
