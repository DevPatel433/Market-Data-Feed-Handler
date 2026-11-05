// feed_gen_events: write a deterministic synthetic replay file plus a manifest.
// Exit codes: 0 ok, 1 I/O error, 2 usage error.
#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "feed/workload.hpp"

namespace {

void usage(const char* argv0) {
  std::fprintf(stderr,
               "Usage: %s --out FILE [options]\n"
               "  --profile P        normal|bursty|malformed|duplicate|gap|reorder (default normal)\n"
               "  --seed N           PRNG seed (default 1)\n"
               "  --count N          clean messages before faults (default 10000)\n"
               "  --rate N           mean messages/second (default 100000)\n"
               "  --stream-id N      stream id (default 1)\n"
               "  --instruments N    instrument universe size (default 16)\n"
               "  --burst-size N     bursty: messages per burst (default 1000)\n"
               "  --burst-mult N     bursty: in-burst rate multiplier (default 20)\n"
               "  --fault-ppm N      fault probability per message, parts per million (default 10000)\n"
               "  --manifest FILE    also write the manifest JSON to FILE\n"
               "  --frames-dir DIR   also write the first --frames-max frames as DIR/frame_NNNNN.bin (fuzz corpus; DIR must exist)\n"
               "  --frames-max N     number of frames to export (default 256)\n"
               "The manifest is always printed to stdout.\n",
               argv0);
}

bool parse_u64(const char* s, std::uint64_t& out) {
  if (s == nullptr || *s == '\0' || *s == '-') return false;
  char* end = nullptr;
  out = std::strtoull(s, &end, 10);
  return end != nullptr && *end == '\0';
}

}  // namespace

int main(int argc, char** argv) {
  feed::WorkloadConfig cfg;
  std::string out_path;
  std::string manifest_path;
  std::string frames_dir;
  std::uint64_t frames_max = 256;

  for (int i = 1; i < argc; ++i) {
    const char* a = argv[i];
    const auto value = [&]() -> const char* { return (i + 1 < argc) ? argv[++i] : nullptr; };
    std::uint64_t n = 0;
    if (std::strcmp(a, "--out") == 0) {
      const char* v = value();
      if (v == nullptr) { usage(argv[0]); return 2; }
      out_path = v;
    } else if (std::strcmp(a, "--manifest") == 0) {
      const char* v = value();
      if (v == nullptr) { usage(argv[0]); return 2; }
      manifest_path = v;
    } else if (std::strcmp(a, "--frames-dir") == 0) {
      const char* v = value();
      if (v == nullptr) { usage(argv[0]); return 2; }
      frames_dir = v;
    } else if (std::strcmp(a, "--frames-max") == 0 && parse_u64(value(), n)) {
      frames_max = n;
    } else if (std::strcmp(a, "--profile") == 0) {
      const char* v = value();
      const auto p = v ? feed::parse_profile(v) : std::nullopt;
      if (!p) { usage(argv[0]); return 2; }
      cfg.profile = *p;
    } else if (std::strcmp(a, "--seed") == 0 && parse_u64(value(), n)) {
      cfg.seed = n;
    } else if (std::strcmp(a, "--count") == 0 && parse_u64(value(), n)) {
      cfg.count = n;
    } else if (std::strcmp(a, "--rate") == 0 && parse_u64(value(), n) && n >= 1) {
      cfg.rate_per_sec = n;
    } else if (std::strcmp(a, "--stream-id") == 0 && parse_u64(value(), n) && n <= 0xFFFFFFFFULL) {
      cfg.stream_id = static_cast<std::uint32_t>(n);
    } else if (std::strcmp(a, "--instruments") == 0 && parse_u64(value(), n) && n >= 1 && n <= 0xFFFFFFFFULL) {
      cfg.instruments = static_cast<std::uint32_t>(n);
    } else if (std::strcmp(a, "--burst-size") == 0 && parse_u64(value(), n) && n >= 1 && n <= 0xFFFFFFFFULL) {
      cfg.burst_size = static_cast<std::uint32_t>(n);
    } else if (std::strcmp(a, "--burst-mult") == 0 && parse_u64(value(), n) && n >= 1 && n <= 0xFFFFFFFFULL) {
      cfg.burst_multiplier = static_cast<std::uint32_t>(n);
    } else if (std::strcmp(a, "--fault-ppm") == 0 && parse_u64(value(), n) && n <= 1'000'000) {
      cfg.fault_ppm = static_cast<std::uint32_t>(n);
    } else {
      std::fprintf(stderr, "error: bad or incomplete argument '%s'\n", a);
      usage(argv[0]);
      return 2;
    }
  }
  if (out_path.empty()) { usage(argv[0]); return 2; }

  const feed::Workload wl = feed::generate_workload(cfg);
  std::vector<std::uint8_t> image;
  if (!feed::serialize_replay(wl.records, image) || !feed::write_file(out_path, image)) {
    std::fprintf(stderr, "error: cannot write '%s'\n", out_path.c_str());
    return 1;
  }
  if (!frames_dir.empty()) {
    const std::size_t limit = static_cast<std::size_t>(std::min<std::uint64_t>(frames_max, wl.records.size()));
    for (std::size_t i = 0; i < limit; ++i) {
      char name[32];
      std::snprintf(name, sizeof name, "/frame_%05zu.bin", i);
      if (!feed::write_file(frames_dir + name, wl.records[i].frame)) {
        std::fprintf(stderr, "error: cannot write frame into '%s'\n", frames_dir.c_str());
        return 1;
      }
    }
  }
  const std::string manifest = feed::manifest_json(cfg, wl.stats);
  std::fputs(manifest.c_str(), stdout);
  if (!manifest_path.empty()) {
    const std::span<const std::uint8_t> bytes(reinterpret_cast<const std::uint8_t*>(manifest.data()), manifest.size());
    if (!feed::write_file(manifest_path, bytes)) {
      std::fprintf(stderr, "error: cannot write '%s'\n", manifest_path.c_str());
      return 1;
    }
  }
  return 0;
}
