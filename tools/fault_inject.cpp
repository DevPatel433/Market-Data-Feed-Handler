// feed_fault_inject: apply drop/duplicate/reorder faults to a replay file.
// Exit codes: 0 ok, 1 I/O or input error, 2 usage error.
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "feed/fault_injector.hpp"
#include "feed/replay_file.hpp"

namespace {

void usage(const char* argv0) {
  std::fprintf(stderr,
               "Usage: %s --in FILE --out FILE [options]\n"
               "  --seed N               PRNG seed (default 1)\n"
               "  --drop-ppm N           chance per frame of starting a dropped run, parts per million\n"
               "  --drop-burst-max N     dropped run length is 1..N (default 3)\n"
               "  --dup-ppm N            chance per frame of being sent twice\n"
               "  --reorder-ppm N        chance per frame of being delayed\n"
               "  --reorder-max N        a delayed frame moves 1..N slots later (default 3)\n"
               "  --allow-drop-first     allow dropping the first frame\n"
               "  --manifest FILE        also write the report JSON to FILE\n"
               "The report is always printed to stdout.\n",
               argv0);
}

bool parse_u32(const char* s, std::uint32_t& out) {
  if (s == nullptr || *s == '\0' || *s == '-') return false;
  char* end = nullptr;
  const unsigned long long v = std::strtoull(s, &end, 10);
  if (*end != '\0' || v > 0xFFFFFFFFULL) return false;
  out = static_cast<std::uint32_t>(v);
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  feed::FaultConfig cfg;
  std::string in_path, out_path, manifest_path;

  for (int i = 1; i < argc; ++i) {
    const char* a = argv[i];
    const char* v = (i + 1 < argc) ? argv[i + 1] : nullptr;
    std::uint32_t n = 0;
    const auto num = [&](std::uint32_t& dst, std::uint32_t max) {
      if (!parse_u32(v, n) || n > max) return false;
      dst = n;
      ++i;
      return true;
    };
    bool ok = true;
    if (std::strcmp(a, "--in") == 0 && v) { in_path = v; ++i; }
    else if (std::strcmp(a, "--out") == 0 && v) { out_path = v; ++i; }
    else if (std::strcmp(a, "--manifest") == 0 && v) { manifest_path = v; ++i; }
    else if (std::strcmp(a, "--allow-drop-first") == 0) { cfg.protect_first = false; }
    else if (std::strcmp(a, "--seed") == 0 && v) {
      char* end = nullptr;
      cfg.seed = std::strtoull(v, &end, 10);
      ok = (*end == '\0');
      ++i;
    }
    else if (std::strcmp(a, "--drop-ppm") == 0) ok = num(cfg.drop_ppm, 1'000'000);
    else if (std::strcmp(a, "--drop-burst-max") == 0) ok = num(cfg.drop_burst_max, 0xFFFFFFFFU);
    else if (std::strcmp(a, "--dup-ppm") == 0) ok = num(cfg.duplicate_ppm, 1'000'000);
    else if (std::strcmp(a, "--reorder-ppm") == 0) ok = num(cfg.reorder_ppm, 1'000'000);
    else if (std::strcmp(a, "--reorder-max") == 0) ok = num(cfg.reorder_distance_max, 0xFFFFFFFFU);
    else ok = false;
    if (!ok) {
      std::fprintf(stderr, "error: bad or incomplete argument '%s'\n", a);
      usage(argv[0]);
      return 2;
    }
  }
  if (in_path.empty() || out_path.empty()) { usage(argv[0]); return 2; }

  std::vector<std::uint8_t> image;
  if (!feed::read_file(in_path, image)) {
    std::fprintf(stderr, "error: cannot read '%s'\n", in_path.c_str());
    return 1;
  }
  std::vector<feed::ReplayRecord> records;
  if (const auto err = feed::parse_replay(image, records); err != feed::ReplayError::None) {
    std::fprintf(stderr, "error: invalid replay file '%s': %s\n", in_path.c_str(), feed::to_string(err));
    return 1;
  }

  feed::FaultReport report;
  const auto faulted = feed::inject_faults(records, cfg, report);
  std::vector<std::uint8_t> out;
  if (!feed::serialize_replay(faulted, out) || !feed::write_file(out_path, out)) {
    std::fprintf(stderr, "error: cannot write '%s'\n", out_path.c_str());
    return 1;
  }
  const std::string json = feed::fault_report_json(cfg, report);
  std::fputs(json.c_str(), stdout);
  if (!manifest_path.empty()) {
    const std::span<const std::uint8_t> bytes(reinterpret_cast<const std::uint8_t*>(json.data()), json.size());
    if (!feed::write_file(manifest_path, bytes)) {
      std::fprintf(stderr, "error: cannot write '%s'\n", manifest_path.c_str());
      return 1;
    }
  }
  return 0;
}
