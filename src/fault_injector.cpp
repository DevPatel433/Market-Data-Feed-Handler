#include "feed/fault_injector.hpp"

#include <algorithm>
#include <cinttypes>
#include <cstdio>

#include "feed/rng.hpp"

namespace feed {

std::vector<ReplayRecord> inject_faults(const std::vector<ReplayRecord>& in, const FaultConfig& cfg,
                                        FaultReport& report) {
  report = {};
  report.frames_in = in.size();

  SplitMix64 drop_rng(cfg.seed ^ 0xA0761D6478BD642FULL);
  SplitMix64 dup_rng(cfg.seed ^ 0xE7037ED1A0B428DBULL);
  SplitMix64 reorder_rng(cfg.seed ^ 0x8EBC6AF09C88C6E3ULL);

  // Pass 1: drop runs.
  std::vector<ReplayRecord> cur;
  cur.reserve(in.size());
  const std::uint64_t burst_max = std::max<std::uint32_t>(1, cfg.drop_burst_max);
  for (std::size_t i = 0; i < in.size();) {
    const bool eligible = i > 0 || !cfg.protect_first;
    if (eligible && drop_rng.chance_ppm(cfg.drop_ppm)) {
      const std::size_t run = std::min<std::size_t>(1 + drop_rng.uniform(burst_max), in.size() - i);
      report.dropped += run;
      i += run;
    } else {
      cur.push_back(in[i]);
      ++i;
    }
  }

  // Pass 2: duplicates (copy shares the original's offset).
  std::vector<ReplayRecord> dup;
  dup.reserve(cur.size() + cur.size() / 20);
  for (auto& rec : cur) {
    const bool twice = dup_rng.chance_ppm(cfg.duplicate_ppm);
    dup.push_back(rec);
    if (twice) {
      dup.push_back(rec);
      ++report.duplicated;
    }
  }

  // Pass 3: delay frames. Offsets stay in their slots; only frames move.
  const std::size_t n = dup.size();
  std::vector<std::vector<std::uint8_t>> frames(n);
  for (std::size_t i = 0; i < n; ++i) frames[i] = std::move(dup[i].frame);
  const std::uint64_t dist_max = std::max<std::uint32_t>(1, cfg.reorder_distance_max);
  for (std::size_t i = 0; i + 1 < n;) {
    if (reorder_rng.chance_ppm(cfg.reorder_ppm)) {
      const std::size_t k = std::min<std::size_t>(1 + reorder_rng.uniform(dist_max), n - 1 - i);
      const auto first = frames.begin() + static_cast<std::ptrdiff_t>(i);
      std::rotate(first, first + 1, first + static_cast<std::ptrdiff_t>(k) + 1);  // frame i -> slot i+k
      ++report.reordered;
      i += k + 1;  // every displaced frame is final: nothing moves twice
    } else {
      ++i;
    }
  }
  for (std::size_t i = 0; i < n; ++i) dup[i].frame = std::move(frames[i]);

  report.frames_out = dup.size();
  return dup;
}

std::string fault_report_json(const FaultConfig& cfg, const FaultReport& r) {
  char buf[640];
  std::snprintf(buf, sizeof buf,
                "{\n"
                "  \"seed\": %" PRIu64 ",\n"
                "  \"drop_ppm\": %" PRIu32 ",\n"
                "  \"drop_burst_max\": %" PRIu32 ",\n"
                "  \"duplicate_ppm\": %" PRIu32 ",\n"
                "  \"reorder_ppm\": %" PRIu32 ",\n"
                "  \"reorder_distance_max\": %" PRIu32 ",\n"
                "  \"frames_in\": %" PRIu64 ",\n"
                "  \"frames_out\": %" PRIu64 ",\n"
                "  \"dropped\": %" PRIu64 ",\n"
                "  \"duplicated\": %" PRIu64 ",\n"
                "  \"reordered\": %" PRIu64 "\n"
                "}\n",
                cfg.seed, cfg.drop_ppm, cfg.drop_burst_max, cfg.duplicate_ppm, cfg.reorder_ppm,
                cfg.reorder_distance_max, r.frames_in, r.frames_out, r.dropped, r.duplicated, r.reordered);
  return buf;
}

}  // namespace feed
