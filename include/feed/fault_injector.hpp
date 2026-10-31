#pragma once
// Post-hoc fault injection on an existing replay stream: drop, duplicate and
// reorder frames. Deterministic: same input + same config => same output.
// Each fault type has its own RNG stream and passes run in order (drop, duplicate,
// reorder), so enabling a later pass never changes what an earlier pass chose.
#include <cstdint>
#include <string>
#include <vector>

#include "feed/replay_file.hpp"

namespace feed {

struct FaultConfig {
  std::uint64_t seed = 1;
  std::uint32_t drop_ppm = 0;               // chance a frame starts a dropped run
  std::uint32_t drop_burst_max = 3;         // dropped run length is 1..drop_burst_max
  std::uint32_t duplicate_ppm = 0;          // chance a frame is sent twice in a row
  std::uint32_t reorder_ppm = 0;            // chance a frame is delayed
  std::uint32_t reorder_distance_max = 3;   // a delayed frame moves 1..max positions later
  bool protect_first = true;                // never drop the very first frame
};

struct FaultReport {
  std::uint64_t frames_in = 0;
  std::uint64_t frames_out = 0;
  std::uint64_t dropped = 0;     // frames removed
  std::uint64_t duplicated = 0;  // extra copies inserted
  std::uint64_t reordered = 0;   // frames moved later (each frame moves at most once)
};

// Offsets stay attached to their positions (slots), not to frames: a delayed
// frame is emitted at its new slot's time, so offsets remain non-decreasing.
// A duplicate shares its original's offset (re-sent immediately).
std::vector<ReplayRecord> inject_faults(const std::vector<ReplayRecord>& in, const FaultConfig& cfg,
                                        FaultReport& report);

std::string fault_report_json(const FaultConfig& cfg, const FaultReport& report);

}  // namespace feed
