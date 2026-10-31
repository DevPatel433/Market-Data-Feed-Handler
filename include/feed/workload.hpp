#pragma once
// Deterministic synthetic workload generation. Normative description: docs/workloads.md.
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "feed/replay_file.hpp"

namespace feed {

enum class Profile { Normal, Bursty, Malformed, Duplicate, Gap, Reorder };

std::string_view to_string(Profile p);
std::optional<Profile> parse_profile(std::string_view s);

struct WorkloadConfig {
  Profile profile = Profile::Normal;
  std::uint64_t seed = 1;
  std::uint64_t count = 10'000;          // clean messages before fault injection
  std::uint64_t rate_per_sec = 100'000;  // mean emission rate
  std::uint32_t stream_id = 1;
  std::uint32_t instruments = 16;
  std::uint32_t burst_size = 1'000;      // bursty: messages per burst
  std::uint32_t burst_multiplier = 20;   // bursty: in-burst rate = multiplier * mean rate
  std::uint32_t fault_ppm = 10'000;      // fault probability per message (10,000 = 1%)
};

// Ground truth about what the generator did. Receivers' counters in later
// phases should be checkable against this manifest.
struct FaultStats {
  std::uint64_t clean_messages = 0;
  std::uint64_t frames_out = 0;
  std::uint64_t duplicated = 0;  // extra copies inserted
  std::uint64_t dropped = 0;     // frames removed (sequence gaps)
  std::uint64_t reordered = 0;   // adjacent swaps performed
  std::uint64_t malformed = 0;   // frames corrupted
};

struct Workload {
  std::vector<ReplayRecord> records;
  FaultStats stats;
};

Workload generate_workload(const WorkloadConfig& cfg);

// Scheduled emission time (ns from the start) of the i-th clean message: constant pacing, or the bursty schedule.
std::uint64_t scheduled_offset_ns(const WorkloadConfig& cfg, std::uint64_t i);

std::string manifest_json(const WorkloadConfig& cfg, const FaultStats& stats);

}  // namespace feed
