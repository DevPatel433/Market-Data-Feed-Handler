#pragma once
// Plays a parsed replay file into a sink, either as fast as possible or
// honouring the recorded offsets (optionally scaled by `speed`).
#include <atomic>
#include <cstdint>
#include <functional>
#include <span>
#include <vector>

#include "feed/replay_file.hpp"

namespace feed {

enum class ReplayMode { MaxSpeed, Timed, Rate };

struct ReplayOptions {
  ReplayMode mode = ReplayMode::MaxSpeed;
  double speed = 1.0;  // Timed only: 2.0 plays twice as fast. Values <= 0 are treated as 1.0.
  std::uint64_t rate_per_sec = 100'000;  // Rate only: frame i is emitted at i / rate, ignoring recorded offsets.
  const std::atomic<bool>* stop = nullptr;  // optional: checked before every frame; true ends the replay early
};

struct ReplayStats {
  std::uint64_t frames = 0;
  std::uint64_t bytes = 0;
  std::uint64_t wall_ns = 0;          // first emit scheduling to last emit
  bool stopped_early = false;         // ended because opts.stop was set
  std::uint64_t max_lateness_ns = 0;  // Timed/Rate only: worst (actual - scheduled) emission time
};

using FrameSink = std::function<void(std::span<const std::uint8_t>)>;

ReplayStats replay(const std::vector<ReplayRecord>& records, const ReplayOptions& opts, const FrameSink& sink);

}  // namespace feed
