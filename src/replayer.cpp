#include "feed/replayer.hpp"

#include <chrono>
#include <thread>

namespace feed {

namespace {
using Clock = std::chrono::steady_clock;  // monotonic; see docs/metrics.md

void wait_until(Clock::time_point target) {
  using namespace std::chrono_literals;
  const auto remaining = target - Clock::now();
  if (remaining > 200us) std::this_thread::sleep_for(remaining - 100us);  // coarse sleep
  while (Clock::now() < target) std::this_thread::yield();                 // fine wait; yielding keeps a one-core consumer running
}
}  // namespace

ReplayStats replay(const std::vector<ReplayRecord>& records, const ReplayOptions& opts, const FrameSink& sink) {
  ReplayStats st;
  const double speed = opts.speed > 0.0 ? opts.speed : 1.0;
  const auto start = Clock::now();
  auto last = start;

  const std::uint64_t rate = opts.rate_per_sec > 0 ? opts.rate_per_sec : 1;
  std::uint64_t index = 0;
  for (const auto& rec : records) {
    if (opts.stop != nullptr && opts.stop->load(std::memory_order_relaxed)) {
      st.stopped_early = true;
      break;
    }
    if (opts.mode != ReplayMode::MaxSpeed) {
      std::uint64_t scaled = 0;
      if (opts.mode == ReplayMode::Timed) {
        scaled = static_cast<std::uint64_t>(static_cast<double>(rec.offset_ns) / speed);
      } else {
        scaled = index * 1'000'000'000ULL / rate;
      }
      ++index;
      const auto target = start + std::chrono::nanoseconds(scaled);
      wait_until(target);
      const auto now = Clock::now();
      if (now > target) {
        const auto late = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now - target).count());
        if (late > st.max_lateness_ns) st.max_lateness_ns = late;
      }
    }
    sink(rec.frame);
    ++st.frames;
    st.bytes += rec.frame.size();
    last = Clock::now();
  }
  st.wall_ns = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(last - start).count());
  return st;
}

}  // namespace feed
