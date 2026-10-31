#pragma once
// Deterministic model of the producer -> bounded queue -> consumer pipeline under overload.
// Normative description: docs/overload_and_recovery.md §1-§2.
//
// Why a model at all: real producer/consumer timing depends on scheduling, core count and clocks, so
// two runs (let alone two machines) disagree. The model takes the scheduled arrival times and a fixed
// service time and computes the exact consequences of each queue-full policy, reproducibly. The real
// pipeline is then checked against it qualitatively (tests, `feed_overload --real`).
//
// Semantics match feed::SpscQueue: the consumer POPS an item (freeing its slot at once) and then spends
// `service_ns` working on it before it can pop the next. So the item being worked on is not in the queue.
#include <cstddef>
#include <cstdint>
#include <vector>

#include "feed/queue_policy.hpp"

namespace feed {

// Queue occupancy as a step function: `occupancy` holds from `t_ns` until the next point.
struct OccPoint {
  std::uint64_t t_ns = 0;
  std::uint64_t occupancy = 0;
};

// Time to steady state, independent of queue capacity (a big queue that absorbs a deep backlog still takes
// a long time to drain it, and that is exactly what this must show):
//   peak       = highest occupancy in the series
//   congested  = a backlog formed: peak >= min(capacity, 2)
//   peak_at    = the last moment occupancy was at its peak (for identical periodic bursts: the end of the last burst's fill)
//   steady     = occupancy <= low_mark (the consumer has caught up: at most 1 item waiting)
//   time_to_steady = first steady moment at or after peak_at, minus peak_at
struct SteadyState {
  std::uint64_t peak = 0;
  std::uint64_t low_mark = 0;
  bool congested = false;
  bool steady_reached = false;      // false if the series ends while the backlog is still above the low mark
  std::uint64_t peak_at_ns = 0;
  std::uint64_t steady_at_ns = 0;
  std::uint64_t time_to_steady_ns = 0;  // 0 if never congested
};

// Works for both the model's exact change-points and the real pipeline's sampled timeline.
SteadyState analyze_occupancy(const std::vector<OccPoint>& points, std::size_t capacity, std::uint64_t end_ns);

struct Distribution {
  std::uint64_t count = 0;
  std::uint64_t min = 0, p50 = 0, p90 = 0, p99 = 0, p999 = 0, max = 0;
  double mean = 0.0;
};
Distribution summarize(std::vector<std::uint64_t> values);  // exact (nearest-rank), not histogram-approximate

struct OverloadModelConfig {
  std::size_t capacity = 1024;
  std::uint64_t service_ns = 1000;
  QueueFullPolicy policy = QueueFullPolicy::DropAndCount;
};

struct OverloadResult {
  std::uint64_t offered = 0;            // events that reached the queue's door (a fail-fast stop leaves the rest unoffered)
  std::uint64_t enqueued = 0;
  std::uint64_t dropped = 0;            // queue_overflow (drop policy), or the single failing event (fail policy)
  std::uint64_t not_offered = 0;        // fail policy: events after the failure that were never sent
  std::uint64_t processed = 0;
  bool failed_fast = false;
  std::uint64_t high_water = 0;
  double mean_occupancy = 0.0;          // time-weighted over [first enqueue, last completion]
  Distribution queue_wait_ns;           // time an event spent waiting in the queue (pop - enqueue)
  Distribution source_delay_ns;         // enqueue - scheduled: how long backpressure held the producer back
  Distribution end_to_end_ns;           // completion - scheduled
  std::uint64_t producer_stall_ns = 0;  // total time the producer was blocked (block policy)
  std::uint64_t completion_ns = 0;      // when the last processed event finished
  SteadyState steady;
  std::vector<OccPoint> timeline;       // exact change-points (always recorded; cheap)

  double loss_fraction() const { return offered + not_offered == 0 ? 0.0 : static_cast<double>(dropped + not_offered) / static_cast<double>(offered + not_offered); }
};

// `scheduled_ns` must be non-decreasing.
OverloadResult simulate_overload(const std::vector<std::uint64_t>& scheduled_ns, const OverloadModelConfig& cfg);

}  // namespace feed
