#include "feed/overload_model.hpp"

#include <algorithm>
#include <deque>

namespace feed {

Distribution summarize(std::vector<std::uint64_t> v) {
  Distribution d;
  d.count = v.size();
  if (v.empty()) return d;
  std::sort(v.begin(), v.end());
  const auto nearest_rank = [&](double p) {
    auto rank = static_cast<std::size_t>(p / 100.0 * static_cast<double>(v.size()));
    if (static_cast<double>(rank) < p / 100.0 * static_cast<double>(v.size())) ++rank;
    return v[std::max<std::size_t>(rank, 1) - 1];
  };
  d.min = v.front();
  d.max = v.back();
  d.p50 = nearest_rank(50);
  d.p90 = nearest_rank(90);
  d.p99 = nearest_rank(99);
  d.p999 = nearest_rank(99.9);
  long double sum = 0;
  for (const std::uint64_t x : v) sum += static_cast<long double>(x);
  d.mean = static_cast<double>(sum / static_cast<long double>(v.size()));
  return d;
}

SteadyState analyze_occupancy(const std::vector<OccPoint>& pts, std::size_t capacity, std::uint64_t end_ns) {
  (void)end_ns;
  SteadyState s;
  s.low_mark = capacity <= 2 ? 0 : 1;
  for (const OccPoint& p : pts) s.peak = std::max(s.peak, p.occupancy);

  if (s.peak < std::min<std::uint64_t>(capacity, 2)) {  // no backlog ever formed
    s.steady_reached = true;
    return s;
  }
  s.congested = true;

  std::size_t peak_idx = 0;  // the LAST point at the peak
  for (std::size_t i = 0; i < pts.size(); ++i) {
    if (pts[i].occupancy == s.peak) peak_idx = i;
  }
  s.peak_at_ns = pts[peak_idx].t_ns;
  for (std::size_t i = peak_idx; i < pts.size(); ++i) {
    if (pts[i].occupancy <= s.low_mark) {
      s.steady_reached = true;
      s.steady_at_ns = pts[i].t_ns;
      s.time_to_steady_ns = s.steady_at_ns - s.peak_at_ns;
      break;
    }
  }
  return s;
}

OverloadResult simulate_overload(const std::vector<std::uint64_t>& sched, const OverloadModelConfig& cfg) {
  OverloadResult r;
  struct Item {
    std::uint64_t enq;
    std::uint64_t scheduled;
  };
  std::deque<Item> q;
  std::uint64_t free_at = 0;  // when the consumer finishes its current item and can pop the next
  std::uint64_t producer_free = 0;
  std::vector<std::uint64_t> waits, delays, e2e;
  const std::size_t cap = std::max<std::size_t>(cfg.capacity, 1);

  const auto note = [&](std::uint64_t t) {
    if (!r.timeline.empty() && r.timeline.back().t_ns == t && r.timeline.back().occupancy == q.size()) return;
    r.timeline.push_back({t, q.size()});
  };

  // Let the consumer pop every item whose service would START at or before `limit`.
  const auto advance = [&](std::uint64_t limit) {
    while (!q.empty()) {
      const std::uint64_t start = std::max(free_at, q.front().enq);
      if (start > limit) break;
      const Item it = q.front();
      q.pop_front();
      waits.push_back(start - it.enq);
      free_at = start + cfg.service_ns;
      e2e.push_back(free_at - it.scheduled);
      ++r.processed;
      r.completion_ns = free_at;
      note(start);
    }
  };

  for (std::size_t i = 0; i < sched.size(); ++i) {
    const std::uint64_t s = sched[i];
    std::uint64_t t = cfg.policy == QueueFullPolicy::Block ? std::max(s, producer_free) : s;  // a blocked producer catches up late
    advance(t);
    ++r.offered;

    if (q.size() >= cap) {
      if (cfg.policy == QueueFullPolicy::DropAndCount) {
        ++r.dropped;
        continue;
      }
      if (cfg.policy == QueueFullPolicy::FailFast) {
        ++r.dropped;
        r.failed_fast = true;
        r.not_offered = sched.size() - i - 1;
        break;
      }
      // Block: wait until the front item starts service, which frees its slot.
      const std::uint64_t t_free = free_at;
      r.producer_stall_ns += t_free - t;
      t = t_free;
      advance(t);
    }

    q.push_back({t, s});
    ++r.enqueued;
    delays.push_back(t - s);
    producer_free = t;
    r.high_water = std::max<std::uint64_t>(r.high_water, q.size());
    note(t);
  }
  advance(~std::uint64_t{0});  // drain whatever is left after the last arrival

  r.queue_wait_ns = summarize(std::move(waits));
  r.source_delay_ns = summarize(std::move(delays));
  r.end_to_end_ns = summarize(std::move(e2e));

  // Time-weighted mean occupancy over the busy period.
  if (r.timeline.size() >= 2) {
    long double area = 0;
    for (std::size_t i = 0; i + 1 < r.timeline.size(); ++i) {
      area += static_cast<long double>(r.timeline[i].occupancy) * static_cast<long double>(r.timeline[i + 1].t_ns - r.timeline[i].t_ns);
    }
    const std::uint64_t span = r.timeline.back().t_ns - r.timeline.front().t_ns;
    if (span > 0) r.mean_occupancy = static_cast<double>(area / static_cast<long double>(span));
  }
  r.steady = analyze_occupancy(r.timeline, cap, r.completion_ns);
  return r;
}

}  // namespace feed
