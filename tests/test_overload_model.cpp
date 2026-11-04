#include <algorithm>
#include <deque>
#include <vector>

#include "check.hpp"
#include "feed/overload_model.hpp"
#include "feed/rng.hpp"
#include "feed/workload.hpp"

using namespace feed;

namespace {

OverloadModelConfig mcfg(std::size_t cap, std::uint64_t service, QueueFullPolicy p) {
  OverloadModelConfig c;
  c.capacity = cap;
  c.service_ns = service;
  c.policy = p;
  return c;
}

// Independent brute-force oracle: steps time one tick at a time instead of jumping between events.
struct Oracle {
  std::uint64_t offered = 0, enqueued = 0, dropped = 0, not_offered = 0, processed = 0, completion = 0, high_water = 0, stall = 0;
  bool failed = false;
  std::vector<std::uint64_t> waits, delays, e2e;
};

Oracle run_oracle(const std::vector<std::uint64_t>& sched, std::size_t cap, std::uint64_t service, QueueFullPolicy policy) {
  Oracle o;
  struct Item {
    std::uint64_t enq, sched;
  };
  std::deque<Item> q;
  std::uint64_t busy_until = 0, cur = 0, producer_free = 0;

  const auto pop_check = [&](std::uint64_t tick) {  // the consumer pops at a tick if it is free and something waits
    if (!q.empty() && busy_until <= tick && q.front().enq <= tick) {
      o.waits.push_back(tick - q.front().enq);
      busy_until = tick + service;
      o.e2e.push_back(busy_until - q.front().sched);
      ++o.processed;
      o.completion = busy_until;
      q.pop_front();
    }
  };
  const auto settle = [&](std::uint64_t t) {
    for (std::uint64_t tk = cur; tk <= t; ++tk) pop_check(tk);
    cur = t;
  };

  for (std::size_t i = 0; i < sched.size(); ++i) {
    std::uint64_t t = policy == QueueFullPolicy::Block ? std::max(sched[i], producer_free) : sched[i];
    settle(t);
    ++o.offered;
    if (q.size() >= cap) {
      if (policy == QueueFullPolicy::DropAndCount) {
        ++o.dropped;
        continue;
      }
      if (policy == QueueFullPolicy::FailFast) {
        ++o.dropped;
        o.failed = true;
        o.not_offered = sched.size() - i - 1;
        break;
      }
      while (q.size() >= cap) {  // block: tick until a slot opens
        ++cur;
        pop_check(cur);
      }
      o.stall += cur - t;
      t = cur;
    }
    q.push_back({t, sched[i]});
    ++o.enqueued;
    o.delays.push_back(t - sched[i]);
    producer_free = t;
    o.high_water = std::max<std::uint64_t>(o.high_water, q.size());
  }
  for (;;) {  // drain
    pop_check(cur);
    if (q.empty()) break;
    ++cur;
  }
  return o;
}

bool same(const Distribution& a, const Distribution& b) {
  return a.count == b.count && a.min == b.min && a.p50 == b.p50 && a.p90 == b.p90 && a.p99 == b.p99 && a.p999 == b.p999 &&
         a.max == b.max && a.mean == b.mean;
}

std::vector<std::uint64_t> random_arrivals(SplitMix64& rng, std::size_t n, std::uint64_t max_gap) {
  std::vector<std::uint64_t> v;
  std::uint64_t t = rng.uniform(5);
  for (std::size_t i = 0; i < n; ++i) {
    t += rng.uniform(max_gap + 1);  // gap 0 => simultaneous arrivals
    v.push_back(t);
  }
  return v;
}

std::vector<std::uint64_t> burst_schedule(std::uint64_t count, std::uint64_t rate, std::uint32_t burst, std::uint32_t mult) {
  WorkloadConfig c;
  c.profile = Profile::Bursty;
  c.count = count;
  c.rate_per_sec = rate;
  c.burst_size = burst;
  c.burst_multiplier = mult;
  std::vector<std::uint64_t> v;
  for (std::uint64_t i = 0; i < count; ++i) v.push_back(scheduled_offset_ns(c, i));
  return v;
}

}  // namespace

// ------------------------------------------------------------- hand-checked ----

TEST(model_hand_computed_example_drop_block_and_fail_fast) {
  // Arrivals at t = 0,1,2,3; service 10; capacity 2.
  const std::vector<std::uint64_t> arr = {0, 1, 2, 3};

  // DROP: e0 pops at once (done 10). e1 waits (pops 10, done 20). e2 waits (pops 20, done 30). e3 finds the queue full.
  const OverloadResult d = simulate_overload(arr, mcfg(2, 10, QueueFullPolicy::DropAndCount));
  CHECK_EQ(d.offered, 4U);
  CHECK_EQ(d.enqueued, 3U);
  CHECK_EQ(d.dropped, 1U);
  CHECK_EQ(d.processed, 3U);
  CHECK_EQ(d.high_water, 2U);
  CHECK_EQ(d.completion_ns, 30U);
  CHECK_EQ(d.queue_wait_ns.count, 3U);
  CHECK_EQ(d.queue_wait_ns.min, 0U);
  CHECK_EQ(d.queue_wait_ns.max, 18U);  // e2: enqueued at 2, popped at 20
  CHECK_EQ(d.queue_wait_ns.p50, 9U);
  CHECK_EQ(d.end_to_end_ns.max, 28U);  // e2: scheduled 2, done 30
  CHECK_EQ(d.producer_stall_ns, 0U);

  // BLOCK: e3 waits for a slot until t=10 (when e1 is popped), so it is enqueued 7 late; nothing is lost.
  const OverloadResult b = simulate_overload(arr, mcfg(2, 10, QueueFullPolicy::Block));
  CHECK_EQ(b.dropped, 0U);
  CHECK_EQ(b.enqueued, 4U);
  CHECK_EQ(b.processed, 4U);
  CHECK_EQ(b.producer_stall_ns, 7U);
  CHECK_EQ(b.source_delay_ns.max, 7U);
  CHECK_EQ(b.queue_wait_ns.max, 20U);  // e3: enqueued at 10, popped at 30
  CHECK_EQ(b.completion_ns, 40U);
  CHECK_EQ(b.end_to_end_ns.max, 37U);  // e3: scheduled 3, done 40

  // FAIL-FAST: stops at e3, the first event that does not fit. Nothing is left to send.
  const OverloadResult f = simulate_overload(arr, mcfg(2, 10, QueueFullPolicy::FailFast));
  CHECK(f.failed_fast);
  CHECK_EQ(f.dropped, 1U);
  CHECK_EQ(f.not_offered, 0U);
  CHECK_EQ(f.processed, 3U);

  // Same again with more events behind the failure.
  const OverloadResult f2 = simulate_overload({0, 1, 2, 3, 4, 5, 6}, mcfg(2, 10, QueueFullPolicy::FailFast));
  CHECK_EQ(f2.not_offered, 3U);
  CHECK_EQ(f2.offered, 4U);
}

TEST(model_empty_and_trivial_inputs) {
  const OverloadResult e = simulate_overload({}, mcfg(4, 10, QueueFullPolicy::DropAndCount));
  CHECK_EQ(e.offered + e.processed + e.dropped, 0U);
  CHECK(!e.steady.congested);
  const OverloadResult one = simulate_overload({5}, mcfg(1, 10, QueueFullPolicy::Block));
  CHECK_EQ(one.processed, 1U);
  CHECK_EQ(one.completion_ns, 15U);
  CHECK_EQ(one.queue_wait_ns.max, 0U);
}

// ---------------------------------------------------- differential testing ----

TEST(model_matches_a_brute_force_tick_simulation_on_thousands_of_random_cases) {
  SplitMix64 rng(2024);
  std::uint64_t drops_seen = 0, stalls_seen = 0, fails_seen = 0;
  for (int trial = 0; trial < 3000; ++trial) {
    const std::size_t n = 1 + static_cast<std::size_t>(rng.uniform(40));
    const auto arr = random_arrivals(rng, n, 1 + rng.uniform(7));
    const std::size_t cap = 1 + static_cast<std::size_t>(rng.uniform(6));
    const std::uint64_t svc = 1 + rng.uniform(8);
    for (const QueueFullPolicy p : {QueueFullPolicy::DropAndCount, QueueFullPolicy::Block, QueueFullPolicy::FailFast}) {
      const OverloadResult m = simulate_overload(arr, mcfg(cap, svc, p));
      const Oracle o = run_oracle(arr, cap, svc, p);
      CHECK_EQ(m.offered, o.offered);
      CHECK_EQ(m.enqueued, o.enqueued);
      CHECK_EQ(m.dropped, o.dropped);
      CHECK_EQ(m.not_offered, o.not_offered);
      CHECK_EQ(m.processed, o.processed);
      CHECK_EQ(m.completion_ns, o.completion);
      CHECK_EQ(m.high_water, o.high_water);
      CHECK_EQ(m.producer_stall_ns, o.stall);
      CHECK_EQ(m.failed_fast, o.failed);
      CHECK(same(m.queue_wait_ns, summarize(o.waits)));
      CHECK(same(m.source_delay_ns, summarize(o.delays)));
      CHECK(same(m.end_to_end_ns, summarize(o.e2e)));
      drops_seen += m.dropped;
      stalls_seen += m.producer_stall_ns > 0 ? 1U : 0U;
      fails_seen += m.failed_fast ? 1U : 0U;
    }
  }
  CHECK(drops_seen > 1000 && stalls_seen > 500 && fails_seen > 500);  // the random cases really exercised overflow
}

// ------------------------------------------------------------- properties ----

TEST(model_conserves_events_and_respects_the_queue_bound) {
  SplitMix64 rng(5);
  for (int trial = 0; trial < 400; ++trial) {
    const auto arr = random_arrivals(rng, 200, 4);
    const std::size_t cap = 1 + static_cast<std::size_t>(rng.uniform(20));
    const std::uint64_t svc = 1 + rng.uniform(6);
    for (const QueueFullPolicy p : {QueueFullPolicy::DropAndCount, QueueFullPolicy::Block, QueueFullPolicy::FailFast}) {
      const OverloadResult r = simulate_overload(arr, mcfg(cap, svc, p));
      CHECK_EQ(r.offered, r.enqueued + r.dropped);
      CHECK_EQ(r.offered + r.not_offered, arr.size());
      CHECK_EQ(r.processed, r.enqueued);
      CHECK_EQ(r.queue_wait_ns.count, r.processed);
      CHECK(r.high_water <= cap);
      for (const OccPoint& pt : r.timeline) CHECK(pt.occupancy <= cap);
      // An accepted event has at most `cap` events ahead of it, so it waits at most cap * service.
      CHECK(r.queue_wait_ns.max <= cap * svc);
      if (p == QueueFullPolicy::Block) CHECK_EQ(r.dropped, 0U);
      if (r.processed > 0) CHECK(r.completion_ns >= r.processed * svc);  // work conservation: the consumer cannot go faster
    }
  }
}

TEST(model_larger_buffers_never_drop_more_and_block_stalls_are_consistent_with_drops) {
  SplitMix64 rng(8);
  for (int trial = 0; trial < 200; ++trial) {
    const auto arr = random_arrivals(rng, 300, 3);
    const std::uint64_t svc = 1 + rng.uniform(5);
    std::uint64_t prev_drops = ~std::uint64_t{0};
    for (const std::size_t cap : {1U, 2U, 4U, 8U, 16U, 64U, 512U}) {
      const OverloadResult d = simulate_overload(arr, mcfg(cap, svc, QueueFullPolicy::DropAndCount));
      CHECK(d.dropped <= prev_drops);  // more memory never costs completeness
      prev_drops = d.dropped;
      const OverloadResult b = simulate_overload(arr, mcfg(cap, svc, QueueFullPolicy::Block));
      CHECK_EQ(d.dropped > 0, b.producer_stall_ns > 0);  // block stalls exactly when drop would have lost something
    }
    CHECK_EQ(prev_drops, 0U);  // 512 slots hold 300 events
  }
}

TEST(model_no_loss_whenever_the_consumer_is_never_slower_than_the_arrivals) {
  SplitMix64 rng(13);
  for (int trial = 0; trial < 100; ++trial) {
    std::vector<std::uint64_t> arr;
    std::uint64_t t = 0;
    for (int i = 0; i < 300; ++i) {
      t += 5 + rng.uniform(10);  // gaps of at least 5
      arr.push_back(t);
    }
    const OverloadResult r = simulate_overload(arr, mcfg(1, 5, QueueFullPolicy::DropAndCount));  // service 5 <= every gap
    CHECK_EQ(r.dropped, 0U);
    CHECK_EQ(r.queue_wait_ns.max, 0U);
  }
}

TEST(model_is_deterministic) {
  const auto arr = burst_schedule(5000, 100'000, 500, 20);
  const OverloadResult a = simulate_overload(arr, mcfg(64, 4000, QueueFullPolicy::DropAndCount));
  const OverloadResult b = simulate_overload(arr, mcfg(64, 4000, QueueFullPolicy::DropAndCount));
  CHECK_EQ(a.dropped, b.dropped);
  CHECK(same(a.queue_wait_ns, b.queue_wait_ns));
  CHECK(a.mean_occupancy == b.mean_occupancy);
  CHECK_EQ(a.steady.time_to_steady_ns, b.steady.time_to_steady_ns);
}

// ------------------------------------------------------------ steady state ----

TEST(steady_state_analysis_on_synthetic_step_functions) {
  // capacity 16: a backlog needs peak >= 2; steady means <= 1 waiting.
  const SteadyState quiet = analyze_occupancy({{0, 0}, {10, 1}, {20, 0}}, 16, 30);
  CHECK(!quiet.congested);
  CHECK(quiet.steady_reached);
  CHECK_EQ(quiet.time_to_steady_ns, 0U);
  CHECK_EQ(quiet.low_mark, 1U);

  const std::vector<OccPoint> burst = {{0, 0}, {10, 9}, {20, 16}, {30, 7}, {40, 3}, {50, 1}, {60, 0}};
  const SteadyState s = analyze_occupancy(burst, 16, 70);
  CHECK(s.congested && s.steady_reached);
  CHECK_EQ(s.peak, 16U);
  CHECK_EQ(s.peak_at_ns, 20U);
  CHECK_EQ(s.steady_at_ns, 50U);  // first <= 1 after the peak
  CHECK_EQ(s.time_to_steady_ns, 30U);

  const SteadyState stuck = analyze_occupancy({{0, 0}, {10, 16}, {20, 12}}, 16, 100);
  CHECK(stuck.congested);
  CHECK(!stuck.steady_reached);  // the series ends with a backlog still waiting

  const SteadyState plateau = analyze_occupancy({{0, 0}, {10, 16}, {20, 5}, {30, 5}}, 16, 40);
  CHECK(plateau.congested && !plateau.steady_reached);  // fell, but never to "caught up"

  // Two bursts of different size: measured from the last moment at the GLOBAL peak.
  const SteadyState two = analyze_occupancy({{0, 0}, {10, 10}, {20, 0}, {30, 12}, {40, 6}, {50, 0}}, 16, 60);
  CHECK_EQ(two.peak, 12U);
  CHECK_EQ(two.peak_at_ns, 30U);
  CHECK_EQ(two.time_to_steady_ns, 20U);

  // A plateau at the peak counts from its LAST point (end of the overload), not its first.
  const SteadyState held = analyze_occupancy({{0, 0}, {10, 16}, {15, 15}, {18, 16}, {40, 1}}, 16, 50);
  CHECK_EQ(held.peak_at_ns, 18U);
  CHECK_EQ(held.time_to_steady_ns, 22U);

  // Tiny capacities: capacity 1 backlogs at 1 and is steady at 0.
  const SteadyState c1 = analyze_occupancy({{0, 0}, {5, 1}, {9, 0}}, 1, 20);
  CHECK(c1.congested && c1.steady_reached);
  CHECK_EQ(c1.low_mark, 0U);
  CHECK_EQ(c1.time_to_steady_ns, 4U);

  // A deep backlog in a huge queue is still "congested": capacity does not hide it.
  const SteadyState big = analyze_occupancy({{0, 0}, {10, 1750}, {7'000'010, 0}}, 4096, 8'000'000);
  CHECK(big.congested && big.steady_reached);
  CHECK_EQ(big.time_to_steady_ns, 7'000'000U);
}

TEST(summarize_uses_nearest_rank_percentiles_exactly) {
  std::vector<std::uint64_t> v;
  for (std::uint64_t i = 1; i <= 100; ++i) v.push_back(i);
  const Distribution d = summarize(v);
  CHECK_EQ(d.count, 100U);
  CHECK_EQ(d.min, 1U);
  CHECK_EQ(d.p50, 50U);
  CHECK_EQ(d.p90, 90U);
  CHECK_EQ(d.p99, 99U);
  CHECK_EQ(d.max, 100U);
  CHECK(d.mean == 50.5);
  CHECK_EQ(summarize({}).count, 0U);
  CHECK_EQ(summarize({7}).p999, 7U);
}

// ------------------------------------------------------------ the scenario ----

TEST(model_bursty_scenario_shows_the_memory_vs_completeness_vs_latency_trade_off) {
  // 100k msg/s on average, in bursts of 2000 at 20x that rate, against a consumer that does 250k msg/s.
  const auto arr = burst_schedule(20'000, 100'000, 2000, 20);
  const std::uint64_t S = 4000;
  const OverloadResult big = simulate_overload(arr, mcfg(4096, S, QueueFullPolicy::DropAndCount));
  const OverloadResult small = simulate_overload(arr, mcfg(16, S, QueueFullPolicy::DropAndCount));
  const OverloadResult blocked = simulate_overload(arr, mcfg(16, S, QueueFullPolicy::Block));

  CHECK_EQ(big.dropped, 0U);                       // enough memory: complete...
  CHECK(big.high_water > 1500 && big.high_water < 2000);
  CHECK(big.queue_wait_ns.max > 5'000'000);        // ...but events wait milliseconds behind the burst
  CHECK(small.dropped > 10'000);                   // tiny queue: low latency, heavy loss
  CHECK(small.queue_wait_ns.max <= 16 * S);
  CHECK_EQ(blocked.dropped, 0U);                   // block: complete AND low queue wait, paid for by stalling the source
  CHECK(blocked.queue_wait_ns.max <= 16 * S);
  CHECK(blocked.source_delay_ns.max > 5'000'000);
  CHECK(big.steady.congested && big.steady.steady_reached);
  CHECK(big.steady.time_to_steady_ns > 5'000'000);   // ~1750 events x 4 us: the big queue needs milliseconds to drain after a burst...
  CHECK(small.steady.congested && small.steady.steady_reached);
  CHECK(small.steady.time_to_steady_ns < 200'000);   // ...while the tiny one is back to steady in microseconds (it threw the burst away)
  CHECK(small.steady.time_to_steady_ns < big.steady.time_to_steady_ns);
}
