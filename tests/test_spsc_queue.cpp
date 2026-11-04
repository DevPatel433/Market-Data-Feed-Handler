#include <atomic>
#include <chrono>
#include <cstdlib>
#include <memory>
#include <thread>
#include <vector>

#include "check.hpp"
#include "feed/consumer_loop.hpp"
#include "feed/queue_policy.hpp"
#include "feed/spsc_queue.hpp"

using namespace feed;

namespace {

constexpr SpscOptions kPlain{false, false, true};
constexpr SpscOptions kPadded{true, false, true};
constexpr SpscOptions kCached{false, true, true};
constexpr SpscOptions kPaddedCached{true, true, true};
constexpr SpscOptions kNoTrack{false, false, false};

// Self-checking payload: a torn or stale read cannot reproduce all seven derived words.
struct StressEvent {
  std::uint64_t ticket = 0;
  std::uint64_t w[7] = {};
};
std::uint64_t mix(std::uint64_t t, std::uint64_t i) {
  std::uint64_t z = t * 0x9E3779B97F4A7C15ULL + i * 0xBF58476D1CE4E5B9ULL + 0x94D049BB133111EBULL;
  z ^= z >> 31U;
  z *= 0xD6E8FEB86659FD93ULL;
  return z ^ (z >> 32U);
}
StressEvent make_event(std::uint64_t ticket) {
  StressEvent e;
  e.ticket = ticket;
  for (std::uint64_t i = 0; i < 7; ++i) e.w[i] = mix(ticket, i);
  return e;
}
bool intact(const StressEvent& e) {
  for (std::uint64_t i = 0; i < 7; ++i) {
    if (e.w[i] != mix(e.ticket, i)) return false;
  }
  return true;
}

void spin_ns(std::uint64_t ns) {
  if (ns == 0) return;
  const auto end = std::chrono::steady_clock::now() + std::chrono::nanoseconds(ns);
  while (std::chrono::steady_clock::now() < end) {
  }
}

std::uint64_t scale() {
  const char* s = std::getenv("FEED_STRESS_SCALE");  // multiply iteration counts for longer soak runs
  const long v = s != nullptr ? std::atol(s) : 1;
  return v > 0 ? static_cast<std::uint64_t>(v) : 1;
}

struct StressParams {
  std::size_t capacity;
  std::uint64_t n;
  std::uint64_t producer_delay_ns;
  std::uint64_t consumer_delay_ns;
  std::uint64_t burst;  // producer pauses for 100 us after every `burst` pushes (0 = never)
};

struct StressResult {
  std::uint64_t received = 0;
  std::uint64_t order_errors = 0;
  std::uint64_t corrupt = 0;
  SpscStats stats;
  std::size_t capacity = 0;
};

// One producer thread, one consumer thread, producer retries (with yield) until each push succeeds.
template <SpscOptions Opt>
StressResult run_stress(const StressParams& p) {
  SpscQueue<StressEvent, Opt> q(p.capacity);
  StressResult r;
  r.capacity = q.capacity();

  std::thread producer([&] {
    for (std::uint64_t i = 1; i <= p.n; ++i) {
      const StressEvent e = make_event(i);
      for (std::uint32_t spins = 0; !q.try_push(e); ++spins) {
        if (spins >= 16) std::this_thread::yield();
      }
      spin_ns(p.producer_delay_ns);
      if (p.burst != 0 && i % p.burst == 0) std::this_thread::sleep_for(std::chrono::microseconds(100));
    }
  });

  std::uint64_t expected = 1;
  StressEvent e;
  for (std::uint32_t spins = 0; r.received < p.n;) {
    if (q.try_pop(e)) {
      spins = 0;
      if (e.ticket != expected) ++r.order_errors;
      if (!intact(e)) ++r.corrupt;
      expected = e.ticket + 1;
      ++r.received;
      spin_ns(p.consumer_delay_ns);
    } else if (++spins >= 16) {
      std::this_thread::yield();
    }
  }
  producer.join();
  r.stats = q.stats();
  return r;
}

template <SpscOptions Opt>
void stress_all_profiles() {
  const std::uint64_t k = scale();
  struct Profile {
    const char* name;
    StressParams p;
  };
  const Profile profiles[] = {
      {"ping-pong cap 1", {1, 20'000 * k, 0, 0, 0}},
      {"cap 2", {2, 50'000 * k, 0, 0, 0}},
      {"fast/fast cap 64", {64, 100'000 * k, 0, 0, 0}},
      {"fast/fast cap 1024", {1024, 100'000 * k, 0, 0, 0}},
      {"slow consumer", {16, 10'000 * k, 0, 400, 0}},
      {"slow producer", {16, 10'000 * k, 400, 0, 0}},
      {"bursty producer", {128, 50'000 * k, 0, 0, 200}},
  };
  for (const Profile& pr : profiles) {
    const StressResult r = run_stress<Opt>(pr.p);
    if (r.received != pr.p.n || r.order_errors != 0 || r.corrupt != 0) {
      std::fprintf(stderr, "    profile '%s' failed: received=%llu order_errors=%llu corrupt=%llu\n", pr.name,
                   static_cast<unsigned long long>(r.received), static_cast<unsigned long long>(r.order_errors),
                   static_cast<unsigned long long>(r.corrupt));
    }
    CHECK_EQ(r.received, pr.p.n);
    CHECK_EQ(r.order_errors, 0U);  // nothing missing, duplicated or reordered
    CHECK_EQ(r.corrupt, 0U);       // nothing torn
    CHECK_EQ(r.stats.pushed, pr.p.n);
    CHECK_EQ(r.stats.popped, pr.p.n);
    CHECK_EQ(r.stats.occupancy, 0U);
    CHECK(r.stats.high_water <= r.capacity);
    if constexpr (Opt.track_high_water) {
      CHECK(r.stats.high_water >= 1);
    } else {
      CHECK_EQ(r.stats.high_water, 0U);
    }
    if (pr.p.consumer_delay_ns > 0) {  // a slow consumer must fill the queue and push back on the producer
      CHECK(r.stats.full_rejects > 0);
      if constexpr (Opt.track_high_water) CHECK_EQ(r.stats.high_water, r.capacity);
    }
    if (pr.p.producer_delay_ns > 0) CHECK(r.stats.empty_polls > 0);  // a slow producer starves the consumer
  }
}

// ---- single-threaded semantics, run for every layout variant ----
template <SpscOptions Opt>
void single_thread_semantics() {
  {  // capacity rounds up to a power of two and is exact
    const std::pair<std::size_t, std::size_t> table[] = {{1, 1}, {2, 2}, {3, 4}, {5, 8}, {1000, 1024}, {1024, 1024}};
    for (const auto& row : table) {
      const SpscQueue<int, Opt> sized(row.first);
      CHECK_EQ(sized.capacity(), row.second);
    }
  }
  SpscQueue<int, Opt> q(4);
  int out = -1;
  CHECK(!q.try_pop(out));  // empty
  CHECK_EQ(out, -1);       // untouched on failure
  for (int i = 0; i < 4; ++i) CHECK(q.try_push(i));
  CHECK(!q.try_push(99));  // full: exactly `capacity` elements fit
  CHECK_EQ(q.size_approx(), 4U);
  for (int i = 0; i < 4; ++i) {
    CHECK(q.try_pop(out));
    CHECK_EQ(out, i);  // FIFO
  }
  CHECK(!q.try_pop(out));
  CHECK_EQ(q.size_approx(), 0U);

  // wraparound: indices pass many multiples of the capacity, with varying fill levels
  int next_in = 0, next_out = 0;
  for (int round = 0; round < 5000; ++round) {
    const int burst = 1 + round % 4;
    for (int i = 0; i < burst; ++i) CHECK(q.try_push(next_in++));
    for (int i = 0; i < burst; ++i) {
      CHECK(q.try_pop(out));
      CHECK_EQ(out, next_out++);
    }
  }
  const SpscStats s = q.stats();
  CHECK_EQ(s.pushed, 4U + static_cast<std::uint64_t>(next_in));
  CHECK_EQ(s.popped, s.pushed);
  CHECK_EQ(s.full_rejects, 1U);
  CHECK(s.empty_polls >= 2U);
}

template <SpscOptions Opt>
void high_water_semantics() {
  SpscQueue<int, Opt> q(8);
  int out = 0;
  for (int i = 0; i < 5; ++i) CHECK(q.try_push(i));
  for (int i = 0; i < 5; ++i) CHECK(q.try_pop(out));
  for (int i = 0; i < 2; ++i) CHECK(q.try_push(i));
  // Exact when quiescent: the peak was 5, and later lower occupancy never lowers it.
  CHECK_EQ(q.stats().high_water, Opt.track_high_water ? 5U : 0U);
  CHECK_EQ(q.stats().occupancy, 2U);
  for (int i = 0; i < 6; ++i) CHECK(q.try_push(i));  // 8 queued: full
  CHECK_EQ(q.stats().high_water, Opt.track_high_water ? 8U : 0U);
  CHECK(!q.try_push(1));
}

template <SpscOptions Opt>
void index_wraparound() {
  // Start 5 below 2^64: the 64-bit indices overflow during the test; (tail - head) must stay correct.
  const std::uint64_t start = ~std::uint64_t{0} - 5;
  SpscQueue<std::uint64_t, Opt> q(4, start);
  std::uint64_t out = 0, next_in = 0, next_out = 0;
  for (int round = 0; round < 1000; ++round) {
    const std::uint64_t burst = 1 + static_cast<std::uint64_t>(round % 4);
    for (std::uint64_t i = 0; i < burst; ++i) CHECK(q.try_push(next_in++));
    CHECK(q.size_approx() == burst);
    for (std::uint64_t i = 0; i < burst; ++i) {
      CHECK(q.try_pop(out));
      CHECK_EQ(out, next_out++);
    }
  }
  for (int i = 0; i < 4; ++i) CHECK(q.try_push(7));
  CHECK(!q.try_push(7));  // still exactly full after wrapping
  CHECK_EQ(q.stats().pushed - q.stats().popped, 4U);
  CHECK_EQ(q.stats().pushed, next_in + 4);
}

}  // namespace

TEST(spsc_single_thread_semantics_all_layouts) {
  single_thread_semantics<kPlain>();
  single_thread_semantics<kPadded>();
  single_thread_semantics<kCached>();
  single_thread_semantics<kPaddedCached>();
  single_thread_semantics<kNoTrack>();
}

TEST(spsc_high_water_mark_semantics) {
  high_water_semantics<kPlain>();
  high_water_semantics<kCached>();
  high_water_semantics<kNoTrack>();
}

TEST(spsc_indices_wrap_past_2_to_the_64) {
  index_wraparound<kPlain>();
  index_wraparound<kPaddedCached>();
}

TEST(spsc_rejects_invalid_capacity) {
  bool threw_zero = false, threw_huge = false;
  try { SpscQueue<int> q(0); } catch (const std::invalid_argument&) { threw_zero = true; }
  try { SpscQueue<int> q(SpscQueue<int>::kMaxCapacity + 1); } catch (const std::length_error&) { threw_huge = true; }
  CHECK(threw_zero);
  CHECK(threw_huge);
}

TEST(spsc_failed_push_leaves_move_only_argument_untouched) {
  SpscQueue<std::unique_ptr<int>> q(2);
  CHECK(q.try_push(std::make_unique<int>(1)));
  CHECK(q.try_push(std::make_unique<int>(2)));
  auto third = std::make_unique<int>(3);
  CHECK(!q.try_push(std::move(third)));
  CHECK(third != nullptr);  // the caller can retry without losing the element
  CHECK_EQ(*third, 3);
  std::unique_ptr<int> out;
  CHECK(q.try_pop(out));
  CHECK_EQ(*out, 1);
  CHECK(q.try_push(std::move(third)));  // retry now succeeds
  CHECK(q.try_pop(out));
  CHECK_EQ(*out, 2);
  CHECK(q.try_pop(out));
  CHECK_EQ(*out, 3);
}

TEST(spsc_padded_layout_really_separates_the_two_sides) {
  using Padded = SpscQueue<int, kPadded>;
  CHECK_EQ(alignof(Padded), kCacheLine);
  CHECK(sizeof(Padded) >= 3 * kCacheLine);  // read-only line + producer line + consumer line
  CHECK(alignof(SpscQueue<int, kPlain>) < kCacheLine);
}

TEST(spsc_stress_plain_layout) { stress_all_profiles<kPlain>(); }
TEST(spsc_stress_padded_layout) { stress_all_profiles<kPadded>(); }
TEST(spsc_stress_cached_index_layout) { stress_all_profiles<kCached>(); }
TEST(spsc_stress_padded_and_cached_layout) { stress_all_profiles<kPaddedCached>(); }
TEST(spsc_stress_without_high_water_tracking) { stress_all_profiles<kNoTrack>(); }

TEST(spsc_producer_done_then_drain_never_loses_the_tail) {
  // The shutdown protocol the pipeline uses: set `done` (release) after the last push; the consumer,
  // once it observes `done`, drains once more. Repeated with tiny queues to shake out lost-tail races.
  for (int iter = 0; iter < 300; ++iter) {
    SpscQueue<StressEvent> q(4);
    std::atomic<bool> done{false};
    const std::uint64_t n = 40 + static_cast<std::uint64_t>(iter % 7);
    std::thread producer([&] {
      for (std::uint64_t i = 1; i <= n; ++i) {
        while (!q.try_push(make_event(i))) std::this_thread::yield();
      }
      done.store(true, std::memory_order_release);
    });
    std::uint64_t expected = 1, bad = 0;
    consume_until_done(q, done, [&](const StressEvent& e) {
      if (e.ticket != expected || !intact(e)) ++bad;
      ++expected;
    });
    producer.join();
    CHECK_EQ(expected, n + 1);
    CHECK_EQ(bad, 0U);
  }
}

// ------------------------------------------------------- full policies ----

TEST(policy_drop_and_count_never_waits_and_accounts_for_everything) {
  SpscQueue<int> q(4);
  OverflowCounters c;
  for (int i = 0; i < 10; ++i) {
    const auto r = enqueue(q, i, QueueFullPolicy::DropAndCount, c);
    CHECK(r == (i < 4 ? EnqueueResult::Enqueued : EnqueueResult::Dropped));
  }
  CHECK_EQ(c.offered, 10U);
  CHECK_EQ(c.enqueued, 4U);
  CHECK_EQ(c.dropped, 6U);
  CHECK_EQ(c.offered, c.enqueued + c.dropped + c.aborted);
  int out = 0;
  for (int i = 0; i < 4; ++i) {  // survivors are the FIRST four, in order (new events are the ones dropped)
    CHECK(q.try_pop(out));
    CHECK_EQ(out, i);
  }
}

TEST(policy_fail_fast_reports_the_first_overflow) {
  SpscQueue<int> q(2);
  OverflowCounters c;
  CHECK(enqueue(q, 1, QueueFullPolicy::FailFast, c) == EnqueueResult::Enqueued);
  CHECK(enqueue(q, 2, QueueFullPolicy::FailFast, c) == EnqueueResult::Enqueued);
  CHECK(enqueue(q, 3, QueueFullPolicy::FailFast, c) == EnqueueResult::Full);
  CHECK_EQ(c.dropped, 1U);  // the event that did not fit is accounted for, not silently lost
  CHECK_EQ(c.offered, c.enqueued + c.dropped);
}

TEST(policy_block_waits_for_space_and_loses_nothing) {
  SpscQueue<StressEvent> q(2);
  OverflowCounters c;
  constexpr std::uint64_t n = 5000;
  std::thread consumer([&] {
    StressEvent e;
    std::uint64_t expected = 1;
    while (expected <= n) {
      if (q.try_pop(e)) {
        if (e.ticket != expected) std::abort();
        ++expected;
        spin_ns(300);  // slower than the producer
      } else {
        std::this_thread::yield();
      }
    }
  });
  for (std::uint64_t i = 1; i <= n; ++i) CHECK(enqueue(q, make_event(i), QueueFullPolicy::Block, c) == EnqueueResult::Enqueued);
  consumer.join();
  CHECK_EQ(c.enqueued, n);
  CHECK_EQ(c.dropped, 0U);
  CHECK(c.stalled_events > 0);  // it really did have to wait
  CHECK(c.stall_spins >= c.stalled_events);
}

TEST(policy_block_can_be_released_by_the_abort_flag) {
  SpscQueue<int> q(1);
  OverflowCounters c;
  CHECK(enqueue(q, 1, QueueFullPolicy::Block, c) == EnqueueResult::Enqueued);
  std::atomic<bool> abort{false};
  std::thread releaser([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    abort.store(true, std::memory_order_relaxed);
  });
  CHECK(enqueue(q, 2, QueueFullPolicy::Block, c, &abort) == EnqueueResult::Aborted);  // nobody pops: only abort frees it
  releaser.join();
  CHECK_EQ(c.aborted, 1U);
  CHECK_EQ(c.offered, c.enqueued + c.dropped + c.aborted);
}

TEST(shutdown_race_window_is_reproduced_deterministically) {
  // Drives the exact interleaving that loses the tail if the final drain is missing:
  //   consumer polls -> queue EMPTY
  //   producer pushes its last events and sets `done`          <- injected via the hook
  //   consumer checks `done` -> true
  // One thread plays both roles purely to make the interleaving exact (no concurrency, so no race).
  SpscQueue<StressEvent> q(8);
  std::atomic<bool> done{false};
  bool injected = false;
  std::uint64_t received = 0;
  consume_until_done(
      q, done, [&](const StressEvent&) { ++received; },
      [&] {
        if (injected) return;
        injected = true;
        for (std::uint64_t i = 1; i <= 5; ++i) CHECK(q.try_push(make_event(i)));
        done.store(true, std::memory_order_release);
      });
  CHECK(injected);
  CHECK_EQ(received, 5U);  // all five are delivered, not lost in the window
  CHECK_EQ(q.size_approx(), 0U);
}
