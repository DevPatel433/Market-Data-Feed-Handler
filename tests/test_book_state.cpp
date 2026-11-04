#include <algorithm>
#include <vector>

#include "check.hpp"
#include "feed/book_state.hpp"
#include "feed/decoder.hpp"
#include "feed/lag_histogram.hpp"
#include "feed/rng.hpp"
#include "feed/workload.hpp"

using namespace feed;

namespace {

Message add(std::uint64_t seq, std::uint64_t id, std::int64_t px = 100, std::uint32_t qty = 10, std::uint32_t inst = 1) {
  return Message{1, seq, AddOrder{id, px, inst, qty, Side::Buy}};
}
Message cancel(std::uint64_t seq, std::uint64_t id) { return Message{1, seq, CancelOrder{id, 1}}; }
Message trade(std::uint64_t seq, std::int64_t px, std::uint32_t qty, std::uint32_t inst = 1) {
  return Message{1, seq, Trade{seq, px, inst, qty, Side::Sell}};
}

std::vector<Message> decode_all(const std::vector<ReplayRecord>& recs) {
  std::vector<Message> out;
  for (const auto& r : recs) {
    Message m;
    CHECK(decode(r.frame, m) == DecodeError::None);
    out.push_back(m);
  }
  return out;
}

}  // namespace

// ---------------------------------------------------------------- BookState ----

TEST(book_applies_add_cancel_trade_heartbeat) {
  BookState b;
  CHECK(b.apply(add(1, 7, 100, 10)) == ApplyOutcome::Applied);
  CHECK(b.apply(add(2, 8, 101, 20)) == ApplyOutcome::Applied);
  CHECK_EQ(b.live_orders(), 2U);
  CHECK(b.apply(trade(3, 100, 5)) == ApplyOutcome::Applied);
  CHECK(b.apply(trade(4, 102, 7)) == ApplyOutcome::Applied);
  CHECK_EQ(b.instruments().at(1).trades, 2U);
  CHECK_EQ(b.instruments().at(1).traded_qty, 12U);
  CHECK_EQ(b.instruments().at(1).last_price, 102);
  CHECK(b.apply(cancel(5, 7)) == ApplyOutcome::Applied);
  CHECK_EQ(b.live_orders(), 1U);
  const std::uint64_t before = b.digest();
  CHECK(b.apply(Message{1, 6, Heartbeat{}}) == ApplyOutcome::Applied);
  CHECK_EQ(b.digest(), before);  // a heartbeat changes nothing
  CHECK_EQ(b.applied(), 6U);
}

TEST(book_flags_anomalies_without_changing_state) {
  BookState b;
  b.apply(add(1, 7));
  const std::uint64_t d = b.digest();
  CHECK(b.apply(cancel(2, 999)) == ApplyOutcome::UnknownCancel);  // its Add was lost
  CHECK(b.apply(add(3, 7, 555, 99)) == ApplyOutcome::DuplicateAdd);
  CHECK_EQ(b.digest(), d);
  CHECK_EQ(b.orders().at(7).price, 100);  // the original order is kept
  CHECK_EQ(b.unknown_cancels(), 1U);
  CHECK_EQ(b.duplicate_adds(), 1U);
}

TEST(book_digest_is_incremental_order_independent_and_content_sensitive) {
  BookState a, b;
  for (std::uint64_t i = 1; i <= 50; ++i) a.apply(add(i, i, 100 + static_cast<std::int64_t>(i), 5));
  for (std::uint64_t i = 50; i >= 1; --i) b.apply(add(51 - i, i, 100 + static_cast<std::int64_t>(i), 5));
  CHECK_EQ(a.digest(), b.digest());  // same content, different insertion order
  CHECK_EQ(a.digest(), BookState::compute_digest(a.orders(), a.instruments()));  // incremental == from scratch

  BookState c = a;
  c.apply(cancel(100, 10));
  CHECK(c.digest() != a.digest());
  c.apply(add(101, 10, 110, 5));  // put it back, identically
  CHECK_EQ(c.digest(), a.digest());
  c.apply(cancel(102, 10));
  c.apply(add(103, 10, 110, 6));  // same id, different qty
  CHECK(c.digest() != a.digest());

  BookState t1, t2;
  t1.apply(trade(1, 100, 5));
  t2.apply(trade(1, 100, 6));
  CHECK(t1.digest() != t2.digest());  // trade stats are part of the state
  CHECK_EQ(t1.digest(), BookState::compute_digest(t1.orders(), t1.instruments()));
}

TEST(book_snapshot_restore_round_trips_exactly) {
  const auto msgs = decode_all([] {
    WorkloadConfig c;
    c.count = 3000;
    c.seed = 4;
    return generate_workload(c).records;
  }());
  BookState live;
  BookSnapshot at1000;
  for (std::size_t i = 0; i < msgs.size(); ++i) {
    live.apply(msgs[i]);
    if (i + 1 == 1000) at1000 = live.snapshot(1000);
  }
  CHECK_EQ(at1000.seq, 1000U);
  CHECK_EQ(at1000.digest, BookState::compute_digest(at1000.orders, at1000.instruments));

  BookState restored;
  restored.restore(at1000);
  for (std::size_t i = 1000; i < msgs.size(); ++i) restored.apply(msgs[i]);  // catch up from the snapshot
  CHECK_EQ(restored.digest(), live.digest());
  CHECK(restored.orders() == live.orders());
  CHECK(restored.instruments() == live.instruments());
  CHECK_EQ(restored.unknown_cancels(), 0U);

  restored.clear();
  CHECK_EQ(restored.digest(), 0U);
  CHECK_EQ(restored.live_orders(), 0U);
}

TEST(book_a_complete_clean_stream_never_raises_an_anomaly) {
  WorkloadConfig c;
  c.count = 20'000;
  c.seed = 9;
  BookState b;
  for (const Message& m : decode_all(generate_workload(c).records)) CHECK(b.apply(m) == ApplyOutcome::Applied);
  CHECK_EQ(b.unknown_cancels() + b.duplicate_adds(), 0U);
  CHECK(b.live_orders() > 0);
}

TEST(book_losing_messages_causes_both_visible_and_silent_damage) {
  WorkloadConfig c;
  c.count = 6000;
  c.seed = 11;
  const auto all = decode_all(generate_workload(c).records);
  BookState reference;
  for (const Message& m : all) reference.apply(m);

  // Drop every 40th message and apply the rest as received (what a naive consumer does).
  BookState naive;
  for (std::size_t i = 0; i < all.size(); ++i) {
    if (i % 40 == 39) continue;
    naive.apply(all[i]);
  }
  CHECK(naive.digest() != reference.digest());  // diverged
  CHECK(naive.unknown_cancels() > 0);           // some damage is visible (a cancel whose add was lost)...
  // ...and some is silent: a lost cancel leaves a stale order that no local check can notice.
  std::size_t stale = 0;
  for (const auto& [id, rec] : naive.orders()) stale += reference.orders().count(id) == 0 ? 1U : 0U;
  CHECK(stale > 0);
}

// ------------------------------------------------------------ LogHistogram ----

TEST(histogram_small_values_are_exact_and_empty_is_safe) {
  LogHistogram h;
  CHECK_EQ(h.count(), 0U);
  CHECK_EQ(h.percentile(50), 0U);
  CHECK_EQ(h.max(), 0U);
  for (std::uint64_t v = 0; v < 8; ++v) h.record(v);
  CHECK_EQ(h.count(), 8U);
  CHECK_EQ(h.min(), 0U);
  CHECK_EQ(h.max(), 7U);
  CHECK_EQ(h.percentile(0), 0U);
  CHECK_EQ(h.percentile(100), 7U);
  CHECK_EQ(h.percentile(50), 3U);  // 4th of 8 values: exactly representable
  CHECK_EQ(h.mean(), 3.5);
}

TEST(histogram_bucket_mapping_is_consistent_everywhere) {
  // index_of(v) must land in a bucket whose upper bound is >= v, and bounds must be monotone.
  SplitMix64 rng(1);
  for (int i = 0; i < 200'000; ++i) {
    const std::uint64_t v = rng.next() >> rng.uniform(64);
    const std::size_t idx = LogHistogram::index_of(v);
    CHECK(idx < LogHistogram::kBuckets);
    CHECK(LogHistogram::upper_bound_of(idx) >= v);
    if (idx > 0) CHECK(LogHistogram::upper_bound_of(idx - 1) < v || LogHistogram::upper_bound_of(idx - 1) < LogHistogram::upper_bound_of(idx));
  }
  for (std::size_t i = 1; i < LogHistogram::kBuckets; ++i) CHECK(LogHistogram::upper_bound_of(i) > LogHistogram::upper_bound_of(i - 1));
  CHECK_EQ(LogHistogram::index_of(~std::uint64_t{0}), LogHistogram::kBuckets - 1);
}

TEST(histogram_percentiles_are_upper_bounds_within_12_5_percent_of_exact) {
  SplitMix64 rng(77);
  LogHistogram h;
  std::vector<std::uint64_t> exact;
  for (int i = 0; i < 100'000; ++i) {
    const std::uint64_t v = 100 + rng.uniform(5'000'000);
    h.record(v);
    exact.push_back(v);
  }
  std::sort(exact.begin(), exact.end());
  for (const double p : {1.0, 25.0, 50.0, 90.0, 99.0, 99.9}) {
    auto rank = static_cast<std::size_t>(p / 100.0 * static_cast<double>(exact.size()));
    if (rank == 0) rank = 1;
    const std::uint64_t truth = exact[rank - 1];
    const std::uint64_t got = h.percentile(p);
    CHECK(got >= truth);                                                     // never under-reports
    CHECK(static_cast<double>(got) <= static_cast<double>(truth) * 1.125 + 1);  // within one sub-bucket
  }
  CHECK_EQ(h.percentile(100), exact.back());
  CHECK_EQ(h.min(), exact.front());
}

TEST(histogram_merge_equals_recording_everything_in_one) {
  SplitMix64 rng(3);
  LogHistogram a, b, all;
  for (int i = 0; i < 10'000; ++i) {
    const std::uint64_t v = rng.uniform(1'000'000);
    (i % 2 == 0 ? a : b).record(v);
    all.record(v);
  }
  a.merge(b);
  CHECK_EQ(a.count(), all.count());
  CHECK_EQ(a.max(), all.max());
  CHECK_EQ(a.min(), all.min());
  for (const double p : {10.0, 50.0, 99.0}) CHECK_EQ(a.percentile(p), all.percentile(p));
}

TEST(histogram_percentile_never_exceeds_the_recorded_maximum) {
  LogHistogram h;
  h.record(100);  // 100 lies in a bucket whose upper bound is 103
  CHECK_EQ(h.percentile(50), 100U);
  CHECK_EQ(h.percentile(99.9), 100U);
  CHECK_EQ(h.percentile(100), 100U);
  LogHistogram g;
  for (std::uint64_t v : {1000ULL, 1001ULL, 1002ULL}) g.record(v);
  for (const double p : {1.0, 50.0, 99.0}) CHECK(g.percentile(p) <= g.max());
}

TEST(histogram_merge_takes_the_minimum_and_maximum_from_either_side) {
  LogHistogram big, small;
  big.record(5000);
  big.record(6000);
  small.record(7);       // the global minimum lives in the OTHER histogram
  small.record(900'000); // and so does the global maximum
  big.merge(small);
  CHECK_EQ(big.min(), 7U);
  CHECK_EQ(big.max(), 900'000U);
  CHECK_EQ(big.count(), 4U);

  LogHistogram empty;
  LogHistogram one;
  one.record(42);
  empty.merge(one);  // merging into an empty histogram must not invent a zero minimum
  CHECK_EQ(empty.min(), 42U);
  one.merge(LogHistogram{});  // merging an empty one changes nothing
  CHECK_EQ(one.min(), 42U);
  CHECK_EQ(one.count(), 1U);
}
