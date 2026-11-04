#include <algorithm>
#include <limits>
#include <map>
#include <vector>

#include "check.hpp"
#include "feed/rng.hpp"
#include "feed/sequencer.hpp"
#include "seq_oracle.hpp"

using namespace feed;

namespace {

Verdict obs(Sequencer& s, std::uint64_t seq, std::uint32_t stream = 1) { return s.observe(stream, seq); }

void feed_range(Sequencer& s, std::uint64_t from, std::uint64_t to, std::uint32_t stream = 1) {
  for (std::uint64_t q = from; q <= to; ++q) obs(s, q, stream);
}

// missing_total must always be explained by exactly one of: filled late, still
// outstanding, given up on (eviction), or cleared by resync.
void check_loss_identity(const Sequencer& s) {
  const SequencerStats t = s.stats();
  CHECK_EQ(t.missing_total, t.out_of_order + s.outstanding_seqs() + t.seqs_evicted + t.resync_cleared_seqs);
  CHECK_EQ(t.observed(), t.forwarded + t.duplicates + t.late_discarded + t.paused_discarded);
}

}  // namespace

TEST(sequencer_in_order_stream_stays_synced) {
  Sequencer s;
  for (std::uint64_t q = 1; q <= 100; ++q) {
    const Verdict v = obs(s, q);
    CHECK(v.cls == Classification::InOrder);
    CHECK(v.action == Action::Forward);
    CHECK(v.state == StreamState::Synced);
  }
  const auto st = s.stats();
  CHECK_EQ(st.in_order, 100U);
  CHECK_EQ(st.forwarded, 100U);
  CHECK_EQ(st.missing_total, 0U);
  check_loss_identity(s);
}

TEST(sequencer_detects_gap_and_degrades_but_keeps_forwarding) {
  Sequencer s;
  feed_range(s, 1, 9);
  const Verdict v = obs(s, 13);
  CHECK(v.cls == Classification::Gap);
  CHECK_EQ(v.gap_first, 10U);
  CHECK_EQ(v.gap_count, 3U);
  CHECK(v.action == Action::Forward);  // degrade policy: the exposing message is still delivered
  CHECK(v.state == StreamState::Degraded);
  CHECK(obs(s, 14).cls == Classification::InOrder);
  CHECK(obs(s, 14).state == StreamState::Degraded);  // stays degraded; no auto-heal
  CHECK_EQ(s.outstanding_seqs(), 3U);
  CHECK_EQ(s.stats().missing_total, 3U);
  check_loss_identity(s);
}

TEST(sequencer_first_message_after_seq1_is_a_gap_unless_joining_mid_stream) {
  Sequencer strict;
  const Verdict v = obs(strict, 5);
  CHECK(v.cls == Classification::Gap);
  CHECK_EQ(v.gap_first, 1U);
  CHECK_EQ(v.gap_count, 4U);

  SequencerConfig cfg;
  cfg.join_mid_stream = true;
  Sequencer joiner(cfg);
  CHECK(obs(joiner, 100).cls == Classification::InOrder);
  CHECK(obs(joiner, 101).cls == Classification::InOrder);
  CHECK(obs(joiner, 50).cls == Classification::Duplicate);  // before the join point: not a tracked gap
  CHECK_EQ(joiner.stats().missing_total, 0U);
}

TEST(sequencer_distinguishes_duplicate_from_out_of_order) {
  Sequencer s;
  feed_range(s, 1, 4);
  obs(s, 8);  // missing 5,6,7
  CHECK(obs(s, 3).cls == Classification::Duplicate);     // received earlier
  CHECK(obs(s, 8).cls == Classification::Duplicate);     // the exposing message again
  CHECK(obs(s, 6).cls == Classification::OutOfOrder);    // fills the gap
  CHECK(obs(s, 6).cls == Classification::Duplicate);     // second copy of the late arrival
  CHECK_EQ(s.outstanding_seqs(), 2U);
  check_loss_identity(s);
}

TEST(sequencer_late_fills_split_ranges_correctly) {
  Sequencer s;
  feed_range(s, 1, 9);
  obs(s, 20);  // missing 10..19
  CHECK_EQ(s.stream(1)->missing_ranges, 1U);
  CHECK(obs(s, 15).cls == Classification::OutOfOrder);  // middle: splits into [10,14] and [16,19]
  CHECK_EQ(s.stream(1)->missing_ranges, 2U);
  CHECK_EQ(s.outstanding_seqs(), 9U);
  CHECK(obs(s, 10).cls == Classification::OutOfOrder);  // start of a range
  CHECK(obs(s, 19).cls == Classification::OutOfOrder);  // end of a range
  CHECK_EQ(s.stream(1)->missing_ranges, 2U);            // [11,14] and [16,18]
  CHECK_EQ(s.outstanding_seqs(), 7U);
  for (const std::uint64_t q : {11U, 12U, 13U, 14U, 16U, 17U, 18U}) CHECK(obs(s, q).cls == Classification::OutOfOrder);
  CHECK_EQ(s.outstanding_seqs(), 0U);
  CHECK_EQ(s.stream(1)->missing_ranges, 0U);
  check_loss_identity(s);
}

TEST(sequencer_late_policy_decides_forwarding_and_recovery) {
  for (const LatePolicy lp : {LatePolicy::Discard, LatePolicy::Forward}) {
    SequencerConfig cfg;
    cfg.late_policy = lp;
    Sequencer s(cfg);
    feed_range(s, 1, 4);
    obs(s, 6);  // missing 5 -> degraded
    const Verdict v = obs(s, 5);
    CHECK(v.cls == Classification::OutOfOrder);
    if (lp == LatePolicy::Discard) {
      CHECK(v.action == Action::Discard);
      CHECK(v.reason == DiscardReason::Late);
      CHECK(v.state == StreamState::Degraded);  // consumer never saw seq 5: still damaged
    } else {
      CHECK(v.action == Action::Forward);
      CHECK(v.state == StreamState::Synced);    // all missing data delivered
    }
    check_loss_identity(s);
  }
}

TEST(sequencer_pause_policy_stops_forwarding_until_resync) {
  SequencerConfig cfg;
  cfg.gap_policy = GapPolicy::Pause;
  Sequencer s(cfg);
  feed_range(s, 1, 2);
  const Verdict gap = obs(s, 4);  // missing 3
  CHECK(gap.cls == Classification::Gap);
  CHECK(gap.state == StreamState::Paused);
  CHECK(gap.action == Action::Discard);  // exposing message is withheld too
  CHECK(gap.reason == DiscardReason::Paused);
  CHECK(obs(s, 5).action == Action::Discard);
  CHECK(obs(s, 3).action == Action::Discard);  // late arrival does not unpause
  CHECK(obs(s, 3).reason == DiscardReason::Duplicate);
  CHECK(obs(s, 8).cls == Classification::Gap);  // sequence tracking continues: missing 6,7
  CHECK_EQ(s.stats().forwarded, 2U);
  check_loss_identity(s);

  CHECK(s.resync(1, 9));  // downstream rebuilt from a snapshot at seq 8
  CHECK_EQ(s.stats().resync_cleared_seqs, 2U);
  CHECK(s.stream(1)->state == StreamState::Synced);
  const Verdict after = obs(s, 9);
  CHECK(after.cls == Classification::InOrder);
  CHECK(after.action == Action::Forward);
  CHECK(obs(s, 7).cls == Classification::Duplicate);  // pre-resync traffic is stale
  check_loss_identity(s);
}

TEST(sequencer_resync_rejects_unknown_stream_and_zero) {
  Sequencer s;
  CHECK(!s.resync(9, 1));
  obs(s, 1);
  CHECK(!s.resync(1, 0));
  CHECK(s.resync(1, 50));
  CHECK(obs(s, 50).cls == Classification::InOrder);
}

TEST(sequencer_streams_are_independent) {
  Sequencer s;
  feed_range(s, 1, 3, 1);
  feed_range(s, 1, 3, 2);
  obs(s, 7, 1);  // stream 1 gaps
  CHECK(s.stream(1)->state == StreamState::Degraded);
  CHECK(s.stream(2)->state == StreamState::Synced);
  CHECK(obs(s, 4, 2).cls == Classification::InOrder);
  CHECK(obs(s, 4, 1).cls == Classification::OutOfOrder);  // missing 4,5,6 on stream 1 only
  CHECK_EQ(s.stream_count(), 2U);
  CHECK_EQ(s.streams().size(), 2U);
  CHECK_EQ(s.streams()[0].stream_id, 1U);  // ordered by id
}

TEST(sequencer_bounds_the_number_of_streams) {
  SequencerConfig cfg;
  cfg.max_streams = 2;
  Sequencer s(cfg);
  obs(s, 1, 10);
  obs(s, 1, 20);
  const Verdict v = obs(s, 1, 30);
  CHECK(v.cls == Classification::StreamLimit);
  CHECK(v.action == Action::Discard);
  CHECK(v.reason == DiscardReason::StreamLimit);
  CHECK(obs(s, 2, 10).cls == Classification::InOrder);  // existing streams unaffected
  CHECK_EQ(s.stats().stream_limit, 1U);
  CHECK_EQ(s.stream_count(), 2U);
}

TEST(sequencer_evicts_oldest_missing_ranges_and_never_heals_after) {
  SequencerConfig cfg;
  cfg.max_missing_ranges = 2;
  cfg.late_policy = LatePolicy::Forward;
  Sequencer s(cfg);
  obs(s, 1);
  obs(s, 3);  // missing {2}
  obs(s, 5);  // missing {4}
  obs(s, 7);  // missing {6} -> over the cap: oldest ({2}) is given up on
  CHECK_EQ(s.stats().ranges_evicted, 1U);
  CHECK_EQ(s.stats().seqs_evicted, 1U);
  CHECK_EQ(s.outstanding_seqs(), 2U);
  check_loss_identity(s);

  // Documented ambiguity: an evicted seq arriving late looks like a duplicate.
  CHECK(obs(s, 2).cls == Classification::Duplicate);
  CHECK(obs(s, 4).cls == Classification::OutOfOrder);
  CHECK(obs(s, 6).cls == Classification::OutOfOrder);
  CHECK_EQ(s.outstanding_seqs(), 0U);
  // Even with late=forward and nothing outstanding, data was given up on => still degraded.
  CHECK(s.stream(1)->state == StreamState::Degraded);
  CHECK_EQ(s.stream(1)->unrecovered_seqs, 1U);
  check_loss_identity(s);
}

TEST(sequencer_memory_stays_bounded_under_hostile_every_other_seq) {
  SequencerConfig cfg;
  cfg.max_missing_ranges = 64;
  Sequencer s(cfg);
  for (std::uint64_t q = 2; q < 20'000; q += 2) obs(s, q);  // a gap before every message
  CHECK(s.stream(1)->missing_ranges <= 64U);
  CHECK(s.stats().ranges_evicted > 0);
  check_loss_identity(s);
}

TEST(sequencer_handles_sequence_numbers_at_the_top_of_the_range) {
  constexpr auto kMax = std::numeric_limits<std::uint64_t>::max();
  Sequencer s;
  const Verdict v = obs(s, kMax);
  CHECK(v.cls == Classification::Gap);
  CHECK_EQ(v.gap_first, 1U);
  CHECK_EQ(v.gap_count, kMax - 1);
  CHECK(obs(s, kMax).cls == Classification::Duplicate);  // no wraparound to "expected == 0"
  CHECK(obs(s, kMax - 1).cls == Classification::OutOfOrder);
  CHECK(obs(s, 1).cls == Classification::OutOfOrder);
  check_loss_identity(s);

  SequencerConfig cfg;
  cfg.join_mid_stream = true;
  Sequencer j(cfg);
  CHECK(obs(j, kMax - 1).cls == Classification::InOrder);
  CHECK(obs(j, kMax).cls == Classification::InOrder);
  CHECK(obs(j, kMax).cls == Classification::Duplicate);
  CHECK(obs(j, kMax - 1).cls == Classification::Duplicate);
}

TEST(sequencer_is_deterministic) {
  const auto run = []() {
    SplitMix64 rng(5);
    Sequencer s;
    std::vector<int> trace;
    for (int i = 0; i < 5000; ++i) {
      const Verdict v = obs(s, 1 + rng.uniform(300));
      trace.push_back(static_cast<int>(v.cls) * 16 + static_cast<int>(v.state) * 4 + static_cast<int>(v.action));
    }
    return trace;
  };
  CHECK(run() == run());
}

TEST(sequencer_matches_reference_model_on_random_faulty_traffic) {
  struct Policy {
    GapPolicy g;
    LatePolicy l;
  };
  const Policy policies[] = {{GapPolicy::MarkDegraded, LatePolicy::Discard},
                             {GapPolicy::MarkDegraded, LatePolicy::Forward},
                             {GapPolicy::Pause, LatePolicy::Discard}};
  for (const Policy& pol : policies) {
    for (std::uint64_t seed = 1; seed <= 25; ++seed) {
      SplitMix64 rng(seed * 7919);

      // Build per-stream arrival lists: drop, duplicate, and delay frames of 1..N.
      constexpr std::uint32_t kStreams = 3;
      std::vector<std::vector<std::uint64_t>> arrivals(kStreams);
      for (auto& list : arrivals) {
        const std::uint64_t n = 200 + rng.uniform(600);
        for (std::uint64_t q = 1; q <= n; ++q) {
          if (rng.chance_ppm(30'000)) continue;  // drop
          list.push_back(q);
          if (rng.chance_ppm(30'000)) list.push_back(q);  // duplicate
        }
        for (std::size_t i = 0; i + 1 < list.size(); ++i) {  // delay by up to 8 positions
          if (rng.chance_ppm(40'000)) {
            const std::size_t k = std::min<std::size_t>(1 + rng.uniform(8), list.size() - 1 - i);
            std::rotate(list.begin() + static_cast<std::ptrdiff_t>(i), list.begin() + static_cast<std::ptrdiff_t>(i) + 1,
                        list.begin() + static_cast<std::ptrdiff_t>(i + k) + 1);
          }
        }
      }

      SequencerConfig cfg;
      cfg.gap_policy = pol.g;
      cfg.late_policy = pol.l;
      cfg.max_missing_ranges = 1'000'000;  // no eviction: the model has no such concept
      Sequencer s(cfg);
      feedtest::Oracle oracle[kStreams];
      std::vector<std::size_t> next(kStreams, 0);

      for (;;) {  // random interleaving that preserves each stream's own order
        std::vector<std::uint32_t> live;
        for (std::uint32_t i = 0; i < kStreams; ++i) {
          if (next[i] < arrivals[i].size()) live.push_back(i);
        }
        if (live.empty()) break;
        const std::uint32_t sid = live[static_cast<std::size_t>(rng.uniform(live.size()))];
        const std::uint64_t seq = arrivals[sid][next[sid]++];

        std::uint64_t first = 0, count = 0;
        const Classification expect = oracle[sid].classify(seq, first, count);
        const Verdict v = s.observe(sid + 1, seq);
        CHECK(v.cls == expect);
        if (expect == Classification::Gap) {
          CHECK_EQ(v.gap_first, first);
          CHECK_EQ(v.gap_count, count);
        }
      }
      for (std::uint32_t i = 0; i < kStreams; ++i) {
        const auto info = s.stream(i + 1);
        CHECK(info.has_value());
        if (info) {
          CHECK_EQ(info->outstanding_seqs, oracle[i].missing.size());
          CHECK_EQ(info->next_expected, oracle[i].expected);
        }
      }
      check_loss_identity(s);
    }
  }
}
