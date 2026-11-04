#include <algorithm>
#include <set>

#include "check.hpp"
#include "feed/protocol.hpp"
#include "feed/workload.hpp"

using namespace feed;

namespace {
WorkloadConfig cfg(Profile p, std::uint64_t seed = 42, std::uint64_t count = 5000) {
  WorkloadConfig c;
  c.profile = p;
  c.seed = seed;
  c.count = count;
  return c;
}

std::vector<std::uint8_t> image(const Workload& w) {
  std::vector<std::uint8_t> img;
  CHECK(serialize_replay(w.records, img));
  return img;
}

std::vector<std::uint64_t> seqs(const Workload& w) {
  std::vector<std::uint64_t> s;
  for (const auto& r : w.records) {
    if (const auto h = peek_header(r.frame)) s.push_back(h->seq);
  }
  return s;
}
}  // namespace

TEST(workload_same_seed_is_byte_identical_for_every_profile) {
  for (const Profile p : {Profile::Normal, Profile::Bursty, Profile::Malformed, Profile::Duplicate, Profile::Gap,
                          Profile::Reorder}) {
    CHECK(image(generate_workload(cfg(p))) == image(generate_workload(cfg(p))));
  }
}

TEST(workload_different_seed_differs) {
  CHECK(image(generate_workload(cfg(Profile::Normal, 1))) != image(generate_workload(cfg(Profile::Normal, 2))));
}

TEST(workload_normal_is_gapless_and_in_order) {
  const auto w = generate_workload(cfg(Profile::Normal));
  CHECK_EQ(w.records.size(), 5000U);
  const auto s = seqs(w);
  CHECK_EQ(s.size(), 5000U);
  for (std::size_t i = 0; i < s.size(); ++i) CHECK_EQ(s[i], i + 1);
  for (const auto& r : w.records) CHECK(r.frame.size() <= kMaxMessageSize);
}

TEST(workload_clean_stream_is_identical_across_profiles_for_same_seed) {
  // Faults use an independent RNG, so un-faulted content must not depend on profile.
  auto c = cfg(Profile::Duplicate);
  c.fault_ppm = 0;
  CHECK(image(generate_workload(c)) == image(generate_workload(cfg(Profile::Normal))));
}

TEST(workload_duplicate_manifest_matches_stream) {
  const auto w = generate_workload(cfg(Profile::Duplicate));
  CHECK(w.stats.duplicated > 0);
  CHECK_EQ(w.stats.frames_out, w.stats.clean_messages + w.stats.duplicated);
  const auto s = seqs(w);
  const std::set<std::uint64_t> unique(s.begin(), s.end());
  CHECK_EQ(s.size() - unique.size(), w.stats.duplicated);
}

TEST(workload_gap_manifest_matches_stream) {
  const auto w = generate_workload(cfg(Profile::Gap));
  CHECK(w.stats.dropped > 0);
  CHECK_EQ(w.stats.frames_out + w.stats.dropped, w.stats.clean_messages);
  const auto s = seqs(w);
  CHECK_EQ(s.front(), 1U);  // first frame is never dropped
  std::uint64_t missing = 0;
  for (std::size_t i = 1; i < s.size(); ++i) {
    CHECK(s[i] > s[i - 1]);
    missing += s[i] - s[i - 1] - 1;
  }
  CHECK_EQ(missing + (w.stats.clean_messages - s.back()), w.stats.dropped);
}

TEST(workload_reorder_keeps_all_frames_but_breaks_order) {
  const auto w = generate_workload(cfg(Profile::Reorder));
  CHECK(w.stats.reordered > 0);
  CHECK_EQ(w.stats.frames_out, w.stats.clean_messages);
  auto s = seqs(w);
  CHECK(!std::is_sorted(s.begin(), s.end()));
  std::sort(s.begin(), s.end());
  for (std::size_t i = 0; i < s.size(); ++i) CHECK_EQ(s[i], i + 1);
}

TEST(workload_malformed_produces_frames_the_header_peek_rejects) {
  const auto w = generate_workload(cfg(Profile::Malformed));
  CHECK(w.stats.malformed > 0);
  CHECK_EQ(w.stats.frames_out, w.stats.clean_messages);
  std::uint64_t bad = 0;
  for (const auto& r : w.records) bad += peek_header(r.frame) ? 0U : 1U;
  CHECK(bad > 0);
  CHECK(bad <= w.stats.malformed);  // some mutations (e.g. oversize) keep a valid header
}

TEST(workload_bursty_preserves_mean_rate_and_has_idle_gaps) {
  auto c = cfg(Profile::Bursty, 7, 10'000);
  c.rate_per_sec = 100'000;
  c.burst_size = 1000;
  c.burst_multiplier = 20;
  const auto w = generate_workload(c);
  auto n = generate_workload(cfg(Profile::Normal, 7, 10'000));

  // Burst k starts exactly where normal pacing would place message k*burst_size.
  for (std::uint64_t k = 0; k < 10; ++k) CHECK_EQ(w.records[k * 1000].offset_ns, n.records[k * 1000].offset_ns);

  // Largest inter-message gap is the idle period; it dwarfs the in-burst spacing.
  std::uint64_t max_gap = 0, max_in_burst = 0;
  for (std::size_t i = 1; i < w.records.size(); ++i) {
    const auto gap = w.records[i].offset_ns - w.records[i - 1].offset_ns;
    max_gap = std::max(max_gap, gap);
    if (i % 1000 != 0) max_in_burst = std::max(max_in_burst, gap);
  }
  CHECK(max_gap > 10 * max_in_burst);
}

TEST(workload_offsets_are_non_decreasing_so_files_are_valid) {
  for (const Profile p : {Profile::Normal, Profile::Bursty, Profile::Reorder, Profile::Duplicate}) {
    const auto w = generate_workload(cfg(p));
    std::vector<std::uint8_t> img = image(w);
    std::vector<ReplayRecord> back;
    CHECK(parse_replay(img, back) == ReplayError::None);
    CHECK_EQ(back.size(), w.records.size());
  }
}
