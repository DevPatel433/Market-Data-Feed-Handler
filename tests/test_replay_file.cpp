#include <vector>

#include "check.hpp"
#include "feed/replay_file.hpp"
#include "feed/replayer.hpp"

using namespace feed;

namespace {
std::vector<ReplayRecord> sample() {
  return {{0, {1, 2, 3}}, {1000, {}}, {1000, {9}}, {5'000'000'000ULL, {7, 7, 7, 7}}};
}
}  // namespace

TEST(replay_file_round_trip) {
  std::vector<std::uint8_t> img;
  CHECK(serialize_replay(sample(), img));
  std::vector<ReplayRecord> back;
  CHECK(parse_replay(img, back) == ReplayError::None);
  const auto orig = sample();
  CHECK_EQ(back.size(), orig.size());
  for (std::size_t i = 0; i < back.size() && i < orig.size(); ++i) {
    CHECK_EQ(back[i].offset_ns, orig[i].offset_ns);
    CHECK(back[i].frame == orig[i].frame);
  }
}

TEST(replay_file_rejects_bad_magic_and_truncation) {
  std::vector<std::uint8_t> img;
  CHECK(serialize_replay(sample(), img));
  std::vector<ReplayRecord> out;

  auto bad = img;
  bad[0] = 'X';
  CHECK(parse_replay(bad, out) == ReplayError::BadMagic);
  CHECK(parse_replay(std::span(img).first(3), out) == ReplayError::BadMagic);

  // Every proper prefix that cuts a record in half must be rejected, never crash.
  for (std::size_t n = 9; n < img.size(); ++n) {
    const auto err = parse_replay(std::span(img).first(n), out);
    CHECK(err == ReplayError::None || err == ReplayError::TruncatedRecord);
  }
  CHECK(parse_replay(std::span(img).first(img.size() - 1), out) == ReplayError::TruncatedRecord);
}

TEST(replay_file_rejects_decreasing_offsets) {
  std::vector<ReplayRecord> recs = {{10, {1}}, {5, {2}}};
  std::vector<std::uint8_t> img;
  CHECK(serialize_replay(recs, img));
  std::vector<ReplayRecord> out;
  CHECK(parse_replay(img, out) == ReplayError::NonMonotonicOffset);
}

TEST(replay_file_rejects_oversized_frame_on_write) {
  std::vector<ReplayRecord> recs = {{0, std::vector<std::uint8_t>(70000, 0)}};
  std::vector<std::uint8_t> img;
  CHECK(!serialize_replay(recs, img));
}

TEST(replayer_max_speed_preserves_order_and_counts) {
  const auto recs = sample();
  std::vector<std::vector<std::uint8_t>> seen;
  const auto st = replay(recs, {}, [&](std::span<const std::uint8_t> f) { seen.emplace_back(f.begin(), f.end()); });
  CHECK_EQ(st.frames, recs.size());
  CHECK_EQ(st.bytes, 3U + 0U + 1U + 4U);
  CHECK_EQ(seen.size(), recs.size());
  for (std::size_t i = 0; i < seen.size() && i < recs.size(); ++i) CHECK(seen[i] == recs[i].frame);
}

TEST(replayer_timed_mode_honours_offsets) {
  // 3 frames spanning 30 ms of schedule at speed 1 must take at least ~30 ms.
  std::vector<ReplayRecord> recs = {{0, {1}}, {15'000'000, {2}}, {30'000'000, {3}}};
  ReplayOptions opts;
  opts.mode = ReplayMode::Timed;
  const auto st = replay(recs, opts, [](std::span<const std::uint8_t>) {});
  CHECK_EQ(st.frames, 3U);
  CHECK(st.wall_ns >= 29'000'000ULL);  // sleeps never return early; allow 1 ms for clock rounding

  // Speed 1000x collapses the same schedule to ~30 us.
  opts.speed = 1000.0;
  const auto fast = replay(recs, opts, [](std::span<const std::uint8_t>) {});
  CHECK(fast.wall_ns < st.wall_ns);
}
