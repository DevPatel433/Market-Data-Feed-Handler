#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <set>
#include <vector>

#include "check.hpp"
#include "feed/decoder.hpp"
#include "feed/rng.hpp"
#include "feed/workload.hpp"

using namespace feed;

namespace {

using Bytes = std::vector<std::uint8_t>;

Bytes enc(const Message& m) {
  Bytes b;
  encode(m, b);
  return b;
}

// Copies the frame into an allocation of EXACTLY its size so AddressSanitizer
// flags any read past the end (a std::vector's spare capacity would hide it).
DecodeError decode_exact(const Bytes& f, Message& out) {
  std::unique_ptr<std::uint8_t[]> buf(new std::uint8_t[f.empty() ? 1 : f.size()]);
  std::copy(f.begin(), f.end(), buf.get());
  return decode(std::span<const std::uint8_t>(buf.get(), f.size()), out);
}

DecodeError decode_only(const Bytes& f) {
  Message m;
  return decode_exact(f, m);
}

Message sentinel() {
  Message m;
  m.stream_id = 0xDEADBEEF;
  m.seq = 0xFEEDFACECAFEBEEFULL;
  m.body = CancelOrder{123, 456};
  return m;
}

// Core invariant, applied to every input in the fuzz-style tests:
//   success => encode(decoded) == input (canonical); failure => `out` untouched.
DecodeError check_invariants(const Bytes& f) {
  Message out = sentinel();
  const DecodeError e = decode_exact(f, out);
  if (e == DecodeError::None) {
    CHECK(enc(out) == f);
  } else {
    CHECK(out == sentinel());
  }
  return e;
}

std::uint64_t nonzero(SplitMix64& r) {
  const std::uint64_t v = r.next();
  return v == 0 ? 1 : v;
}
std::int64_t positive(SplitMix64& r) { return static_cast<std::int64_t>((r.next() >> 1U) | 1U); }
std::uint32_t nonzero32(SplitMix64& r) {
  const auto v = static_cast<std::uint32_t>(r.next());
  return v == 0 ? 1U : v;
}
Side side(SplitMix64& r) { return r.uniform(2) == 0 ? Side::Buy : Side::Sell; }

Message random_valid(SplitMix64& r) {
  Message m;
  m.stream_id = static_cast<std::uint32_t>(r.next());
  m.seq = nonzero(r);
  switch (r.uniform(4)) {
    case 0: m.body = AddOrder{nonzero(r), positive(r), static_cast<std::uint32_t>(r.next()), nonzero32(r), side(r)}; break;
    case 1: m.body = CancelOrder{nonzero(r), static_cast<std::uint32_t>(r.next())}; break;
    case 2: m.body = Trade{nonzero(r), positive(r), static_cast<std::uint32_t>(r.next()), nonzero32(r), side(r)}; break;
    default: m.body = Heartbeat{}; break;
  }
  return m;
}

std::array<Message, 4> one_of_each() {
  return {Message{1, 1, AddOrder{1000, 10050, 7, 100, Side::Buy}},
          Message{1, 2, CancelOrder{1000, 7}},
          Message{1, 3, Trade{55, 10049, 7, 40, Side::Sell}},
          Message{1, 4, Heartbeat{}}};
}

void mutate_random(Bytes& f, SplitMix64& r) {
  const std::uint64_t rounds = 1 + r.uniform(4);
  for (std::uint64_t k = 0; k < rounds; ++k) {
    switch (r.uniform(6)) {
      case 0:
        if (!f.empty()) f[r.uniform(f.size())] ^= static_cast<std::uint8_t>(1U << r.uniform(8));
        break;
      case 1:
        if (!f.empty()) f[r.uniform(f.size())] = static_cast<std::uint8_t>(r.next());
        break;
      case 2:
        f.insert(f.begin() + static_cast<std::ptrdiff_t>(r.uniform(f.size() + 1)), static_cast<std::uint8_t>(r.next()));
        break;
      case 3:
        if (!f.empty()) f.erase(f.begin() + static_cast<std::ptrdiff_t>(r.uniform(f.size())));
        break;
      case 4:
        f.resize(r.uniform(f.size() + 1));
        break;
      default: {
        const std::uint64_t extra = 1 + r.uniform(70);
        for (std::uint64_t i = 0; i < extra; ++i) f.push_back(static_cast<std::uint8_t>(r.next()));
        break;
      }
    }
  }
}

}  // namespace

TEST(decoder_round_trips_each_type) {
  for (const Message& m : one_of_each()) {
    Message out = sentinel();
    CHECK(decode_exact(enc(m), out) == DecodeError::None);
    CHECK(out == m);
  }
}

TEST(decoder_round_trips_boundary_values) {
  constexpr auto u64max = std::numeric_limits<std::uint64_t>::max();
  constexpr auto i64max = std::numeric_limits<std::int64_t>::max();
  constexpr auto u32max = std::numeric_limits<std::uint32_t>::max();
  const Message cases[] = {
      {u32max, u64max, AddOrder{u64max, i64max, u32max, u32max, Side::Sell}},
      {0, 1, AddOrder{1, 1, 0, 1, Side::Buy}},
      {u32max, u64max, CancelOrder{u64max, u32max}},
      {0, 1, Trade{u64max, i64max, u32max, u32max, Side::Sell}},
      {u32max, u64max, Heartbeat{}},
  };
  for (const Message& m : cases) {
    Message out = sentinel();
    CHECK(decode_exact(enc(m), out) == DecodeError::None);
    CHECK(out == m);
  }
}

TEST(decoder_round_trips_random_valid_messages) {
  SplitMix64 rng(1234);
  for (int i = 0; i < 50'000; ++i) {
    const Message m = random_valid(rng);
    Message out = sentinel();
    CHECK(decode_exact(enc(m), out) == DecodeError::None);
    CHECK(out == m);
  }
}

TEST(decoder_rejects_each_reason) {
  const auto msgs = one_of_each();

  // too_short / too_large
  CHECK(decode_only({}) == DecodeError::TooShort);
  CHECK(decode_only(Bytes(15, 1)) == DecodeError::TooShort);
  Bytes big = enc(msgs[3]);
  big.resize(kMaxMessageSize + 1, 0);
  CHECK(decode_only(big) == DecodeError::TooLarge);

  // bad_version
  for (const Message& m : msgs) {
    for (const int v : {0, 2, 255}) {
      Bytes f = enc(m);
      f[0] = static_cast<std::uint8_t>(v);
      CHECK(decode_only(f) == DecodeError::BadVersion);
    }
  }

  // unknown_type
  for (const int t : {0, 5, 6, 127, 255}) {
    Bytes f = enc(msgs[0]);
    f[1] = static_cast<std::uint8_t>(t);
    CHECK(decode_only(f) == DecodeError::UnknownType);
  }

  // length_mismatch: lying payload_len (both directions), wrong-for-type, and trailing bytes
  for (const Message& m : msgs) {
    Bytes f = enc(m);
    f[3] = static_cast<std::uint8_t>(f[3] + 1);
    CHECK(decode_only(f) == DecodeError::LengthMismatch);
    Bytes g = enc(m);
    g[2] = 0xFF;  // huge declared length
    CHECK(decode_only(g) == DecodeError::LengthMismatch);
    Bytes h = enc(m);
    h.push_back(0);
    CHECK(decode_only(h) == DecodeError::LengthMismatch);
  }
  {  // type says Cancel (12 bytes) but frame/payload_len describe an Add (25 bytes)
    Bytes f = enc(msgs[0]);
    f[1] = static_cast<std::uint8_t>(MsgType::Cancel);
    CHECK(decode_only(f) == DecodeError::LengthMismatch);
  }

  // bad_field: every documented range, via encode() which does not validate
  const auto bad = [](Message m) { return decode_only(enc(m)) == DecodeError::BadField; };
  for (const Message& base : msgs) {
    Message m = base;
    m.seq = 0;
    CHECK(bad(m));
  }
  CHECK(bad({1, 1, AddOrder{0, 100, 1, 1, Side::Buy}}));
  CHECK(bad({1, 1, AddOrder{1, 0, 1, 1, Side::Buy}}));
  CHECK(bad({1, 1, AddOrder{1, -5, 1, 1, Side::Buy}}));
  CHECK(bad({1, 1, AddOrder{1, std::numeric_limits<std::int64_t>::min(), 1, 1, Side::Buy}}));
  CHECK(bad({1, 1, AddOrder{1, 100, 1, 0, Side::Buy}}));
  CHECK(bad({1, 1, AddOrder{1, 100, 1, 1, static_cast<Side>(2)}}));
  CHECK(bad({1, 1, AddOrder{1, 100, 1, 1, static_cast<Side>(255)}}));
  CHECK(bad({1, 1, CancelOrder{0, 1}}));
  CHECK(bad({1, 1, Trade{0, 100, 1, 1, Side::Buy}}));
  CHECK(bad({1, 1, Trade{1, 0, 1, 1, Side::Buy}}));
  CHECK(bad({1, 1, Trade{1, -1, 1, 1, Side::Buy}}));
  CHECK(bad({1, 1, Trade{1, 100, 1, 0, Side::Buy}}));
  CHECK(bad({1, 1, Trade{1, 100, 1, 1, static_cast<Side>(3)}}));
}

TEST(decoder_error_precedence_matches_documented_order) {
  const Bytes valid = enc(Message{1, 1, AddOrder{1, 100, 1, 1, Side::Buy}});

  Bytes f = valid;  // too_large beats bad_version
  f[0] = 9;
  f.resize(kMaxMessageSize + 5, 0);
  CHECK(decode_only(f) == DecodeError::TooLarge);

  f = valid;  // bad_version beats unknown_type and length_mismatch
  f[0] = 9;
  f[1] = 99;
  f[3] = 0;
  CHECK(decode_only(f) == DecodeError::BadVersion);

  f = valid;  // unknown_type beats length_mismatch
  f[1] = 99;
  f[3] = 0;
  CHECK(decode_only(f) == DecodeError::UnknownType);

  f = valid;  // length_mismatch beats bad_field (seq == 0 as well)
  for (std::size_t i = 8; i < 16; ++i) f[i] = 0;
  f[3] = 1;
  CHECK(decode_only(f) == DecodeError::LengthMismatch);
}

TEST(decoder_rejects_every_truncation_and_extension) {
  for (const Message& m : one_of_each()) {
    const Bytes full = enc(m);
    for (std::size_t n = 0; n < full.size(); ++n) {
      const Bytes cut(full.begin(), full.begin() + static_cast<std::ptrdiff_t>(n));
      const DecodeError e = decode_only(cut);
      CHECK(e == (n < kHeaderSize ? DecodeError::TooShort : DecodeError::LengthMismatch));
    }
    for (std::size_t extra = 1; extra <= 2 * kMaxMessageSize; ++extra) {
      Bytes longer = full;
      longer.resize(full.size() + extra, 0xAB);
      const DecodeError e = decode_only(longer);
      CHECK(e == (longer.size() > kMaxMessageSize ? DecodeError::TooLarge : DecodeError::LengthMismatch));
    }
  }
}

TEST(decoder_exhaustive_single_byte_mutations) {
  // Every byte position x every value, for one frame of each type.
  std::uint64_t accepted = 0;
  for (const Message& m : one_of_each()) {
    const Bytes base = enc(m);
    for (std::size_t pos = 0; pos < base.size(); ++pos) {
      for (int v = 0; v < 256; ++v) {
        Bytes f = base;
        f[pos] = static_cast<std::uint8_t>(v);
        if (check_invariants(f) == DecodeError::None) ++accepted;
      }
    }
  }
  CHECK(accepted > 0);  // e.g. changing stream_id or a price byte stays valid
}

TEST(decoder_random_mutation_fuzz) {
  SplitMix64 rng(0xFEED);
  std::set<DecodeError> seen;
  std::uint64_t accepted = 0;
  constexpr int kIterations = 200'000;
  for (int i = 0; i < kIterations; ++i) {
    Bytes f = enc(random_valid(rng));
    mutate_random(f, rng);
    const DecodeError e = check_invariants(f);
    seen.insert(e);
    if (e == DecodeError::None) ++accepted;
  }
  CHECK(accepted > 0);
  CHECK(accepted < static_cast<std::uint64_t>(kIterations));
  CHECK_EQ(seen.size(), kDecodeErrorCount);  // the run exercised every outcome, None included
}

TEST(decoder_survives_pure_random_bytes) {
  SplitMix64 rng(99);
  for (int i = 0; i < 100'000; ++i) {
    Bytes f(static_cast<std::size_t>(rng.uniform(2 * kMaxMessageSize)));
    for (auto& b : f) b = static_cast<std::uint8_t>(rng.next());
    check_invariants(f);
  }
}

TEST(decoder_leaves_out_untouched_on_error) {
  Message out = sentinel();
  Bytes f = enc(Message{1, 1, Heartbeat{}});
  f[0] = 7;
  CHECK(decode_exact(f, out) == DecodeError::BadVersion);
  CHECK(out == sentinel());
}

TEST(decoder_error_names_are_distinct) {
  std::set<std::string> names;
  for (std::size_t i = 0; i < kDecodeErrorCount; ++i) names.insert(to_string(static_cast<DecodeError>(i)));
  CHECK_EQ(names.size(), kDecodeErrorCount);
}

TEST(decoder_agrees_with_generated_workloads) {
  // The generator's ground-truth manifest must match what the decoder says.
  for (const Profile p : {Profile::Normal, Profile::Bursty, Profile::Duplicate, Profile::Gap, Profile::Reorder,
                          Profile::Malformed}) {
    WorkloadConfig cfg;
    cfg.profile = p;
    cfg.seed = 31;
    cfg.count = 20'000;
    const Workload w = generate_workload(cfg);
    std::uint64_t rejected = 0;
    for (const auto& rec : w.records) {
      if (check_invariants(rec.frame) != DecodeError::None) ++rejected;
    }
    CHECK_EQ(rejected, p == Profile::Malformed ? w.stats.malformed : 0U);
  }
}
