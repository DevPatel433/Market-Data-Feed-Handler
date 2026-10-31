#pragma once
// SplitMix64: tiny, fast, fully specified PRNG. We avoid <random> distributions
// because their output is implementation-defined, which would break the
// "same seed => byte-identical workload on every compiler" guarantee.
#include <cstdint>

namespace feed {

class SplitMix64 {
 public:
  explicit SplitMix64(std::uint64_t seed) : state_(seed) {}

  std::uint64_t next() {
    state_ += 0x9E3779B97F4A7C15ULL;
    std::uint64_t z = state_;
    z = (z ^ (z >> 30U)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27U)) * 0x94D049BB133111EBULL;
    return z ^ (z >> 31U);
  }

  // Uniform-ish in [0, n). Modulo bias is negligible for the small n used here
  // and, more importantly, is deterministic. Returns 0 when n == 0.
  std::uint64_t uniform(std::uint64_t n) { return n == 0 ? 0 : next() % n; }

  // True with probability ppm / 1,000,000.
  bool chance_ppm(std::uint32_t ppm) { return uniform(1'000'000) < ppm; }

 private:
  std::uint64_t state_;
};

}  // namespace feed
