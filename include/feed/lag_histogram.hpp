#pragma once
// Fixed-memory histogram for latency-like values (nanoseconds). Values below 8 are exact; above that each
// power of two is split into 8 linear sub-buckets, so a reported percentile is an UPPER BOUND at most
// 12.5% above the true value (and never above the recorded maximum). No allocation after construction,
// O(1) record, which is what a consumer hot path needs.
#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>

namespace feed {

class LogHistogram {
 public:
  static constexpr unsigned kSubBits = 3;
  static constexpr std::size_t kSub = std::size_t{1} << kSubBits;
  static constexpr std::size_t kBuckets = (64 - kSubBits + 1) * kSub;  // 496

  void record(std::uint64_t v) noexcept {
    ++buckets_[index_of(v)];
    ++count_;
    sum_ += v;
    min_ = std::min(min_, v);
    max_ = std::max(max_, v);
  }

  std::uint64_t count() const noexcept { return count_; }
  std::uint64_t min() const noexcept { return count_ == 0 ? 0 : min_; }
  std::uint64_t max() const noexcept { return max_; }
  double mean() const noexcept { return count_ == 0 ? 0.0 : static_cast<double>(sum_) / static_cast<double>(count_); }

  // p in [0, 100]. Returns 0 for an empty histogram. p == 100 returns the exact maximum, p == 0 the exact minimum.
  std::uint64_t percentile(double p) const noexcept {
    if (count_ == 0) return 0;
    if (p <= 0.0) return min();
    if (p >= 100.0) return max_;
    auto rank = static_cast<std::uint64_t>(p / 100.0 * static_cast<double>(count_));
    if (static_cast<double>(rank) < p / 100.0 * static_cast<double>(count_)) ++rank;  // ceil
    rank = std::max<std::uint64_t>(rank, 1);
    std::uint64_t seen = 0;
    for (std::size_t i = 0; i < kBuckets; ++i) {
      seen += buckets_[i];
      if (seen >= rank) return std::min(upper_bound_of(i), max_);
    }
    return max_;
  }

  void merge(const LogHistogram& o) noexcept {
    for (std::size_t i = 0; i < kBuckets; ++i) buckets_[i] += o.buckets_[i];
    count_ += o.count_;
    sum_ += o.sum_;
    min_ = std::min(min_, o.min_);
    max_ = std::max(max_, o.max_);
  }

  static std::size_t index_of(std::uint64_t v) noexcept {
    if (v < kSub) return static_cast<std::size_t>(v);
    const unsigned msb = 63U - static_cast<unsigned>(std::countl_zero(v));
    const std::uint64_t sub = (v >> (msb - kSubBits)) & (kSub - 1);
    return static_cast<std::size_t>(msb - kSubBits + 1) * kSub + static_cast<std::size_t>(sub);
  }

  static std::uint64_t upper_bound_of(std::size_t idx) noexcept {
    if (idx < kSub) return idx;
    const std::size_t group = idx / kSub;
    const unsigned msb = static_cast<unsigned>(group) + kSubBits - 1;
    const std::uint64_t width = std::uint64_t{1} << (msb - kSubBits);
    const std::uint64_t lower = (std::uint64_t{1} << msb) + (idx % kSub) * width;
    return lower + (width - 1);
  }

 private:
  std::array<std::uint64_t, kBuckets> buckets_{};
  std::uint64_t count_ = 0;
  std::uint64_t sum_ = 0;
  std::uint64_t min_ = ~std::uint64_t{0};
  std::uint64_t max_ = 0;
};

}  // namespace feed
