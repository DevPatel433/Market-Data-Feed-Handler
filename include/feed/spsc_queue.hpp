#pragma once
// Bounded single-producer / single-consumer ring buffer. Normative description,
// memory-ordering argument and layout measurements: docs/spsc_queue.md.
//
// CONTRACT (violating it is undefined behaviour, not a recoverable error):
//   * exactly ONE thread calls try_push() (the producer) and exactly ONE thread
//     calls try_pop() (the consumer) for the lifetime of the queue. They may be
//     different threads over time only if a happens-before edge separates the hand-over.
//   * stats()/size_approx() may be called from any thread at any time.
//   * the queue never blocks, allocates after construction, or throws after construction.
//
// FULL / EMPTY: try_push returns false when `capacity()` elements are queued and leaves
// its argument untouched; try_pop returns false when empty and leaves `out` untouched.
// What to do about a full queue (drop, wait, abort) is the caller's policy; see queue_policy.hpp.
#include <algorithm>
#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <type_traits>
#include <utility>

namespace feed {

// Fixed rather than std::hardware_destructive_interference_size, whose value is
// not ABI-stable across compiler flags (GCC warns about exactly this).
inline constexpr std::size_t kCacheLine = 64;

// Layout/instrumentation choices. They never change correctness, only performance
// and what is reported. The default is the simplest layout: per the roadmap, cache-line
// separation and index caching are adopted only after measurement (benchmarks/bench_spsc_layout.cpp).
struct SpscOptions {
  bool pad_indices = false;         // put producer-owned and consumer-owned state on separate cache lines
  bool cache_remote_index = false;  // each side remembers the other's last-seen index; reloads only when needed
  bool track_high_water = true;     // producer records peak occupancy (costs one extra relaxed load per push)
};
inline constexpr SpscOptions kDefaultSpscOptions{};

struct SpscStats {
  std::uint64_t pushed = 0;        // successful pushes
  std::uint64_t popped = 0;        // successful pops
  std::uint64_t full_rejects = 0;  // failed push attempts (a spinning producer counts every attempt)
  std::uint64_t empty_polls = 0;   // failed pop attempts
  std::uint64_t high_water = 0;    // peak occupancy seen by the producer; 0 if tracking is off
  std::uint64_t occupancy = 0;     // pushed - popped at the time of the snapshot
};

namespace detail {
// State written only by the producer / only by the consumer. Counters are atomics
// with a single writer (plain load + store, no read-modify-write) so any thread can read them.
template <std::size_t Align>
struct alignas(Align) ProducerSide {
  std::atomic<std::uint64_t> tail{0};  // next index to write; published with release
  std::uint64_t head_cache = 0;        // producer's last view of the consumer's head (non-atomic: producer-only)
  std::atomic<std::uint64_t> full_rejects{0};
  std::atomic<std::uint64_t> high_water{0};
};
template <std::size_t Align>
struct alignas(Align) ConsumerSide {
  std::atomic<std::uint64_t> head{0};  // next index to read; published with release
  std::uint64_t tail_cache = 0;        // consumer's last view of the producer's tail (consumer-only)
  std::atomic<std::uint64_t> empty_polls{0};
};
}  // namespace detail

template <class T, SpscOptions Opt = kDefaultSpscOptions>
class SpscQueue {
  static_assert(std::is_default_constructible_v<T>, "slots are default-constructed up front");
  static_assert(std::is_nothrow_move_assignable_v<T>, "push/pop assign into slots and must not throw");

 public:
  using value_type = T;
  static constexpr SpscOptions options = Opt;
  static constexpr std::size_t kMaxCapacity = std::size_t{1} << 30;

  // Capacity is rounded UP to a power of two (index masking), and is exact: a queue
  // built with capacity() == N holds N elements, not N-1.
  // `start_index` is a test hook: indices are 64-bit and monotonically increasing, so
  // starting near 2^64 exercises wraparound arithmetic that would otherwise take centuries.
  explicit SpscQueue(std::size_t min_capacity, std::uint64_t start_index = 0)
      : cap_(checked_capacity(min_capacity)), mask_(cap_ - 1), start_(start_index), buf_(new T[cap_]()) {
    p_.tail.store(start_index, std::memory_order_relaxed);
    p_.head_cache = start_index;
    c_.head.store(start_index, std::memory_order_relaxed);
    c_.tail_cache = start_index;
  }
  SpscQueue(const SpscQueue&) = delete;
  SpscQueue& operator=(const SpscQueue&) = delete;

  // Producer thread only. On failure (full) `v` is untouched, so it can be retried.
  bool try_push(const T& v) noexcept { return push_impl(v); }
  bool try_push(T&& v) noexcept { return push_impl(std::move(v)); }

  // Consumer thread only. On failure (empty) `out` is untouched.
  bool try_pop(T& out) noexcept {
    const std::uint64_t h = c_.head.load(std::memory_order_relaxed);  // only this thread writes head
    std::uint64_t t;
    if constexpr (Opt.cache_remote_index) {
      t = c_.tail_cache;
      if (h == t) {
        t = p_.tail.load(std::memory_order_acquire);
        c_.tail_cache = t;
      }
    } else {
      t = p_.tail.load(std::memory_order_acquire);  // pairs with the producer's release store of tail
    }
    if (h == t) {
      bump(c_.empty_polls);
      return false;
    }
    out = std::move(buf_[h & mask_]);
    c_.head.store(h + 1, std::memory_order_release);  // slot is free: producer's acquire load of head sees this
    return true;
  }

  std::size_t capacity() const noexcept { return cap_; }

  // Any thread. A racy snapshot: always within [0, capacity()], exact when the queue is quiescent.
  std::size_t size_approx() const noexcept {
    const std::uint64_t h = c_.head.load(std::memory_order_acquire);  // head first: head <= tail at every instant,
    const std::uint64_t t = p_.tail.load(std::memory_order_acquire);  // so t - h cannot underflow
    return static_cast<std::size_t>(std::min<std::uint64_t>(t - h, cap_));
  }

  // Any thread. Individual fields are consistent; the set is a best-effort snapshot, not atomic as a whole.
  SpscStats stats() const noexcept {
    SpscStats s;
    const std::uint64_t h = c_.head.load(std::memory_order_acquire);
    const std::uint64_t t = p_.tail.load(std::memory_order_acquire);
    s.pushed = t - start_;
    s.popped = h - start_;
    s.occupancy = std::min<std::uint64_t>(t - h, cap_);
    s.full_rejects = p_.full_rejects.load(std::memory_order_relaxed);
    s.empty_polls = c_.empty_polls.load(std::memory_order_relaxed);
    s.high_water = p_.high_water.load(std::memory_order_relaxed);
    return s;
  }

 private:
  static std::size_t checked_capacity(std::size_t n) {
    if (n == 0) throw std::invalid_argument("SpscQueue: capacity must be > 0");
    if (n > kMaxCapacity) throw std::length_error("SpscQueue: capacity too large");
    return std::bit_ceil(n);
  }

  // Single-writer counter: plain load + store (no RMW), so it is cheap and still race-free to read elsewhere.
  static void bump(std::atomic<std::uint64_t>& c) noexcept {
    c.store(c.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed);
  }

  template <class U>
  bool push_impl(U&& v) noexcept {
    const std::uint64_t t = p_.tail.load(std::memory_order_relaxed);  // only this thread writes tail
    if constexpr (Opt.cache_remote_index) {
      if (t - p_.head_cache >= cap_) {
        p_.head_cache = c_.head.load(std::memory_order_acquire);
        if (t - p_.head_cache >= cap_) {
          bump(p_.full_rejects);
          return false;
        }
      }
    } else {
      // acquire pairs with the consumer's release store of head: the consumer has finished
      // reading the slot we are about to overwrite.
      if (t - c_.head.load(std::memory_order_acquire) >= cap_) {
        bump(p_.full_rejects);
        return false;
      }
    }
    buf_[t & mask_] = std::forward<U>(v);
    p_.tail.store(t + 1, std::memory_order_release);  // publishes the slot write to the consumer's acquire load

    if constexpr (Opt.track_high_water) {
      // Occupancy right after this push. A relaxed (possibly slightly stale) head can only make
      // this an overestimate, by pops completing during the push itself; never above capacity.
      const std::uint64_t occ = std::min<std::uint64_t>(t + 1 - c_.head.load(std::memory_order_relaxed), cap_);
      if (occ > p_.high_water.load(std::memory_order_relaxed)) p_.high_water.store(occ, std::memory_order_relaxed);
    }
    return true;
  }

  static constexpr std::size_t kSideAlign = Opt.pad_indices ? kCacheLine : alignof(std::uint64_t);

  // Read-only after construction (shared by both threads, never written).
  const std::size_t cap_;
  const std::size_t mask_;
  const std::uint64_t start_;
  const std::unique_ptr<T[]> buf_;
  // Producer-written, then consumer-written. With pad_indices each starts on its own cache line.
  detail::ProducerSide<kSideAlign> p_;
  detail::ConsumerSide<kSideAlign> c_;
};

}  // namespace feed
