#pragma once
// What the producer does when the queue is full. The queue itself only reports "full".
// Normative description: docs/spsc_queue.md §4.
#include <atomic>
#include <chrono>
#include <cstdint>
#include <thread>
#include <utility>

namespace feed {

enum class QueueFullPolicy : std::uint8_t {
  DropAndCount,  // never wait: discard the new event and count it as queue_overflow
  Block,         // backpressure: wait until space appears. Only valid where the source can be paused (file replay).
  FailFast       // treat a full queue as a fatal error: report it and let the caller stop
};

enum class EnqueueResult : std::uint8_t { Enqueued, Dropped, Full, Aborted };

constexpr const char* to_string(QueueFullPolicy p) {
  switch (p) {
    case QueueFullPolicy::DropAndCount: return "drop";
    case QueueFullPolicy::Block: return "block";
    case QueueFullPolicy::FailFast: return "fail";
  }
  return "unknown";
}

// Always: offered == enqueued + dropped + aborted. A fail-fast failure counts as dropped
// (the event was not queued) and is also reported to the caller as EnqueueResult::Full.
struct OverflowCounters {
  std::uint64_t offered = 0;
  std::uint64_t enqueued = 0;
  std::uint64_t dropped = 0;         // queue_overflow in docs/metrics.md
  std::uint64_t aborted = 0;         // Block interrupted by the abort flag before space appeared
  std::uint64_t stalled_events = 0;  // Block: events that had to wait at least once
  std::uint64_t stall_spins = 0;     // Block: failed push attempts while waiting
  std::uint64_t stall_ns = 0;        // Block: total wall time the producer spent waiting for space
};

// Producer-thread only (it calls try_push). `abort` (optional) lets another thread release a blocked producer.
template <class Queue, class T>
EnqueueResult enqueue(Queue& q, T&& item, QueueFullPolicy policy, OverflowCounters& c,
                      const std::atomic<bool>* abort = nullptr) {
  ++c.offered;
  if (q.try_push(std::forward<T>(item))) {  // a failed push leaves `item` untouched, so retries are safe
    ++c.enqueued;
    return EnqueueResult::Enqueued;
  }
  switch (policy) {
    case QueueFullPolicy::DropAndCount:
      ++c.dropped;
      return EnqueueResult::Dropped;
    case QueueFullPolicy::FailFast:
      ++c.dropped;
      return EnqueueResult::Full;
    case QueueFullPolicy::Block:
      break;
  }
  ++c.stalled_events;
  const auto began = std::chrono::steady_clock::now();
  const auto stalled_for = [&] {
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - began).count());
  };
  for (std::uint32_t spins = 0;; ++spins) {
    ++c.stall_spins;
    if (abort != nullptr && abort->load(std::memory_order_relaxed)) {
      ++c.aborted;
      c.stall_ns += stalled_for();
      return EnqueueResult::Aborted;
    }
    if (spins >= 32) std::this_thread::yield();  // never starve the consumer on a single core
    if (q.try_push(std::forward<T>(item))) {
      ++c.enqueued;
      c.stall_ns += stalled_for();
      return EnqueueResult::Enqueued;
    }
  }
}

}  // namespace feed
