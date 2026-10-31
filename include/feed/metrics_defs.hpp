#pragma once
// Code-level form of the definitions in docs/metrics.md. Later phases fill these
// in at runtime; Phase 0 fixes their meaning so results stay comparable.
#include <cstdint>
#include <string_view>

namespace feed {

// Every frame that does not reach the consumer is attributed to exactly one
// category. Network loss and queue overflow are never merged into one counter.
enum class LossKind : std::uint8_t {
  NetworkGap,         // sequence gap: frame never arrived (inferred, not observed)
  Rejected,           // arrived but failed decode/validation
  QueueOverflow,      // decoded OK but the bounded queue was full (drop-and-count policy)
  DuplicateDiscarded, // seq already seen; intentionally not forwarded
  LateDiscarded,      // filled a declared gap, but late_policy = discard
  PausedDiscarded,    // stream paused after a gap (gap_policy = pause)
  StreamLimitDiscarded, // too many distinct streams (memory bound)
  KernelDropped       // UDP: the kernel socket buffer was full; the datagram never reached user space
};

constexpr std::string_view to_string(LossKind k) {
  switch (k) {
    case LossKind::NetworkGap: return "network_gap";
    case LossKind::Rejected: return "rejected";
    case LossKind::QueueOverflow: return "queue_overflow";
    case LossKind::DuplicateDiscarded: return "duplicate_discarded";
    case LossKind::LateDiscarded: return "late_discarded";
    case LossKind::PausedDiscarded: return "paused_discarded";
    case LossKind::StreamLimitDiscarded: return "stream_limit_discarded";
    case LossKind::KernelDropped: return "kernel_dropped";
  }
  return "unknown";
}

// Timestamps for one event, all from std::chrono::steady_clock in nanoseconds
// (0 = not captured). Placement is normative: see docs/metrics.md §2.
struct StageTimestamps {
  std::uint64_t t_sched = 0;     // sender/replayer scheduled emission time (replay offset)
  std::uint64_t t_rx = 0;        // immediately after bytes are in user memory
  std::uint64_t t_decoded = 0;   // after decode + sequence check succeed
  std::uint64_t t_enqueued = 0;  // after a successful queue push
  std::uint64_t t_dequeued = 0;  // after a successful queue pop
  std::uint64_t t_done = 0;      // after the consumer finishes applying the event

  constexpr std::uint64_t decode_ns() const { return t_decoded - t_rx; }
  constexpr std::uint64_t queue_ns() const { return t_dequeued - t_enqueued; }
  constexpr std::uint64_t process_ns() const { return t_done - t_dequeued; }
  constexpr std::uint64_t end_to_end_ns() const { return t_done - t_rx; }
};

}  // namespace feed
