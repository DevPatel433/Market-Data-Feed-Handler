#pragma once
// Two-thread pipeline: [replay -> decode -> sequence -> enqueue] on the calling thread (producer),
// [dequeue -> verify -> count] on a second thread (consumer). Normative description: docs/spsc_queue.md §5.
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "feed/fnv.hpp"
#include "feed/ingest.hpp"
#include "feed/lag_histogram.hpp"
#include "feed/overload_model.hpp"
#include "feed/queue_policy.hpp"
#include "feed/recovery.hpp"
#include "feed/replay_file.hpp"
#include "feed/replayer.hpp"
#include "feed/sequencer.hpp"
#include "feed/spsc_queue.hpp"

namespace feed {

// What crosses the queue. `ticket` numbers every event offered to the queue (1, 2, 3, ...) so the
// consumer can detect loss, duplication and reordering independently of protocol sequence numbers.
// `check` is derived from the other fields; a torn or stale read cannot reproduce it.
struct QueuedEvent {
  Message msg;
  StreamState state = StreamState::Synced;
  std::uint64_t ticket = 0;
  std::uint64_t check = 0;
  std::uint64_t t_offered_ns = 0;  // steady_clock ns when the producer offered it to the queue (0 unless measure_lag)
};

std::uint64_t event_check(const QueuedEvent& ev);
void digest_event(Fnv& d, const QueuedEvent& ev);  // order-sensitive fingerprint of what was delivered

struct ConsumerReport {
  std::uint64_t processed = 0;
  std::uint64_t out_of_order = 0;  // ticket not greater than the previous one (reordered or duplicated delivery)
  std::uint64_t skipped = 0;       // tickets missing between consecutive deliveries (dropped before the queue)
  std::uint64_t corrupt = 0;       // integrity check failed (torn or stale read)
  std::uint64_t last_ticket = 0;
  std::uint64_t digest = 0;
};

// The consumer's verification logic, separated from the thread so it can be tested with crafted input.
// Tickets must be strictly increasing; a jump means events were dropped before the queue (counted as
// skipped), a non-increase means duplicated or reordered delivery, and a bad `check` means a torn read.
class ConsumerChecker {
 public:
  void on_event(const QueuedEvent& ev);
  ConsumerReport report() const;

 private:
  ConsumerReport r_;
  Fnv digest_;
};

// Downstream application state, rebuilt by the consumer thread (docs/overload_and_recovery.md §3-§5).
struct StateConfig {
  bool enabled = false;
  RecoveryConfig recovery;
  std::shared_ptr<const AuthoritativeData> authoritative;  // required by retransmit / snapshot / hybrid
};

struct PipelineConfig {
  SequencerConfig sequencer;
  ReplayOptions replay;  // `stop` is managed by the pipeline
  std::size_t queue_capacity = 1024;
  QueueFullPolicy full_policy = QueueFullPolicy::Block;
  std::uint64_t consumer_work_ns = 0;  // busy-wait per event: simulates a slow consumer
  StateConfig state;
  bool measure_lag = false;               // stamp offer time; record offer->dequeue lag per event
  std::uint64_t sample_interval_us = 0;   // >0: sample queue occupancy this often (timeline + time to steady state)

  // False (with a reason) for configurations that cannot work; run_pipeline_source then refuses to run.
  bool validate(std::string& error) const;
};


struct PipelineReport {
  ReplayStats replay;
  IngestCounters ingest;
  SequencerStats sequencing;
  std::vector<StreamInfo> streams;
  OverflowCounters overflow;
  SpscStats queue;
  std::size_t queue_capacity = 0;
  ConsumerReport consumer;
  std::uint64_t producer_digest = 0;  // over events the producer enqueued, in order
  bool aborted = false;               // fail-fast stopped the replay early
  std::string config_error;           // non-empty: the configuration was invalid and nothing ran

  // Overload measurements (docs/overload_and_recovery.md §2).
  bool lag_measured = false;
  LogHistogram lag;                   // per event: dequeue time - offer time (includes any time the producer was blocked)
  std::uint64_t producer_end_ns = 0;  // when the source finished, relative to the start of the run
  std::vector<OccPoint> timeline;     // sampled queue occupancy (sample_interval_us > 0)
  SteadyState steady;                 // derived from `timeline`

  // Recovery (state_enabled): one report per stream.
  bool state_enabled = false;
  RecoveryStrategy state_strategy = RecoveryStrategy::None;
  std::vector<StreamStateReport> state;

  // Every received frame is in exactly one bucket (docs/metrics.md §4).
  bool frames_accounted() const {
    return ingest.balanced() &&
           ingest.rx_frames == ingest.rejected + ingest.duplicate_discarded + ingest.late_discarded +
                                   ingest.paused_discarded + ingest.stream_limit_discarded + overflow.dropped +
                                   overflow.aborted + consumer.processed;
  }
  // Producer and consumer agree on what crossed the queue.
  bool queue_consistent() const {
    return ingest.forwarded == overflow.offered &&
           overflow.offered == overflow.enqueued + overflow.dropped + overflow.aborted &&
           overflow.enqueued == consumer.processed && queue.occupancy == 0 &&
           consumer.skipped + (overflow.offered - consumer.last_ticket) == overflow.dropped + overflow.aborted &&
           producer_digest == consumer.digest;
  }
  bool order_and_integrity_ok() const { return consumer.out_of_order == 0 && consumer.corrupt == 0; }
  // Every processed event reached the state engines, and each engine accounted for every message it saw.
  bool state_consistent() const {
    if (!state_enabled) return true;
    std::uint64_t events = 0;
    for (const StreamStateReport& r : state) {
      const RecoveryCounters& c = r.counters;
      events += c.events;
      if (c.events != c.applied_direct + c.buffered + c.stale_ignored + c.overflow_dropped + c.discarded_unavailable) return false;
      if (c.buffered != c.applied_from_buffer + c.covered_discarded + c.dropped_on_unavailable + r.buffered) return false;
      if (c.events != c.events_synced + c.events_recovering + c.events_unavailable) return false;
    }
    return events == consumer.processed;
  }
  bool balanced() const {
    return config_error.empty() && frames_accounted() && queue_consistent() && order_and_integrity_ok() && state_consistent() &&
           (!lag_measured || lag.count() == consumer.processed);
  }
};

// Where frames come from. The pipeline owns decode/sequence/queue/consumer; a source only delivers bytes.
class FrameIntake {
 public:
  virtual IngestResult on_frame(std::span<const std::uint8_t> frame) = 0;
  virtual IngestResult on_invalid(DecodeError error) = 0;  // bad before decoding (e.g. truncated datagram)

 protected:
  ~FrameIntake() = default;
};

// Runs on the calling thread (the producer). Must return when `stop` becomes true (fail-fast overflow)
// or when it has no more input. `cfg.replay` is ignored by run_pipeline_source.
using FrameSource = std::function<void(FrameIntake& sink, const std::atomic<bool>& stop)>;

PipelineReport run_pipeline_source(const PipelineConfig& cfg, const FrameSource& source);

// File replay as a source (what feed_pipeline uses).
PipelineReport run_pipeline(const std::vector<ReplayRecord>& records, const PipelineConfig& cfg);

}  // namespace feed
