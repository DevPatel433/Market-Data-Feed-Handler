#pragma once
// Per-stream sequence validation. Normative description: docs/sequencing.md.
//
// Every valid message is classified as exactly one of:
//   InOrder    seq == next expected
//   Gap        seq  > next expected (the skipped numbers become "missing")
//   OutOfOrder seq  < next expected AND previously declared missing (a late arrival)
//   Duplicate  seq  < next expected and NOT missing (already seen / before the join point)
// plus StreamLimit when too many distinct streams exist (memory bound).
#include <cstddef>
#include <cstdint>
#include <map>
#include <optional>
#include <vector>

namespace feed {

enum class Classification : std::uint8_t { InOrder, Gap, Duplicate, OutOfOrder, StreamLimit };
enum class Action : std::uint8_t { Forward, Discard };
enum class DiscardReason : std::uint8_t { None, Duplicate, Late, Paused, StreamLimit };

// Whether downstream state built from this stream can be trusted.
enum class StreamState : std::uint8_t {
  Synced,    // no known loss
  Degraded,  // loss detected; events still forwarded, consumer is told the stream is damaged
  Paused     // loss detected; nothing is forwarded until resync()
};

enum class GapPolicy : std::uint8_t { MarkDegraded, Pause };
enum class LatePolicy : std::uint8_t { Discard, Forward };

const char* to_string(Classification c);
const char* to_string(Action a);
const char* to_string(DiscardReason r);
const char* to_string(StreamState s);
const char* to_string(GapPolicy p);
const char* to_string(LatePolicy p);

struct SequencerConfig {
  GapPolicy gap_policy = GapPolicy::MarkDegraded;
  LatePolicy late_policy = LatePolicy::Discard;
  // false: a new stream is expected to start at seq 1 (a later first seq is a gap).
  // true:  join mid-stream: the first message seen defines the starting point.
  bool join_mid_stream = false;
  std::size_t max_streams = 1024;
  // Bound on tracked missing ranges per stream. Beyond it the oldest ranges are
  // evicted (counted as permanently lost) so hostile input cannot grow memory.
  std::size_t max_missing_ranges = 4096;
};

struct Verdict {
  Classification cls = Classification::InOrder;
  Action action = Action::Forward;
  DiscardReason reason = DiscardReason::None;
  StreamState state = StreamState::Synced;  // stream state AFTER this message
  std::uint64_t gap_first = 0;              // Gap only: first missing seq
  std::uint64_t gap_count = 0;              // Gap only: number of missing seqs
};

struct SequencerStats {
  std::uint64_t in_order = 0;
  std::uint64_t gap_events = 0;
  std::uint64_t missing_total = 0;  // sequence numbers ever declared missing
  std::uint64_t duplicates = 0;
  std::uint64_t out_of_order = 0;   // late arrivals that filled a declared gap
  std::uint64_t forwarded = 0;
  std::uint64_t late_discarded = 0;
  std::uint64_t paused_discarded = 0;
  std::uint64_t ranges_evicted = 0;
  std::uint64_t seqs_evicted = 0;   // missing seqs given up on (cap exceeded)
  std::uint64_t resyncs = 0;
  std::uint64_t resync_cleared_seqs = 0;
  std::uint64_t stream_limit = 0;   // messages dropped because max_streams was reached

  // Messages routed to a stream (excludes stream_limit).
  std::uint64_t observed() const { return in_order + gap_events + duplicates + out_of_order; }
};

struct StreamInfo {
  std::uint32_t stream_id = 0;
  StreamState state = StreamState::Synced;
  std::uint64_t next_expected = 1;
  std::uint64_t outstanding_seqs = 0;   // missing and still tracked
  std::uint64_t missing_ranges = 0;
  std::uint64_t unrecovered_seqs = 0;   // evicted since last resync
  SequencerStats counters;
};

class Sequencer {
 public:
  explicit Sequencer(SequencerConfig cfg = {}) : cfg_(cfg) {}

  // Classify one decoded message. Only call with messages that passed decode():
  // a rejected frame's sequence number is untrusted and must not reach here.
  Verdict observe(std::uint32_t stream_id, std::uint64_t seq);

  // Simulated recovery hook (used by Phase 5): declare the stream rebuilt and
  // expect `next_expected` next. Clears missing ranges, returns state to Synced.
  // Returns false for an unknown stream or next_expected == 0.
  bool resync(std::uint32_t stream_id, std::uint64_t next_expected);

  SequencerStats stats() const;  // aggregate over all streams
  std::uint64_t outstanding_seqs() const;
  std::size_t stream_count() const { return streams_.size(); }
  std::optional<StreamInfo> stream(std::uint32_t stream_id) const;
  std::vector<StreamInfo> streams() const;  // ordered by stream_id
  const SequencerConfig& config() const { return cfg_; }

 private:
  struct Stream {
    std::uint64_t expected = 1;
    bool exhausted = false;  // seq UINT64_MAX was accepted; nothing can follow
    StreamState state = StreamState::Synced;
    std::map<std::uint64_t, std::uint64_t> missing;  // start -> end (inclusive), disjoint, ordered
    std::uint64_t outstanding = 0;
    std::uint64_t unrecovered = 0;
    SequencerStats c;
  };

  void add_missing(Stream& st, std::uint64_t first, std::uint64_t last);
  bool take_missing(Stream& st, std::uint64_t seq);
  void enforce_cap(Stream& st);
  static StreamInfo info_of(std::uint32_t id, const Stream& st);

  SequencerConfig cfg_;
  std::map<std::uint32_t, Stream> streams_;
  std::uint64_t stream_limit_ = 0;
};

}  // namespace feed
