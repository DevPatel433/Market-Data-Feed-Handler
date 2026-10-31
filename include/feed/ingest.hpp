#pragma once
// Decode -> sequence -> deliver. The transport-independent front half of the
// pipeline: file replay now, UDP receive in Phase 4, queue handoff in Phase 3.
#include <array>
#include <cstdint>
#include <functional>
#include <span>

#include "feed/decoder.hpp"
#include "feed/protocol.hpp"
#include "feed/sequencer.hpp"

namespace feed {

// Every received frame lands in exactly one bucket; balanced() is the
// accounting identity from docs/metrics.md §4 (queue overflow joins it in Phase 3).
struct IngestCounters {
  std::uint64_t rx_frames = 0;
  std::uint64_t rejected = 0;
  std::array<std::uint64_t, kDecodeErrorCount> rejected_by{};  // index = DecodeError
  std::uint64_t decoded = 0;
  std::uint64_t forwarded = 0;
  std::uint64_t duplicate_discarded = 0;
  std::uint64_t late_discarded = 0;
  std::uint64_t paused_discarded = 0;
  std::uint64_t stream_limit_discarded = 0;

  bool balanced() const {
    return rx_frames == rejected + decoded &&
           decoded == forwarded + duplicate_discarded + late_discarded + paused_discarded + stream_limit_discarded;
  }
};

struct IngestResult {
  DecodeError error = DecodeError::None;
  // Valid only when error == None:
  std::uint32_t stream_id = 0;
  std::uint64_t seq = 0;
  Verdict verdict;
  bool forwarded() const { return error == DecodeError::None && verdict.action == Action::Forward; }
};

class Ingest {
 public:
  using Consumer = std::function<void(const Message&, StreamState)>;

  explicit Ingest(SequencerConfig cfg = {}, Consumer consumer = {})
      : sequencer_(cfg), consumer_(std::move(consumer)) {}

  // Policy: a frame that fails decode never reaches the sequencer. Its sequence
  // number is untrusted, so the sender's consumed number simply shows up as a
  // gap when the next valid message arrives.
  IngestResult on_frame(std::span<const std::uint8_t> frame);

  // For a datagram that is known to be bad before decoding (e.g. truncated by the kernel because it
  // exceeded the receive buffer). Counted exactly like a decode failure, so the accounting stays balanced.
  IngestResult on_invalid(DecodeError error);

  Sequencer& sequencer() { return sequencer_; }
  const Sequencer& sequencer() const { return sequencer_; }
  const IngestCounters& counters() const { return counters_; }

 private:
  Sequencer sequencer_;
  Consumer consumer_;
  IngestCounters counters_;
};

}  // namespace feed
