#include "feed/ingest.hpp"

namespace feed {

IngestResult Ingest::on_invalid(DecodeError error) {
  IngestResult r;
  r.error = error;
  ++counters_.rx_frames;
  ++counters_.rejected;
  ++counters_.rejected_by[static_cast<std::size_t>(error)];
  return r;
}

IngestResult Ingest::on_frame(std::span<const std::uint8_t> frame) {
  IngestResult r;
  ++counters_.rx_frames;

  Message msg;
  r.error = decode(frame, msg);
  if (r.error != DecodeError::None) {
    ++counters_.rejected;
    ++counters_.rejected_by[static_cast<std::size_t>(r.error)];
    return r;
  }
  ++counters_.decoded;
  r.stream_id = msg.stream_id;
  r.seq = msg.seq;
  r.verdict = sequencer_.observe(msg.stream_id, msg.seq);

  switch (r.verdict.action) {
    case Action::Forward:
      ++counters_.forwarded;
      if (consumer_) consumer_(msg, r.verdict.state);
      break;
    case Action::Discard:
      switch (r.verdict.reason) {
        case DiscardReason::Duplicate: ++counters_.duplicate_discarded; break;
        case DiscardReason::Late: ++counters_.late_discarded; break;
        case DiscardReason::Paused: ++counters_.paused_discarded; break;
        case DiscardReason::StreamLimit: ++counters_.stream_limit_discarded; break;
        case DiscardReason::None: break;  // unreachable: a discard always has a reason
      }
      break;
  }
  return r;
}

}  // namespace feed
