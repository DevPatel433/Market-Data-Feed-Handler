#include "feed/sequencer.hpp"

#include <limits>

namespace feed {

const char* to_string(Classification c) {
  switch (c) {
    case Classification::InOrder: return "in_order";
    case Classification::Gap: return "gap";
    case Classification::Duplicate: return "duplicate";
    case Classification::OutOfOrder: return "out_of_order";
    case Classification::StreamLimit: return "stream_limit";
  }
  return "unknown";
}
const char* to_string(Action a) { return a == Action::Forward ? "forward" : "discard"; }
const char* to_string(DiscardReason r) {
  switch (r) {
    case DiscardReason::None: return "none";
    case DiscardReason::Duplicate: return "duplicate";
    case DiscardReason::Late: return "late";
    case DiscardReason::Paused: return "paused";
    case DiscardReason::StreamLimit: return "stream_limit";
  }
  return "unknown";
}
const char* to_string(StreamState s) {
  switch (s) {
    case StreamState::Synced: return "synced";
    case StreamState::Degraded: return "degraded";
    case StreamState::Paused: return "paused";
  }
  return "unknown";
}
const char* to_string(GapPolicy p) { return p == GapPolicy::MarkDegraded ? "degrade" : "pause"; }
const char* to_string(LatePolicy p) { return p == LatePolicy::Discard ? "discard" : "forward"; }

namespace {
constexpr std::uint64_t kSeqMax = std::numeric_limits<std::uint64_t>::max();
}

void Sequencer::enforce_cap(Stream& st) {
  while (st.missing.size() > cfg_.max_missing_ranges) {
    const auto oldest = st.missing.begin();
    const std::uint64_t size = oldest->second - oldest->first + 1;
    st.outstanding -= size;
    st.unrecovered += size;
    st.c.seqs_evicted += size;
    ++st.c.ranges_evicted;
    st.missing.erase(oldest);
  }
}

void Sequencer::add_missing(Stream& st, std::uint64_t first, std::uint64_t last) {
  // New gaps always start at `expected`, beyond every existing range.
  st.missing.emplace_hint(st.missing.end(), first, last);
  st.outstanding += last - first + 1;
  enforce_cap(st);
}

// If `seq` lies in a tracked missing range, remove it (splitting the range) and return true.
bool Sequencer::take_missing(Stream& st, std::uint64_t seq) {
  auto it = st.missing.upper_bound(seq);
  if (it == st.missing.begin()) return false;
  --it;
  const std::uint64_t first = it->first;
  const std::uint64_t last = it->second;
  if (seq > last) return false;
  st.missing.erase(it);
  if (first < seq) st.missing.emplace(first, seq - 1);
  if (seq < last) st.missing.emplace(seq + 1, last);
  --st.outstanding;
  enforce_cap(st);
  return true;
}

Verdict Sequencer::observe(std::uint32_t stream_id, std::uint64_t seq) {
  auto it = streams_.find(stream_id);
  if (it == streams_.end()) {
    if (streams_.size() >= cfg_.max_streams) {
      ++stream_limit_;
      Verdict v;
      v.cls = Classification::StreamLimit;
      v.action = Action::Discard;
      v.reason = DiscardReason::StreamLimit;
      return v;
    }
    Stream fresh;
    fresh.expected = cfg_.join_mid_stream ? seq : 1;
    it = streams_.emplace(stream_id, std::move(fresh)).first;
  }
  Stream& st = it->second;
  SequencerStats& c = st.c;

  const auto advance = [&st](std::uint64_t s) {
    if (s == kSeqMax) {
      st.expected = kSeqMax;
      st.exhausted = true;
    } else {
      st.expected = s + 1;
    }
  };

  Verdict v;
  if (!st.exhausted && seq == st.expected) {
    v.cls = Classification::InOrder;
    ++c.in_order;
    advance(seq);
  } else if (!st.exhausted && seq > st.expected) {
    v.cls = Classification::Gap;
    v.gap_first = st.expected;
    v.gap_count = seq - st.expected;
    ++c.gap_events;
    c.missing_total += v.gap_count;
    add_missing(st, v.gap_first, seq - 1);
    advance(seq);
    if (cfg_.gap_policy == GapPolicy::Pause) {
      st.state = StreamState::Paused;
    } else if (st.state == StreamState::Synced) {
      st.state = StreamState::Degraded;
    }
  } else if (take_missing(st, seq)) {
    v.cls = Classification::OutOfOrder;
    ++c.out_of_order;
  } else {
    v.cls = Classification::Duplicate;
    ++c.duplicates;
  }

  // Action. Duplicates are always discarded (and counted as such); otherwise a
  // paused stream discards everything; otherwise only late arrivals are policy-dependent.
  if (v.cls == Classification::Duplicate) {
    v.action = Action::Discard;
    v.reason = DiscardReason::Duplicate;
  } else if (st.state == StreamState::Paused) {
    v.action = Action::Discard;
    v.reason = DiscardReason::Paused;
    ++c.paused_discarded;
  } else if (v.cls == Classification::OutOfOrder && cfg_.late_policy == LatePolicy::Discard) {
    v.action = Action::Discard;
    v.reason = DiscardReason::Late;
    ++c.late_discarded;
  }
  if (v.action == Action::Forward) ++c.forwarded;

  // A degraded stream is repaired only if the late data really reached the
  // consumer and nothing was given up on.
  if (v.cls == Classification::OutOfOrder && v.action == Action::Forward && st.state == StreamState::Degraded &&
      st.outstanding == 0 && st.unrecovered == 0) {
    st.state = StreamState::Synced;
  }
  v.state = st.state;
  return v;
}

bool Sequencer::resync(std::uint32_t stream_id, std::uint64_t next_expected) {
  const auto it = streams_.find(stream_id);
  if (it == streams_.end() || next_expected == 0) return false;
  Stream& st = it->second;
  st.c.resync_cleared_seqs += st.outstanding;
  ++st.c.resyncs;
  st.missing.clear();
  st.outstanding = 0;
  st.unrecovered = 0;
  st.expected = next_expected;
  st.exhausted = false;
  st.state = StreamState::Synced;
  return true;
}

SequencerStats Sequencer::stats() const {
  SequencerStats t;
  for (const auto& [id, st] : streams_) {
    (void)id;
    const SequencerStats& c = st.c;
    t.in_order += c.in_order;
    t.gap_events += c.gap_events;
    t.missing_total += c.missing_total;
    t.duplicates += c.duplicates;
    t.out_of_order += c.out_of_order;
    t.forwarded += c.forwarded;
    t.late_discarded += c.late_discarded;
    t.paused_discarded += c.paused_discarded;
    t.ranges_evicted += c.ranges_evicted;
    t.seqs_evicted += c.seqs_evicted;
    t.resyncs += c.resyncs;
    t.resync_cleared_seqs += c.resync_cleared_seqs;
  }
  t.stream_limit = stream_limit_;
  return t;
}

std::uint64_t Sequencer::outstanding_seqs() const {
  std::uint64_t n = 0;
  for (const auto& [id, st] : streams_) {
    (void)id;
    n += st.outstanding;
  }
  return n;
}

StreamInfo Sequencer::info_of(std::uint32_t id, const Stream& st) {
  StreamInfo i;
  i.stream_id = id;
  i.state = st.state;
  i.next_expected = st.expected;
  i.outstanding_seqs = st.outstanding;
  i.missing_ranges = st.missing.size();
  i.unrecovered_seqs = st.unrecovered;
  i.counters = st.c;
  return i;
}

std::optional<StreamInfo> Sequencer::stream(std::uint32_t stream_id) const {
  const auto it = streams_.find(stream_id);
  if (it == streams_.end()) return std::nullopt;
  return info_of(it->first, it->second);
}

std::vector<StreamInfo> Sequencer::streams() const {
  std::vector<StreamInfo> out;
  out.reserve(streams_.size());
  for (const auto& [id, st] : streams_) out.push_back(info_of(id, st));
  return out;
}

}  // namespace feed
