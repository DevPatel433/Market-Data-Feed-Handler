#include "feed/recovery.hpp"

#include <algorithm>
#include <chrono>

#include "feed/decoder.hpp"

namespace feed {

namespace {
std::uint64_t now_ns() {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
}
}  // namespace

// ------------------------------------------------------------ AuthoritativeData ----

bool AuthoritativeData::from_messages(const std::vector<Message>& msgs, AuthoritativeData& out, std::string& error) {
  out = AuthoritativeData{};
  std::map<std::uint32_t, BookState> books;
  for (const Message& m : msgs) {
    auto& stream = out.streams_[m.stream_id];
    if (m.seq != stream.size() + 1) {
      error = "authoritative stream " + std::to_string(m.stream_id) + ": expected seq " + std::to_string(stream.size() + 1) +
              ", got " + std::to_string(m.seq) + " (the authoritative file must be complete and in order)";
      return false;
    }
    stream.push_back(m);
    auto& digests = out.digests_[m.stream_id];
    if (digests.empty()) digests.push_back(0);
    BookState& book = books[m.stream_id];
    book.apply(m);
    digests.push_back(book.digest());
  }
  return true;
}

bool AuthoritativeData::from_records(const std::vector<ReplayRecord>& records, AuthoritativeData& out, std::string& error) {
  std::vector<Message> msgs;
  msgs.reserve(records.size());
  for (std::size_t i = 0; i < records.size(); ++i) {
    Message m;
    const DecodeError e = decode(records[i].frame, m);
    if (e != DecodeError::None) {
      error = "authoritative record " + std::to_string(i) + " is not a valid frame (" + to_string(e) + ")";
      return false;
    }
    msgs.push_back(m);
  }
  return from_messages(msgs, out, error);
}

bool AuthoritativeData::load_file(const std::string& path, std::shared_ptr<const AuthoritativeData>& out, std::string& error) {
  std::vector<std::uint8_t> image;
  if (!read_file(path, image)) {
    error = "cannot read '" + path + "'";
    return false;
  }
  std::vector<ReplayRecord> records;
  if (const ReplayError e = parse_replay(image, records); e != ReplayError::None) {
    error = "invalid replay file '" + path + "': " + feed::to_string(e);
    return false;
  }
  auto data = std::make_shared<AuthoritativeData>();
  if (!from_records(records, *data, error)) return false;
  out = std::move(data);
  return true;
}

const std::vector<Message>* AuthoritativeData::messages(std::uint32_t stream) const {
  const auto it = streams_.find(stream);
  return it == streams_.end() ? nullptr : &it->second;
}

std::optional<std::uint64_t> AuthoritativeData::digest_after(std::uint32_t stream, std::uint64_t seq) const {
  const auto it = digests_.find(stream);
  if (it == digests_.end() || seq >= it->second.size()) return std::nullopt;
  return it->second[static_cast<std::size_t>(seq)];
}

// ------------------------------------------------------------ AuthoritativeView ----

AuthoritativeView::AuthoritativeView(std::shared_ptr<const AuthoritativeData> data, std::uint32_t stream, SourceConfig cfg)
    : data_(std::move(data)), cfg_(cfg) {
  if (data_) msgs_ = data_->messages(stream);
}

void AuthoritativeView::advance_to(std::uint64_t head) {
  if (msgs_ == nullptr) return;
  const std::uint64_t target = std::min<std::uint64_t>(head, msgs_->size());
  while (published_ < target) {
    book_.apply((*msgs_)[static_cast<std::size_t>(published_)]);
    ++published_;
    if (cfg_.snapshot_interval > 0 && published_ % cfg_.snapshot_interval == 0 && cfg_.max_snapshots > 0) {
      snaps_.push_back(std::make_shared<const BookSnapshot>(book_.snapshot(published_)));
      while (snaps_.size() > cfg_.max_snapshots) snaps_.pop_front();
    }
  }
}

std::uint64_t AuthoritativeView::log_oldest() const {
  return published_ > cfg_.retention ? published_ - cfg_.retention + 1 : 1;
}

bool AuthoritativeView::fetch(std::uint64_t first, std::uint64_t last, std::vector<Message>& out) const {
  out.clear();
  if (msgs_ == nullptr || first == 0 || first > last || last > published_ || first < log_oldest()) return false;
  out.assign(msgs_->begin() + static_cast<std::ptrdiff_t>(first - 1), msgs_->begin() + static_cast<std::ptrdiff_t>(last));
  return true;
}

std::shared_ptr<const BookSnapshot> AuthoritativeView::snapshot_covering(std::uint64_t min_seq) const {
  for (auto it = snaps_.rbegin(); it != snaps_.rend(); ++it) {  // newest first
    if ((*it)->seq >= min_seq) return *it;
  }
  return nullptr;
}

// ----------------------------------------------------------------- strategies ----

const char* to_string(RecoveryStrategy s) {
  switch (s) {
    case RecoveryStrategy::Naive: return "naive";
    case RecoveryStrategy::None: return "none";
    case RecoveryStrategy::Retransmit: return "retransmit";
    case RecoveryStrategy::Snapshot: return "snapshot";
    case RecoveryStrategy::Hybrid: return "hybrid";
  }
  return "unknown";
}

const char* to_string(SyncState s) {
  switch (s) {
    case SyncState::Synced: return "synced";
    case SyncState::Recovering: return "recovering";
    case SyncState::Unavailable: return "unavailable";
  }
  return "unknown";
}

bool parse_recovery_strategy(std::string_view text, RecoveryStrategy& out) {
  for (const RecoveryStrategy s : {RecoveryStrategy::Naive, RecoveryStrategy::None, RecoveryStrategy::Retransmit,
                                   RecoveryStrategy::Snapshot, RecoveryStrategy::Hybrid}) {
    if (text == to_string(s)) {
      out = s;
      return true;
    }
  }
  return false;
}

// ----------------------------------------------------------------- engine ----

RecoveryEngine::RecoveryEngine(const RecoveryConfig& cfg, std::shared_ptr<const AuthoritativeData> data, std::uint32_t stream_id,
                               bool join_mid_stream)
    : cfg_(cfg), stream_id_(stream_id), join_mid_stream_(join_mid_stream), view_(std::move(data), stream_id, cfg.source) {}

void RecoveryEngine::on_message(const Message& m) {
  ++c_.events;
  if (!started_) {
    started_ = true;
    if (join_mid_stream_) next_apply_ = m.seq;
  }
  max_seen_ = std::max(max_seen_, m.seq);

  const auto account = [this] {
    switch (state_) {
      case SyncState::Synced: ++c_.events_synced; break;
      case SyncState::Recovering: ++c_.events_recovering; break;
      case SyncState::Unavailable: ++c_.events_unavailable; break;
    }
  };

  if (cfg_.strategy == RecoveryStrategy::Naive) {  // control: arrival order, no gap handling, always claims to be synced
    if (m.seq > naive_last_) {
      book_.apply(m);
      naive_last_ = m.seq;
      next_apply_ = m.seq + 1;
      ++c_.applied_direct;
    } else {
      ++c_.stale_ignored;
    }
    account();
    return;
  }

  if (state_ == SyncState::Unavailable) {  // terminal: the book stays frozen at its last consistent prefix
    ++c_.discarded_unavailable;
    account();
    return;
  }

  if (view_.valid()) view_.advance_to(max_seen_);

  if (m.seq < next_apply_ || pending_.count(m.seq) != 0) {
    ++c_.stale_ignored;
  } else if (m.seq == next_apply_) {
    book_.apply(m);
    ++next_apply_;
    ++c_.applied_direct;
    drain();
  } else if (pending_.size() < cfg_.max_buffered) {
    pending_.emplace(m.seq, m);
    ++c_.buffered;
  } else {
    ++c_.overflow_dropped;  // the hole stays; recovery will fetch it
  }
  update_gap();
  account();
}

void RecoveryEngine::drain() {
  while (!pending_.empty() && pending_.begin()->first == next_apply_) {
    book_.apply(pending_.begin()->second);
    pending_.erase(pending_.begin());
    ++next_apply_;
    ++c_.applied_from_buffer;
  }
}

void RecoveryEngine::update_gap() {
  const bool hole = max_seen_ >= next_apply_;  // something was seen that has not been applied: next_apply_ itself is missing
  if (hole) {
    if (!gap_open_) {
      gap_open_ = true;
      gap_open_event_ = c_.events;
      gap_open_ns_ = now_ns();
      ++c_.gaps_opened;
      state_ = SyncState::Recovering;
    }
    if (c_.events >= gap_open_event_ + cfg_.reorder_wait_events + cfg_.latency_events) attempt_recovery();
  } else if (gap_open_) {
    ++c_.self_healed;  // late arrivals filled the hole before any recovery was needed
    close_gap();
  }
}

void RecoveryEngine::close_gap() {
  const std::uint64_t events = c_.events - gap_open_event_;
  const std::uint64_t ns = now_ns() - gap_open_ns_;
  ++c_.episodes;
  c_.episode_events_total += events;
  c_.episode_events_max = std::max(c_.episode_events_max, events);
  c_.episode_ns_total += ns;
  c_.episode_ns_max = std::max(c_.episode_ns_max, ns);
  gap_open_ = false;
  state_ = SyncState::Synced;
}

void RecoveryEngine::go_unavailable() {
  state_ = SyncState::Unavailable;
  gap_open_ = false;
  ++c_.unrecoverable;
  c_.dropped_on_unavailable += pending_.size();
  pending_.clear();
}

void RecoveryEngine::attempt_recovery() {
  switch (cfg_.strategy) {
    case RecoveryStrategy::Naive:
      break;
    case RecoveryStrategy::None:
      go_unavailable();
      break;
    case RecoveryStrategy::Retransmit:
      if (!try_retransmit()) go_unavailable();
      break;
    case RecoveryStrategy::Snapshot:
      if (!try_snapshot()) ++c_.retry_waits;
      break;
    case RecoveryStrategy::Hybrid:
      if (!try_retransmit() && !try_snapshot()) ++c_.retry_waits;
      break;
  }
}

bool RecoveryEngine::try_retransmit() {
  if (!view_.valid() || next_apply_ < view_.log_oldest()) return false;  // the hole has aged out of the retention window
  std::vector<Message> log;
  if (!view_.fetch(next_apply_, max_seen_, log)) return false;
  const std::uint64_t first = next_apply_;
  for (std::uint64_t s = first; s <= max_seen_; ++s) {
    const auto it = pending_.find(s);
    if (it != pending_.end()) {  // prefer what we already hold
      book_.apply(it->second);
      pending_.erase(it);
      ++c_.applied_from_buffer;
    } else {
      book_.apply(log[static_cast<std::size_t>(s - first)]);
      ++c_.recovered_applied;
    }
  }
  next_apply_ = max_seen_ + 1;
  ++c_.recoveries_retransmit;
  close_gap();
  return true;
}

bool RecoveryEngine::try_snapshot() {
  if (!view_.valid()) return false;
  std::uint64_t last_hole = max_seen_;  // the newest sequence number we are still missing
  while (last_hole >= next_apply_ && pending_.count(last_hole) != 0) --last_hole;
  const auto snap = view_.snapshot_covering(last_hole);
  if (!snap) return false;  // none published yet: keep buffering and try again as the head advances

  book_.restore(*snap);
  ++c_.snapshots_loaded;
  next_apply_ = snap->seq + 1;
  while (!pending_.empty() && pending_.begin()->first <= snap->seq) {
    pending_.erase(pending_.begin());
    ++c_.covered_discarded;
  }
  drain();
  ++c_.recoveries_snapshot;
  close_gap();
  return true;
}

// -------------------------------------------------------------------- tracker ----

void StateTracker::on_message(const Message& m) {
  auto it = engines_.find(m.stream_id);
  if (it == engines_.end()) it = engines_.try_emplace(m.stream_id, cfg_, data_, m.stream_id, join_).first;
  it->second.on_message(m);
}

const RecoveryEngine* StateTracker::engine(std::uint32_t stream) const {
  const auto it = engines_.find(stream);
  return it == engines_.end() ? nullptr : &it->second;
}

std::vector<StreamStateReport> StateTracker::report() const {
  std::vector<StreamStateReport> out;
  for (const auto& [id, e] : engines_) {
    StreamStateReport r;
    r.stream_id = id;
    r.state = e.state();
    r.next_apply = e.next_apply();
    r.max_seen = e.max_seen();
    r.buffered = e.buffered_now();
    r.live_orders = e.book().live_orders();
    r.book_digest = e.book().digest();
    r.unknown_cancels = e.book().unknown_cancels();
    r.duplicate_adds = e.book().duplicate_adds();
    r.counters = e.counters();
    if (data_) {
      const std::uint64_t applied_through = e.next_apply() > 0 ? e.next_apply() - 1 : 0;
      if (const auto ref = data_->digest_after(id, applied_through)) {
        r.reference_known = true;
        r.reference_digest = *ref;
        r.matches_reference = (*ref == r.book_digest);
      }
    }
    out.push_back(r);
  }
  return out;
}

}  // namespace feed
