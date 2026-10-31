#pragma once
// Gap recovery for downstream state. Normative description: docs/overload_and_recovery.md §4-§5.
//
// RecoveryEngine owns one stream's BookState and applies messages STRICTLY IN SEQUENCE ORDER: anything
// that arrives ahead of a hole waits in a bounded buffer. The consequence, tested as an invariant, is that
// the book can be STALE but is never WRONG: at every moment its digest equals the reference digest at
// (next_apply - 1). RecoveryStrategy::Naive is the control that applies messages as they arrive and shows
// what happens without that discipline.
#include <cstdint>
#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "feed/book_state.hpp"
#include "feed/protocol.hpp"
#include "feed/replay_file.hpp"

namespace feed {

// ------------------------------------------------------------ authoritative source ----

// The complete, correct message history per stream: what a retransmission server and snapshot service
// would have. Built from a clean replay file. Immutable after construction (safe to share across threads).
class AuthoritativeData {
 public:
  // Each stream's seq must be exactly 1, 2, 3, ... in order.
  static bool from_messages(const std::vector<Message>& msgs, AuthoritativeData& out, std::string& error);
  static bool from_records(const std::vector<ReplayRecord>& records, AuthoritativeData& out, std::string& error);
  static bool load_file(const std::string& path, std::shared_ptr<const AuthoritativeData>& out, std::string& error);

  const std::vector<Message>* messages(std::uint32_t stream) const;
  // Digest of the book after applying messages 1..seq of `stream` (seq 0 -> 0). nullopt if unknown / beyond the end.
  std::optional<std::uint64_t> digest_after(std::uint32_t stream, std::uint64_t seq) const;

 private:
  std::map<std::uint32_t, std::vector<Message>> streams_;
  std::map<std::uint32_t, std::vector<std::uint64_t>> digests_;  // digests_[s][i] = digest after i messages
};

struct SourceConfig {
  std::size_t retention = 1024;           // retransmit window: the newest `retention` messages are available
  std::uint64_t snapshot_interval = 500;  // a snapshot is published every this many messages (0 = never)
  std::size_t max_snapshots = 8;          // only the newest few are kept (memory bound)
};

// One stream's recovery services as seen from a receiver that has observed messages up to `head`.
// Stateful: it publishes snapshots as the head advances, exactly as a live service would.
class AuthoritativeView {
 public:
  AuthoritativeView() = default;
  AuthoritativeView(std::shared_ptr<const AuthoritativeData> data, std::uint32_t stream, SourceConfig cfg);

  bool valid() const { return msgs_ != nullptr; }
  void advance_to(std::uint64_t head);  // monotone; clamped to the end of the authoritative stream
  std::uint64_t head() const { return published_; }

  std::uint64_t log_oldest() const;  // oldest sequence number still retransmittable at the current head
  // Messages [first, last] if the whole range is inside the retention window and published; else false.
  bool fetch(std::uint64_t first, std::uint64_t last, std::vector<Message>& out) const;
  // Newest published snapshot with seq >= min_seq, or nullptr (none published yet, or all too old/evicted).
  std::shared_ptr<const BookSnapshot> snapshot_covering(std::uint64_t min_seq) const;
  std::size_t snapshots_held() const { return snaps_.size(); }

 private:
  std::shared_ptr<const AuthoritativeData> data_;
  const std::vector<Message>* msgs_ = nullptr;
  SourceConfig cfg_;
  std::uint64_t published_ = 0;
  BookState book_;
  std::deque<std::shared_ptr<const BookSnapshot>> snaps_;
};

// ----------------------------------------------------------------- engine ----

enum class RecoveryStrategy : std::uint8_t {
  Naive,       // control: apply in arrival order, ignore gaps (silent corruption)
  None,        // strict order; a gap that does not heal on its own makes the state UNAVAILABLE
  Retransmit,  // fill the hole from the retained log; if it has aged out of the window: UNAVAILABLE
  Snapshot,    // wait for a published snapshot that covers the hole, load it, continue
  Hybrid       // retransmit if possible, otherwise snapshot
};

enum class SyncState : std::uint8_t {
  Synced,       // up to date: every message through next_apply-1 applied, nothing outstanding
  Recovering,   // a hole is open; the book is a consistent but STALE prefix; repair in progress
  Unavailable   // repair is impossible with this strategy; the book is frozen at its last consistent prefix
};

const char* to_string(RecoveryStrategy s);
const char* to_string(SyncState s);
bool parse_recovery_strategy(std::string_view text, RecoveryStrategy& out);

struct RecoveryConfig {
  RecoveryStrategy strategy = RecoveryStrategy::None;
  SourceConfig source;
  std::uint64_t reorder_wait_events = 0;  // events to wait for late arrivals to heal a gap before requesting recovery
  std::uint64_t latency_events = 0;       // simulated request round trip, measured in events received meanwhile
  std::size_t max_buffered = 4096;        // bound on the out-of-order buffer
};

struct RecoveryCounters {
  std::uint64_t events = 0;  // messages handed to the engine
  // Fate of each message: events = applied_direct + buffered + stale_ignored + overflow_dropped + discarded_unavailable
  std::uint64_t applied_direct = 0;          // applied on arrival (it was the next one expected)
  std::uint64_t buffered = 0;                // arrived ahead of a hole and was held
  std::uint64_t stale_ignored = 0;           // already applied / covered, or a duplicate of a buffered one
  std::uint64_t overflow_dropped = 0;        // buffer full; the hole will be repaired from the log/snapshot
  std::uint64_t discarded_unavailable = 0;   // arrived after the engine gave up
  // Fate of buffered messages: buffered = applied_from_buffer + covered_discarded + dropped_on_unavailable + (still buffered)
  std::uint64_t applied_from_buffer = 0;
  std::uint64_t covered_discarded = 0;       // made redundant by a snapshot
  std::uint64_t dropped_on_unavailable = 0;
  // Gap handling
  std::uint64_t gaps_opened = 0;
  std::uint64_t self_healed = 0;             // closed by late arrivals before any recovery
  std::uint64_t recoveries_retransmit = 0;
  std::uint64_t recoveries_snapshot = 0;
  std::uint64_t recovered_applied = 0;       // messages applied from the retransmit log
  std::uint64_t snapshots_loaded = 0;
  std::uint64_t retry_waits = 0;             // attempts that found no covering snapshot yet
  std::uint64_t unrecoverable = 0;           // entered Unavailable
  // Time to steady state, per closed gap episode (open -> Synced again)
  std::uint64_t episodes = 0;
  std::uint64_t episode_events_total = 0;
  std::uint64_t episode_events_max = 0;
  std::uint64_t episode_ns_total = 0;
  std::uint64_t episode_ns_max = 0;
  // Where the engine spent its events (state after each event)
  std::uint64_t events_synced = 0;
  std::uint64_t events_recovering = 0;
  std::uint64_t events_unavailable = 0;
};

class RecoveryEngine {
 public:
  RecoveryEngine(const RecoveryConfig& cfg, std::shared_ptr<const AuthoritativeData> data, std::uint32_t stream_id,
                 bool join_mid_stream);

  void on_message(const Message& m);

  SyncState state() const { return state_; }
  std::uint32_t stream_id() const { return stream_id_; }
  std::uint64_t next_apply() const { return next_apply_; }  // next sequence number the book needs
  std::uint64_t max_seen() const { return max_seen_; }
  std::size_t buffered_now() const { return pending_.size(); }
  const BookState& book() const { return book_; }
  const RecoveryCounters& counters() const { return c_; }
  const RecoveryConfig& config() const { return cfg_; }

 private:
  void drain();
  void update_gap();
  void attempt_recovery();
  bool try_retransmit();
  bool try_snapshot();
  void close_gap();
  void go_unavailable();

  RecoveryConfig cfg_;
  std::uint32_t stream_id_;
  bool join_mid_stream_;
  AuthoritativeView view_;
  BookState book_;
  std::map<std::uint64_t, Message> pending_;
  std::uint64_t next_apply_ = 1;
  std::uint64_t max_seen_ = 0;
  bool started_ = false;
  SyncState state_ = SyncState::Synced;
  bool gap_open_ = false;
  std::uint64_t gap_open_event_ = 0;
  std::uint64_t gap_open_ns_ = 0;
  std::uint64_t naive_last_ = 0;
  RecoveryCounters c_;
};

// -------------------------------------------------------- per-stream tracker ----

struct StreamStateReport {
  std::uint32_t stream_id = 0;
  SyncState state = SyncState::Synced;
  std::uint64_t next_apply = 1;
  std::uint64_t max_seen = 0;
  std::uint64_t buffered = 0;
  std::uint64_t live_orders = 0;
  std::uint64_t book_digest = 0;
  bool reference_known = false;
  std::uint64_t reference_digest = 0;   // the correct digest after applying 1..next_apply-1
  bool matches_reference = false;       // book == truth at next_apply-1 (what "known state" means)
  std::uint64_t unknown_cancels = 0;
  std::uint64_t duplicate_adds = 0;
  RecoveryCounters counters;
};

class StateTracker {
 public:
  StateTracker(RecoveryConfig cfg, std::shared_ptr<const AuthoritativeData> data, bool join_mid_stream)
      : cfg_(cfg), data_(std::move(data)), join_(join_mid_stream) {}

  void on_message(const Message& m);
  std::vector<StreamStateReport> report() const;
  std::size_t streams() const { return engines_.size(); }
  const RecoveryEngine* engine(std::uint32_t stream) const;

 private:
  RecoveryConfig cfg_;
  std::shared_ptr<const AuthoritativeData> data_;
  bool join_;
  std::map<std::uint32_t, RecoveryEngine> engines_;
};

}  // namespace feed
