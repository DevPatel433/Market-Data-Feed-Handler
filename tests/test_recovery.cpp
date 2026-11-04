#include <unistd.h>

#include <algorithm>
#include <vector>

#include "check.hpp"
#include "feed/decoder.hpp"
#include "feed/recovery.hpp"
#include "feed/rng.hpp"
#include "feed/workload.hpp"

using namespace feed;

namespace {

std::vector<Message> clean_messages(std::uint64_t n, std::uint64_t seed = 21) {
  WorkloadConfig c;
  c.count = n;
  c.seed = seed;
  std::vector<Message> out;
  for (const auto& r : generate_workload(c).records) {
    Message m;
    CHECK(decode(r.frame, m) == DecodeError::None);
    out.push_back(m);
  }
  return out;
}

std::shared_ptr<const AuthoritativeData> authoritative(const std::vector<Message>& msgs) {
  auto d = std::make_shared<AuthoritativeData>();
  std::string e;
  CHECK(AuthoritativeData::from_messages(msgs, *d, e));
  return d;
}

// Fixture: a clean stream plus its authoritative data and engine factory.
struct Fx {
  std::vector<Message> msgs;
  std::shared_ptr<const AuthoritativeData> data;
  explicit Fx(std::uint64_t n = 200, std::uint64_t seed = 21) : msgs(clean_messages(n, seed)), data(authoritative(msgs)) {}
  RecoveryEngine engine(RecoveryStrategy s, SourceConfig src = {}, std::uint64_t wait = 0, std::uint64_t latency = 0,
                        std::size_t buffer = 4096) const {
    RecoveryConfig c;
    c.strategy = s;
    c.source = src;
    c.reorder_wait_events = wait;
    c.latency_events = latency;
    c.max_buffered = buffer;
    return RecoveryEngine(c, data, 1, false);
  }
  void send(RecoveryEngine& e, std::initializer_list<std::uint64_t> seqs) const {
    for (const std::uint64_t s : seqs) e.on_message(msgs[static_cast<std::size_t>(s - 1)]);
  }
  void send_range(RecoveryEngine& e, std::uint64_t first, std::uint64_t last) const {
    for (std::uint64_t s = first; s <= last; ++s) e.on_message(msgs[static_cast<std::size_t>(s - 1)]);
  }
  std::uint64_t ref(std::uint64_t seq) const { return *data->digest_after(1, seq); }
};

SourceConfig src(std::size_t retention, std::uint64_t interval, std::size_t max_snaps = 8) {
  SourceConfig s;
  s.retention = retention;
  s.snapshot_interval = interval;
  s.max_snapshots = max_snaps;
  return s;
}

// Fate-of-every-message identities from recovery.hpp.
void check_identities(const RecoveryEngine& e) {
  const RecoveryCounters& c = e.counters();
  CHECK_EQ(c.events, c.applied_direct + c.buffered + c.stale_ignored + c.overflow_dropped + c.discarded_unavailable);
  CHECK_EQ(c.buffered, c.applied_from_buffer + c.covered_discarded + c.dropped_on_unavailable + e.buffered_now());
  CHECK_EQ(c.events, c.events_synced + c.events_recovering + c.events_unavailable);
  CHECK_EQ(e.book().applied(), c.applied_direct + c.applied_from_buffer + c.recovered_applied);
}

}  // namespace

// ------------------------------------------------------- authoritative source ----

TEST(authoritative_data_requires_complete_in_order_streams_and_knows_digests) {
  const auto msgs = clean_messages(100);
  AuthoritativeData d;
  std::string e;
  CHECK(AuthoritativeData::from_messages(msgs, d, e));
  CHECK_EQ(d.messages(1)->size(), 100U);
  CHECK(d.messages(2) == nullptr);
  CHECK_EQ(*d.digest_after(1, 0), 0U);
  BookState b;
  for (std::size_t i = 0; i < msgs.size(); ++i) {
    b.apply(msgs[i]);
    CHECK_EQ(*d.digest_after(1, i + 1), b.digest());
  }
  CHECK(!d.digest_after(1, 101).has_value());
  CHECK(!d.digest_after(9, 1).has_value());

  auto holey = msgs;
  holey.erase(holey.begin() + 10);  // seq 11 missing
  AuthoritativeData h;
  CHECK(!AuthoritativeData::from_messages(holey, h, e));
  CHECK(e.find("expected seq 11") != std::string::npos);
  auto dup = msgs;
  dup.insert(dup.begin() + 5, msgs[4]);
  CHECK(!AuthoritativeData::from_messages(dup, h, e));
}

TEST(authoritative_data_loads_from_a_replay_file_and_rejects_bad_ones) {
  WorkloadConfig c;
  c.count = 500;
  const auto recs = generate_workload(c).records;
  std::vector<std::uint8_t> img;
  CHECK(serialize_replay(recs, img));
  const std::string path = "/tmp/feed_auth_test_" + std::to_string(::getpid()) + ".fdr";
  CHECK(write_file(path, img));
  std::shared_ptr<const AuthoritativeData> d;
  std::string e;
  CHECK(AuthoritativeData::load_file(path, d, e));
  CHECK_EQ(d->messages(1)->size(), 500U);
  CHECK(!AuthoritativeData::load_file(path + ".missing", d, e));

  WorkloadConfig m;
  m.profile = Profile::Malformed;
  m.count = 500;
  m.fault_ppm = 50'000;
  std::vector<std::uint8_t> bad;
  CHECK(serialize_replay(generate_workload(m).records, bad));
  CHECK(write_file(path, bad));
  CHECK(!AuthoritativeData::load_file(path, d, e));  // a faulty file is not authoritative
  ::unlink(path.c_str());
}

TEST(authoritative_view_models_retention_and_snapshot_publication) {
  const Fx fx(60);
  AuthoritativeView v(fx.data, 1, src(10, 5, 2));
  v.advance_to(3);
  CHECK_EQ(v.head(), 3U);
  CHECK_EQ(v.log_oldest(), 1U);
  std::vector<Message> out;
  CHECK(v.fetch(1, 3, out));
  CHECK_EQ(out.size(), 3U);
  CHECK(!v.fetch(1, 4, out));  // not published yet
  CHECK(v.snapshot_covering(1) == nullptr);

  v.advance_to(12);
  CHECK_EQ(v.log_oldest(), 3U);  // newest 10 of 12
  CHECK(!v.fetch(2, 5, out));    // aged out of the window
  CHECK(v.fetch(3, 12, out));
  CHECK(out[0].seq == 3 && out.back().seq == 12);
  CHECK_EQ(v.snapshots_held(), 2U);  // snapshots at 5 and 10
  CHECK_EQ(v.snapshot_covering(1)->seq, 10U);  // newest first
  CHECK(v.snapshot_covering(11) == nullptr);   // nothing at or beyond 11 yet
  CHECK_EQ(v.snapshot_covering(10)->digest, fx.ref(10));
  CHECK_EQ(v.snapshot_covering(10)->digest, BookState::compute_digest(v.snapshot_covering(10)->orders, v.snapshot_covering(10)->instruments));

  v.advance_to(20);  // snapshots 15 and 20 now; 5 and 10 were evicted (bounded memory)
  CHECK_EQ(v.snapshots_held(), 2U);
  CHECK_EQ(v.snapshot_covering(7)->seq, 20U);
  v.advance_to(5);  // the head never moves backwards
  CHECK_EQ(v.head(), 20U);
  v.advance_to(1'000);  // and is clamped to the end of the authoritative stream
  CHECK_EQ(v.head(), 60U);

  AuthoritativeView none(fx.data, 7, {});  // unknown stream: no service at all
  CHECK(!none.valid());
  none.advance_to(10);
  CHECK(!none.fetch(1, 2, out));
}

// ----------------------------------------------------------------- engine ----

TEST(engine_in_order_stream_stays_synced_for_every_strategy) {
  const Fx fx(300);
  for (const RecoveryStrategy s : {RecoveryStrategy::Naive, RecoveryStrategy::None, RecoveryStrategy::Retransmit,
                                   RecoveryStrategy::Snapshot, RecoveryStrategy::Hybrid}) {
    RecoveryEngine e = fx.engine(s);
    fx.send_range(e, 1, 300);
    CHECK(e.state() == SyncState::Synced);
    CHECK_EQ(e.next_apply(), 301U);
    CHECK_EQ(e.book().digest(), fx.ref(300));
    CHECK_EQ(e.counters().applied_direct, 300U);
    CHECK_EQ(e.counters().gaps_opened, 0U);
    check_identities(e);
  }
}

TEST(engine_applies_strictly_in_order_and_heals_when_the_late_message_arrives) {
  const Fx fx;
  RecoveryEngine e = fx.engine(RecoveryStrategy::None, {}, /*wait=*/100);
  fx.send(e, {1, 2});
  fx.send(e, {4, 5});  // 3 is late
  CHECK(e.state() == SyncState::Recovering);
  CHECK_EQ(e.next_apply(), 3U);
  CHECK_EQ(e.buffered_now(), 2U);
  CHECK_EQ(e.book().digest(), fx.ref(2));  // stale but exactly right: nothing past the hole was applied
  fx.send(e, {3});
  CHECK(e.state() == SyncState::Synced);
  CHECK_EQ(e.next_apply(), 6U);
  CHECK_EQ(e.book().digest(), fx.ref(5));
  CHECK_EQ(e.counters().self_healed, 1U);
  CHECK_EQ(e.counters().applied_from_buffer, 2U);
  CHECK_EQ(e.counters().episodes, 1U);
  check_identities(e);
}

TEST(engine_duplicates_are_ignored_whether_applied_or_buffered) {
  const Fx fx;
  RecoveryEngine e = fx.engine(RecoveryStrategy::None, {}, 100);
  fx.send(e, {1, 2, 2, 1, 5, 5, 4, 4});
  CHECK_EQ(e.counters().stale_ignored, 4U);
  CHECK_EQ(e.buffered_now(), 2U);
  fx.send(e, {3});
  CHECK_EQ(e.book().digest(), fx.ref(5));
  check_identities(e);
}

TEST(engine_none_strategy_declares_the_state_unavailable_and_freezes_a_consistent_prefix) {
  const Fx fx;
  RecoveryEngine e = fx.engine(RecoveryStrategy::None);
  fx.send_range(e, 1, 10);
  fx.send(e, {13, 14, 15});  // 11 and 12 never arrive
  CHECK(e.state() == SyncState::Unavailable);
  CHECK_EQ(e.counters().unrecoverable, 1U);
  CHECK_EQ(e.next_apply(), 11U);
  CHECK_EQ(e.book().digest(), fx.ref(10));  // frozen exactly at the last good state, not corrupted
  const std::uint64_t digest = e.book().digest();
  fx.send_range(e, 16, 40);
  CHECK(e.state() == SyncState::Unavailable);  // terminal for this strategy
  CHECK_EQ(e.book().digest(), digest);
  // It gives up the moment 13 arrives (nothing waits, no latency), so only 13 was buffered; 14, 15 and 16..40 are discarded.
  CHECK_EQ(e.counters().dropped_on_unavailable, 1U);
  CHECK_EQ(e.counters().discarded_unavailable, 27U);
  CHECK_EQ(e.counters().events_synced, 10U);       // 1..10
  CHECK_EQ(e.counters().events_recovering, 0U);    // it gave up the instant the gap opened (no wait, no latency)
  CHECK_EQ(e.counters().events_unavailable, 28U);  // 13 itself plus the 27 that followed
  check_identities(e);
}

TEST(engine_retransmit_fills_the_hole_from_the_retained_log) {
  const Fx fx;
  RecoveryEngine e = fx.engine(RecoveryStrategy::Retransmit, src(1000, 0));
  fx.send_range(e, 1, 10);
  fx.send(e, {13, 14});  // 11, 12 lost: recovery runs immediately
  CHECK(e.state() == SyncState::Synced);
  CHECK_EQ(e.next_apply(), 15U);
  CHECK_EQ(e.book().digest(), fx.ref(14));
  CHECK_EQ(e.counters().recovered_applied, 2U);
  CHECK_EQ(e.counters().recoveries_retransmit, 1U);
  CHECK_EQ(e.counters().applied_from_buffer, 1U);  // 13 was buffered when the gap was seen; 14 then applied directly
  fx.send_range(e, 15, 100);
  CHECK_EQ(e.book().digest(), fx.ref(100));
  check_identities(e);
}

TEST(engine_retransmit_fails_when_the_hole_has_aged_out_of_the_window) {
  const Fx fx;
  // Retention 3 but the response takes 8 events: by then the hole is older than the log reaches.
  RecoveryEngine e = fx.engine(RecoveryStrategy::Retransmit, src(3, 0), 0, /*latency=*/8);
  fx.send_range(e, 1, 10);
  fx.send_range(e, 13, 25);  // 11, 12 lost
  CHECK(e.state() == SyncState::Unavailable);
  CHECK_EQ(e.counters().unrecoverable, 1U);
  CHECK_EQ(e.counters().recoveries_retransmit, 0U);
  CHECK_EQ(e.book().digest(), fx.ref(10));  // reported unavailable, and still exactly the last good state
  check_identities(e);
}

TEST(engine_snapshot_waits_for_a_covering_snapshot_then_catches_up) {
  const Fx fx;
  RecoveryEngine e = fx.engine(RecoveryStrategy::Snapshot, src(1000, 10));
  fx.send_range(e, 1, 12);
  fx.send(e, {15});  // 13, 14 lost; the next snapshot (20) is not published yet
  CHECK(e.state() == SyncState::Recovering);
  CHECK(e.counters().retry_waits > 0);
  CHECK_EQ(e.book().digest(), fx.ref(12));  // stale, consistent
  fx.send_range(e, 16, 19);
  CHECK(e.state() == SyncState::Recovering);  // head 19: still no snapshot >= 14 beyond the one at 10
  fx.send(e, {20});                           // head 20 publishes the snapshot at 20: covers the hole
  CHECK(e.state() == SyncState::Synced);
  CHECK_EQ(e.next_apply(), 21U);
  CHECK_EQ(e.book().digest(), fx.ref(20));
  CHECK_EQ(e.counters().snapshots_loaded, 1U);
  CHECK_EQ(e.counters().covered_discarded, 6U);  // 15..20 were buffered and are now redundant
  CHECK_EQ(e.counters().episode_events_max, 5U);  // time to steady state: 5 events after the one that opened the gap (16..20)
  fx.send_range(e, 21, 60);
  CHECK_EQ(e.book().digest(), fx.ref(60));
  check_identities(e);
}

TEST(engine_snapshot_without_any_service_never_corrupts_it_just_stays_stale) {
  const Fx fx;
  RecoveryConfig c;
  c.strategy = RecoveryStrategy::Snapshot;
  RecoveryEngine e(c, nullptr, 1, false);  // no authoritative source at all
  for (std::uint64_t s = 1; s <= 10; ++s) e.on_message(fx.msgs[s - 1]);
  for (std::uint64_t s = 13; s <= 100; ++s) e.on_message(fx.msgs[s - 1]);
  CHECK(e.state() == SyncState::Recovering);  // waiting forever for a service that does not exist
  CHECK_EQ(e.book().digest(), fx.ref(10));
  CHECK(e.counters().retry_waits > 50);
  check_identities(e);

  c.strategy = RecoveryStrategy::Retransmit;
  RecoveryEngine r(c, nullptr, 1, false);
  for (std::uint64_t s = 1; s <= 10; ++s) r.on_message(fx.msgs[s - 1]);
  r.on_message(fx.msgs[12]);
  CHECK(r.state() == SyncState::Unavailable);  // with nothing to fetch from, it says so at once
}

TEST(engine_hybrid_prefers_retransmit_and_falls_back_to_snapshot) {
  const Fx fx;
  {  // hole inside the retention window: retransmit, no snapshot loaded
    RecoveryEngine e = fx.engine(RecoveryStrategy::Hybrid, src(1000, 10));
    fx.send_range(e, 1, 12);
    fx.send(e, {15});
    CHECK(e.state() == SyncState::Synced);
    CHECK_EQ(e.counters().recoveries_retransmit, 1U);
    CHECK_EQ(e.counters().snapshots_loaded, 0U);
    CHECK_EQ(e.book().digest(), fx.ref(15));
  }
  {  // hole older than the log reaches: falls back to the snapshot, which still repairs it
    RecoveryEngine e = fx.engine(RecoveryStrategy::Hybrid, src(3, 5), 0, /*latency=*/12);
    fx.send_range(e, 1, 12);
    fx.send_range(e, 15, 40);  // 13, 14 lost; response arrives 12 events later
    CHECK(e.state() == SyncState::Synced);
    CHECK_EQ(e.counters().recoveries_retransmit, 0U);
    CHECK_EQ(e.counters().recoveries_snapshot, 1U);
    CHECK_EQ(e.book().digest(), fx.ref(e.next_apply() - 1));
    check_identities(e);
  }
}

TEST(engine_reorder_wait_gives_late_arrivals_a_chance_before_recovery_is_requested) {
  const Fx fx;
  {  // the missing message shows up inside the wait window: healed, no recovery used
    RecoveryEngine e = fx.engine(RecoveryStrategy::Retransmit, src(1000, 0), /*wait=*/5);
    fx.send_range(e, 1, 10);
    fx.send(e, {12, 13, 11});
    CHECK(e.state() == SyncState::Synced);
    CHECK_EQ(e.counters().self_healed, 1U);
    CHECK_EQ(e.counters().recoveries_retransmit, 0U);
    CHECK_EQ(e.book().digest(), fx.ref(13));
  }
  {  // it does not: recovery is requested once the window has passed
    RecoveryEngine e = fx.engine(RecoveryStrategy::Retransmit, src(1000, 0), /*wait=*/3);
    fx.send_range(e, 1, 10);
    fx.send(e, {12, 13});
    CHECK(e.state() == SyncState::Recovering);  // 2 events into a 3-event wait
    fx.send(e, {14, 15});
    CHECK(e.state() == SyncState::Synced);
    CHECK_EQ(e.counters().recoveries_retransmit, 1U);
    CHECK_EQ(e.counters().recovered_applied, 1U);
    CHECK_EQ(e.book().digest(), fx.ref(15));
  }
}

TEST(engine_recovery_latency_is_modelled_in_events_and_live_traffic_keeps_buffering) {
  const Fx fx;
  RecoveryEngine e = fx.engine(RecoveryStrategy::Retransmit, src(1000, 0), 0, /*latency=*/4);
  fx.send_range(e, 1, 10);
  fx.send(e, {12});
  CHECK(e.state() == SyncState::Recovering);
  fx.send(e, {13, 14, 15});
  CHECK(e.state() == SyncState::Recovering);  // response not back yet
  CHECK_EQ(e.buffered_now(), 4U);
  CHECK_EQ(e.book().digest(), fx.ref(10));
  fx.send(e, {16});
  CHECK(e.state() == SyncState::Synced);  // 4 events after the gap opened
  CHECK_EQ(e.next_apply(), 17U);
  CHECK_EQ(e.book().digest(), fx.ref(16));
  CHECK(e.counters().episode_events_max >= 4);
}

TEST(engine_bounded_buffer_drops_overflow_and_recovery_still_repairs_everything) {
  const Fx fx;
  RecoveryEngine e = fx.engine(RecoveryStrategy::Retransmit, src(1000, 0), 0, /*latency=*/5, /*buffer=*/2);
  fx.send_range(e, 1, 10);
  fx.send_range(e, 12, 17);  // 11 lost; only 2 of the 6 later messages fit in the buffer; the response is due 5 events after 12
  CHECK_EQ(e.counters().overflow_dropped, 4U);
  CHECK(e.state() == SyncState::Synced);  // the log supplies 11 and also the dropped ones
  CHECK_EQ(e.book().digest(), fx.ref(17));
  CHECK_EQ(e.counters().recovered_applied, 5U);  // 11 plus the four overflow victims
  check_identities(e);
}

TEST(engine_join_mid_stream_starts_at_the_first_message_seen) {
  const Fx fx;
  RecoveryConfig c;
  c.strategy = RecoveryStrategy::None;
  RecoveryEngine e(c, fx.data, 1, /*join=*/true);
  for (std::uint64_t s = 100; s <= 110; ++s) e.on_message(fx.msgs[s - 1]);
  CHECK(e.state() == SyncState::Synced);
  CHECK_EQ(e.next_apply(), 111U);
  CHECK_EQ(e.counters().gaps_opened, 0U);
}

TEST(engine_naive_control_applies_in_arrival_order_but_cannot_apply_anything_older) {
  const Fx fx;
  RecoveryEngine dup = fx.engine(RecoveryStrategy::Naive);
  fx.send(dup, {1, 2, 2, 3});  // a duplicate must not be applied twice
  CHECK_EQ(dup.counters().stale_ignored, 1U);
  CHECK_EQ(dup.counters().applied_direct, 3U);
  CHECK_EQ(dup.book().duplicate_adds(), 0U);
  CHECK_EQ(dup.book().digest(), fx.ref(3));

  RecoveryEngine late = fx.engine(RecoveryStrategy::Naive);
  fx.send(late, {1, 2, 4, 3});  // 3 arrives after 4: too old to apply, so the book silently lacks it
  CHECK_EQ(late.counters().stale_ignored, 1U);
  CHECK_EQ(late.next_apply(), 5U);
  CHECK(late.book().digest() != fx.ref(4));
  CHECK(late.state() == SyncState::Synced);  // and it still believes it is fine
}

TEST(engine_naive_control_silently_corrupts_while_claiming_to_be_synced) {
  const Fx fx(4000);
  RecoveryEngine naive = fx.engine(RecoveryStrategy::Naive);
  RecoveryEngine strict = fx.engine(RecoveryStrategy::Hybrid, src(1000, 200));
  for (std::uint64_t s = 1; s <= 4000; ++s) {
    if (s % 53 == 0) continue;  // a lossy feed
    naive.on_message(fx.msgs[s - 1]);
    strict.on_message(fx.msgs[s - 1]);
  }
  CHECK(naive.state() == SyncState::Synced);  // it believes it is fine...
  CHECK(naive.book().digest() != fx.ref(naive.next_apply() - 1));  // ...and it is wrong
  CHECK(naive.book().unknown_cancels() > 0);
  CHECK(strict.book().digest() == fx.ref(strict.next_apply() - 1));  // the strict engine is right at every point it claims
  CHECK_EQ(strict.book().unknown_cancels(), 0U);
  CHECK(strict.state() != SyncState::Unavailable);
}

// ------------------------------------------------- the central invariant ----

namespace {

struct Plan {
  std::vector<std::uint64_t> arrivals;
};

// Random drop / duplicate / delay of 1..n.
Plan faulty_arrivals(std::uint64_t n, SplitMix64& rng, std::uint32_t drop_ppm, std::uint32_t dup_ppm, std::uint32_t delay_ppm,
                     std::size_t max_delay) {
  Plan p;
  for (std::uint64_t s = 1; s <= n; ++s) {
    if (rng.chance_ppm(drop_ppm)) continue;
    p.arrivals.push_back(s);
    if (rng.chance_ppm(dup_ppm)) p.arrivals.push_back(s);
  }
  for (std::size_t i = 0; i + 1 < p.arrivals.size(); ++i) {
    if (rng.chance_ppm(delay_ppm)) {
      const std::size_t k = std::min<std::size_t>(1 + rng.uniform(max_delay), p.arrivals.size() - 1 - i);
      std::rotate(p.arrivals.begin() + static_cast<std::ptrdiff_t>(i), p.arrivals.begin() + static_cast<std::ptrdiff_t>(i) + 1,
                  p.arrivals.begin() + static_cast<std::ptrdiff_t>(i + k) + 1);
    }
  }
  return p;
}

}  // namespace

TEST(engine_property_strict_strategies_are_always_a_consistent_prefix_never_wrong) {
  const Fx fx(1500, 5);
  const RecoveryStrategy strategies[] = {RecoveryStrategy::None, RecoveryStrategy::Retransmit, RecoveryStrategy::Snapshot,
                                         RecoveryStrategy::Hybrid};
  std::uint64_t runs = 0, synced_at_end = 0, unavailable_runs = 0, recoveries = 0;
  for (std::uint64_t seed = 1; seed <= 60; ++seed) {
    SplitMix64 rng(seed * 104729);
    const Plan plan = faulty_arrivals(1500, rng, 20'000, 20'000, 40'000, 12);
    for (const RecoveryStrategy strat : strategies) {
      RecoveryConfig c;
      c.strategy = strat;
      c.source.retention = static_cast<std::size_t>(5 + rng.uniform(300));
      c.source.snapshot_interval = 20 + rng.uniform(200);
      c.source.max_snapshots = 1 + static_cast<std::size_t>(rng.uniform(6));
      c.reorder_wait_events = rng.uniform(15);
      c.latency_events = rng.uniform(40);
      c.max_buffered = 1 + static_cast<std::size_t>(rng.uniform(400));
      RecoveryEngine e(c, fx.data, 1, false);

      std::uint64_t last_next = e.next_apply();
      for (const std::uint64_t seq : plan.arrivals) {
        e.on_message(fx.msgs[static_cast<std::size_t>(seq - 1)]);
        // THE invariant: whatever state it is in, the book equals the truth at next_apply - 1.
        CHECK_EQ(e.book().digest(), fx.ref(e.next_apply() - 1));
        CHECK(e.next_apply() >= last_next);  // never moves backwards
        last_next = e.next_apply();
        if (e.state() == SyncState::Synced) CHECK_EQ(e.next_apply(), e.max_seen() + 1);  // synced means nothing outstanding
        if (e.state() == SyncState::Unavailable) CHECK(strat == RecoveryStrategy::None || strat == RecoveryStrategy::Retransmit);
        if (e.book().unknown_cancels() != 0 || e.book().duplicate_adds() != 0) CHECK(false);  // never a visible anomaly
      }
      check_identities(e);
      ++runs;
      synced_at_end += e.state() == SyncState::Synced ? 1U : 0U;
      unavailable_runs += e.state() == SyncState::Unavailable ? 1U : 0U;
      recoveries += e.counters().recoveries_retransmit + e.counters().recoveries_snapshot;
    }
  }
  // Sanity: the random plans really exercised recovery, healing and failure.
  CHECK_EQ(runs, 240U);
  CHECK(recoveries > 100);
  CHECK(unavailable_runs > 0);
  CHECK(synced_at_end > 0);
}

TEST(engine_property_the_naive_control_does_violate_the_invariant) {
  const Fx fx(1500, 5);
  std::uint64_t violated_runs = 0;
  for (std::uint64_t seed = 1; seed <= 20; ++seed) {
    SplitMix64 rng(seed * 7);
    const Plan plan = faulty_arrivals(1500, rng, 20'000, 20'000, 40'000, 12);
    RecoveryEngine e = fx.engine(RecoveryStrategy::Naive);
    for (const std::uint64_t seq : plan.arrivals) e.on_message(fx.msgs[static_cast<std::size_t>(seq - 1)]);
    if (e.book().digest() != fx.ref(e.next_apply() - 1)) ++violated_runs;
  }
  CHECK(violated_runs >= 15);  // the check is meaningful: it fails for the control
}

TEST(engine_property_with_a_generous_service_every_complete_stream_ends_synced_and_exact) {
  // Drop/reorder everything except the final stretch, retain plenty: Hybrid must end Synced with the exact final state.
  const Fx fx(2000, 8);
  for (std::uint64_t seed = 1; seed <= 25; ++seed) {
    SplitMix64 rng(seed * 31);
    Plan plan = faulty_arrivals(1800, rng, 30'000, 20'000, 40'000, 10);
    for (std::uint64_t s = 1801; s <= 2000; ++s) plan.arrivals.push_back(s);  // an intact tail exposes any trailing gap
    RecoveryEngine e = fx.engine(RecoveryStrategy::Hybrid, src(5000, 100), 3, 10);
    for (const std::uint64_t seq : plan.arrivals) e.on_message(fx.msgs[static_cast<std::size_t>(seq - 1)]);
    CHECK(e.state() == SyncState::Synced);
    CHECK_EQ(e.next_apply(), 2001U);
    CHECK_EQ(e.book().digest(), fx.ref(2000));
    check_identities(e);
  }
}

TEST(engine_snapshot_and_retransmit_agree_on_the_state_they_restore) {
  const Fx fx(1000, 12);
  SplitMix64 rng(99);
  Plan plan = faulty_arrivals(900, rng, 25'000, 0, 0, 1);
  for (std::uint64_t s = 901; s <= 1000; ++s) plan.arrivals.push_back(s);
  RecoveryEngine a = fx.engine(RecoveryStrategy::Retransmit, src(5000, 0));
  RecoveryEngine b = fx.engine(RecoveryStrategy::Snapshot, src(5000, 50));
  for (const std::uint64_t seq : plan.arrivals) {
    a.on_message(fx.msgs[static_cast<std::size_t>(seq - 1)]);
    b.on_message(fx.msgs[static_cast<std::size_t>(seq - 1)]);
  }
  CHECK(a.state() == SyncState::Synced && b.state() == SyncState::Synced);
  CHECK_EQ(a.book().digest(), b.book().digest());
  CHECK(a.book().orders() == b.book().orders());
  CHECK(a.counters().recovered_applied > 0 && b.counters().snapshots_loaded > 0);
}

// ----------------------------------------------------------------- tracker ----

TEST(tracker_keeps_streams_independent_and_reports_against_the_reference) {
  const auto msgs = clean_messages(400, 3);
  std::vector<Message> two;  // the same content as two streams
  for (const Message& m : msgs) {
    Message a = m;
    a.stream_id = 1;
    Message b = m;
    b.stream_id = 2;
    two.push_back(a);
    two.push_back(b);
  }
  auto data = authoritative(two);
  RecoveryConfig c;
  c.strategy = RecoveryStrategy::Hybrid;
  c.source.snapshot_interval = 50;
  StateTracker t(c, data, false);
  for (const Message& m : two) {
    if (m.stream_id == 2 && m.seq % 37 == 0) continue;  // only stream 2 is lossy
    t.on_message(m);
  }
  CHECK_EQ(t.streams(), 2U);
  const auto rep = t.report();
  CHECK_EQ(rep.size(), 2U);
  CHECK(rep[0].stream_id == 1 && rep[0].state == SyncState::Synced && rep[0].matches_reference);
  CHECK_EQ(rep[0].next_apply, 401U);
  CHECK(rep[1].reference_known && rep[1].matches_reference);  // stale or synced, but never wrong
  CHECK(rep[1].counters.gaps_opened > 0);
  CHECK(t.engine(1) != nullptr && t.engine(3) == nullptr);

  StateTracker none(c, nullptr, false);  // no authoritative data: reports no reference, does not crash
  none.on_message(msgs[0]);
  CHECK(!none.report()[0].reference_known);
}
