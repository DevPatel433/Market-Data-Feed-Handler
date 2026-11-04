#include <algorithm>
#include <vector>

#include "check.hpp"
#include "feed/fault_injector.hpp"
#include "feed/ingest.hpp"
#include "feed/replayer.hpp"
#include "feed/rng.hpp"
#include "feed/workload.hpp"
#include "seq_oracle.hpp"

using namespace feed;

namespace {

WorkloadConfig wcfg(Profile p, std::uint64_t count = 10'000, std::uint64_t seed = 77) {
  WorkloadConfig c;
  c.profile = p;
  c.count = count;
  c.seed = seed;
  return c;
}

// Max valid sequence number present in a record list (via the real decoder).
std::uint64_t max_valid_seq(const std::vector<ReplayRecord>& recs) {
  std::uint64_t m = 0;
  for (const auto& r : recs) {
    Message msg;
    if (decode(r.frame, msg) == DecodeError::None) m = std::max(m, msg.seq);
  }
  return m;
}

void run(Ingest& in, const std::vector<ReplayRecord>& recs) {
  for (const auto& r : recs) in.on_frame(r.frame);
}

std::vector<ReplayRecord> heartbeat_stream(std::uint64_t n) {
  std::vector<ReplayRecord> recs(n);
  for (std::uint64_t i = 0; i < n; ++i) {
    recs[i].offset_ns = i * 1000;
    encode(Message{1, i + 1, Heartbeat{}}, recs[i].frame);
  }
  return recs;
}

std::vector<std::uint64_t> seqs_of(const std::vector<ReplayRecord>& recs) {
  std::vector<std::uint64_t> s;
  for (const auto& r : recs) {
    Message m;
    if (decode(r.frame, m) == DecodeError::None) s.push_back(m.seq);
  }
  return s;
}

}  // namespace

// ---------------------------------------------------------------- Ingest ----

TEST(ingest_clean_workload_is_fully_forwarded_in_order) {
  std::vector<std::uint64_t> delivered;
  Ingest in({}, [&](const Message& m, StreamState st) {
    CHECK(st == StreamState::Synced);
    delivered.push_back(m.seq);
  });
  const Workload w = generate_workload(wcfg(Profile::Normal));
  run(in, w.records);
  CHECK_EQ(delivered.size(), 10'000U);
  for (std::size_t i = 0; i < delivered.size(); ++i) CHECK_EQ(delivered[i], i + 1);
  CHECK(in.counters().balanced());
  CHECK_EQ(in.counters().forwarded, 10'000U);
  CHECK_EQ(in.sequencer().stats().missing_total, 0U);
}

TEST(ingest_bursty_workload_has_no_anomalies) {
  Ingest in;
  run(in, generate_workload(wcfg(Profile::Bursty)).records);
  CHECK_EQ(in.counters().forwarded, 10'000U);
  CHECK(in.sequencer().stream(1)->state == StreamState::Synced);
}

TEST(ingest_duplicate_profile_matches_manifest) {
  const Workload w = generate_workload(wcfg(Profile::Duplicate));
  Ingest in;
  run(in, w.records);
  CHECK(w.stats.duplicated > 0);
  CHECK_EQ(in.counters().duplicate_discarded, w.stats.duplicated);
  CHECK_EQ(in.counters().forwarded, w.stats.clean_messages);
  CHECK_EQ(in.sequencer().stats().missing_total, 0U);
  CHECK(in.sequencer().stream(1)->state == StreamState::Synced);  // duplicates alone never degrade
  CHECK(in.counters().balanced());
}

TEST(ingest_gap_profile_matches_manifest) {
  const Workload w = generate_workload(wcfg(Profile::Gap));
  Ingest in;
  run(in, w.records);
  const std::uint64_t trailing = w.stats.clean_messages - max_valid_seq(w.records);  // undetectable tail loss
  const auto s = in.sequencer().stats();
  CHECK(w.stats.dropped > 0);
  CHECK_EQ(s.missing_total + trailing, w.stats.dropped);
  CHECK_EQ(in.sequencer().outstanding_seqs(), s.missing_total);  // nothing ever arrives late
  CHECK_EQ(s.out_of_order, 0U);
  CHECK(in.sequencer().stream(1)->state == StreamState::Degraded);
  CHECK(in.counters().balanced());
}

TEST(ingest_reorder_profile_every_swap_is_one_gap_and_one_late_arrival) {
  const Workload w = generate_workload(wcfg(Profile::Reorder));
  Ingest in;
  run(in, w.records);
  const auto s = in.sequencer().stats();
  CHECK(w.stats.reordered > 0);
  CHECK_EQ(s.gap_events, w.stats.reordered);
  CHECK_EQ(s.out_of_order, w.stats.reordered);
  CHECK_EQ(s.missing_total, w.stats.reordered);
  CHECK_EQ(in.sequencer().outstanding_seqs(), 0U);  // everything eventually arrived
  CHECK_EQ(in.counters().late_discarded, w.stats.reordered);
  CHECK(in.counters().balanced());
}

TEST(ingest_rejected_frames_never_reach_the_sequencer_and_show_up_as_gaps) {
  const Workload w = generate_workload(wcfg(Profile::Malformed));
  Ingest in;
  run(in, w.records);
  const auto& c = in.counters();
  CHECK(w.stats.malformed > 0);
  CHECK_EQ(c.rejected, w.stats.malformed);
  CHECK_EQ(in.sequencer().stats().observed(), c.decoded);  // sequencer saw valid frames only
  const std::uint64_t trailing = w.stats.clean_messages - max_valid_seq(w.records);
  CHECK_EQ(in.sequencer().stats().missing_total + trailing, c.rejected);  // each reject = one missing seq
  CHECK(c.balanced());
}

TEST(ingest_counters_stay_balanced_under_every_policy_and_all_faults) {
  const Workload base = generate_workload(wcfg(Profile::Malformed, 5000, 9));
  FaultConfig f;
  f.seed = 3;
  f.drop_ppm = f.duplicate_ppm = f.reorder_ppm = 20'000;
  FaultReport rep;
  const auto faulted = inject_faults(base.records, f, rep);
  for (const GapPolicy g : {GapPolicy::MarkDegraded, GapPolicy::Pause}) {
    for (const LatePolicy l : {LatePolicy::Discard, LatePolicy::Forward}) {
      SequencerConfig cfg;
      cfg.gap_policy = g;
      cfg.late_policy = l;
      Ingest in(cfg);
      run(in, faulted);
      CHECK(in.counters().balanced());
      CHECK_EQ(in.counters().rx_frames, faulted.size());
      const auto s = in.sequencer().stats();
      CHECK_EQ(s.missing_total, s.out_of_order + in.sequencer().outstanding_seqs() + s.seqs_evicted + s.resync_cleared_seqs);
    }
  }
}

TEST(ingest_pause_policy_forwards_only_the_prefix_before_the_first_gap) {
  auto recs = heartbeat_stream(100);
  recs.erase(recs.begin() + 49);  // drop seq 50
  SequencerConfig cfg;
  cfg.gap_policy = GapPolicy::Pause;
  Ingest in(cfg);
  run(in, recs);
  CHECK_EQ(in.counters().forwarded, 49U);
  CHECK_EQ(in.counters().paused_discarded, 50U);
  CHECK(in.sequencer().stream(1)->state == StreamState::Paused);
  CHECK(in.counters().balanced());
}

// -------------------------------------------------------- Fault injector ----

TEST(injector_zero_rates_is_identity) {
  const auto in = heartbeat_stream(500);
  FaultReport rep;
  const auto out = inject_faults(in, {}, rep);
  CHECK_EQ(out.size(), in.size());
  for (std::size_t i = 0; i < in.size(); ++i) CHECK(out[i].frame == in[i].frame && out[i].offset_ns == in[i].offset_ns);
  CHECK_EQ(rep.dropped + rep.duplicated + rep.reordered, 0U);
}

TEST(injector_is_deterministic_and_seed_sensitive) {
  const auto in = heartbeat_stream(2000);
  FaultConfig f;
  f.drop_ppm = f.duplicate_ppm = f.reorder_ppm = 30'000;
  FaultReport r1, r2, r3;
  const auto a = inject_faults(in, f, r1);
  const auto b = inject_faults(in, f, r2);
  f.seed = 2;
  const auto c = inject_faults(in, f, r3);
  CHECK_EQ(a.size(), b.size());
  CHECK(seqs_of(a) == seqs_of(b));
  CHECK(seqs_of(a) != seqs_of(c));
}

TEST(injector_drop_only_creates_exactly_the_expected_loss_metrics) {
  const auto in = heartbeat_stream(10'000);
  FaultConfig f;
  f.drop_ppm = 10'000;
  FaultReport rep;
  const auto out = inject_faults(in, f, rep);
  CHECK(rep.dropped > 0);
  CHECK_EQ(out.size() + rep.dropped, in.size());
  CHECK_EQ(seqs_of(out).front(), 1U);  // first frame protected

  Ingest ing;
  run(ing, out);
  const auto s = ing.sequencer().stats();
  const std::uint64_t last = seqs_of(out).back();
  CHECK_EQ(s.missing_total, last - out.size());  // everything before the last delivered frame that is absent
  CHECK_EQ(ing.sequencer().outstanding_seqs(), s.missing_total);
  CHECK_EQ(s.duplicates + s.out_of_order, 0U);
  CHECK(ing.sequencer().stream(1)->state == StreamState::Degraded);
}

TEST(injector_duplicate_only_creates_exactly_the_expected_duplicates) {
  const auto in = heartbeat_stream(10'000);
  FaultConfig f;
  f.duplicate_ppm = 20'000;
  FaultReport rep;
  const auto out = inject_faults(in, f, rep);
  CHECK(rep.duplicated > 0);
  CHECK_EQ(out.size(), in.size() + rep.duplicated);

  Ingest ing;
  run(ing, out);
  CHECK_EQ(ing.sequencer().stats().duplicates, rep.duplicated);
  CHECK_EQ(ing.counters().forwarded, in.size());
  CHECK_EQ(ing.sequencer().stats().missing_total, 0U);
  CHECK(ing.sequencer().stream(1)->state == StreamState::Synced);

  for (std::size_t i = 1; i < out.size(); ++i) CHECK(out[i].offset_ns >= out[i - 1].offset_ns);
}

TEST(injector_reorder_only_is_one_gap_plus_one_late_arrival_per_delayed_frame) {
  const auto in = heartbeat_stream(10'000);
  FaultConfig f;
  f.reorder_ppm = 20'000;
  f.reorder_distance_max = 5;
  FaultReport rep;
  const auto out = inject_faults(in, f, rep);
  CHECK(rep.reordered > 0);
  CHECK_EQ(out.size(), in.size());

  // Offsets stay in their slots; every frame is displaced at most 5 positions.
  const auto seqs = seqs_of(out);
  for (std::size_t i = 0; i < out.size(); ++i) {
    CHECK_EQ(out[i].offset_ns, in[i].offset_ns);
    const auto home = static_cast<std::int64_t>(seqs[i] - 1);
    const auto here = static_cast<std::int64_t>(i);
    CHECK(here - home <= 5 && home - here <= 5);
  }
  auto sorted = seqs;
  std::sort(sorted.begin(), sorted.end());
  for (std::size_t i = 0; i < sorted.size(); ++i) CHECK_EQ(sorted[i], i + 1);  // nothing lost or duplicated

  Ingest ing;
  run(ing, out);
  const auto s = ing.sequencer().stats();
  CHECK_EQ(s.gap_events, rep.reordered);
  CHECK_EQ(s.out_of_order, rep.reordered);
  CHECK_EQ(ing.sequencer().outstanding_seqs(), 0U);
  CHECK_EQ(s.duplicates, 0U);
  // With late=discard the consumer never saw the late frames, so the stream stays degraded...
  CHECK(ing.sequencer().stream(1)->state == StreamState::Degraded);

  // ...whereas forwarding them repairs the stream completely.
  SequencerConfig cfg;
  cfg.late_policy = LatePolicy::Forward;
  Ingest healed(cfg);
  run(healed, out);
  CHECK_EQ(healed.counters().forwarded, in.size());
  CHECK(healed.sequencer().stream(1)->state == StreamState::Synced);
}

TEST(injector_combined_faults_match_the_reference_model_arrival_by_arrival) {
  const auto in = heartbeat_stream(20'000);
  FaultConfig f;
  f.seed = 77;
  f.drop_ppm = f.duplicate_ppm = f.reorder_ppm = 15'000;
  f.reorder_distance_max = 6;
  FaultReport rep;
  const auto out = inject_faults(in, f, rep);
  CHECK(rep.dropped > 0 && rep.duplicated > 0 && rep.reordered > 0);
  CHECK_EQ(out.size(), in.size() - rep.dropped + rep.duplicated);

  feedtest::Oracle oracle;
  Ingest ing;
  for (const auto& r : out) {
    const IngestResult res = ing.on_frame(r.frame);
    CHECK(res.error == DecodeError::None);
    std::uint64_t first = 0, count = 0;
    CHECK(res.verdict.cls == oracle.classify(res.seq, first, count));
    if (res.verdict.cls == Classification::Gap) CHECK_EQ(res.verdict.gap_count, count);
  }
  CHECK_EQ(ing.sequencer().outstanding_seqs(), oracle.missing.size());
  CHECK(ing.counters().balanced());

  // The faulted stream is still a valid replay file.
  std::vector<std::uint8_t> img;
  std::vector<ReplayRecord> back;
  CHECK(serialize_replay(out, img));
  CHECK(parse_replay(img, back) == ReplayError::None);
  CHECK_EQ(back.size(), out.size());
}

TEST(injector_handles_degenerate_inputs) {
  FaultConfig f;
  f.drop_ppm = f.duplicate_ppm = f.reorder_ppm = 1'000'000;  // everything eligible
  f.drop_burst_max = 0;
  f.reorder_distance_max = 0;
  FaultReport rep;
  CHECK(inject_faults({}, f, rep).empty());
  const auto one = inject_faults(heartbeat_stream(1), f, rep);
  CHECK_EQ(one.size(), 2U);  // first frame protected from drop, then duplicated
  const auto many = inject_faults(heartbeat_stream(50), f, rep);
  CHECK(!many.empty());
  Ingest ing;
  run(ing, many);
  CHECK(ing.counters().balanced());
}

// ---------------------------------------------------------- Rate replay ----

TEST(replayer_rate_mode_ignores_recorded_offsets_and_paces_by_rate) {
  std::vector<ReplayRecord> recs;
  for (std::uint8_t i = 0; i < 50; ++i) recs.push_back({1'000'000'000'000ULL + i, {i}});  // offsets ~1000 s: must be ignored
  ReplayOptions opts;
  opts.mode = ReplayMode::Rate;
  opts.rate_per_sec = 2500;  // 50 frames -> last one scheduled at 49/2500 s = 19.6 ms
  std::vector<std::uint8_t> order;
  const auto st = replay(recs, opts, [&](std::span<const std::uint8_t> f) { order.push_back(f[0]); });
  CHECK_EQ(st.frames, 50U);
  CHECK(st.wall_ns >= 19'000'000ULL);   // paced, not instantaneous
  CHECK(st.wall_ns < 5'000'000'000ULL); // and certainly did not wait for the recorded offsets
  for (std::size_t i = 0; i < order.size(); ++i) CHECK_EQ(order[i], i);
}

TEST(ingest_counts_each_rejection_reason_separately) {
  const auto valid = [] {
    std::vector<std::uint8_t> f;
    encode(Message{1, 1, AddOrder{1, 100, 1, 1, Side::Buy}}, f);
    return f;
  };
  Ingest in;
  in.on_frame({});                                    // too_short
  auto big = valid();
  big.resize(kMaxMessageSize + 1);
  in.on_frame(big);                                   // too_large
  auto f = valid();
  f[0] = 9;
  in.on_frame(f);                                     // bad_version (x1)
  f = valid();
  f[1] = 77;
  in.on_frame(f);                                     // unknown_type
  f = valid();
  f[3] = 1;
  in.on_frame(f);                                     // length_mismatch
  Message m{1, 0, Heartbeat{}};                       // seq 0
  f.clear();
  encode(m, f);
  in.on_frame(f);                                     // bad_field
  const auto& c = in.counters();
  CHECK_EQ(c.rejected, 6U);
  for (const DecodeError e : {DecodeError::TooShort, DecodeError::TooLarge, DecodeError::BadVersion,
                              DecodeError::UnknownType, DecodeError::LengthMismatch, DecodeError::BadField}) {
    CHECK_EQ(c.rejected_by[static_cast<std::size_t>(e)], 1U);
  }
  CHECK_EQ(c.rejected_by[0], 0U);  // index 0 is DecodeError::None
  CHECK_EQ(in.sequencer().stream_count(), 0U);  // none of it reached the sequencer
  CHECK(c.balanced());
}

TEST(injector_duplicates_share_their_originals_offset) {
  const auto in = heartbeat_stream(5000);
  FaultConfig f;
  f.duplicate_ppm = 50'000;
  FaultReport rep;
  const auto out = inject_faults(in, f, rep);
  std::uint64_t pairs = 0;
  for (std::size_t i = 1; i < out.size(); ++i) {
    if (out[i].frame == out[i - 1].frame) {  // heartbeats are unique per seq: equal frames are copies
      CHECK_EQ(out[i].offset_ns, out[i - 1].offset_ns);
      ++pairs;
    }
  }
  CHECK_EQ(pairs, rep.duplicated);
}

TEST(injector_later_passes_never_change_what_earlier_passes_chose) {
  // Passes run drop -> duplicate -> reorder, each with its own RNG stream, so enabling a
  // later fault type must not change the frames chosen by an earlier one.
  const auto in = heartbeat_stream(20'000);
  const auto dedup_sorted = [](std::vector<std::uint64_t> s) {
    std::sort(s.begin(), s.end());
    s.erase(std::unique(s.begin(), s.end()), s.end());
    return s;
  };
  FaultReport r;
  FaultConfig drop_only;
  drop_only.drop_ppm = 20'000;
  const auto base = seqs_of(inject_faults(in, drop_only, r));
  const std::uint64_t dropped = r.dropped;

  FaultConfig plus_dup = drop_only;
  plus_dup.duplicate_ppm = 20'000;
  const auto with_dup = inject_faults(in, plus_dup, r);
  CHECK_EQ(r.dropped, dropped);
  CHECK(dedup_sorted(seqs_of(with_dup)) == dedup_sorted(base));  // same frames dropped

  FaultConfig plus_reorder = plus_dup;
  plus_reorder.reorder_ppm = 20'000;
  const auto with_reorder = inject_faults(in, plus_reorder, r);
  CHECK_EQ(r.dropped, dropped);
  auto a = seqs_of(with_dup), b = seqs_of(with_reorder);  // same multiset, only the order differs
  std::sort(a.begin(), a.end());
  std::sort(b.begin(), b.end());
  CHECK(a == b);
}
