#include <algorithm>
#include <vector>

#include "check.hpp"
#include "feed/fault_injector.hpp"
#include "feed/pipeline.hpp"
#include "feed/workload.hpp"

using namespace feed;

namespace {

std::vector<ReplayRecord> workload(std::uint64_t count, std::uint64_t seed = 5, Profile p = Profile::Normal) {
  WorkloadConfig c;
  c.profile = p;
  c.count = count;
  c.seed = seed;
  return generate_workload(c).records;
}

std::shared_ptr<const AuthoritativeData> authoritative(const std::vector<ReplayRecord>& clean) {
  auto d = std::make_shared<AuthoritativeData>();
  std::string e;
  CHECK(AuthoritativeData::from_records(clean, *d, e));
  return d;
}

PipelineConfig state_config(RecoveryStrategy s, std::shared_ptr<const AuthoritativeData> data) {
  PipelineConfig c;
  c.state.enabled = true;
  c.state.recovery.strategy = s;
  c.state.recovery.source.retention = 100'000;
  c.state.recovery.source.snapshot_interval = 250;
  c.state.authoritative = std::move(data);
  c.sequencer.late_policy = LatePolicy::Forward;  // let late arrivals reach the engine, which can use them
  c.queue_capacity = 256;
  c.full_policy = QueueFullPolicy::Block;
  return c;
}

}  // namespace

// ------------------------------------------------------------- configuration ----

TEST(pipeline_refuses_configurations_that_cannot_work) {
  const auto recs = workload(100);
  {
    PipelineConfig c = state_config(RecoveryStrategy::Hybrid, nullptr);  // needs a source
    const PipelineReport r = run_pipeline(recs, c);
    CHECK(!r.config_error.empty());
    CHECK(r.config_error.find("authoritative") != std::string::npos);
    CHECK(!r.balanced());
    CHECK_EQ(r.ingest.rx_frames, 0U);  // nothing ran
  }
  {
    PipelineConfig c = state_config(RecoveryStrategy::Hybrid, authoritative(recs));
    c.sequencer.gap_policy = GapPolicy::Pause;  // starves the engine of the messages it needs
    CHECK(!run_pipeline(recs, c).config_error.empty());
    c.state.recovery.strategy = RecoveryStrategy::Naive;  // the control has no such requirement
    CHECK(run_pipeline(recs, c).config_error.empty());
  }
  {
    PipelineConfig c;
    c.queue_capacity = 0;
    std::string e;
    CHECK(!c.validate(e));
  }
  PipelineConfig ok = state_config(RecoveryStrategy::None, nullptr);  // strict-order-only needs no source
  std::string e;
  CHECK(ok.validate(e));
}

// -------------------------------------------------------- lag and occupancy ----

TEST(pipeline_measures_offer_to_dequeue_lag_for_every_processed_event) {
  const auto recs = workload(1500);
  PipelineConfig c;
  c.queue_capacity = 8;
  c.full_policy = QueueFullPolicy::Block;
  c.consumer_work_ns = 20'000;
  c.measure_lag = true;
  const PipelineReport r = run_pipeline(recs, c);
  CHECK(r.balanced());  // includes lag.count() == processed
  CHECK_EQ(r.lag.count(), 1500U);
  CHECK(r.lag.percentile(50) <= r.lag.percentile(99));
  CHECK(r.lag.percentile(99) <= r.lag.max());
  CHECK(r.lag.max() >= 5'000);  // behind a slow consumer, somebody waited
  CHECK(r.lag.max() < 2'000'000'000ULL);  // and the lag is a real interval, not "now minus an unset timestamp"
  CHECK(r.lag.mean() > 0.0);

  PipelineConfig off;
  const PipelineReport q = run_pipeline(recs, off);
  CHECK(!q.lag_measured);
  CHECK_EQ(q.lag.count(), 0U);
}

TEST(pipeline_samples_queue_occupancy_into_a_timeline) {
  const auto recs = workload(2000);
  PipelineConfig c;
  c.queue_capacity = 64;
  c.full_policy = QueueFullPolicy::Block;
  c.consumer_work_ns = 10'000;
  c.sample_interval_us = 200;
  const PipelineReport r = run_pipeline(recs, c);
  CHECK(r.balanced());
  CHECK(r.timeline.size() >= 5);
  for (std::size_t i = 0; i < r.timeline.size(); ++i) {
    CHECK(r.timeline[i].occupancy <= r.queue_capacity);
    if (i > 0) CHECK(r.timeline[i].t_ns >= r.timeline[i - 1].t_ns);
  }
  CHECK_EQ(r.timeline.back().occupancy, 0U);  // the final sample is taken after the queue drained
  CHECK(r.steady.peak <= r.queue_capacity);
  CHECK(r.producer_end_ns > 0);
  CHECK(r.timeline.back().t_ns >= r.producer_end_ns);
}

TEST(pipeline_timeline_always_ends_with_a_post_drain_sample) {
  // A sampling interval far longer than the run: the sampler gets one early sample, so the run's end state is
  // present only because the pipeline records an explicit final point after the consumer has drained.
  const auto recs = workload(1000);
  PipelineConfig c;
  c.queue_capacity = 32;
  c.full_policy = QueueFullPolicy::Block;
  c.consumer_work_ns = 5'000;
  c.sample_interval_us = 100'000;
  const PipelineReport r = run_pipeline(recs, c);
  CHECK(r.balanced());
  CHECK(r.timeline.size() >= 2);
  CHECK_EQ(r.timeline.back().occupancy, 0U);
  CHECK(r.timeline.back().t_ns >= r.producer_end_ns);  // taken after the source finished and the queue drained
}

TEST(pipeline_block_holds_the_source_back_while_drop_lets_it_run_ahead) {
  const auto recs = workload(1500);
  PipelineConfig block;
  block.queue_capacity = 8;
  block.full_policy = QueueFullPolicy::Block;
  block.consumer_work_ns = 20'000;  // 1500 x 20 us = 30 ms of consumer work
  PipelineConfig drop = block;
  drop.full_policy = QueueFullPolicy::DropAndCount;

  const PipelineReport b = run_pipeline(recs, block);
  const PipelineReport d = run_pipeline(recs, drop);
  CHECK(b.balanced() && d.balanced());
  CHECK_EQ(b.overflow.dropped, 0U);                     // complete...
  CHECK(b.replay.wall_ns >= 15'000'000ULL);             // ...but the source was held to the consumer's pace (>= half the work)
  CHECK(b.overflow.stall_ns > 0);
  CHECK(b.overflow.stall_ns <= b.replay.wall_ns + 1'000'000ULL);  // stalls are part of the replay's wall time
  CHECK(d.overflow.dropped > 0);                        // lossy...
  CHECK(d.replay.wall_ns < b.replay.wall_ns);           // ...but the source ran ahead at its own pace
  CHECK_EQ(d.overflow.stall_ns, 0U);
}

// ------------------------------------------------------------ state through the pipeline ----

TEST(pipeline_state_on_a_clean_stream_ends_synced_and_exact) {
  const auto recs = workload(3000);
  for (const RecoveryStrategy s : {RecoveryStrategy::None, RecoveryStrategy::Hybrid, RecoveryStrategy::Naive}) {
    const PipelineConfig c = state_config(s, authoritative(recs));
    const PipelineReport r = run_pipeline(recs, c);
    CHECK(r.balanced());
    CHECK(r.state_consistent());
    CHECK_EQ(r.state.size(), 1U);
    CHECK(r.state[0].state == SyncState::Synced);
    CHECK_EQ(r.state[0].next_apply, 3001U);
    CHECK(r.state[0].matches_reference);
    CHECK_EQ(r.state[0].unknown_cancels, 0U);
    CHECK_EQ(r.state[0].counters.events, r.consumer.processed);
  }
}

TEST(pipeline_recovery_strategies_compared_on_the_same_faulty_feed) {
  const auto clean = workload(8000, 33);
  FaultConfig f;
  f.seed = 33;
  f.drop_ppm = f.duplicate_ppm = f.reorder_ppm = 10'000;
  FaultReport fr;
  auto faulty = inject_faults(std::vector<ReplayRecord>(clean.begin(), clean.begin() + 7500), f, fr);
  faulty.insert(faulty.end(), clean.begin() + 7500, clean.end());  // an intact tail, so any trailing loss is exposed
  CHECK(fr.dropped > 20);
  const auto data = authoritative(clean);

  const auto run = [&](RecoveryStrategy s) {
    PipelineConfig c = state_config(s, data);
    c.state.recovery.reorder_wait_events = 4;
    c.state.recovery.latency_events = 8;
    return run_pipeline(faulty, c);
  };

  const PipelineReport naive = run(RecoveryStrategy::Naive);
  CHECK(naive.balanced());
  CHECK(naive.state[0].state == SyncState::Synced);          // claims to be fine...
  CHECK(!naive.state[0].matches_reference);                    // ...is wrong
  CHECK(naive.state[0].unknown_cancels > 0);                   // and some of the damage is visible

  const PipelineReport none = run(RecoveryStrategy::None);
  CHECK(none.balanced());
  CHECK(none.state[0].state == SyncState::Unavailable);        // says so, instead of guessing
  CHECK(none.state[0].matches_reference);                      // frozen at an exactly correct earlier state
  CHECK(none.state[0].next_apply < 8000);

  for (const RecoveryStrategy s : {RecoveryStrategy::Retransmit, RecoveryStrategy::Snapshot, RecoveryStrategy::Hybrid}) {
    const PipelineReport r = run(s);
    CHECK(r.balanced());
    CHECK(r.state[0].state == SyncState::Synced);              // the lost messages were recovered
    CHECK_EQ(r.state[0].next_apply, 8001U);
    CHECK(r.state[0].matches_reference);                       // and the state is exactly the true final state
    CHECK_EQ(r.state[0].unknown_cancels, 0U);
    CHECK(r.state[0].counters.gaps_opened > 10);
    CHECK(r.state[0].counters.recoveries_retransmit + r.state[0].counters.recoveries_snapshot > 0);
  }
}

TEST(pipeline_a_hole_older_than_the_log_makes_retransmit_unavailable_but_hybrid_still_recovers) {
  const auto clean = workload(6000, 9);
  FaultConfig f;
  f.seed = 9;
  f.drop_ppm = 8'000;
  FaultReport fr;
  auto faulty = inject_faults(std::vector<ReplayRecord>(clean.begin(), clean.begin() + 5500), f, fr);
  faulty.insert(faulty.end(), clean.begin() + 5500, clean.end());
  const auto data = authoritative(clean);

  PipelineConfig c = state_config(RecoveryStrategy::Retransmit, data);
  c.state.recovery.source.retention = 5;         // a tiny window...
  c.state.recovery.latency_events = 40;          // ...and a slow round trip: the hole is gone from the log before the answer comes
  const PipelineReport r = run_pipeline(faulty, c);
  CHECK(r.balanced());
  CHECK(r.state[0].state == SyncState::Unavailable);
  CHECK(r.state[0].matches_reference);             // reported unavailable AND still exactly correct as of its last good point

  c.state.recovery.strategy = RecoveryStrategy::Hybrid;  // same unlucky log, but with a snapshot service behind it
  const PipelineReport h = run_pipeline(faulty, c);
  CHECK(h.balanced());
  CHECK(h.state[0].state == SyncState::Synced);
  CHECK_EQ(h.state[0].next_apply, 6001U);
  CHECK(h.state[0].matches_reference);
  CHECK(h.state[0].counters.recoveries_snapshot > 0);
}

TEST(pipeline_overload_loss_in_the_queue_is_repaired_by_recovery_downstream) {
  // Queue overflow is just another gap to the engine. A paced feed (20k events/s) against a consumer that needs
  // 150 us per event (6.7k/s) overflows a small queue steadily, with drops interleaved with accepted events on any
  // core count. The engine must still end with exactly the right state for what it has reached.
  const auto recs = workload(1500, 17);
  PipelineConfig c = state_config(RecoveryStrategy::Hybrid, authoritative(recs));
  c.queue_capacity = 8;
  c.full_policy = QueueFullPolicy::DropAndCount;
  c.consumer_work_ns = 150'000;
  c.replay.mode = ReplayMode::Rate;
  c.replay.rate_per_sec = 20'000;
  const PipelineReport r = run_pipeline(recs, c);
  CHECK(r.balanced());
  CHECK(r.overflow.dropped > 300);                       // heavy queue loss
  CHECK(r.consumer.processed > 100);                     // and the consumer really did keep up with a share of the stream
  CHECK_EQ(r.sequencing.missing_total, 0U);              // the "network" delivered everything
  CHECK(r.state[0].counters.gaps_opened > 20);           // the engine saw the queue's losses as many separate gaps
  CHECK(r.state[0].matches_reference);                   // and never applied anything out of order
  CHECK_EQ(r.state[0].unknown_cancels, 0U);
  CHECK(r.state[0].counters.recoveries_retransmit + r.state[0].counters.recoveries_snapshot > 20);
  CHECK(r.state[0].next_apply > 100);                    // it progressed well beyond the first few events

  // The control, same overload: applying whatever arrives silently diverges.
  c.state.recovery.strategy = RecoveryStrategy::Naive;
  const PipelineReport naive = run_pipeline(recs, c);
  CHECK(naive.balanced());
  CHECK(naive.overflow.dropped > 300);
  CHECK(!naive.state[0].matches_reference);
}
