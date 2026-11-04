#include <algorithm>
#include <atomic>
#include <vector>

#include "check.hpp"
#include "feed/fault_injector.hpp"
#include "feed/pipeline.hpp"
#include "feed/workload.hpp"

using namespace feed;

namespace {

std::vector<ReplayRecord> workload(Profile p, std::uint64_t count, std::uint64_t seed = 5) {
  WorkloadConfig c;
  c.profile = p;
  c.count = count;
  c.seed = seed;
  return generate_workload(c).records;
}

// Single-threaded reference: what the consumer SHOULD see when nothing is lost in the queue.
std::uint64_t reference_digest(const std::vector<ReplayRecord>& recs, const SequencerConfig& scfg, std::uint64_t& forwarded) {
  Fnv d;
  std::uint64_t ticket = 0;
  Ingest in(scfg, [&](const Message& m, StreamState st) {
    QueuedEvent ev;
    ev.msg = m;
    ev.state = st;
    ev.ticket = ++ticket;
    digest_event(d, ev);
  });
  for (const auto& r : recs) in.on_frame(r.frame);
  forwarded = ticket;
  return d.h;
}

void expect_balanced(const PipelineReport& r) {
  CHECK(r.frames_accounted());
  CHECK(r.queue_consistent());
  CHECK(r.order_and_integrity_ok());
  CHECK(r.balanced());
}

}  // namespace

TEST(pipeline_block_policy_delivers_everything_in_order) {
  const auto recs = workload(Profile::Normal, 30'000);
  PipelineConfig cfg;
  cfg.queue_capacity = 64;
  cfg.full_policy = QueueFullPolicy::Block;
  const PipelineReport r = run_pipeline(recs, cfg);
  expect_balanced(r);
  CHECK_EQ(r.consumer.processed, 30'000U);
  CHECK_EQ(r.overflow.dropped, 0U);
  CHECK_EQ(r.consumer.skipped, 0U);
  CHECK_EQ(r.consumer.last_ticket, 30'000U);
  CHECK(r.queue.high_water <= r.queue_capacity);
  CHECK(!r.aborted);
}

TEST(pipeline_block_policy_matches_the_single_threaded_reference_exactly) {
  const auto recs = workload(Profile::Malformed, 20'000);
  FaultConfig f;
  f.seed = 2;
  f.drop_ppm = f.duplicate_ppm = f.reorder_ppm = 10'000;
  FaultReport fr;
  const auto faulted = inject_faults(recs, f, fr);

  for (const std::size_t cap : {1U, 8U, 4096U}) {
    for (const LatePolicy lp : {LatePolicy::Discard, LatePolicy::Forward}) {
      PipelineConfig cfg;
      cfg.queue_capacity = cap;
      cfg.sequencer.late_policy = lp;
      std::uint64_t fwd = 0;
      const std::uint64_t expect = reference_digest(faulted, cfg.sequencer, fwd);
      const PipelineReport r = run_pipeline(faulted, cfg);
      expect_balanced(r);
      CHECK_EQ(r.consumer.processed, fwd);
      CHECK_EQ(r.consumer.digest, expect);  // same events, same order, regardless of timing or capacity
      CHECK_EQ(r.producer_digest, expect);
    }
  }
}

TEST(pipeline_is_deterministic_under_block_policy) {
  const auto recs = workload(Profile::Duplicate, 15'000);
  PipelineConfig cfg;
  cfg.queue_capacity = 16;
  cfg.consumer_work_ns = 200;
  const PipelineReport a = run_pipeline(recs, cfg);
  const PipelineReport b = run_pipeline(recs, cfg);
  CHECK_EQ(a.consumer.digest, b.consumer.digest);
  CHECK_EQ(a.consumer.processed, b.consumer.processed);
}

TEST(pipeline_drop_policy_quantifies_loss_under_a_slow_consumer) {
  const auto recs = workload(Profile::Normal, 20'000);
  PipelineConfig cfg;
  cfg.queue_capacity = 16;
  cfg.full_policy = QueueFullPolicy::DropAndCount;
  cfg.consumer_work_ns = 3'000;  // ~30x slower than the producer
  const PipelineReport r = run_pipeline(recs, cfg);
  expect_balanced(r);
  CHECK(r.overflow.dropped > 0);
  CHECK_EQ(r.overflow.offered, 20'000U);
  CHECK_EQ(r.consumer.processed + r.overflow.dropped, 20'000U);
  CHECK_EQ(r.consumer.out_of_order, 0U);  // survivors are an in-order subsequence
  CHECK_EQ(r.queue.high_water, r.queue_capacity);  // the queue really did fill
  CHECK(r.queue.full_rejects >= r.overflow.dropped);
  // Overflow is attributed to the queue, NOT to the network: the sequencer saw a perfect stream.
  CHECK_EQ(r.sequencing.missing_total, 0U);
  CHECK_EQ(r.ingest.rejected, 0U);
}

TEST(pipeline_drop_policy_with_ample_capacity_loses_nothing) {
  const auto recs = workload(Profile::Bursty, 20'000);
  PipelineConfig cfg;
  cfg.queue_capacity = 32'768;  // larger than the whole run: cannot overflow, whatever the scheduling
  cfg.full_policy = QueueFullPolicy::DropAndCount;
  const PipelineReport r = run_pipeline(recs, cfg);
  expect_balanced(r);
  CHECK_EQ(r.overflow.dropped, 0U);
  CHECK_EQ(r.consumer.processed, 20'000U);
}

TEST(pipeline_fail_fast_stops_at_the_first_overflow_and_still_balances) {
  const auto recs = workload(Profile::Normal, 20'000);
  PipelineConfig cfg;
  cfg.queue_capacity = 4;
  cfg.full_policy = QueueFullPolicy::FailFast;
  cfg.consumer_work_ns = 5'000;
  const PipelineReport r = run_pipeline(recs, cfg);
  expect_balanced(r);
  CHECK(r.aborted);
  CHECK(r.replay.stopped_early);
  CHECK_EQ(r.overflow.dropped, 1U);  // exactly the event that did not fit
  CHECK(r.ingest.rx_frames < 20'000U);
  CHECK_EQ(r.consumer.processed, r.overflow.enqueued);
}

TEST(pipeline_pause_gap_policy_flows_through_the_queue) {
  auto recs = workload(Profile::Normal, 5'000);
  recs.erase(recs.begin() + 99);  // drop seq 100
  PipelineConfig cfg;
  cfg.sequencer.gap_policy = GapPolicy::Pause;
  cfg.queue_capacity = 8;
  const PipelineReport r = run_pipeline(recs, cfg);
  expect_balanced(r);
  CHECK_EQ(r.consumer.processed, 99U);
  CHECK_EQ(r.ingest.paused_discarded, 4'900U);
}

TEST(pipeline_handles_empty_input_and_all_rejected_input) {
  PipelineConfig cfg;
  PipelineReport r = run_pipeline({}, cfg);
  expect_balanced(r);
  CHECK_EQ(r.consumer.processed, 0U);

  std::vector<ReplayRecord> junk(100);
  for (auto& rec : junk) rec.frame = {1, 2, 3};
  r = run_pipeline(junk, cfg);
  expect_balanced(r);
  CHECK_EQ(r.ingest.rejected, 100U);
  CHECK_EQ(r.consumer.processed, 0U);
}

TEST(pipeline_repeated_start_and_stop_is_clean) {
  const auto recs = workload(Profile::Normal, 300);
  for (int i = 0; i < 100; ++i) {  // thread creation/teardown races would show up as hangs or imbalance
    PipelineConfig cfg;
    cfg.queue_capacity = 2;
    expect_balanced(run_pipeline(recs, cfg));
  }
}

TEST(replayer_stop_flag_ends_replay_early) {
  const auto recs = workload(Profile::Normal, 100);
  std::atomic<bool> stop{true};
  ReplayOptions opts;
  opts.stop = &stop;
  const auto st = replay(recs, opts, [](std::span<const std::uint8_t>) {});
  CHECK_EQ(st.frames, 0U);
  CHECK(st.stopped_early);

  stop.store(false);
  std::uint64_t seen = 0;
  const auto st2 = replay(recs, opts, [&](std::span<const std::uint8_t>) {
    if (++seen == 10) stop.store(true);
  });
  CHECK_EQ(st2.frames, 10U);
  CHECK(st2.stopped_early);
}

// ---------------------------------------------------------------------------
// The consumer's detectors, exercised with crafted bad input (real runs never produce any).
// ---------------------------------------------------------------------------
namespace {
QueuedEvent make_ev(std::uint64_t ticket, std::uint64_t seq = 1) {
  QueuedEvent ev;
  ev.msg = Message{1, seq, Heartbeat{}};
  ev.ticket = ticket;
  ev.check = event_check(ev);
  return ev;
}
}  // namespace

TEST(consumer_checker_accepts_a_clean_in_order_stream) {
  ConsumerChecker c;
  for (std::uint64_t t = 1; t <= 100; ++t) c.on_event(make_ev(t, t));
  const ConsumerReport r = c.report();
  CHECK_EQ(r.processed, 100U);
  CHECK_EQ(r.out_of_order + r.skipped + r.corrupt, 0U);
  CHECK_EQ(r.last_ticket, 100U);
}

TEST(consumer_checker_counts_skipped_tickets) {
  ConsumerChecker c;
  c.on_event(make_ev(1));
  c.on_event(make_ev(5));  // 2,3,4 never arrived
  c.on_event(make_ev(6));
  c.on_event(make_ev(10));  // 7,8,9 never arrived
  const ConsumerReport r = c.report();
  CHECK_EQ(r.skipped, 6U);
  CHECK_EQ(r.out_of_order, 0U);
  CHECK_EQ(r.last_ticket, 10U);
}

TEST(consumer_checker_flags_duplicates_and_reordering) {
  ConsumerChecker c;
  c.on_event(make_ev(1));
  c.on_event(make_ev(2));
  c.on_event(make_ev(2));  // duplicate delivery
  c.on_event(make_ev(4));
  c.on_event(make_ev(3));  // reordered
  const ConsumerReport r = c.report();
  CHECK_EQ(r.out_of_order, 2U);
  CHECK_EQ(r.skipped, 1U);  // ticket 3 was skipped when 4 arrived
}

TEST(consumer_checker_flags_corrupted_payloads) {
  ConsumerChecker c;
  QueuedEvent a = make_ev(1, 7);
  a.msg.seq = 8;  // field changed after the check was computed: a torn/stale read looks like this
  QueuedEvent b = make_ev(2, 7);
  b.state = StreamState::Degraded;
  QueuedEvent d = make_ev(3, 7);
  d.msg.body = CancelOrder{1, 1};
  c.on_event(a);
  c.on_event(b);
  c.on_event(d);
  c.on_event(make_ev(4, 7));
  CHECK_EQ(c.report().corrupt, 3U);
}

TEST(consumer_checker_digest_depends_on_order_and_content) {
  ConsumerChecker x, y, z;
  x.on_event(make_ev(1, 10));
  x.on_event(make_ev(2, 11));
  y.on_event(make_ev(1, 10));
  y.on_event(make_ev(2, 11));
  z.on_event(make_ev(1, 11));  // different content
  z.on_event(make_ev(2, 10));
  CHECK_EQ(x.report().digest, y.report().digest);
  CHECK(x.report().digest != z.report().digest);
}

TEST(pipeline_report_balance_checks_each_catch_their_own_kind_of_error) {
  const auto recs = workload(Profile::Normal, 1000);
  PipelineConfig cfg;
  const PipelineReport good = run_pipeline(recs, cfg);
  CHECK(good.balanced());

  PipelineReport r = good;
  r.consumer.processed += 1;  // consumer claims one extra event
  CHECK(!r.queue_consistent());
  CHECK(!r.frames_accounted());
  r = good;
  r.overflow.dropped += 1;  // an overflow nobody saw on the consumer side
  CHECK(!r.queue_consistent());
  r = good;
  r.consumer.digest ^= 1;  // different content delivered
  CHECK(!r.queue_consistent());
  r = good;
  r.consumer.out_of_order = 1;
  CHECK(!r.order_and_integrity_ok());
  CHECK(!r.balanced());
  r = good;
  r.ingest.rx_frames += 1;  // a received frame in no bucket
  CHECK(!r.frames_accounted());
}
