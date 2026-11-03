#include "feed/pipeline.hpp"

#include "feed/consumer_loop.hpp"

#include <atomic>
#include <chrono>
#include <memory>
#include <thread>

namespace feed {

std::uint64_t event_check(const QueuedEvent& ev) {
  std::uint64_t z = ev.ticket * 0x9E3779B97F4A7C15ULL;
  z ^= (static_cast<std::uint64_t>(ev.msg.stream_id) << 32U) ^ ev.msg.seq ^
       (static_cast<std::uint64_t>(ev.msg.body.index()) << 56U) ^ static_cast<std::uint64_t>(ev.state);
  z ^= z >> 31U;
  z *= 0xD6E8FEB86659FD93ULL;
  return z ^ (z >> 32U);
}

void digest_event(Fnv& d, const QueuedEvent& ev) {
  d.u64(ev.ticket);
  d.u64(ev.msg.stream_id);
  d.u64(ev.msg.seq);
  d.byte(static_cast<std::uint8_t>(type_of(ev.msg.body)));
}

void ConsumerChecker::on_event(const QueuedEvent& ev) {
  if (ev.check != event_check(ev)) ++r_.corrupt;
  if (ev.ticket <= r_.last_ticket) {
    ++r_.out_of_order;
  } else {
    r_.skipped += ev.ticket - r_.last_ticket - 1;
    r_.last_ticket = ev.ticket;
  }
  digest_event(digest_, ev);
  ++r_.processed;
}

ConsumerReport ConsumerChecker::report() const {
  ConsumerReport out = r_;
  out.digest = digest_.h;
  return out;
}

namespace {

void spin_for(std::uint64_t ns) {
  if (ns == 0) return;
  const auto end = std::chrono::steady_clock::now() + std::chrono::nanoseconds(ns);
  while (std::chrono::steady_clock::now() < end) {
  }
}

}  // namespace

bool PipelineConfig::validate(std::string& error) const {
  if (queue_capacity == 0 || queue_capacity > SpscQueue<QueuedEvent>::kMaxCapacity) {
    error = "queue capacity out of range";
    return false;
  }
  if (state.enabled) {
    const RecoveryStrategy st = state.recovery.strategy;
    const bool needs_source =
        st == RecoveryStrategy::Retransmit || st == RecoveryStrategy::Snapshot || st == RecoveryStrategy::Hybrid;
    if (needs_source && !state.authoritative) {
      error = std::string("recovery strategy '") + to_string(st) + "' needs authoritative data (a complete clean replay file)";
      return false;
    }
    if (st != RecoveryStrategy::Naive && sequencer.gap_policy == GapPolicy::Pause) {
      error = "recovery needs gap policy 'degrade': the engine does its own gap handling, and 'pause' stops the feed it needs";
      return false;
    }
  }
  return true;
}

namespace {

std::uint64_t now_ns() {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count());
}

constexpr std::size_t kMaxTimelineSamples = 2'000'000;

}  // namespace

PipelineReport run_pipeline_source(const PipelineConfig& cfg, const FrameSource& source) {
  PipelineReport rep;
  if (!cfg.validate(rep.config_error)) return rep;

  SpscQueue<QueuedEvent> queue(cfg.queue_capacity);
  rep.queue_capacity = queue.capacity();
  rep.state_enabled = cfg.state.enabled;
  rep.state_strategy = cfg.state.recovery.strategy;
  rep.lag_measured = cfg.measure_lag;
  const std::uint64_t t0 = now_ns();

  std::atomic<bool> producer_done{false};
  std::atomic<bool> stop_replay{false};

  // ---- consumer thread: touches only the queue and its own locals ----
  ConsumerReport cons;
  LogHistogram lag;
  std::vector<StreamStateReport> state_reports;
  std::thread consumer([&] {
    ConsumerChecker checker;
    std::unique_ptr<StateTracker> tracker;
    if (cfg.state.enabled) {
      tracker = std::make_unique<StateTracker>(cfg.state.recovery, cfg.state.authoritative, cfg.sequencer.join_mid_stream);
    }
    const auto handle = [&](const QueuedEvent& ev) {
      checker.on_event(ev);
      if (cfg.measure_lag) {
        const std::uint64_t now = now_ns();
        lag.record(now > ev.t_offered_ns ? now - ev.t_offered_ns : 0);
      }
      if (tracker) tracker->on_message(ev.msg);
      spin_for(cfg.consumer_work_ns);
    };
    consume_until_done(queue, producer_done, handle);
    cons = checker.report();
    if (tracker) state_reports = tracker->report();
  });

  // ---- optional occupancy sampler ----
  std::vector<OccPoint> timeline;
  std::atomic<bool> sampler_stop{false};
  std::thread sampler;
  if (cfg.sample_interval_us > 0) {
    sampler = std::thread([&] {
      timeline.reserve(4096);
      while (!sampler_stop.load(std::memory_order_acquire) && timeline.size() < kMaxTimelineSamples) {
        timeline.push_back({now_ns() - t0, queue.size_approx()});
        std::this_thread::sleep_for(std::chrono::microseconds(cfg.sample_interval_us));
      }
    });
  }

  // ---- producer: this thread (runs the source) ----
  Fnv producer_digest;
  std::uint64_t ticket = 0;
  bool failed = false;
  Ingest ingest(cfg.sequencer, [&](const Message& m, StreamState st) {
    QueuedEvent ev;
    ev.msg = m;
    ev.state = st;
    ev.ticket = ++ticket;
    ev.check = event_check(ev);
    if (cfg.measure_lag) ev.t_offered_ns = now_ns();
    // A source may still deliver frames after a fail-fast overflow (e.g. the rest of a socket drain batch).
    // They are offered like any other event (enqueued if there is room, counted as dropped if not) and are
    // never silently skipped, so `forwarded == offered` and the books balance whatever the source does.
    switch (enqueue(queue, ev, cfg.full_policy, rep.overflow)) {
      case EnqueueResult::Enqueued:
        digest_event(producer_digest, ev);
        break;
      case EnqueueResult::Full:  // fail-fast: stop feeding and ask the replayer to stop
        failed = true;
        stop_replay.store(true, std::memory_order_relaxed);
        break;
      case EnqueueResult::Dropped:
      case EnqueueResult::Aborted:
        break;
    }
  });

  struct IngestSink final : FrameIntake {
    Ingest& in;
    explicit IngestSink(Ingest& i) : in(i) {}
    IngestResult on_frame(std::span<const std::uint8_t> frame) override { return in.on_frame(frame); }
    IngestResult on_invalid(DecodeError e) override { return in.on_invalid(e); }
  } sink(ingest);
  source(sink, stop_replay);
  rep.producer_end_ns = now_ns() - t0;

  producer_done.store(true, std::memory_order_release);
  consumer.join();
  if (sampler.joinable()) {
    sampler_stop.store(true, std::memory_order_release);
    sampler.join();
    timeline.push_back({now_ns() - t0, queue.size_approx()});  // final point: the queue has drained
  }

  rep.aborted = failed;
  rep.ingest = ingest.counters();
  rep.sequencing = ingest.sequencer().stats();
  rep.streams = ingest.sequencer().streams();
  rep.queue = queue.stats();
  rep.consumer = cons;
  rep.producer_digest = producer_digest.h;
  rep.lag = lag;
  rep.state = std::move(state_reports);
  if (!timeline.empty()) {
    rep.steady = analyze_occupancy(timeline, rep.queue_capacity, timeline.back().t_ns);
    rep.timeline = std::move(timeline);
  }
  return rep;
}

PipelineReport run_pipeline(const std::vector<ReplayRecord>& records, const PipelineConfig& cfg) {
  ReplayStats stats;
  PipelineReport rep = run_pipeline_source(cfg, [&](FrameIntake& sink, const std::atomic<bool>& stop) {
    ReplayOptions ropts = cfg.replay;
    ropts.stop = &stop;
    stats = replay(records, ropts, [&](std::span<const std::uint8_t> frame) { sink.on_frame(frame); });
  });
  rep.replay = stats;
  return rep;
}

}  // namespace feed
