// feed_overload: compare queue-full policies under bursty overload.
//   * the DETERMINISTIC MODEL (default): exact, reproducible numbers for every (policy, capacity) cell
//   * --real: also run the real two-thread pipeline for each cell and print its measurements next to the model's
// Exit codes: 0 ok, 1 I/O error, 2 usage error.
#include <algorithm>
#include <bit>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "feed/overload_model.hpp"
#include "feed/pipeline.hpp"
#include "feed/report_json.hpp"
#include "feed/workload.hpp"

namespace {

void usage(const char* argv0) {
  std::fprintf(stderr,
               "Usage: %s [options]\n"
               "Scenario (bursty traffic against a fixed-speed consumer):\n"
               "  --count N          events (default 20000)\n"
               "  --rate R           mean events/second (default 100000)\n"
               "  --burst-size B     events per burst (default 2000)\n"
               "  --burst-mult M     in-burst rate = M x mean rate (default 20)\n"
               "  --service-ns S     consumer time per event (default 4000, i.e. 250k events/s)\n"
               "Sweep:\n"
               "  --capacities LIST  comma-separated queue capacities, rounded up to powers of two (default 16,64,256,1024,2048,4096)\n"
               "  --policies LIST    comma-separated: drop,block,fail (default drop,block,fail)\n"
               "Output:\n"
               "  --format json|table|csv   (default json)\n"
               "  --real             also run the real pipeline per cell (needs >= 2 hardware threads to be comparable)\n"
               "  --timeline-out FILE --timeline-capacity C --timeline-policy P   model occupancy change-points for one cell, as CSV\n",
               argv0);
}

bool parse_u64(const char* s, std::uint64_t& out) {
  if (s == nullptr || *s == '\0' || *s == '-') return false;
  char* end = nullptr;
  out = std::strtoull(s, &end, 10);
  return *end == '\0';
}

std::vector<std::string> split(const std::string& s) {
  std::vector<std::string> out;
  std::stringstream ss(s);
  for (std::string item; std::getline(ss, item, ',');) {
    if (!item.empty()) out.push_back(item);
  }
  return out;
}

bool parse_policy(const std::string& s, feed::QueueFullPolicy& out) {
  if (s == "drop") out = feed::QueueFullPolicy::DropAndCount;
  else if (s == "block") out = feed::QueueFullPolicy::Block;
  else if (s == "fail") out = feed::QueueFullPolicy::FailFast;
  else return false;
  return true;
}

struct RealResult {
  bool ran = false;
  std::uint64_t dropped = 0, processed = 0, lag_p50 = 0, lag_p99 = 0, lag_max = 0, high_water = 0, stall_ns = 0, wall_ns = 0;
  std::uint64_t time_to_steady_ns = 0;
  bool steady_reached = false, balanced = false, aborted = false;
};

struct Row {
  feed::QueueFullPolicy policy;
  std::size_t capacity;
  feed::OverloadResult model;
  RealResult real;
};

double us(std::uint64_t ns) { return static_cast<double>(ns) / 1000.0; }

}  // namespace

int main(int argc, char** argv) {
  feed::WorkloadConfig wl;
  wl.profile = feed::Profile::Bursty;
  wl.count = 20'000;
  wl.rate_per_sec = 100'000;
  wl.burst_size = 2000;
  wl.burst_multiplier = 20;
  std::uint64_t service_ns = 4000;
  std::vector<std::size_t> capacities = {16, 64, 256, 1024, 2048, 4096};
  std::vector<feed::QueueFullPolicy> policies = {feed::QueueFullPolicy::DropAndCount, feed::QueueFullPolicy::Block, feed::QueueFullPolicy::FailFast};
  std::string format = "json", timeline_out;
  bool real = false;
  std::size_t timeline_capacity = 0;
  feed::QueueFullPolicy timeline_policy = feed::QueueFullPolicy::DropAndCount;

  for (int i = 1; i < argc; ++i) {
    const char* a = argv[i];
    if (std::strcmp(a, "--real") == 0) { real = true; continue; }
    const char* v = (i + 1 < argc) ? argv[i + 1] : nullptr;
    if (v == nullptr) { std::fprintf(stderr, "error: '%s' needs a value\n", a); usage(argv[0]); return 2; }
    ++i;
    std::uint64_t n = 0;
    bool ok = true;
    if (std::strcmp(a, "--count") == 0) { ok = parse_u64(v, n) && n >= 1; wl.count = n; }
    else if (std::strcmp(a, "--rate") == 0) { ok = parse_u64(v, n) && n >= 1; wl.rate_per_sec = n; }
    else if (std::strcmp(a, "--burst-size") == 0) { ok = parse_u64(v, n) && n >= 1 && n <= 4'000'000'000ULL; wl.burst_size = static_cast<std::uint32_t>(n); }
    else if (std::strcmp(a, "--burst-mult") == 0) { ok = parse_u64(v, n) && n >= 1 && n <= 4'000'000'000ULL; wl.burst_multiplier = static_cast<std::uint32_t>(n); }
    else if (std::strcmp(a, "--service-ns") == 0) { ok = parse_u64(v, n) && n >= 1; service_ns = n; }
    else if (std::strcmp(a, "--seed") == 0) { ok = parse_u64(v, n); wl.seed = n; }
    else if (std::strcmp(a, "--capacities") == 0) {
      capacities.clear();
      for (const std::string& t : split(v)) {
        if (!parse_u64(t.c_str(), n) || n < 1 || n > (1U << 30)) { ok = false; break; }
        capacities.push_back(std::bit_ceil(static_cast<std::size_t>(n)));
      }
      ok = ok && !capacities.empty();
    }
    else if (std::strcmp(a, "--policies") == 0) {
      policies.clear();
      for (const std::string& t : split(v)) {
        feed::QueueFullPolicy p;
        if (!parse_policy(t, p)) { ok = false; break; }
        policies.push_back(p);
      }
      ok = ok && !policies.empty();
    }
    else if (std::strcmp(a, "--format") == 0) { format = v; ok = format == "json" || format == "table" || format == "csv"; }
    else if (std::strcmp(a, "--timeline-out") == 0) timeline_out = v;
    else if (std::strcmp(a, "--timeline-capacity") == 0) { ok = parse_u64(v, n) && n >= 1; timeline_capacity = std::bit_ceil(static_cast<std::size_t>(n)); }
    else if (std::strcmp(a, "--timeline-policy") == 0) ok = parse_policy(v, timeline_policy);
    else ok = false;
    if (!ok) { std::fprintf(stderr, "error: bad argument '%s %s'\n", a, v); usage(argv[0]); return 2; }
  }

  std::vector<std::uint64_t> arrivals;
  arrivals.reserve(wl.count);
  for (std::uint64_t k = 0; k < wl.count; ++k) arrivals.push_back(feed::scheduled_offset_ns(wl, k));

  std::vector<feed::ReplayRecord> records;
  if (real) records = feed::generate_workload(wl).records;
  const unsigned hw = std::thread::hardware_concurrency();

  std::vector<Row> rows;
  for (const feed::QueueFullPolicy p : policies) {
    for (const std::size_t cap : capacities) {
      Row row{p, cap, {}, {}};
      feed::OverloadModelConfig mc;
      mc.capacity = cap;
      mc.service_ns = service_ns;
      mc.policy = p;
      row.model = feed::simulate_overload(arrivals, mc);

      if (real) {
        feed::PipelineConfig pc;
        pc.queue_capacity = cap;
        pc.full_policy = p;
        pc.consumer_work_ns = service_ns;
        pc.replay.mode = feed::ReplayMode::Timed;
        pc.measure_lag = true;
        pc.sample_interval_us = 100;
        const feed::PipelineReport r = feed::run_pipeline(records, pc);
        row.real.ran = true;
        row.real.dropped = r.overflow.dropped;
        row.real.processed = r.consumer.processed;
        row.real.lag_p50 = r.lag.percentile(50);
        row.real.lag_p99 = r.lag.percentile(99);
        row.real.lag_max = r.lag.max();
        row.real.high_water = r.queue.high_water;
        row.real.stall_ns = r.overflow.stall_ns;
        row.real.wall_ns = r.replay.wall_ns;
        row.real.time_to_steady_ns = r.steady.time_to_steady_ns;
        row.real.steady_reached = r.steady.steady_reached;
        row.real.balanced = r.balanced();
        row.real.aborted = r.aborted;
      }
      rows.push_back(std::move(row));
    }
  }

  if (!timeline_out.empty()) {
    const std::size_t cap = timeline_capacity != 0 ? timeline_capacity : capacities.front();
    feed::OverloadModelConfig mc;
    mc.capacity = cap;
    mc.service_ns = service_ns;
    mc.policy = timeline_policy;
    if (!feed::write_timeline_csv(timeline_out.c_str(), feed::simulate_overload(arrivals, mc).timeline)) {
      std::fprintf(stderr, "error: cannot write '%s'\n", timeline_out.c_str());
      return 1;
    }
  }

  const double mean_rate = static_cast<double>(wl.rate_per_sec);
  const double burst_rate = mean_rate * wl.burst_multiplier;
  const double service_rate = 1e9 / static_cast<double>(service_ns);
  const double mean_util = mean_rate / service_rate;
  const double burst_factor = burst_rate / service_rate;
  const double backlog_peak = burst_factor > 1.0 ? static_cast<double>(wl.burst_size) * (1.0 - 1.0 / burst_factor) : 0.0;
  const std::size_t slot_bytes = sizeof(feed::QueuedEvent);

  if (format == "csv") {
    std::printf("policy,capacity,memory_bytes,offered,enqueued,dropped,not_offered,loss_pct,processed,high_water,mean_occupancy,"
                "queue_wait_p50_ns,queue_wait_p99_ns,queue_wait_max_ns,source_delay_p99_ns,source_delay_max_ns,e2e_p99_ns,e2e_max_ns,"
                "producer_stall_ns,completion_ns,time_to_steady_ns,steady_reached\n");
    for (const Row& r : rows) {
      const auto& m = r.model;
      std::printf("%s,%zu,%zu,%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%.3f,%" PRIu64 ",%" PRIu64 ",%.2f,%" PRIu64 ",%" PRIu64 ",%" PRIu64
                  ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%s\n",
                  feed::to_string(r.policy), r.capacity, r.capacity * slot_bytes, m.offered, m.enqueued, m.dropped, m.not_offered,
                  100.0 * m.loss_fraction(), m.processed, m.high_water, m.mean_occupancy, m.queue_wait_ns.p50, m.queue_wait_ns.p99,
                  m.queue_wait_ns.max, m.source_delay_ns.p99, m.source_delay_ns.max, m.end_to_end_ns.p99, m.end_to_end_ns.max,
                  m.producer_stall_ns, m.completion_ns, m.steady.time_to_steady_ns, m.steady.steady_reached ? "true" : "false");
    }
    return 0;
  }

  if (format == "table") {
    std::printf("scenario: %" PRIu64 " events, mean %.0f/s in bursts of %u at %ux (%.0f/s); consumer %.0f/s (%" PRIu64 " ns/event)\n", wl.count,
                mean_rate, wl.burst_size, wl.burst_multiplier, burst_rate, service_rate, service_ns);
    std::printf("mean utilisation %.2f, burst overload %.1fx, expected backlog peak ~%.0f events, %zu bytes per queue slot\n\n", mean_util,
                burst_factor, backlog_peak, slot_bytes);
    std::printf("%-6s %6s %9s %7s %6s %11s %11s %12s %11s %11s\n", "policy", "cap", "mem(KiB)", "loss%", "hw", "qwait p99us", "qwait maxus",
                "src delay mus", "stall ms", "steady us");
    for (const Row& r : rows) {
      const auto& m = r.model;
      std::printf("%-6s %6zu %9.1f %7.2f %6" PRIu64 " %11.1f %11.1f %12.1f %11.2f %11.1f%s\n", feed::to_string(r.policy), r.capacity,
                  static_cast<double>(r.capacity * slot_bytes) / 1024.0, 100.0 * m.loss_fraction(), m.high_water, us(m.queue_wait_ns.p99),
                  us(m.queue_wait_ns.max), us(m.source_delay_ns.max), static_cast<double>(m.producer_stall_ns) / 1e6, us(m.steady.time_to_steady_ns),
                  m.steady.steady_reached ? "" : " (not reached)");
      if (r.real.ran) {
        std::printf("   real: loss %.2f%%, hw %" PRIu64 ", lag p99 <=%.1fus max %.1fus, stall %.2fms, steady %.1fus%s, %s\n",
                    100.0 * static_cast<double>(r.real.dropped) / static_cast<double>(std::max<std::uint64_t>(1, r.real.dropped + r.real.processed)),
                    r.real.high_water, us(r.real.lag_p99), us(r.real.lag_max), static_cast<double>(r.real.stall_ns) / 1e6, us(r.real.time_to_steady_ns),
                    r.real.steady_reached ? "" : " (not reached)", r.real.balanced ? "balanced" : "NOT BALANCED");
      }
    }
    if (real && hw < 2) std::printf("\nNOTE: %u hardware thread(s): producer and consumer time-slice on one core, so the real columns do not reflect a two-core machine.\n", hw);
    return 0;
  }

  std::printf("{\n");
  std::printf("  \"scenario\": {\"count\": %" PRIu64 ", \"mean_rate_per_sec\": %.0f, \"burst_size\": %u, \"burst_multiplier\": %u, "
              "\"burst_rate_per_sec\": %.0f, \"service_ns\": %" PRIu64 ", \"service_rate_per_sec\": %.0f, \"mean_utilisation\": %.3f, "
              "\"burst_overload_factor\": %.2f, \"expected_backlog_peak\": %.0f, \"queue_slot_bytes\": %zu},\n",
              wl.count, mean_rate, wl.burst_size, wl.burst_multiplier, burst_rate, service_ns, service_rate, mean_util, burst_factor, backlog_peak, slot_bytes);
  if (real) {
    std::printf("  \"real\": {\"hardware_threads\": %u, \"valid_for_comparison\": %s},\n", hw, hw >= 2 ? "true" : "false");
  }
  std::printf("  \"rows\": [");
  bool first = true;
  for (const Row& r : rows) {
    const auto& m = r.model;
    std::printf("%s\n    {\"policy\": \"%s\", \"capacity\": %zu, \"memory_bytes\": %zu,\n", first ? "" : ",", feed::to_string(r.policy), r.capacity,
                r.capacity * slot_bytes);
    std::printf("     \"model\": {\"offered\": %" PRIu64 ", \"enqueued\": %" PRIu64 ", \"dropped\": %" PRIu64 ", \"not_offered\": %" PRIu64
                ", \"loss_pct\": %.3f, \"processed\": %" PRIu64 ", \"high_water\": %" PRIu64 ", \"mean_occupancy\": %.2f,\n",
                m.offered, m.enqueued, m.dropped, m.not_offered, 100.0 * m.loss_fraction(), m.processed, m.high_water, m.mean_occupancy);
    std::printf("               \"queue_wait_ns\": {\"p50\": %" PRIu64 ", \"p99\": %" PRIu64 ", \"max\": %" PRIu64 "}, "
                "\"source_delay_ns\": {\"p99\": %" PRIu64 ", \"max\": %" PRIu64 "}, \"end_to_end_ns\": {\"p99\": %" PRIu64 ", \"max\": %" PRIu64 "},\n",
                m.queue_wait_ns.p50, m.queue_wait_ns.p99, m.queue_wait_ns.max, m.source_delay_ns.p99, m.source_delay_ns.max, m.end_to_end_ns.p99,
                m.end_to_end_ns.max);
    std::printf("               \"producer_stall_ns\": %" PRIu64 ", \"completion_ns\": %" PRIu64 ", \"congested\": %s, \"time_to_steady_ns\": %" PRIu64
                ", \"steady_reached\": %s}",
                m.producer_stall_ns, m.completion_ns, m.steady.congested ? "true" : "false", m.steady.time_to_steady_ns, m.steady.steady_reached ? "true" : "false");
    if (r.real.ran) {
      const auto& x = r.real;
      std::printf(",\n     \"real\": {\"dropped\": %" PRIu64 ", \"processed\": %" PRIu64 ", \"high_water\": %" PRIu64 ", \"lag_p50_ns\": %" PRIu64
                  ", \"lag_p99_ns\": %" PRIu64 ", \"lag_max_ns\": %" PRIu64 ", \"stall_ns\": %" PRIu64 ", \"replay_wall_ns\": %" PRIu64
                  ", \"time_to_steady_ns\": %" PRIu64 ", \"steady_reached\": %s, \"aborted\": %s, \"balanced\": %s}",
                  x.dropped, x.processed, x.high_water, x.lag_p50, x.lag_p99, x.lag_max, x.stall_ns, x.wall_ns, x.time_to_steady_ns,
                  x.steady_reached ? "true" : "false", x.aborted ? "true" : "false", x.balanced ? "true" : "false");
    }
    std::printf("}");
    first = false;
  }
  std::printf("\n  ]\n}\n");
  return 0;
}
