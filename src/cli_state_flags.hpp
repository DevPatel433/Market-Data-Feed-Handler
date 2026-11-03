#pragma once
// Command-line flags for downstream state, recovery and overload measurement, shared by
// feed_pipeline and feed_udp_recv so both tools accept exactly the same options.
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>

#include "feed/pipeline.hpp"

namespace feedcli {

enum class Flag { NotMine, Ok, Bad };

struct StateCli {
  std::string authoritative_path;
  std::string timeline_out;
};

inline const char* state_flags_usage() {
  return "Downstream state and recovery (docs/overload_and_recovery.md):\n"
         "  --state                      track a book per stream (strategy none unless --recovery is given)\n"
         "  --recovery S                 naive|none|retransmit|snapshot|hybrid  (enables --state)\n"
         "                                 naive: apply as received (control; silently diverges)\n"
         "                                 none: strict order, UNAVAILABLE on a gap that does not heal\n"
         "                                 retransmit / snapshot / hybrid: repair from a retained log and/or snapshots\n"
         "  --authoritative FILE         the complete clean replay file: source for recovery and reference for checking\n"
         "  --retention N                retransmit window in messages (default 1024)\n"
         "  --snapshot-interval K        publish a snapshot every K messages (default 500; 0 = never)\n"
         "  --max-snapshots M            snapshots kept (default 8)\n"
         "  --recovery-latency N         simulated round trip, in events received meanwhile (default 0)\n"
         "  --reorder-wait N             events to wait for late arrivals before requesting recovery (default 0)\n"
         "  --recovery-buffer N          out-of-order buffer bound (default 4096)\n"
         "Overload measurement:\n"
         "  --measure-lag                record offer->dequeue lag per event\n"
         "  --sample-interval-us N       sample queue occupancy every N us (enables time-to-steady-state)\n"
         "  --timeline-out FILE          write the occupancy samples as CSV\n"
         "(recovery works best with --gap-policy degrade --late-policy forward)\n";
}

inline bool parse_u64(const char* s, std::uint64_t& out) {
  if (s == nullptr || *s == '\0' || *s == '-') return false;
  char* end = nullptr;
  out = std::strtoull(s, &end, 10);
  return *end == '\0';
}

// Consumes argv[i] (and its value, advancing i) if it is one of the flags above.
inline Flag parse_state_flag(int argc, char** argv, int& i, feed::PipelineConfig& cfg, StateCli& cli) {
  const char* a = argv[i];
  const char* v = (i + 1 < argc) ? argv[i + 1] : nullptr;
  if (std::strcmp(a, "--state") == 0) {
    cfg.state.enabled = true;
    return Flag::Ok;
  }
  if (std::strcmp(a, "--measure-lag") == 0) {
    cfg.measure_lag = true;
    return Flag::Ok;
  }
  static const char* const valued[] = {"--recovery", "--authoritative", "--retention", "--snapshot-interval", "--max-snapshots",
                                       "--recovery-latency", "--reorder-wait", "--recovery-buffer", "--sample-interval-us", "--timeline-out"};
  bool mine = false;
  for (const char* f : valued) mine = mine || std::strcmp(a, f) == 0;
  if (!mine) return Flag::NotMine;
  if (v == nullptr) return Flag::Bad;
  ++i;

  std::uint64_t n = 0;
  if (std::strcmp(a, "--recovery") == 0) {
    if (!feed::parse_recovery_strategy(v, cfg.state.recovery.strategy)) return Flag::Bad;
    cfg.state.enabled = true;
  } else if (std::strcmp(a, "--authoritative") == 0) {
    cli.authoritative_path = v;
    cfg.state.enabled = true;
  } else if (std::strcmp(a, "--timeline-out") == 0) {
    cli.timeline_out = v;
    if (cfg.sample_interval_us == 0) cfg.sample_interval_us = 200;
  } else if (!parse_u64(v, n)) {
    return Flag::Bad;
  } else if (std::strcmp(a, "--retention") == 0) {
    cfg.state.recovery.source.retention = static_cast<std::size_t>(n);
  } else if (std::strcmp(a, "--snapshot-interval") == 0) {
    cfg.state.recovery.source.snapshot_interval = n;
  } else if (std::strcmp(a, "--max-snapshots") == 0) {
    cfg.state.recovery.source.max_snapshots = static_cast<std::size_t>(n);
  } else if (std::strcmp(a, "--recovery-latency") == 0) {
    cfg.state.recovery.latency_events = n;
  } else if (std::strcmp(a, "--reorder-wait") == 0) {
    cfg.state.recovery.reorder_wait_events = n;
  } else if (std::strcmp(a, "--recovery-buffer") == 0) {
    cfg.state.recovery.max_buffered = static_cast<std::size_t>(n);
  } else if (std::strcmp(a, "--sample-interval-us") == 0) {
    if (n == 0) return Flag::Bad;
    cfg.sample_interval_us = n;
  }
  return Flag::Ok;
}

// Loads the authoritative file (if any). Call after parsing, before running.
inline bool finalize_state(feed::PipelineConfig& cfg, const StateCli& cli, std::string& error) {
  if (!cli.authoritative_path.empty() &&
      !feed::AuthoritativeData::load_file(cli.authoritative_path, cfg.state.authoritative, error)) {
    return false;
  }
  return true;
}

}  // namespace feedcli
