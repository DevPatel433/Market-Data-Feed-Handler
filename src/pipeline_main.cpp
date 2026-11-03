// feed_pipeline: replay a file through decode -> sequence -> bounded SPSC queue -> verifying consumer.
// Exit codes: 0 ok, 1 input error, 2 usage error,
//             3 stopped early by --full-policy fail (queue overflow),
//             4 internal consistency failure (accounting, ordering or integrity): always a bug.
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "feed/pipeline.hpp"
#include "cli_state_flags.hpp"
#include "feed/report_json.hpp"

namespace {

void usage(const char* argv0) {
  std::fprintf(stderr,
               "Usage: %s --input FILE [options]\n"
               "Replay:   --mode max|timed|rate  --speed X  --rate N      (as in feed_replay)\n"
               "Sequencing: --gap-policy degrade|pause  --late-policy discard|forward  --join-mid-stream\n"
               "Queue:\n"
               "  --queue-capacity N          rounded up to a power of two (default 1024)\n"
               "  --full-policy block|drop|fail\n"
               "        block: wait for space (backpressure; replay can be paused)   [default]\n"
               "        drop:  discard the new event and count it as queue_overflow\n"
               "        fail:  stop at the first overflow and exit with status 3\n"
               "  --consumer-work-ns N        busy-wait per event to simulate a slow consumer (default 0)\n"
               "%s",
               argv0, feedcli::state_flags_usage());
}

bool parse_u64(const char* s, std::uint64_t& out) {
  if (s == nullptr || *s == '\0' || *s == '-') return false;
  char* end = nullptr;
  out = std::strtoull(s, &end, 10);
  return *end == '\0';
}

}  // namespace

int main(int argc, char** argv) {
  std::string input;
  feed::PipelineConfig cfg;
  feedcli::StateCli scli;

  for (int i = 1; i < argc; ++i) {
    const char* a = argv[i];
    const char* v = (i + 1 < argc) ? argv[i + 1] : nullptr;
    std::uint64_t n = 0;
    bool ok = true;
    if (const auto fr = feedcli::parse_state_flag(argc, argv, i, cfg, scli); fr != feedcli::Flag::NotMine) {
      if (fr == feedcli::Flag::Bad) {
        std::fprintf(stderr, "error: bad or incomplete argument '%s'\n", a);
        usage(argv[0]);
        return 2;
      }
      continue;
    }
    if (std::strcmp(a, "--input") == 0 && v) { input = v; ++i; }
    else if (std::strcmp(a, "--join-mid-stream") == 0) { cfg.sequencer.join_mid_stream = true; }
    else if (std::strcmp(a, "--mode") == 0 && v) {
      if (std::strcmp(v, "max") == 0) cfg.replay.mode = feed::ReplayMode::MaxSpeed;
      else if (std::strcmp(v, "timed") == 0) cfg.replay.mode = feed::ReplayMode::Timed;
      else if (std::strcmp(v, "rate") == 0) cfg.replay.mode = feed::ReplayMode::Rate;
      else ok = false;
      ++i;
    } else if (std::strcmp(a, "--speed") == 0 && v) {
      char* end = nullptr;
      cfg.replay.speed = std::strtod(v, &end);
      ok = end != v && *end == '\0' && cfg.replay.speed > 0.0;
      ++i;
    } else if (std::strcmp(a, "--rate") == 0 && v) {
      ok = parse_u64(v, n) && n >= 1;
      cfg.replay.rate_per_sec = n;
      ++i;
    } else if (std::strcmp(a, "--gap-policy") == 0 && v) {
      if (std::strcmp(v, "degrade") == 0) cfg.sequencer.gap_policy = feed::GapPolicy::MarkDegraded;
      else if (std::strcmp(v, "pause") == 0) cfg.sequencer.gap_policy = feed::GapPolicy::Pause;
      else ok = false;
      ++i;
    } else if (std::strcmp(a, "--late-policy") == 0 && v) {
      if (std::strcmp(v, "discard") == 0) cfg.sequencer.late_policy = feed::LatePolicy::Discard;
      else if (std::strcmp(v, "forward") == 0) cfg.sequencer.late_policy = feed::LatePolicy::Forward;
      else ok = false;
      ++i;
    } else if (std::strcmp(a, "--queue-capacity") == 0 && v) {
      ok = parse_u64(v, n) && n >= 1 && n <= feed::SpscQueue<feed::QueuedEvent>::kMaxCapacity;
      cfg.queue_capacity = static_cast<std::size_t>(n);
      ++i;
    } else if (std::strcmp(a, "--full-policy") == 0 && v) {
      if (std::strcmp(v, "block") == 0) cfg.full_policy = feed::QueueFullPolicy::Block;
      else if (std::strcmp(v, "drop") == 0) cfg.full_policy = feed::QueueFullPolicy::DropAndCount;
      else if (std::strcmp(v, "fail") == 0) cfg.full_policy = feed::QueueFullPolicy::FailFast;
      else ok = false;
      ++i;
    } else if (std::strcmp(a, "--consumer-work-ns") == 0 && v) {
      ok = parse_u64(v, n);
      cfg.consumer_work_ns = n;
      ++i;
    } else {
      ok = false;
    }
    if (!ok) {
      std::fprintf(stderr, "error: bad or incomplete argument '%s'\n", a);
      usage(argv[0]);
      return 2;
    }
  }
  if (input.empty()) { usage(argv[0]); return 2; }

  std::vector<std::uint8_t> image;
  if (!feed::read_file(input, image)) {
    std::fprintf(stderr, "error: cannot read '%s'\n", input.c_str());
    return 1;
  }
  std::vector<feed::ReplayRecord> records;
  if (const auto err = feed::parse_replay(image, records); err != feed::ReplayError::None) {
    std::fprintf(stderr, "error: invalid replay file '%s': %s\n", input.c_str(), feed::to_string(err));
    return 1;
  }

  std::string state_error;
  if (!feedcli::finalize_state(cfg, scli, state_error)) {
    std::fprintf(stderr, "error: %s\n", state_error.c_str());
    return 1;
  }
  if (std::string verr; !cfg.validate(verr)) {
    std::fprintf(stderr, "error: %s\n", verr.c_str());
    return 2;
  }

  const feed::PipelineReport r = feed::run_pipeline(records, cfg);
  if (!scli.timeline_out.empty() && !feed::write_timeline_csv(scli.timeline_out.c_str(), r.timeline)) {
    std::fprintf(stderr, "error: cannot write '%s'\n", scli.timeline_out.c_str());
    return 1;
  }

  std::printf("{\n");
  std::printf("  \"input\": \"%s\",\n", input.c_str());
  std::printf("  \"policy\": {\"gap\": \"%s\", \"late\": \"%s\", \"queue_full\": \"%s\", \"consumer_work_ns\": %" PRIu64 "},\n",
              feed::to_string(cfg.sequencer.gap_policy), feed::to_string(cfg.sequencer.late_policy),
              feed::to_string(cfg.full_policy), cfg.consumer_work_ns);
  std::printf("  \"frames\": %" PRIu64 ",\n  \"replay_stopped_early\": %s,\n", r.replay.frames, r.replay.stopped_early ? "true" : "false");
  feed::print_pipeline_sections(stdout, r);
  std::printf("  \"wall_ns\": %" PRIu64 "\n}\n", r.replay.wall_ns);

  if (!r.balanced()) return 4;
  if (r.aborted) return 3;
  return 0;
}
