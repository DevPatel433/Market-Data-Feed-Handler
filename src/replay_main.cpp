// feed_replay: replay a file through decode -> sequence -> (counting) consumer
// and print a JSON summary. Optionally writes a structured event log (JSON Lines)
// of every rejection, gap, duplicate, late arrival and paused discard.
// Exit codes: 0 ok, 1 input error, 2 usage error.
#include <array>
#include <cinttypes>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <string>

#include "feed/event_log.hpp"
#include "feed/fnv.hpp"
#include "feed/ingest.hpp"
#include "feed/replay_file.hpp"
#include "feed/replayer.hpp"

namespace {

void usage(const char* argv0) {
  std::fprintf(stderr,
               "Usage: %s --input FILE [options]\n"
               "Replay:\n"
               "  --mode max|timed|rate   max: as fast as possible (default, deterministic)\n"
               "                          timed: honour recorded offsets, scaled by --speed\n"
               "                          rate: fixed --rate frames/s, ignoring recorded offsets\n"
               "  --speed X               timed mode multiplier (default 1)\n"
               "  --rate N                rate mode frames per second (default 100000)\n"
               "Sequencing policy:\n"
               "  --gap-policy degrade|pause     on a gap: keep forwarding but mark degraded (default),\n"
               "                                 or stop forwarding until resync\n"
               "  --late-policy discard|forward  late arrival that fills a gap (default discard)\n"
               "  --join-mid-stream              first message seen defines the stream start\n"
               "  --max-streams N                (default 1024)\n"
               "  --max-missing-ranges N         per stream (default 4096)\n"
               "Output:\n"
               "  --event-log FILE        write one JSON line per anomaly\n",
               argv0);
}

bool parse_u64(const char* s, std::uint64_t& out) {
  if (s == nullptr || *s == '\0' || *s == '-') return false;
  char* end = nullptr;
  out = std::strtoull(s, &end, 10);
  return *end == '\0';
}



}  // namespace

int main(int argc, char** argv) {
  std::string input, event_log_path;
  feed::ReplayOptions opts;
  feed::SequencerConfig scfg;

  for (int i = 1; i < argc; ++i) {
    const char* a = argv[i];
    const char* v = (i + 1 < argc) ? argv[i + 1] : nullptr;
    std::uint64_t n = 0;
    bool ok = true;
    if (std::strcmp(a, "--input") == 0 && v) { input = v; ++i; }
    else if (std::strcmp(a, "--event-log") == 0 && v) { event_log_path = v; ++i; }
    else if (std::strcmp(a, "--join-mid-stream") == 0) { scfg.join_mid_stream = true; }
    else if (std::strcmp(a, "--mode") == 0 && v) {
      if (std::strcmp(v, "max") == 0) opts.mode = feed::ReplayMode::MaxSpeed;
      else if (std::strcmp(v, "timed") == 0) opts.mode = feed::ReplayMode::Timed;
      else if (std::strcmp(v, "rate") == 0) opts.mode = feed::ReplayMode::Rate;
      else ok = false;
      ++i;
    } else if (std::strcmp(a, "--speed") == 0 && v) {
      char* end = nullptr;
      opts.speed = std::strtod(v, &end);
      ok = end != v && *end == '\0' && opts.speed > 0.0;
      ++i;
    } else if (std::strcmp(a, "--rate") == 0 && v) {
      ok = parse_u64(v, n) && n >= 1;
      opts.rate_per_sec = n;
      ++i;
    } else if (std::strcmp(a, "--gap-policy") == 0 && v) {
      if (std::strcmp(v, "degrade") == 0) scfg.gap_policy = feed::GapPolicy::MarkDegraded;
      else if (std::strcmp(v, "pause") == 0) scfg.gap_policy = feed::GapPolicy::Pause;
      else ok = false;
      ++i;
    } else if (std::strcmp(a, "--late-policy") == 0 && v) {
      if (std::strcmp(v, "discard") == 0) scfg.late_policy = feed::LatePolicy::Discard;
      else if (std::strcmp(v, "forward") == 0) scfg.late_policy = feed::LatePolicy::Forward;
      else ok = false;
      ++i;
    } else if (std::strcmp(a, "--max-streams") == 0 && v) {
      ok = parse_u64(v, n);
      scfg.max_streams = static_cast<std::size_t>(n);
      ++i;
    } else if (std::strcmp(a, "--max-missing-ranges") == 0 && v) {
      ok = parse_u64(v, n);
      scfg.max_missing_ranges = static_cast<std::size_t>(n);
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

  std::FILE* log = nullptr;
  if (!event_log_path.empty()) {
    log = std::fopen(event_log_path.c_str(), "w");
    if (log == nullptr) {
      std::fprintf(stderr, "error: cannot write '%s'\n", event_log_path.c_str());
      return 1;
    }
  }

  // Counting consumer: tallies forwarded events and digests (stream, seq, type) in delivery
  // order, so two runs can be compared for identical downstream behaviour.
  std::array<std::uint64_t, 5> by_type{};
  std::uint64_t forwarded_in_degraded = 0;
  feed::Fnv consumer_digest;
  feed::Ingest ingest(scfg, [&](const feed::Message& m, feed::StreamState st) {
    ++by_type[static_cast<std::size_t>(feed::type_of(m.body))];
    if (st != feed::StreamState::Synced) ++forwarded_in_degraded;
    consumer_digest.u64(m.stream_id);
    consumer_digest.u64(m.seq);
    consumer_digest.byte(static_cast<std::uint8_t>(feed::type_of(m.body)));
  });

  feed::Fnv input_digest;
  std::uint64_t arrival = 0;
  std::uint64_t seq_min = std::numeric_limits<std::uint64_t>::max(), seq_max = 0;
  const feed::ReplayStats st = feed::replay(records, opts, [&](std::span<const std::uint8_t> frame) {
    input_digest.byte(static_cast<std::uint8_t>(frame.size() >> 8U));
    input_digest.byte(static_cast<std::uint8_t>(frame.size() & 0xFFU));
    for (const std::uint8_t b : frame) input_digest.byte(b);

    ++arrival;
    const feed::IngestResult r = ingest.on_frame(frame);
    if (r.error == feed::DecodeError::None) {
      if (r.seq < seq_min) seq_min = r.seq;
      if (r.seq > seq_max) seq_max = r.seq;
    }
    if (log != nullptr) feed::log_event(log, arrival, r);
  });
  if (log != nullptr && std::fclose(log) != 0) {
    std::fprintf(stderr, "error: failed writing '%s'\n", event_log_path.c_str());
    return 1;
  }

  const feed::IngestCounters& c = ingest.counters();
  const feed::SequencerStats s = ingest.sequencer().stats();
  const double secs = static_cast<double>(st.wall_ns) / 1e9;
  const double rate = secs > 0.0 ? static_cast<double>(st.frames) / secs : 0.0;
  const char* mode = opts.mode == feed::ReplayMode::Timed ? "timed" : opts.mode == feed::ReplayMode::Rate ? "rate" : "max";

  std::printf("{\n");
  std::printf("  \"input\": \"%s\",\n  \"mode\": \"%s\",\n", input.c_str(), mode);
  std::printf("  \"policy\": {\"gap\": \"%s\", \"late\": \"%s\", \"join_mid_stream\": %s},\n", feed::to_string(scfg.gap_policy),
              feed::to_string(scfg.late_policy), scfg.join_mid_stream ? "true" : "false");
  std::printf("  \"frames\": %" PRIu64 ",\n  \"bytes\": %" PRIu64 ",\n", st.frames, st.bytes);
  std::printf("  \"digest\": \"0x%016" PRIx64 "\",\n", input_digest.h);

  std::printf("  \"decode\": {\"decoded\": %" PRIu64 ", \"rejected_total\": %" PRIu64 ", \"rejected\": {", c.decoded, c.rejected);
  for (std::size_t i = 1; i < feed::kDecodeErrorCount; ++i) {  // index 0 is "none"
    std::printf("%s\"%s\": %" PRIu64, i > 1 ? ", " : "", feed::to_string(static_cast<feed::DecodeError>(i)), c.rejected_by[i]);
  }
  std::printf("}},\n");
  // Kept at top level as well for scripts written against Phase 1 output.
  std::printf("  \"decoded\": %" PRIu64 ",\n  \"rejected_total\": %" PRIu64 ",\n", c.decoded, c.rejected);

  std::printf("  \"sequencing\": {\"observed\": %" PRIu64 ", \"in_order\": %" PRIu64 ", \"gap_events\": %" PRIu64
              ", \"missing_total\": %" PRIu64 ", \"duplicates\": %" PRIu64 ", \"out_of_order\": %" PRIu64
              ", \"outstanding_missing\": %" PRIu64 ", \"seqs_evicted\": %" PRIu64 ", \"stream_limit\": %" PRIu64 "},\n",
              s.observed(), s.in_order, s.gap_events, s.missing_total, s.duplicates, s.out_of_order,
              ingest.sequencer().outstanding_seqs(), s.seqs_evicted, s.stream_limit);

  std::printf("  \"disposition\": {\"forwarded\": %" PRIu64 ", \"duplicate_discarded\": %" PRIu64 ", \"late_discarded\": %" PRIu64
              ", \"paused_discarded\": %" PRIu64 ", \"stream_limit_discarded\": %" PRIu64 ", \"balanced\": %s},\n",
              c.forwarded, c.duplicate_discarded, c.late_discarded, c.paused_discarded, c.stream_limit_discarded,
              c.balanced() ? "true" : "false");

  std::printf("  \"streams\": [");
  bool first = true;
  for (const feed::StreamInfo& si : ingest.sequencer().streams()) {
    std::printf("%s\n    {\"id\": %" PRIu32 ", \"state\": \"%s\", \"next_expected\": %" PRIu64 ", \"outstanding_missing\": %" PRIu64
                ", \"missing_ranges\": %" PRIu64 "}",
                first ? "" : ",", si.stream_id, feed::to_string(si.state), si.next_expected, si.outstanding_seqs, si.missing_ranges);
    first = false;
  }
  std::printf("%s],\n", first ? "" : "\n  ");

  std::printf("  \"consumer\": {\"by_type\": {\"add\": %" PRIu64 ", \"cancel\": %" PRIu64 ", \"trade\": %" PRIu64
              ", \"heartbeat\": %" PRIu64 "}, \"forwarded_while_not_synced\": %" PRIu64 ", \"digest\": \"0x%016" PRIx64 "\"},\n",
              by_type[1], by_type[2], by_type[3], by_type[4], forwarded_in_degraded, consumer_digest.h);
  if (c.decoded > 0) {
    std::printf("  \"seq_min\": %" PRIu64 ",\n  \"seq_max\": %" PRIu64 ",\n", seq_min, seq_max);
  } else {
    std::printf("  \"seq_min\": null,\n  \"seq_max\": null,\n");
  }
  std::printf("  \"wall_ns\": %" PRIu64 ",\n  \"max_lateness_ns\": %" PRIu64 ",\n  \"achieved_msgs_per_sec\": %.0f\n}\n", st.wall_ns,
              st.max_lateness_ns, rate);
  return 0;
}
