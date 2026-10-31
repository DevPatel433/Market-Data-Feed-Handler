#pragma once
// JSON sections shared by feed_pipeline and feed_udp_recv, so both report the same keys.
#include <cinttypes>
#include <cstdio>

#include "feed/pipeline.hpp"

namespace feed {

// Prints "ingest", "sequencing", "queue", "overflow", "consumer" and "checks" members, each followed by ",\n".
inline void print_pipeline_sections(std::FILE* out, const PipelineReport& r) {
  const auto& c = r.ingest;
  const auto& o = r.overflow;
  const auto& q = r.queue;
  const auto& k = r.consumer;
  std::fprintf(out, "  \"ingest\": {\"rx_frames\": %" PRIu64 ", \"rejected\": %" PRIu64 ", \"decoded\": %" PRIu64 ", \"forwarded_to_queue\": %" PRIu64
                    ", \"duplicate_discarded\": %" PRIu64 ", \"late_discarded\": %" PRIu64 ", \"paused_discarded\": %" PRIu64
                    ", \"stream_limit_discarded\": %" PRIu64 "},\n",
               c.rx_frames, c.rejected, c.decoded, c.forwarded, c.duplicate_discarded, c.late_discarded, c.paused_discarded,
               c.stream_limit_discarded);
  std::fprintf(out, "  \"rejected_by_reason\": {");
  for (std::size_t i = 1; i < kDecodeErrorCount; ++i) {  // index 0 is "none"
    std::fprintf(out, "%s\"%s\": %" PRIu64, i > 1 ? ", " : "", to_string(static_cast<DecodeError>(i)), c.rejected_by[i]);
  }
  std::fprintf(out, "},\n");
  std::fprintf(out, "  \"sequencing\": {\"gap_events\": %" PRIu64 ", \"missing_total\": %" PRIu64 ", \"duplicates\": %" PRIu64
                    ", \"out_of_order\": %" PRIu64 "},\n",
               r.sequencing.gap_events, r.sequencing.missing_total, r.sequencing.duplicates, r.sequencing.out_of_order);
  std::fprintf(out, "  \"queue\": {\"capacity\": %zu, \"pushed\": %" PRIu64 ", \"popped\": %" PRIu64 ", \"high_water\": %" PRIu64
                    ", \"full_rejects\": %" PRIu64 ", \"empty_polls\": %" PRIu64 "},\n",
               r.queue_capacity, q.pushed, q.popped, q.high_water, q.full_rejects, q.empty_polls);
  std::fprintf(out, "  \"overflow\": {\"offered\": %" PRIu64 ", \"enqueued\": %" PRIu64 ", \"queue_overflow\": %" PRIu64
                    ", \"aborted\": %" PRIu64 ", \"stalled_events\": %" PRIu64 ", \"stall_spins\": %" PRIu64 ", \"stall_ns\": %" PRIu64 "},\n",
               o.offered, o.enqueued, o.dropped, o.aborted, o.stalled_events, o.stall_spins, o.stall_ns);
  std::fprintf(out, "  \"consumer\": {\"processed\": %" PRIu64 ", \"ticket_out_of_order\": %" PRIu64 ", \"skipped_tickets\": %" PRIu64
                    ", \"corrupt\": %" PRIu64 ", \"digest\": \"0x%016" PRIx64 "\"},\n",
               k.processed, k.out_of_order, k.skipped, k.corrupt, k.digest);
  if (r.lag_measured) {
    // Histogram percentiles are upper bounds, within 12.5% of the exact value (lag_histogram.hpp).
    std::fprintf(out, "  \"lag_ns\": {\"count\": %" PRIu64 ", \"mean\": %.0f, \"p50\": %" PRIu64 ", \"p90\": %" PRIu64 ", \"p99\": %" PRIu64
                      ", \"p999\": %" PRIu64 ", \"max\": %" PRIu64 "},\n",
                 r.lag.count(), r.lag.mean(), r.lag.percentile(50), r.lag.percentile(90), r.lag.percentile(99), r.lag.percentile(99.9), r.lag.max());
  }
  if (!r.timeline.empty()) {
    std::fprintf(out, "  \"steady_state\": {\"samples\": %zu, \"peak_occupancy\": %" PRIu64 ", \"congested\": %s, \"steady_reached\": %s"
                      ", \"time_to_steady_ns\": %" PRIu64 ", \"producer_end_ns\": %" PRIu64 "},\n",
                 r.timeline.size(), r.steady.peak, r.steady.congested ? "true" : "false", r.steady.steady_reached ? "true" : "false",
                 r.steady.time_to_steady_ns, r.producer_end_ns);
  }
  if (r.state_enabled) {
    std::fprintf(out, "  \"state\": {\"strategy\": \"%s\", \"consistent\": %s, \"streams\": [", to_string(r.state_strategy),
                 r.state_consistent() ? "true" : "false");
    bool first = true;
    for (const StreamStateReport& s : r.state) {
      const RecoveryCounters& rc = s.counters;
      std::fprintf(out, "%s\n    {\"id\": %" PRIu32 ", \"sync_state\": \"%s\", \"next_apply\": %" PRIu64 ", \"max_seen\": %" PRIu64
                        ", \"buffered\": %" PRIu64 ", \"live_orders\": %" PRIu64 ", \"book_digest\": \"0x%016" PRIx64
                        "\", \"reference_known\": %s, \"reference_digest\": \"0x%016" PRIx64 "\", \"matches_reference\": %s"
                        ", \"unknown_cancels\": %" PRIu64 ", \"duplicate_adds\": %" PRIu64 ", \"gaps_opened\": %" PRIu64
                        ", \"self_healed\": %" PRIu64 ", \"recoveries_retransmit\": %" PRIu64 ", \"recoveries_snapshot\": %" PRIu64
                        ", \"recovered_applied\": %" PRIu64 ", \"snapshots_loaded\": %" PRIu64 ", \"covered_discarded\": %" PRIu64
                        ", \"overflow_dropped\": %" PRIu64 ", \"unrecoverable\": %" PRIu64 ", \"retry_waits\": %" PRIu64
                        ", \"episodes\": %" PRIu64 ", \"episode_events_max\": %" PRIu64 ", \"episode_ns_max\": %" PRIu64
                        ", \"events_recovering\": %" PRIu64 ", \"events_unavailable\": %" PRIu64 "}",
                   first ? "" : ",", s.stream_id, to_string(s.state), s.next_apply, s.max_seen, s.buffered, s.live_orders, s.book_digest,
                   s.reference_known ? "true" : "false", s.reference_digest, s.matches_reference ? "true" : "false", s.unknown_cancels,
                   s.duplicate_adds, rc.gaps_opened, rc.self_healed, rc.recoveries_retransmit, rc.recoveries_snapshot, rc.recovered_applied,
                   rc.snapshots_loaded, rc.covered_discarded, rc.overflow_dropped, rc.unrecoverable, rc.retry_waits, rc.episodes,
                   rc.episode_events_max, rc.episode_ns_max, rc.events_recovering, rc.events_unavailable);
      first = false;
    }
    std::fprintf(out, "%s]},\n", first ? "" : "\n  ");
  }
  std::fprintf(out, "  \"checks\": {\"frames_accounted\": %s, \"queue_consistent\": %s, \"order_and_integrity_ok\": %s, \"balanced\": %s},\n",
               r.frames_accounted() ? "true" : "false", r.queue_consistent() ? "true" : "false",
               r.order_and_integrity_ok() ? "true" : "false", r.balanced() ? "true" : "false");
}

inline bool write_timeline_csv(const char* path, const std::vector<OccPoint>& timeline) {
  std::FILE* f = std::fopen(path, "w");
  if (f == nullptr) return false;
  std::fprintf(f, "t_ns,occupancy\n");
  for (const OccPoint& p : timeline) std::fprintf(f, "%" PRIu64 ",%" PRIu64 "\n", p.t_ns, p.occupancy);
  return std::fclose(f) == 0;
}

}  // namespace feed
