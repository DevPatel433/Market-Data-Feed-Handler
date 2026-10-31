#pragma once
// Structured diagnostics shared by feed_replay and the UDP receiver (JSON Lines).
//  * log_event:  one line per anomaly (rejection, gap, duplicate, late arrival, paused discard).
//  * log_packet: one line per datagram with a hex prefix, for diagnosing wire-level problems when a packet
//                capture is not at hand. (With one: tcpdump -i lo -n -X udp port <port>.)
#include <algorithm>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <span>

#include "feed/ingest.hpp"

namespace feed {

inline void log_event(std::FILE* f, std::uint64_t arrival, const IngestResult& r) {
  if (r.error != DecodeError::None) {
    std::fprintf(f, "{\"arrival\":%" PRIu64 ",\"event\":\"rejected\",\"reason\":\"%s\"}\n", arrival, to_string(r.error));
    return;
  }
  const Verdict& v = r.verdict;
  const bool paused_discard = v.cls == Classification::InOrder && v.action == Action::Discard;
  if (v.cls == Classification::InOrder && !paused_discard) return;  // normal traffic is not logged

  const char* event = paused_discard ? "paused_discard" : to_string(v.cls);
  std::fprintf(f, "{\"arrival\":%" PRIu64 ",\"event\":\"%s\",\"stream\":%" PRIu32 ",\"seq\":%" PRIu64, arrival, event,
               r.stream_id, r.seq);
  if (v.cls == Classification::Gap) {
    std::fprintf(f, ",\"first_missing\":%" PRIu64 ",\"count\":%" PRIu64, v.gap_first, v.gap_count);
  }
  std::fprintf(f, ",\"action\":\"%s\",\"state\":\"%s\"}\n", to_string(v.action), to_string(v.state));
}

inline void log_packet(std::FILE* f, std::uint64_t arrival, std::uint64_t t_rx_ns, std::span<const std::uint8_t> bytes,
                       std::size_t datagram_size, const IngestResult& r, bool truncated) {
  static constexpr std::size_t kHexBytes = 48;
  char hex[kHexBytes * 2 + 1];
  const std::size_t n = std::min(bytes.size(), kHexBytes);
  for (std::size_t i = 0; i < n; ++i) std::snprintf(hex + i * 2, 3, "%02x", bytes[i]);
  hex[n * 2] = '\0';
  std::fprintf(f, "{\"arrival\":%" PRIu64 ",\"t_rx_ns\":%" PRIu64 ",\"size\":%zu,\"truncated\":%s,\"hex\":\"%s\",\"result\":\"", arrival,
               t_rx_ns, datagram_size, truncated ? "true" : "false", hex);
  if (r.error != DecodeError::None) {
    std::fprintf(f, "rejected:%s\"}\n", to_string(r.error));
  } else {
    std::fprintf(f, "%s:%s\",\"stream\":%" PRIu32 ",\"seq\":%" PRIu64 "}\n", to_string(r.verdict.cls), to_string(r.verdict.action),
                 r.stream_id, r.seq);
  }
}

}  // namespace feed
