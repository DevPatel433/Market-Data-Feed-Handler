#pragma once
// Sends the frames of a replay file as UDP datagrams to a loopback receiver, with pacing, bursts,
// an optional run-time limit, and optional junk datagrams for negative testing.
// Normative description: docs/udp_transport.md.
#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "feed/replay_file.hpp"

namespace feed {

enum class SendMode {
  MaxSpeed,  // no pacing (floods; on a single core this overflows the receiver's socket buffer)
  Timed,     // honour recorded offsets, scaled by `speed`
  Rate       // fixed `rate_per_sec`, delivered in bursts of `burst_size`
};

struct UdpSenderConfig {
  std::string host = "127.0.0.1";  // must be loopback
  std::uint16_t port = 0;
  SendMode mode = SendMode::Rate;
  std::uint64_t rate_per_sec = 10'000;  // Rate: average datagrams per second
  std::uint32_t burst_size = 1;         // Rate: datagrams sent back-to-back, then wait for the next burst slot
  double speed = 1.0;                   // Timed
  std::uint64_t duration_ms = 0;        // 0 = send the whole file; otherwise stop sending after this long
  // Junk: with probability junk_ppm per frame, first send an EXTRA datagram of junk_size random bytes
  // (first byte forced to 0xFF so it is always an invalid frame). The protocol forbids padding and
  // batching, so this is how datagram size is varied: it exercises the receiver's too-short/bad-version/
  // too-large/truncation paths. Junk never replaces a real frame, so it never creates sequence gaps.
  std::uint32_t junk_ppm = 0;
  std::size_t junk_size = 64;
  std::uint64_t seed = 1;
  int sndbuf_bytes = 0;  // 0 = leave the kernel default
};

struct UdpSendReport {
  std::uint64_t frames_in_file = 0;
  std::uint64_t sent = 0;                 // real frames handed to the kernel successfully
  std::uint64_t bytes = 0;                // bytes of those frames
  std::uint64_t junk_sent = 0;            // extra invalid datagrams
  std::uint64_t send_errors = 0;          // sendto failures (frames or junk)
  std::uint64_t not_sent = 0;             // frames left unsent because of --duration or a stop request
  std::uint64_t wall_ns = 0;
  std::uint64_t max_lateness_ns = 0;      // worst (actual - scheduled) send time for paced modes
  bool stopped_by_duration = false;
  bool stopped_by_request = false;
  std::string first_error;                // text of the first send error, if any
};

// Returns false (with `error` set) only for setup failures (bad address, socket creation).
// Individual send failures are counted in the report, not fatal.
bool send_records(const std::vector<ReplayRecord>& records, const UdpSenderConfig& cfg, UdpSendReport& report,
                  std::string& error, const std::atomic<bool>* stop = nullptr);

}  // namespace feed
