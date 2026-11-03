#include "feed/udp_sender.hpp"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <thread>

#include "feed/rng.hpp"
#include "feed/udp_socket.hpp"

namespace feed {

namespace {

using Clock = std::chrono::steady_clock;

// Sleep for the bulk of the wait, then yield-spin. The yield matters: on a single core a pure spin
// would starve the receiver thread/process and cause the very buffer overflows we want to attribute.
void wait_until(Clock::time_point target) {
  using namespace std::chrono_literals;
  const auto remaining = target - Clock::now();
  if (remaining > 300us) std::this_thread::sleep_for(remaining - 150us);
  while (Clock::now() < target) std::this_thread::yield();
}

std::uint64_t ns_since(Clock::time_point start) {
  return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());
}

}  // namespace

bool send_records(const std::vector<ReplayRecord>& records, const UdpSenderConfig& cfg, UdpSendReport& rep,
                  std::string& error, const std::atomic<bool>* stop) {
  rep = {};
  rep.frames_in_file = records.size();
  if (cfg.port == 0) {
    error = "destination port must be set";
    return false;
  }

  UdpSocket sock;
  if (!sock.open(error) || !sock.set_peer(cfg.host, cfg.port, error)) return false;
  if (cfg.sndbuf_bytes > 0 && !sock.set_sndbuf(cfg.sndbuf_bytes, error)) return false;

  const std::uint64_t rate = std::max<std::uint64_t>(1, cfg.rate_per_sec);
  const std::uint64_t burst = std::max<std::uint32_t>(1, cfg.burst_size);
  const double speed = cfg.speed > 0.0 ? cfg.speed : 1.0;
  SplitMix64 junk_rng(cfg.seed ^ 0x2545F4914F6CDD1DULL);
  std::vector<std::uint8_t> junk(cfg.junk_size);

  const auto count_error = [&](int e) {
    ++rep.send_errors;
    if (rep.first_error.empty()) rep.first_error = std::strerror(e);
  };

  const auto start = Clock::now();
  for (std::size_t i = 0; i < records.size(); ++i) {
    if (stop != nullptr && stop->load(std::memory_order_relaxed)) {
      rep.stopped_by_request = true;
      rep.not_sent = records.size() - i;
      break;
    }
    if (cfg.duration_ms > 0 && ns_since(start) >= cfg.duration_ms * 1'000'000ULL) {
      rep.stopped_by_duration = true;
      rep.not_sent = records.size() - i;
      break;
    }

    if (cfg.mode != SendMode::MaxSpeed) {
      std::uint64_t at_ns = 0;
      if (cfg.mode == SendMode::Rate) {
        at_ns = (i / burst) * burst * 1'000'000'000ULL / rate;  // every frame of a burst shares its slot
      } else {
        at_ns = static_cast<std::uint64_t>(static_cast<double>(records[i].offset_ns) / speed);
      }
      const auto target = start + std::chrono::nanoseconds(at_ns);
      wait_until(target);
      const auto now = Clock::now();
      if (now > target) {
        const auto late = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now - target).count());
        rep.max_lateness_ns = std::max(rep.max_lateness_ns, late);
      }
    }

    if (cfg.junk_ppm > 0 && junk_rng.chance_ppm(cfg.junk_ppm)) {
      for (auto& b : junk) b = static_cast<std::uint8_t>(junk_rng.next() & 0xFFU);
      if (!junk.empty()) junk[0] = 0xFF;  // never a valid version byte
      int e = 0;
      if (sock.send(junk, e) == SendStatus::Sent) ++rep.junk_sent;
      else count_error(e);
    }

    int e = 0;
    if (sock.send(records[i].frame, e) == SendStatus::Sent) {
      ++rep.sent;
      rep.bytes += records[i].frame.size();
    } else {
      count_error(e);
    }
  }
  rep.wall_ns = ns_since(start);
  return true;
}

}  // namespace feed
