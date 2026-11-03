#include "feed/udp_receiver.hpp"

#include <poll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <thread>
#include <vector>

namespace feed {

namespace {
using Clock = std::chrono::steady_clock;

std::uint64_t now_ns() {
  return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch()).count());
}
constexpr std::size_t kBatch = 256;                        // datagrams per drain pass, so stop/timers are re-checked often
constexpr std::chrono::milliseconds kFinalDrainBudget{100};  // a flooding sender must not keep shutdown from completing
}  // namespace

const char* to_string(RecvMode m) { return m == RecvMode::Poll ? "poll" : "busy"; }
const char* to_string(ShutdownReason r) {
  switch (r) {
    case ShutdownReason::StopRequested: return "stop_requested";
    case ShutdownReason::Duration: return "duration";
    case ShutdownReason::Idle: return "idle";
    case ShutdownReason::FailFast: return "fail_fast";
  }
  return "unknown";
}

UdpReceiver::~UdpReceiver() {
  if (wake_fd_ >= 0) ::close(wake_fd_);
}

bool UdpReceiver::open(std::string& error) {
  if (cfg_.pipeline.full_policy == QueueFullPolicy::Block) {
    error =
        "queue-full policy 'block' is not allowed for a UDP source: a blocked receiver cannot slow the sender, it only "
        "overflows the kernel socket buffer, which is loss this process cannot count in-line (use drop or fail)";
    return false;
  }
  if (cfg_.max_datagram <= kMaxMessageSize || cfg_.max_datagram > 65'535) {
    error = "max_datagram must be in [" + std::to_string(kMaxMessageSize + 1) + ", 65535]";
    return false;
  }
  if (cfg_.pipeline.queue_capacity == 0 || cfg_.pipeline.queue_capacity > SpscQueue<QueuedEvent>::kMaxCapacity) {
    error = "queue capacity out of range";
    return false;
  }
  if (!sock_.open(error) || !sock_.bind_loopback(cfg_.host, cfg_.port, error) || !sock_.set_nonblocking(error)) return false;
  if (!sock_.set_rcvbuf(cfg_.rcvbuf_bytes, rcvbuf_effective_, error)) return false;
  sock_.enable_overflow_counter();  // best effort: the /proc counter is the authoritative one

  wake_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wake_fd_ < 0) {
    error = "eventfd failed";
    return false;
  }
  return true;
}

void UdpReceiver::request_stop() noexcept {
  const int saved = errno;  // keep errno intact when called from a signal handler
  stop_.store(true, std::memory_order_release);
  if (wake_fd_ >= 0) {
    const std::uint64_t one = 1;
    const ssize_t ignored = ::write(wake_fd_, &one, sizeof one);
    (void)ignored;
  }
  errno = saved;
}

void UdpReceiver::receive_loop(FrameIntake& sink, const std::atomic<bool>& pipeline_stop, SocketStats& ss,
                               ShutdownReason& reason) {
  std::vector<std::uint8_t> buf(cfg_.max_datagram);
  const auto start = Clock::now();
  auto last_rx = start;
  bool got_any = false;
  std::uint64_t arrival = 0;

  const auto handle = [&](const RecvInfo& info) {
    const std::uint64_t t_rx = now_ns();  // t_rx: right after the bytes are in user memory (docs/metrics.md §2)
    ++ss.datagrams;
    ss.bytes += info.size;
    ++arrival;
    if (info.has_overflow_counter) ss.overflow_counter_last = info.overflow_counter;
    IngestResult r;
    if (info.truncated) {
      ++ss.truncated;
      r = sink.on_invalid(DecodeError::TooLarge);
    } else {
      r = sink.on_frame(std::span<const std::uint8_t>(buf.data(), info.size));
    }
    if (cfg_.observer) {
      cfg_.observer(arrival, t_rx, std::span<const std::uint8_t>(buf.data(), std::min(info.size, buf.size())), info.size, r, info.truncated);
    }
  };

  // Returns datagrams handled. Stops at EAGAIN or after `limit`.
  const auto drain = [&](std::size_t limit) -> std::size_t {
    std::size_t n = 0;
    while (n < limit) {
      if (pipeline_stop.load(std::memory_order_relaxed)) break;  // fail-fast: stop between datagrams, not just between batches
      RecvInfo info;
      int err = 0;
      const RecvStatus st = sock_.recv(buf, info, err);
      if (st == RecvStatus::Datagram) {
        handle(info);
        ++n;
      } else if (st == RecvStatus::WouldBlock) {
        break;
      } else {
        ++ss.recv_errors;
        break;
      }
    }
    return n;
  };

  const auto ms_until = [](Clock::time_point t) {
    const auto d = std::chrono::duration_cast<std::chrono::milliseconds>(t - Clock::now()).count();
    return d < 0 ? 0 : static_cast<int>(std::min<long long>(d + 1, 1'000'000));
  };

  for (;;) {
    if (stop_.load(std::memory_order_acquire)) { reason = ShutdownReason::StopRequested; break; }
    if (pipeline_stop.load(std::memory_order_relaxed)) { reason = ShutdownReason::FailFast; break; }
    const auto now = Clock::now();
    if (cfg_.duration_ms > 0 && now - start >= std::chrono::milliseconds(cfg_.duration_ms)) { reason = ShutdownReason::Duration; break; }
    if (cfg_.idle_exit_ms > 0 && got_any && now - last_rx >= std::chrono::milliseconds(cfg_.idle_exit_ms)) { reason = ShutdownReason::Idle; break; }

    if (cfg_.mode == RecvMode::Poll) {
      int timeout = 50;  // upper bound on how long a deadline check can be delayed (request_stop wakes us at once)
      if (cfg_.duration_ms > 0) timeout = std::min(timeout, ms_until(start + std::chrono::milliseconds(cfg_.duration_ms)));
      if (cfg_.idle_exit_ms > 0 && got_any) timeout = std::min(timeout, ms_until(last_rx + std::chrono::milliseconds(cfg_.idle_exit_ms)));
      pollfd fds[2] = {{sock_.fd(), POLLIN, 0}, {wake_fd_, POLLIN, 0}};
      const int rc = ::poll(fds, 2, timeout);
      if (rc == 0) { ++ss.poll_timeouts; continue; }
      if (rc < 0) { if (errno != EINTR) ++ss.recv_errors; continue; }
      if ((fds[1].revents & POLLIN) != 0) { ++ss.wakeups; continue; }  // stop_ is set; the loop top handles it
    }

    const std::size_t n = drain(kBatch);
    if (n > 0) {
      got_any = true;
      last_rx = Clock::now();
      ss.max_batch = std::max<std::uint64_t>(ss.max_batch, n);
    } else if (cfg_.mode == RecvMode::BusyPoll) {
      if ((++ss.empty_polls & 31U) == 0) std::this_thread::yield();  // never starve the sender on a single core
    }
  }

  // Graceful shutdown: everything the kernel has already queued is still processed. Bounded in time
  // so a sender that is still flooding cannot keep us here forever.
  const auto budget_end = Clock::now() + kFinalDrainBudget;
  if (reason == ShutdownReason::FailFast) {
    // Fail-fast means stop now: read what is left only so it can be counted (and the socket reconciled
    // with the sender), without running it through the pipeline.
    RecvInfo info;
    int err = 0;
    while (Clock::now() < budget_end && sock_.recv(buf, info, err) == RecvStatus::Datagram) ++ss.discarded_at_shutdown;
  } else {
    while (Clock::now() < budget_end && drain(kBatch) > 0) {
    }
  }
}

UdpReport UdpReceiver::run() {
  UdpReport rep;
  rep.port = sock_.local_port();
  const auto start = Clock::now();
  ShutdownReason reason = ShutdownReason::StopRequested;
  rep.pipeline = run_pipeline_source(cfg_.pipeline, [&](FrameIntake& sink, const std::atomic<bool>& pstop) {
    receive_loop(sink, pstop, rep.socket, reason);
  });
  rep.reason = reason;
  rep.elapsed_ns = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count());

  // The /proc counter covers drops after the last datagram we received, which no ancillary data can report.
  if (const auto d = sock_.kernel_drops()) {
    rep.socket.kernel_drops = *d;
    rep.socket.kernel_drops_from_proc = true;
  } else {
    rep.socket.kernel_drops = rep.socket.overflow_counter_last;
  }
  rep.socket.rcvbuf_effective = rcvbuf_effective_;
  return rep;
}

}  // namespace feed
