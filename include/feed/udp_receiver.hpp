#pragma once
// UDP receive loop feeding the decode -> sequence -> SPSC queue -> consumer pipeline.
// Socket handling lives in UdpSocket and this loop; decoding and sequencing are untouched Phase 1-3
// code reached through FrameIntake. Normative description: docs/udp_transport.md.
#include <atomic>
#include <cstdint>
#include <functional>
#include <span>
#include <string>

#include "feed/pipeline.hpp"
#include "feed/udp_socket.hpp"

namespace feed {

enum class RecvMode {
  Poll,      // non-blocking socket + poll(2) with a bounded timeout and a wake-up fd; drains until EAGAIN. Default.
  BusyPoll   // non-blocking socket, spin on recv (yielding when idle). Burns a core; lowest wake-up latency.
};

enum class ShutdownReason { StopRequested, Duration, Idle, FailFast };

const char* to_string(RecvMode m);
const char* to_string(ShutdownReason r);

// Called on the receive thread for every datagram, after it has been decoded and sequenced.
// `bytes` is the (possibly truncated) datagram content; valid only during the call.
using DatagramObserver = std::function<void(std::uint64_t arrival, std::uint64_t t_rx_ns, std::span<const std::uint8_t> bytes,
                                            std::size_t datagram_size, const IngestResult& result, bool truncated)>;

struct UdpReceiverConfig {
  std::string host = "127.0.0.1";  // must be loopback
  std::uint16_t port = 0;          // 0 = ephemeral; read it back with port()
  RecvMode mode = RecvMode::Poll;
  int rcvbuf_bytes = 4 * 1024 * 1024;  // requested; the kernel clamps it (see SocketStats::rcvbuf_effective)
  std::size_t max_datagram = 2048;     // receive buffer size; larger datagrams are truncated and rejected as too_large
  std::uint64_t duration_ms = 0;       // 0 = run until stopped
  std::uint64_t idle_exit_ms = 0;      // 0 = disabled; otherwise stop after this long with no datagram (once one has arrived)
  PipelineConfig pipeline;             // queue, sequencing, consumer. full_policy Block is REJECTED for a network source.
  DatagramObserver observer;           // optional diagnostics hook
};

struct SocketStats {
  std::uint64_t datagrams = 0;        // datagrams returned by recvmsg (all handed to the pipeline)
  std::uint64_t bytes = 0;
  std::uint64_t truncated = 0;        // larger than max_datagram
  std::uint64_t recv_errors = 0;
  std::uint64_t wakeups = 0;          // poll woke because of request_stop
  std::uint64_t poll_timeouts = 0;
  std::uint64_t empty_polls = 0;      // BusyPoll: recv calls that found nothing
  std::uint64_t max_batch = 0;        // most datagrams drained in one pass
  std::uint64_t discarded_at_shutdown = 0;  // fail-fast only: read from the socket but deliberately not processed
  std::uint64_t kernel_drops = 0;     // datagrams the kernel dropped because the socket buffer was full
  bool kernel_drops_from_proc = false;  // true: /proc/net/udp (authoritative); false: last SO_RXQ_OVFL value
  std::uint32_t overflow_counter_last = 0;
  int rcvbuf_effective = 0;
};

struct UdpReport {
  PipelineReport pipeline;
  SocketStats socket;
  ShutdownReason reason = ShutdownReason::StopRequested;
  std::uint64_t elapsed_ns = 0;
  std::uint16_t port = 0;

  // Every datagram the socket returned was handed to the pipeline (and so is in exactly one bucket).
  bool socket_consistent() const { return socket.datagrams == pipeline.ingest.rx_frames; }
  bool balanced() const { return pipeline.balanced() && socket_consistent(); }
};

class UdpReceiver {
 public:
  explicit UdpReceiver(UdpReceiverConfig cfg) : cfg_(std::move(cfg)) {}
  ~UdpReceiver();
  UdpReceiver(const UdpReceiver&) = delete;
  UdpReceiver& operator=(const UdpReceiver&) = delete;

  // Validates the configuration, creates and binds the socket. After success port() is valid and
  // datagrams sent to it are queued by the kernel even before run() starts.
  bool open(std::string& error);
  std::uint16_t port() const { return sock_.local_port(); }

  // Blocks until a shutdown condition. Call from exactly one thread, after open().
  // Shutdown is graceful: the socket is drained, the queue is drained, the consumer joins, then the report is built.
  UdpReport run();

  // Async-signal-safe and callable from any thread: asks run() to shut down gracefully.
  void request_stop() noexcept;

 private:
  void receive_loop(FrameIntake& sink, const std::atomic<bool>& pipeline_stop, SocketStats& stats, ShutdownReason& reason);

  UdpReceiverConfig cfg_;
  UdpSocket sock_;
  int wake_fd_ = -1;
  int rcvbuf_effective_ = 0;
  std::atomic<bool> stop_{false};
};

}  // namespace feed
