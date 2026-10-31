#pragma once
// Thin RAII wrapper over a Linux IPv4 UDP socket. It knows nothing about the feed protocol:
// bytes in, bytes out. Decoding, sequencing and queueing live elsewhere (docs/udp_transport.md).
//
// Scope guard: this project is a local simulation. Every address given to this layer must be a
// loopback address (127.0.0.0/8 or "localhost"); anything else is refused, so a mistyped flag can
// never aim a rate-controlled sender at a real host.
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>

namespace feed {

// Accepts "localhost" or a dotted-quad inside 127.0.0.0/8. Writes the address in host byte order.
bool parse_loopback_ipv4(const std::string& host, std::uint32_t& addr_host_order);

struct RecvInfo {
  std::size_t size = 0;                  // bytes copied into the caller's buffer
  bool truncated = false;                // datagram was larger than the buffer; the rest was discarded by the kernel
  bool has_overflow_counter = false;     // SO_RXQ_OVFL ancillary data was present
  std::uint32_t overflow_counter = 0;    // cumulative receive-queue drops, as of this datagram
};

enum class RecvStatus { Datagram, WouldBlock, Error };
enum class SendStatus { Sent, WouldBlock, Error };

class UdpSocket {
 public:
  UdpSocket() = default;
  ~UdpSocket();
  UdpSocket(const UdpSocket&) = delete;
  UdpSocket& operator=(const UdpSocket&) = delete;
  UdpSocket(UdpSocket&& other) noexcept;
  UdpSocket& operator=(UdpSocket&& other) noexcept;

  bool open(std::string& error);
  void close();
  bool is_open() const { return fd_ >= 0; }
  int fd() const { return fd_; }

  // Receive side.
  bool bind_loopback(const std::string& host, std::uint16_t port, std::string& error);  // port 0 = ephemeral
  std::uint16_t local_port() const { return local_port_; }
  bool set_nonblocking(std::string& error);
  bool set_rcvbuf(int requested_bytes, int& effective_bytes, std::string& error);  // kernel may clamp/double
  bool enable_overflow_counter();                                                // SO_RXQ_OVFL, best effort
  RecvStatus recv(std::span<std::uint8_t> buffer, RecvInfo& info, int& error_number);
  // Total datagrams the kernel dropped for this socket (full receive buffer), from /proc/net/udp.
  std::optional<std::uint64_t> kernel_drops() const;

  // Send side.
  bool set_peer(const std::string& host, std::uint16_t port, std::string& error);  // loopback only
  bool set_sndbuf(int requested_bytes, std::string& error);
  SendStatus send(std::span<const std::uint8_t> data, int& error_number);

 private:
  int fd_ = -1;
  std::uint16_t local_port_ = 0;
  bool has_peer_ = false;
  alignas(8) unsigned char peer_[16] = {};  // sockaddr_in, kept opaque so this header needs no socket includes
};

}  // namespace feed
