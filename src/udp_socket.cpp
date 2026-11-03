#include "feed/udp_socket.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <fstream>
#include <sstream>

namespace feed {

static_assert(sizeof(sockaddr_in) == 16, "peer_ storage assumes a 16-byte sockaddr_in");

namespace {

std::string errno_text(const char* what) { return std::string(what) + ": " + std::strerror(errno); }

bool make_loopback_addr(const std::string& host, std::uint16_t port, sockaddr_in& out, std::string& error) {
  std::uint32_t a = 0;
  if (!parse_loopback_ipv4(host, a)) {
    error = "address '" + host + "' is not a loopback address (127.0.0.0/8 or localhost); this tool is local-only";
    return false;
  }
  std::memset(&out, 0, sizeof out);
  out.sin_family = AF_INET;
  out.sin_port = htons(port);
  out.sin_addr.s_addr = htonl(a);
  return true;
}

}  // namespace

bool parse_loopback_ipv4(const std::string& host, std::uint32_t& addr_host_order) {
  const std::string text = host == "localhost" ? "127.0.0.1" : host;
  in_addr a{};
  if (inet_pton(AF_INET, text.c_str(), &a) != 1) return false;
  const std::uint32_t v = ntohl(a.s_addr);
  if ((v >> 24U) != 127U) return false;
  addr_host_order = v;
  return true;
}

UdpSocket::~UdpSocket() { close(); }

UdpSocket::UdpSocket(UdpSocket&& o) noexcept
    : fd_(o.fd_), local_port_(o.local_port_), has_peer_(o.has_peer_) {
  std::memcpy(peer_, o.peer_, sizeof peer_);
  o.fd_ = -1;
}

UdpSocket& UdpSocket::operator=(UdpSocket&& o) noexcept {
  if (this != &o) {
    close();
    fd_ = o.fd_;
    local_port_ = o.local_port_;
    has_peer_ = o.has_peer_;
    std::memcpy(peer_, o.peer_, sizeof peer_);
    o.fd_ = -1;
  }
  return *this;
}

bool UdpSocket::open(std::string& error) {
  close();
  fd_ = ::socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
  if (fd_ < 0) {
    error = errno_text("socket");
    return false;
  }
  return true;
}

void UdpSocket::close() {
  if (fd_ >= 0) ::close(fd_);
  fd_ = -1;
  local_port_ = 0;
  has_peer_ = false;
}

bool UdpSocket::bind_loopback(const std::string& host, std::uint16_t port, std::string& error) {
  sockaddr_in a{};
  if (!make_loopback_addr(host, port, a, error)) return false;
  if (::bind(fd_, reinterpret_cast<const sockaddr*>(&a), sizeof a) != 0) {
    error = errno_text("bind");
    return false;
  }
  socklen_t len = sizeof a;
  if (::getsockname(fd_, reinterpret_cast<sockaddr*>(&a), &len) != 0) {
    error = errno_text("getsockname");
    return false;
  }
  local_port_ = ntohs(a.sin_port);
  return true;
}

bool UdpSocket::set_nonblocking(std::string& error) {
  const int flags = ::fcntl(fd_, F_GETFL, 0);
  if (flags < 0 || ::fcntl(fd_, F_SETFL, flags | O_NONBLOCK) != 0) {
    error = errno_text("fcntl(O_NONBLOCK)");
    return false;
  }
  return true;
}

bool UdpSocket::set_rcvbuf(int requested, int& effective, std::string& error) {
  if (::setsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &requested, sizeof requested) != 0) {
    error = errno_text("setsockopt(SO_RCVBUF)");
    return false;
  }
  effective = 0;
  socklen_t len = sizeof effective;
  ::getsockopt(fd_, SOL_SOCKET, SO_RCVBUF, &effective, &len);
  return true;
}

bool UdpSocket::set_sndbuf(int requested, std::string& error) {
  if (::setsockopt(fd_, SOL_SOCKET, SO_SNDBUF, &requested, sizeof requested) != 0) {
    error = errno_text("setsockopt(SO_SNDBUF)");
    return false;
  }
  return true;
}

bool UdpSocket::enable_overflow_counter() {
  const int one = 1;
  return ::setsockopt(fd_, SOL_SOCKET, SO_RXQ_OVFL, &one, sizeof one) == 0;
}

RecvStatus UdpSocket::recv(std::span<std::uint8_t> buffer, RecvInfo& info, int& error_number) {
  info = {};
  alignas(cmsghdr) unsigned char control[CMSG_SPACE(sizeof(std::uint32_t))];
  iovec iov{buffer.data(), buffer.size()};
  msghdr msg{};
  msg.msg_iov = &iov;
  msg.msg_iovlen = 1;
  msg.msg_control = control;
  msg.msg_controllen = sizeof control;

  ssize_t n = 0;
  do {
    n = ::recvmsg(fd_, &msg, 0);
  } while (n < 0 && errno == EINTR);
  if (n < 0) {
    error_number = errno;
    return (errno == EAGAIN || errno == EWOULDBLOCK) ? RecvStatus::WouldBlock : RecvStatus::Error;
  }
  info.size = static_cast<std::size_t>(n);
  info.truncated = (msg.msg_flags & MSG_TRUNC) != 0;
  for (cmsghdr* c = CMSG_FIRSTHDR(&msg); c != nullptr; c = CMSG_NXTHDR(&msg, c)) {
    if (c->cmsg_level == SOL_SOCKET && c->cmsg_type == SO_RXQ_OVFL) {
      std::memcpy(&info.overflow_counter, CMSG_DATA(c), sizeof info.overflow_counter);
      info.has_overflow_counter = true;
    }
  }
  return RecvStatus::Datagram;
}

std::optional<std::uint64_t> UdpSocket::kernel_drops() const {
  struct stat st {};
  if (fd_ < 0 || ::fstat(fd_, &st) != 0) return std::nullopt;
  std::ifstream f("/proc/net/udp");
  if (!f) return std::nullopt;
  const std::string want = std::to_string(static_cast<unsigned long long>(st.st_ino));
  std::string line;
  std::getline(f, line);  // header
  while (std::getline(f, line)) {
    std::istringstream is(line);
    std::string tok, inode, drops;
    for (int idx = 0; is >> tok; ++idx) {  // columns: sl local rem st queues tr retrnsmt uid timeout inode ref pointer drops
      if (idx == 9) inode = tok;
      if (idx == 12) drops = tok;
    }
    if (inode == want && !drops.empty()) return std::stoull(drops);
  }
  return std::nullopt;
}

bool UdpSocket::set_peer(const std::string& host, std::uint16_t port, std::string& error) {
  sockaddr_in a{};
  if (!make_loopback_addr(host, port, a, error)) return false;
  std::memcpy(peer_, &a, sizeof a);
  has_peer_ = true;
  return true;
}

SendStatus UdpSocket::send(std::span<const std::uint8_t> data, int& error_number) {
  if (!has_peer_) {
    error_number = EDESTADDRREQ;
    return SendStatus::Error;
  }
  sockaddr_in a{};
  std::memcpy(&a, peer_, sizeof a);
  ssize_t n = 0;
  do {
    n = ::sendto(fd_, data.data(), data.size(), 0, reinterpret_cast<const sockaddr*>(&a), sizeof a);
  } while (n < 0 && errno == EINTR);
  if (n < 0) {
    error_number = errno;
    return (errno == EAGAIN || errno == EWOULDBLOCK) ? SendStatus::WouldBlock : SendStatus::Error;
  }
  return SendStatus::Sent;
}

}  // namespace feed
