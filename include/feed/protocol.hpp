#pragma once
// Wire protocol v1. Normative description: docs/protocol.md.
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <variant>
#include <vector>

namespace feed {

inline constexpr std::uint8_t kProtocolVersion = 1;
inline constexpr std::size_t kHeaderSize = 16;
inline constexpr std::size_t kMaxPayloadSize = 64;
inline constexpr std::size_t kMaxMessageSize = kHeaderSize + kMaxPayloadSize;

enum class MsgType : std::uint8_t { Add = 1, Cancel = 2, Trade = 3, Heartbeat = 4 };
enum class Side : std::uint8_t { Buy = 0, Sell = 1 };

inline constexpr std::size_t kAddPayloadSize = 25;
inline constexpr std::size_t kCancelPayloadSize = 12;
inline constexpr std::size_t kTradePayloadSize = 25;
inline constexpr std::size_t kHeartbeatPayloadSize = 0;

constexpr bool is_known_type(std::uint8_t t) { return t >= 1 && t <= 4; }

constexpr std::size_t payload_size(MsgType t) {
  switch (t) {
    case MsgType::Add: return kAddPayloadSize;
    case MsgType::Cancel: return kCancelPayloadSize;
    case MsgType::Trade: return kTradePayloadSize;
    case MsgType::Heartbeat: return kHeartbeatPayloadSize;
  }
  return 0;
}

// Transport-level header. `seq` is the per-stream transport sequence number and
// is unrelated to order IDs.
struct Header {
  std::uint8_t version = kProtocolVersion;
  MsgType type = MsgType::Heartbeat;
  std::uint16_t payload_len = 0;
  std::uint32_t stream_id = 0;
  std::uint64_t seq = 0;

  bool operator==(const Header&) const = default;
};

// Typed payloads. Prices are signed integer ticks (no floating point on the wire).
struct AddOrder {
  std::uint64_t order_id = 0;
  std::int64_t price = 0;
  std::uint32_t instrument_id = 0;
  std::uint32_t qty = 0;
  Side side = Side::Buy;

  bool operator==(const AddOrder&) const = default;
};
struct CancelOrder {
  std::uint64_t order_id = 0;
  std::uint32_t instrument_id = 0;

  bool operator==(const CancelOrder&) const = default;
};
struct Trade {
  std::uint64_t trade_id = 0;
  std::int64_t price = 0;
  std::uint32_t instrument_id = 0;
  std::uint32_t qty = 0;
  Side aggressor = Side::Buy;

  bool operator==(const Trade&) const = default;
};
struct Heartbeat {
  bool operator==(const Heartbeat&) const = default;
};

// NOTE: alternative order must match MsgType values (index + 1 == type code).
using Payload = std::variant<AddOrder, CancelOrder, Trade, Heartbeat>;

struct Message {
  std::uint32_t stream_id = 1;
  std::uint64_t seq = 0;
  Payload body;

  bool operator==(const Message&) const = default;
};

inline MsgType type_of(const Payload& p) { return static_cast<MsgType>(p.index() + 1); }

// Appends the encoded message (header + payload) to `out`.
void encode(const Message& msg, std::vector<std::uint8_t>& out);

// Cheap structural peek: succeeds iff the frame holds a full header with a
// supported version and a known type. It does NOT validate payload length or
// field ranges. Use feed::decode (decoder.hpp) for anything that trusts the data.
std::optional<Header> peek_header(std::span<const std::uint8_t> frame);

}  // namespace feed
