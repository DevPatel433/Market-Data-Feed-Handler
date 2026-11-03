#include "feed/decoder.hpp"

#include "feed/endian.hpp"

namespace feed {

const char* to_string(DecodeError e) {
  switch (e) {
    case DecodeError::None: return "none";
    case DecodeError::TooShort: return "too_short";
    case DecodeError::TooLarge: return "too_large";
    case DecodeError::BadVersion: return "bad_version";
    case DecodeError::UnknownType: return "unknown_type";
    case DecodeError::LengthMismatch: return "length_mismatch";
    case DecodeError::BadField: return "bad_field";
  }
  return "unknown";
}

namespace {

constexpr bool valid_side(std::uint8_t s) { return s <= static_cast<std::uint8_t>(Side::Sell); }

}  // namespace

DecodeError decode(std::span<const std::uint8_t> frame, Message& out) noexcept {
  // Size checks come first, so every later read is provably in bounds.
  if (frame.size() < kHeaderSize) return DecodeError::TooShort;
  if (frame.size() > kMaxMessageSize) return DecodeError::TooLarge;

  ByteReader r(frame);
  const std::uint8_t version = r.u8();
  const std::uint8_t type_code = r.u8();
  const std::size_t payload_len = r.u16();
  const std::uint32_t stream_id = r.u32();
  const std::uint64_t seq = r.u64();

  if (version != kProtocolVersion) return DecodeError::BadVersion;
  if (!is_known_type(type_code)) return DecodeError::UnknownType;

  const auto type = static_cast<MsgType>(type_code);
  if (payload_len != payload_size(type) || payload_len != frame.size() - kHeaderSize) {
    return DecodeError::LengthMismatch;
  }
  if (seq == 0) return DecodeError::BadField;

  Message m;
  m.stream_id = stream_id;
  m.seq = seq;

  switch (type) {
    case MsgType::Add: {
      AddOrder a;
      a.order_id = r.u64();
      a.price = r.i64();
      a.instrument_id = r.u32();
      a.qty = r.u32();
      const std::uint8_t side = r.u8();
      if (a.order_id == 0 || a.price <= 0 || a.qty == 0 || !valid_side(side)) return DecodeError::BadField;
      a.side = static_cast<Side>(side);
      m.body = a;
      break;
    }
    case MsgType::Cancel: {
      CancelOrder c;
      c.order_id = r.u64();
      c.instrument_id = r.u32();
      if (c.order_id == 0) return DecodeError::BadField;
      m.body = c;
      break;
    }
    case MsgType::Trade: {
      Trade t;
      t.trade_id = r.u64();
      t.price = r.i64();
      t.instrument_id = r.u32();
      t.qty = r.u32();
      const std::uint8_t side = r.u8();
      if (t.trade_id == 0 || t.price <= 0 || t.qty == 0 || !valid_side(side)) return DecodeError::BadField;
      t.aggressor = static_cast<Side>(side);
      m.body = t;
      break;
    }
    case MsgType::Heartbeat:
      m.body = Heartbeat{};
      break;
  }

  // Defensive: the length checks above make this unreachable, but a latched
  // reader failure must never be reported as success.
  if (!r.ok() || r.remaining() != 0) return DecodeError::LengthMismatch;

  out = m;
  return DecodeError::None;
}

}  // namespace feed
