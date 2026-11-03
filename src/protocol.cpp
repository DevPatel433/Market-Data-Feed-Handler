#include "feed/protocol.hpp"

#include "feed/endian.hpp"

namespace feed {

namespace {
struct PayloadWriter {
  ByteWriter& w;
  void operator()(const AddOrder& m) const {
    w.u64(m.order_id);
    w.i64(m.price);
    w.u32(m.instrument_id);
    w.u32(m.qty);
    w.u8(static_cast<std::uint8_t>(m.side));
  }
  void operator()(const CancelOrder& m) const {
    w.u64(m.order_id);
    w.u32(m.instrument_id);
  }
  void operator()(const Trade& m) const {
    w.u64(m.trade_id);
    w.i64(m.price);
    w.u32(m.instrument_id);
    w.u32(m.qty);
    w.u8(static_cast<std::uint8_t>(m.aggressor));
  }
  void operator()(const Heartbeat&) const {}
};
}  // namespace

void encode(const Message& msg, std::vector<std::uint8_t>& out) {
  const std::size_t start = out.size();
  ByteWriter w(out);
  w.u8(kProtocolVersion);
  w.u8(static_cast<std::uint8_t>(type_of(msg.body)));
  w.u16(0);  // payload_len, patched below
  w.u32(msg.stream_id);
  w.u64(msg.seq);
  std::visit(PayloadWriter{w}, msg.body);

  const std::size_t payload_len = out.size() - start - kHeaderSize;
  out[start + 2] = static_cast<std::uint8_t>((payload_len >> 8U) & 0xFFU);
  out[start + 3] = static_cast<std::uint8_t>(payload_len & 0xFFU);
}

std::optional<Header> peek_header(std::span<const std::uint8_t> frame) {
  if (frame.size() < kHeaderSize) return std::nullopt;
  ByteReader r(frame);
  Header h;
  h.version = r.u8();
  const std::uint8_t type = r.u8();
  h.payload_len = r.u16();
  h.stream_id = r.u32();
  h.seq = r.u64();
  if (h.version != kProtocolVersion || !is_known_type(type)) return std::nullopt;
  h.type = static_cast<MsgType>(type);
  return h;
}

}  // namespace feed
