#include <vector>

#include "check.hpp"
#include "feed/endian.hpp"
#include "feed/metrics_defs.hpp"
#include "feed/protocol.hpp"

using namespace feed;

namespace {
std::vector<std::uint8_t> bytes(std::initializer_list<int> il) {
  std::vector<std::uint8_t> v;
  for (const int b : il) v.push_back(static_cast<std::uint8_t>(b));
  return v;
}
}  // namespace

// The first two cases pin the exact bytes shown in docs/protocol.md §4.
TEST(protocol_add_order_hex_example) {
  Message m;
  m.stream_id = 1;
  m.seq = 1;
  m.body = AddOrder{1000, 10050, 7, 100, Side::Buy};
  std::vector<std::uint8_t> out;
  encode(m, out);
  const auto expected = bytes({0x01, 0x01, 0x00, 0x19,                          // ver, type, payload_len=25
                               0x00, 0x00, 0x00, 0x01,                          // stream_id
                               0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,  // seq
                               0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0xE8,  // order_id 1000
                               0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x27, 0x42,  // price 10050
                               0x00, 0x00, 0x00, 0x07,                          // instrument 7
                               0x00, 0x00, 0x00, 0x64,                          // qty 100
                               0x00});                                          // side Buy
  CHECK(out == expected);
  CHECK_EQ(out.size(), kHeaderSize + kAddPayloadSize);
}

TEST(protocol_heartbeat_hex_example) {
  Message m;
  m.stream_id = 1;
  m.seq = 2;
  m.body = Heartbeat{};
  std::vector<std::uint8_t> out;
  encode(m, out);
  CHECK(out == bytes({0x01, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02}));
}

TEST(protocol_sizes_match_payload_size_table) {
  const Message msgs[] = {
      {1, 1, AddOrder{}}, {1, 2, CancelOrder{}}, {1, 3, Trade{}}, {1, 4, Heartbeat{}}};
  for (const auto& m : msgs) {
    std::vector<std::uint8_t> out;
    encode(m, out);
    CHECK_EQ(out.size(), kHeaderSize + payload_size(type_of(m.body)));
    CHECK(out.size() <= kMaxMessageSize);
    const auto h = peek_header(out);
    CHECK(h.has_value());
    if (h) {
      CHECK(h->type == type_of(m.body));
      CHECK_EQ(h->payload_len, payload_size(type_of(m.body)));
      CHECK_EQ(h->seq, m.seq);
    }
  }
}

TEST(protocol_negative_price_is_big_endian_twos_complement) {
  Message m;
  m.body = Trade{5, -1, 1, 1, Side::Sell};
  std::vector<std::uint8_t> out;
  encode(m, out);
  for (std::size_t i = 0; i < 8; ++i) CHECK_EQ(out[kHeaderSize + 8 + i], 0xFF);
}

TEST(peek_header_rejects_short_bad_version_and_unknown_type) {
  std::vector<std::uint8_t> good;
  Message m;
  m.seq = 9;
  m.body = Heartbeat{};
  encode(m, good);
  CHECK(peek_header(good).has_value());

  for (std::size_t n = 0; n < kHeaderSize; ++n) CHECK(!peek_header(std::span(good).first(n)).has_value());

  auto bad_version = good;
  bad_version[0] = 2;
  CHECK(!peek_header(bad_version).has_value());

  for (const int t : {0, 5, 255}) {
    auto bad_type = good;
    bad_type[1] = static_cast<std::uint8_t>(t);
    CHECK(!peek_header(bad_type).has_value());
  }
}

TEST(byte_reader_never_reads_past_end) {
  const auto data = bytes({1, 2, 3});
  ByteReader r(data);
  CHECK_EQ(r.u16(), 0x0102);
  CHECK(r.ok());
  CHECK_EQ(r.u32(), 0U);  // only 1 byte left
  CHECK(!r.ok());
  CHECK_EQ(r.u8(), 0);  // stays failed
}

TEST(metrics_defs_derived_latencies) {
  StageTimestamps t;
  t.t_rx = 100;
  t.t_decoded = 130;
  t.t_enqueued = 140;
  t.t_dequeued = 200;
  t.t_done = 260;
  CHECK_EQ(t.decode_ns(), 30U);
  CHECK_EQ(t.queue_ns(), 60U);
  CHECK_EQ(t.process_ns(), 60U);
  CHECK_EQ(t.end_to_end_ns(), 160U);
  CHECK(to_string(LossKind::QueueOverflow) == "queue_overflow");
}
