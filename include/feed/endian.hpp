#pragma once
// Explicit big-endian (network byte order) serialization helpers.
// Nothing here reinterprets raw bytes as a C++ object: every field is
// assembled byte by byte, so alignment and host endianness never matter.
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

namespace feed {

// Appends big-endian values to a growable buffer.
class ByteWriter {
 public:
  explicit ByteWriter(std::vector<std::uint8_t>& out) : out_(out) {}
  void u8(std::uint8_t v) { out_.push_back(v); }
  void u16(std::uint16_t v) { put(v, 2); }
  void u32(std::uint32_t v) { put(v, 4); }
  void u64(std::uint64_t v) { put(v, 8); }
  void i64(std::int64_t v) { put(static_cast<std::uint64_t>(v), 8); }

 private:
  void put(std::uint64_t v, unsigned n) {
    for (unsigned i = n; i > 0; --i) {
      out_.push_back(static_cast<std::uint8_t>((v >> (8U * (i - 1U))) & 0xFFU));
    }
  }
  std::vector<std::uint8_t>& out_;
};

// Bounds-checked big-endian reader. A read past the end latches ok() == false
// and returns 0; it never touches memory outside the span.
class ByteReader {
 public:
  explicit ByteReader(std::span<const std::uint8_t> data) : data_(data) {}
  std::uint8_t u8() { return static_cast<std::uint8_t>(take(1)); }
  std::uint16_t u16() { return static_cast<std::uint16_t>(take(2)); }
  std::uint32_t u32() { return static_cast<std::uint32_t>(take(4)); }
  std::uint64_t u64() { return take(8); }
  std::int64_t i64() { return static_cast<std::int64_t>(take(8)); }
  bool ok() const { return ok_; }
  std::size_t remaining() const { return data_.size() - pos_; }

 private:
  std::uint64_t take(std::size_t n) {
    if (!ok_ || remaining() < n) {
      ok_ = false;
      return 0;
    }
    std::uint64_t v = 0;
    for (std::size_t i = 0; i < n; ++i) v = (v << 8U) | data_[pos_ + i];
    pos_ += n;
    return v;
  }
  std::span<const std::uint8_t> data_;
  std::size_t pos_ = 0;
  bool ok_ = true;
};

}  // namespace feed
