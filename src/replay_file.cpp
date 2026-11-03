#include "feed/replay_file.hpp"

#include <array>
#include <cstdio>
#include <cstring>

#include "feed/endian.hpp"

namespace feed {

namespace {
constexpr std::array<std::uint8_t, 8> kMagic = {'F', 'D', 'R', 'P', 'L', 'A', 'Y', '1'};
constexpr std::size_t kRecordHeader = 10;  // u64 offset + u16 length
}  // namespace

const char* to_string(ReplayError e) {
  switch (e) {
    case ReplayError::None: return "none";
    case ReplayError::BadMagic: return "bad_magic";
    case ReplayError::TruncatedRecord: return "truncated_record";
    case ReplayError::NonMonotonicOffset: return "non_monotonic_offset";
  }
  return "unknown";
}

bool serialize_replay(const std::vector<ReplayRecord>& records, std::vector<std::uint8_t>& out) {
  out.assign(kMagic.begin(), kMagic.end());
  ByteWriter w(out);
  for (const auto& rec : records) {
    if (rec.frame.size() > 0xFFFFU) return false;
    w.u64(rec.offset_ns);
    w.u16(static_cast<std::uint16_t>(rec.frame.size()));
    out.insert(out.end(), rec.frame.begin(), rec.frame.end());
  }
  return true;
}

ReplayError parse_replay(std::span<const std::uint8_t> data, std::vector<ReplayRecord>& out) {
  out.clear();
  if (data.size() < kMagic.size() || std::memcmp(data.data(), kMagic.data(), kMagic.size()) != 0) {
    return ReplayError::BadMagic;
  }
  std::size_t pos = kMagic.size();
  std::uint64_t prev_offset = 0;
  while (pos < data.size()) {
    if (data.size() - pos < kRecordHeader) return ReplayError::TruncatedRecord;
    ByteReader r(data.subspan(pos, kRecordHeader));
    const std::uint64_t offset = r.u64();
    const std::size_t len = r.u16();
    pos += kRecordHeader;
    if (data.size() - pos < len) return ReplayError::TruncatedRecord;
    if (offset < prev_offset) return ReplayError::NonMonotonicOffset;
    prev_offset = offset;
    ReplayRecord rec;
    rec.offset_ns = offset;
    rec.frame.assign(data.begin() + static_cast<std::ptrdiff_t>(pos),
                     data.begin() + static_cast<std::ptrdiff_t>(pos + len));
    out.push_back(std::move(rec));
    pos += len;
  }
  return ReplayError::None;
}

bool read_file(const std::string& path, std::vector<std::uint8_t>& out) {
  std::FILE* f = std::fopen(path.c_str(), "rb");
  if (f == nullptr) return false;
  out.clear();
  std::array<std::uint8_t, 65536> buf{};
  std::size_t n = 0;
  while ((n = std::fread(buf.data(), 1, buf.size(), f)) > 0) {
    out.insert(out.end(), buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(n));
  }
  const bool ok = std::ferror(f) == 0;
  std::fclose(f);
  return ok;
}

bool write_file(const std::string& path, std::span<const std::uint8_t> data) {
  std::FILE* f = std::fopen(path.c_str(), "wb");
  if (f == nullptr) return false;
  const bool ok = std::fwrite(data.data(), 1, data.size(), f) == data.size();
  return (std::fclose(f) == 0) && ok;
}

}  // namespace feed
