#pragma once
// Replay file format ("FDRPLAY1"). Normative description: docs/protocol.md §5.
//   magic[8] = "FDRPLAY1"
//   repeated: u64 offset_ns | u16 frame_len | frame bytes   (big-endian)
// Frames are opaque here: a record may hold a malformed frame on purpose.
#include <cstdint>
#include <span>
#include <string>
#include <vector>

namespace feed {

struct ReplayRecord {
  std::uint64_t offset_ns = 0;        // scheduled emission time since replay start
  std::vector<std::uint8_t> frame;    // exactly the bytes a datagram would carry
};

enum class ReplayError { None, BadMagic, TruncatedRecord, NonMonotonicOffset };

const char* to_string(ReplayError e);

// Returns false if any frame exceeds 65535 bytes (u16 length prefix).
bool serialize_replay(const std::vector<ReplayRecord>& records, std::vector<std::uint8_t>& out);

// Parses a whole replay image. On error, `out` holds the records parsed so far.
ReplayError parse_replay(std::span<const std::uint8_t> data, std::vector<ReplayRecord>& out);

bool read_file(const std::string& path, std::vector<std::uint8_t>& out);
bool write_file(const std::string& path, std::span<const std::uint8_t> data);

}  // namespace feed
