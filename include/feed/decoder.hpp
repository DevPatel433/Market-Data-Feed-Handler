#pragma once
// Safe, allocation-free decoder for wire protocol v1 (docs/protocol.md §6).
// Independent of sockets, queues and files: bytes in, typed message or error out.
#include <cstddef>
#include <cstdint>
#include <span>

#include "feed/protocol.hpp"

namespace feed {

// Exactly one reason per rejected frame, reported in this check order.
enum class DecodeError : std::uint8_t {
  None = 0,
  TooShort,        // frame_len < header size
  TooLarge,        // frame_len > kMaxMessageSize
  BadVersion,      // version != kProtocolVersion
  UnknownType,     // msg_type not in 1..4
  LengthMismatch,  // payload_len != fixed size for type, or != frame_len - header
  BadField         // a value is outside its documented range (incl. seq == 0)
};

inline constexpr std::size_t kDecodeErrorCount = 7;  // number of DecodeError enumerators

const char* to_string(DecodeError e);

// Decodes one frame. Guarantees:
//  * never reads outside `frame`, never allocates, never throws, never aborts;
//  * returns DecodeError::None only for frames that are canonical encodings:
//    encode(out) reproduces `frame` byte for byte;
//  * leaves `out` untouched on error.
DecodeError decode(std::span<const std::uint8_t> frame, Message& out) noexcept;

}  // namespace feed
