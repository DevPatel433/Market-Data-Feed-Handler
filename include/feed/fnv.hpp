#pragma once
// FNV-1a 64-bit digest: cheap, deterministic fingerprint of an ordered stream of values.
#include <cstdint>

namespace feed {

struct Fnv {
  std::uint64_t h = 0xCBF29CE484222325ULL;
  void byte(std::uint8_t b) {
    h ^= b;
    h *= 0x100000001B3ULL;
  }
  void u64(std::uint64_t v) {
    for (int i = 7; i >= 0; --i) byte(static_cast<std::uint8_t>((v >> (8 * i)) & 0xFFU));
  }
};

}  // namespace feed
