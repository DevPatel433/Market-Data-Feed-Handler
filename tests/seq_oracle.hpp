#pragma once
// Deliberately naive reference model for sequence classification: it tracks
// missing sequence numbers one by one in a std::set, whereas the production
// Sequencer tracks ranges. Used for differential testing only.
#include <cstdint>
#include <set>

#include "feed/sequencer.hpp"

namespace feedtest {

struct Oracle {
  std::uint64_t expected = 1;
  std::set<std::uint64_t> missing;

  feed::Classification classify(std::uint64_t seq, std::uint64_t& gap_first, std::uint64_t& gap_count) {
    gap_first = gap_count = 0;
    if (seq == expected) {
      ++expected;
      return feed::Classification::InOrder;
    }
    if (seq > expected) {
      gap_first = expected;
      gap_count = seq - expected;
      for (std::uint64_t s = expected; s < seq; ++s) missing.insert(s);
      expected = seq + 1;
      return feed::Classification::Gap;
    }
    return missing.erase(seq) > 0 ? feed::Classification::OutOfOrder : feed::Classification::Duplicate;
  }
};

}  // namespace feedtest
