#pragma once
// Downstream application state: a minimal order book per stream. Normative description:
// docs/overload_and_recovery.md §3.
//
//   Add     -> inserts a resting order
//   Cancel  -> removes it
//   Trade   -> updates per-instrument trade statistics (the protocol's trades carry no order id, so they
//              do not reduce resting quantity; a deliberate simplification)
//   Heartbeat -> nothing
//
// Two kinds of damage are observable: an *anomaly* (cancel of an unknown order, add of an existing one)
// and *silent divergence* (a missed cancel leaves a stale order, which no check can see locally).
// The only complete test is comparing digest() with a reference, which is why digest() exists.
#include <cstdint>
#include <map>
#include <memory>

#include "feed/protocol.hpp"

namespace feed {

struct OrderRecord {
  std::uint32_t instrument_id = 0;
  std::int64_t price = 0;
  std::uint32_t qty = 0;
  Side side = Side::Buy;
  bool operator==(const OrderRecord&) const = default;
};

struct InstrumentStats {
  std::uint64_t trades = 0;
  std::uint64_t traded_qty = 0;
  std::int64_t last_price = 0;
  bool operator==(const InstrumentStats&) const = default;
};

// A complete copy of book contents as of `seq` (every message with seq <= `seq` applied).
struct BookSnapshot {
  std::uint64_t seq = 0;
  std::map<std::uint64_t, OrderRecord> orders;
  std::map<std::uint32_t, InstrumentStats> instruments;
  std::uint64_t digest = 0;
};

enum class ApplyOutcome : std::uint8_t { Applied, UnknownCancel, DuplicateAdd };

class BookState {
 public:
  ApplyOutcome apply(const Message& m);

  void clear();
  void restore(const BookSnapshot& s);
  BookSnapshot snapshot(std::uint64_t seq) const;

  // Order-independent 64-bit fingerprint of the contents, maintained incrementally (O(1) per message).
  // Two books have equal digests iff (with overwhelming probability) they hold identical orders and trade stats.
  std::uint64_t digest() const { return digest_; }
  static std::uint64_t compute_digest(const std::map<std::uint64_t, OrderRecord>& orders,
                                      const std::map<std::uint32_t, InstrumentStats>& instruments);

  std::size_t live_orders() const { return orders_.size(); }
  std::uint64_t applied() const { return applied_; }
  std::uint64_t unknown_cancels() const { return unknown_cancels_; }
  std::uint64_t duplicate_adds() const { return duplicate_adds_; }
  const std::map<std::uint64_t, OrderRecord>& orders() const { return orders_; }
  const std::map<std::uint32_t, InstrumentStats>& instruments() const { return instruments_; }

 private:
  std::map<std::uint64_t, OrderRecord> orders_;
  std::map<std::uint32_t, InstrumentStats> instruments_;
  std::uint64_t digest_ = 0;
  std::uint64_t applied_ = 0;
  std::uint64_t unknown_cancels_ = 0;
  std::uint64_t duplicate_adds_ = 0;
};

}  // namespace feed
