#include "feed/book_state.hpp"

namespace feed {

namespace {

std::uint64_t mix(std::uint64_t z) {
  z += 0x9E3779B97F4A7C15ULL;
  z = (z ^ (z >> 30U)) * 0xBF58476D1CE4E5B9ULL;
  z = (z ^ (z >> 27U)) * 0x94D049BB133111EBULL;
  return z ^ (z >> 31U);
}

std::uint64_t order_hash(std::uint64_t id, const OrderRecord& r) {
  std::uint64_t h = mix(id ^ 0x1111111111111111ULL);
  h = mix(h ^ r.instrument_id);
  h = mix(h ^ static_cast<std::uint64_t>(r.price));
  h = mix(h ^ r.qty);
  h = mix(h ^ static_cast<std::uint64_t>(r.side));
  return h;
}

std::uint64_t instrument_hash(std::uint32_t id, const InstrumentStats& s) {
  std::uint64_t h = mix(id ^ 0x2222222222222222ULL);
  h = mix(h ^ s.trades);
  h = mix(h ^ s.traded_qty);
  h = mix(h ^ static_cast<std::uint64_t>(s.last_price));
  return h;
}

}  // namespace

std::uint64_t BookState::compute_digest(const std::map<std::uint64_t, OrderRecord>& orders,
                                        const std::map<std::uint32_t, InstrumentStats>& instruments) {
  std::uint64_t d = 0;
  for (const auto& [id, rec] : orders) d ^= order_hash(id, rec);
  for (const auto& [id, st] : instruments) d ^= instrument_hash(id, st);
  return d;
}

ApplyOutcome BookState::apply(const Message& m) {
  ++applied_;
  if (const auto* a = std::get_if<AddOrder>(&m.body)) {
    OrderRecord rec{a->instrument_id, a->price, a->qty, a->side};
    const auto [it, inserted] = orders_.emplace(a->order_id, rec);
    if (!inserted) {
      ++duplicate_adds_;
      return ApplyOutcome::DuplicateAdd;  // keep the existing order; the state is already suspect
    }
    digest_ ^= order_hash(it->first, rec);
    return ApplyOutcome::Applied;
  }
  if (const auto* c = std::get_if<CancelOrder>(&m.body)) {
    const auto it = orders_.find(c->order_id);
    if (it == orders_.end()) {
      ++unknown_cancels_;
      return ApplyOutcome::UnknownCancel;
    }
    digest_ ^= order_hash(it->first, it->second);
    orders_.erase(it);
    return ApplyOutcome::Applied;
  }
  if (const auto* t = std::get_if<Trade>(&m.body)) {
    InstrumentStats& st = instruments_[t->instrument_id];
    if (st.trades != 0) digest_ ^= instrument_hash(t->instrument_id, st);  // remove the old contribution
    ++st.trades;
    st.traded_qty += t->qty;
    st.last_price = t->price;
    digest_ ^= instrument_hash(t->instrument_id, st);
    return ApplyOutcome::Applied;
  }
  return ApplyOutcome::Applied;  // heartbeat
}

void BookState::clear() {
  orders_.clear();
  instruments_.clear();
  digest_ = 0;
}

void BookState::restore(const BookSnapshot& s) {
  orders_ = s.orders;
  instruments_ = s.instruments;
  digest_ = s.digest;
}

BookSnapshot BookState::snapshot(std::uint64_t seq) const {
  BookSnapshot s;
  s.seq = seq;
  s.orders = orders_;
  s.instruments = instruments_;
  s.digest = digest_;
  return s;
}

}  // namespace feed
