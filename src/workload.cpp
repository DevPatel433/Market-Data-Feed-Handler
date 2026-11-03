#include "feed/workload.hpp"

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <utility>

#include "feed/protocol.hpp"
#include "feed/rng.hpp"

namespace feed {

namespace {

constexpr std::uint64_t kNanosPerSec = 1'000'000'000ULL;
constexpr std::size_t kMaxLiveOrders = 4096;

struct LiveOrder {
  std::uint64_t order_id;
  std::uint32_t instrument_id;
};

// Clean, fault-free message stream. Uses its own RNG so the clean stream for a
// given seed is identical across every profile.
std::vector<Message> make_messages(const WorkloadConfig& cfg) {
  SplitMix64 rng(cfg.seed);
  const std::uint32_t instruments = std::max<std::uint32_t>(1, cfg.instruments);
  std::vector<Message> msgs;
  msgs.reserve(cfg.count);
  std::vector<LiveOrder> live;
  std::uint64_t next_order_id = 1;
  std::uint64_t next_trade_id = 1;

  for (std::uint64_t i = 0; i < cfg.count; ++i) {
    enum class Kind { Add, Cancel, Trade, Heartbeat };
    const std::uint64_t roll = rng.uniform(100);
    Kind kind = roll < 45 ? Kind::Add : roll < 65 ? Kind::Cancel : roll < 90 ? Kind::Trade : Kind::Heartbeat;
    if (kind == Kind::Cancel && live.empty()) kind = Kind::Add;
    if (kind == Kind::Add && live.size() >= kMaxLiveOrders) kind = Kind::Cancel;

    Message m;
    m.stream_id = cfg.stream_id;
    m.seq = i + 1;

    const auto make_price = [&](std::uint32_t inst) {
      return 10'000 + static_cast<std::int64_t>(inst) * 100 + static_cast<std::int64_t>(rng.uniform(201)) - 100;
    };
    const auto make_side = [&]() { return rng.uniform(2) == 0 ? Side::Buy : Side::Sell; };

    switch (kind) {
      case Kind::Add: {
        const auto inst = static_cast<std::uint32_t>(rng.uniform(instruments));
        AddOrder a;
        a.order_id = next_order_id++;
        a.instrument_id = inst;
        a.price = make_price(inst);
        a.qty = static_cast<std::uint32_t>(1 + rng.uniform(1000));
        a.side = make_side();
        live.push_back({a.order_id, inst});
        m.body = a;
        break;
      }
      case Kind::Cancel: {
        const std::size_t idx = static_cast<std::size_t>(rng.uniform(live.size()));
        CancelOrder c;
        c.order_id = live[idx].order_id;
        c.instrument_id = live[idx].instrument_id;
        live[idx] = live.back();
        live.pop_back();
        m.body = c;
        break;
      }
      case Kind::Trade: {
        const auto inst = static_cast<std::uint32_t>(rng.uniform(instruments));
        Trade t;
        t.trade_id = next_trade_id++;
        t.instrument_id = inst;
        t.price = make_price(inst);
        t.qty = static_cast<std::uint32_t>(1 + rng.uniform(1000));
        t.aggressor = make_side();
        m.body = t;
        break;
      }
      case Kind::Heartbeat:
        m.body = Heartbeat{};
        break;
    }
    msgs.push_back(std::move(m));
  }
  return msgs;
}

}  // namespace

// Scheduled emission time of the i-th clean message.
std::uint64_t scheduled_offset_ns(const WorkloadConfig& cfg, std::uint64_t i) {
  const std::uint64_t rate = std::max<std::uint64_t>(1, cfg.rate_per_sec);
  if (cfg.profile != Profile::Bursty) return i * kNanosPerSec / rate;

  // Bursty: preserve the mean rate, but emit each group of `burst_size`
  // messages at `burst_multiplier` times that rate, then idle until the next
  // cycle boundary.
  const std::uint64_t b = std::max<std::uint32_t>(1, cfg.burst_size);
  const std::uint64_t mult = std::max<std::uint32_t>(1, cfg.burst_multiplier);
  const std::uint64_t cycle_ns = b * kNanosPerSec / rate;
  const std::uint64_t burst = i / b;
  const std::uint64_t j = i % b;
  return burst * cycle_ns + j * cycle_ns / (mult * b);
}

namespace {

void mutate(std::vector<std::uint8_t>& f, SplitMix64& rng) {
  switch (rng.uniform(5)) {
    case 0:  // truncate to [0, size)
      f.resize(static_cast<std::size_t>(rng.uniform(f.size())));
      break;
    case 1:  // unsupported version (anything but 1)
      f[0] = static_cast<std::uint8_t>(2 + rng.uniform(250));
      break;
    case 2:  // unknown message type
      f[1] = rng.chance_ppm(500'000) ? std::uint8_t{0} : static_cast<std::uint8_t>(5 + rng.uniform(250));
      break;
    case 3: {  // payload_len disagrees with the actual frame
      auto len = static_cast<std::uint32_t>((f[2] << 8U) | f[3]);
      len += static_cast<std::uint32_t>(1 + rng.uniform(8));
      f[2] = static_cast<std::uint8_t>((len >> 8U) & 0xFFU);
      f[3] = static_cast<std::uint8_t>(len & 0xFFU);
      break;
    }
    default: {  // oversize: trailing junk pushes the frame past the maximum size
      const std::uint64_t extra = kMaxPayloadSize + 1 + rng.uniform(64);
      for (std::uint64_t k = 0; k < extra; ++k) f.push_back(static_cast<std::uint8_t>(rng.next() & 0xFFU));
      break;
    }
  }
}

}  // namespace

std::string_view to_string(Profile p) {
  switch (p) {
    case Profile::Normal: return "normal";
    case Profile::Bursty: return "bursty";
    case Profile::Malformed: return "malformed";
    case Profile::Duplicate: return "duplicate";
    case Profile::Gap: return "gap";
    case Profile::Reorder: return "reorder";
  }
  return "unknown";
}

std::optional<Profile> parse_profile(std::string_view s) {
  for (const Profile p : {Profile::Normal, Profile::Bursty, Profile::Malformed, Profile::Duplicate, Profile::Gap,
                          Profile::Reorder}) {
    if (to_string(p) == s) return p;
  }
  return std::nullopt;
}

Workload generate_workload(const WorkloadConfig& cfg) {
  Workload wl;
  const std::vector<Message> msgs = make_messages(cfg);
  wl.stats.clean_messages = msgs.size();

  std::vector<ReplayRecord> clean(msgs.size());
  for (std::size_t i = 0; i < msgs.size(); ++i) {
    clean[i].offset_ns = scheduled_offset_ns(cfg, i);
    encode(msgs[i], clean[i].frame);
  }

  // Independent RNG stream for faults so fault choices never perturb content.
  SplitMix64 frng(cfg.seed ^ 0xD1B54A32D192ED03ULL);
  std::vector<ReplayRecord> out;
  out.reserve(clean.size() + clean.size() / 50);

  switch (cfg.profile) {
    case Profile::Normal:
    case Profile::Bursty:
      out = std::move(clean);
      break;

    case Profile::Duplicate:
      for (auto& rec : clean) {
        const bool dup = frng.chance_ppm(cfg.fault_ppm);
        out.push_back(rec);
        if (dup) {
          out.push_back(rec);  // same offset: re-sent immediately
          ++wl.stats.duplicated;
        }
      }
      break;

    case Profile::Gap:
      for (std::size_t i = 0; i < clean.size();) {
        if (i > 0 && frng.chance_ppm(cfg.fault_ppm)) {  // never drop the first frame
          const std::size_t run = std::min<std::size_t>(1 + frng.uniform(3), clean.size() - i);
          wl.stats.dropped += run;
          i += run;
        } else {
          out.push_back(std::move(clean[i]));
          ++i;
        }
      }
      break;

    case Profile::Reorder:
      for (std::size_t i = 0; i < clean.size(); ++i) {
        if (i + 1 < clean.size() && frng.chance_ppm(cfg.fault_ppm)) {
          std::swap(clean[i].frame, clean[i + 1].frame);  // offsets stay in their slots
          ++wl.stats.reordered;
          out.push_back(std::move(clean[i]));
          out.push_back(std::move(clean[i + 1]));
          ++i;
        } else {
          out.push_back(std::move(clean[i]));
        }
      }
      break;

    case Profile::Malformed:
      for (auto& rec : clean) {
        if (frng.chance_ppm(cfg.fault_ppm)) {
          mutate(rec.frame, frng);
          ++wl.stats.malformed;
        }
        out.push_back(std::move(rec));
      }
      break;
  }

  wl.stats.frames_out = out.size();
  wl.records = std::move(out);
  return wl;
}

std::string manifest_json(const WorkloadConfig& cfg, const FaultStats& s) {
  char buf[768];
  std::snprintf(buf, sizeof buf,
                "{\n"
                "  \"profile\": \"%s\",\n"
                "  \"seed\": %" PRIu64 ",\n"
                "  \"stream_id\": %" PRIu32 ",\n"
                "  \"rate_per_sec\": %" PRIu64 ",\n"
                "  \"fault_ppm\": %" PRIu32 ",\n"
                "  \"clean_messages\": %" PRIu64 ",\n"
                "  \"frames_out\": %" PRIu64 ",\n"
                "  \"duplicated\": %" PRIu64 ",\n"
                "  \"dropped\": %" PRIu64 ",\n"
                "  \"reordered\": %" PRIu64 ",\n"
                "  \"malformed\": %" PRIu64 "\n"
                "}\n",
                std::string(to_string(cfg.profile)).c_str(), cfg.seed, cfg.stream_id, cfg.rate_per_sec,
                cfg.fault_ppm, s.clean_messages, s.frames_out, s.duplicated, s.dropped, s.reordered,
                s.malformed);
  return buf;
}

}  // namespace feed
