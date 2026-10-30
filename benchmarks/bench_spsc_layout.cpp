// Layout benchmark for feed::SpscQueue: does cache-line padding or index caching help?
// The roadmap says to adopt them only after measuring, so this harness exists to make that
// decision reproducible. It prints JSON with the machine description and per-variant statistics.
//
//   feed_bench_spsc [--messages N] [--repeats R] [--pin]
//
// IMPORTANT: false sharing and cache-line ping-pong only exist when the two threads run on
// different cores. On a single-core machine the result is dominated by scheduler behaviour and
// says NOTHING about layout; the report includes "valid_for_layout_decision" for that reason.
#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <pthread.h>
#include <sched.h>

#include "feed/spsc_queue.hpp"

using namespace feed;

namespace {

template <std::size_t Bytes>
struct Payload {
  std::uint64_t seq = 0;
  std::array<std::uint8_t, Bytes - sizeof(std::uint64_t)> pad{};  // std::array: legal even when the size is 0
};

void pin_to(std::size_t cpu) {
#ifdef __linux__
  cpu_set_t set;
  CPU_ZERO(&set);
  CPU_SET(cpu, &set);
  pthread_setaffinity_np(pthread_self(), sizeof(set), &set);
#else
  (void)cpu;
#endif
}

// One run: producer pushes `n` events as fast as it can; the consumer pops them all. Returns events/second.
template <class Q>
double run_once(std::uint64_t n, bool pin) {
  Q q(1024);
  const int cores = static_cast<int>(std::thread::hardware_concurrency());
  std::uint64_t bad = 0;
  const auto t0 = std::chrono::steady_clock::now();
  std::thread producer([&] {
    if (pin && cores >= 2) pin_to(1);
    typename Q::value_type ev;
    for (std::uint64_t i = 1; i <= n; ++i) {
      ev.seq = i;
      std::uint32_t spins = 0;
      while (!q.try_push(ev)) {
        if (++spins >= 64) std::this_thread::yield();
      }
    }
  });
  if (pin && cores >= 2) pin_to(0);
  typename Q::value_type ev;
  std::uint64_t expected = 1;
  std::uint32_t spins = 0;
  while (expected <= n) {
    if (q.try_pop(ev)) {
      if (ev.seq != expected) ++bad;
      ++expected;
      spins = 0;
    } else if (++spins >= 64) {
      std::this_thread::yield();
    }
  }
  producer.join();
  const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
  if (bad != 0) {
    std::fprintf(stderr, "ORDER ERRORS in benchmark: %llu\n", static_cast<unsigned long long>(bad));
    std::exit(1);
  }
  return static_cast<double>(n) / secs;
}

struct Stats {
  double median, min, max;
};
Stats summarize(std::vector<double> v) {
  std::sort(v.begin(), v.end());
  return {v[v.size() / 2], v.front(), v.back()};
}

template <class Q>
void bench(const char* name, std::size_t event_bytes, std::uint64_t n, int repeats, bool pin, bool& first) {
  run_once<Q>(n / 10 + 1, pin);  // warm-up, discarded
  std::vector<double> samples;
  for (int i = 0; i < repeats; ++i) samples.push_back(run_once<Q>(n, pin));
  const Stats s = summarize(samples);
  std::printf("%s    {\"variant\": \"%s\", \"event_bytes\": %zu, \"median_mops\": %.2f, \"min_mops\": %.2f, \"max_mops\": %.2f}",
              first ? "" : ",\n", name, event_bytes, s.median / 1e6, s.min / 1e6, s.max / 1e6);
  first = false;
}

template <std::size_t Bytes>
void bench_all(std::uint64_t n, int repeats, bool pin, bool& first) {
  using E = Payload<Bytes>;
  bench<SpscQueue<E, SpscOptions{false, false, true}>>("plain", Bytes, n, repeats, pin, first);
  bench<SpscQueue<E, SpscOptions{false, false, false}>>("plain_no_high_water", Bytes, n, repeats, pin, first);
  bench<SpscQueue<E, SpscOptions{true, false, true}>>("padded", Bytes, n, repeats, pin, first);
  bench<SpscQueue<E, SpscOptions{false, true, true}>>("cached_index", Bytes, n, repeats, pin, first);
  bench<SpscQueue<E, SpscOptions{true, true, true}>>("padded_cached", Bytes, n, repeats, pin, first);
  bench<SpscQueue<E, SpscOptions{true, true, false}>>("padded_cached_no_high_water", Bytes, n, repeats, pin, first);
}

std::string cpu_model() {
  std::ifstream f("/proc/cpuinfo");
  for (std::string line; std::getline(f, line);) {
    if (line.rfind("model name", 0) == 0) {
      const auto pos = line.find(':');
      if (pos != std::string::npos) return line.substr(pos + 2);
    }
  }
  return "unknown";
}

}  // namespace

int main(int argc, char** argv) {
  std::uint64_t n = 2'000'000;
  int repeats = 5;
  bool pin = false;
  for (int i = 1; i < argc; ++i) {
    if (std::strcmp(argv[i], "--messages") == 0 && i + 1 < argc) n = std::strtoull(argv[++i], nullptr, 10);
    else if (std::strcmp(argv[i], "--repeats") == 0 && i + 1 < argc) repeats = std::atoi(argv[++i]);
    else if (std::strcmp(argv[i], "--pin") == 0) pin = true;
    else {
      std::fprintf(stderr, "usage: %s [--messages N] [--repeats R] [--pin]\n", argv[0]);
      return 2;
    }
  }
  if (repeats < 1 || n < 1000) return 2;

  const unsigned cores = std::thread::hardware_concurrency();
#ifdef NDEBUG
  const char* build = "optimized (NDEBUG)";
#else
  const char* build = "NOT optimized: numbers are meaningless";
#endif
  std::printf("{\n  \"machine\": {\"cpu\": \"%s\", \"hardware_threads\": %u, \"pinned\": %s, \"build\": \"%s\"},\n", cpu_model().c_str(),
              cores, (pin && cores >= 2) ? "true" : "false", build);
  std::printf("  \"messages_per_run\": %llu, \"repeats\": %d, \"queue_capacity\": 1024,\n", static_cast<unsigned long long>(n), repeats);
  std::printf("  \"valid_for_layout_decision\": %s,\n", cores >= 2 ? "true" : "false");
  if (cores < 2) {
    std::printf("  \"warning\": \"single hardware thread: producer and consumer time-slice on one core, so there is no cache-line contention to measure\",\n");
  }
  std::printf("  \"results\": [\n");
  bool first = true;
  bench_all<8>(n, repeats, pin, first);
  bench_all<64>(n, repeats, pin, first);
  std::printf("\n  ]\n}\n");
  return 0;
}
