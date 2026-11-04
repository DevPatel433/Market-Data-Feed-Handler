#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "check.hpp"

int main() {
  int failed_cases = 0;
  std::size_t ran = 0;
  const char* filter = std::getenv("FEED_TEST_FILTER");  // run only tests whose name contains this substring
  for (const auto& c : feedtest::registry()) {
    if (filter != nullptr) {  // several substrings may be given, separated by '|'
      bool match = false;
      for (const char* f = filter; *f != '\0' && !match;) {
        const char* bar = std::strchr(f, '|');
        const std::size_t len = bar != nullptr ? static_cast<std::size_t>(bar - f) : std::strlen(f);
        match = len > 0 && std::string(c.name).find(std::string(f, len)) != std::string::npos;
        f += len + (bar != nullptr ? 1 : 0);
      }
      if (!match) continue;
    }
    ++ran;
    const int before = feedtest::failures();
    c.fn();
    const bool ok = feedtest::failures() == before;
    std::printf("[%s] %s\n", ok ? " OK " : "FAIL", c.name);
    if (!ok) ++failed_cases;
  }
  std::printf("%zu tests, %d failed\n", ran, failed_cases);
  return failed_cases == 0 ? 0 : 1;
}
