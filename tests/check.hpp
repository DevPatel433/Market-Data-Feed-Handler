#pragma once
// Minimal dependency-free test harness (CI and offline builds need no downloads).
#include <cstdio>
#include <vector>

namespace feedtest {
struct Case {
  const char* name;
  void (*fn)();
};
inline std::vector<Case>& registry() {
  static std::vector<Case> r;
  return r;
}
inline int& failures() {
  static int f = 0;
  return f;
}
struct Registrar {
  Registrar(const char* n, void (*f)()) { registry().push_back({n, f}); }
};
}  // namespace feedtest

#define TEST(name)                                                  \
  static void name();                                               \
  static ::feedtest::Registrar registrar_##name(#name, &name);      \
  static void name()

#define CHECK(cond)                                                                         \
  do {                                                                                      \
    if (!(cond)) {                                                                          \
      std::fprintf(stderr, "    CHECK failed at %s:%d: %s\n", __FILE__, __LINE__, #cond);   \
      ++::feedtest::failures();                                                             \
    }                                                                                       \
  } while (0)

#define CHECK_EQ(a, b) CHECK((a) == (b))
