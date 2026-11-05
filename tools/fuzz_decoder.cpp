// Fuzz target for feed::decode.
//  * libFuzzer build (clang): cmake -DFEED_FUZZ=ON ... then run feed_fuzz_decoder CORPUS_DIR
//  * Standalone build (any compiler): feed_fuzz_replay FILE... runs each file once;
//    use it to reproduce a crash artifact or to smoke-test a corpus under sanitizers.
// The oracle is the decoder's contract: no crash/UB on any input, and a frame
// that decodes successfully must re-encode to exactly the same bytes.
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "feed/decoder.hpp"

extern "C" int LLVMFuzzerTestOneInput(const std::uint8_t* data, std::size_t size) {
  feed::Message msg;
  if (feed::decode(std::span<const std::uint8_t>(data, size), msg) == feed::DecodeError::None) {
    std::vector<std::uint8_t> again;
    feed::encode(msg, again);
    if (again.size() != size || (size != 0 && std::memcmp(again.data(), data, size) != 0)) __builtin_trap();
  }
  return 0;
}

#ifdef FEED_FUZZ_STANDALONE
#include <cstdio>
#include <string>

#include "feed/replay_file.hpp"

int main(int argc, char** argv) {
  int rc = 0;
  for (int i = 1; i < argc; ++i) {
    std::vector<std::uint8_t> bytes;
    if (!feed::read_file(argv[i], bytes)) {
      std::fprintf(stderr, "cannot read %s\n", argv[i]);
      rc = 1;
      continue;
    }
    LLVMFuzzerTestOneInput(bytes.data(), bytes.size());
  }
  std::printf("ran %d input(s)\n", argc - 1);
  return rc;
}
#endif
