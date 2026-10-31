# Decoder (Phase 1)

`feed::decode(std::span<const uint8_t> frame, Message& out) noexcept` in `include/feed/decoder.hpp`,
`src/decoder.cpp`. It implements the contract in `protocol.md` §6 and knows nothing about sockets,
files or queues.

## Guarantees

1. **Memory safe.** Size checks (`too_short`, `too_large`) run before any read; all reads go through the bounds-checked `ByteReader`. No pointer casts, no unaligned loads.
2. **No side effects.** No allocation, no exceptions, no global state, no logging. Safe to call from any thread.
3. **Canonical only.** `None` is returned only if `encode(out)` reproduces the input byte for byte. There is exactly one valid byte string per message, so trailing bytes, a lying `payload_len`, out-of-range enums etc. can never slip through as "mostly valid".
4. **Single, ordered reason.** Rejects carry exactly one `DecodeError`, in the order: `too_short`, `too_large`, `bad_version`, `unknown_type`, `length_mismatch`, `bad_field`.
5. **`out` is untouched on error**, so a caller cannot accidentally use half-decoded data.

Field ranges enforced (`bad_field`): `seq >= 1`; `order_id`/`trade_id` non-zero; `price > 0`; `qty >= 1`; `side`/`aggressor` in {0,1}. `stream_id` and `instrument_id` are unrestricted at this layer; whether a stream or instrument is *known* is a policy question for later stages.

## Test strategy (`tests/test_decoder.cpp`)

| Technique | What it establishes |
|-----------|--------------------|
| Round trip: one of each type, boundary values (max `u64`, `INT64_MAX`, ...), 50k random valid messages | `decode(encode(m)) == m` |
| One constructed case per reject reason and per field range | every documented error is reachable and correctly attributed |
| Precedence tests | the documented check order |
| Every truncation and every extension (up to 2x max size) of each frame type | exact error for cut/extended frames |
| Exhaustive single-byte mutation (every position x all 256 values, each type) | no single corrupted byte is mishandled |
| 200k random multi-step mutations (flip, set, insert, delete, truncate, extend) | invariant holds under compound corruption; run asserts all 7 outcomes were exercised |
| 100k pure-random buffers | no crash on garbage |
| Generator cross-check | for every workload profile, rejects == manifest `malformed` (0 for non-malformed profiles) |

The invariant checked on every fuzz-style input: success implies re-encoding gives identical bytes; failure implies `out` unchanged. Inputs are copied into **exactly-sized heap allocations** so AddressSanitizer catches any read past the end (a `std::vector`'s spare capacity would hide one). Everything runs under ASan+UBSan in CI.

### Does the suite actually catch bugs?

I planted bugs in a copy of the decoder and re-ran the suite. All were caught: dropping the `qty`, `seq` or version checks; `price < 0` instead of `<= 0`; allowing `side == 2`; an off-by-one in the size limit; skipping the frame-length check; and writing to `out` on the error path. One mutant survives by design: removing the final `r.ok()` check, which is documented as unreachable because the length checks run first. Removing the minimum-size check is caught by wrong-error assertions rather than by ASan, because `ByteReader` independently refuses to read past the end (two layers of protection).

## Fuzzing

`tools/fuzz_decoder.cpp` is a libFuzzer target with the same oracle as the unit tests. Build and run instructions are in the README; CI runs it for 60 s per push, seeded from generated traffic (`scripts/make_fuzz_corpus.sh`). The deterministic mutation tests above run everywhere, including compilers without libFuzzer. The CTest `smoke_corpus` test pushes exported frames through the same oracle.

## Not in scope here

Sequence tracking (Phase 2), what a rejected frame does to the expected sequence (Phase 2), and decode throughput measurement (Phase 6; the decoder is written to be allocation-free, but no speed claim is made without benchmarks).
