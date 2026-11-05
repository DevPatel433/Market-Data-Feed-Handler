cmake_minimum_required(VERSION 3.20)
project(market_data_feed_handler VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

option(FEED_WERROR "Treat warnings as errors" ON)
option(FEED_BUILD_BENCHMARKS "Build benchmarks (not run by ctest)" ON)
option(FEED_FUZZ "Build the libFuzzer decoder target (Clang only; use with FEED_SANITIZE=address,undefined)" OFF)
set(FEED_SANITIZE "" CACHE STRING "Sanitizers, e.g. address,undefined or thread (do not mix thread with address)")

# ---- shared compile options (applied to every target via an interface lib) ----
add_library(feed_options INTERFACE)
target_compile_features(feed_options INTERFACE cxx_std_20)
target_compile_options(feed_options INTERFACE
  -Wall -Wextra -Wpedantic -Wshadow -Wconversion -Wsign-conversion)
if(FEED_WERROR)
  target_compile_options(feed_options INTERFACE -Werror)
endif()
if(FEED_SANITIZE)
  target_compile_options(feed_options INTERFACE -fsanitize=${FEED_SANITIZE} -fno-omit-frame-pointer -fno-sanitize-recover=undefined)
  target_link_options(feed_options INTERFACE -fsanitize=${FEED_SANITIZE})
endif()

find_package(Threads REQUIRED)

# ---- core library: protocol, replay file, workload generation, replayer ----
add_library(feed_core STATIC
  src/protocol.cpp
  src/decoder.cpp
  src/replay_file.cpp
  src/workload.cpp
  src/replayer.cpp
  src/sequencer.cpp
  src/ingest.cpp
  src/fault_injector.cpp
  src/pipeline.cpp
  src/udp_socket.cpp
  src/udp_sender.cpp
  src/udp_receiver.cpp
  src/book_state.cpp
  src/recovery.cpp
  src/overload_model.cpp)
target_include_directories(feed_core PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(feed_core PUBLIC feed_options Threads::Threads)
if(FEED_FUZZ)
  if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    message(FATAL_ERROR "FEED_FUZZ requires Clang (libFuzzer)")
  endif()
  # Coverage instrumentation for the code under test; needs the ASan runtime (set FEED_SANITIZE).
  target_compile_options(feed_core PRIVATE -fsanitize=fuzzer-no-link)
endif()

# ---- executables ----
add_executable(feed_gen_events tools/gen_events.cpp)
target_link_libraries(feed_gen_events PRIVATE feed_core)

add_executable(feed_fault_inject tools/fault_inject.cpp)
target_link_libraries(feed_fault_inject PRIVATE feed_core)

add_executable(feed_replay src/replay_main.cpp)
target_link_libraries(feed_replay PRIVATE feed_core)

add_executable(feed_pipeline src/pipeline_main.cpp)
target_include_directories(feed_pipeline PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(feed_pipeline PRIVATE feed_core)

add_executable(feed_overload src/overload_main.cpp)
target_link_libraries(feed_overload PRIVATE feed_core)

add_executable(feed_udp_send src/udp_send_main.cpp)
target_link_libraries(feed_udp_send PRIVATE feed_core)

add_executable(feed_udp_recv src/udp_recv_main.cpp)
target_include_directories(feed_udp_recv PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(feed_udp_recv PRIVATE feed_core)

if(FEED_BUILD_BENCHMARKS)
  add_executable(feed_bench_spsc benchmarks/bench_spsc_layout.cpp)
  target_link_libraries(feed_bench_spsc PRIVATE feed_core)
endif()

# ---- fuzzing / crash reproduction ----
# Always built: runs files through the same oracle as the fuzzer (no libFuzzer needed).
add_executable(feed_fuzz_replay tools/fuzz_decoder.cpp)
target_compile_definitions(feed_fuzz_replay PRIVATE FEED_FUZZ_STANDALONE)
target_link_libraries(feed_fuzz_replay PRIVATE feed_core)

if(FEED_FUZZ)
  add_executable(feed_fuzz_decoder tools/fuzz_decoder.cpp)
  target_compile_options(feed_fuzz_decoder PRIVATE -fsanitize=fuzzer)
  target_link_options(feed_fuzz_decoder PRIVATE -fsanitize=fuzzer)
  target_link_libraries(feed_fuzz_decoder PRIVATE feed_core)
endif()

# ---- tests (dependency-free harness; see tests/check.hpp) ----
enable_testing()
add_executable(feed_tests
  tests/test_main.cpp
  tests/test_protocol.cpp
  tests/test_replay_file.cpp
  tests/test_workload.cpp
  tests/test_decoder.cpp
  tests/test_sequencer.cpp
  tests/test_ingest.cpp
  tests/test_spsc_queue.cpp
  tests/test_pipeline.cpp
  tests/test_udp.cpp
  tests/test_book_state.cpp
  tests/test_recovery.cpp
  tests/test_overload_model.cpp
  tests/test_overload_pipeline.cpp)
target_include_directories(feed_tests PRIVATE ${PROJECT_SOURCE_DIR}/tests)
target_link_libraries(feed_tests PRIVATE feed_core)
add_test(NAME unit COMMAND feed_tests)

add_test(NAME smoke_corpus
  COMMAND sh ${PROJECT_SOURCE_DIR}/tests/smoke_corpus.sh $<TARGET_FILE:feed_gen_events> $<TARGET_FILE:feed_fuzz_replay>)

add_test(NAME smoke_recovery
  COMMAND sh ${PROJECT_SOURCE_DIR}/tests/smoke_recovery.sh
          $<TARGET_FILE:feed_gen_events> $<TARGET_FILE:feed_fault_inject> $<TARGET_FILE:feed_pipeline>
          $<TARGET_FILE:feed_udp_send> $<TARGET_FILE:feed_udp_recv>)

add_test(NAME smoke_overload
  COMMAND sh ${PROJECT_SOURCE_DIR}/tests/smoke_overload.sh $<TARGET_FILE:feed_overload>)

add_test(NAME smoke_udp
  COMMAND sh ${PROJECT_SOURCE_DIR}/tests/smoke_udp.sh
          $<TARGET_FILE:feed_gen_events> $<TARGET_FILE:feed_fault_inject>
          $<TARGET_FILE:feed_udp_send> $<TARGET_FILE:feed_udp_recv>)

add_test(NAME smoke_pipeline
  COMMAND sh ${PROJECT_SOURCE_DIR}/tests/smoke_pipeline.sh
          $<TARGET_FILE:feed_gen_events> $<TARGET_FILE:feed_fault_inject> $<TARGET_FILE:feed_pipeline>)

add_test(NAME smoke_faults
  COMMAND sh ${PROJECT_SOURCE_DIR}/tests/smoke_faults.sh
          $<TARGET_FILE:feed_gen_events> $<TARGET_FILE:feed_fault_inject> $<TARGET_FILE:feed_replay>)

add_test(NAME smoke_replay
  COMMAND sh ${PROJECT_SOURCE_DIR}/tests/smoke_replay.sh $<TARGET_FILE:feed_gen_events> $<TARGET_FILE:feed_replay>)
