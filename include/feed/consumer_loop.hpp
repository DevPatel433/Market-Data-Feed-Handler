#pragma once
// The consumer side of the shutdown protocol, shared by the pipeline and the tests.
//
// Producer: push everything, THEN done.store(true, release).
// Consumer: pop until empty; when empty, check `done` (acquire); if set, drain once more and stop.
//
// The second drain is essential. Without it there is a window: the consumer sees the queue empty,
// the producer then pushes its last events and sets `done`, the consumer reads `done == true` and
// exits with events still queued. `after_empty_poll` is a test seam that runs inside exactly that
// window so the race can be reproduced deterministically instead of by luck.
#include <atomic>
#include <cstdint>
#include <thread>
#include <utility>

namespace feed {

template <class Queue, class Handler, class Hook>
void consume_until_done(Queue& q, const std::atomic<bool>& done, Handler&& handle, Hook&& after_empty_poll) {
  typename Queue::value_type ev;
  std::uint32_t idle = 0;
  for (;;) {
    if (q.try_pop(ev)) {
      idle = 0;
      handle(ev);
      continue;
    }
    after_empty_poll();  // production code passes a no-op
    if (done.load(std::memory_order_acquire)) {
      while (q.try_pop(ev)) handle(ev);  // everything pushed before `done` is visible: drain, then stop
      return;
    }
    if (++idle >= 16) std::this_thread::yield();
  }
}

template <class Queue, class Handler>
void consume_until_done(Queue& q, const std::atomic<bool>& done, Handler&& handle) {
  consume_until_done(q, done, std::forward<Handler>(handle), [] {});
}

}  // namespace feed
