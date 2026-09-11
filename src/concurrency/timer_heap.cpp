#include "concurrency/timer_heap.h"

#include "common/affinity.h"

namespace dge {

TimerHeap::TimerHeap() {
  thread_ = std::thread([this]() {
    set_thread_low_priority();
    RunLoop();
  });
}

TimerHeap::~TimerHeap() { Shutdown(); }

void TimerHeap::Add(uint64_t delay_ms, std::function<void()> fn) {
  uint64_t now = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
  Entry e;
  e.due_ms = now + delay_ms;
  e.seq = seq_.fetch_add(1, std::memory_order_relaxed);
  e.fn = std::make_shared<std::function<void()>>(std::move(fn));
  {
    std::lock_guard<std::mutex> lk(mu_);
    heap_.push(std::move(e));
    pending_.fetch_add(1, std::memory_order_relaxed);
  }
  cv_.notify_one();
}

void TimerHeap::RunLoop() {
  while (true) {
    std::shared_ptr<std::function<void()>> fn;
    {
      std::unique_lock<std::mutex> lk(mu_);
      if (stop_) {
        return;
      }
      if (heap_.empty()) {
        cv_.wait(lk, [this] { return stop_ || !heap_.empty(); });
        if (stop_) {
          return;
        }
      }
      auto now = std::chrono::steady_clock::now();
      auto due = std::chrono::time_point<std::chrono::steady_clock>(
          std::chrono::milliseconds(heap_.top().due_ms));
      if (due > now) {
        cv_.wait_until(lk, due, [this] { return stop_; });
        if (stop_) {
          return;
        }
        continue;
      }
      fn = heap_.top().fn;
      heap_.pop();
      pending_.fetch_sub(1, std::memory_order_relaxed);
    }
    (*fn)();
  }
}

void TimerHeap::Clear() {
  std::lock_guard<std::mutex> lk(mu_);
  std::priority_queue<Entry, std::vector<Entry>, std::greater<Entry>> empty;
  heap_.swap(empty);
  pending_.store(0, std::memory_order_relaxed);
}

void TimerHeap::Shutdown() {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (stop_) {
      return;
    }
    stop_ = true;
  }
  cv_.notify_all();
  if (thread_.joinable()) {
    thread_.join();
  }
}

}  // namespace dge
