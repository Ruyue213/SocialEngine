#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

namespace dge {

class TimerHeap {
 public:
  TimerHeap();
  ~TimerHeap();
  TimerHeap(const TimerHeap&) = delete;
  TimerHeap& operator=(const TimerHeap&) = delete;

  void Add(uint64_t delay_ms, std::function<void()> fn);

  void Clear();

  void Shutdown();

  uint64_t pending() const { return pending_.load(std::memory_order_relaxed); }

 private:
  struct Entry {
    uint64_t due_ms;
    uint64_t seq;
    std::shared_ptr<std::function<void()>> fn;
    bool operator>(const Entry& o) const {
      if (due_ms != o.due_ms) {
        return due_ms > o.due_ms;
      }
      return seq > o.seq;
    }
  };

  void RunLoop();

  std::priority_queue<Entry, std::vector<Entry>, std::greater<Entry>> heap_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::atomic<uint64_t> seq_{0};
  std::atomic<uint64_t> pending_{0};
  bool stop_ = false;
  std::thread thread_;
};

}  // namespace dge
