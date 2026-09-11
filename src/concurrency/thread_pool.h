#pragma once

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

namespace dge {

class ThreadPool {
 public:
  ThreadPool(size_t threads, size_t queue_cap, bool pin, int first_core,
             std::function<void(size_t)> worker_init = {});
  ~ThreadPool();
  ThreadPool(const ThreadPool&) = delete;
  ThreadPool& operator=(const ThreadPool&) = delete;

  bool Submit(std::function<void()> fn);

  void SubmitInternal(std::function<void()> fn);

  void Shutdown();

  size_t queue_size() const { return approx_size_.load(std::memory_order_relaxed); }
  size_t thread_count() const { return threads_.size(); }

 private:
  void WorkerLoop(size_t idx);

  std::deque<std::function<void()>> queue_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::atomic<uint64_t> approx_size_{0};
  size_t queue_cap_;
  bool stop_ = false;
  std::vector<std::thread> threads_;
};

}  // namespace dge
