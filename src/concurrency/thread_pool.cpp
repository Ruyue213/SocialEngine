#include "concurrency/thread_pool.h"

#include "common/affinity.h"

namespace dge {

ThreadPool::ThreadPool(size_t threads, size_t queue_cap, bool pin, int first_core,
                       std::function<void(size_t)> worker_init)
    : queue_cap_(queue_cap) {
  threads_.reserve(threads);
  for (size_t i = 0; i < threads; ++i) {
    threads_.emplace_back([this, i, pin, first_core, worker_init]() {
      if (pin) {
        pin_thread_to_core(static_cast<int>(first_core + i));
      }
      if (worker_init) {
        worker_init(i);
      }
      WorkerLoop(i);
    });
  }
}

ThreadPool::~ThreadPool() { Shutdown(); }

bool ThreadPool::Submit(std::function<void()> fn) {
  if (approx_size_.load(std::memory_order_relaxed) >= queue_cap_) {
    return false;
  }
  std::unique_lock<std::mutex> lk(mu_, std::try_to_lock);
  if (!lk.owns_lock()) {
    return false;
  }
  queue_.push_back(std::move(fn));
  approx_size_.fetch_add(1, std::memory_order_relaxed);
  cv_.notify_one();
  return true;
}

void ThreadPool::SubmitInternal(std::function<void()> fn) {
  {
    std::lock_guard<std::mutex> lk(mu_);
    queue_.push_back(std::move(fn));
    approx_size_.fetch_add(1, std::memory_order_relaxed);
  }
  cv_.notify_one();
}

void ThreadPool::WorkerLoop(size_t) {
  while (true) {
    std::function<void()> fn;
    {
      std::unique_lock<std::mutex> lk(mu_);
      cv_.wait(lk, [this] { return stop_ || !queue_.empty(); });
      if (stop_ && queue_.empty()) {
        return;
      }
      fn = std::move(queue_.front());
      queue_.pop_front();
      approx_size_.fetch_sub(1, std::memory_order_relaxed);
    }
    fn();
  }
}

void ThreadPool::Shutdown() {
  {
    std::lock_guard<std::mutex> lk(mu_);
    if (stop_) {
      return;
    }
    stop_ = true;
  }
  cv_.notify_all();
  for (auto& t : threads_) {
    if (t.joinable()) {
      t.join();
    }
  }
  threads_.clear();
}

}  // namespace dge
