#pragma once

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <functional>
#include <mutex>
#include <vector>

namespace dge {

class SlabPool;

class EpochReclaimer {
 public:
  static EpochReclaimer& Instance();

  uint32_t Pin();
  void Unpin(uint32_t ticket);

  bool ReadersActive() const;

  void Retire(void* p, size_t size, SlabPool* pool);

  void Sweep();

  void DrainPool(SlabPool* pool);

  uint64_t epoch() const { return g_epoch_.load(std::memory_order_relaxed); }
  uint64_t retired_pending() const { return pending_.load(std::memory_order_relaxed); }

 private:
  EpochReclaimer() = default;

  static constexpr int kBuckets = 64;
  static constexpr int kMaxTickets = 16384;

  struct RetireEntry {
    void* ptr;
    size_t size;
    SlabPool* pool;
  };

  std::atomic<uint64_t> g_epoch_{1};
  std::mutex retire_mu_;
  std::vector<RetireEntry> retire_lists_[kBuckets];
  uint64_t bucket_epoch_[kBuckets] = {0};
  std::atomic<uint64_t> pending_{0};

  std::atomic<uint64_t> active_epochs_[kMaxTickets];
  std::atomic<uint32_t> ticket_head_{0};
  uint32_t slot_next_[kMaxTickets];
  std::atomic<uint32_t> free_head_{0};  // 0 = 空, 否则 1-based slot
  uint32_t ticket_high_water_ = 0;
  std::atomic<int64_t> reader_count_{0};
};

}  // namespace dge
