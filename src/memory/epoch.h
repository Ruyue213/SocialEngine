#pragma once

#include <atomic>
#include <cstdint>
#include <cstddef>
#include <functional>
#include <mutex>
#include <vector>

namespace dge {

class SlabPool;

// Epoch-Based Reclamation(EBR)回收器。
//
// 协议约定(修复后):
// 1. 读者:Pin() 取得 ticket 并登记进入时的全局 epoch;读取已发布对象;
//    Unpin(ticket) 退出保护区。Pin 返回后才能解引用受保护指针。
// 2. 写者:先原子发布新版本指针,再 Retire() 旧对象。因此任何持有旧对象
//    指针的读者,其登记 epoch 必然 <= Retire 时该对象所在桶的 epoch。
// 3. Sweep():回收条件是「所有活跃读者登记 epoch > 桶 epoch」,而不是
//    「全局 epoch 已前进」。无活跃读者时才允许 epoch 跳跃加速回收。
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
  uint64_t freed_total() const { return freed_total_.load(std::memory_order_relaxed); }

 private:
  EpochReclaimer() = default;

  static constexpr int kBuckets = 64;
  static constexpr int kMaxTickets = 16384;

  struct RetireEntry {
    void* ptr;
    size_t size;
    SlabPool* pool;
  };

  static void FreedBatch(std::vector<RetireEntry>& entries);
  void RegisterSlot(uint32_t slot);

  std::atomic<uint64_t> g_epoch_{1};
  std::mutex retire_mu_;
  std::vector<RetireEntry> retire_lists_[kBuckets];
  uint64_t bucket_epoch_[kBuckets] = {0};
  std::atomic<uint64_t> pending_{0};
  std::atomic<uint64_t> freed_total_{0};
  std::mutex sweep_mu_;

  std::atomic<uint64_t> active_epochs_[kMaxTickets];
  std::atomic<uint64_t> ticket_head_{0};
  std::atomic<uint32_t> slot_next_[kMaxTickets];
  std::atomic<uint32_t> free_head_{0};  // 0 = 空, 否则 1-based slot
  std::atomic<uint32_t> ticket_high_water_{0};
  std::atomic<int64_t> reader_count_{0};
};

}  // namespace dge
