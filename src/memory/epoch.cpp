#include "memory/epoch.h"

#include <chrono>
#include <cstdlib>
#include <thread>

#include "memory/slab_pool.h"

namespace dge {

EpochReclaimer& EpochReclaimer::Instance() {
  static EpochReclaimer r;
  return r;
}

uint32_t EpochReclaimer::Pin() {
  while (true) {
    uint32_t head = free_head_.load(std::memory_order_acquire);
    if (head != 0) {
      uint32_t slot = head - 1;
      uint32_t next = slot_next_[slot].load(std::memory_order_relaxed);
      if (free_head_.compare_exchange_weak(head, next, std::memory_order_acquire)) {
        // seq_cst 读全局 epoch + release 写 slot:Sweep 端要么在本次登记
        // 写入前完成 ticket 扫描(该读者晚于本轮回收决策开始,不会持有
        // 本轮回收对象),要么在扫描中观察到本登记。
        active_epochs_[slot].store(g_epoch_.load(std::memory_order_seq_cst),
                                   std::memory_order_release);
        reader_count_.fetch_add(1, std::memory_order_relaxed);
        return slot + 1;
      }
      continue;
    }
    uint32_t slot = ticket_head_.fetch_add(1, std::memory_order_relaxed);
    if (slot < kMaxTickets) {
      uint32_t high = slot + 1;
      uint32_t prev = ticket_high_water_.load(std::memory_order_relaxed);
      while (prev < high &&
             !ticket_high_water_.compare_exchange_weak(prev, high, std::memory_order_acq_rel)) {
      }
      active_epochs_[slot].store(g_epoch_.load(std::memory_order_seq_cst),
                                 std::memory_order_release);
      reader_count_.fetch_add(1, std::memory_order_relaxed);
      return slot + 1;
    }
    // ticket 耗尽:让出 CPU 等待其他读者 Unpin 归还 slot。
    std::this_thread::yield();
  }
}

void EpochReclaimer::Unpin(uint32_t ticket) {
  if (ticket == 0 || ticket > kMaxTickets) {
    return;
  }
  uint32_t slot = ticket - 1;
  active_epochs_[slot].store(0, std::memory_order_release);
  reader_count_.fetch_sub(1, std::memory_order_relaxed);
  while (true) {
    uint32_t head = free_head_.load(std::memory_order_relaxed);
    slot_next_[slot].store(head, std::memory_order_relaxed);
    if (free_head_.compare_exchange_weak(head, ticket, std::memory_order_release)) {
      return;
    }
  }
}

bool EpochReclaimer::ReadersActive() const {
  return reader_count_.load(std::memory_order_acquire) > 0;
}

void EpochReclaimer::Retire(void* p, size_t size, SlabPool* pool) {
  uint64_t e = g_epoch_.load(std::memory_order_seq_cst);
  std::lock_guard<std::mutex> lk(retire_mu_);
  retire_lists_[e % kBuckets].push_back({p, size, pool});
  // 无条件用当前(更大的)epoch 覆盖桶标记:桶被复用时,旧对象挂到
  // 更大标记下,回收条件 be < min_active 更难满足 → 只会更保守。
  // 若保留旧小值,新 Retire 的对象会以过小的标记参与回收判断,
  // 导致持有新对象的活跃读者尚未退出就被回收。
  bucket_epoch_[e % kBuckets] = e;
  pending_.fetch_add(1, std::memory_order_relaxed);
}

void EpochReclaimer::DrainPool(SlabPool* pool) {
  std::lock_guard<std::mutex> lk(retire_mu_);
  for (int b = 0; b < kBuckets; ++b) {
    auto& list = retire_lists_[b];
    auto it = list.begin();
    while (it != list.end()) {
      if (it->pool == pool) {
        pool->Deallocate(it->ptr, it->size);
        pending_.fetch_sub(1, std::memory_order_relaxed);
        freed_total_.fetch_add(1, std::memory_order_relaxed);
        it = list.erase(it);
      } else {
        ++it;
      }
    }
    if (list.empty()) {
      bucket_epoch_[b] = 0;
    }
  }
}

void EpochReclaimer::Sweep() {
  // 串行化 Sweep:并发 Sweep 对 ticket 的两次扫描之间读者可能进出,
  // 会导致 min_active 计算基于过期快照。
  std::lock_guard<std::mutex> sweep_lk(sweep_mu_);
  if (getenv("DGE_NO_RECLAIM") != nullptr) {
    g_epoch_.fetch_add(1, std::memory_order_seq_cst);
    return;
  }
  uint64_t g = g_epoch_.load(std::memory_order_seq_cst);
  // 修复:min_active 初始化为 UINT64_MAX 表示「无读者」哨兵。
  // 原实现初始化为当前 epoch g,活跃读者恰好登记在 g 时被
  // `v < min_active` 条件忽略,导致同 epoch 读者持有对象被提前回收。
  uint64_t min_active = UINT64_MAX;
  bool has_readers = false;
  uint32_t high = ticket_high_water_.load(std::memory_order_acquire);
  for (uint32_t i = 0; i < high; ++i) {
    uint64_t v = active_epochs_[i].load(std::memory_order_acquire);
    if (v != 0) {
      has_readers = true;
      if (v < min_active) {
        min_active = v;
      }
    }
  }

  if (!has_readers) {
    // 无读者:所有桶都可安全回收,并允许 epoch 跳跃加速后续回收。
    uint64_t max_retire = 0;
    for (int b = 0; b < kBuckets; ++b) {
      std::lock_guard<std::mutex> lk(retire_mu_);
      if (bucket_epoch_[b] > max_retire) {
        max_retire = bucket_epoch_[b];
      }
    }
    for (int b = 0; b < kBuckets; ++b) {
      std::vector<RetireEntry> batch;
      {
        std::lock_guard<std::mutex> lk(retire_mu_);
        batch.swap(retire_lists_[b]);
        bucket_epoch_[b] = 0;
      }
      for (auto& e : batch) {
        if (e.pool != nullptr) {
          e.pool->Deallocate(e.ptr, e.size);
        } else {
          std::free(e.ptr);
        }
        pending_.fetch_sub(1, std::memory_order_relaxed);
        freed_total_.fetch_add(1, std::memory_order_relaxed);
      }
    }
    if (max_retire > 0) {
      g_epoch_.store(max_retire + 2, std::memory_order_seq_cst);
    }
    return;
  }

  // 有读者:只回收 bucket_epoch < min_active 的桶。
  // 安全性:持有桶内对象的读者必然在 Retire 前完成 Pin,其登记
  // epoch <= bucket_epoch < min_active,与「活跃读者 epoch > bucket_epoch」
  // 矛盾,故不存在这样的活跃读者。
  for (int b = 0; b < kBuckets; ++b) {
    uint64_t be = 0;
    {
      std::lock_guard<std::mutex> lk(retire_mu_);
      be = bucket_epoch_[b];
    }
    if (be == 0 || be >= min_active) {
      continue;
    }
    std::vector<RetireEntry> batch;
    {
      std::lock_guard<std::mutex> lk(retire_mu_);
      batch.swap(retire_lists_[b]);
      bucket_epoch_[b] = 0;
    }
    for (auto& e : batch) {
      if (e.pool != nullptr) {
        e.pool->Deallocate(e.ptr, e.size);
      } else {
        std::free(e.ptr);
      }
      pending_.fetch_sub(1, std::memory_order_relaxed);
      freed_total_.fetch_add(1, std::memory_order_relaxed);
    }
  }
  // 有读者时 epoch 只做保守推进:桶 epoch 被更大值覆盖只会让回收
  // 更保守,不会越过活跃读者。
  (void)g;
  g_epoch_.fetch_add(1, std::memory_order_seq_cst);
}

}  // namespace dge
