#include "memory/epoch.h"

#include <cstdlib>

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
      uint32_t next = slot_next_[slot];
      if (free_head_.compare_exchange_weak(head, next, std::memory_order_acquire)) {
        active_epochs_[slot].store(g_epoch_.load(std::memory_order_seq_cst),
                                   std::memory_order_relaxed);
        reader_count_.fetch_add(1, std::memory_order_relaxed);
        return slot + 1;
      }
      continue;
    }
    uint32_t slot = ticket_head_.fetch_add(1, std::memory_order_relaxed);
    if (slot < kMaxTickets) {
      if (slot + 1 > ticket_high_water_) {
        ticket_high_water_ = slot + 1;
      }
      active_epochs_[slot].store(g_epoch_.load(std::memory_order_seq_cst),
                                 std::memory_order_relaxed);
      reader_count_.fetch_add(1, std::memory_order_relaxed);
      return slot + 1;
    }
  }
}

void EpochReclaimer::Unpin(uint32_t ticket) {
  if (ticket == 0 || ticket > kMaxTickets) {
    return;
  }
  uint32_t slot = ticket - 1;
  active_epochs_[slot].store(0, std::memory_order_relaxed);
  reader_count_.fetch_sub(1, std::memory_order_relaxed);
  while (true) {
    uint32_t head = free_head_.load(std::memory_order_relaxed);
    slot_next_[slot] = head;
    if (free_head_.compare_exchange_weak(head, ticket, std::memory_order_relaxed)) {
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
  if (getenv("DGE_NO_RECLAIM") != nullptr) {
    g_epoch_.fetch_add(1, std::memory_order_seq_cst);
    return;
  }
  uint64_t g = g_epoch_.load(std::memory_order_seq_cst);
  uint64_t min_active = g;
  uint64_t max_retire = 0;
  for (uint32_t i = 0; i < ticket_high_water_; ++i) {
    uint64_t v = active_epochs_[i].load(std::memory_order_relaxed);
    if (v != 0 && v < min_active) {
      min_active = v;
    }
  }
  {
    std::lock_guard<std::mutex> lk(retire_mu_);
    for (int b = 0; b < kBuckets; ++b) {
      if (bucket_epoch_[b] > max_retire) {
        max_retire = bucket_epoch_[b];
      }
    }
  }
  if (min_active == g && max_retire > 0) {
    g_epoch_.store(max_retire + 2, std::memory_order_seq_cst);
    min_active = max_retire + 2;
  }
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
    }
  }
  if (min_active == g) {
    g_epoch_.fetch_add(1, std::memory_order_seq_cst);
  }
}

}  // namespace dge
