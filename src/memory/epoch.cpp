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

// =============================================================================
// 协议证明(2026-10 第二轮审查后重写,C++ 内存模型级)
//
// 参与方与原语:
//   Reader(Pin):   (1) e = g.load(seq_cst)      (2) slot.store(e, seq_cst)
//                  (3) e2 = g.load(seq_cst) 双检,若 e2 != e 回到 (1)
//   Writer(Retire): (W1) adj.store(nd, release)  (W2) e_r = g.load(seq_cst)
//                  (W3) mutex 内入桶
//   Sweeper(Sweep): (S1) g.fetch_add(1, seq_cst) (S2) slot.load(seq_cst) 扫描
//                  (S3) 回收 be < min_active 的桶;全零时回收全部并跳跃
//
// g 与 slot 的全部读写均为 seq_cst,故 SC 全序与二者的 modification
// order 一致,且 seq_cst store 蕴含 release、seq_cst load 蕴含 acquire。
//
// 定理 A(登记可见性,Dekker 双向确认):
//   若 S2 未观察到 R 的登记(读到 0),则 SC 序上 S2 <sc R 的 slot.store。
//   又 R 的程序序给出 slot.store <sc (3),而 (3) 若读到 S1 之后的值则
//   双检失败、R 重登记——(3) 通过意味着读到 S1 之前的值,即 (3) <sc S1。
//   汇总:S1 <sc S2(程序序),S2 <sc slot.store(假设),(3) <sc S1,
//   slot.store <sc (3)(程序序)——成环,矛盾。
//   故「S2 读到 0」与「R 双检通过」不可能同时成立:要么 S 看到 R,
//   要么 R 重登记且新登记 epoch ≥ S1 写入值。
//
// 定理 B(持有对象的读者不被回收):
//   回收条件 be < min_active 中,min_active 取自被观察到的登记;
//   未被观察到的登记由定理 A 排除。设 R 被观察到且登记 e_R:
//   (i) 若 R 的稳定双检值 e_R > be:R 进入保护区前 g 已推进到
//       e_R > be,而 Writer 在 Retire(X)(be 时刻)之前已执行 W1
//       (adj.store(nd, release)),W1 hb W2(seq_cst) <sc R 的 g.load
//       (值序)hb R 随后的 adj.load(acquire)——release/acquire 配对使
//       R 必读到 nd 或更新版本,不持有 X。
//   (ii) 若 e_R <= be:回收条件 be < min_active <= e_R <= be 不成立,
//       桶不被回收。
//   两种情形下 UAF 均不可能成立。
//
// 定理 C(全零快速路径安全):
//   S2 观察到全部 slot 为 0 时,由定理 A,任何正在 Pin 的读者都将
//   以 ≥ S1 写入值的 epoch 重登记,且对 S1 之前 Retire 的对象适用
//   情形 (i)(不持有)。故本轮回收全部桶安全;跳跃写(max_retire+2)
//   发生在 S2 之后,不影响本轮论证,仅影响后续轮次(桶 epoch 覆盖
//   只会更保守)。
//
// 已知代价:Pin 增加一次 seq_cst load 与一次 seq_cst store(双检),
// x86 上约为一次额外 XCHG;收益是协议可在抽象机上证明,而不依赖
// 真实硬件的 per-location coherence(x86 TSO / ARM cache 一致性)
// 对理论交错的掩盖。
// =============================================================================

uint32_t EpochReclaimer::Pin() {
  while (true) {
    uint32_t head = free_head_.load(std::memory_order_acquire);
    if (head != 0) {
      uint32_t slot = head - 1;
      uint32_t next = slot_next_[slot].load(std::memory_order_relaxed);
      if (free_head_.compare_exchange_weak(head, next, std::memory_order_acquire)) {
        RegisterSlot(slot);
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
      RegisterSlot(slot);
      reader_count_.fetch_add(1, std::memory_order_relaxed);
      return slot + 1;
    }
    // ticket 耗尽:让出 CPU 等待其他读者 Unpin 归还 slot。
    std::this_thread::yield();
  }
}

void EpochReclaimer::RegisterSlot(uint32_t slot) {
  // Dekker 双检登记:登记与全局 epoch 推进(见 Sweep 的 S1)构成
  // 双向确认。循环通常一轮通过;仅当与 Sweep 的推进并发时重登记。
  uint64_t e;
  do {
    e = g_epoch_.load(std::memory_order_seq_cst);
    active_epochs_[slot].store(e, std::memory_order_seq_cst);
  } while (g_epoch_.load(std::memory_order_seq_cst) != e);
}

void EpochReclaimer::Unpin(uint32_t ticket) {
  if (ticket == 0 || ticket > kMaxTickets) {
    return;
  }
  uint32_t slot = ticket - 1;
  active_epochs_[slot].store(0, std::memory_order_seq_cst);
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
  // seq_cst 读:参与定理 B 的值序论证(与读者的 seq_cst 读可比序)。
  uint64_t e = g_epoch_.load(std::memory_order_seq_cst);
  std::lock_guard<std::mutex> lk(retire_mu_);
  retire_lists_[e % kBuckets].push_back({p, size, pool});
  // 无条件用当前(更大的)epoch 覆盖桶标记:桶被复用时,旧对象挂到
  // 更大标记下,回收条件更难满足 → 只会更保守。
  bucket_epoch_[e % kBuckets] = e;
  pending_.fetch_add(1, std::memory_order_relaxed);
}

void EpochReclaimer::DrainPool(SlabPool* pool) {
  std::vector<std::pair<void*, size_t>> batch;
  {
    std::lock_guard<std::mutex> lk(retire_mu_);
    for (int b = 0; b < kBuckets; ++b) {
      auto& list = retire_lists_[b];
      auto it = list.begin();
      while (it != list.end()) {
        if (it->pool == pool) {
          batch.emplace_back(it->ptr, it->size);
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
  // 锁外批量归还:每档一次锁交接。
  if (pool != nullptr) {
    pool->DeallocateBatch(batch);
  } else {
    for (auto& [p, sz] : batch) {
      (void)sz;
      std::free(p);
    }
  }
}

void EpochReclaimer::FreedBatch(std::vector<RetireEntry>& entries) {
  if (entries.empty()) {
    return;
  }
  Instance().freed_total_.fetch_add(static_cast<uint64_t>(entries.size()),
                                    std::memory_order_relaxed);
  // 按 pool 分组后批量归还:回收风暴下逐个 Deallocate 的锁交接
  // 与分配热路径构成显著竞争(profiling 实测)。
  std::unordered_map<SlabPool*, std::vector<std::pair<void*, size_t>>> by_pool;
  for (auto& e : entries) {
    if (e.pool != nullptr) {
      by_pool[e.pool].emplace_back(e.ptr, e.size);
    } else {
      std::free(e.ptr);
    }
  }
  for (auto& [pool, items] : by_pool) {
    pool->DeallocateBatch(items);
  }
}

void EpochReclaimer::Sweep() {
  // 串行化 Sweep:并发 Sweep 的 min_active 快照与跳跃写会互相破坏
  // 定理 A 的单写者前提。
  std::lock_guard<std::mutex> sweep_lk(sweep_mu_);
  if (getenv("DGE_NO_RECLAIM") != nullptr) {
    g_epoch_.fetch_add(1, std::memory_order_seq_cst);
    return;
  }
  // S1:先推进 epoch(seq_cst),再扫描登记——顺序是定理 A 的前提,
  // 不可交换。
  uint64_t prev = g_epoch_.fetch_add(1, std::memory_order_seq_cst);
  uint64_t min_active = UINT64_MAX;
  bool has_readers = false;
  uint32_t high = ticket_high_water_.load(std::memory_order_acquire);
  for (uint32_t i = 0; i < high; ++i) {
    uint64_t v = active_epochs_[i].load(std::memory_order_seq_cst);
    if (v != 0) {
      has_readers = true;
      if (v < min_active) {
        min_active = v;
      }
    }
  }

  if (!has_readers) {
    // 定理 C:全零时回收全部桶安全;跳跃只影响后续轮次。
    uint64_t max_retire = 0;
    std::vector<RetireEntry> all;
    {
      std::lock_guard<std::mutex> lk(retire_mu_);
      for (int b = 0; b < kBuckets; ++b) {
        if (bucket_epoch_[b] > max_retire) {
          max_retire = bucket_epoch_[b];
        }
        all.insert(all.end(),
                   std::make_move_iterator(retire_lists_[b].begin()),
                   std::make_move_iterator(retire_lists_[b].end()));
        retire_lists_[b].clear();
        bucket_epoch_[b] = 0;
      }
      pending_.fetch_sub(static_cast<uint64_t>(all.size()), std::memory_order_relaxed);
    }
    FreedBatch(all);
    if (max_retire > prev) {
      g_epoch_.store(max_retire + 2, std::memory_order_seq_cst);
    }
    return;
  }

  // 有读者:只回收 bucket_epoch < min_active 的桶(定理 B)。
  std::vector<RetireEntry> recycle;
  {
    std::lock_guard<std::mutex> lk(retire_mu_);
    for (int b = 0; b < kBuckets; ++b) {
      uint64_t be = bucket_epoch_[b];
      if (be == 0 || be >= min_active) {
        continue;
      }
      recycle.insert(recycle.end(),
                     std::make_move_iterator(retire_lists_[b].begin()),
                     std::make_move_iterator(retire_lists_[b].end()));
      retire_lists_[b].clear();
      bucket_epoch_[b] = 0;
    }
    pending_.fetch_sub(static_cast<uint64_t>(recycle.size()), std::memory_order_relaxed);
  }
  FreedBatch(recycle);
}

}  // namespace dge
