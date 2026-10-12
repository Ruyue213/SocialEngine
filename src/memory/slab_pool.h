#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

namespace dge {

class SlabPool {
 public:
  static constexpr uint64_t kFreeMagic = 0xDEADBEEFCAFEBABEULL;
  static constexpr uint8_t kFillByte = 0xCD;
  static constexpr size_t kPageSize = 4096;

  SlabPool();
  ~SlabPool();
  SlabPool(const SlabPool&) = delete;
  SlabPool& operator=(const SlabPool&) = delete;

  void* Allocate(size_t size);
  void Deallocate(void* p, size_t size);
  // 批量释放:按档位分组,每档只取一次锁。回收器 Sweep 一次可能
  // 归还成百上千个对象,逐个 Deallocate 的锁交接在高并发分配下
  // 构成显著竞争(profiling 实测)。
  void DeallocateBatch(const std::vector<std::pair<void*, size_t>>& items);

  size_t bytes_allocated() const { return bytes_allocated_.load(std::memory_order_relaxed); }
  size_t bytes_in_use() const { return bytes_in_use_.load(std::memory_order_relaxed); }
  uint64_t alloc_count() const { return alloc_count_.load(std::memory_order_relaxed); }

  size_t round_up_class(size_t size) const;
  bool ValidateFreeBlock(void* p, size_t size) const;

 private:
  struct Page {
    void* base;
    size_t slot_size;
  };

  struct SizeClass {
    SizeClass() = default;
    SizeClass(SizeClass&& o) noexcept
        : slot(o.slot), free_head(o.free_head), pages(std::move(o.pages)), live(o.live) {}
    SizeClass& operator=(SizeClass&&) = delete;

    size_t slot;
    std::mutex mu;
    void* free_head = nullptr;
    std::vector<Page> pages;
    uint64_t live = 0;
  };

  void* Refill(SizeClass& cls);
  SizeClass* FindClass(size_t size);
  const SizeClass* FindClass(size_t size) const;

  std::vector<SizeClass> classes_;
  // 全局账本为原子计数:各档位锁互不相同,原先普通 uint64 被不同
  // 档的锁并发读写,构成数据竞争且计数漂移(压测中曾观察到
  // bytes_in_use 无符号下溢回绕出 17TB 的荒谬值)。
  std::atomic<uint64_t> bytes_allocated_{0};
  std::atomic<uint64_t> bytes_in_use_{0};
  std::atomic<uint64_t> alloc_count_{0};
};

}  // namespace dge

