#pragma once

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

  size_t bytes_allocated() const { return bytes_allocated_; }
  size_t bytes_in_use() const { return bytes_in_use_; }
  uint64_t alloc_count() const { return alloc_count_; }

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
  uint64_t bytes_allocated_ = 0;
  uint64_t bytes_in_use_ = 0;
  uint64_t alloc_count_ = 0;
};

}  // namespace dge
