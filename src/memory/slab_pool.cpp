#include "memory/slab_pool.h"

#include <cstdlib>
#include <cstring>
#include <stdexcept>
#include <string>

namespace dge {

namespace {

uint64_t load_u64(const void* p) {
  uint64_t v;
  std::memcpy(&v, p, sizeof(v));
  return v;
}

void store_u64(void* p, uint64_t v) { std::memcpy(p, &v, sizeof(v)); }

}  // namespace

SlabPool::SlabPool() {
  static constexpr size_t kSizes[] = {64, 128, 256, 512, 1024, 2048, 4096};
  for (size_t s : kSizes) {
    classes_.emplace_back();
    classes_.back().slot = s;
  }
}

SlabPool::~SlabPool() {
  for (auto& cls : classes_) {
    for (auto& page : cls.pages) {
      std::free(page.base);
    }
  }
}

SlabPool::SizeClass* SlabPool::FindClass(size_t size) {
  for (auto& cls : classes_) {
    if (cls.slot >= size) {
      return &cls;
    }
  }
  return nullptr;
}

const SlabPool::SizeClass* SlabPool::FindClass(size_t size) const {
  for (auto& cls : classes_) {
    if (cls.slot >= size) {
      return &cls;
    }
  }
  return nullptr;
}

size_t SlabPool::round_up_class(size_t size) const {
  const SizeClass* cls = FindClass(size);
  return cls ? cls->slot : 0;
}

bool SlabPool::ValidateFreeBlock(void* p, size_t size) const {
  if (p == nullptr) {
    return false;
  }
  const SizeClass* cls = FindClass(size);
  if (cls == nullptr || size > cls->slot) {
    return false;
  }
  bool in_page = false;
  for (const auto& page : cls->pages) {
    uintptr_t base = reinterpret_cast<uintptr_t>(page.base);
    uintptr_t addr = reinterpret_cast<uintptr_t>(p);
    if (addr >= base && addr < base + kPageSize &&
        (addr - base) % cls->slot == 0) {
      in_page = true;
      break;
    }
  }
  if (!in_page) {
    return false;
  }
  const uint8_t* b = static_cast<const uint8_t*>(p);
  if (cls->slot < 16) {
    return true;
  }
  return load_u64(b + 8) == kFreeMagic;
}

void* SlabPool::Refill(SizeClass& cls) {
  void* page_base = std::aligned_alloc(kPageSize, kPageSize);
  if (page_base == nullptr) {
    throw std::bad_alloc();
  }
  Page page;
  page.base = page_base;
  page.slot_size = cls.slot;
  cls.pages.push_back(page);

  uint8_t* base = static_cast<uint8_t*>(page_base);
  for (size_t off = 0; off + cls.slot <= kPageSize; off += cls.slot) {
    uint8_t* slot = base + off;
    store_u64(slot, reinterpret_cast<uint64_t>(cls.free_head));
    if (cls.slot >= 16) {
      store_u64(slot + 8, kFreeMagic);
    }
    if (cls.slot > 16) {
      std::memset(slot + 16, kFillByte, cls.slot - 16);
    }
    cls.free_head = slot;
  }
  return cls.free_head;
}

void* SlabPool::Allocate(size_t size) {
  SizeClass* cls = FindClass(size);
  if (cls == nullptr) {
    return std::malloc(size);
  }
  std::lock_guard<std::mutex> lk(cls->mu);
  if (cls->free_head == nullptr) {
    Refill(*cls);
  }
  void* p = cls->free_head;
  uint64_t next = load_u64(p);
  uint8_t* b = static_cast<uint8_t*>(p);
  auto in_page = [&](const void* ptr) {
    for (const auto& page : cls->pages) {
      uintptr_t base = reinterpret_cast<uintptr_t>(page.base);
      uintptr_t addr = reinterpret_cast<uintptr_t>(ptr);
      if (addr >= base && addr < base + kPageSize && (addr - base) % cls->slot == 0) {
        return true;
      }
    }
    return false;
  };
  bool head_valid = in_page(p);
  bool next_valid = next == 0 || in_page(reinterpret_cast<const void*>(next));
  bool magic_ok = cls->slot < 16 || load_u64(b + 8) == kFreeMagic;
  if (!head_valid || !next_valid || !magic_ok) {
    throw std::runtime_error(
        "SlabPool: free block corrupted (intrusive next pointer or magic sentinel "
        "0xDEADBEEFCAFEBABE tampered), allocation aborted");
  }
  cls->free_head = reinterpret_cast<void*>(next);
  ++cls->live;
  bytes_allocated_ += cls->slot;
  bytes_in_use_ += cls->slot;
  ++alloc_count_;
  return p;
}

void SlabPool::Deallocate(void* p, size_t size) {
  if (p == nullptr) {
    return;
  }
  SizeClass* cls = FindClass(size);
  if (cls == nullptr) {
    std::free(p);
    return;
  }
  std::lock_guard<std::mutex> lk(cls->mu);
  uint8_t* b = static_cast<uint8_t*>(p);
  store_u64(b, reinterpret_cast<uint64_t>(cls->free_head));
  if (cls->slot >= 16) {
    store_u64(b + 8, kFreeMagic);
  }
  if (cls->slot > 16) {
    std::memset(b + 16, kFillByte, cls->slot - 16);
  }
  cls->free_head = p;
  --cls->live;
  bytes_in_use_ -= cls->slot;
}

}  // namespace dge
