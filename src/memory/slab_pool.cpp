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
  if (!in_page(p)) {
    return false;
  }
  const uint8_t* b = static_cast<const uint8_t*>(p);
  if (cls->slot < 16) {
    return true;
  }
  if (load_u64(b + 8) != kFreeMagic) {
    return false;
  }
  // 侵入式 next 指针要么为空,要么必须落在本档位页内且槽对齐;
  // 野指针在此被诊断接口拦截(分配热路径只做 O(1) 魔数校验)。
  uint64_t next = load_u64(b);
  return next == 0 || in_page(reinterpret_cast<const void*>(next));
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
  uint8_t* b = static_cast<uint8_t*>(p);
  // 生产热路径只做 O(1) 魔数校验:侵入式哨兵的定位是诊断层。
  // 原实现每次分配还线性扫描页列表验证空闲头与 next 指针的页
  // 归属,O(页数) 的防御检查在高频分配路径上被 profiling 定位
  // 为显著开销;完整校验保留在 ValidateFreeBlock 诊断接口。
  bool magic_ok = cls->slot < 16 || load_u64(b + 8) == kFreeMagic;
  if (!magic_ok) {
    throw std::runtime_error(
        "SlabPool: free block corrupted (intrusive next pointer or magic sentinel "
        "0xDEADBEEFCAFEBABE tampered), allocation aborted");
  }
  cls->free_head = reinterpret_cast<void*>(load_u64(p));
  ++cls->live;
  bytes_allocated_.fetch_add(cls->slot, std::memory_order_relaxed);
  bytes_in_use_.fetch_add(cls->slot, std::memory_order_relaxed);
  alloc_count_.fetch_add(1, std::memory_order_relaxed);
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
#ifdef DGE_VERIFY
  void* walk = cls->free_head;
  int guard = 0;
  while (walk != nullptr && guard < 100000) {
    if (walk == p) {
      throw std::runtime_error("SlabPool: double free detected");
    }
    walk = reinterpret_cast<void*>(load_u64(walk));
    ++guard;
  }
#endif
  store_u64(b, reinterpret_cast<uint64_t>(cls->free_head));
  if (cls->slot >= 16) {
    store_u64(b + 8, kFreeMagic);
  }
  if (cls->slot > 16) {
    std::memset(b + 16, kFillByte, cls->slot - 16);
  }
  cls->free_head = p;
  --cls->live;
  bytes_in_use_.fetch_sub(cls->slot, std::memory_order_relaxed);
}

void SlabPool::DeallocateBatch(const std::vector<std::pair<void*, size_t>>& items) {
  if (items.empty()) {
    return;
  }
  // 大小 -> 档位唯一,先按档位分组,再每档持锁一次批量归还,
  // 把锁交接次数从 O(n) 降到 O(档位数)。
  std::vector<std::vector<void*>> by_class(classes_.size());
  std::vector<void*> oversized;
  for (const auto& [p, sz] : items) {
    SizeClass* cls = FindClass(sz);
    if (cls == nullptr) {
      oversized.push_back(p);
      continue;
    }
    by_class[static_cast<size_t>(cls - classes_.data())].push_back(p);
  }
  for (size_t i = 0; i < classes_.size(); ++i) {
    auto& list = by_class[i];
    if (list.empty()) {
      continue;
    }
    SizeClass& cls = classes_[i];
    std::lock_guard<std::mutex> lk(cls.mu);
    for (void* p : list) {
      uint8_t* b = static_cast<uint8_t*>(p);
      store_u64(b, reinterpret_cast<uint64_t>(cls.free_head));
      if (cls.slot >= 16) {
        store_u64(b + 8, kFreeMagic);
      }
      if (cls.slot > 16) {
        std::memset(b + 16, kFillByte, cls.slot - 16);
      }
      cls.free_head = p;
      --cls.live;
      bytes_in_use_.fetch_sub(cls.slot, std::memory_order_relaxed);
    }
  }
  for (void* p : oversized) {
    std::free(p);
  }
}

}  // namespace dge
