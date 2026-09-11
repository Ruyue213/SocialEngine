#include "storage/adjacency_store.h"

#include <algorithm>
#include <cstring>

#include "common/varint.h"
#include "memory/epoch.h"

namespace dge {
namespace storage {

namespace {

constexpr size_t kDataClass = 64;

AdjacencyData* NewData(SlabPool& pool, uint8_t* head, uint64_t* tail, uint32_t cap,
                       uint32_t len, uint32_t deleted) {
  auto* d = static_cast<AdjacencyData*>(pool.Allocate(kDataClass));
  d->head = head;
  d->tail = tail;
  d->cap.store(cap, std::memory_order_relaxed);
  d->len.store(len, std::memory_order_relaxed);
  d->deleted.store(deleted, std::memory_order_relaxed);
  d->pad = 0;
  return d;
}

uint64_t* NewTail(SlabPool& pool, size_t cap_entries) {
  return static_cast<uint64_t*>(pool.Allocate(cap_entries * sizeof(uint64_t)));
}

void RetireData(SlabPool& pool, AdjacencyData* d) {
  EpochReclaimer::Instance().Retire(d, kDataClass, &pool);
}

void RetireTail(SlabPool& pool, uint64_t* tail, uint32_t cap_entries) {
  if (tail != nullptr) {
    EpochReclaimer::Instance().Retire(tail, static_cast<size_t>(cap_entries) * sizeof(uint64_t),
                                      &pool);
  }
}

bool ReadersActive() { return EpochReclaimer::Instance().ReadersActive(); }

}  // namespace

size_t block_free_space(const uint8_t* block) {
  const BlockHeader* h = header_of(const_cast<uint8_t*>(block));
  size_t bitmap_bytes = (static_cast<size_t>(h->count) + 7) / 8;
  size_t used = h->data_end + bitmap_bytes;
  return used >= kBlockPayload ? 0 : kBlockPayload - used;
}

uint32_t block_append(uint8_t* block, uint64_t delta) {
  BlockHeader* h = header_of(block);
  uint32_t idx = h->count;
  uint8_t* p = block + kBlockHeaderSize + h->data_end;
  uint8_t* end = put_varint(p, delta);
  h->data_end = static_cast<uint16_t>(end - (block + kBlockHeaderSize));
  ++h->count;
  return idx;
}

void block_iterate(const uint8_t* block, const std::function<void(uint32_t)>& fn) {
  const BlockHeader* h = header_of(const_cast<uint8_t*>(block));
  const uint8_t* p = data_of(block);
  const uint8_t* end = p + h->data_end;
  uint64_t id = 0;
  for (uint32_t i = 0; i < h->count; ++i) {
    uint64_t delta = 0;
    p = get_varint(p, end, delta);
    if (p == nullptr) {
      return;
    }
    id += delta;
    if (!bit_is_set(block, i)) {
      fn(static_cast<uint32_t>(id));
    }
  }
}

bool block_remove(SlabPool& pool, NodeAdjacency& adj, uint32_t id, bool& found) {
  AdjacencyData* d = adj.data.load(std::memory_order_relaxed);
  if (d == nullptr) {
    return false;
  }
  uint8_t* b = d->head;
  while (b != nullptr) {
    BlockHeader* h = header_of(b);
    const uint8_t* p = data_of(b);
    const uint8_t* end = p + h->data_end;
    uint64_t cur = 0;
    for (uint32_t i = 0; i < h->count; ++i) {
      uint64_t delta = 0;
      p = get_varint(p, end, delta);
      if (p == nullptr) {
        return false;
      }
      cur += delta;
      if (cur == id && !bit_is_set(b, i)) {
        if (!ReadersActive()) {
          bit_set(b, i);
          ++h->deleted;
          found = true;
          return true;
        }
        uint8_t* new_head = nullptr;
        uint8_t* last_new = nullptr;
        uint8_t* walk = d->head;
        while (walk != nullptr) {
          uint8_t* nb = static_cast<uint8_t*>(pool.Allocate(kBlockSize));
          std::memcpy(nb, walk, kBlockSize);
          if (new_head == nullptr) {
            new_head = nb;
          } else {
            header_of(last_new)->next = nb;
          }
          last_new = nb;
          uint8_t* old_next = header_of(walk)->next;
          bool is_target = (walk == b);
          EpochReclaimer::Instance().Retire(walk, kBlockSize, &pool);
          if (is_target) {
            bit_set(nb, i);
            ++header_of(nb)->deleted;
            header_of(nb)->next = old_next;
            walk = nullptr;
          } else {
            walk = old_next;
          }
        }
        AdjacencyData* nd = NewData(
            pool, new_head, d->tail, d->cap.load(std::memory_order_relaxed),
            d->len.load(std::memory_order_relaxed), d->deleted.load(std::memory_order_relaxed));
        adj.data.store(nd, std::memory_order_release);
        RetireData(pool, d);
        found = true;
        return true;
      }
      if (cur > id) {
        return false;
      }
    }
    b = h->next;
  }
  return true;
}

TailAddResult tail_add(SlabPool& pool, NodeAdjacency& adj, uint64_t id) {
  AdjacencyData* d = adj.data.load(std::memory_order_relaxed);
  if (d == nullptr || d->tail == nullptr) {
    uint64_t* tail = NewTail(pool, 8);
    tail[0] = id;
    AdjacencyData* nd = NewData(pool, d != nullptr ? d->head : nullptr, tail, 8, 1, 0);
    adj.data.store(nd, std::memory_order_release);
    if (d != nullptr) {
      RetireData(pool, d);
    }
    return TailAddResult::kAdded;
  }
  uint64_t* work = d->tail;
  uint32_t len = d->len.load(std::memory_order_acquire);
  uint32_t deleted = d->deleted.load(std::memory_order_relaxed);
  if (ReadersActive()) {
    uint64_t* clone = NewTail(pool, d->cap);
    std::memcpy(clone, d->tail, static_cast<size_t>(d->len) * sizeof(uint64_t));
    work = clone;
  }
  uint32_t hole = 0;
  bool has_hole = false;
  for (uint32_t i = 0; i < len; ++i) {
    if (work[i] == id) {
      if (work != d->tail) {
        RetireTail(pool, work, d->cap);
      }
      return TailAddResult::kDuplicate;
    }
    if (work[i] == kDeletedTail && !has_hole) {
      hole = i;
      has_hole = true;
    }
  }
  if (has_hole) {
    work[hole] = id;
    --deleted;
    if (work != d->tail) {
      AdjacencyData* nd =
          NewData(pool, d->head, work, d->cap.load(std::memory_order_relaxed), len, deleted);
      adj.data.store(nd, std::memory_order_release);
      RetireData(pool, d);
      RetireTail(pool, d->tail, d->cap.load(std::memory_order_relaxed));
    } else {
      d->deleted.store(deleted, std::memory_order_release);
    }
    return TailAddResult::kAdded;
  }
  if (len >= d->cap.load(std::memory_order_relaxed)) {
    if (work != d->tail) {
      RetireTail(pool, work, d->cap);
    }
    return TailAddResult::kFull;
  }
  work[len] = id;
  if (work != d->tail) {
    AdjacencyData* nd = NewData(pool, d->head, work, d->cap.load(std::memory_order_relaxed),
                                len + 1, deleted);
    adj.data.store(nd, std::memory_order_release);
    RetireData(pool, d);
    RetireTail(pool, d->tail, d->cap.load(std::memory_order_relaxed));
  } else {
    d->len.store(len + 1, std::memory_order_release);
  }
  return TailAddResult::kAdded;
}

bool tail_remove(SlabPool& pool, NodeAdjacency& adj, uint64_t id) {
  AdjacencyData* d = adj.data.load(std::memory_order_relaxed);
  if (d == nullptr || d->tail == nullptr) {
    return false;
  }
  uint64_t* work = d->tail;
  uint32_t len = d->len.load(std::memory_order_acquire);
  uint32_t deleted = d->deleted.load(std::memory_order_relaxed);
  if (ReadersActive()) {
    uint64_t* clone = NewTail(pool, d->cap);
    std::memcpy(clone, d->tail, static_cast<size_t>(d->len) * sizeof(uint64_t));
    work = clone;
  }
  for (uint32_t i = 0; i < len; ++i) {
    if (work[i] == id) {
      work[i] = kDeletedTail;
      ++deleted;
      if (work != d->tail) {
        AdjacencyData* nd = NewData(pool, d->head, work,
                                    d->cap.load(std::memory_order_relaxed), len, deleted);
        adj.data.store(nd, std::memory_order_release);
        RetireData(pool, d);
        RetireTail(pool, d->tail, d->cap.load(std::memory_order_relaxed));
      } else {
        d->deleted.store(deleted, std::memory_order_release);
      }
      return true;
    }
  }
  if (work != d->tail) {
    RetireTail(pool, work, d->cap);
  }
  return false;
}

bool tail_contains(const NodeAdjacency& adj, uint64_t id) {
  const AdjacencyData* d = adj.data.load(std::memory_order_relaxed);
  if (d == nullptr || d->tail == nullptr) {
    return false;
  }
  uint32_t len = d->len.load(std::memory_order_acquire);
  for (uint32_t i = 0; i < len; ++i) {
    if (d->tail[i] == id) {
      return true;
    }
  }
  return false;
}

void node_iterate(uint8_t* head, const uint64_t* tail, uint32_t tail_len,
                  const std::function<void(uint32_t)>& fn) {
  uint8_t* b = head;
  while (b != nullptr) {
    block_iterate(b, fn);
    b = header_of(b)->next;
  }
  for (uint32_t i = 0; i < tail_len; ++i) {
    if (tail[i] != kDeletedTail) {
      fn(static_cast<uint32_t>(tail[i]));
    }
  }
}

void node_iterate(const NodeAdjacency& adj, const std::function<void(uint32_t)>& fn) {
  const AdjacencyData* d = adj.data.load(std::memory_order_relaxed);
  if (d == nullptr) {
    return;
  }
  node_iterate(d->head, d->tail, d->len.load(std::memory_order_acquire), fn);
}

size_t node_live_count(const NodeAdjacency& adj) {
  const AdjacencyData* d = adj.data.load(std::memory_order_relaxed);
  if (d == nullptr) {
    return 0;
  }
  size_t n = 0;
  uint8_t* b = d->head;
  while (b != nullptr) {
    const BlockHeader* h = header_of(b);
    n += h->count - h->deleted;
    b = h->next;
  }
  n += d->len.load(std::memory_order_relaxed) - d->deleted.load(std::memory_order_relaxed);
  return n;
}

size_t gather_live(const NodeAdjacency& adj, std::vector<uint32_t>& out) {
  out.clear();
  out.reserve(node_live_count(adj) + 8);
  node_iterate(adj, [&out](uint32_t id) { out.push_back(id); });
  std::sort(out.begin(), out.end());
  return out.size();
}

size_t grow_tail(SlabPool& pool, NodeAdjacency& adj, const std::vector<uint32_t>& live) {
  size_t want_entries = live.empty() ? 8 : live.size() * 2;
  size_t cap_entries = 8;
  while (cap_entries < want_entries && cap_entries < 512) {
    cap_entries *= 2;
  }
  if (cap_entries < live.size()) {
    cap_entries = 512;
  }
  uint64_t* nt = NewTail(pool, cap_entries);
  for (size_t i = 0; i < live.size(); ++i) {
    nt[i] = live[i];
  }
  AdjacencyData* d = adj.data.load(std::memory_order_relaxed);
  AdjacencyData* nd =
      NewData(pool, d != nullptr ? d->head : nullptr, nt, static_cast<uint32_t>(cap_entries),
              static_cast<uint32_t>(live.size()), 0);
  adj.data.store(nd, std::memory_order_release);
  if (d != nullptr) {
    RetireData(pool, d);
    RetireTail(pool, d->tail, d->cap.load(std::memory_order_relaxed));
  }
  return cap_entries * sizeof(uint64_t);
}

void rebuild_blocks(SlabPool& pool, NodeAdjacency& adj, const std::vector<uint32_t>& live) {
  uint8_t* cur = nullptr;
  uint8_t* last = nullptr;
  uint8_t* new_head = nullptr;
  uint64_t prev = 0;
  size_t i = 0;
  while (i < live.size()) {
    if (cur == nullptr) {
      cur = static_cast<uint8_t*>(pool.Allocate(kBlockSize));
      std::memset(cur, 0, kBlockSize);
      header_of(cur)->magic = kBlockMagic;
      if (new_head == nullptr) {
        new_head = cur;
      } else {
        header_of(last)->next = cur;
      }
      last = cur;
      ++adj.block_count;
    }
    uint64_t delta = static_cast<uint64_t>(live[i]) - prev;
    size_t vlen = varint_size(delta);
    size_t new_count = header_of(cur)->count + 1;
    size_t bitmap_bytes = (new_count + 7) / 8;
    if (header_of(cur)->count > 0 &&
        header_of(cur)->data_end + vlen + bitmap_bytes > kBlockPayload) {
      cur = nullptr;
      prev = 0;
      continue;
    }
    block_append(cur, delta);
    prev = live[i];
    ++i;
  }

  uint64_t* nt = NewTail(pool, 8);
  AdjacencyData* d = adj.data.exchange(nullptr, std::memory_order_relaxed);
  adj.data.store(NewData(pool, new_head, nt, 8, 0, 0), std::memory_order_release);
  if (d != nullptr) {
    RetireData(pool, d);
    uint8_t* old_head = d->head;
    while (old_head != nullptr) {
      uint8_t* n = header_of(old_head)->next;
      EpochReclaimer::Instance().Retire(old_head, kBlockSize, &pool);
      old_head = n;
    }
    RetireTail(pool, d->tail, d->cap.load(std::memory_order_relaxed));
  }
}

void release_node(SlabPool& pool, NodeAdjacency& adj) {
  AdjacencyData* d = adj.data.exchange(nullptr);
  adj.block_count = 0;
  if (d == nullptr) {
    return;
  }
  uint8_t* head = d->head;
  while (head != nullptr) {
    uint8_t* n = header_of(head)->next;
    pool.Deallocate(head, kBlockSize);
    head = n;
  }
  if (d->tail != nullptr) {
    pool.Deallocate(d->tail,
                    static_cast<size_t>(d->cap.load(std::memory_order_relaxed)) *
                        sizeof(uint64_t));
  }
  pool.Deallocate(d, kDataClass);
}

bool tail_full(const NodeAdjacency& adj) {
  const AdjacencyData* d = adj.data.load(std::memory_order_relaxed);
  if (d == nullptr) {
    return false;
  }
  return d->len.load(std::memory_order_relaxed) >= d->cap.load(std::memory_order_relaxed);
}

bool node_needs_compaction(const NodeAdjacency& adj, double hole_ratio_threshold) {
  const AdjacencyData* d = adj.data.load(std::memory_order_relaxed);
  if (d == nullptr) {
    return false;
  }
  uint32_t len = d->len.load(std::memory_order_relaxed);
  if (len >= d->cap.load(std::memory_order_relaxed)) {
    return true;
  }
  if (len > 0 &&
      static_cast<double>(d->deleted.load(std::memory_order_relaxed)) / len >
          hole_ratio_threshold) {
    return true;
  }
  uint8_t* b = d->head;
  while (b != nullptr) {
    const BlockHeader* h = header_of(b);
    if (h->count > 0 && static_cast<double>(h->deleted) / h->count > hole_ratio_threshold) {
      return true;
    }
    b = h->next;
  }
  return false;
}

}  // namespace storage
}  // namespace dge
