#include "storage/adjacency_store.h"

#include <algorithm>
#include <cstring>

#include "common/varint.h"

namespace dge {
namespace storage {

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

void block_iterate(const uint8_t* block,
                   const std::function<void(uint32_t)>& fn) {
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

bool block_remove(uint8_t* block, uint32_t id, bool& found) {
  BlockHeader* h = header_of(block);
  const uint8_t* p = data_of(block);
  const uint8_t* end = p + h->data_end;
  uint64_t cur = 0;
  for (uint32_t i = 0; i < h->count; ++i) {
    uint64_t delta = 0;
    p = get_varint(p, end, delta);
    if (p == nullptr) {
      return false;
    }
    cur += delta;
    if (cur == id && !bit_is_set(block, i)) {
      bit_set(block, i);
      ++h->deleted;
      found = true;
      return true;
    }
    if (cur > id) {
      return false;
    }
  }
  return true;
}

TailAddResult tail_add(NodeAdjacency& adj, uint64_t id) {
  uint32_t hole = 0;
  bool has_hole = false;
  for (uint32_t i = 0; i < adj.tail_len; ++i) {
    if (adj.tail[i] == id) {
      return TailAddResult::kDuplicate;
    }
    if (adj.tail[i] == kDeletedTail && !has_hole) {
      hole = i;
      has_hole = true;
    }
  }
  if (has_hole) {
    adj.tail[hole] = id;
    --adj.tail_deleted;
    return TailAddResult::kAdded;
  }
  if (adj.tail_len >= adj.tail_cap) {
    return TailAddResult::kFull;
  }
  adj.tail[adj.tail_len++] = id;
  return TailAddResult::kAdded;
}

bool tail_remove(NodeAdjacency& adj, uint64_t id) {
  for (uint32_t i = 0; i < adj.tail_len; ++i) {
    if (adj.tail[i] == id) {
      adj.tail[i] = kDeletedTail;
      ++adj.tail_deleted;
      return true;
    }
  }
  return false;
}

bool tail_contains(const NodeAdjacency& adj, uint64_t id) {
  for (uint32_t i = 0; i < adj.tail_len; ++i) {
    if (adj.tail[i] == id) {
      return true;
    }
  }
  return false;
}

void node_iterate(const NodeAdjacency& adj,
                  const std::function<void(uint32_t)>& fn) {
  uint8_t* b = adj.head;
  while (b != nullptr) {
    block_iterate(b, fn);
    b = header_of(b)->next;
  }
  for (uint32_t i = 0; i < adj.tail_len; ++i) {
    if (adj.tail[i] != kDeletedTail) {
      fn(static_cast<uint32_t>(adj.tail[i]));
    }
  }
}

size_t node_live_count(const NodeAdjacency& adj) {
  size_t n = 0;
  uint8_t* b = adj.head;
  while (b != nullptr) {
    const BlockHeader* h = header_of(b);
    n += h->count - h->deleted;
    b = h->next;
  }
  n += adj.tail_len - adj.tail_deleted;
  return n;
}

size_t gather_live(const NodeAdjacency& adj, std::vector<uint32_t>& out) {
  out.clear();
  out.reserve(node_live_count(adj) + 8);
  node_iterate(adj, [&out](uint32_t id) { out.push_back(id); });
  std::sort(out.begin(), out.end());
  return out.size();
}

size_t grow_tail(SlabPool& pool, NodeAdjacency& adj,
                 const std::vector<uint32_t>& live) {
  size_t want_entries = live.empty() ? 8 : live.size() * 2;
  size_t cap_entries = 8;
  while (cap_entries < want_entries && cap_entries < 512) {
    cap_entries *= 2;
  }
  if (cap_entries < live.size()) {
    cap_entries = 512;
  }
  size_t cap = cap_entries * sizeof(uint64_t);
  uint64_t* old_tail = adj.tail;
  size_t old_cap = static_cast<size_t>(adj.tail_cap) * sizeof(uint64_t);
  adj.tail = static_cast<uint64_t*>(pool.Allocate(cap));
  adj.tail_cap = static_cast<uint32_t>(cap_entries);
  adj.tail_len = 0;
  adj.tail_deleted = 0;
  for (uint32_t id : live) {
    adj.tail[adj.tail_len++] = id;
  }
  if (old_tail != nullptr) {
    pool.Deallocate(old_tail, old_cap);
  }
  return cap;
}

void rebuild_blocks(SlabPool& pool, NodeAdjacency& adj,
                    const std::vector<uint32_t>& live) {
  uint8_t* old_head = adj.head;
  uint64_t* old_tail = adj.tail;
  size_t old_tail_cap = static_cast<size_t>(adj.tail_cap) * sizeof(uint64_t);
  adj.head = nullptr;
  adj.block_count = 0;
  adj.tail = nullptr;
  adj.tail_cap = 0;
  adj.tail_len = 0;
  adj.tail_deleted = 0;

  uint8_t* cur = nullptr;
  uint8_t* last = nullptr;
  uint64_t prev = 0;
  size_t i = 0;
  while (i < live.size()) {
    if (cur == nullptr) {
      cur = static_cast<uint8_t*>(pool.Allocate(kBlockSize));
      std::memset(cur, 0, kBlockSize);
      header_of(cur)->magic = kBlockMagic;
      if (adj.head == nullptr) {
        adj.head = cur;
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
      continue;
    }
    block_append(cur, delta);
    prev = live[i];
    ++i;
  }

  size_t cap = 64;
  adj.tail = static_cast<uint64_t*>(pool.Allocate(cap));
  adj.tail_cap = static_cast<uint32_t>(cap / sizeof(uint64_t));

  while (old_head != nullptr) {
    uint8_t* n = header_of(old_head)->next;
    pool.Deallocate(old_head, kBlockSize);
    old_head = n;
  }
  if (old_tail != nullptr) {
    pool.Deallocate(old_tail, old_tail_cap);
  }
}

void release_node(SlabPool& pool, NodeAdjacency& adj) {
  uint8_t* b = adj.head;
  while (b != nullptr) {
    uint8_t* n = header_of(b)->next;
    pool.Deallocate(b, kBlockSize);
    b = n;
  }
  if (adj.tail != nullptr) {
    pool.Deallocate(adj.tail, static_cast<size_t>(adj.tail_cap) * sizeof(uint64_t));
  }
  adj.head = nullptr;
  adj.tail = nullptr;
  adj.block_count = 0;
  adj.tail_cap = 0;
  adj.tail_len = 0;
  adj.tail_deleted = 0;
}

bool tail_full(const NodeAdjacency& adj) {
  return adj.tail_len >= adj.tail_cap;
}

bool node_needs_compaction(const NodeAdjacency& adj, double hole_ratio_threshold) {
  if (tail_full(adj)) {
    return true;
  }
  if (adj.tail_len > 0 &&
      static_cast<double>(adj.tail_deleted) / adj.tail_len > hole_ratio_threshold) {
    return true;
  }
  uint8_t* b = adj.head;
  while (b != nullptr) {
    const BlockHeader* h = header_of(b);
    if (h->count > 0 &&
        static_cast<double>(h->deleted) / h->count > hole_ratio_threshold) {
      return true;
    }
    b = h->next;
  }
  return false;
}

}  // namespace storage
}  // namespace dge
