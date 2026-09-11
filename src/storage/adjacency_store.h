#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

#include "memory/slab_pool.h"

namespace dge {
namespace storage {

static constexpr size_t kBlockSize = 1024;
static constexpr size_t kBlockHeaderSize = 24;
static constexpr size_t kBlockPayload = kBlockSize - kBlockHeaderSize;
static constexpr uint32_t kBlockMagic = 0x0B10C0DEu;
static constexpr uint64_t kDeletedTail = 0xFFFFFFFFFFFFFFFFULL;
static constexpr size_t kKeepTailThreshold = 256;
static constexpr double kHoleRatioThreshold = 0.4;
static constexpr size_t kMaxVarint = 5;

struct BlockHeader {
  uint8_t* next;
  uint16_t count;
  uint16_t deleted;
  uint16_t data_end;
  uint32_t magic;
};

struct NodeAdjacency {
  uint8_t* head;
  uint32_t block_count;
  uint32_t tail_cap;
  uint32_t tail_len;
  uint32_t tail_deleted;
  uint64_t* tail;
};

static_assert(sizeof(BlockHeader) == 24, "BlockHeader must be 24 bytes");

inline BlockHeader* header_of(uint8_t* block) {
  return reinterpret_cast<BlockHeader*>(block);
}

inline const uint8_t* data_of(const uint8_t* block) { return block + kBlockHeaderSize; }

inline bool bit_is_set(const uint8_t* block, uint32_t idx) {
  uint32_t byte_idx = kBlockSize - 1 - (idx >> 3);
  uint8_t mask = static_cast<uint8_t>(1u << (idx & 7));
  return (block[byte_idx] & mask) != 0;
}

inline void bit_set(uint8_t* block, uint32_t idx) {
  uint32_t byte_idx = kBlockSize - 1 - (idx >> 3);
  uint8_t mask = static_cast<uint8_t>(1u << (idx & 7));
  block[byte_idx] |= mask;
}

size_t block_free_space(const uint8_t* block);

uint32_t block_append(uint8_t* block, uint64_t delta);

void block_iterate(const uint8_t* block,
                   const std::function<void(uint32_t)>& fn);

bool block_remove(uint8_t* block, uint32_t id, bool& found);

enum class TailAddResult { kAdded, kDuplicate, kFull };

TailAddResult tail_add(NodeAdjacency& adj, uint64_t id);

bool tail_remove(NodeAdjacency& adj, uint64_t id);

bool tail_contains(const NodeAdjacency& adj, uint64_t id);

void node_iterate(const NodeAdjacency& adj,
                  const std::function<void(uint32_t)>& fn);

size_t node_live_count(const NodeAdjacency& adj);

size_t gather_live(const NodeAdjacency& adj, std::vector<uint32_t>& out);

size_t grow_tail(SlabPool& pool, NodeAdjacency& adj, const std::vector<uint32_t>& live);

void rebuild_blocks(SlabPool& pool, NodeAdjacency& adj,
                    const std::vector<uint32_t>& live);

void release_node(SlabPool& pool, NodeAdjacency& adj);

bool tail_full(const NodeAdjacency& adj);

bool node_needs_compaction(const NodeAdjacency& adj);

}  // namespace storage
}  // namespace dge
