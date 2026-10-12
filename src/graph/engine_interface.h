#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

namespace dge {

enum class QueryType : uint8_t { kCommonFriends, kSecondDegree, kDistance };

struct QueryResult {
  bool ok = true;
  int32_t distance = -2;
  std::vector<uint32_t> friends;
};

struct QueryRequest {
  uint64_t seq = 0;
  uint64_t start_us = 0;
  uint32_t epoch_ticket = 0;
  QueryType type = QueryType::kDistance;
  uint32_t a = 0;
  uint32_t b = 0;
  std::function<void(QueryResult&&)> cb;
};

struct EngineStats {
  uint64_t queries_submitted = 0;
  uint64_t queries_completed = 0;
  uint64_t shed_count = 0;
  uint64_t cache_hits = 0;
  uint64_t cache_misses = 0;
  uint64_t cache_gets = 0;
  uint64_t cache_errors = 0;
  uint64_t dropped_writes = 0;
  uint64_t updates = 0;
  uint64_t compactions = 0;
  uint64_t maintenance_scans = 0;
  size_t memory_bytes = 0;   // 全账本:Slab+节点表+工作区
  size_t slab_bytes = 0;     // 仅 Slab 池在用槽字节

  double cache_hit_rate() const {
    return cache_gets == 0 ? 0.0
                           : static_cast<double>(cache_hits) / cache_gets;
  }
};

class IGraphEngine {
 public:
  virtual ~IGraphEngine() = default;

  virtual bool Submit(QueryRequest&& req) = 0;

  virtual bool AddEdge(uint32_t u, uint32_t v) = 0;

  virtual bool RemoveEdge(uint32_t u, uint32_t v) = 0;

  virtual EngineStats stats() const = 0;

  virtual size_t node_count() const = 0;

  virtual void Shutdown() = 0;
};

}  // namespace dge
