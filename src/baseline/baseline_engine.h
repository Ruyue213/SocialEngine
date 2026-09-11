#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <shared_mutex>
#include <vector>

#include "concurrency/thread_pool.h"
#include "graph/engine_interface.h"

namespace dge {

class BaselineGraphEngine : public IGraphEngine {
 public:
  struct Config {
    size_t nodes = 0;
    size_t pool_threads = 0;
    size_t queue_capacity = 4096;
    uint32_t max_bfs_depth = 32;
  };

  explicit BaselineGraphEngine(const Config& cfg);
  ~BaselineGraphEngine() override;

  bool Submit(QueryRequest&& req) override;
  bool AddEdge(uint32_t u, uint32_t v) override;
  bool RemoveEdge(uint32_t u, uint32_t v) override;
  EngineStats stats() const override;
  size_t node_count() const override { return nodes_; }
  void Shutdown() override;

 private:
  void ProcessQuery(const std::shared_ptr<QueryRequest>& req);
  int32_t BfsDistance(uint32_t src, uint32_t dst);
  std::vector<uint32_t> SecondDegree(uint32_t src);
  std::vector<uint32_t> CommonFriends(uint32_t u, uint32_t v);

  size_t nodes_;
  std::vector<std::vector<uint32_t>> adjacency_;
  mutable std::shared_mutex mu_;
  ThreadPool pool_;
  std::vector<bool> visited_a_;
  std::vector<bool> visited_b_;
  std::vector<uint32_t> frontier_;
  std::vector<uint32_t> next_;

  std::atomic<uint64_t> shed_count_{0};
  std::atomic<uint64_t> queries_submitted_{0};
  std::atomic<uint64_t> queries_completed_{0};
  std::atomic<uint64_t> updates_{0};
  std::atomic<bool> shutdown_{false};
};

}  // namespace dge
