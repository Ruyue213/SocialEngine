#pragma once

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <vector>

#include "cache/redis_client.h"
#include "concurrency/thread_pool.h"
#include "concurrency/timer_heap.h"
#include "graph/engine_interface.h"
#include "memory/slab_pool.h"
#include "storage/adjacency_store.h"
#include "storage/wal.h"

namespace dge {

struct WorkerCtx {
  std::vector<uint32_t> visited;
  std::vector<uint32_t> dist_a;
  std::vector<uint32_t> dist_b;
  uint32_t epoch = 0;
  std::vector<uint32_t> frontier_a;
  std::vector<uint32_t> frontier_b;
  std::vector<uint32_t> next_a;
  std::vector<uint32_t> next_b;
  std::vector<uint32_t> scratch;
  std::vector<uint32_t> scratch2;

  uint32_t NextEpoch() {
    ++epoch;
    if (epoch == 0) {
      std::fill(visited.begin(), visited.end(), 0);
      epoch = 1;
    }
    return epoch;
  }
};

class DynamicGraphEngine : public IGraphEngine {
 public:
  struct Config {
    size_t nodes = 0;
    size_t buckets = 256;
    size_t pool_threads = 0;
    size_t queue_capacity = 4096;
    bool pin_threads = true;
    bool cache_enabled = true;
    std::string redis_host = "127.0.0.1";
    int redis_port = 6379;
    int redis_io_threads = 4;
    int redis_ttl_seconds = 120;
    int double_delete_delay_ms = 5;
    uint32_t max_bfs_depth = 32;
    size_t keep_tail_threshold = 256;
    double hole_ratio_threshold = 0.4;
    size_t cache_max_friends = 512;
    int dirty_window_ms = 100;
    std::string cache_namespace;
    std::string wal_path;
    bool wal_sync_mode = false;
    std::string snapshot_path;
    size_t wal_checkpoint_bytes = 64 * 1024 * 1024;
  };

  explicit DynamicGraphEngine(const Config& cfg);
  ~DynamicGraphEngine() override;

  bool Submit(QueryRequest&& req) override;
  bool AddEdge(uint32_t u, uint32_t v) override;
  bool RemoveEdge(uint32_t u, uint32_t v) override;
  EngineStats stats() const override;
  size_t node_count() const override { return nodes_; }
  void Shutdown() override;

  SlabPool& pool() { return pool_; }
  size_t pool_thread_count() const { return workers_.thread_count(); }
  bool cache_available() const { return redis_ && redis_->enabled(); }

  void DedupCheck(const std::string& key, int ttl_seconds,
                  std::function<void(RedisClient::DedupStatus)> cb);

  bool Recover();
  // 原子检查点:同一 ckpt_mu_ 独占区间内完成「快照 + WAL 截断」,
  // 消除快照完成后、截断前进来的更新被误删的窗口。
  bool Checkpoint();
  void RotateWal() {
    if (wal_) {
      wal_->Rotate();
    }
  }
  void SetWalEnabled(bool on) { wal_enabled_.store(on, std::memory_order_relaxed); }

  void SetCacheInvalidation(bool on) {
    cache_invalidation_.store(on, std::memory_order_relaxed);
    if (on) {
      timer_.Clear();
    }
  }

 private:
  struct Bucket {
    std::shared_mutex mu;
    std::mutex dirty_mu;
    std::vector<uint32_t> dirty;
  };

  void ProcessQuery(const std::shared_ptr<QueryRequest>& req);
  void ComputeDistance(const std::shared_ptr<QueryRequest>& req);
  void ComputeCommonFriends(const std::shared_ptr<QueryRequest>& req);
  void ComputeSecondDegree(const std::shared_ptr<QueryRequest>& req);
  void FinishWithDistance(const std::shared_ptr<QueryRequest>& req, int32_t dist);
  void FinishWithFriends(const std::shared_ptr<QueryRequest>& req,
                         std::vector<uint32_t> friends);

  int32_t BidirectionalBfs(uint32_t src, uint32_t dst, WorkerCtx& ctx);
  std::vector<uint32_t> SecondDegreeBfs(uint32_t src, WorkerCtx& ctx);
  std::vector<uint32_t> CommonFriends(uint32_t u, uint32_t v, WorkerCtx& ctx);

  void ForEachNeighbor(uint32_t u, const std::function<void(uint32_t)>& fn);

  size_t BucketOf(uint32_t id) const { return hash_u64(id) % buckets_.size(); }

  static uint64_t hash_u64(uint64_t x);

  bool PrepareAddToNode(uint32_t node, uint32_t id);
  void MarkDirty(uint32_t node);
  void MaintenanceLoop();
  void CompactNode(uint32_t node);

  void InvalidateCache(uint32_t u, uint32_t v);
  void DoubleDelete(const std::vector<std::string>& keys);
  bool WriteSnapshotLocked();

  std::string DistKey(uint32_t u, uint32_t v) const;
  std::string CfKey(uint32_t u, uint32_t v) const;
  std::string SdKey(uint32_t u) const;
  std::string VersionKey(uint32_t u) const;
  std::string DirtyKey(uint32_t u) const;
  std::string SerializeFriends(const std::vector<uint32_t>& f);
  bool ParseFriends(const std::string& s, std::vector<uint32_t>& out);

  Config cfg_;
  size_t nodes_ = 0;
  std::vector<storage::NodeAdjacency> adj_;
  std::vector<Bucket> buckets_;
  std::vector<std::atomic<uint8_t>> dirty_flags_;
  SlabPool pool_;
  std::vector<WorkerCtx> worker_ctxs_;
  ThreadPool workers_;
  TimerHeap timer_;
  std::unique_ptr<RedisClient> redis_;
  std::mutex maint_mu_;
  std::condition_variable maint_cv_;
  bool maint_stop_ = false;
  std::thread maint_thread_;
  std::vector<uint32_t> maint_scratch_;
  // 检查点屏障:checkpoint 持独占(期间图静止,快照 LSN 与图状态
  // 严格对应);AddEdge/RemoveEdge/CompactNode/Recover 持共享。
  // 消除「先读 last_seq 再扫描,期间更新追加 WAL,checkpoint 后
  // Rotate 截断丢失这些记录」的窗口。
  std::shared_mutex ckpt_mu_;

  std::atomic<uint64_t> shed_count_{0};
  std::atomic<uint64_t> queries_submitted_{0};
  std::atomic<uint64_t> queries_completed_{0};
  std::atomic<uint64_t> cache_hits_{0};
  std::atomic<uint64_t> cache_misses_{0};
  std::atomic<uint64_t> updates_{0};
  std::atomic<uint64_t> compactions_{0};
  std::atomic<uint64_t> maintenance_scans_{0};
  std::atomic<bool> shutdown_{false};
  std::atomic<bool> cache_invalidation_{true};
  std::atomic<bool> recovering_{false};
  std::atomic<bool> wal_enabled_{true};
  std::unique_ptr<WalWriter> wal_;
};

}  // namespace dge
