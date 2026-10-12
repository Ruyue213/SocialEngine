#include "graph/dynamic_graph_engine.h"

#include "common/metrics.h"
#include "memory/epoch.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <sstream>

#include "common/affinity.h"
#include "graph/intersect.h"

namespace dge {

namespace {

thread_local WorkerCtx* tls_worker_ctx = nullptr;

constexpr const char* kReadScript3 =
    "local v1 = redis.call('GET', KEYS[1]) or '0'\n"
    "local v2 = redis.call('GET', KEYS[2]) or '0'\n"
    "local val = redis.call('GET', KEYS[3])\n"
    "if val then\n"
    "  local prefix = v1 .. '.' .. v2 .. '.'\n"
    "  if string.sub(val, 1, string.len(prefix)) == prefix then return val end\n"
    "  local d1 = redis.call('EXISTS', KEYS[4])\n"
    "  local d2 = redis.call('EXISTS', KEYS[5])\n"
    "  if (d1 + d2) > 0 then return val .. '|stale' end\n"
    "end\n"
    "return v1 .. '.' .. v2 .. '.'";

constexpr const char* kReadScript2 =
    "local v1 = redis.call('GET', KEYS[1]) or '0'\n"
    "local val = redis.call('GET', KEYS[2])\n"
    "if val then\n"
    "  local prefix = v1 .. '.'\n"
    "  if string.sub(val, 1, string.len(prefix)) == prefix then return val end\n"
    "  local d1 = redis.call('EXISTS', KEYS[3])\n"
    "  if d1 > 0 then return val .. '|stale' end\n"
    "end\n"
    "return v1 .. '.'";

constexpr const char* kInvalidateScript =
    "redis.call('INCR', KEYS[1])\n"
    "redis.call('INCR', KEYS[2])\n"
    "redis.call('DEL', KEYS[3], KEYS[4], KEYS[5], KEYS[6])\n"
    "redis.call('PSETEX', KEYS[7], ARGV[1], '1')\n"
    "redis.call('PSETEX', KEYS[8], ARGV[1], '1')\n"
    "return 1";

constexpr const char* kDelScript =
    "redis.call('DEL', KEYS[1], KEYS[2], KEYS[3], KEYS[4])\n"
    "return 1";

void SplitPayload(const std::string& value, std::string& prefix, std::string& payload) {
  std::string v = value;
  size_t mark = v.find("|stale");
  if (mark != std::string::npos) {
    v.erase(mark);
  }
  size_t dot = v.rfind('.');
  if (dot == std::string::npos) {
    prefix.clear();
    payload = v;
    return;
  }
  prefix = v.substr(0, dot + 1);
  payload = v.substr(dot + 1);
}

bool ParseInt(const std::string& s, int32_t& out) {
  if (s.empty()) {
    return false;
  }
  int64_t v = 0;
  bool neg = false;
  size_t i = 0;
  if (s[0] == '-') {
    neg = true;
    i = 1;
  }
  if (i >= s.size()) {
    return false;
  }
  for (; i < s.size(); ++i) {
    char c = s[i];
    if (c < '0' || c > '9') {
      return false;
    }
    v = v * 10 + (c - '0');
  }
  out = static_cast<int32_t>(neg ? -v : v);
  return true;
}

size_t ResolvePoolThreads(const DynamicGraphEngine::Config& cfg) {
  if (cfg.pool_threads > 0) {
    return cfg.pool_threads;
  }
  unsigned cores = hardware_cores();
  return cores > 1 ? cores - 1 : 1;
}

std::vector<WorkerCtx> MakeWorkerCtxs(size_t workers, size_t nodes) {
  std::vector<WorkerCtx> ctxs(workers);
  for (auto& ctx : ctxs) {
    ctx.visited.resize(nodes + 1, 0);
    ctx.dist_a.resize(nodes + 1, 0);
    ctx.dist_b.resize(nodes + 1, 0);
  }
  return ctxs;
}

}  // namespace

uint64_t DynamicGraphEngine::hash_u64(uint64_t x) {
  x += 0x9E3779B97F4A7C15ULL;
  x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
  x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
  return x ^ (x >> 31);
}

DynamicGraphEngine::DynamicGraphEngine(const Config& cfg)
    : cfg_(cfg),
      nodes_(cfg.nodes),
      adj_(cfg.nodes + 1),
      buckets_(cfg.buckets ? cfg.buckets : 256),
      dirty_flags_(cfg.nodes + 1),
      worker_ctxs_(MakeWorkerCtxs(ResolvePoolThreads(cfg), cfg.nodes)),
      workers_(ResolvePoolThreads(cfg), cfg.queue_capacity ? cfg.queue_capacity : 4096,
            cfg.pin_threads, 1,
            [this](size_t i) { tls_worker_ctx = &worker_ctxs_[i]; }) {
  if (cfg_.cache_namespace.empty()) {
    cfg_.cache_namespace =
        "g" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) +
        ":";
  }
  if (cfg_.cache_enabled && !cfg_.redis_host.empty()) {
    RedisClient::Config rcfg;
    rcfg.host = cfg_.redis_host;
    rcfg.port = cfg_.redis_port;
    rcfg.io_threads = cfg_.redis_io_threads;
    rcfg.ttl_seconds = cfg_.redis_ttl_seconds;
    rcfg.double_delete_delay_ms = cfg_.double_delete_delay_ms;
    redis_ = std::make_unique<RedisClient>(
        rcfg, [this](std::function<void()> fn) {
          if (!shutdown_.load(std::memory_order_relaxed)) {
            workers_.SubmitInternal(std::move(fn));
          }
        });
  }
  if (!cfg_.wal_path.empty()) {
    wal_ = std::make_unique<WalWriter>(
        cfg_.wal_path, cfg_.wal_sync_mode, cfg_.wal_checkpoint_bytes,
        [this]() {
          if (WriteSnapshot()) {
            wal_->Rotate();
          }
        });
  }
  maint_thread_ = std::thread([this]() { MaintenanceLoop(); });
}

DynamicGraphEngine::~DynamicGraphEngine() { Shutdown(); }

bool DynamicGraphEngine::Submit(QueryRequest&& req) {
  if (shutdown_.load(std::memory_order_relaxed)) {
    return false;
  }
  queries_submitted_.fetch_add(1, std::memory_order_relaxed);
  metrics::Inc("engine_queries_submitted_total");
  metrics::SetGauge("engine_queue_depth", workers_.queue_size());
  req.start_us = static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::microseconds>(
          std::chrono::steady_clock::now().time_since_epoch())
          .count());
  auto sp = std::make_shared<QueryRequest>(std::move(req));
  bool ok = workers_.Submit([this, sp]() { ProcessQuery(sp); });
  if (!ok) {
    shed_count_.fetch_add(1, std::memory_order_relaxed);
    metrics::Inc("engine_shed_total");
  }
  return ok;
}

void DynamicGraphEngine::ProcessQuery(const std::shared_ptr<QueryRequest>& req) {
  WorkerCtx& ctx = *tls_worker_ctx;
  req->epoch_ticket = EpochReclaimer::Instance().Pin();
  if (!redis_ || !redis_->enabled()) {
    switch (req->type) {
      case QueryType::kDistance:
        FinishWithDistance(req, BidirectionalBfs(req->a, req->b, ctx));
        return;
      case QueryType::kCommonFriends:
        FinishWithFriends(req, CommonFriends(req->a, req->b, ctx));
        return;
      case QueryType::kSecondDegree:
        FinishWithFriends(req, SecondDegreeBfs(req->a, ctx));
        return;
    }
    return;
  }

  uint32_t a = req->a;
  uint32_t b = req->b;
  switch (req->type) {
    case QueryType::kDistance: {
      uint32_t lo = std::min(a, b);
      uint32_t hi = std::max(a, b);
      std::vector<std::string> keys = {VersionKey(a), VersionKey(b), DistKey(lo, hi),
                                      DirtyKey(a), DirtyKey(b)};
      redis_->EvalReadAsync(keys, kReadScript3,
                            [this, req](bool ok, const std::string& value) {
        std::string prefix;
        std::string payload;
        SplitPayload(value, prefix, payload);
        int32_t d = 0;
        if (ok && !payload.empty() && ParseInt(payload, d)) {
          cache_hits_.fetch_add(1, std::memory_order_relaxed);
          FinishWithDistance(req, d);
          return;
        }
        cache_misses_.fetch_add(1, std::memory_order_relaxed);
        metrics::Inc("engine_cache_misses_total");
        WorkerCtx& ctx = *tls_worker_ctx;
        d = BidirectionalBfs(req->a, req->b, ctx);
        uint32_t lo = std::min(req->a, req->b);
        uint32_t hi = std::max(req->a, req->b);
        redis_->SetAsync(DistKey(lo, hi), prefix + std::to_string(d));
        FinishWithDistance(req, d);
      });
      return;
    }
    case QueryType::kCommonFriends: {
      uint32_t lo = std::min(a, b);
      uint32_t hi = std::max(a, b);
      std::vector<std::string> keys = {VersionKey(a), VersionKey(b), CfKey(lo, hi),
                                      DirtyKey(a), DirtyKey(b)};
      redis_->EvalReadAsync(keys, kReadScript3,
                            [this, req](bool ok, const std::string& value) {
        std::string prefix;
        std::string payload;
        SplitPayload(value, prefix, payload);
        std::vector<uint32_t> friends;
        if (ok && !payload.empty() && ParseFriends(payload, friends)) {
          cache_hits_.fetch_add(1, std::memory_order_relaxed);
          metrics::Inc("engine_cache_hits_total");
          FinishWithFriends(req, std::move(friends));
          return;
        }
        cache_misses_.fetch_add(1, std::memory_order_relaxed);
        metrics::Inc("engine_cache_misses_total");
        WorkerCtx& ctx = *tls_worker_ctx;
        friends = CommonFriends(req->a, req->b, ctx);
        uint32_t lo = std::min(req->a, req->b);
        uint32_t hi = std::max(req->a, req->b);
        if (friends.size() <= 512) {
          redis_->SetAsync(CfKey(lo, hi), prefix + SerializeFriends(friends));
        }
        FinishWithFriends(req, std::move(friends));
      });
      return;
    }
    case QueryType::kSecondDegree: {
      std::vector<std::string> keys = {VersionKey(a), SdKey(a), DirtyKey(a)};
      redis_->EvalReadAsync(keys, kReadScript2,
                            [this, req](bool ok, const std::string& value) {
        std::string prefix;
        std::string payload;
        SplitPayload(value, prefix, payload);
        std::vector<uint32_t> friends;
        if (ok && !payload.empty() && ParseFriends(payload, friends)) {
          cache_hits_.fetch_add(1, std::memory_order_relaxed);
          metrics::Inc("engine_cache_hits_total");
          FinishWithFriends(req, std::move(friends));
          return;
        }
        cache_misses_.fetch_add(1, std::memory_order_relaxed);
        metrics::Inc("engine_cache_misses_total");
        WorkerCtx& ctx = *tls_worker_ctx;
        friends = SecondDegreeBfs(req->a, ctx);
        if (friends.size() <= cfg_.cache_max_friends) {
          redis_->SetAsync(SdKey(req->a), prefix + SerializeFriends(friends));
        }
        FinishWithFriends(req, std::move(friends));
      });
      return;
    }
  }
}

void DynamicGraphEngine::FinishWithDistance(const std::shared_ptr<QueryRequest>& req,
                                            int32_t dist) {
  EpochReclaimer::Instance().Unpin(req->epoch_ticket);
  QueryResult res;
  res.distance = dist;
  queries_completed_.fetch_add(1, std::memory_order_relaxed);
  metrics::Inc("engine_queries_completed_total");
  if (req->start_us > 0) {
    uint64_t now_us = static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now().time_since_epoch())
            .count());
    metrics::Observe("engine_query_duration_us", now_us - req->start_us);
  }
  if (req->cb) {
    req->cb(std::move(res));
  }
}

void DynamicGraphEngine::FinishWithFriends(const std::shared_ptr<QueryRequest>& req,
                                           std::vector<uint32_t> friends) {
  EpochReclaimer::Instance().Unpin(req->epoch_ticket);
  QueryResult res;
  res.friends = std::move(friends);
  queries_completed_.fetch_add(1, std::memory_order_relaxed);
  metrics::Inc("engine_queries_completed_total");
  if (req->cb) {
    req->cb(std::move(res));
  }
}

int32_t DynamicGraphEngine::BidirectionalBfs(uint32_t src, uint32_t dst,
                                             WorkerCtx& ctx) {
  if (src == dst) {
    return 0;
  }
  const uint32_t max_depth = cfg_.max_bfs_depth;
  uint32_t e1 = ctx.NextEpoch();
  uint32_t e2 = ctx.NextEpoch();

  std::vector<uint32_t>& fa = ctx.frontier_a;
  std::vector<uint32_t>& fb = ctx.frontier_b;
  std::vector<uint32_t>& na = ctx.next_a;
  std::vector<uint32_t>& nb = ctx.next_b;
  fa.clear();
  fb.clear();
  ctx.visited[src] = e1;
  ctx.dist_a[src] = 0;
  fa.push_back(src);
  ctx.visited[dst] = e2;
  ctx.dist_b[dst] = 0;
  fb.push_back(dst);

  uint32_t dist_a = 0;
  uint32_t dist_b = 0;
  int32_t found = -1;

  auto update_found = [&found](int32_t cand) {
    if (cand >= 0 && (found < 0 || cand < found)) {
      found = cand;
    }
  };

  while (!fa.empty() && !fb.empty() && found < 0) {
    if (fa.size() <= fb.size()) {
      if (dist_a >= max_depth) {
        break;
      }
      na.clear();
      for (uint32_t x : fa) {
        ForEachNeighbor(x, [&](uint32_t n) {
          if (ctx.visited[n] == e2) {
            update_found(static_cast<int32_t>(dist_a + 1 + ctx.dist_b[n]));
          } else if (ctx.visited[n] != e1) {
            ctx.visited[n] = e1;
            ctx.dist_a[n] = dist_a + 1;
            na.push_back(n);
          }
        });
      }
      fa.swap(na);
      ++dist_a;
    } else {
      if (dist_b >= max_depth) {
        break;
      }
      nb.clear();
      for (uint32_t x : fb) {
        ForEachNeighbor(x, [&](uint32_t n) {
          if (ctx.visited[n] == e1) {
            update_found(static_cast<int32_t>(dist_b + 1 + ctx.dist_a[n]));
          } else if (ctx.visited[n] != e2) {
            ctx.visited[n] = e2;
            ctx.dist_b[n] = dist_b + 1;
            nb.push_back(n);
          }
        });
      }
      fb.swap(nb);
      ++dist_b;
    }
  }
  return found;
}

std::vector<uint32_t> DynamicGraphEngine::SecondDegreeBfs(uint32_t src,
                                                          WorkerCtx& ctx) {
  uint32_t e = ctx.NextEpoch();
  std::vector<uint32_t>& level1 = ctx.scratch;
  std::vector<uint32_t>& level2 = ctx.scratch2;
  level1.clear();
  level2.clear();
  ctx.visited[src] = e;
  ForEachNeighbor(src, [&](uint32_t n) {
    if (ctx.visited[n] != e) {
      ctx.visited[n] = e;
      level1.push_back(n);
    }
  });
  for (uint32_t x : level1) {
    ForEachNeighbor(x, [&](uint32_t n) {
      if (ctx.visited[n] != e) {
        ctx.visited[n] = e;
        level2.push_back(n);
      }
    });
  }
  std::sort(level2.begin(), level2.end());
  return level2;
}

std::vector<uint32_t> DynamicGraphEngine::CommonFriends(uint32_t u, uint32_t v,
                                                        WorkerCtx& ctx) {
  std::vector<uint32_t>& sa = ctx.scratch;
  std::vector<uint32_t>& sb = ctx.scratch2;
  storage::NodeAdjacency& au = adj_[u];
  storage::NodeAdjacency& av = adj_[v];
  const storage::AdjacencyData* du = au.data.load(std::memory_order_acquire);
  const storage::AdjacencyData* dv = av.data.load(std::memory_order_acquire);
  sa.clear();
  if (du != nullptr) {
    storage::node_iterate(du->head, du->tail, du->len.load(std::memory_order_acquire),
                          [&sa](uint32_t id) { sa.push_back(id); });
  }
  sb.clear();
  if (dv != nullptr) {
    storage::node_iterate(dv->head, dv->tail, dv->len.load(std::memory_order_acquire),
                          [&sb](uint32_t id) { sb.push_back(id); });
  }
  std::sort(sa.begin(), sa.end());
  std::sort(sb.begin(), sb.end());
  std::vector<uint32_t> out;
  out.resize(std::min(sa.size(), sb.size()));
  size_t n = IntersectSorted(sa.data(), sa.size(), sb.data(), sb.size(), out.data());
  out.resize(n);
  return out;
}

void DynamicGraphEngine::ForEachNeighbor(uint32_t u,
                                         const std::function<void(uint32_t)>& fn) {
  storage::NodeAdjacency& adj = adj_[u];
  const storage::AdjacencyData* d = adj.data.load(std::memory_order_acquire);
  if (d != nullptr) {
    storage::node_iterate(d->head, d->tail, d->len.load(std::memory_order_acquire), fn);
  }
}

bool DynamicGraphEngine::PrepareAddToNode(uint32_t node, uint32_t id) {
  storage::NodeAdjacency& adj = adj_[node];
  // 修复:重复边判定必须在桶独占锁内覆盖完整邻接(压缩块+尾部)。
  // 原实现 tail_add 只检查尾部,已进入压缩块的边可被再次插入,
  // 破坏简单图集合语义与差值编码的严格递增前提。
  // 桶锁保证检查与插入原子,无并发窗口。
  if (storage::node_contains(adj, id)) {
    return false;
  }
  for (int attempt = 0; attempt < 2; ++attempt) {
    storage::TailAddResult r = storage::tail_add(pool_, adj, id);
    if (r == storage::TailAddResult::kDuplicate) {
      return false;
    }
    if (r == storage::TailAddResult::kAdded) {
      return true;
    }
    std::vector<uint32_t> live;
    storage::gather_live(adj, live);
#ifdef DGE_VERIFY
    for (size_t k = 1; k < live.size(); ++k) {
      if (live[k] == live[k - 1]) {
        fprintf(stderr,
                "PRE-REBUILD DUP in PrepareAddToNode(node=%u, id=%u): live[%zu]=%u size=%zu "
                "block_count=%u\n", node, id, k, live[k], live.size(), adj.block_count);
        exit(4);
      }
    }
#endif
    if (adj.block_count > 0 || live.size() > cfg_.keep_tail_threshold) {
      storage::rebuild_blocks(pool_, adj, live);
#ifdef DGE_VERIFY
      {
        std::vector<uint32_t> chk;
        storage::gather_live(adj, chk);
        for (size_t k = 1; k < chk.size(); ++k) {
          if (chk[k] == chk[k - 1]) {
            fprintf(stderr, "POST-REBUILD DUP node=%u id=%u: live[%zu]=%u size=%zu\n", node,
                    id, k, chk[k], chk.size());
            exit(5);
          }
        }
      }
#endif
    } else {
      storage::grow_tail(pool_, adj, live);
    }
    compactions_.fetch_add(1, std::memory_order_relaxed);
  }
  return false;
}

bool DynamicGraphEngine::AddEdge(uint32_t u, uint32_t v) {
  if (u == 0 || v == 0 || u > nodes_ || v > nodes_ || u == v) {
    return false;
  }
  size_t b1 = BucketOf(u);
  size_t b2 = BucketOf(v);
  if (b1 > b2) {
    std::swap(b1, b2);
    std::swap(u, v);
  }
  std::unique_lock<std::shared_mutex> lk1(buckets_[b1].mu);
  std::unique_lock<std::shared_mutex> lk2(buckets_[b2].mu, std::defer_lock);
  if (b2 != b1) {
    lk2.lock();
  }
  if (!PrepareAddToNode(u, v)) {
    return false;
  }
  if (!PrepareAddToNode(v, u)) {
    storage::tail_remove(pool_, adj_[u], v);
    return false;
  }
  updates_.fetch_add(1, std::memory_order_relaxed);
  metrics::Inc("engine_updates_total");
  if (wal_ && wal_enabled_.load(std::memory_order_relaxed) &&
      !recovering_.load(std::memory_order_relaxed)) {
    wal_->Append(0x01, u, v);
  }
#ifdef DGE_VERIFY
  {
    for (uint32_t node : {u, v}) {
      std::vector<uint32_t> checkv;
      storage::gather_live(adj_[node], checkv);
      for (size_t k = 1; k < checkv.size(); ++k) {
        if (checkv[k] == checkv[k - 1]) {
          fprintf(stderr, "ADJ DUP AFTER ADD(%u,%u): node %u dup=%u idx=%zu\n", u, v, node,
                  checkv[k], k);
          exit(3);
        }
      }
    }
  }
#endif
  InvalidateCache(u, v);
  return true;
}

bool DynamicGraphEngine::RemoveEdge(uint32_t u, uint32_t v) {
  if (u == 0 || v == 0 || u > nodes_ || v > nodes_ || u == v) {
    return false;
  }
  size_t b1 = BucketOf(u);
  size_t b2 = BucketOf(v);
  if (b1 > b2) {
    std::swap(b1, b2);
    std::swap(u, v);
  }
  std::unique_lock<std::shared_mutex> lk1(buckets_[b1].mu);
  std::unique_lock<std::shared_mutex> lk2(buckets_[b2].mu, std::defer_lock);
  if (b2 != b1) {
    lk2.lock();
  }

  bool found_u = storage::tail_remove(pool_, adj_[u], v);
  if (!found_u) {
    storage::block_remove(pool_, adj_[u], v, found_u);
  }
  bool found_v = storage::tail_remove(pool_, adj_[v], u);
  if (!found_v) {
    storage::block_remove(pool_, adj_[v], u, found_v);
  }
  bool found = found_u || found_v;
  if (!found) {
    return false;
  }
  updates_.fetch_add(1, std::memory_order_relaxed);
  metrics::Inc("engine_updates_total");
  if (wal_ && wal_enabled_.load(std::memory_order_relaxed) &&
      !recovering_.load(std::memory_order_relaxed)) {
    wal_->Append(0x02, u, v);
  }
  if (storage::node_needs_compaction(adj_[u], cfg_.hole_ratio_threshold)) {
    MarkDirty(u);
  }
  if (storage::node_needs_compaction(adj_[v], cfg_.hole_ratio_threshold)) {
    MarkDirty(v);
  }
  InvalidateCache(u, v);
  return true;
}

void DynamicGraphEngine::MarkDirty(uint32_t node) {
  uint8_t expected = 0;
  if (dirty_flags_[node].compare_exchange_strong(expected, 1)) {
    Bucket& b = buckets_[BucketOf(node)];
    std::lock_guard<std::mutex> lk(b.dirty_mu);
    b.dirty.push_back(node);
    maint_cv_.notify_one();
  }
}

void DynamicGraphEngine::CompactNode(uint32_t node) {
  Bucket& b = buckets_[BucketOf(node)];
  std::unique_lock<std::shared_mutex> lk(b.mu);
  if (dirty_flags_[node].load(std::memory_order_relaxed) == 0) {
    return;
  }
  dirty_flags_[node].store(0, std::memory_order_relaxed);
  storage::NodeAdjacency& adj = adj_[node];
  storage::gather_live(adj, maint_scratch_);
  if (adj.block_count > 0 || maint_scratch_.size() > cfg_.keep_tail_threshold) {
    storage::rebuild_blocks(pool_, adj, maint_scratch_);
  } else {
    storage::grow_tail(pool_, adj, maint_scratch_);
  }
  compactions_.fetch_add(1, std::memory_order_relaxed);
  metrics::Inc("engine_compactions_total");
}

void DynamicGraphEngine::MaintenanceLoop() {
  set_thread_low_priority();
  std::vector<uint32_t> batch;
  while (true) {
    {
      std::unique_lock<std::mutex> lk(maint_mu_);
      maint_cv_.wait_for(lk, std::chrono::milliseconds(5),
                         [this] { return maint_stop_; });
      if (maint_stop_) {
        return;
      }
    }
    batch.clear();
    for (Bucket& b : buckets_) {
      std::lock_guard<std::mutex> lk(b.dirty_mu);
      batch.insert(batch.end(), b.dirty.begin(), b.dirty.end());
      b.dirty.clear();
    }
    EpochReclaimer::Instance().Sweep();
    if (batch.empty()) {
      continue;
    }
    maintenance_scans_.fetch_add(1, std::memory_order_relaxed);
    for (uint32_t node : batch) {
      if (dirty_flags_[node].load(std::memory_order_relaxed) != 0) {
        CompactNode(node);
      }
    }
    EpochReclaimer::Instance().Sweep();
  }
}

void DynamicGraphEngine::InvalidateCache(uint32_t u, uint32_t v) {
  if (!cache_invalidation_.load(std::memory_order_relaxed) || !redis_ ||
      !redis_->enabled()) {
    return;
  }
  uint32_t lo = std::min(u, v);
  uint32_t hi = std::max(u, v);
  std::vector<std::string> keys = {VersionKey(u), VersionKey(v), DistKey(lo, hi),
                                   CfKey(lo, hi), SdKey(u), SdKey(v), DirtyKey(u), DirtyKey(v)};
  redis_->EvalWriteAsync(keys, kInvalidateScript, nullptr,
                         static_cast<int>(cfg_.dirty_window_ms));
  std::vector<std::string> del_keys = {DistKey(lo, hi), CfKey(lo, hi), SdKey(u), SdKey(v)};
  timer_.Add(cfg_.double_delete_delay_ms, [this, del_keys]() {
    redis_->EvalWriteAsync(del_keys, kDelScript);
  });
}

std::string DynamicGraphEngine::DistKey(uint32_t u, uint32_t v) const {
  return cfg_.cache_namespace + "d:" + std::to_string(std::min(u, v)) + ":" +
         std::to_string(std::max(u, v));
}

std::string DynamicGraphEngine::CfKey(uint32_t u, uint32_t v) const {
  return cfg_.cache_namespace + "cf:" + std::to_string(std::min(u, v)) + ":" +
         std::to_string(std::max(u, v));
}

std::string DynamicGraphEngine::SdKey(uint32_t u) const {
  return cfg_.cache_namespace + "sd:" + std::to_string(u);
}

std::string DynamicGraphEngine::VersionKey(uint32_t u) const {
  return cfg_.cache_namespace + "v:" + std::to_string(u);
}

std::string DynamicGraphEngine::DirtyKey(uint32_t u) const {
  return cfg_.cache_namespace + "dirty:" + std::to_string(u);
}

std::string DynamicGraphEngine::SerializeFriends(const std::vector<uint32_t>& f) {
  std::ostringstream oss;
  if (f.empty()) {
    return "0";
  }
  for (size_t i = 0; i < f.size(); ++i) {
    if (i > 0) {
      oss << ",";
    }
    oss << f[i];
  }
  return oss.str();
}

bool DynamicGraphEngine::ParseFriends(const std::string& s, std::vector<uint32_t>& out) {
  out.clear();
  if (s == "0") {
    return true;
  }
  size_t pos = 0;
  while (pos < s.size()) {
    size_t comma = s.find(',', pos);
    std::string token = s.substr(pos, comma == std::string::npos ? std::string::npos
                                                                 : comma - pos);
    uint32_t id = 0;
    for (char c : token) {
      if (c < '0' || c > '9') {
        return false;
      }
      id = id * 10 + static_cast<uint32_t>(c - '0');
    }
    out.push_back(id);
    if (comma == std::string::npos) {
      break;
    }
    pos = comma + 1;
  }
  return true;
}

void DynamicGraphEngine::DedupCheck(const std::string& key, int ttl_seconds,
                                  std::function<void(bool)> cb) {
  if (!redis_ || !redis_->enabled()) {
    if (cb) {
      cb(true);
    }
    return;
  }
  redis_->SetNxAsync(key, ttl_seconds, std::move(cb));
}

bool DynamicGraphEngine::WriteSnapshot() {
  if (cfg_.snapshot_path.empty() || !wal_) {
    return false;
  }
  uint64_t lsn = wal_->last_seq();
  bool ok = WriteSnapshotFile(
      cfg_.snapshot_path, lsn, [this](const std::function<void(uint64_t, uint64_t)>& emit) {
        for (uint32_t u = 1; u <= nodes_; ++u) {
          Bucket& b = buckets_[BucketOf(u)];
          std::shared_lock<std::shared_mutex> lk(b.mu);
          storage::NodeAdjacency& adj = adj_[u];
          storage::node_iterate(adj, [&](uint32_t v) {
            if (u < v) {
              emit(u, v);
            }
          });
        }
        return true;
      });
  metrics::Inc("engine_snapshots_total");
  return ok;
}

bool DynamicGraphEngine::Recover() {
  if (cfg_.wal_path.empty()) {
    return true;
  }
  recovering_.store(true, std::memory_order_relaxed);
  bool ok = RecoverFromWal(cfg_.wal_path, cfg_.snapshot_path,
                           [this](uint8_t op, uint64_t u, uint64_t v) {
                             if (op == 0x01) {
                               AddEdge(static_cast<uint32_t>(u), static_cast<uint32_t>(v));
                             } else {
                               RemoveEdge(static_cast<uint32_t>(u), static_cast<uint32_t>(v));
                             }
                           });
  recovering_.store(false, std::memory_order_relaxed);
  metrics::Inc("engine_recoveries_total");
  return ok;
}

EngineStats DynamicGraphEngine::stats() const {
  EngineStats s;
  s.queries_submitted = queries_submitted_.load(std::memory_order_relaxed);
  s.queries_completed = queries_completed_.load(std::memory_order_relaxed);
  s.shed_count = shed_count_.load(std::memory_order_relaxed);
  s.cache_hits = cache_hits_.load(std::memory_order_relaxed);
  s.cache_misses = cache_misses_.load(std::memory_order_relaxed);
  s.cache_errors = redis_ ? redis_->cache_errors() : 0;
  s.dropped_writes = redis_ ? redis_->dropped_writes() : 0;
  s.cache_gets = s.cache_hits + s.cache_misses;
  s.updates = updates_.load(std::memory_order_relaxed);
  s.compactions = compactions_.load(std::memory_order_relaxed);
  s.maintenance_scans = maintenance_scans_.load(std::memory_order_relaxed);

  size_t visited_bytes = 0;
  size_t scratch_bytes = 0;
  for (const auto& ctx : worker_ctxs_) {
    visited_bytes +=
        (ctx.visited.capacity() + ctx.dist_a.capacity() + ctx.dist_b.capacity()) *
        sizeof(uint32_t);
    scratch_bytes +=
        (ctx.frontier_a.capacity() + ctx.frontier_b.capacity() + ctx.next_a.capacity() +
         ctx.next_b.capacity() + ctx.scratch.capacity() + ctx.scratch2.capacity()) *
        sizeof(uint32_t);
  }
  s.memory_bytes = pool_.bytes_in_use() + nodes_ * sizeof(storage::NodeAdjacency) +
                   visited_bytes + scratch_bytes;
  return s;
}

void DynamicGraphEngine::Shutdown() {
  shutdown_.store(true, std::memory_order_relaxed);
  {
    std::lock_guard<std::mutex> lk(maint_mu_);
    maint_stop_ = true;
  }
  maint_cv_.notify_all();
  if (maint_thread_.joinable()) {
    maint_thread_.join();
  }
  timer_.Shutdown();
  if (redis_) {
    redis_->Shutdown();
  }
  workers_.Shutdown();
  EpochReclaimer::Instance().DrainPool(&pool_);
}

}  // namespace dge
