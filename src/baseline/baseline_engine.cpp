#include "baseline/baseline_engine.h"

#include <algorithm>
#include <cstdlib>
#include <cstring>

#include "common/affinity.h"

namespace dge {

namespace {

size_t ResolveThreads(const BaselineGraphEngine::Config& cfg) {
  if (cfg.pool_threads > 0) {
    return cfg.pool_threads;
  }
  unsigned cores = hardware_cores();
  return cores > 1 ? cores - 1 : 1;
}

}  // namespace

BaselineGraphEngine::BaselineGraphEngine(const Config& cfg)
    : nodes_(cfg.nodes),
      adjacency_(cfg.nodes + 1),
      pool_(ResolveThreads(cfg), cfg.queue_capacity, false, 0) {}

BaselineGraphEngine::~BaselineGraphEngine() { Shutdown(); }

bool BaselineGraphEngine::Submit(QueryRequest&& req) {
  if (shutdown_.load(std::memory_order_relaxed)) {
    return false;
  }
  queries_submitted_.fetch_add(1, std::memory_order_relaxed);
  auto sp = std::make_shared<QueryRequest>(std::move(req));
  bool ok = pool_.Submit([this, sp]() { ProcessQuery(sp); });
  if (!ok) {
    shed_count_.fetch_add(1, std::memory_order_relaxed);
  }
  return ok;
}

void BaselineGraphEngine::ProcessQuery(const std::shared_ptr<QueryRequest>& req) {
  QueryResult res;
  switch (req->type) {
    case QueryType::kDistance:
      res.distance = BfsDistance(req->a, req->b);
      break;
    case QueryType::kCommonFriends:
      res.friends = CommonFriends(req->a, req->b);
      break;
    case QueryType::kSecondDegree:
      res.friends = SecondDegree(req->a);
      break;
  }
  queries_completed_.fetch_add(1, std::memory_order_relaxed);
  if (req->cb) {
    req->cb(std::move(res));
  }
}

int32_t BaselineGraphEngine::BfsDistance(uint32_t src, uint32_t dst) {
  if (src == dst) {
    return 0;
  }
  std::shared_lock<std::shared_mutex> lk(mu_);
  uint8_t* va = static_cast<uint8_t*>(std::calloc(nodes_ + 1, 1));
  uint8_t* vb = static_cast<uint8_t*>(std::calloc(nodes_ + 1, 1));
  uint32_t* da = static_cast<uint32_t*>(std::calloc(nodes_ + 1, sizeof(uint32_t)));
  uint32_t* db = static_cast<uint32_t*>(std::calloc(nodes_ + 1, sizeof(uint32_t)));
  if (va == nullptr || vb == nullptr || da == nullptr || db == nullptr) {
    std::free(va);
    std::free(vb);
    std::free(da);
    std::free(db);
    return -1;
  }
  std::vector<uint32_t> fa;
  std::vector<uint32_t> fb;
  std::vector<uint32_t> na;
  std::vector<uint32_t> nb;
  va[src] = 1;
  da[src] = 0;
  fa.push_back(src);
  vb[dst] = 1;
  db[dst] = 0;
  fb.push_back(dst);
  uint32_t dist_a = 0;
  uint32_t dist_b = 0;
  int32_t found = -1;
  const uint32_t max_depth = 32;
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
        for (uint32_t n : adjacency_[x]) {
          if (vb[n]) {
            update_found(static_cast<int32_t>(dist_a + 1 + db[n]));
          } else if (!va[n]) {
            va[n] = 1;
            da[n] = dist_a + 1;
            na.push_back(n);
          }
        }
      }
      fa.swap(na);
      ++dist_a;
    } else {
      if (dist_b >= max_depth) {
        break;
      }
      nb.clear();
      for (uint32_t x : fb) {
        for (uint32_t n : adjacency_[x]) {
          if (va[n]) {
            update_found(static_cast<int32_t>(dist_b + 1 + da[n]));
          } else if (!vb[n]) {
            vb[n] = 1;
            db[n] = dist_b + 1;
            nb.push_back(n);
          }
        }
      }
      fb.swap(nb);
      ++dist_b;
    }
  }
  std::free(va);
  std::free(vb);
  std::free(da);
  std::free(db);
  return found;
}

std::vector<uint32_t> BaselineGraphEngine::SecondDegree(uint32_t src) {
  std::shared_lock<std::shared_mutex> lk(mu_);
  std::vector<uint8_t> visited(nodes_ + 1, 0);
  std::vector<uint32_t> level1;
  std::vector<uint32_t> level2;
  visited[src] = 1;
  for (uint32_t n : adjacency_[src]) {
    if (!visited[n]) {
      visited[n] = 1;
      level1.push_back(n);
    }
  }
  for (uint32_t x : level1) {
    for (uint32_t n : adjacency_[x]) {
      if (!visited[n]) {
        visited[n] = 1;
        level2.push_back(n);
      }
    }
  }
  std::sort(level2.begin(), level2.end());
  return level2;
}

std::vector<uint32_t> BaselineGraphEngine::CommonFriends(uint32_t u, uint32_t v) {
  std::shared_lock<std::shared_mutex> lk(mu_);
  std::vector<uint32_t> sa(adjacency_[u].begin(), adjacency_[u].end());
  std::vector<uint32_t> sb(adjacency_[v].begin(), adjacency_[v].end());
  std::sort(sa.begin(), sa.end());
  std::sort(sb.begin(), sb.end());
  std::vector<uint32_t> out;
  size_t i = 0, j = 0;
  while (i < sa.size() && j < sb.size()) {
    if (sa[i] < sb[j]) {
      ++i;
    } else if (sb[j] < sa[i]) {
      ++j;
    } else {
      out.push_back(sa[i]);
      ++i;
      ++j;
    }
  }
  return out;
}

bool BaselineGraphEngine::AddEdge(uint32_t u, uint32_t v) {
  if (u == 0 || v == 0 || u > nodes_ || v > nodes_ || u == v) {
    return false;
  }
  std::unique_lock<std::shared_mutex> lk(mu_);
  for (uint32_t n : adjacency_[u]) {
    if (n == v) {
      return false;
    }
  }
  adjacency_[u].push_back(v);
  adjacency_[v].push_back(u);
  updates_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

bool BaselineGraphEngine::RemoveEdge(uint32_t u, uint32_t v) {
  if (u == 0 || v == 0 || u > nodes_ || v > nodes_ || u == v) {
    return false;
  }
  std::unique_lock<std::shared_mutex> lk(mu_);
  bool found = false;
  auto it = std::find(adjacency_[u].begin(), adjacency_[u].end(), v);
  if (it != adjacency_[u].end()) {
    adjacency_[u].erase(it);
    found = true;
  }
  auto it2 = std::find(adjacency_[v].begin(), adjacency_[v].end(), u);
  if (it2 != adjacency_[v].end()) {
    adjacency_[v].erase(it2);
  }
  if (!found) {
    return false;
  }
  updates_.fetch_add(1, std::memory_order_relaxed);
  return true;
}

EngineStats BaselineGraphEngine::stats() const {
  EngineStats s;
  s.queries_submitted = queries_submitted_.load(std::memory_order_relaxed);
  s.queries_completed = queries_completed_.load(std::memory_order_relaxed);
  s.shed_count = shed_count_.load(std::memory_order_relaxed);
  s.updates = updates_.load(std::memory_order_relaxed);
  std::shared_lock<std::shared_mutex> lk(mu_);
  size_t capacity = 0;
  for (const auto& l : adjacency_) {
    capacity += l.capacity();
  }
  s.memory_bytes = nodes_ * sizeof(std::vector<uint32_t>) + capacity * sizeof(uint32_t);
  return s;
}

void BaselineGraphEngine::Shutdown() {
  shutdown_.store(true, std::memory_order_relaxed);
  pool_.Shutdown();
}

}  // namespace dge
