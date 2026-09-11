#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cmath>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "baseline/baseline_engine.h"
#include "common/affinity.h"
#include "graph/dynamic_graph_engine.h"
#include "graph/engine_interface.h"

using namespace dge;

namespace {

struct BenchArgs {
  size_t nodes = 300000;
  uint32_t degree = 20;
  size_t clients = 16;
  int seconds = 10;
  int warmup = 2;
  size_t pool_threads = 0;
  size_t queue_capacity = 4096;
  size_t buckets = 256;
  bool cache = true;
  std::string redis_host = "127.0.0.1";
  int redis_port = 6379;
  int redis_io_threads = 6;
  std::string mode = "both";
  uint64_t seed = 42;
  uint64_t mix_cf = 55;
  uint64_t mix_sd = 25;
  uint64_t mix_dist = 10;
  uint64_t mix_add = 5;
  uint64_t mix_del = 5;
  double zipf = 1.0;
};

constexpr int kLatBuckets = 40;
constexpr uint64_t kBackoffBaseUs = 1000;
constexpr int kBackoffMaxShift = 6;

struct ZipfSampler {
  std::vector<double> cdf;
  ZipfSampler() = default;
  ZipfSampler(size_t n, double s) { Build(n, s); }
  void Build(size_t n, double s) {
    cdf.resize(n + 1);
    double sum = 0;
    for (size_t i = 1; i <= n; ++i) {
      sum += 1.0 / std::pow(static_cast<double>(i), s);
    }
    double acc = 0;
    cdf[0] = 0;
    for (size_t i = 1; i <= n; ++i) {
      acc += 1.0 / std::pow(static_cast<double>(i), s);
      cdf[i] = acc / sum;
    }
  }
  uint32_t Sample(std::mt19937_64& rng) const {
    if (cdf.size() <= 1) {
      return 0;
    }
    double u = static_cast<double>(rng() >> 11) / (1ULL << 53);
    size_t lo = 1, hi = cdf.size() - 1;
    while (lo < hi) {
      size_t mid = (lo + hi) / 2;
      if (cdf[mid] < u) {
        lo = mid + 1;
      } else {
        hi = mid;
      }
    }
    return static_cast<uint32_t>(lo);
  }
};

struct BenchCtx {
  std::array<std::atomic<uint64_t>, kLatBuckets> hist;
  std::atomic<uint64_t> completed{0};
  std::atomic<uint64_t> sum_us{0};
  std::atomic<uint64_t> shed{0};
  std::atomic<uint64_t> updates_done{0};
  std::atomic<uint64_t> max_us{0};
};

void Record(BenchCtx& c, uint64_t us) {
  int b = us < 2 ? 0 : 63 - __builtin_clzll(us);
  if (b < 0) {
    b = 0;
  }
  if (b >= kLatBuckets) {
    b = kLatBuckets - 1;
  }
  c.hist[b].fetch_add(1, std::memory_order_relaxed);
  c.sum_us.fetch_add(us, std::memory_order_relaxed);
  c.completed.fetch_add(1, std::memory_order_relaxed);
  uint64_t prev = c.max_us.load(std::memory_order_relaxed);
  while (us > prev && !c.max_us.compare_exchange_weak(prev, us)) {
  }
}

void SleepUs(uint64_t us) {
  std::this_thread::sleep_for(std::chrono::microseconds(us));
}

void BuildGraph(IGraphEngine& engine, const BenchArgs& args) {
  std::mt19937_64 rng(args.seed ^ 0x5DEECE66DULL);
  uint32_t m = args.degree;
  uint32_t n = static_cast<uint32_t>(args.nodes);
  std::vector<uint32_t> deg(n + 1, 0);
  uint32_t max_deg = 0;
  auto attach = [&](uint32_t u, uint32_t v) {
    if (engine.AddEdge(u, v)) {
      ++deg[u];
      ++deg[v];
      max_deg = std::max(max_deg, std::max(deg[u], deg[v]));
    }
  };
  for (uint32_t i = 1; i <= std::min<uint32_t>(m + 1, n); ++i) {
    for (uint32_t j = 1; j < i; ++j) {
      attach(i, j);
    }
  }
  for (uint32_t i = std::min<uint32_t>(m + 1, n) + 1; i <= n; ++i) {
    uint32_t picked = 0;
    uint32_t guard = 0;
    std::vector<uint32_t> targets;
    while (picked < m && guard < 10000) {
      ++guard;
      uint32_t r = 1 + static_cast<uint32_t>(rng() % (i - 1));
      bool dup = false;
      for (uint32_t t : targets) {
        if (t == r) {
          dup = true;
          break;
        }
      }
      if (dup) {
        continue;
      }
      if (max_deg == 0 || (rng() % (max_deg + 1)) <= deg[r]) {
        targets.push_back(r);
        ++picked;
      }
    }
    for (uint32_t t : targets) {
      attach(i, t);
    }
  }
}

enum class OpType : uint8_t { kCommonFriends, kSecondDegree, kDistance, kAddEdge, kRemoveEdge };

void ClientLoop(IGraphEngine& engine, BenchCtx& ctx, const BenchArgs& args, size_t client_id,
                const std::vector<std::pair<uint32_t, uint32_t>>& edges,
                std::atomic<bool>& run, std::atomic<uint64_t>& seq_counter,
                const ZipfSampler& zipf) {
  std::mt19937_64 rng(args.seed ^ (client_id * 0x9E3779B97F4A7C15ULL));
  uint32_t n = static_cast<uint32_t>(args.nodes);
  bool use_zipf = args.zipf > 0.0 && zipf.cdf.size() > 1;
  auto pick_node = [&]() -> uint32_t {
    uint32_t u;
    if (use_zipf) {
      u = zipf.Sample(rng);
    } else {
      u = 1 + static_cast<uint32_t>(rng() % n);
    }
    return u;
  };
  const uint64_t cf_p = 55;
  const uint64_t sd_p = 25;
  const uint64_t dist_p = 10;
  const uint64_t add_p = 5;
  while (run.load(std::memory_order_relaxed)) {
    uint64_t roll = rng() % 100;
    OpType op;
    if (roll < cf_p) {
      op = OpType::kCommonFriends;
    } else if (roll < cf_p + sd_p) {
      op = OpType::kSecondDegree;
    } else if (roll < cf_p + sd_p + dist_p) {
      op = OpType::kDistance;
    } else if (roll < cf_p + sd_p + dist_p + add_p) {
      op = OpType::kAddEdge;
    } else {
      op = OpType::kRemoveEdge;
    }

    if (op == OpType::kAddEdge) {
      uint32_t u = 1 + static_cast<uint32_t>(rng() % n);
      uint32_t v = 1 + static_cast<uint32_t>(rng() % n);
      if (u == v) {
        v = v % n + 1;
      }
      if (engine.AddEdge(u, v)) {
        ctx.updates_done.fetch_add(1, std::memory_order_relaxed);
      }
      continue;
    }
    if (op == OpType::kRemoveEdge) {
      if (edges.empty()) {
        continue;
      }
      const auto& e = edges[rng() % edges.size()];
      if (engine.RemoveEdge(e.first, e.second)) {
        ctx.updates_done.fetch_add(1, std::memory_order_relaxed);
      }
      continue;
    }

    uint32_t u = pick_node();
    uint32_t v = pick_node();
    if (u == v) {
      v = v % n + 1;
    }
    QueryType qt;
    switch (op) {
      case OpType::kCommonFriends:
        qt = QueryType::kCommonFriends;
        break;
      case OpType::kSecondDegree:
        qt = QueryType::kSecondDegree;
        break;
      default:
        qt = QueryType::kDistance;
        break;
    }

    int backoff_shift = 0;
    while (run.load(std::memory_order_relaxed)) {
      auto start = std::chrono::steady_clock::now();
      QueryRequest req;
      req.seq = seq_counter.fetch_add(1, std::memory_order_relaxed);
      req.type = qt;
      req.a = u;
      req.b = v;
      req.cb = [&ctx, start](QueryResult&&) {
        auto now = std::chrono::steady_clock::now();
        uint64_t us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(now - start).count());
        Record(ctx, us);
      };
      if (engine.Submit(std::move(req))) {
        break;
      }
      ctx.shed.fetch_add(1, std::memory_order_relaxed);
      ++backoff_shift;
      uint64_t sleep_us = kBackoffBaseUs << std::min(backoff_shift, kBackoffMaxShift);
      SleepUs(sleep_us);
    }
  }
}

void PrintLatency(BenchCtx& ctx, const char* label) {
  uint64_t total = ctx.completed.load(std::memory_order_relaxed);
  if (total == 0) {
    printf("  %-8s: (no completions)\n", label);
    return;
  }
  std::array<uint64_t, kLatBuckets> h;
  uint64_t cum = 0;
  for (int i = 0; i < kLatBuckets; ++i) {
    h[i] = ctx.hist[i].load(std::memory_order_relaxed);
    cum += h[i];
  }
  auto pctl = [&](double p) {
    uint64_t target = static_cast<uint64_t>(cum * p);
    uint64_t acc = 0;
    for (int i = 0; i < kLatBuckets; ++i) {
      acc += h[i];
      if (acc >= target) {
        return (1ULL << i);
      }
    }
    return 1ULL << (kLatBuckets - 1);
  };
  double avg = static_cast<double>(ctx.sum_us.load(std::memory_order_relaxed)) / total;
  printf("  %-8s: avg %.0fus | p50 %lluus | p90 %lluus | p99 %lluus | p999 %lluus | max %lluus\n",
         label, avg, static_cast<unsigned long long>(pctl(0.50)),
         static_cast<unsigned long long>(pctl(0.90)),
         static_cast<unsigned long long>(pctl(0.99)),
         static_cast<unsigned long long>(pctl(0.999)),
         static_cast<unsigned long long>(ctx.max_us.load(std::memory_order_relaxed)));
}

void PrintStats(IGraphEngine& engine, BenchCtx& ctx, int seconds, const char* label,
                const char* note) {
  dge::EngineStats s = engine.stats();
  double qps = static_cast<double>(ctx.completed.load(std::memory_order_relaxed)) / seconds;
  printf("== %s ==%s\n", label, note ? note : "");
  printf("  completed queries : %llu (qps %.0f)\n",
         static_cast<unsigned long long>(ctx.completed.load(std::memory_order_relaxed)), qps);
  printf("  shed (503)        : %llu\n",
         static_cast<unsigned long long>(ctx.shed.load(std::memory_order_relaxed)));
  printf("  successful updates: %llu\n",
         static_cast<unsigned long long>(ctx.updates_done.load(std::memory_order_relaxed)));
  if (s.cache_gets > 0 || s.cache_errors > 0) {
    printf("  cache hit rate    : %.1f%% (gets %llu, errors %llu, dropped writes %llu)\n",
           s.cache_hit_rate() * 100.0,
           static_cast<unsigned long long>(s.cache_gets),
           static_cast<unsigned long long>(s.cache_errors),
           static_cast<unsigned long long>(s.dropped_writes));
  }
  if (s.compactions > 0) {
    printf("  compactions       : %llu (maintenance scans %llu)\n",
           static_cast<unsigned long long>(s.compactions),
           static_cast<unsigned long long>(s.maintenance_scans));
  }
  printf("  memory            : %.1f MB\n", s.memory_bytes / 1024.0 / 1024.0);
  PrintLatency(ctx, "latency");
}

void Usage() {
  printf(
      "usage: bench [options]\n"
      "  --nodes N         节点数 (default 300000)\n"
      "  --degree M        每个节点平均好友数 (default 20)\n"
      "  --clients C       并发客户端线程 (default 16)\n"
      "  --seconds S       测量时长秒 (default 10)\n"
      "  --warmup W        预热秒 (default 2)\n"
      "  --pool T          计算线程池线程数 (default cores-1)\n"
      "  --queue Q         任务队列容量 (default 4096)\n"
      "  --buckets K       哈希分片桶数 (default 256)\n"
      "  --no-redis        禁用Redis缓存\n"
      "  --io N            Redis I/O线程数 (default 4)\n"
      "  --redis H:P       Redis地址 (default 127.0.0.1:6379)\n"
      "  --mode M          optimized|baseline|both (default both)\n"
      "  --mix A,B,C,D,E   查询占比%%: 共同好友,二度,距离,加边,删边 (default 55,25,10,5,5)\n"
      "  --zipf S          Zipf偏斜度, 0=均匀 (default 1.0)\n"
      "  --seed S          随机种子 (default 42)\n");
}

bool ParseArgs(int argc, char** argv, BenchArgs& args) {
  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto need = [&](const std::string& name) -> const char* {
      if (i + 1 >= argc) {
        printf("missing value for %s\n", name.c_str());
        exit(1);
      }
      return argv[++i];
    };
    if (a == "--nodes") {
      args.nodes = strtoull(need(a), nullptr, 10);
    } else if (a == "--degree") {
      args.degree = static_cast<uint32_t>(strtoul(need(a), nullptr, 10));
    } else if (a == "--clients") {
      args.clients = strtoull(need(a), nullptr, 10);
    } else if (a == "--seconds") {
      args.seconds = atoi(need(a));
    } else if (a == "--warmup") {
      args.warmup = atoi(need(a));
    } else if (a == "--pool") {
      args.pool_threads = strtoull(need(a), nullptr, 10);
    } else if (a == "--queue") {
      args.queue_capacity = strtoull(need(a), nullptr, 10);
    } else if (a == "--buckets") {
      args.buckets = strtoull(need(a), nullptr, 10);
    } else if (a == "--no-redis") {
      args.cache = false;
    } else if (a == "--io") {
      args.redis_io_threads = atoi(need(a));
    } else if (a == "--redis") {
      std::string hp = need(a);
      auto pos = hp.find(':');
      args.redis_host = hp.substr(0, pos);
      if (pos != std::string::npos) {
        args.redis_port = atoi(hp.substr(pos + 1).c_str());
      }
    } else if (a == "--mode") {
      args.mode = need(a);
    } else if (a == "--mix") {
      std::string m = need(a);
      auto parse_part = [&](size_t& pos) {
        size_t comma = m.find(',', pos);
        uint64_t v = strtoull(m.substr(pos, comma == std::string::npos
                                              ? std::string::npos
                                              : comma - pos)
                                  .c_str(),
                              nullptr, 10);
        pos = comma == std::string::npos ? m.size() : comma + 1;
        return v;
      };
      size_t pos = 0;
      args.mix_cf = parse_part(pos);
      args.mix_sd = parse_part(pos);
      args.mix_dist = parse_part(pos);
      args.mix_add = parse_part(pos);
      args.mix_del = parse_part(pos);
    } else if (a == "--zipf") {
      args.zipf = atof(need(a));
    } else if (a == "--seed") {
      args.seed = strtoull(need(a), nullptr, 10);
    } else if (a == "--help" || a == "-h") {
      Usage();
      exit(0);
    } else {
      printf("unknown option: %s\n", a.c_str());
      Usage();
      return false;
    }
  }
  return true;
}

struct RunResult {
  double qps = 0;
  double avg_us = 0;
  size_t memory = 0;
  uint64_t shed = 0;
  double hit_rate = 0;
};

RunResult RunEngine(IGraphEngine& engine, const BenchArgs& args,
                    const std::vector<std::pair<uint32_t, uint32_t>>& edges,
                    const char* label, const char* note) {
  dge::DynamicGraphEngine* dyn = dynamic_cast<dge::DynamicGraphEngine*>(&engine);
  if (dyn != nullptr && args.cache && !dyn->cache_available()) {
    printf("note: Redis不可达，缓存路径已熔断，引擎回退为直连计算模式\n");
  }
  if (dyn != nullptr) {
    dyn->SetCacheInvalidation(false);
  }
  printf("building graph (%s)...\n", label);
  auto t0 = std::chrono::steady_clock::now();
  BuildGraph(engine, args);
  if (dyn != nullptr) {
    dyn->SetCacheInvalidation(true);
  }
  auto t1 = std::chrono::steady_clock::now();
  double build_s = std::chrono::duration<double>(t1 - t0).count();
  printf("graph built in %.2fs (%zu nodes)\n", build_s, args.nodes);

  ZipfSampler zipf(args.nodes, args.zipf);
  BenchCtx discard{};
  BenchCtx meas{};
  std::atomic<bool> run{false};
  std::atomic<uint64_t> seq{0};

  std::vector<std::thread> warmers;
  warmers.reserve(args.clients);
  run.store(true, std::memory_order_relaxed);
  for (size_t i = 0; i < args.clients; ++i) {
    warmers.emplace_back([&, i]() {
      ClientLoop(engine, discard, args, i, edges, run, seq, zipf);
    });
  }
  std::this_thread::sleep_for(std::chrono::seconds(args.warmup));
  run.store(false, std::memory_order_relaxed);
  for (auto& t : warmers) {
    t.join();
  }

  std::vector<std::thread> mclients;
  mclients.reserve(args.clients);
  run.store(true, std::memory_order_relaxed);
  for (size_t i = 0; i < args.clients; ++i) {
    mclients.emplace_back([&, i]() {
      ClientLoop(engine, meas, args, i, edges, run, seq, zipf);
    });
  }
  std::this_thread::sleep_for(std::chrono::seconds(args.seconds));
  run.store(false, std::memory_order_relaxed);
  for (auto& t : mclients) {
    t.join();
  }

  for (int i = 0; i < 1500; ++i) {
    dge::EngineStats s = engine.stats();
    if (s.queries_completed >= s.queries_submitted - s.shed_count) {
      break;
    }
    SleepUs(10000);
  }
  engine.Shutdown();

  PrintStats(engine, meas, args.seconds, label, note);
  dge::EngineStats s = engine.stats();
  RunResult r;
  r.qps = static_cast<double>(meas.completed.load(std::memory_order_relaxed)) / args.seconds;
  r.avg_us = meas.completed.load(std::memory_order_relaxed) > 0
                 ? static_cast<double>(meas.sum_us.load(std::memory_order_relaxed)) /
                       meas.completed.load(std::memory_order_relaxed)
                 : 0;
  r.memory = s.memory_bytes;
  r.shed = meas.shed.load(std::memory_order_relaxed);
  r.hit_rate = s.cache_hit_rate();
  return r;
}

}  // namespace

int main(int argc, char** argv) {
  setvbuf(stdout, nullptr, _IONBF, 0);
  BenchArgs args;
  if (!ParseArgs(argc, argv, args)) {
    return 1;
  }
  printf("=== 社交网络动态图处理引擎 基准测试 ===\n");
  printf("nodes=%zu degree=%u clients=%zu seconds=%d warmup=%d pool=%zu\n", args.nodes,
         args.degree, args.clients, args.seconds, args.warmup,
         args.pool_threads ? args.pool_threads : dge::hardware_cores() - 1);
  printf("mix(cf,sd,dist,add,del)=%llu,%llu,%llu,%llu,%llu\n",
         static_cast<unsigned long long>(args.mix_cf),
         static_cast<unsigned long long>(args.mix_sd),
         static_cast<unsigned long long>(args.mix_dist),
         static_cast<unsigned long long>(args.mix_add),
         static_cast<unsigned long long>(args.mix_del));
  if (args.cache) {
    printf("cache=redis(%s:%d) mode=%s\n", args.redis_host.c_str(), args.redis_port,
           args.mode.c_str());
  } else {
    printf("cache=off mode=%s\n", args.mode.c_str());
  }
  printf("\n");

  std::vector<std::pair<uint32_t, uint32_t>> edges;
  std::mt19937_64 rng(args.seed ^ 0x5DEECE66DULL);
  {
    std::vector<uint32_t> deg(args.nodes + 1, 0);
    uint32_t max_deg = 0;
    uint32_t m = args.degree;
    uint32_t n = static_cast<uint32_t>(args.nodes);
    auto attach = [&](uint32_t u, uint32_t v) {
      ++deg[u];
      ++deg[v];
      max_deg = std::max(max_deg, std::max(deg[u], deg[v]));
      edges.emplace_back(std::min(u, v), std::max(u, v));
    };
    for (uint32_t i = 1; i <= std::min<uint32_t>(m + 1, n); ++i) {
      for (uint32_t j = 1; j < i; ++j) {
        attach(i, j);
      }
    }
    for (uint32_t i = std::min<uint32_t>(m + 1, n) + 1; i <= n; ++i) {
      std::vector<uint32_t> targets;
      uint32_t guard = 0;
      while (targets.size() < m && guard < 10000) {
        ++guard;
        uint32_t r = 1 + static_cast<uint32_t>(rng() % (i - 1));
        bool dup = false;
        for (uint32_t t : targets) {
          if (t == r) {
            dup = true;
            break;
          }
        }
        if (dup) {
          continue;
        }
        if (max_deg == 0 || (rng() % (max_deg + 1)) <= deg[r]) {
          targets.push_back(r);
        }
      }
      for (uint32_t t : targets) {
        attach(i, t);
      }
    }
  }
  printf("拓扑生成完成: %zu 节点, %zu 边\n\n", args.nodes, edges.size());

  RunResult opt;
  RunResult base;
  bool have_opt = args.mode == "optimized" || args.mode == "both";
  bool have_base = args.mode == "baseline" || args.mode == "both";

  if (have_opt) {
    dge::DynamicGraphEngine::Config cfg;
    cfg.nodes = args.nodes;
    cfg.buckets = args.buckets;
    cfg.pool_threads = args.pool_threads;
    cfg.queue_capacity = args.queue_capacity;
    cfg.cache_enabled = args.cache;
    cfg.redis_host = args.cache ? args.redis_host : "";
    cfg.redis_port = args.redis_port;
    cfg.redis_io_threads = args.redis_io_threads;
    dge::DynamicGraphEngine engine(cfg);
    opt = RunEngine(engine, args, edges, "Optimized",
                    args.cache ? " (slab+压缩+分片锁+epoch+redis)" : " (无缓存)");
  }
  if (have_base) {
    dge::BaselineGraphEngine::Config cfg;
    cfg.nodes = args.nodes;
    cfg.pool_threads = args.pool_threads;
    cfg.queue_capacity = args.queue_capacity;
    dge::BaselineGraphEngine engine(cfg);
    base = RunEngine(engine, args, edges, "Baseline", " (原生链表+memset+全局锁)");
  }

  if (have_opt && have_base) {
    printf("\n== 对比 ==\n");
    printf("  QPS  : optimized %.0f vs baseline %.0f  => %.2fx\n", opt.qps, base.qps,
           base.qps > 0 ? opt.qps / base.qps : 0);
    printf("  avg  : optimized %.0fus vs baseline %.0fus => %.2fx\n", opt.avg_us, base.avg_us,
           opt.avg_us > 0 ? base.avg_us / opt.avg_us : 0);
    printf("  mem  : optimized %.1fMB vs baseline %.1fMB => -%.0f%%\n",
           opt.memory / 1024.0 / 1024.0, base.memory / 1024.0 / 1024.0,
           base.memory > 0 ? (1.0 - static_cast<double>(opt.memory) / base.memory) * 100.0
                           : 0);
  }
  return 0;
}
