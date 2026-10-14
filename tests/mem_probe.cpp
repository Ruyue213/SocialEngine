// 内存拆分探针:量化 optimized 引擎各部分内存,与 baseline 对照。
// 用与 bench 相同的 BA 拓扑与种子,保证与性能报告同图。
#include <chrono>
#include <cstdio>
#include <thread>
#include <cstdlib>
#include <random>
#include <vector>

#include "baseline/baseline_engine.h"
#include "graph/dynamic_graph_engine.h"
#include "graph/engine_interface.h"

namespace {

// 与 src/bench/bench_main.cpp 的 BuildGraph 相同的 BA 生成逻辑(同 seed)。
void BuildBA(dge::IGraphEngine& engine, size_t nodes, uint32_t m, uint64_t seed) {
  std::mt19937_64 rng(seed ^ 0x5DEECE66DULL);
  std::vector<uint32_t> deg(nodes + 1, 0);
  uint32_t max_deg = 0;
  auto attach = [&](uint32_t u, uint32_t v) {
    if (engine.AddEdge(u, v)) {
      ++deg[u];
      ++deg[v];
      max_deg = std::max(max_deg, std::max(deg[u], deg[v]));
    }
  };
  uint32_t n = static_cast<uint32_t>(nodes);
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

}  // namespace

int main(int argc, char** argv) {
  size_t nodes = argc > 1 ? strtoull(argv[1], nullptr, 10) : 300000;
  uint32_t m = argc > 2 ? static_cast<uint32_t>(atoi(argv[2])) : 20;
  uint64_t seed = argc > 3 ? strtoull(argv[3], nullptr, 10) : 42;
  printf("nodes=%zu degree=%u seed=%llu\n", nodes, m,
         static_cast<unsigned long long>(seed));

  {
    printf("[optimized] building...\n");
    dge::DynamicGraphEngine::Config cfg;
    cfg.nodes = nodes;
    cfg.buckets = 256;
    cfg.pool_threads = 9;
    cfg.cache_enabled = false;
    dge::DynamicGraphEngine eng(cfg);
    BuildBA(eng, nodes, m, seed);
    if (getenv("MEM_PROBE_WAIT")) {
      int secs = atoi(getenv("MEM_PROBE_WAIT"));
      printf("[optimized] waiting %ds for reclaimer to drain...\n", secs);
      std::this_thread::sleep_for(std::chrono::seconds(secs));
    }
    dge::EngineStats s = eng.stats();
    size_t workers = eng.pool_thread_count();
    size_t node_table = (nodes + 1) * 16;  // NodeAdjacency ≈ 16B
    size_t workspace = workers * 3 * (nodes + 1) * 4;
    size_t adjacency = s.slab_bytes;  // Slab 槽内即邻接数据(块/尾/元数据)
    printf("[optimized] edges_added, updates=%llu\n",
           static_cast<unsigned long long>(s.updates));
    printf("[optimized] 邻接数据(Slab 在用): %.1f MB\n", adjacency / 1048576.0);
    printf("[optimized] 节点表:              %.1f MB\n", node_table / 1048576.0);
    printf("[optimized] 线程工作区(%zu 线程): %.1f MB\n", workers,
           workspace / 1048576.0);
    printf("[optimized] 全账本(memory_bytes): %.1f MB\n", s.memory_bytes / 1048576.0);
    printf("[optimized] 每条目平均字节(邻接/边数): %.2f B\n",
           static_cast<double>(adjacency) / static_cast<double>(s.updates));
    eng.Shutdown();
  }
  {
    printf("[baseline] building...\n");
    dge::BaselineGraphEngine::Config bc;
    bc.nodes = nodes;
    bc.pool_threads = 2;
    dge::BaselineGraphEngine base(bc);
    BuildBA(base, nodes, m, seed);
    dge::EngineStats s = base.stats();
    printf("[baseline]  全账本(memory_bytes): %.1f MB\n", s.memory_bytes / 1048576.0);
    printf("[baseline]  每条目平均字节: %.2f B\n",
           static_cast<double>(s.memory_bytes) / static_cast<double>(s.updates));
    base.Shutdown();
  }
  return 0;
}
