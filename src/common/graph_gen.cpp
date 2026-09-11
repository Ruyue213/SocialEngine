#include "common/graph_gen.h"

#include <algorithm>
#include <random>

namespace dge {

void GenerateBarabasiAlbert(IGraphEngine& engine, size_t nodes, uint32_t degree,
                            uint64_t seed, std::vector<std::pair<uint32_t, uint32_t>>& edges) {
  std::mt19937_64 rng(seed ^ 0x5DEECE66DULL);
  uint32_t m = degree;
  uint32_t n = static_cast<uint32_t>(nodes);
  std::vector<uint32_t> deg(n + 1, 0);
  uint32_t max_deg = 0;
  auto attach = [&](uint32_t u, uint32_t v) {
    if (engine.AddEdge(u, v)) {
      ++deg[u];
      ++deg[v];
      max_deg = std::max(max_deg, std::max(deg[u], deg[v]));
      edges.emplace_back(std::min(u, v), std::max(u, v));
    }
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

}  // namespace dge
