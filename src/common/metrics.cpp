#include "common/metrics.h"

#include <cstdio>
#include <sstream>

namespace dge {
namespace metrics {

Registry& Registry::Instance() {
  static Registry r;
  return r;
}

void Registry::Inc(const std::string& name, const std::map<std::string, std::string>& labels,
                   uint64_t delta) {
  std::lock_guard<std::mutex> lk(mu_);
  LabelKey key{name, labels};
  auto it = counters_.find(key);
  if (it == counters_.end()) {
    auto c = std::make_unique<LabeledCounter>();
    c->name = name;
    c->labels = labels;
    it = counters_.emplace(key, std::move(c)).first;
  }
  it->second->value.fetch_add(delta, std::memory_order_relaxed);
}

void Registry::Observe(const std::string& name, uint64_t value) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = histograms_.find(name);
  if (it == histograms_.end()) {
    auto h = std::make_unique<std::array<std::atomic<uint64_t>, kHistBuckets>>();
    for (auto& b : *h) {
      b.store(0, std::memory_order_relaxed);
    }
    it = histograms_.emplace(name, std::move(h)).first;
  }
  // 桶 i 的区间是 (2^i, 2^(i+1)],0/1 落桶 0。
  // 修复:原先标签输出 le=2^i 而样本按同一公式分桶,Observe(3)
  // 计入 le=2(3>2,边界错误);现在标签输出 le=2^(i+1),与
  // 分桶公式严格对应。63 号桶直接标 +Inf(2^64 溢出)。
  int b = value < 2 ? 0 : 63 - __builtin_clzll(value);
  if (b < 0) {
    b = 0;
  }
  if (b >= kHistBuckets) {
    b = kHistBuckets - 1;
  }
  (*it->second)[b].fetch_add(1, std::memory_order_relaxed);
}

void Registry::SetGauge(const std::string& name, uint64_t value) {
  std::lock_guard<std::mutex> lk(mu_);
  auto it = gauges_.find(name);
  if (it == gauges_.end()) {
    auto g = std::make_unique<std::atomic<uint64_t>>(0);
    it = gauges_.emplace(name, std::move(g)).first;
  }
  it->second->store(value, std::memory_order_relaxed);
}

std::string Registry::PrometheusText() const {
  std::lock_guard<std::mutex> lk(mu_);
  std::ostringstream oss;
  for (const auto& [key, c] : counters_) {
    oss << "# TYPE " << key.name << " counter\n";
    oss << key.name;
    if (!key.labels.empty()) {
      oss << "{";
      bool first = true;
      for (const auto& [lk_, lv] : key.labels) {
        if (!first) {
          oss << ",";
        }
        first = false;
        oss << lk_ << "=\"" << lv << "\"";
      }
      oss << "}";
    }
    oss << " " << c->value.load(std::memory_order_relaxed) << "\n";
  }
  for (const auto& [name, h] : histograms_) {
    oss << "# TYPE " << name << " histogram\n";
    uint64_t cum = 0;
    for (int i = 0; i < kHistBuckets; ++i) {
      cum += (*h)[i].load(std::memory_order_relaxed);
      // 桶 i 覆盖 (2^i, 2^(i+1)],上界标签为 2^(i+1);最高桶 +Inf。
      oss << name << "_bucket{le=\""
          << (i == kHistBuckets - 1 ? "+Inf" : std::to_string(1ULL << (i + 1)))
          << "\"} " << cum << "\n";
    }
    oss << name << "_count " << cum << "\n";
  }
  for (const auto& [name, g] : gauges_) {
    oss << "# TYPE " << name << " gauge\n";
    oss << name << " " << g->load(std::memory_order_relaxed) << "\n";
  }
  return oss.str();
}

}  // namespace metrics
}  // namespace dge
