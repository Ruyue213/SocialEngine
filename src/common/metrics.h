#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace dge {
namespace metrics {

class Registry {
 public:
  static Registry& Instance();

  void Inc(const std::string& name, const std::map<std::string, std::string>& labels,
           uint64_t delta = 1);
  void Observe(const std::string& name, uint64_t value);
  void SetGauge(const std::string& name, uint64_t value);

  std::string PrometheusText() const;

 private:
  Registry() = default;

  struct LabeledCounter {
    std::string name;
    std::map<std::string, std::string> labels;
    std::atomic<uint64_t> value{0};
  };

  struct LabelKey {
    std::string name;
    std::map<std::string, std::string> labels;
    bool operator<(const LabelKey& o) const {
      if (name != o.name) {
        return name < o.name;
      }
      return labels < o.labels;
    }
  };

  static constexpr int kHistBuckets = 24;
  static constexpr uint64_t kBucketBase = 2;

  mutable std::mutex mu_;
  std::map<LabelKey, std::unique_ptr<LabeledCounter>> counters_;
  std::map<std::string, std::unique_ptr<std::array<std::atomic<uint64_t>, kHistBuckets>>>
      histograms_;
  std::map<std::string, std::unique_ptr<std::atomic<uint64_t>>> gauges_;
};

inline void Inc(const std::string& name, const std::map<std::string, std::string>& labels = {},
                uint64_t delta = 1) {
  Registry::Instance().Inc(name, labels, delta);
}

inline void Observe(const std::string& name, uint64_t value) {
  Registry::Instance().Observe(name, value);
}

inline void SetGauge(const std::string& name, uint64_t value) {
  Registry::Instance().SetGauge(name, value);
}

}  // namespace metrics
}  // namespace dge
