#pragma once

#include <functional>
#include <memory>
#include <string>

#include "graph/dynamic_graph_engine.h"
#include "net/http.h"

namespace dge {

class GraphService {
 public:
  GraphService(DynamicGraphEngine& engine, int dedup_ttl_seconds);

  void Handle(net::HttpRequest&& req, std::function<void(int, std::string)> respond);

 private:
  void HandleQuery(net::HttpRequest&& req, std::function<void(int, std::string)> respond);
  void HandleEdge(net::HttpRequest&& req, std::function<void(int, std::string)> respond);
  void HandleMetrics(std::function<void(int, std::string)> respond);
  void HandleStats(std::function<void(int, std::string)> respond);

  static std::string JsonEscape(const std::string& s);

  DynamicGraphEngine& engine_;
  int dedup_ttl_;
};

}  // namespace dge
