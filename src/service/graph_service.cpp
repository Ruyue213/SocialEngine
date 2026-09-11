#include "service/graph_service.h"

#include <cstdlib>
#include <sstream>

#include "common/logging.h"
#include "common/metrics.h"

namespace dge {

namespace {

struct Json {
  static std::string ParseStr(const std::string& body, const char* key, bool* ok) {
    std::string needle = std::string("\"") + key + "\"";
    size_t pos = body.find(needle);
    if (pos == std::string::npos) {
      *ok = false;
      return "";
    }
    pos = body.find(':', pos + needle.size());
    if (pos == std::string::npos) {
      *ok = false;
      return "";
    }
    ++pos;
    while (pos < body.size() && (body[pos] == ' ' || body[pos] == '"')) {
      ++pos;
    }
    if (pos >= body.size()) {
      *ok = false;
      return "";
    }
    size_t end = pos;
    while (end < body.size() && body[end] != '"' && body[end] != ',' && body[end] != '}' &&
           body[end] != ' ' && body[end] != '\n') {
      ++end;
    }
    *ok = true;
    return body.substr(pos, end - pos);
  }

  static uint64_t ParseU64(const std::string& body, const char* key, bool* ok) {
    std::string v = ParseStr(body, key, ok);
    if (!*ok) {
      return 0;
    }
    *ok = !v.empty();
    return std::strtoull(v.c_str(), nullptr, 10);
  }
};

}  // namespace

GraphService::GraphService(DynamicGraphEngine& engine, int dedup_ttl_seconds)
    : engine_(engine), dedup_ttl_(dedup_ttl_seconds) {}

std::string GraphService::JsonEscape(const std::string& s) {
  std::string out;
  for (char c : s) {
    switch (c) {
      case '"':
        out += "\\\"";
        break;
      case '\\':
        out += "\\\\";
        break;
      default:
        out += c;
    }
  }
  return out;
}

void GraphService::Handle(net::HttpRequest&& req, std::function<void(int, std::string)> respond) {
  metrics::Inc("http_requests_total", {{"method", req.method}, {"path", req.path}});
  if (req.path == "/healthz") {
    respond(200, "{\"status\":\"ok\"}");
    return;
  }
  if (req.path == "/metrics") {
    HandleMetrics(std::move(respond));
    return;
  }
  if (req.path == "/stats") {
    HandleStats(std::move(respond));
    return;
  }
  if (req.method == "POST" && req.path == "/query") {
    HandleQuery(std::move(req), std::move(respond));
    return;
  }
  if (req.method == "POST" && req.path == "/edge") {
    HandleEdge(std::move(req), std::move(respond));
    return;
  }
  respond(404, "{\"error\":\"not found\"}");
}

void GraphService::HandleQuery(net::HttpRequest&& req,
                               std::function<void(int, std::string)> respond) {
  bool ok = false;
  std::string type = Json::ParseStr(req.body, "type", &ok);
  if (!ok) {
    respond(400, "{\"error\":\"missing type\"}");
    return;
  }
  uint64_t a = Json::ParseU64(req.body, "a", &ok);
  if (!ok) {
    respond(400, "{\"error\":\"missing a\"}");
    return;
  }
  uint64_t b = 0;
  bool b_ok = true;
  if (type != "sd") {
    b = Json::ParseU64(req.body, "b", &b_ok);
    if (!b_ok) {
      respond(400, "{\"error\":\"missing b\"}");
      return;
    }
  }
  QueryType qt;
  if (type == "cf") {
    qt = QueryType::kCommonFriends;
  } else if (type == "sd") {
    qt = QueryType::kSecondDegree;
  } else if (type == "dist") {
    qt = QueryType::kDistance;
  } else {
    respond(400, "{\"error\":\"unknown type\"}");
    return;
  }
  QueryRequest qreq;
  qreq.type = qt;
  qreq.a = static_cast<uint32_t>(a);
  qreq.b = static_cast<uint32_t>(b);
  qreq.cb = [respond](QueryResult&& r) {
    std::ostringstream oss;
    oss << "{";
    if (r.distance >= -1) {
      oss << "\"distance\":" << r.distance;
    } else {
      oss << "\"friends\":[";
      for (size_t i = 0; i < r.friends.size(); ++i) {
        if (i > 0) {
          oss << ",";
        }
        oss << r.friends[i];
      }
      oss << "]";
    }
    oss << "}";
    respond(200, oss.str());
  };
  if (!engine_.Submit(std::move(qreq))) {
    metrics::Inc("http_responses_total", {{"status", "503"}});
    respond(503, "{\"error\":\"server overloaded\"}");
    return;
  }
}

void GraphService::HandleEdge(net::HttpRequest&& req,
                              std::function<void(int, std::string)> respond) {
  bool ok = false;
  std::string op = Json::ParseStr(req.body, "op", &ok);
  if (!ok || (op != "add" && op != "del")) {
    respond(400, "{\"error\":\"bad op\"}");
    return;
  }
  uint64_t u = Json::ParseU64(req.body, "u", &ok);
  if (!ok) {
    respond(400, "{\"error\":\"missing u\"}");
    return;
  }
  uint64_t v = Json::ParseU64(req.body, "v", &ok);
  if (!ok) {
    respond(400, "{\"error\":\"missing v\"}");
    return;
  }
  std::string dedup_key = Json::ParseStr(req.body, "dedup_key", &ok);
  if (!ok || dedup_key.empty()) {
    bool done = op == "add" ? engine_.AddEdge(static_cast<uint32_t>(u),
                                              static_cast<uint32_t>(v))
                            : engine_.RemoveEdge(static_cast<uint32_t>(u),
                                                 static_cast<uint32_t>(v));
    if (!done) {
      respond(409, "{\"error\":\"edge not applied\"}");
      return;
    }
    respond(200, "{\"ok\":true}");
    return;
  }
  engine_.DedupCheck(dedup_key, dedup_ttl_,
                     [this, op, u, v, respond](bool acquired) {
    if (!acquired) {
      respond(200, "{\"ok\":true,\"dedup\":true}");
      return;
    }
    bool done = op == "add" ? engine_.AddEdge(static_cast<uint32_t>(u),
                                              static_cast<uint32_t>(v))
                            : engine_.RemoveEdge(static_cast<uint32_t>(u),
                                                 static_cast<uint32_t>(v));
    if (!done) {
      respond(409, "{\"error\":\"edge not applied\"}");
      return;
    }
    respond(200, "{\"ok\":true}");
  });
}

void GraphService::HandleMetrics(std::function<void(int, std::string)> respond) {
  EngineStats s = engine_.stats();
  metrics::SetGauge("engine_slab_bytes_in_use", s.memory_bytes);
  metrics::SetGauge("engine_memory_bytes", s.memory_bytes);
  metrics::SetGauge("engine_cache_hit_rate", s.cache_gets == 0
                                                  ? 0
                                                  : s.cache_hits * 1000 / s.cache_gets);
  respond(200, metrics::Registry::Instance().PrometheusText());
}

void GraphService::HandleStats(std::function<void(int, std::string)> respond) {
  EngineStats s = engine_.stats();
  std::ostringstream oss;
  oss << "{\"queries_submitted\":" << s.queries_submitted
      << ",\"queries_completed\":" << s.queries_completed << ",\"shed_count\":" << s.shed_count
      << ",\"cache_hits\":" << s.cache_hits << ",\"cache_misses\":" << s.cache_misses
      << ",\"updates\":" << s.updates << ",\"compactions\":" << s.compactions
      << ",\"memory_bytes\":" << s.memory_bytes << "}";
  respond(200, oss.str());
}

}  // namespace dge
