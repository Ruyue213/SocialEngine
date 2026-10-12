#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cmath>
#include <cstring>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "baseline/baseline_engine.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>
#include "common/affinity.h"
#include "graph/dynamic_graph_engine.h"
#include "memory/epoch.h"
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
  bool http = false;
  std::string http_host = "127.0.0.1";
  int http_port = 8080;
};

constexpr int kLatBuckets = 40;
constexpr uint64_t kBackoffBaseUs = 1000;
constexpr int kBackoffMaxShift = 6;

struct HttpClient {
  int fd = -1;
  std::string in;

  ~HttpClient() {
    if (fd >= 0) {
      ::close(fd);
    }
  }

  bool Connect(const std::string& host, int port) {
    if (fd >= 0) {
      ::close(fd);
    }
    fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      return false;
    }
    struct sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    ::inet_pton(AF_INET, host.c_str(), &addr.sin_addr);
    if (::connect(fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
      ::close(fd);
      fd = -1;
      return false;
    }
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    struct timeval tv;
    tv.tv_sec = 30;
    tv.tv_usec = 0;
    ::setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    return true;
  }

  bool Post(const std::string& path, const std::string& body, int* status,
            std::string* resp_body) {
    std::string req = "POST " + path +
                      " HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\n"
                      "Content-Length: " +
                      std::to_string(body.size()) + "\r\nConnection: keep-alive\r\n\r\n" +
                      body;
    if (!SendAll(req)) {
      return false;
    }
    return ReadResponse(status, resp_body);
  }

  bool Get(const std::string& path, int* status, std::string* resp_body) {
    std::string req = "GET " + path + " HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    if (!SendAll(req)) {
      return false;
    }
    return ReadResponse(status, resp_body);
  }

  bool SendAll(const std::string& data) {
    size_t off = 0;
    while (off < data.size()) {
      ssize_t n = ::write(fd, data.data() + off, data.size() - off);
      if (n <= 0) {
        return false;
      }
      off += static_cast<size_t>(n);
    }
    return true;
  }

  bool ReadResponse(int* status, std::string* body) {
    while (true) {
      size_t hdr_end = in.find("\r\n\r\n");
      if (hdr_end == std::string::npos) {
        if (in.size() > 1 << 20) {
          return false;
        }
        char buf[8192];
        ssize_t n = ::read(fd, buf, sizeof(buf));
        if (n <= 0) {
          return false;
        }
        in.append(buf, static_cast<size_t>(n));
        continue;
      }
      std::string head = in.substr(0, hdr_end);
      *status = atoi(head.substr(9, 3).c_str());
      size_t clen = 0;
      size_t pos = head.find("Content-Length:");
      if (pos != std::string::npos) {
        clen = strtoull(head.substr(pos + 15).c_str(), nullptr, 10);
      }
      size_t total = hdr_end + 4 + clen;
      while (in.size() < total) {
        char buf[8192];
        ssize_t n = ::read(fd, buf, sizeof(buf));
        if (n <= 0) {
          return false;
        }
        in.append(buf, static_cast<size_t>(n));
      }
      *body = in.substr(hdr_end + 4, clen);
      in.erase(0, total);
      return true;
    }
  }
};

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
  std::atomic<uint64_t> submitted{0};
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
                const ZipfSampler& zipf, bool queries_only = false) {
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
  // 修复:接入 --mix 参数(原先硬编码 55/25/10/5/5,--mix 解析
  // 后被静默忽略,负载与文档声明不一致)。
  const uint64_t cf_p = args.mix_cf;
  const uint64_t sd_p = args.mix_sd;
  const uint64_t dist_p = args.mix_dist;
  const uint64_t add_p = args.mix_add;
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

    if (queries_only && (op == OpType::kAddEdge || op == OpType::kRemoveEdge)) {
      op = OpType::kCommonFriends;  // 预热期不更新图,保持测量初始状态一致
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
        ctx.submitted.fetch_add(1, std::memory_order_relaxed);
        break;
      }
      ctx.shed.fetch_add(1, std::memory_order_relaxed);
      ++backoff_shift;
      uint64_t sleep_us = kBackoffBaseUs << std::min(backoff_shift, kBackoffMaxShift);
      SleepUs(sleep_us);
    }
  }
}

void ClientLoopHttp(BenchCtx& ctx, const BenchArgs& args, size_t client_id,
                    const std::vector<std::pair<uint32_t, uint32_t>>& edges,
                    std::atomic<bool>& run, const ZipfSampler& zipf) {
  HttpClient cli;
  if (!cli.Connect(args.http_host, args.http_port)) {
    printf("client %zu: connect failed\n", client_id);
    return;
  }
  std::mt19937_64 rng(args.seed ^ (client_id * 0x9E3779B97F4A7C15ULL));
  uint32_t n = static_cast<uint32_t>(args.nodes);
  bool use_zipf = args.zipf > 0.0 && zipf.cdf.size() > 1;
  auto pick_node = [&]() -> uint32_t {
    if (use_zipf) {
      return zipf.Sample(rng);
    }
    return 1 + static_cast<uint32_t>(rng() % n);
  };
  const uint64_t cf_p = args.mix_cf;
  const uint64_t sd_p = args.mix_sd;
  const uint64_t dist_p = args.mix_dist;
  const uint64_t add_p = args.mix_add;
  char body[256];
  while (run.load(std::memory_order_relaxed)) {
    uint64_t roll = rng() % 100;
    uint32_t u = pick_node();
    uint32_t v = pick_node();
    if (u == v) {
      v = v % n + 1;
    }
    int status = 0;
    std::string resp;
    if (roll < cf_p + sd_p + dist_p) {
      const char* t = roll < cf_p ? "cf" : (roll < cf_p + sd_p ? "sd" : "dist");
      std::snprintf(body, sizeof(body), "{\"type\":\"%s\",\"a\":%u,\"b\":%u}", t, u, v);
      auto start = std::chrono::steady_clock::now();
      if (!cli.Post("/query", body, &status, &resp)) {
        if (!cli.Connect(args.http_host, args.http_port)) {
          SleepUs(1000);
        }
        continue;
      }
      auto now = std::chrono::steady_clock::now();
      uint64_t us = static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::microseconds>(now - start).count());
      if (status == 503) {
        ctx.shed.fetch_add(1, std::memory_order_relaxed);
        SleepUs(1000 << (roll % 5));
        continue;
      }
      Record(ctx, us);
      continue;
    }
    if (roll < cf_p + sd_p + dist_p + add_p) {
      std::snprintf(body, sizeof(body), "{\"op\":\"add\",\"u\":%u,\"v\":%u}", u, v);
      if (cli.Post("/edge", body, &status, &resp) && status == 200) {
        ctx.updates_done.fetch_add(1, std::memory_order_relaxed);
      }
      continue;
    }
    if (!edges.empty()) {
      const auto& e = edges[rng() % edges.size()];
      std::snprintf(body, sizeof(body), "{\"op\":\"del\",\"u\":%u,\"v\":%u}", e.first,
                    e.second);
      if (cli.Post("/edge", body, &status, &resp) && status == 200) {
        ctx.updates_done.fetch_add(1, std::memory_order_relaxed);
      }
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
  // 桶 i 覆盖 [2^i, 2^(i+1));返回桶上界,报告值 >= 真实值的保守估计。
  auto pctl = [&](double p) {
    uint64_t target = static_cast<uint64_t>(cum * p);
    uint64_t acc = 0;
    for (int i = 0; i < kLatBuckets; ++i) {
      acc += h[i];
      if (acc >= target) {
        return (1ULL << (i + 1));
      }
    }
    return 1ULL << kLatBuckets;
  };
  double avg = static_cast<double>(ctx.sum_us.load(std::memory_order_relaxed)) / total;
  printf("  %-8s: avg %.0fus | p50 %lluus | p90 %lluus | p99 %lluus | p999 %lluus | max %lluus\n",
         label, avg, static_cast<unsigned long long>(pctl(0.50)),
         static_cast<unsigned long long>(pctl(0.90)),
         static_cast<unsigned long long>(pctl(0.99)),
         static_cast<unsigned long long>(pctl(0.999)),
         static_cast<unsigned long long>(ctx.max_us.load(std::memory_order_relaxed)));
}

void PrintStatsHttp(BenchCtx& ctx, int seconds) {
  double qps = static_cast<double>(ctx.completed.load(std::memory_order_relaxed)) / seconds;
  printf("== HTTP 压测结果 ==\n");
  printf("  completed queries : %llu (qps %.0f)\n",
         static_cast<unsigned long long>(ctx.completed.load(std::memory_order_relaxed)), qps);
  printf("  shed (503)        : %llu\n",
         static_cast<unsigned long long>(ctx.shed.load(std::memory_order_relaxed)));
  printf("  successful updates: %llu\n",
         static_cast<unsigned long long>(ctx.updates_done.load(std::memory_order_relaxed)));
  PrintLatency(ctx, "latency");
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
  printf("  memory            : %.1f MB (pending retire: %llu)\n", s.memory_bytes / 1024.0 / 1024.0,
         static_cast<unsigned long long>(dge::EpochReclaimer::Instance().retired_pending()));
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
      "  --http H:P        压测远程HTTP服务 (如 127.0.0.1:8080)\n"
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
    } else if (a == "--http") {
      std::string hp = need(a);
      auto pos = hp.find(':');
      args.http = true;
      args.http_host = hp.substr(0, pos);
      if (pos != std::string::npos) {
        args.http_port = atoi(hp.substr(pos + 1).c_str());
      }
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

  // 预热:纯查询负载。原实现预热含更新,两个引擎的图沿不同路径
  // 演化,正式对比不在同一初始状态上。
  std::vector<std::thread> warmers;
  warmers.reserve(args.clients);
  run.store(true, std::memory_order_relaxed);
  for (size_t i = 0; i < args.clients; ++i) {
    warmers.emplace_back([&, i]() {
      ClientLoop(engine, discard, args, i, edges, run, seq, zipf,
                 /*queries_only=*/true);
    });
  }
  std::this_thread::sleep_for(std::chrono::seconds(args.warmup));
  run.store(false, std::memory_order_relaxed);
  for (auto& t : warmers) {
    t.join();
  }

  // 正式测量:分子(完成量)与分母(真实墙钟,含排空)自洽。
  // 原实现分母固定为 args.seconds,join 与排空期完成的请求被
  // 额外计入分子,QPS 被高估。
  auto t_measure_start = std::chrono::steady_clock::now();
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

  // 排空:只等 meas 在飞完成(原条件用引擎累计计数,含预热期
  // 请求,可能提前满足)。
  for (int i = 0; i < 1500; ++i) {
    if (meas.submitted.load(std::memory_order_relaxed) ==
        meas.completed.load(std::memory_order_relaxed) +
            meas.shed.load(std::memory_order_relaxed)) {
      break;
    }
    SleepUs(10000);
  }
  auto t_measure_end = std::chrono::steady_clock::now();
  double actual_seconds =
      std::chrono::duration<double>(t_measure_end - t_measure_start).count();
  engine.Shutdown();

  PrintStats(engine, meas, actual_seconds, label, note);
  dge::EngineStats s = engine.stats();
  RunResult r;
  r.qps = static_cast<double>(meas.completed.load(std::memory_order_relaxed)) / actual_seconds;
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
  // 压测客户端必须忽略 SIGPIPE:服务端过载关连接时,向已关闭
  // socket 的 write 会以 SIGPIPE 终止压测进程(exit=141)。
  std::signal(SIGPIPE, SIG_IGN);
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

  if (args.http) {
    printf("HTTP模式: 压测 %s:%d（服务端建图由server负责）\n\n", args.http_host.c_str(),
           args.http_port);
    HttpClient probe;
    int status = 0;
    std::string resp;
    if (!probe.Connect(args.http_host, args.http_port) || !probe.Get("/healthz", &status, &resp)) {
      printf("无法连接服务端 %s:%d，请先启动 bin/server\n", args.http_host.c_str(),
             args.http_port);
      return 1;
    }
    ZipfSampler zipf(args.nodes, args.zipf);
    BenchCtx meas{};
    std::atomic<bool> run{true};
    std::vector<std::thread> clients;
    clients.reserve(args.clients);
    for (size_t i = 0; i < args.clients; ++i) {
      clients.emplace_back([&, i]() {
        ClientLoopHttp(meas, args, i, edges, run, zipf);
      });
    }
    std::this_thread::sleep_for(std::chrono::seconds(args.seconds));
    run.store(false, std::memory_order_relaxed);
    for (auto& t : clients) {
      t.join();
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    PrintStatsHttp(meas, args.seconds);
    if (probe.Connect(args.http_host, args.http_port) &&
        probe.Get("/stats", &status, &resp)) {
      printf("\n服务端统计: %s\n", resp.c_str());
    }
    return 0;
  }

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
