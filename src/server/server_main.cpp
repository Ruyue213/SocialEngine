#include <atomic>
#include <csignal>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>

#include "common/affinity.h"
#include "common/graph_gen.h"
#include "common/logging.h"
#include "common/metrics.h"
#include "graph/dynamic_graph_engine.h"
#include "net/reactor.h"
#include "service/graph_service.h"

namespace {

std::atomic<bool> g_stop{false};

void OnSignal(int sig) {
  DGE_LOG_INFO("received signal %d, draining...", sig);
  g_stop.store(true, std::memory_order_relaxed);
}

void Usage() {
  printf(
      "usage: server [options]\n"
      "  --port P          监听端口 (default 8080)\n"
      "  --nodes N         节点数 (default 1000000)\n"
      "  --degree M        平均好友数 (default 20)\n"
      "  --reactors R      子Reactor数 (default 4)\n"
      "  --pool T          计算线程数 (default cores-1)\n"
      "  --buckets K       哈希分片桶数 (default 256)\n"
      "  --queue Q         任务队列容量 (default 4096)\n"
      "  --no-redis        禁用缓存\n"
      "  --redis H:P       Redis地址 (default 127.0.0.1:6379)\n"
      "  --io N            Redis I/O线程数 (default 4)\n"
      "  --dedup-ttl S     幂等去重键TTL秒 (default 60)\n"
      "  --wal P           WAL文件路径 (default 关闭)\n"
      "  --snapshot P      快照文件路径\n"
      "  --wal-sync        WAL同步fsync模式 (default 组提交)\n"
      "  --wal-checkpoint B WAL触发快照字节阈值 (default 64MB)\n"
      "  --seed S          建图随机种子 (default 42)\n");
}

}  // namespace

int main(int argc, char** argv) {
  int port = 8080;
  size_t nodes = 1000000;
  uint32_t degree = 20;
  int reactors = 4;
  size_t pool_threads = 0;
  size_t buckets = 256;
  size_t queue_capacity = 4096;
  bool cache = true;
  std::string redis_host = "127.0.0.1";
  int redis_port = 6379;
  int io_threads = 4;
  int dedup_ttl = 60;
  uint64_t seed = 42;
  std::string wal_path;
  std::string snapshot_path;
  bool wal_sync = false;
  size_t wal_checkpoint = 64 * 1024 * 1024;

  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    auto need = [&](const std::string& name) -> const char* {
      if (i + 1 >= argc) {
        printf("missing value for %s\n", name.c_str());
        exit(1);
      }
      return argv[++i];
    };
    if (a == "--port") {
      port = atoi(need(a));
    } else if (a == "--nodes") {
      nodes = strtoull(need(a), nullptr, 10);
    } else if (a == "--degree") {
      degree = static_cast<uint32_t>(strtoul(need(a), nullptr, 10));
    } else if (a == "--reactors") {
      reactors = atoi(need(a));
    } else if (a == "--pool") {
      pool_threads = strtoull(need(a), nullptr, 10);
    } else if (a == "--buckets") {
      buckets = strtoull(need(a), nullptr, 10);
    } else if (a == "--queue") {
      queue_capacity = strtoull(need(a), nullptr, 10);
    } else if (a == "--no-redis") {
      cache = false;
    } else if (a == "--redis") {
      std::string hp = need(a);
      auto pos = hp.find(':');
      redis_host = hp.substr(0, pos);
      if (pos != std::string::npos) {
        redis_port = atoi(hp.substr(pos + 1).c_str());
      }
    } else if (a == "--io") {
      io_threads = atoi(need(a));
    } else if (a == "--dedup-ttl") {
      dedup_ttl = atoi(need(a));
    } else if (a == "--wal") {
      wal_path = need(a);
    } else if (a == "--snapshot") {
      snapshot_path = need(a);
    } else if (a == "--wal-sync") {
      wal_sync = true;
    } else if (a == "--wal-checkpoint") {
      wal_checkpoint = strtoull(need(a), nullptr, 10);
    } else if (a == "--seed") {
      seed = strtoull(need(a), nullptr, 10);
    } else {
      Usage();
      return 1;
    }
  }

  dge::log::SetLevel(dge::log::Level::kInfo);

  dge::DynamicGraphEngine::Config cfg;
  cfg.nodes = nodes;
  cfg.buckets = buckets;
  cfg.pool_threads = pool_threads;
  cfg.queue_capacity = queue_capacity;
  cfg.cache_enabled = cache;
  cfg.redis_host = cache ? redis_host : "";
  cfg.redis_port = redis_port;
  cfg.redis_io_threads = io_threads;
  cfg.wal_path = wal_path;
  cfg.wal_sync_mode = wal_sync;
  cfg.snapshot_path = snapshot_path;
  cfg.wal_checkpoint_bytes = wal_checkpoint;
  dge::DynamicGraphEngine engine(cfg);
  engine.SetCacheInvalidation(false);
  engine.SetWalEnabled(false);
  if (!wal_path.empty()) {
    engine.Recover();
    DGE_LOG_INFO("wal recovery done, graph has %llu updates",
                 static_cast<unsigned long long>(engine.stats().updates));
  }
  DGE_LOG_INFO("building graph: %zu nodes, degree %u", nodes, degree);
  std::vector<std::pair<uint32_t, uint32_t>> edges;
  dge::GenerateBarabasiAlbert(engine, nodes, degree, seed, edges);
  DGE_LOG_INFO("graph ready: %zu edges, memory %.1fMB", edges.size(),
               engine.stats().memory_bytes / 1024.0 / 1024.0);
  engine.SetCacheInvalidation(true);
  if (!wal_path.empty()) {
    engine.WriteSnapshot();
    engine.RotateWal();
    engine.SetWalEnabled(true);
    DGE_LOG_INFO("bulk load snapshot done, wal enabled");
  }

  dge::GraphService service(engine, dedup_ttl);
  dge::net::HttpServer server(port, reactors,
                              [&service](dge::net::HttpRequest&& req,
                                         std::function<void(int, std::string)> respond) {
    service.Handle(std::move(req), std::move(respond));
  });
  server.Start();

  std::signal(SIGINT, OnSignal);
  std::signal(SIGTERM, OnSignal);
  while (!g_stop.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
  }

  server.Shutdown();
  engine.Shutdown();
  DGE_LOG_INFO("server exited cleanly");
  return 0;
}
