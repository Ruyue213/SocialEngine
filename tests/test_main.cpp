#include <chrono>
#include <cstdio>
#include <cstring>
#include <future>
#include <random>
#include <set>
#include <string>
#include <vector>

#include "baseline/baseline_engine.h"
#include "common/varint.h"
#include "concurrency/thread_pool.h"
#include "concurrency/timer_heap.h"
#include "graph/dynamic_graph_engine.h"
#include "graph/engine_interface.h"
#include "graph/intersect.h"
#include "memory/slab_pool.h"
#include "storage/adjacency_store.h"
#include "storage/wal.h"

namespace {

int g_failures = 0;
int g_checks = 0;

void Check(bool cond, const char* msg) {
  ++g_checks;
  if (!cond) {
    ++g_failures;
    printf("  [FAIL] %s\n", msg);
  }
}

#define CHECK(cond) Check((cond), #cond)

std::vector<uint32_t> ToVec(const std::vector<uint32_t>& v) { return v; }

void TestVarint() {
  printf("[test] varint roundtrip\n");
  uint64_t values[] = {0, 1, 127, 128, 16383, 16384, 1ULL << 31, 1ULL << 32, ~0ULL};
  for (uint64_t v : values) {
    uint8_t buf[16];
    uint8_t* end = dge::put_varint(buf, v);
    uint64_t out = 0;
    const uint8_t* p = dge::get_varint(buf, end, out);
    CHECK(p == end);
    CHECK(out == v);
  }
}

void TestSlabBasics() {
  printf("[test] slab pool basics\n");
  dge::SlabPool pool;
  void* a = pool.Allocate(64);
  void* b = pool.Allocate(64);
  CHECK(a != nullptr && b != nullptr && a != b);
  CHECK(pool.round_up_class(64) == 64);
  CHECK(pool.round_up_class(70) == 128);
  CHECK(pool.round_up_class(4096) == 4096);
  CHECK(pool.bytes_in_use() == 128);
  pool.Deallocate(a, 64);
  pool.Deallocate(b, 64);
  CHECK(pool.bytes_in_use() == 0);
  void* c = pool.Allocate(64);
  CHECK(c == b);
  pool.Deallocate(c, 64);
}

void TestSlabCorruption() {
  printf("[test] slab corruption detection (magic sentinel)\n");
  dge::SlabPool pool;
  void* a = pool.Allocate(64);
  void* b = pool.Allocate(64);
  pool.Deallocate(a, 64);
  CHECK(pool.ValidateFreeBlock(a, 64));
  uint8_t* raw = static_cast<uint8_t*>(a);
  uint64_t junk = 0x1122334455667788ULL;
  std::memcpy(raw + 8, &junk, 8);
  CHECK(!pool.ValidateFreeBlock(a, 64));
  bool threw = false;
  try {
    pool.Allocate(64);
  } catch (const std::runtime_error&) {
    threw = true;
  }
  CHECK(threw);
  pool.Deallocate(b, 64);
  CHECK(!pool.ValidateFreeBlock(a, 64));
}

void TestSlabWildNext() {
  printf("[test] slab corruption detection (wild next pointer)\n");
  dge::SlabPool pool;
  void* a = pool.Allocate(128);
  pool.Deallocate(a, 128);
  uint8_t* raw = static_cast<uint8_t*>(a);
  uint64_t wild = 0xDEADDEADDEADDEADULL;
  std::memcpy(raw, &wild, 8);
  bool threw = false;
  try {
    pool.Allocate(128);
  } catch (const std::runtime_error&) {
    threw = true;
  }
  CHECK(threw);
}

void TestThreadPoolShed() {
  printf("[test] thread pool load shedding\n");
  dge::ThreadPool pool(2, 0, false, 0);
  int ran = 0;
  CHECK(!pool.Submit([&]() { ++ran; }));
  pool.SubmitInternal([&]() { ++ran; });
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  CHECK(ran == 1);
  pool.Shutdown();
}

void TestTimer() {
  printf("[test] min-heap timer\n");
  dge::TimerHeap timer;
  std::promise<void> p;
  auto fut = p.get_future();
  timer.Add(30, [&p]() { p.set_value(); });
  CHECK(fut.wait_for(std::chrono::milliseconds(1000)) == std::future_status::ready);
  timer.Shutdown();
}

void TestIntersect() {
  printf("[test] two-pointer intersection\n");
  std::vector<uint32_t> a = {1, 3, 5, 7, 9};
  std::vector<uint32_t> b = {2, 3, 5, 8};
  std::vector<uint32_t> out(16);
  size_t n = dge::IntersectSorted(a.data(), a.size(), b.data(), b.size(), out.data());
  CHECK(n == 2);
  CHECK(out[0] == 3 && out[1] == 5);
}

struct SmallEngines {
  std::unique_ptr<dge::DynamicGraphEngine> opt;
  std::unique_ptr<dge::BaselineGraphEngine> base;
};

SmallEngines MakeEngines(size_t nodes) {
  SmallEngines e;
  dge::DynamicGraphEngine::Config oc;
  oc.nodes = nodes;
  oc.buckets = 8;
  oc.pool_threads = 2;
  oc.queue_capacity = 4096;
  oc.cache_enabled = false;
  e.opt = std::make_unique<dge::DynamicGraphEngine>(oc);
  dge::BaselineGraphEngine::Config bc;
  bc.nodes = nodes;
  bc.pool_threads = 2;
  bc.queue_capacity = 4096;
  e.base = std::make_unique<dge::BaselineGraphEngine>(bc);
  return e;
}

dge::QueryResult SyncQuery(dge::IGraphEngine& e, dge::QueryType t, uint32_t a,
                            uint32_t b = 0) {
  std::promise<dge::QueryResult> p;
  auto fut = p.get_future();
  for (int attempt = 0; attempt < 5; ++attempt) {
    dge::QueryRequest req;
    req.type = t;
    req.a = a;
    req.b = b;
    req.cb = [&p](dge::QueryResult&& r) { p.set_value(std::move(r)); };
    if (e.Submit(std::move(req))) {
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  auto status = fut.wait_for(std::chrono::seconds(5));
  CHECK(status == std::future_status::ready);
  if (status == std::future_status::ready) {
    return fut.get();
  }
  return dge::QueryResult{};
}

void TestAddRemove() {
  printf("[test] O(1) add/remove edge semantics\n");
  auto e = MakeEngines(50);
  CHECK(e.opt->AddEdge(1, 2));
  CHECK(!e.opt->AddEdge(1, 2));
  CHECK(e.opt->RemoveEdge(1, 2));
  CHECK(!e.opt->RemoveEdge(1, 2));
  CHECK(!e.opt->AddEdge(0, 5));
  CHECK(!e.opt->AddEdge(1, 1));
  CHECK(!e.opt->AddEdge(99, 100));
  e.opt->Shutdown();
  e.base->Shutdown();
}

void TestSmallTopology() {
  printf("[test] query correctness on small fixed graph\n");
  auto e = MakeEngines(20);
  struct Edge {
    uint32_t u, v;
  };
  Edge edges[] = {{1, 2}, {2, 3}, {3, 4}, {1, 4}, {4, 5}, {5, 6}, {6, 7}, {1, 7}};
  for (auto& ed : edges) {
    CHECK(e.opt->AddEdge(ed.u, ed.v));
    CHECK(e.base->AddEdge(ed.u, ed.v));
  }
  CHECK(SyncQuery(*e.opt, dge::QueryType::kDistance, 1, 7).distance == 1);
  CHECK(SyncQuery(*e.opt, dge::QueryType::kDistance, 2, 7).distance == 2);
  CHECK(SyncQuery(*e.opt, dge::QueryType::kDistance, 1, 6).distance == 2);
  CHECK(SyncQuery(*e.opt, dge::QueryType::kDistance, 5, 1).distance == 2);
  CHECK(SyncQuery(*e.opt, dge::QueryType::kDistance, 8, 9).distance == -1);

  auto cf = SyncQuery(*e.opt, dge::QueryType::kCommonFriends, 2, 4);
  std::vector<uint32_t> expect_cf = {1, 3};
  CHECK(cf.friends == expect_cf);

  auto sd = SyncQuery(*e.opt, dge::QueryType::kSecondDegree, 1);
  std::vector<uint32_t> expect_sd = {3, 5, 6};
  CHECK(sd.friends == expect_sd);

  e.opt->Shutdown();
  e.base->Shutdown();
}

void TestRandomEquivalence(size_t nodes, uint64_t seed, int samples) {
  printf("[test] optimized vs baseline equivalence (nodes=%zu, samples=%d)\n", nodes,
         samples);
  auto e = MakeEngines(nodes);
  std::mt19937_64 rng(seed);
  std::vector<std::pair<uint32_t, uint32_t>> added;
  for (size_t i = 0; i < nodes * 4; ++i) {
    uint32_t u = 1 + rng() % nodes;
    uint32_t v = 1 + rng() % nodes;
    if (u == v) {
      continue;
    }
    bool ok = e.opt->AddEdge(u, v);
    CHECK(ok == e.base->AddEdge(u, v));
    if (ok) {
      added.emplace_back(u, v);
    }
  }
  for (size_t i = 0; i < samples; ++i) {
    uint32_t u = 1 + rng() % nodes;
    uint32_t v = 1 + rng() % nodes;
    if (u == v) {
      v = v % nodes + 1;
    }
    auto d1 = SyncQuery(*e.opt, dge::QueryType::kDistance, u, v);
    auto d2 = SyncQuery(*e.base, dge::QueryType::kDistance, u, v);
    if (d1.distance != d2.distance) {
      printf("  distance mismatch u=%u v=%u: %d vs %d\n", u, v, d1.distance, d2.distance);
      CHECK(false);
    }
    auto c1 = SyncQuery(*e.opt, dge::QueryType::kCommonFriends, u, v);
    auto c2 = SyncQuery(*e.base, dge::QueryType::kCommonFriends, u, v);
    if (c1.friends != c2.friends) {
      printf("  common friends mismatch u=%u v=%u\n", u, v);
      CHECK(false);
    }
    auto s1 = SyncQuery(*e.opt, dge::QueryType::kSecondDegree, u);
    auto s2 = SyncQuery(*e.base, dge::QueryType::kSecondDegree, u);
    if (s1.friends != s2.friends) {
      printf("  second degree mismatch u=%u\n", u);
      CHECK(false);
    }
  }
  if (!added.empty()) {
    for (size_t i = 0; i < added.size() / 2; ++i) {
      const auto& ed = added[rng() % added.size()];
      CHECK(e.opt->RemoveEdge(ed.first, ed.second) ==
            e.base->RemoveEdge(ed.first, ed.second));
    }
    for (size_t i = 0; i < 50; ++i) {
      uint32_t u = 1 + rng() % nodes;
      uint32_t v = 1 + rng() % nodes;
      if (u == v) {
        v = v % nodes + 1;
      }
      auto d1 = SyncQuery(*e.opt, dge::QueryType::kDistance, u, v);
      auto d2 = SyncQuery(*e.base, dge::QueryType::kDistance, u, v);
      if (d1.distance != d2.distance) {
        printf("  post-remove distance mismatch u=%u v=%u: %d vs %d\n", u, v, d1.distance,
               d2.distance);
        CHECK(false);
      }
    }
  }
  e.opt->Shutdown();
  e.base->Shutdown();
}

void TestCompaction() {
  printf("[test] tail growth + block compression + hole compaction\n");
  auto e = MakeEngines(2000);
  for (uint32_t v = 2; v <= 700; ++v) {
    CHECK(e.opt->AddEdge(1, v));
    CHECK(e.base->AddEdge(1, v));
  }
  auto d1 = SyncQuery(*e.opt, dge::QueryType::kDistance, 1, 700);
  auto d2 = SyncQuery(*e.base, dge::QueryType::kDistance, 1, 700);
  CHECK(d1.distance == 1 && d2.distance == 1);
  auto cf = SyncQuery(*e.opt, dge::QueryType::kCommonFriends, 1, 2);
  auto cfb = SyncQuery(*e.base, dge::QueryType::kCommonFriends, 1, 2);
  CHECK(cf.friends == cfb.friends);

  for (uint32_t v = 2; v <= 400; ++v) {
    CHECK(e.opt->RemoveEdge(1, v));
    CHECK(e.base->RemoveEdge(1, v));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  auto d3 = SyncQuery(*e.opt, dge::QueryType::kDistance, 1, 2);
  auto d4 = SyncQuery(*e.base, dge::QueryType::kDistance, 1, 2);
  CHECK(d3.distance == d4.distance);
  auto d5 = SyncQuery(*e.opt, dge::QueryType::kDistance, 1, 500);
  CHECK(d5.distance == 1);

  dge::EngineStats s = e.opt->stats();
  CHECK(s.compactions > 0);
  printf("  compactions=%llu memory=%.1fKB\n",
         static_cast<unsigned long long>(s.compactions), s.memory_bytes / 1024.0);

  auto sd = SyncQuery(*e.opt, dge::QueryType::kSecondDegree, 700);
  auto sdb = SyncQuery(*e.base, dge::QueryType::kSecondDegree, 700);
  CHECK(sd.friends == sdb.friends);

  e.opt->Shutdown();
  e.base->Shutdown();
}

void TestMultiBlock() {
  printf("[test] multi-block compression correctness (spaced ids)\n");
  auto e = MakeEngines(20000);
  std::vector<uint32_t> targets;
  for (uint32_t id = 2; id <= 19000; id += 37) {
    targets.push_back(id);
  }
  for (uint32_t t : targets) {
    CHECK(e.opt->AddEdge(1, t));
    CHECK(e.base->AddEdge(1, t));
  }
  for (uint32_t t : targets) {
    auto d1 = SyncQuery(*e.opt, dge::QueryType::kDistance, 1, t);
    auto d2 = SyncQuery(*e.base, dge::QueryType::kDistance, 1, t);
    CHECK(d1.distance == d2.distance);
    if (d1.distance != d2.distance) {
      printf("  dist mismatch for %u: %d vs %d\n", t, d1.distance, d2.distance);
      break;
    }
  }
  auto cf1 = SyncQuery(*e.opt, dge::QueryType::kCommonFriends, 1, targets[0]);
  auto cf2 = SyncQuery(*e.base, dge::QueryType::kCommonFriends, 1, targets[0]);
  CHECK(cf1.friends == cf2.friends);
  auto sd1 = SyncQuery(*e.opt, dge::QueryType::kSecondDegree, 1);
  auto sd2 = SyncQuery(*e.base, dge::QueryType::kSecondDegree, 1);
  CHECK(sd1.friends == sd2.friends);
  CHECK(sd1.friends.size() == sd2.friends.size());

  for (size_t i = 0; i < targets.size() / 2; ++i) {
    CHECK(e.opt->RemoveEdge(1, targets[i]));
    CHECK(e.base->RemoveEdge(1, targets[i]));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  auto d3 = SyncQuery(*e.opt, dge::QueryType::kDistance, 1, targets.back());
  auto d4 = SyncQuery(*e.base, dge::QueryType::kDistance, 1, targets.back());
  CHECK(d3.distance == d4.distance);
  auto cf3 = SyncQuery(*e.opt, dge::QueryType::kCommonFriends, 1, targets[targets.size() - 2]);
  auto cf4 = SyncQuery(*e.base, dge::QueryType::kCommonFriends, 1, targets[targets.size() - 2]);
  CHECK(cf3.friends == cf4.friends);

  e.opt->Shutdown();
  e.base->Shutdown();
}

void TestWalSyncRecovery() {
  printf("[test] wal sync mode crash recovery\n");
  const char* dir = "/tmp/dge_wal_test";
  std::string cmd = "rm -rf " + std::string(dir) + " && mkdir -p " + std::string(dir);
  system(cmd.c_str());
  std::string wal_path = std::string(dir) + "/wal.log";
  std::string snap_path = std::string(dir) + "/snap.bin";

  auto make_engine = [&](bool) {
    dge::DynamicGraphEngine::Config cfg;
    cfg.nodes = 500;
    cfg.buckets = 8;
    cfg.pool_threads = 2;
    cfg.cache_enabled = false;
    // 修复夹具:恢复引擎必须配置 WAL/快照路径,否则 Recover 在
    // wal_path 为空时提前返回,测试实际在空图上与基线比较,
    // 依赖稀疏图距离多为 -1 的巧合通过,掩盖真实回归。
    cfg.wal_path = wal_path;
    cfg.wal_sync_mode = true;
    cfg.snapshot_path = snap_path;
    return std::make_unique<dge::DynamicGraphEngine>(cfg);
  };
  dge::BaselineGraphEngine::Config bc;
  bc.nodes = 500;
  bc.pool_threads = 2;
  dge::BaselineGraphEngine base(bc);

  std::mt19937_64 rng(777);
  std::vector<std::pair<uint32_t, uint32_t>> ops;
  auto eng = make_engine(true);
  for (int i = 0; i < 300; ++i) {
    uint32_t u = 1 + rng() % 500;
    uint32_t v = 1 + rng() % 500;
    if (u == v) {
      continue;
    }
    bool add = (i % 3) != 0;
    if (add) {
      eng->AddEdge(u, v);
      base.AddEdge(u, v);
      ops.emplace_back(0, i);
    } else {
      eng->RemoveEdge(u, v);
      base.RemoveEdge(u, v);
      ops.emplace_back(1, i);
    }
  }
  eng->Shutdown();

  auto rec = make_engine(false);
  CHECK(rec->Recover());
  for (int i = 0; i < 100; ++i) {
    uint32_t u = 1 + rng() % 500;
    uint32_t v = 1 + rng() % 500;
    if (u == v) {
      continue;
    }
    auto d1 = SyncQuery(*rec, dge::QueryType::kDistance, u, v);
    auto d2 = SyncQuery(base, dge::QueryType::kDistance, u, v);
    if (d1.distance != d2.distance) {
      printf("  wal recovery dist mismatch %u-%u: %d vs %d\n", u, v, d1.distance, d2.distance);
      CHECK(false);
      break;
    }
    auto c1 = SyncQuery(*rec, dge::QueryType::kCommonFriends, u, v);
    auto c2 = SyncQuery(base, dge::QueryType::kCommonFriends, u, v);
    if (c1.friends != c2.friends) {
      printf("  wal recovery cf mismatch %u-%u\n", u, v);
      CHECK(false);
      break;
    }
  }
  rec->Shutdown();
  base.Shutdown();
}

void TestWalSnapshotRecovery() {
  printf("[test] wal + snapshot recovery\n");
  const char* dir = "/tmp/dge_wal_test2";
  std::string cmd = "rm -rf " + std::string(dir) + " && mkdir -p " + std::string(dir);
  system(cmd.c_str());
  std::string wal_path = std::string(dir) + "/wal.log";
  std::string snap_path = std::string(dir) + "/snap.bin";

  auto make_engine = [&]() {
    dge::DynamicGraphEngine::Config cfg;
    cfg.nodes = 500;
    cfg.buckets = 8;
    cfg.pool_threads = 2;
    cfg.cache_enabled = false;
    cfg.wal_path = wal_path;
    cfg.wal_sync_mode = true;
    cfg.snapshot_path = snap_path;
    return std::make_unique<dge::DynamicGraphEngine>(cfg);
  };
  dge::BaselineGraphEngine::Config bc;
  bc.nodes = 500;
  bc.pool_threads = 2;
  dge::BaselineGraphEngine base(bc);

  std::mt19937_64 rng(888);
  auto eng = make_engine();
  for (int i = 0; i < 150; ++i) {
    uint32_t u = 1 + rng() % 500;
    uint32_t v = 1 + rng() % 500;
    if (u == v) {
      continue;
    }
    eng->AddEdge(u, v);
    base.AddEdge(u, v);
  }
  CHECK(eng->Checkpoint());
  for (int i = 0; i < 100; ++i) {
    uint32_t u = 1 + rng() % 500;
    uint32_t v = 1 + rng() % 500;
    if (u == v) {
      continue;
    }
    if (i % 2 == 0) {
      eng->AddEdge(u, v);
      base.AddEdge(u, v);
    } else {
      eng->RemoveEdge(u, v);
      base.RemoveEdge(u, v);
    }
  }
  eng->Shutdown();

  auto rec = make_engine();
  CHECK(rec->Recover());
  for (int i = 0; i < 100; ++i) {
    uint32_t u = 1 + rng() % 500;
    uint32_t v = 1 + rng() % 500;
    if (u == v) {
      continue;
    }
    auto d1 = SyncQuery(*rec, dge::QueryType::kDistance, u, v);
    auto d2 = SyncQuery(base, dge::QueryType::kDistance, u, v);
    if (d1.distance != d2.distance) {
      printf("  wal+snapshot recovery dist mismatch %u-%u: %d vs %d\n", u, v, d1.distance,
             d2.distance);
      CHECK(false);
      break;
    }
  }
  rec->Shutdown();
  base.Shutdown();
}

void TestWalTornWrite() {
  printf("[test] wal torn write tolerance\n");
  const char* dir = "/tmp/dge_wal_test3";
  std::string cmd = "rm -rf " + std::string(dir) + " && mkdir -p " + std::string(dir);
  system(cmd.c_str());
  std::string wal_path = std::string(dir) + "/wal.log";

  auto make_engine = [&]() {
    dge::DynamicGraphEngine::Config cfg;
    cfg.nodes = 200;
    cfg.buckets = 8;
    cfg.pool_threads = 2;
    cfg.cache_enabled = false;
    cfg.wal_path = wal_path;
    cfg.wal_sync_mode = true;
    return std::make_unique<dge::DynamicGraphEngine>(cfg);
  };
  dge::BaselineGraphEngine::Config bc;
  bc.nodes = 200;
  bc.pool_threads = 2;
  dge::BaselineGraphEngine base(bc);

  auto eng = make_engine();
  for (uint32_t v = 2; v <= 60; ++v) {
    eng->AddEdge(1, v);
    base.AddEdge(1, v);
  }
  eng->Shutdown();

  std::fstream f(wal_path, std::ios::binary | std::ios::in | std::ios::out);
  f.seekp(0, std::ios::end);
  auto size = f.tellp();
  f.seekp(static_cast<std::streamoff>(size) - 10, std::ios::beg);
  f.write("XXXXXXXXXX", 10);
  f.close();

  auto rec = make_engine();
  CHECK(rec->Recover());
  auto d1 = SyncQuery(*rec, dge::QueryType::kDistance, 1, 59);
  CHECK(d1.distance == 1);
  auto d2 = SyncQuery(*rec, dge::QueryType::kDistance, 1, 60);
  CHECK(d2.distance == -1);
  rec->Shutdown();
  base.Shutdown();
}

void TestConcurrentAddRemove() {
  printf("[test] concurrent updates + queries stay consistent\n");
  auto e = MakeEngines(500);
  std::vector<std::thread> threads;
  for (int t = 0; t < 4; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937_64 rng(1000 + t);
      for (int i = 0; i < 2000; ++i) {
        uint32_t u = 1 + rng() % 500;
        uint32_t v = 1 + rng() % 500;
        if (u == v) {
          continue;
        }
        if (i % 2 == 0) {
          e.opt->AddEdge(u, v);
        } else {
          e.opt->RemoveEdge(u, v);
        }
      }
    });
  }
  for (int t = 0; t < 2; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937_64 rng(2000 + t);
      for (int i = 0; i < 2000; ++i) {
        uint32_t u = 1 + rng() % 500;
        uint32_t v = 1 + rng() % 500;
        if (u == v) {
          continue;
        }
        SyncQuery(*e.opt, dge::QueryType::kDistance, u, v);
        SyncQuery(*e.opt, dge::QueryType::kCommonFriends, u, v);
      }
    });
  }
  for (auto& th : threads) {
    if (th.joinable()) {
      th.join();
    }
  }
  e.opt->Shutdown();
  e.base->Shutdown();
  printf("  done\n");
}

}  // namespace

int main() {
  setvbuf(stdout, nullptr, _IONBF, 0);
  TestVarint();
  TestSlabBasics();
  TestSlabCorruption();
  TestSlabWildNext();
  TestThreadPoolShed();
  TestTimer();
  TestIntersect();
  TestAddRemove();
  TestSmallTopology();
  TestRandomEquivalence(200, 12345, 200);
  TestCompaction();
  TestMultiBlock();
  TestWalSyncRecovery();
  TestWalSnapshotRecovery();
  TestWalTornWrite();
  TestConcurrentAddRemove();

  printf("\n%d checks, %d failures\n", g_checks, g_failures);
  return g_failures == 0 ? 0 : 1;
}
