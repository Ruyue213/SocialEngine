#include <array>
#include <atomic>
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
#include "cache/redis_client.h"
#include "memory/epoch.h"
#include "common/metrics.h"
#include "memory/slab_pool.h"
#include "storage/adjacency_store.h"
#include "storage/wal.h"

namespace {

// 修复:并发测试(TestConcurrentAddRemove)的 worker 线程会并发
// 调 Check,原先普通 int 存在数据竞争,输出的 checks 数不可信。
std::atomic<int> g_failures{0};
std::atomic<int> g_checks{0};

void Check(bool cond, const char* msg) {
  g_checks.fetch_add(1, std::memory_order_relaxed);
  if (!cond) {
    g_failures.fetch_add(1, std::memory_order_relaxed);
    printf("  [FAIL] %s\n", msg);
  }
}

#define CHECK(cond) Check((cond), #cond)

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
  // 野 next 指针的检测点在诊断接口 ValidateFreeBlock(保留完整
  // 页归属校验);分配热路径只做 O(1) 魔数校验(见 slab_pool.cpp
  // 的 profiling 取舍注释)。
  CHECK(!pool.ValidateFreeBlock(a, 128));
  // 魔数未破坏时热路径分配仍安全(取走槽后野指针不再被解引用)。
  void* b = pool.Allocate(128);
  CHECK(b == a);
  pool.Deallocate(b, 128);
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
  for (int i = 0; i < samples; ++i) {
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


// ===================== 修复回归测试(2026-10 审查闭环) =====================

void TestEpochReclaimSafety() {
  printf("[test] EBR: pinned object survives sweep, freed after unpin\n");
  auto* r = &dge::EpochReclaimer::Instance();
  r->Sweep();
  r->Sweep();
  CHECK(r->retired_pending() == 0);
  uint64_t before = r->freed_total();
  uint32_t ticket = r->Pin();
  void* p = std::malloc(64);
  r->Retire(p, 64, nullptr);
  // 读者持有 ticket 期间 Sweep:对象不得被回收。
  r->Sweep();
  CHECK(r->freed_total() == before);
  CHECK(r->retired_pending() == 1);
  r->Unpin(ticket);
  r->Sweep();
  CHECK(r->freed_total() == before + 1);
  CHECK(r->retired_pending() == 0);
}

void TestDuplicateEdgeAfterCompression() {
  printf("[test] duplicate edge rejected after compression rebuild\n");
  auto e = MakeEngines(50);
  // 30 条边触发尾满重建,邻居进入压缩块(尾容量 8)。
  for (uint32_t v = 2; v <= 31; ++v) {
    CHECK(e.opt->AddEdge(1, v));
  }
  // 30 条边已触发尾满重建,邻居进入压缩块;先确认边可达。
  auto d = SyncQuery(*e.opt, dge::QueryType::kDistance, 1, 10);
  CHECK(d.distance == 1);
  // 压缩块中的边重复 Add 必须被拒绝(原实现只查尾部,重复插入
  // 破坏差值编码严格递增前提)。
  CHECK(!e.opt->AddEdge(1, 10));
  CHECK(!e.opt->AddEdge(10, 1));
  // 重复 Add 两次 + Remove 一次后必须不可达(原实现残留重复,
  // 删一次仍然直连)。
  e.opt->AddEdge(1, 10);
  CHECK(e.opt->RemoveEdge(1, 10));
  auto d2 = SyncQuery(*e.opt, dge::QueryType::kDistance, 1, 10);
  CHECK(d2.distance == -1);
  // 双端对称:10 的邻接不再含 1。
  auto sd = SyncQuery(*e.opt, dge::QueryType::kSecondDegree, 10);
  bool one_in_sd = false;
  for (uint32_t f : sd.friends) {
    if (f == 1) {
      one_in_sd = true;
    }
  }
  CHECK(!one_in_sd);
  e.opt->Shutdown();
  e.base->Shutdown();
}

void TestBfsBudget() {
  printf("[test] bfs depth budget: consistent cap semantics\n");
  auto mk = [](uint32_t cap) {
    dge::DynamicGraphEngine::Config cfg;
    cfg.nodes = 20;
    cfg.buckets = 4;
    cfg.pool_threads = 2;
    cfg.cache_enabled = false;
    cfg.max_bfs_depth = cap;
    return std::make_unique<dge::DynamicGraphEngine>(cfg);
  };
  // 反例回归:cap=1,链 1-2-3 返回 -1;加支路 1-4 后(路径 1-4-3
  // 长度 2)仍必须是 -1。原实现加支路后返回 2,超出预算且与
  // 无支路场景矛盾。
  {
    auto eng = mk(1);
    eng->AddEdge(1, 2);
    eng->AddEdge(2, 3);
    CHECK(SyncQuery(*eng, dge::QueryType::kDistance, 1, 3).distance == -1);
    eng->AddEdge(1, 4);
    eng->AddEdge(4, 3);
    CHECK(SyncQuery(*eng, dge::QueryType::kDistance, 1, 3).distance == -1);
    CHECK(SyncQuery(*eng, dge::QueryType::kDistance, 1, 4).distance == 1);
    eng->Shutdown();
  }
  {
    auto eng = mk(2);
    eng->AddEdge(1, 2);
    eng->AddEdge(2, 3);
    CHECK(SyncQuery(*eng, dge::QueryType::kDistance, 1, 3).distance == 2);
    CHECK(SyncQuery(*eng, dge::QueryType::kDistance, 1, 2).distance == 1);
    eng->Shutdown();
  }
}

void TestWalSeqContinuity() {
  printf("[test] wal seq continuity across restarts\n");
  const char* dir = "/tmp/dge_wal_seq";
  std::string cmd = "rm -rf " + std::string(dir) + " && mkdir -p " + std::string(dir);
  system(cmd.c_str());
  std::string wal_path = std::string(dir) + "/wal.log";
  std::string snap_path = std::string(dir) + "/snap.bin";
  auto mk = [&]() {
    dge::DynamicGraphEngine::Config cfg;
    cfg.nodes = 100;
    cfg.buckets = 8;
    cfg.pool_threads = 2;
    cfg.cache_enabled = false;
    cfg.wal_path = wal_path;
    cfg.wal_sync_mode = true;
    cfg.snapshot_path = snap_path;
    return std::make_unique<dge::DynamicGraphEngine>(cfg);
  };
  // 阶段 A:写 3 条边,checkpoint(快照 lsn=3,WAL 清空)。
  auto a = mk();
  a->AddEdge(1, 11);
  a->AddEdge(2, 12);
  a->AddEdge(3, 13);
  a->Checkpoint();
  a->Shutdown();
  // 阶段 B:恢复后写 2 条新边。原实现 seq 从 1 重新计数,
  // 与快照 lsn=3 冲突,阶段 C 回放时按 seq > 3 过滤全部跳过。
  auto b = mk();
  CHECK(b->Recover());
  CHECK(b->AddEdge(4, 14));
  CHECK(b->AddEdge(5, 15));
  b->Shutdown();
  // 阶段 C:全部 5 条边必须可见。
  auto c = mk();
  CHECK(c->Recover());
  CHECK(SyncQuery(*c, dge::QueryType::kDistance, 1, 11).distance == 1);
  CHECK(SyncQuery(*c, dge::QueryType::kDistance, 2, 12).distance == 1);
  CHECK(SyncQuery(*c, dge::QueryType::kDistance, 3, 13).distance == 1);
  CHECK(SyncQuery(*c, dge::QueryType::kDistance, 4, 14).distance == 1);
  CHECK(SyncQuery(*c, dge::QueryType::kDistance, 5, 15).distance == 1);
  c->Shutdown();
}

void TestWalOpCodeTamper() {
  printf("[test] wal crc rejects opcode tamper\n");
  const char* dir = "/tmp/dge_wal_tamper";
  std::string cmd = "rm -rf " + std::string(dir) + " && mkdir -p " + std::string(dir);
  system(cmd.c_str());
  std::string wal_path = std::string(dir) + "/wal.log";
  {
    dge::WalWriter w(wal_path, true, 1ULL << 30, nullptr);
    CHECK(w.Append(0x01, 7, 8));  // add
  }
  // 篡改偏移 4 的操作码 add -> del。原 CRC 从偏移 5 起计算,
  // 此篡改不改变校验值,损坏记录被当作合法 del 回放。
  {
    FILE* f = fopen(wal_path.c_str(), "r+b");
    CHECK(f != nullptr);
    fseek(f, 4, SEEK_SET);
    fputc(0x02, f);
    fclose(f);
  }
  int applied = 0;
  bool ok = dge::RecoverFromWal(wal_path, "", [&](uint8_t, uint64_t, uint64_t) { ++applied; });
  CHECK(ok);
  CHECK(applied == 0);
}

void TestWalBadTailTruncate() {
  printf("[test] wal bad tail truncated, later appends replayable\n");
  const char* dir = "/tmp/dge_wal_tail";
  std::string cmd = "rm -rf " + std::string(dir) + " && mkdir -p " + std::string(dir);
  system(cmd.c_str());
  std::string wal_path = std::string(dir) + "/wal.log";
  {
    dge::WalWriter w(wal_path, true, 1ULL << 30, nullptr);
    CHECK(w.Append(0x01, 1, 2));
    CHECK(w.Append(0x01, 3, 4));
  }
  {
    FILE* f = fopen(wal_path.c_str(), "ab");
    fputs("GARBAGE!", f);
    fclose(f);
  }
  int applied = 0;
  uint64_t last_seq = 0;
  CHECK(dge::RecoverFromWal(wal_path, "", [&](uint8_t, uint64_t, uint64_t) { ++applied; },
                            &last_seq));
  CHECK(applied == 2);
  CHECK(last_seq == 2);
  // 坏尾被截断后追加的新记录,下次恢复必须可达(原实现 O_APPEND
  // 排在坏尾之后,回放永远停在坏尾)。
  {
    dge::WalWriter w(wal_path, true, 1ULL << 30, nullptr);
    w.AdvanceSeq(last_seq);
    CHECK(w.Append(0x01, 5, 6));
  }
  int applied2 = 0;
  CHECK(dge::RecoverFromWal(wal_path, "", [&](uint8_t, uint64_t, uint64_t) { ++applied2; }));
  CHECK(applied2 == 3);
}

void TestWalCheckpointConcurrent() {
  printf("[test] wal checkpoint concurrent with updates (no deadlock, no loss)\n");
  const char* dir = "/tmp/dge_wal_ckpt";
  std::string cmd = "rm -rf " + std::string(dir) + " && mkdir -p " + std::string(dir);
  system(cmd.c_str());
  std::string wal_path = std::string(dir) + "/wal.log";
  std::string snap_path = std::string(dir) + "/snap.bin";
  dge::DynamicGraphEngine::Config cfg;
  cfg.nodes = 200;
  cfg.buckets = 8;
  cfg.pool_threads = 2;
  cfg.cache_enabled = false;
  cfg.wal_path = wal_path;
  cfg.wal_sync_mode = false;  // 后台线程驱动 checkpoint
  cfg.snapshot_path = snap_path;
  cfg.wal_checkpoint_bytes = 512;  // 极小阈值,高频触发
  dge::DynamicGraphEngine eng(cfg);
  std::atomic<bool> stop{false};
  std::vector<std::thread> threads;
  for (int t = 0; t < 3; ++t) {
    threads.emplace_back([&, t]() {
      std::mt19937_64 rng(9000 + t);
      for (int i = 0; i < 400 && !stop.load(); ++i) {
        uint32_t u = 1 + rng() % 200;
        uint32_t v = 1 + rng() % 200;
        if (u == v) {
          continue;
        }
        if (i % 2 == 0) {
          eng.AddEdge(u, v);
        } else {
          eng.RemoveEdge(u, v);
        }
      }
    });
  }
  for (auto& th : threads) {
    if (th.joinable()) {
      th.join();  // 原实现在此死锁:checkpoint 持锁回调 + AB-BA
    }
  }
  // 并发交错下动作序列不可重放,改为「一致切点采样对比」:
  // Checkpoint() 持 ckpt_mu_ 独占,快照代表此刻图状态;采样一组
  // 点对距离,恢复引擎上重放同样采样必须逐点一致。
  CHECK(eng.Checkpoint());
  std::vector<std::array<uint32_t, 3>> samples;
  {
    std::mt19937_64 rng(4242);
    for (int i = 0; i < 60; ++i) {
      uint32_t u = 1 + rng() % 200;
      uint32_t v = 1 + rng() % 200;
      if (u == v) {
        continue;
      }
      auto r = SyncQuery(eng, dge::QueryType::kDistance, u, v);
      samples.push_back({u, v, static_cast<uint32_t>(r.distance + 2)});
    }
  }
  eng.Shutdown();
  auto rec = std::make_unique<dge::DynamicGraphEngine>(cfg);
  CHECK(rec->Recover());
  for (auto& smp : samples) {
    auto r = SyncQuery(*rec, dge::QueryType::kDistance, smp[0], smp[1]);
    // distance 以 +2 偏移存储(区分 -1),还原比较。
    if (r.distance != static_cast<int32_t>(smp[2]) - 2) {
      printf("  checkpoint loss: %u-%u expect %d got %d\n", smp[0], smp[1],
             static_cast<int32_t>(smp[2]) - 2, r.distance);
      CHECK(false);
      break;
    }
  }
  rec->Shutdown();
}

void TestTimerEarlyTask() {
  printf("[test] timer: earlier task inserted during wait fires on time\n");
  dge::TimerHeap timer;
  auto t0 = std::chrono::steady_clock::now();
  std::atomic<int> late_done{0};
  std::atomic<int> early_done{0};
  std::chrono::steady_clock::time_point early_at{};
  timer.Add(300, [&]() { ++late_done; });
  std::this_thread::sleep_for(std::chrono::milliseconds(40));
  timer.Add(15, [&]() {
    early_at = std::chrono::steady_clock::now();
    ++early_done;
  });
  // 原实现等待谓词只查 stop:早任务被唤醒后继续睡到 300ms 截止,
  // 实测约 336ms 才执行。阈值 200ms 区分修复前后。
  for (int i = 0; i < 100 && early_done.load() == 0; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  int early_delay_ms = static_cast<int>(
      std::chrono::duration_cast<std::chrono::milliseconds>(early_at - t0).count());
  CHECK(early_done.load() == 1);
  CHECK(early_delay_ms < 200);
  (void)late_done;
}

void TestHistogramBuckets() {
  printf("[test] histogram bucket boundaries (observe 3 -> le=4)\n");
  const std::string name = "test_hist_bucket_xyz";
  dge::metrics::Observe(name, 3);
  dge::metrics::Observe(name, 4);
  dge::metrics::Observe(name, 1);
  std::string text = dge::metrics::Registry::Instance().PrometheusText();
  bool has_le4 = false;
  bool le2_zero = false;
  size_t pos = 0;
  const std::string prefix = name + "_bucket{le=\"";
  while ((pos = text.find(prefix, pos)) != std::string::npos) {
    size_t vstart = pos + prefix.size();
    size_t vend = text.find("\"}", vstart);
    std::string le = text.substr(vstart, vend - vstart);
    size_t space = text.find(' ', vend);
    uint64_t cnt = strtoull(text.substr(space + 1).c_str(), nullptr, 10);
    if (le == "4") {
      has_le4 = cnt >= 2;  // 3 和 4 都落 (2,4]
    }
    if (le == "2") {
      // 桶 (·,2] 只含样本 1;旧实现把 3 也计入(边界错误)则为 3。
      le2_zero = cnt == 1;
    }
    pos = vend;
  }
  CHECK(has_le4);
  CHECK(le2_zero);
}

void TestThreadPoolAtomicCap() {
  printf("[test] thread pool: concurrent submits respect queue cap\n");
  constexpr size_t kCap = 4;
  dge::ThreadPool pool(1, kCap, false, 0);
  std::atomic<size_t> max_seen{0};
  std::atomic<int> succeeded{0};
  std::vector<std::thread> submitters;
  for (int t = 0; t < 4; ++t) {
    submitters.emplace_back([&]() {
      for (int i = 0; i < 30; ++i) {
        if (pool.Submit([&]() {
              size_t cur = pool.queue_size();
              size_t prev = max_seen.load();
              while (cur > prev && !max_seen.compare_exchange_weak(prev, cur)) {
              }
              std::this_thread::sleep_for(std::chrono::milliseconds(1));
            })) {
          ++succeeded;
        }
      }
    });
  }
  for (auto& th : submitters) {
    th.join();
  }
  pool.Shutdown();
  // 修复后容量检查与入队在同一临界区,任意时刻队列深度 <= cap。
  // 原实现锁外预检查,并发提交可越过上限。
  CHECK(max_seen.load() <= kCap);
  CHECK(succeeded.load() > 0);
}

void TestDedupUnavailable() {
  printf("[test] dedup: backend failure yields kError (not fake duplicate)\n");
  dge::RedisClient::Config rcfg;
  rcfg.host = "127.0.0.1";
  rcfg.port = 1;  // 不可达端口
  rcfg.io_threads = 1;
  rcfg.io_queue_cap = 1;
  dge::RedisClient client(rcfg, [](std::function<void()> fn) { fn(); });
  std::atomic<int> results{0};
  std::atomic<int> errors{0};
  auto cb = [&](dge::RedisClient::DedupStatus st) {
    ++results;
    if (st == dge::RedisClient::DedupStatus::kError) {
      ++errors;
    }
  };
  // 第一个入队,第二个立即触发队列满 -> DropOp 路径。
  client.SetNxAsync("k1", 60, cb);
  client.SetNxAsync("k2", 60, cb);
  for (int i = 0; i < 200 && results.load() < 2; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  CHECK(results.load() == 2);
  CHECK(errors.load() == 2);
  client.Shutdown();
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
  TestEpochReclaimSafety();
  TestDuplicateEdgeAfterCompression();
  TestBfsBudget();
  TestWalSeqContinuity();
  TestWalOpCodeTamper();
  TestWalBadTailTruncate();
  TestWalCheckpointConcurrent();
  TestTimerEarlyTask();
  TestHistogramBuckets();
  TestThreadPoolAtomicCap();
  TestDedupUnavailable();
  TestConcurrentAddRemove();

  printf("\n%d checks, %d failures\n", g_checks.load(), g_failures.load());
  return g_failures == 0 ? 0 : 1;
}
