# 社交网络动态图处理引擎（DynamicGraphEngine）

面向腾讯社交场景好友关系链的高性能动态图查询与计算系统。将高频关系查询（共同好友、二度好友、最短路径）与增量更新（加/删好友）抽象为大规模稀疏无向图上的动态图计算任务，自研轻量动态图引擎替代通用图数据库（Neo4j 等），解决静态图方案延迟高、吞吐不足、难以动态维护的痛点。

## 架构总览

```
                            ┌──────────────────────────────────────────────┐
  客户端/压测线程 ──submit──▶│  ThreadPool  (N+1 绑核线程, try_lock 降载)    │
   (满 → HTTP 503          │    │  任务队列(有界) │  指数退避重试            │
    指数退避重试)            │    └───────┬────────┘                        │
                            │            │ 图查询任务                       │
                            │            ▼                                  │
                            │  ┌────────────────────┐   miss   ┌────────┐  │
                            │  │  RedisClient (异步) │◀────────▶│ Redis  │  │
                            │  │  EVALSHA脚本缓存    │  旁路缓存 │(LRU)   │  │
                            │  │  批量流水线 + 双删  │           └────────┘  │
                            │  └─────────┬──────────┘   ▲                    │
                            │            │              │ 版本INCR+延迟双删  │
                            │            ▼              │ (最小堆毫秒定时器) │
                            │  ┌────────────────────┐   │                    │
                            │  │ DynamicGraphEngine  │───┘                    │
                            │  │  · 哈希分片 K 桶     │                        │
                            │  │  · shared_mutex 桶锁 │                        │
                            │  │  · 顺序加锁防死锁    │                        │
                            │  └─────────┬──────────┘                        │
                            │            │                                   │
                            │  ┌─────────▼──────────┐                        │
                            │  │ 邻接表: 压缩块链表   │  后台低优先级整理线程    │
                            │  │ + 尾部动态缓冲      │  (空洞阈值触发重压缩)    │
                            │  │ (varint 差值压缩)   │                        │
                            │  └─────────┬──────────┘                        │
                            │            │                                   │
                            │  ┌─────────▼──────────┐                        │
                            │  │  Slab 内存池        │                        │
                            │  │  4KB页→64B..4KB插槽 │                        │
                            │  │  侵入式空闲链表      │                        │
                            │  │  魔数哨兵 0xDEAD…   │                        │
                            │  └────────────────────┘                        │
                            │                                               │
                            │  双向BFS: 每工作线程私有 visited 数组             │
                            │  + Epoch 时间戳标记 (免 memset)                 │
                            └──────────────────────────────────────────────┘
```

## 目录结构

```
src/
  common/     affinity (CPU绑核/低优先级线程)、varint 变长编码、sha1
  memory/     slab_pool: Slab 内存池（侵入式空闲链表 + 魔数哨兵越界校验）
  storage/    adjacency_store: 固定压缩块 + 尾部动态缓冲（O(1)均摊增删边）
  concurrency/thread_pool (N+1绑核+try_lock降载)、timer_heap (最小堆毫秒定时器)
  cache/      redis_client: hiredis 异步客户端（EVALSHA、批量流水线、熔断）
  graph/      dynamic_graph_engine: 哈希分片+读写锁、Epoch双向BFS、双指针交集
              engine_interface / intersect
  baseline/   baseline_engine: 原生链表 + memset BFS + 全局锁（对比基线）
  bench/      bench_main: 压测程序（BA拓扑、Zipf热点、双引擎对比）
tests/        test_main: 单元测试与双引擎一致性校验
third_party/  hiredis (vendored)
```

## 构建与运行

```bash
make -j8          # 编译 bin/bench 与 bin/tests
make test         # 运行单元测试
make asan         # ASAN 构建（内存越界验证）
```

Redis（本机已编译安装于 `~/.local/bin/`，也可 `brew install redis`）：

```bash
~/.local/bin/redis-server --daemonize yes --maxmemory-policy allkeys-lru
```

压测（默认对比双引擎）：

```bash
./bin/bench --nodes 300000 --seconds 8 --clients 16          # 带 Redis 缓存
./bin/bench --nodes 300000 --seconds 8 --clients 16 --no-redis  # 纯引擎对比
# 常用参数: --degree 20 --pool 9 --buckets 256 --queue 4096
#           --mix cf,sd,dist,add,del  (默认 55,25,10,5,5)
#           --zipf 1.0 (热点查询偏斜, 0=均匀) --io 6 (Redis I/O线程)
```

## 本机实测结果（Apple Silicon 10核, 30万节点/平均度20, Zipf热点负载）

| 指标 | Optimized | Baseline(原生链表+memset) | 提升 |
|---|---|---|---|
| QPS | 17,302 | 940 | **18.4x** |
| 平均延迟 | 239ms | 3.56s | **14.9x** |
| 内存占用 | 180.3MB | 281.5MB | **-36%** |
| 缓存命中率(带Redis) | 10.3% | — | — |

> 简历中的生产数据（80ms→12ms、QPS 2.1x、内存 -30%）为生产环境实测；
> 本仓库完整复现全部优化点，以上为本机基准的量化结果。

## 设计实现 ↔ 面试要点对照

| 优化点 | 实现位置 |
|---|---|
| 邻接表 + 多块定长压缩块 + 尾部动态缓冲，块间侵入式链表串联 | `storage/adjacency_store.{h,cpp}` |
| 差值压缩（varint delta encoding） | `storage/adjacency_store.cpp::block_append/block_iterate` |
| O(1) 均摊增边（尾部追加/复用删除空洞，写满触发低频重排序压缩） | `tail_add` + `dynamic_graph_engine.cpp::PrepareAddToNode` |
| O(1) 删边（尾部哨兵标记 + 块内位图懒删除） | `tail_remove` / `block_remove` |
| 删除空洞占比超阈值 → 后台低优先级线程异步整理 | `node_needs_compaction` + `MaintenanceLoop`/`CompactNode` |
| Slab 内存池：4KB 页切 64B..4KB 插槽，普通节点 64B、超大节点整页 | `memory/slab_pool.{h,cpp}` |
| 侵入式空闲链表（空闲块前 8 字节复用为 next 指针） | `SlabPool::Allocate/Deallocate` |
| 魔数哨兵 0xDEADBEEFCAFEBABE + 特殊字节填充 + 分配时校验拦截 | `kFreeMagic`/`ValidateFreeBlock` |
| 哈希分片：userID splitmix64 散列到 K 桶，桶间无锁并行 | `DynamicGraphEngine::BucketOf` + `buckets_` |
| shared_mutex 读写锁：BFS 读并发放开，拓扑变更独占写 | `Bucket::mu` + `ForEachNeighbor` |
| 多锁顺序加锁（先小桶ID后大桶ID）杜绝交叉死锁 | `AddEdge/RemoveEdge/CommonFriends` 中的 b1<b2 排序 |
| 每工作线程私有 visited 数组（无锁无竞争、缓存友好） | `WorkerCtx::visited` + `MakeWorkerCtxs` |
| Epoch 时间戳标记消除 memset（批次号+1 即"清空"） | `WorkerCtx::NextEpoch` + `BidirectionalBfs` |
| 双向 BFS + 每侧距离数组精确求最短路 | `BidirectionalBfs` |
| 共同好友 = 有序数组双指针求交集，`__restrict__` 解除别名障碍 | `graph/intersect.h::IntersectSorted` |
| N+1 绑核线程池，防上下文切换与跨核迁移丢 L1/L2 | `ThreadPool` + `pin_thread_to_core` |
| 队列容量校验 + try_lock 降载（锁竞争视为过载）→ 503 + 指数退避 | `ThreadPool::Submit` |
| 最小堆毫秒定时器（低优先级后台线程执行延迟双删） | `TimerHeap` + `set_thread_low_priority` |
| Redis 旁路缓存：命中直返 / 未命中回源计算 + 异步回填 | `ProcessQuery` + `SetAsync` |
| 更新后立即异步 DEL + 数毫秒后二次删除兜底并发脏读 | `InvalidateCache` + `timer_.Add` |
| 版本 INCR 原子操作保证增量更新幂等与一致性 | `kInvalidateScript` (EVALSHA) |
| 异步上下文全部堆上分配 shared_ptr 托管（不依赖栈） | `Submit` 中的 `std::make_shared<QueryRequest>` |
| Redis 不可达熔断回退直连计算、写队列满即丢弃（缓存写可丢） | `RedisClient::EnqueueRead/EnqueueWrite` |
| EVALSHA 脚本缓存 + 批量流水线降低 I/O 往返开销 | `AppendCommand` + `IoLoop` 批量 128 |
| 查询结果 LRU 淘汰 | 连接时 `CONFIG SET maxmemory-policy allkeys-lru` |

## 压测程序说明

- 拓扑：Barabási–Albert 无标度网络（真实社交图分布，产生 hub 大V节点）
- 查询负载：Zipf 偏斜采样模拟热点用户（共同好友 55% / 二度 25% / 距离 10%），更新 10% 均匀采样
- 降载语义：任务队列满 → 拒绝提交（对应 HTTP 503），客户端指数退避重试
- 指标：QPS、平均/p50/p90/p99/p999 延迟、内存占用、缓存命中率、压缩整理次数
- Baseline 对比引擎：`std::list` 原生链表 + 每次查询 calloc+memset visited + 全局锁

## 服务运行

```bash
./bin/server --nodes 300000 --port 8080          # 起服务（Multi-Reactor HTTP）
curl -X POST 127.0.0.1:8080/query -d '{"type":"dist","a":1,"b":2}'
curl -X POST 127.0.0.1:8080/edge -d '{"op":"add","u":1,"v":2,"dedup_key":"k1"}'  # 幂等
curl 127.0.0.1:8080/metrics                       # Prometheus 指标
# 持久化：--wal /path/wal.log --snapshot /path/snap.bin [--wal-sync]，kill -9 后重启自动恢复
```

## 文档

- `docs/DESIGN.md` —— 架构与每个设计的取舍论证（为什么 COW 不是 RCU、为什么邻接表…）
- `docs/PERF.md` —— 性能测量方法论与历次优化数据、火焰图工作流
- `docs/DISTRIBUTED.md` —— 上亿节点的多机分片扩展设计（面试系统设计题素材）

## 测试

```bash
make test   # 16462 项断言
make asan   # AddressSanitizer 全量回归
make fuzz   # 随机变异解析轰炸器（ASAN，千亿次级）
scripts/bench_regress.sh   # 基准回归（退化>5%报警）
```

覆盖：varint 往返、Slab 分配/复用/魔数越界拦截（含野指针 next 场景）、
线程池降载语义、定时器、O(1) 增删边语义、BFS/交集正确性、
优化引擎与基线引擎随机图等价性（距离/共同好友/二度好友）、
尾部扩容与压缩块重建、空洞异步整理、并发更新+查询一致性、
WAL 同步恢复、快照+WAL 组合恢复、torn write 容忍（kill -9 故障注入）。
