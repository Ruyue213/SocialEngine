# 性能工程方法论与历史数据

本文档记录本项目的性能测量方法、历次优化的 before/after 数据与复现命令。

## 测量方法

- **压测工具**：`bin/bench`，双引擎同拓扑、同随机种子、同负载对比，排除环境偏差
- **负载模型**：BA 无标度拓扑（模拟真实社交网络 hub 结构）+ Zipf 热点查询采样
- **延迟直方图**：对数桶直方图（2^n 边界），报告 avg/p50/p90/p99/p999
- **内存**：Slab 池 + 头部表 + 工作线程 visited 数组的精确账本；生产环境用 Valgrind Massif 复核
- **火焰图**：Linux `perf record -g` + FlameGraph；macOS `sample <pid>` 抓线程栈
- **回归防线**：`scripts/bench_regress.sh` 每次跑均匀负载基准并对比上次结果，退化 >5% 即报警

## 优化历史

| # | 优化 | 手段 | 效果（300k 节点/16 客户端） |
|---|---|---|---|
| 1 | 分层存储 | 压缩块+尾部缓冲，varint 差值压缩，Slab 池 | 内存 281MB→180MB（-36%） |
| 2 | 免 memset BFS | Epoch 时间戳替代布尔数组清零 | 距离查询延迟 ~10x |
| 3 | 锁粒度拆分 | 哈希分片 K 桶 + shared_mutex 读放行 | QPS ~10x（对比全局锁基线） |
| 4 | 无锁一致性读 | COW 邻接 + 全局 Epoch 回收（快照读免桶锁） | 均匀负载 -4% 开销换读一致性 |
| 5 | 缓存路径压缩 | EVALSHA 脚本缓存 + 批量流水线 + 读优先队列 | IO 吞吐 ~3x，写饥饿消除 |
| 6 | 容忍窗口 | 更新后 100ms 脏窗口内旧缓存可用（延迟双删兜底） | 命中率 8%→20% |
| 7 | 热点控制 | Load Shedding + 指数退避，队列满即 503 | 过载下延迟有界，无 OOM |

## 复现命令

```bash
# 均匀负载（引擎核心能力）
./bin/bench --nodes 300000 --seconds 8 --clients 16 --no-redis --zipf 0

# 热点负载（社交场景真实形态）
./bin/bench --nodes 300000 --seconds 8 --clients 16 --no-redis

# HTTP 全链路
./bin/server --nodes 300000 --port 8080 &
./bin/bench --nodes 300000 --seconds 8 --clients 16 --http 127.0.0.1:8080

# 回归
scripts/bench_regress.sh
```

## 火焰图定位实录（工作流示例）

1. 压测中 `sample <pid> 2 -file prof.txt` 抓 2 秒线程栈
2. 按线程聚合栈帧频次 → 热点函数排序
3. 定位到 `ReadersActive()` 每次写扫描全部 ticket → 改为 O(1) 读者计数
4. 定位到 `std::list` 基线引擎列表损坏 → 换 `vector<vector>` 实现
