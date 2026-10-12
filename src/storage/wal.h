#pragma once

#include <atomic>
#include <cstdint>
#include <fstream>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

namespace dge {

struct WalRecord {
  uint8_t op;  // 0x01 add, 0x02 del
  uint64_t seq;
  uint64_t u;
  uint64_t v;
};

class WalWriter {
 public:
  using CheckpointFn = std::function<void()>;

  WalWriter(const std::string& path, bool sync_mode, size_t checkpoint_bytes,
            CheckpointFn on_checkpoint);
  ~WalWriter();

  WalWriter(const WalWriter&) = delete;
  WalWriter& operator=(const WalWriter&) = delete;

  bool Append(uint8_t op, uint64_t u, uint64_t v);

  // 恢复后回传已回放的最大序号,保证重启后新记录序号延续持久化历史。
  void AdvanceSeq(uint64_t seq);

  uint64_t last_seq() const { return last_seq_.load(std::memory_order_relaxed); }

  void Rotate();
  uint64_t wal_errors() const { return errors_.load(std::memory_order_relaxed); }
  uint64_t bytes_written() const { return bytes_.load(std::memory_order_relaxed); }

  void Shutdown();

 private:
  void FlushLocked();
  void BackgroundLoop();

  std::string path_;
  bool sync_mode_;
  size_t checkpoint_bytes_;
  CheckpointFn on_checkpoint_;

  std::mutex mu_;
  std::string buffer_;
  int fd_ = -1;
  std::atomic<uint64_t> last_seq_{0};
  std::atomic<uint64_t> errors_{0};
  std::atomic<uint64_t> bytes_{0};
  std::atomic<bool> stop_{false};
  std::thread bg_;
};

// 恢复快照+WAL。out_last_seq 返回已回放(含快照覆盖)的最大序号,
// 调用方必须将其回传给 WalWriter::AdvanceSeq,否则重启后新记录序号
// 从 1 重新计数,下次恢复按 seq > snapshot_lsn 过滤时会被跳过。
// 同时将 WAL 文件截断到最后一条有效记录边界,丢弃坏尾,保证后续
// 追加的记录在下次恢复时可达。
bool RecoverFromWal(const std::string& wal_path, const std::string& snapshot_path,
                    std::function<void(uint8_t op, uint64_t u, uint64_t v)> apply,
                    uint64_t* out_last_seq = nullptr);

bool WriteSnapshotFile(const std::string& snapshot_path, uint64_t snapshot_lsn,
                       const std::function<bool(std::function<void(uint64_t, uint64_t)>)>&
                           foreach_edge);

}  // namespace dge
