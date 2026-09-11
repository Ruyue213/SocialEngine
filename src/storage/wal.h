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

bool RecoverFromWal(const std::string& wal_path, const std::string& snapshot_path,
                    std::function<void(uint8_t op, uint64_t u, uint64_t v)> apply);

bool WriteSnapshotFile(const std::string& snapshot_path, uint64_t snapshot_lsn,
                       const std::function<bool(std::function<void(uint64_t, uint64_t)>)>&
                           foreach_edge);

}  // namespace dge
