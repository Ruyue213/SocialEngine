#include "storage/wal.h"

#include <chrono>
#include <cstring>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>

#include "common/logging.h"

namespace dge {

namespace {

int FsyncFd(int fd) {
#if defined(__linux__)
  return ::fdatasync(fd);
#elif defined(__APPLE__)
  return ::fsync(fd);
#else
  return ::fsync(fd);
#endif
}

constexpr uint32_t kWalMagic = 0x3157414C;  // 'WAL1'
constexpr uint32_t kSnapMagic = 0x50414E53;  // 'SNAP'

uint32_t Crc32(const uint8_t* data, size_t len) {
  uint32_t crc = 0xFFFFFFFFu;
  for (size_t i = 0; i < len; ++i) {
    crc ^= data[i];
    for (int j = 0; j < 8; ++j) {
      uint32_t mask = static_cast<uint32_t>(-(int32_t)(crc & 1));
      crc = (crc >> 1) ^ (0xEDB88320u & mask);
    }
  }
  return ~crc;
}

void AppendU64(std::string& out, uint64_t v) {
  for (int i = 0; i < 8; ++i) {
    out.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
  }
}

void AppendU32(std::string& out, uint32_t v) {
  for (int i = 0; i < 4; ++i) {
    out.push_back(static_cast<char>((v >> (i * 8)) & 0xFF));
  }
}

bool ReadU64(const uint8_t* p, uint64_t& v) {
  v = 0;
  for (int i = 0; i < 8; ++i) {
    v |= static_cast<uint64_t>(p[i]) << (i * 8);
  }
  return true;
}

bool ReadU32(const uint8_t* p, uint32_t& v) {
  v = 0;
  for (int i = 0; i < 4; ++i) {
    v |= static_cast<uint32_t>(p[i]) << (i * 8);
  }
  return true;
}

}  // namespace

WalWriter::WalWriter(const std::string& path, bool sync_mode, size_t checkpoint_bytes,
                     CheckpointFn on_checkpoint)
    : path_(path),
      sync_mode_(sync_mode),
      checkpoint_bytes_(checkpoint_bytes),
      on_checkpoint_(std::move(on_checkpoint)) {
  fd_ = ::open(path_.c_str(), O_CREAT | O_WRONLY | O_APPEND, 0644);
  if (fd_ < 0) {
    DGE_LOG_ERROR("wal open failed: %s", std::strerror(errno));
  }
  if (!sync_mode_) {
    bg_ = std::thread([this]() { BackgroundLoop(); });
  }
}

WalWriter::~WalWriter() { Shutdown(); }

bool WalWriter::Append(uint8_t op, uint64_t u, uint64_t v) {
  uint64_t seq = last_seq_.fetch_add(1, std::memory_order_relaxed) + 1;
  std::string rec;
  rec.reserve(33);
  AppendU32(rec, kWalMagic);
  rec.push_back(static_cast<char>(op));
  AppendU64(rec, seq);
  AppendU64(rec, u);
  AppendU64(rec, v);
  // CRC 覆盖范围含操作码(偏移 4 起):原先从偏移 5 起计算,add/del
  // 单字节篡改(偏移 4)不会改变校验值,损坏记录会被当作合法操作回放。
  uint32_t crc = Crc32(reinterpret_cast<const uint8_t*>(rec.data() + 4), 25);
  AppendU32(rec, crc);

  if (sync_mode_) {
    std::lock_guard<std::mutex> lk(mu_);
    if (fd_ < 0) {
      errors_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    ssize_t n = ::write(fd_, rec.data(), rec.size());
    if (n != static_cast<ssize_t>(rec.size())) {
      errors_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    if (FsyncFd(fd_) != 0) {
      errors_.fetch_add(1, std::memory_order_relaxed);
      return false;
    }
    bytes_.fetch_add(rec.size(), std::memory_order_relaxed);
    return true;
  }

  {
    std::lock_guard<std::mutex> lk(mu_);
    buffer_ += rec;
  }
  return true;
}

void WalWriter::AdvanceSeq(uint64_t seq) {
  uint64_t cur = last_seq_.load(std::memory_order_relaxed);
  while (cur < seq &&
         !last_seq_.compare_exchange_weak(cur, seq, std::memory_order_acq_rel)) {
  }
}

void WalWriter::FlushLocked() {
  if (buffer_.empty()) {
    return;
  }
  if (fd_ >= 0) {
    ssize_t n = ::write(fd_, buffer_.data(), buffer_.size());
    if (n < 0) {
      errors_.fetch_add(1, std::memory_order_relaxed);
    } else {
      bytes_.fetch_add(static_cast<uint64_t>(n), std::memory_order_relaxed);
      if (FsyncFd(fd_) != 0) {
        errors_.fetch_add(1, std::memory_order_relaxed);
      }
    }
  } else {
    errors_.fetch_add(1, std::memory_order_relaxed);
  }
  buffer_.clear();
}

void WalWriter::BackgroundLoop() {
  while (!stop_.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    // 修复:检查点请求在锁内只置标志,回调移到锁外执行。
    // 原实现持 mu_ 调用 on_checkpoint_,回调进入 WriteSnapshot →
    // Rotate 再次获取同一把非递归 mu_,构成必现死锁;同时回调
    // 持有 mu_ 期间逐桶取共享锁,与更新路径「桶锁 → WAL 锁」形成
    // 反向依赖(AB-BA)。锁外执行后依赖方向单一,两类死锁同时消除。
    bool request_ckpt = false;
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (bytes_.load(std::memory_order_relaxed) > checkpoint_bytes_ && on_checkpoint_) {
        request_ckpt = true;
      }
      FlushLocked();
    }
    if (request_ckpt) {
      on_checkpoint_();
    }
  }
  std::lock_guard<std::mutex> lk(mu_);
  FlushLocked();
}

void WalWriter::Rotate() {
  std::lock_guard<std::mutex> lk(mu_);
  if (fd_ >= 0) {
    ::ftruncate(fd_, 0);
    ::lseek(fd_, 0, SEEK_SET);
  }
  DGE_LOG_INFO("wal rotated");
}

void WalWriter::Shutdown() {
  bool expected = false;
  if (!stop_.compare_exchange_strong(expected, true)) {
    return;
  }
  if (bg_.joinable()) {
    bg_.join();
  }
  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

bool RecoverFromWal(const std::string& wal_path, const std::string& snapshot_path,
                    std::function<void(uint8_t op, uint64_t u, uint64_t v)> apply,
                    uint64_t* out_last_seq) {
  uint64_t snapshot_lsn = 0;
  {
    std::ifstream sf(snapshot_path, std::ios::binary);
    if (sf.good()) {
      uint8_t hdr[20];
      sf.read(reinterpret_cast<char*>(hdr), 20);
      if (sf.gcount() == 20) {
        uint32_t magic = 0;
        ReadU32(hdr, magic);
        if (magic == kSnapMagic) {
          ReadU64(hdr + 4, snapshot_lsn);
          uint64_t count = 0;
          ReadU64(hdr + 12, count);
          for (uint64_t i = 0; i < count; ++i) {
            uint8_t eb[16];
            sf.read(reinterpret_cast<char*>(eb), 16);
            if (sf.gcount() != 16) {
              DGE_LOG_ERROR("snapshot truncated");
              return false;
            }
            uint64_t u = 0, v = 0;
            ReadU64(eb, u);
            ReadU64(eb + 8, v);
            apply(0x01, u, v);
          }
          DGE_LOG_INFO("snapshot loaded: lsn=%llu", static_cast<unsigned long long>(snapshot_lsn));
        }
      }
    }
  }
  uint64_t max_seq = snapshot_lsn;

  std::ifstream wf(wal_path, std::ios::binary);
  if (!wf.good()) {
    DGE_LOG_INFO("no wal file, recovered from snapshot only");
    if (out_last_seq != nullptr) {
      *out_last_seq = max_seq;
    }
    return true;
  }
  uint64_t replayed = 0;
  // good_offset 跟踪最后一条完整通过校验的记录边界;恢复结束后将
  // 文件截断到该边界,否则 O_APPEND 追加的新记录排在坏尾之后,
  // 下次恢复在坏尾处停止回放,新记录永久不可达。
  std::streamoff good_offset = 0;
  bool tail_bad = false;
  while (wf.good()) {
    uint8_t rec[33];
    wf.read(reinterpret_cast<char*>(rec), 33);
    std::streamsize got = wf.gcount();
    if (got == 0) {
      break;
    }
    if (got < 33) {
      DGE_LOG_WARN("wal tail truncated (%zd bytes), stop replay", got);
      tail_bad = true;
      break;
    }
    uint32_t magic = 0;
    ReadU32(rec, magic);
    if (magic != kWalMagic) {
      DGE_LOG_WARN("wal magic mismatch, stop replay");
      tail_bad = true;
      break;
    }
    uint32_t crc = 0;
    ReadU32(rec + 29, crc);
    if (Crc32(rec + 4, 25) != crc) {
      DGE_LOG_WARN("wal crc mismatch, stop replay (torn write)");
      tail_bad = true;
      break;
    }
    uint64_t seq = 0, u = 0, v = 0;
    ReadU64(rec + 5, seq);
    ReadU64(rec + 13, u);
    ReadU64(rec + 21, v);
    if (seq > snapshot_lsn) {
      apply(rec[4], u, v);
      ++replayed;
    }
    if (seq > max_seq) {
      max_seq = seq;
    }
    good_offset += 33;
  }
  if (tail_bad) {
    if (::truncate(wal_path.c_str(), good_offset) != 0) {
      DGE_LOG_ERROR("wal tail truncate failed: %s", std::strerror(errno));
    } else {
      DGE_LOG_INFO("wal truncated to last valid record (%lld bytes)",
                   static_cast<long long>(good_offset));
    }
  }
  DGE_LOG_INFO("wal replay done: %llu records, last_seq=%llu",
               static_cast<unsigned long long>(replayed),
               static_cast<unsigned long long>(max_seq));
  if (out_last_seq != nullptr) {
    *out_last_seq = max_seq;
  }
  return true;
}

bool WriteSnapshotFile(const std::string& snapshot_path, uint64_t snapshot_lsn,
                       const std::function<bool(std::function<void(uint64_t, uint64_t)>)>&
                           foreach_edge) {
  // 崩溃原子性协议(2026-10 第二轮审查加固):
  //   1. 写临时文件(完整写入)
  //   2. fsync 临时文件——数据先于元数据落盘
  //   3. rename 原子发布(POSIX 保证 rename 的原子性)
  //   4. fsync 父目录——让 rename 本身持久化;断电后 rename 要么
  //      完全生效要么完全未生效,不会出现「快照路径指向未落盘数据」
  // 原实现用 ofstream 直接写 + close 后立即 rename:close 不保证
  // fsync,断电时 rename 可能先于数据持久化,恢复读到空/损坏快照。
  std::string tmp = snapshot_path + ".tmp";
  std::string body;
  std::string head;
  AppendU32(head, kSnapMagic);
  AppendU64(head, snapshot_lsn);
  uint64_t count = 0;
  bool ok = foreach_edge([&](uint64_t u, uint64_t v) {
    AppendU64(body, u);
    AppendU64(body, v);
    ++count;
  });
  AppendU64(head, count);
  if (!ok) {
    DGE_LOG_ERROR("snapshot iterate failed");
    return false;
  }
  int fd = ::open(tmp.c_str(), O_CREAT | O_WRONLY | O_TRUNC, 0644);
  if (fd < 0) {
    DGE_LOG_ERROR("snapshot open failed: %s", std::strerror(errno));
    return false;
  }
  auto write_all = [&](const std::string& data) {
    size_t off = 0;
    while (off < data.size()) {
      ssize_t n = ::write(fd, data.data() + off, data.size() - off);
      if (n <= 0) {
        if (n < 0 && errno == EINTR) {
          continue;
        }
        DGE_LOG_ERROR("snapshot write failed: %s", std::strerror(errno));
        ::close(fd);
        ::unlink(tmp.c_str());
        return false;
      }
      off += static_cast<size_t>(n);
    }
    return true;
  };
  if (!write_all(head) || !write_all(body)) {
    return false;
  }
  if (FsyncFd(fd) != 0) {
    DGE_LOG_ERROR("snapshot fsync failed: %s", std::strerror(errno));
    ::close(fd);
    ::unlink(tmp.c_str());
    return false;
  }
  if (::close(fd) != 0) {
    DGE_LOG_ERROR("snapshot close failed: %s", std::strerror(errno));
    ::unlink(tmp.c_str());
    return false;
  }
  if (::rename(tmp.c_str(), snapshot_path.c_str()) != 0) {
    DGE_LOG_ERROR("snapshot rename failed: %s", std::strerror(errno));
    ::unlink(tmp.c_str());
    return false;
  }
  // 目录 fsync:持久化 rename 目录项。
  size_t slash = snapshot_path.find_last_of('/');
  std::string dir = slash == std::string::npos ? "." : snapshot_path.substr(0, slash);
  int dfd = ::open(dir.c_str(), O_RDONLY);
  if (dfd >= 0) {
    ::fsync(dfd);
    ::close(dfd);
  }
  DGE_LOG_INFO("snapshot written: %llu edges at lsn=%llu (fsynced)",
               static_cast<unsigned long long>(count),
               static_cast<unsigned long long>(snapshot_lsn));
  return true;
}

}  // namespace dge
