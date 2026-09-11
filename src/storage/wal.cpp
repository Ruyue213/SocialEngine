#include "storage/wal.h"

#include <chrono>
#include <cstring>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>

#include "common/logging.h"

namespace dge {

namespace {

void FsyncFd(int fd) {
#if defined(__linux__)
  ::fdatasync(fd);
#elif defined(__APPLE__)
  ::fsync(fd);
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
  uint32_t crc = Crc32(reinterpret_cast<const uint8_t*>(rec.data() + 5), 24);
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
    FsyncFd(fd_);
    bytes_.fetch_add(rec.size(), std::memory_order_relaxed);
    return true;
  }

  {
    std::lock_guard<std::mutex> lk(mu_);
    buffer_ += rec;
  }
  return true;
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
      FsyncFd(fd_);
    }
  } else {
    errors_.fetch_add(1, std::memory_order_relaxed);
  }
  buffer_.clear();
}

void WalWriter::BackgroundLoop() {
  auto last_fsync = std::chrono::steady_clock::now();
  while (!stop_.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
    {
      std::lock_guard<std::mutex> lk(mu_);
      if (bytes_.load(std::memory_order_relaxed) > checkpoint_bytes_ && on_checkpoint_) {
        on_checkpoint_();
      }
      FlushLocked();
    }
    auto now = std::chrono::steady_clock::now();
    (void)now;
    (void)last_fsync;
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
                    std::function<void(uint8_t op, uint64_t u, uint64_t v)> apply) {
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

  std::ifstream wf(wal_path, std::ios::binary);
  if (!wf.good()) {
    DGE_LOG_INFO("no wal file, recovered from snapshot only");
    return true;
  }
  uint64_t replayed = 0;
  while (wf.good()) {
    uint8_t rec[33];
    wf.read(reinterpret_cast<char*>(rec), 33);
    std::streamsize got = wf.gcount();
    if (got == 0) {
      break;
    }
    if (got < 33) {
      DGE_LOG_WARN("wal tail truncated (%zd bytes), stop replay", got);
      break;
    }
    uint32_t magic = 0;
    ReadU32(rec, magic);
    if (magic != kWalMagic) {
      DGE_LOG_WARN("wal magic mismatch, stop replay");
      break;
    }
    uint32_t crc = 0;
    ReadU32(rec + 29, crc);
    if (Crc32(rec + 5, 24) != crc) {
      DGE_LOG_WARN("wal crc mismatch, stop replay (torn write)");
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
  }
  DGE_LOG_INFO("wal replay done: %llu records", static_cast<unsigned long long>(replayed));
  return true;
}

bool WriteSnapshotFile(const std::string& snapshot_path, uint64_t snapshot_lsn,
                       const std::function<bool(std::function<void(uint64_t, uint64_t)>)>&
                           foreach_edge) {
  std::string tmp = snapshot_path + ".tmp";
  std::ofstream of(tmp, std::ios::binary | std::ios::trunc);
  if (!of.good()) {
    DGE_LOG_ERROR("snapshot open failed: %s", tmp.c_str());
    return false;
  }
  std::string body;
  uint64_t count = 0;
  bool ok = foreach_edge([&](uint64_t u, uint64_t v) {
    AppendU64(body, u);
    AppendU64(body, v);
    ++count;
  });
  if (!ok) {
    return false;
  }
  std::string head;
  AppendU32(head, kSnapMagic);
  AppendU64(head, snapshot_lsn);
  AppendU64(head, count);
  of.write(head.data(), static_cast<std::streamsize>(head.size()));
  of.write(body.data(), static_cast<std::streamsize>(body.size()));
  of.close();
  if (::rename(tmp.c_str(), snapshot_path.c_str()) != 0) {
    DGE_LOG_ERROR("snapshot rename failed: %s", std::strerror(errno));
    return false;
  }
  DGE_LOG_INFO("snapshot written: %llu edges at lsn=%llu",
               static_cast<unsigned long long>(count),
               static_cast<unsigned long long>(snapshot_lsn));
  return true;
}

}  // namespace dge
