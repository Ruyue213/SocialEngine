#pragma once

namespace dge {
namespace net {

class WakeFd {
 public:
  WakeFd();
  ~WakeFd();
  WakeFd(const WakeFd&) = delete;
  WakeFd& operator=(const WakeFd&) = delete;

  void Notify();
  void Drain();
  int fd() const { return read_fd_; }

 private:
  int read_fd_ = -1;
  int write_fd_ = -1;
};

}  // namespace net
}  // namespace dge
