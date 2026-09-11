#include "net/wake_fd.h"

#include <cerrno>
#include <fcntl.h>
#include <unistd.h>

#if defined(__linux__)
#include <sys/eventfd.h>
#endif

namespace dge {
namespace net {

WakeFd::WakeFd() {
#if defined(__linux__)
  read_fd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  write_fd_ = read_fd_;
#else
  int fds[2];
  if (::pipe(fds) != 0) {
    return;
  }
  read_fd_ = fds[0];
  write_fd_ = fds[1];
  ::fcntl(read_fd_, F_SETFL, O_NONBLOCK);
  ::fcntl(write_fd_, F_SETFL, O_NONBLOCK);
#endif
}

WakeFd::~WakeFd() {
  if (read_fd_ >= 0) {
    ::close(read_fd_);
  }
#if !defined(__linux__)
  if (write_fd_ >= 0) {
    ::close(write_fd_);
  }
#endif
}

void WakeFd::Notify() {
#if defined(__linux__)
  uint64_t one = 1;
  ssize_t n = ::write(write_fd_, &one, sizeof(one));
  (void)n;
#else
  char c = 'w';
  ssize_t n = ::write(write_fd_, &c, 1);
  (void)n;
#endif
}

void WakeFd::Drain() {
  char buf[64];
  while (::read(read_fd_, buf, sizeof(buf)) > 0) {
  }
}

}  // namespace net
}  // namespace dge
