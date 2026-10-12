#include "net/poller.h"

#include <cerrno>
#include <cstring>
#include <stdexcept>

#include <fcntl.h>
#include <unistd.h>

#if defined(__linux__)
#include <sys/epoll.h>
#elif defined(__APPLE__)
#include <sys/event.h>
#include <sys/types.h>
#include <sys/time.h>
#endif

namespace dge {
namespace net {

Poller::Poller() {
#if defined(__linux__)
  fd_ = ::epoll_create1(EPOLL_CLOEXEC);
#elif defined(__APPLE__)
  fd_ = ::kqueue();
#endif
  if (fd_ < 0) {
    throw std::runtime_error("poller create failed");
  }
}

Poller::~Poller() {
  if (fd_ >= 0) {
    ::close(fd_);
  }
}

void Poller::Add(int fd, uint32_t events, void* udata) {
  (void)udata;
#if defined(__linux__)
  struct epoll_event ev;
  std::memset(&ev, 0, sizeof(ev));
  ev.events = EPOLLET;
  if (events & kRead) {
    ev.events |= EPOLLIN;
  }
  if (events & kWrite) {
    ev.events |= EPOLLOUT;
  }
  // 修复:统一以 fd 作为事件载荷。原实现注册时写 epoll_data.ptr,
  // 返回时却读同一 union 的 fd 成员——指针位模式被当成连接 fd,
  // SubReactor 按错误值查连接表,事件无法关联到正确连接。
  // 生命周期由连接表(fd → Conn)管理,与 kqueue 路径一致。
  ev.data.fd = fd;
  ::epoll_ctl(fd_, EPOLL_CTL_ADD, fd, &ev);
#elif defined(__APPLE__)
  struct kevent changes[2];
  int n = 0;
  if (events & kRead) {
    EV_SET(&changes[n++], fd, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, udata);
  }
  if (events & kWrite) {
    EV_SET(&changes[n++], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, udata);
  }
  ::kevent(fd_, changes, n, nullptr, 0, nullptr);
#endif
}

void Poller::Mod(int fd, uint32_t events, void* udata) {
  (void)udata;
#if defined(__linux__)
  struct epoll_event ev;
  std::memset(&ev, 0, sizeof(ev));
  ev.events = EPOLLET;
  if (events & kRead) {
    ev.events |= EPOLLIN;
  }
  if (events & kWrite) {
    ev.events |= EPOLLOUT;
  }
  ev.data.fd = fd;
  ::epoll_ctl(fd_, EPOLL_CTL_MOD, fd, &ev);
#elif defined(__APPLE__)
  struct kevent changes[2];
  int n = 0;
  if (events & kRead) {
    EV_SET(&changes[n++], fd, EVFILT_READ, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, udata);
  }
  if (events & kWrite) {
    EV_SET(&changes[n++], fd, EVFILT_WRITE, EV_ADD | EV_ENABLE | EV_CLEAR, 0, 0, udata);
  }
  ::kevent(fd_, changes, n, nullptr, 0, nullptr);
#endif
}

void Poller::Del(int fd) {
#if defined(__linux__)
  ::epoll_ctl(fd_, EPOLL_CTL_DEL, fd, nullptr);
#elif defined(__APPLE__)
  struct kevent change;
  EV_SET(&change, fd, EVFILT_READ, EV_DELETE, 0, 0, nullptr);
  ::kevent(fd_, &change, 1, nullptr, 0, nullptr);
  EV_SET(&change, fd, EVFILT_WRITE, EV_DELETE, 0, 0, nullptr);
  ::kevent(fd_, &change, 1, nullptr, 0, nullptr);
#endif
}

int Poller::Wait(std::vector<PollEvent>& out, int timeout_ms) {
#if defined(__linux__)
  constexpr int kMaxEvents = 256;
  struct epoll_event events[kMaxEvents];
  int n = ::epoll_wait(fd_, events, kMaxEvents, timeout_ms);
  if (n < 0) {
    if (errno == EINTR) {
      return 0;
    }
    return -1;
  }
  out.resize(static_cast<size_t>(n));
  for (int i = 0; i < n; ++i) {
    // 与注册端约定一致:读 data.fd(注册时写入的也是 fd)。
    out[i].fd = events[i].data.fd;
    out[i].udata = nullptr;  // 连接上下文由上层经 fd 查表获得
    out[i].events = 0;
    if (events[i].events & (EPOLLIN | EPOLLHUP | EPOLLERR)) {
      out[i].events |= kRead;
    }
    if (events[i].events & EPOLLOUT) {
      out[i].events |= kWrite;
    }
  }
#elif defined(__APPLE__)
  constexpr int kMaxEvents = 256;
  struct kevent events[kMaxEvents];
  struct timespec ts;
  ts.tv_sec = timeout_ms / 1000;
  ts.tv_nsec = static_cast<long>(timeout_ms % 1000) * 1000000L;
  int n = ::kevent(fd_, nullptr, 0, events, kMaxEvents, &ts);
  if (n < 0) {
    if (errno == EINTR) {
      return 0;
    }
    return -1;
  }
  out.resize(static_cast<size_t>(n));
  for (int i = 0; i < n; ++i) {
    out[i].fd = static_cast<int>(events[i].ident);
    out[i].udata = events[i].udata;
    out[i].events = 0;
    if (events[i].filter == EVFILT_READ) {
      out[i].events |= kRead;
    }
    if (events[i].filter == EVFILT_WRITE) {
      out[i].events |= kWrite;
    }
    if (events[i].flags & EV_EOF) {
      out[i].events |= kRead;
    }
  }
#endif
  return static_cast<int>(out.size());
}

}  // namespace net
}  // namespace dge
