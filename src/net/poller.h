#pragma once

#include <cstdint>
#include <vector>

namespace dge {
namespace net {

enum Event : uint32_t { kNone = 0, kRead = 1, kWrite = 2 };

struct PollEvent {
  int fd;
  uint32_t events;
  void* udata;
};

class Poller {
 public:
  Poller();
  ~Poller();
  Poller(const Poller&) = delete;
  Poller& operator=(const Poller&) = delete;

  void Add(int fd, uint32_t events, void* udata);
  void Mod(int fd, uint32_t events, void* udata);
  void Del(int fd);

  int Wait(std::vector<PollEvent>& out, int timeout_ms);

 private:
  int fd_ = -1;
};

}  // namespace net
}  // namespace dge
