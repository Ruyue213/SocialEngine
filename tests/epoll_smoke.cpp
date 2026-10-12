// epoll 语义单元测试(仅 Linux 编译)。
// 验证 poller.cpp 修复后的载荷约定:注册写 epoll_data.fd,返回读
// data.fd——与 kqueue 路径语义对齐。覆盖 Add/Mod/Del、边缘触发的
// 读到 EAGAIN、WakeFd 唤醒、fd 关闭后事件不误投。
#if defined(__linux__)

#include <arpa/inet.h>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>

#include "net/poller.h"
#include "net/wake_fd.h"

using namespace dge::net;

static int g_fail = 0;
#define EXPECT(cond)                                              \
  do {                                                            \
    if (!(cond)) {                                                \
      printf("  [FAIL] %s (%s:%d)\n", #cond, __FILE__, __LINE__); \
      ++g_fail;                                                   \
    }                                                             \
  } while (0)

// 1. 载荷一致性:注册的 fd 必须原样出现在返回事件中(修复前写 ptr
//    读 fd,返回的是指针低 32 位)。
void TestPollerFdPayload() {
  printf("[epoll] fd payload roundtrip\n");
  Poller p;
  int fds[2];
  assert(pipe(fds) == 0);
  p.Add(fds[0], kRead, nullptr);
  char c = 'x';
  EXPECT(write(fds[1], &c, 1) == 1);
  std::vector<PollEvent> evs;
  int n = p.Wait(evs, 200);
  EXPECT(n == 1);
  EXPECT(evs[0].fd == fds[0]);  // 修复前:这里返回指针位模式
  EXPECT(evs[0].events & kRead);
  char r = 0;
  EXPECT(read(fds[0], &r, 1) == 1 && r == 'x');
  p.Del(fds[0]);
  close(fds[0]);
  close(fds[1]);
}

// 2. 边缘触发:一次通知后必须读到 EAGAIN;不排空不会再来事件。
void TestPollerEdgeTriggerDrain() {
  printf("[epoll] edge-trigger requires drain to EAGAIN\n");
  Poller p;
  int fds[2];
  assert(pipe(fds) == 0);
  p.Add(fds[0], kRead, nullptr);
  char buf[16];
  memset(buf, 'y', sizeof(buf));
  EXPECT(write(fds[1], buf, 16) == 16);
  std::vector<PollEvent> evs;
  EXPECT(p.Wait(evs, 200) == 1);
  // 排空(reactor 的读循环语义)
  while (read(fds[0], buf, sizeof(buf)) > 0) {
  }
  EXPECT(errno == EAGAIN);
  // 已排空且无新数据:ET 下不再有事件。
  evs.clear();
  EXPECT(p.Wait(evs, 100) == 0);
  // 新数据到达才触发新事件。
  EXPECT(write(fds[1], buf, 1) == 1);
  evs.clear();
  EXPECT(p.Wait(evs, 200) == 1);
  p.Del(fds[0]);
  close(fds[0]);
  close(fds[1]);
}

// 3. Mod 读写切换:注册读,Mod 成读写,再 Mod 回读。
void TestPollerMod() {
  printf("[epoll] mod switches interest set\n");
  Poller p;
  int fds[2];
  assert(pipe(fds) == 0);
  p.Add(fds[0], kRead, nullptr);
  p.Mod(fds[0], kRead | kWrite, nullptr);
  char c = 'z';
  EXPECT(write(fds[1], &c, 1) == 1);
  std::vector<PollEvent> evs;
  EXPECT(p.Wait(evs, 200) == 1);
  EXPECT((evs[0].events & kRead) && (evs[0].events & kWrite));
  p.Mod(fds[0], kRead, nullptr);
  evs.clear();
  EXPECT(p.Wait(evs, 200) == 1);
  EXPECT((evs[0].events & kRead) && !(evs[0].events & kWrite));
  p.Del(fds[0]);
  close(fds[0]);
  close(fds[1]);
}

// 4. WakeFd:跨线程唤醒阻塞中的 Wait。
void TestWakeFd() {
  printf("[epoll] wake fd interrupts blocked wait\n");
  Poller p;
  WakeFd wake;
  p.Add(wake.fd(), kRead, nullptr);
  std::vector<PollEvent> evs;
  std::thread notifier([&]() {
    usleep(50 * 1000);
    wake.Notify();
  });
  auto t0 = std::chrono::steady_clock::now();
  int n = p.Wait(evs, 5000);
  auto dt = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - t0)
                .count();
  EXPECT(n >= 1);
  bool woke = false;
  for (auto& e : evs) {
    if (e.fd == wake.fd()) {
      woke = true;
    }
  }
  EXPECT(woke);
  EXPECT(dt < 2000);
  wake.Drain();
  notifier.join();
}

// 5. fd 关闭与复用:关闭后 Del,新 fd 注册不串扰(连接代际由
//    reactor 层测试覆盖,此处验证 poller 层 fd 身份)。
void TestPollerFdIdentity() {
  printf("[epoll] fd identity across close/reopen\n");
  Poller p;
  int fd1 = ::open("/dev/null", O_RDONLY);
  EXPECT(fd1 >= 0);
  p.Add(fd1, kRead, nullptr);
  close(fd1);
  p.Del(fd1);
  int fd2 = ::open("/dev/null", O_RDONLY);
  EXPECT(fd2 >= 0);
  if (fd2 == fd1) {
    // fd 被复用:重新注册后 poller 不残留旧状态(ET 下 /dev/null
    // 恒可读,应恰好返回一次事件)。
    p.Add(fd2, kRead, nullptr);
    std::vector<PollEvent> evs;
    EXPECT(p.Wait(evs, 200) == 1);
    EXPECT(evs[0].fd == fd2);
    p.Del(fd2);
  }
  close(fd2);
}

int main() {
  setvbuf(stdout, nullptr, _IONBF, 0);
  TestPollerFdPayload();
  TestPollerEdgeTriggerDrain();
  TestPollerMod();
  TestWakeFd();
  TestPollerFdIdentity();
  printf("\nepoll smoke: %s (%d failures)\n", g_fail == 0 ? "PASS" : "FAIL", g_fail);
  return g_fail == 0 ? 0 : 1;
}

#else
#include <cstdio>
int main() {
  printf("epoll smoke: SKIPPED (non-linux platform)\n");
  return 0;
}
#endif
