#include "net/reactor.h"

#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include "common/logging.h"

namespace dge {
namespace net {

namespace {

void SetNonBlocking(int fd) {
  int flags = ::fcntl(fd, F_GETFL, 0);
  ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

}  // namespace

HttpServer::HttpServer(int port, int sub_reactors, RequestHandler handler)
    : port_(port), sub_count_(sub_reactors), handler_(std::move(handler)) {
  listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
  if (listen_fd_ < 0) {
    DGE_LOG_ERROR("socket create failed: %s", std::strerror(errno));
    return;
  }
  int reuse = 1;
  ::setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
  struct sockaddr_in addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(static_cast<uint16_t>(port_));
  if (::bind(listen_fd_, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) != 0) {
    DGE_LOG_ERROR("bind :%d failed: %s", port_, std::strerror(errno));
    return;
  }
  if (::listen(listen_fd_, 4096) != 0) {
    DGE_LOG_ERROR("listen failed: %s", std::strerror(errno));
    return;
  }
  SetNonBlocking(listen_fd_);

  for (int i = 0; i < sub_count_; ++i) {
    auto sr = std::make_unique<SubReactor>();
    sr->poller = std::make_unique<Poller>();
    subs_.push_back(std::move(sr));
  }
}

HttpServer::~HttpServer() { Shutdown(); }

int64_t HttpServer::NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

void HttpServer::Start() {
  if (listen_fd_ < 0) {
    return;
  }
  main_poller_ = std::make_unique<Poller>();
  main_poller_->Add(listen_fd_, kRead, nullptr);
  for (auto& sr : subs_) {
    sr->thread = std::thread([this, raw = sr.get()]() { SubLoop(raw); });
  }
  main_thread_ = std::thread([this]() { MainLoop(); });
  DGE_LOG_INFO("http server listening on :%d with %d sub-reactors", port_, sub_count_);
}

void HttpServer::MainLoop() {
  std::vector<PollEvent> events;
  uint64_t rr = 0;
  while (!stop_.load(std::memory_order_relaxed)) {
    events.clear();
    int n = main_poller_->Wait(events, 100);
    if (n <= 0) {
      continue;
    }
    while (true) {
      struct sockaddr_in cli;
      socklen_t cli_len = sizeof(cli);
      int cfd = ::accept(listen_fd_, reinterpret_cast<struct sockaddr*>(&cli), &cli_len);
      if (cfd < 0) {
        break;
      }
      SetNonBlocking(cfd);
      int one = 1;
      ::setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
      auto conn = std::make_unique<Conn>();
      conn->fd = cfd;
      conn->last_active_ms = NowMs();
      SubReactor* sr = subs_[rr++ % subs_.size()].get();
      Conn* raw = conn.get();
      {
        std::lock_guard<std::mutex> lk(sr->mu);
        conn->gen = sr->next_gen++;
        sr->new_conns.emplace_back(cfd, raw);
      }
      sr->wake.Notify();
      conn.release();
    }
  }
  ::close(listen_fd_);
}

void HttpServer::CompactSentPrefix(Conn* c) {
  // 修复:部分写(EAGAIN)后 out_off > 0 时,新响应不能直接
  // out += resp 并把 out_off 归零——那会从已发送前缀开始重发,
  // 对端收到字节流重复,HTTP 响应损坏。先把已发送前缀裁掉。
  if (c->out_off > 0) {
    c->out.erase(0, c->out_off);
    c->out_off = 0;
  }
}

void HttpServer::FlushResponses(SubReactor* sr, Conn* c) {
  while (true) {
    auto it = c->resp_map.find(c->next_resp_seq);
    if (it == c->resp_map.end()) {
      break;
    }
    // 503 不再强制断连:Retry-After 语义是「稍后重试」,连接应
    // 保留复用。原实现过载即断连,客户端重连风暴放大压力,且
    // 对端若未忽略 SIGPIPE 会被直接杀死(压测中复现 exit=141)。
    CompactSentPrefix(c);
    c->out += it->second.second;
    c->resp_map.erase(it);
    ++c->next_resp_seq;
  }
  if (!c->out.empty() || c->close_after) {
    DGE_LOG_DEBUG("flush: fd=%d out=%zu close_after=%d", c->fd, c->out.size(),
                  c->close_after);
    sr->poller->Mod(c->fd, kRead | kWrite, c);
  }
}

void HttpServer::SubLoop(SubReactor* sr) {
  sr->poller->Add(sr->wake.fd(), kRead, nullptr);
  std::vector<PollEvent> events;
  int64_t last_scan = NowMs();
  while (!sr->stop.load(std::memory_order_relaxed)) {
    events.clear();
    sr->poller->Wait(events, 100);
    for (const auto& ev : events) {
      if (ev.fd == sr->wake.fd()) {
        DGE_LOG_DEBUG("sub wake: pending=%zu conns=%zu",
                      sr->pending_responses.size(), sr->conns.size());
        sr->wake.Drain();
        std::vector<std::pair<int, Conn*>> incoming;
        std::vector<PendingResp> pend;
        {
          std::lock_guard<std::mutex> lk(sr->mu);
          incoming.swap(sr->new_conns);
          for (auto& pr : sr->pending_responses) {
            auto it = sr->conns.find(pr.fd);
            // 修复:fd+代际双重校验。原实现只按 fd 查表,连接关闭后
            // fd 被新连接复用时,迟到响应会投递到新连接,把 A 请求的
            // 响应发给 B 连接。代际不匹配的响应直接丢弃。
            if (it != sr->conns.end() && it->second->gen == pr.gen) {
              it->second->resp_map[pr.req_seq] = {pr.status, std::move(pr.resp)};
            }
          }
          sr->pending_responses.clear();
        }
        for (auto it = sr->conns.begin(); it != sr->conns.end(); ++it) {
          FlushResponses(sr, it->second.get());
        }
        for (auto& pr : incoming) {
          if (sr->conns.find(pr.first) != sr->conns.end()) {
            ::close(pr.first);
            continue;
          }
          auto c = std::unique_ptr<Conn>(pr.second);
          sr->poller->Add(pr.first, kRead, c.get());
          sr->conns.emplace(pr.first, std::move(c));
        }
        continue;
      }
      auto it = sr->conns.find(ev.fd);
      if (it == sr->conns.end()) {
        continue;
      }
      Conn* c = it->second.get();
      if (ev.events & kRead) {
        if (!HandleRead(sr, c)) {
          it = sr->conns.erase(it);
          continue;
        }
      }
      if (ev.events & kWrite) {
        if (!HandleWrite(sr, c)) {
          it = sr->conns.erase(it);
          continue;
        }
      }
    }
    int64_t now = NowMs();
    if (now - last_scan > 1000) {
      last_scan = now;
      for (auto it = sr->conns.begin(); it != sr->conns.end();) {
        if (now - it->second->last_active_ms > 60000) {
          CloseConn(sr, it->second.get());
          it = sr->conns.erase(it);
        } else {
          ++it;
        }
      }
    }
  }
  for (auto it = sr->conns.begin(); it != sr->conns.end();) {
    CloseConn(sr, it->second.get());
    it = sr->conns.erase(it);
  }
}

bool HttpServer::HandleRead(SubReactor* sr, Conn* c) {
  char buf[8192];
  while (true) {
    ssize_t n = ::read(c->fd, buf, sizeof(buf));
    if (n > 0) {
      c->in.append(buf, static_cast<size_t>(n));
      if (c->in.size() > 1024 * 1024) {
        c->close_after = true;
        c->resp_map.clear();
        c->resp_map[c->next_resp_seq] = {400, BuildResponse(400, "text/plain", "too large", false)};
        FlushResponses(sr, c);
        return true;
      }
      continue;
    }
    if (n == 0) {
      CloseConn(sr, c);
      return false;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      break;
    }
    CloseConn(sr, c);
    return false;
  }
  while (true) {
    HttpRequest req;
    size_t consumed = 0;
    ParseResult r = ParseRequest(c->in.data(), c->in.size(), req, consumed);
    if (r == ParseResult::kNeedMore) {
      break;
    }
    if (r == ParseResult::kError) {
      c->close_after = true;
      c->resp_map.clear();
      c->resp_map[c->next_resp_seq] = {400, BuildResponse(400, "text/plain", "bad request", false)};
      FlushResponses(sr, c);
      return true;
    }
    bool keep_alive = req.headers["Connection"] != "close";
    c->in.erase(0, consumed);
    c->last_active_ms = NowMs();
    int conn_fd = c->fd;
    uint64_t conn_gen = c->gen;
    uint64_t req_seq = c->next_req_seq++;
    handler_(std::move(req),
             [this, sr, conn_fd, conn_gen, req_seq, keep_alive](int status, std::string body) {
      std::string resp = BuildResponse(status, "application/json", body, keep_alive);
      if (sr->thread.get_id() == std::this_thread::get_id()) {
        auto it = sr->conns.find(conn_fd);
        if (it != sr->conns.end() && it->second->gen == conn_gen) {
          Conn* cc = it->second.get();
          cc->resp_map[req_seq] = {status, std::move(resp)};
          FlushResponses(sr, cc);
        }
        return;
      }
      {
        std::lock_guard<std::mutex> lk(sr->mu);
        sr->pending_responses.push_back(
            {conn_fd, conn_gen, req_seq, status, std::move(resp)});
      }
      sr->wake.Notify();
    });
    if (c->close_after) {
      break;
    }
  }
  return true;
}

bool HttpServer::HandleWrite(SubReactor* sr, Conn* c) {
  DGE_LOG_DEBUG("write event: fd=%d out=%zu off=%zu", c->fd, c->out.size(), c->out_off);
  while (c->out_off < c->out.size()) {
    ssize_t n = ::write(c->fd, c->out.data() + c->out_off, c->out.size() - c->out_off);
    if (n > 0) {
      c->out_off += static_cast<size_t>(n);
      continue;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK) {
      return true;
    }
    CloseConn(sr, c);
    return false;
  }
  c->out.clear();
  c->out_off = 0;
  if (c->close_after) {
    CloseConn(sr, c);
    return false;
  }
  sr->poller->Mod(c->fd, kRead, c);
  return true;
}

void HttpServer::CloseConn(SubReactor* sr, Conn* c) {
  sr->poller->Del(c->fd);
  ::close(c->fd);
}

void HttpServer::Shutdown() {
  bool expected = false;
  if (!stop_.compare_exchange_strong(expected, true)) {
    return;
  }
  if (main_thread_.joinable()) {
    main_thread_.join();
  }
  for (auto& sr : subs_) {
    sr->stop.store(true, std::memory_order_relaxed);
    sr->wake.Notify();
  }
  for (auto& sr : subs_) {
    if (sr->thread.joinable()) {
      sr->thread.join();
    }
  }
  DGE_LOG_INFO("http server stopped");
}

}  // namespace net
}  // namespace dge
