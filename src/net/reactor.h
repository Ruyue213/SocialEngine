#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "net/http.h"
#include "net/poller.h"
#include "net/wake_fd.h"

namespace dge {
namespace net {

using RequestHandler = std::function<void(HttpRequest&&, std::function<void(int, std::string)>)>;

class HttpServer {
 public:
  HttpServer(int port, int sub_reactors, RequestHandler handler);
  ~HttpServer();
  HttpServer(const HttpServer&) = delete;
  HttpServer& operator=(const HttpServer&) = delete;

  void Start();
  void Shutdown();

 private:
  struct Conn {
    int fd;
    std::string in;
    std::string out;
    size_t out_off = 0;
    bool close_after = false;
    int64_t last_active_ms = 0;
    uint64_t next_req_seq = 0;
    uint64_t next_resp_seq = 0;
    std::map<uint64_t, std::pair<int, std::string>> resp_map;
  };

  struct SubReactor {
    std::unique_ptr<Poller> poller;
    WakeFd wake;
    std::mutex mu;
    std::vector<std::pair<int, Conn*>> new_conns;
    std::unordered_map<int, std::unique_ptr<Conn>> conns;
    std::vector<std::pair<int, std::pair<uint64_t, std::pair<int, std::string>>>>
        pending_responses;
    std::thread thread;
    std::atomic<bool> stop{false};
  };

  void MainLoop();
  void SubLoop(SubReactor* sr);
  void FlushResponses(SubReactor* sr, Conn* c);
  bool HandleRead(SubReactor* sr, Conn* c);
  bool HandleWrite(SubReactor* sr, Conn* c);
  void CloseConn(SubReactor* sr, Conn* c);
  int64_t NowMs();

  int port_;
  int listen_fd_ = -1;
  int sub_count_;
  RequestHandler handler_;
  std::vector<std::unique_ptr<SubReactor>> subs_;
  std::unique_ptr<Poller> main_poller_;
  std::thread main_thread_;
  std::atomic<bool> stop_{false};
  std::atomic<uint64_t> conn_seq_{0};
};

}  // namespace net
}  // namespace dge
