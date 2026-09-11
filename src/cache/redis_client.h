#pragma once

#include <atomic>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

struct redisContext;
struct redisReply;

namespace dge {

class RedisClient {
 public:
  struct Config {
    std::string host = "127.0.0.1";
    int port = 6379;
    int io_threads = 2;
    int ttl_seconds = 120;
    int double_delete_delay_ms = 5;
    size_t io_queue_cap = 8192;
  };

  using CallbackExecutor = std::function<void(std::function<void()>)>;

  RedisClient(const Config& cfg, CallbackExecutor schedule);
  ~RedisClient();
  RedisClient(const RedisClient&) = delete;
  RedisClient& operator=(const RedisClient&) = delete;

  bool enabled() const { return enabled_.load(std::memory_order_relaxed); }

  void GetAsync(const std::string& key, std::function<void(bool, const std::string&)> cb);

  void EvalReadAsync(const std::vector<std::string>& keys, const std::string& script,
                     std::function<void(bool, const std::string&)> cb);

  void EvalWriteAsync(const std::vector<std::string>& keys, const std::string& script,
                      std::function<void()> cb = nullptr);

  void SetNxAsync(const std::string& key, int ttl_seconds,
                  std::function<void(bool)> cb);

  void SetAsync(const std::string& key, const std::string& value, std::function<void()> cb = nullptr);

  void DelAsync(const std::string& key, std::function<void()> cb = nullptr);

  void IncrAsync(const std::string& key, std::function<void(int64_t)> cb = nullptr);

  void Shutdown();

  uint64_t cache_gets() const { return cache_gets_.load(std::memory_order_relaxed); }
  uint64_t cache_errors() const { return cache_errors_.load(std::memory_order_relaxed); }
  uint64_t dropped_writes() const {
    return dropped_writes_.load(std::memory_order_relaxed);
  }

 private:
  struct Op {
    enum class Type { kGet, kSet, kDel, kIncr, kEvalRead, kEvalWrite, kEvalResult } type;
    std::string key;
    std::vector<std::string> keys;
    std::string value;
    std::string script;
    std::function<void(bool, const std::string&)> get_cb;
    std::function<void()> done_cb;
    std::function<void(int64_t)> incr_cb;
    std::function<void(bool)> result_cb;
  };

  void IoLoop(size_t idx);
  bool EnsureConnected(size_t idx, redisContext*& ctx);
  bool AppendCommand(redisContext* ctx, Op& op, std::string& sha);
  void HandleReply(Op& op, redisReply* r);
  void DropOp(Op& op);
  void EnqueueRead(size_t idx, Op op);
  void EnqueueWrite(size_t idx, Op op);

  Config cfg_;
  CallbackExecutor schedule_;
  std::atomic<bool> enabled_{true};

  std::vector<std::deque<Op>> io_queues_;
  std::vector<uint64_t> io_queue_sizes_;
  std::vector<std::unique_ptr<std::mutex>> io_mus_;
  std::vector<std::unique_ptr<std::condition_variable>> io_cvs_;
  std::atomic<uint64_t> rr_{0};
  std::atomic<uint64_t> dropped_writes_{0};
  bool stop_ = false;
  std::mutex stop_mu_;
  std::vector<std::thread> io_threads_;
  int connect_failures_ = 0;

  std::atomic<uint64_t> cache_gets_{0};
  std::atomic<uint64_t> cache_errors_{0};
};

}  // namespace dge
