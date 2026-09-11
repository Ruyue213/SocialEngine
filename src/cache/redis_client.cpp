#include "cache/redis_client.h"

#include "common/sha1.h"

#include <chrono>
#include <cstring>

extern "C" {
#include "hiredis.h"
}

namespace dge {

namespace {

constexpr int kConnectTimeoutMs = 500;
constexpr int kCommandTimeoutMs = 500;

}  // namespace

RedisClient::RedisClient(const Config& cfg, CallbackExecutor schedule)
    : cfg_(cfg), schedule_(std::move(schedule)) {
  if (cfg_.io_threads == 0) {
    enabled_.store(false, std::memory_order_relaxed);
    return;
  }
  io_queues_.resize(cfg_.io_threads);
  io_queue_sizes_.assign(cfg_.io_threads, 0);
  io_mus_.reserve(cfg_.io_threads);
  io_cvs_.reserve(cfg_.io_threads);
  for (int i = 0; i < cfg_.io_threads; ++i) {
    io_mus_.push_back(std::make_unique<std::mutex>());
    io_cvs_.push_back(std::make_unique<std::condition_variable>());
  }
  io_threads_.reserve(cfg_.io_threads);
  for (int i = 0; i < cfg_.io_threads; ++i) {
    io_threads_.emplace_back([this, i]() { IoLoop(i); });
  }
}

RedisClient::~RedisClient() { Shutdown(); }

void RedisClient::Shutdown() {
  {
    std::lock_guard<std::mutex> lk(stop_mu_);
    if (stop_) {
      return;
    }
    stop_ = true;
  }
  for (auto& cv : io_cvs_) {
    cv->notify_all();
  }
  for (auto& t : io_threads_) {
    if (t.joinable()) {
      t.join();
    }
  }
  io_threads_.clear();
}

bool RedisClient::EnsureConnected(size_t, redisContext*& ctx) {
  if (ctx != nullptr) {
    return true;
  }
  struct timeval tv;
  tv.tv_sec = kConnectTimeoutMs / 1000;
  tv.tv_usec = (kConnectTimeoutMs % 1000) * 1000;
  ctx = redisConnectWithTimeout(cfg_.host.c_str(), cfg_.port, tv);
  if (ctx == nullptr || ctx->err) {
    if (ctx != nullptr) {
      redisFree(ctx);
      ctx = nullptr;
    }
    return false;
  }
  redisSetTimeout(ctx, {kCommandTimeoutMs / 1000, (kCommandTimeoutMs % 1000) * 1000});
  redisReply* r = static_cast<redisReply*>(
      redisCommand(ctx, "CONFIG SET maxmemory-policy allkeys-lru"));
  if (r != nullptr) {
    freeReplyObject(r);
  }
  return true;
}

bool RedisClient::AppendCommand(redisContext* ctx, Op& op, std::string& sha) {
  const char* k0 = op.keys.size() > 0 ? op.keys[0].data() : "";
  size_t s0 = op.keys.size() > 0 ? op.keys[0].size() : 0;
  const char* k1 = op.keys.size() > 1 ? op.keys[1].data() : "";
  size_t s1 = op.keys.size() > 1 ? op.keys[1].size() : 0;
  const char* k2 = op.keys.size() > 2 ? op.keys[2].data() : "";
  size_t s2 = op.keys.size() > 2 ? op.keys[2].size() : 0;
  const char* k3 = op.keys.size() > 3 ? op.keys[3].data() : "";
  size_t s3 = op.keys.size() > 3 ? op.keys[3].size() : 0;
  const char* k4 = op.keys.size() > 4 ? op.keys[4].data() : "";
  size_t s4 = op.keys.size() > 4 ? op.keys[4].size() : 0;
  const char* k5 = op.keys.size() > 5 ? op.keys[5].data() : "";
  size_t s5 = op.keys.size() > 5 ? op.keys[5].size() : 0;
  switch (op.type) {
    case Op::Type::kGet:
      redisAppendCommand(ctx, "GET %s", op.key.c_str());
      return true;
    case Op::Type::kSet:
      redisAppendCommand(ctx, "SET %s %b EX %d", op.key.c_str(), op.value.data(),
                         op.value.size(), cfg_.ttl_seconds);
      return true;
    case Op::Type::kDel:
      redisAppendCommand(ctx, "DEL %s", op.key.c_str());
      return true;
    case Op::Type::kIncr:
      redisAppendCommand(ctx, "INCR %s", op.key.c_str());
      return true;
    case Op::Type::kEvalRead:
    case Op::Type::kEvalWrite: {
      sha = sha1_hex(op.script);
      redisAppendCommand(ctx, "EVALSHA %s %d %b %b %b %b %b %b", sha.c_str(),
                         static_cast<int>(op.keys.size()), k0, s0, k1, s1, k2, s2, k3, s3,
                         k4, s4, k5, s5);
      return true;
    }
    case Op::Type::kEvalResult: {
      sha = sha1_hex(op.script);
      redisAppendCommand(ctx, "EVALSHA %s %d %b %b", sha.c_str(),
                         static_cast<int>(op.keys.size()), k0, s0, op.value.data(),
                         op.value.size());
      return true;
    }
  }
  return true;
}

void RedisClient::HandleReply(Op& op, redisReply* r) {
  if (r == nullptr) {
    cache_errors_.fetch_add(1, std::memory_order_relaxed);
    DropOp(op);
    return;
  }
  switch (op.type) {
    case Op::Type::kGet: {
      bool hit = r->type == REDIS_REPLY_STRING;
      std::string value;
      if (hit) {
        value.assign(r->str, r->len);
      }
      if (op.get_cb) {
        schedule_([cb = std::move(op.get_cb), hit, value]() mutable { cb(hit, value); });
      }
      break;
    }
    case Op::Type::kEvalRead: {
      bool hit = r->type == REDIS_REPLY_STRING;
      std::string value;
      if (hit) {
        value.assign(r->str, r->len);
      }
      if (op.get_cb) {
        schedule_([cb = std::move(op.get_cb), hit, value]() mutable { cb(hit, value); });
      }
      break;
    }
    case Op::Type::kIncr: {
      if (op.incr_cb) {
        int64_t v = r->type == REDIS_REPLY_INTEGER ? r->integer : -1;
        schedule_([cb = std::move(op.incr_cb), v]() { cb(v); });
      }
      break;
    }
    case Op::Type::kEvalResult: {
      bool acquired = r->type == REDIS_REPLY_STATUS || r->type == REDIS_REPLY_STRING;
      if (op.result_cb) {
        schedule_([cb = std::move(op.result_cb), acquired]() { cb(acquired); });
      }
      break;
    }
    case Op::Type::kSet:
    case Op::Type::kDel:
    case Op::Type::kEvalWrite: {
      if (op.done_cb) {
        schedule_(std::move(op.done_cb));
      }
      break;
    }
  }
  freeReplyObject(r);
}

void RedisClient::DropOp(Op& op) {
  switch (op.type) {
    case Op::Type::kEvalRead:
      if (op.get_cb) {
        schedule_([cb = std::move(op.get_cb)]() mutable { cb(false, ""); });
      }
      break;
    case Op::Type::kEvalResult:
      if (op.result_cb) {
        schedule_([cb = std::move(op.result_cb)]() { cb(false); });
      }
      break;
    case Op::Type::kGet:
      if (op.get_cb) {
        schedule_([cb = std::move(op.get_cb)]() mutable { cb(false, ""); });
      }
      break;
    default:
      break;
  }
}

void RedisClient::IoLoop(size_t idx) {
  redisContext* ctx = nullptr;
  auto next_retry = std::chrono::steady_clock::now();
  std::deque<Op> batch;
  while (true) {
    {
      std::unique_lock<std::mutex> lk(*io_mus_[idx]);
      io_cvs_[idx]->wait_for(lk, std::chrono::milliseconds(1), [this, idx] {
        return stop_ || !io_queues_[idx].empty();
      });
      {
        std::lock_guard<std::mutex> slk(stop_mu_);
        if (stop_ && io_queues_[idx].empty()) {
          if (ctx != nullptr) {
            redisFree(ctx);
          }
          return;
        }
      }
      size_t take = std::min<size_t>(io_queues_[idx].size(), 128);
      for (size_t i = 0; i < take; ++i) {
        batch.push_back(std::move(io_queues_[idx].front()));
        io_queues_[idx].pop_front();
      }
      if (take > 0) {
        io_queue_sizes_[idx] -= take;
      }
    }
    if (batch.empty()) {
      continue;
    }
    if (!enabled_.load(std::memory_order_relaxed)) {
      while (!batch.empty()) {
        DropOp(batch.front());
        batch.pop_front();
      }
      continue;
    }
    if (ctx == nullptr && std::chrono::steady_clock::now() < next_retry) {
      while (!batch.empty()) {
        DropOp(batch.front());
        batch.pop_front();
      }
      continue;
    }
    if (!EnsureConnected(idx, ctx)) {
      next_retry = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
      if (++connect_failures_ > 10) {
        enabled_.store(false, std::memory_order_relaxed);
      }
      while (!batch.empty()) {
        DropOp(batch.front());
        batch.pop_front();
      }
      continue;
    }
    connect_failures_ = 0;
    bool conn_lost = false;
    std::string sha;
    for (auto& op : batch) {
      sha.clear();
      if (!AppendCommand(ctx, op, sha)) {
        conn_lost = true;
        break;
      }
    }
    if (conn_lost) {
      redisFree(ctx);
      ctx = nullptr;
      next_retry = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
      while (!batch.empty()) {
        DropOp(batch.front());
        batch.pop_front();
      }
      continue;
    }
    bool fail = false;
    std::vector<size_t> noscript;
    for (size_t i = 0; i < batch.size(); ++i) {
      redisReply* r = nullptr;
      if (redisGetReply(ctx, reinterpret_cast<void**>(&r)) != REDIS_OK) {
        redisFree(ctx);
        ctx = nullptr;
        next_retry = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
        cache_errors_.fetch_add(1, std::memory_order_relaxed);
        DropOp(batch[i]);
        for (size_t j = i + 1; j < batch.size(); ++j) {
          DropOp(batch[j]);
        }
        batch.clear();
        fail = true;
        break;
      }
      if ((batch[i].type == Op::Type::kEvalRead || batch[i].type == Op::Type::kEvalWrite ||
           batch[i].type == Op::Type::kEvalResult) &&
          r != nullptr && r->type == REDIS_REPLY_ERROR &&
          strstr(r->str, "NOSCRIPT") != nullptr) {
        freeReplyObject(r);
        noscript.push_back(i);
        continue;
      }
      HandleReply(batch[i], r);
    }
    for (size_t i : noscript) {
      redisReply* r = nullptr;
      if (batch[i].type == Op::Type::kEvalResult) {
        r = static_cast<redisReply*>(redisCommand(
            ctx, "EVAL %s %d %b %b", batch[i].script.c_str(),
            static_cast<int>(batch[i].keys.size()),
            batch[i].keys.size() > 0 ? batch[i].keys[0].data() : "",
            batch[i].keys.size() > 0 ? batch[i].keys[0].size() : 0, batch[i].value.data(),
            batch[i].value.size()));
      } else {
        r = static_cast<redisReply*>(redisCommand(
            ctx, "EVAL %s %d %b %b %b %b %b %b", batch[i].script.c_str(),
            static_cast<int>(batch[i].keys.size()),
            batch[i].keys.size() > 0 ? batch[i].keys[0].data() : "", batch[i].keys.size() > 0 ? batch[i].keys[0].size() : 0,
            batch[i].keys.size() > 1 ? batch[i].keys[1].data() : "", batch[i].keys.size() > 1 ? batch[i].keys[1].size() : 0,
            batch[i].keys.size() > 2 ? batch[i].keys[2].data() : "", batch[i].keys.size() > 2 ? batch[i].keys[2].size() : 0,
            batch[i].keys.size() > 3 ? batch[i].keys[3].data() : "", batch[i].keys.size() > 3 ? batch[i].keys[3].size() : 0,
            batch[i].keys.size() > 4 ? batch[i].keys[4].data() : "", batch[i].keys.size() > 4 ? batch[i].keys[4].size() : 0,
            batch[i].keys.size() > 5 ? batch[i].keys[5].data() : "", batch[i].keys.size() > 5 ? batch[i].keys[5].size() : 0));
      }
      if (r == nullptr) {
        redisFree(ctx);
        ctx = nullptr;
        next_retry = std::chrono::steady_clock::now() + std::chrono::milliseconds(200);
        cache_errors_.fetch_add(1, std::memory_order_relaxed);
        DropOp(batch[i]);
        fail = true;
        break;
      }
      HandleReply(batch[i], r);
    }
    if (fail) {
      for (size_t i : noscript) {
        DropOp(batch[i]);
      }
      batch.clear();
      continue;
    }
    batch.clear();
  }
}

void RedisClient::EnqueueRead(size_t idx, Op op) {
  {
    std::lock_guard<std::mutex> lk(*io_mus_[idx]);
    if (io_queue_sizes_[idx] >= cfg_.io_queue_cap) {
      dropped_writes_.fetch_add(1, std::memory_order_relaxed);
      if (op.get_cb) {
        schedule_([cb = std::move(op.get_cb)]() mutable { cb(false, ""); });
      }
      return;
    }
    io_queues_[idx].push_front(std::move(op));
    ++io_queue_sizes_[idx];
  }
  io_cvs_[idx]->notify_one();
}

void RedisClient::EnqueueWrite(size_t idx, Op op) {
  {
    std::lock_guard<std::mutex> lk(*io_mus_[idx]);
    if (io_queue_sizes_[idx] >= cfg_.io_queue_cap) {
      dropped_writes_.fetch_add(1, std::memory_order_relaxed);
      if (op.done_cb) {
        schedule_(std::move(op.done_cb));
      }
      return;
    }
    io_queues_[idx].push_back(std::move(op));
    ++io_queue_sizes_[idx];
  }
  io_cvs_[idx]->notify_one();
}

void RedisClient::GetAsync(const std::string& key,
                           std::function<void(bool, const std::string&)> cb) {
  cache_gets_.fetch_add(1, std::memory_order_relaxed);
  if (!enabled_.load(std::memory_order_relaxed) || stop_) {
    if (cb) {
      schedule_([cb = std::move(cb)]() mutable { cb(false, ""); });
    }
    return;
  }
  Op op;
  op.type = Op::Type::kGet;
  op.key = key;
  op.get_cb = std::move(cb);
  size_t idx = rr_.fetch_add(1, std::memory_order_relaxed) % io_queues_.size();
  EnqueueRead(idx, std::move(op));
}

void RedisClient::EvalReadAsync(const std::vector<std::string>& keys,
                                const std::string& script,
                                std::function<void(bool, const std::string&)> cb) {
  cache_gets_.fetch_add(1, std::memory_order_relaxed);
  if (!enabled_.load(std::memory_order_relaxed) || stop_) {
    if (cb) {
      schedule_([cb = std::move(cb)]() mutable { cb(false, ""); });
    }
    return;
  }
  Op op;
  op.type = Op::Type::kEvalRead;
  op.keys = keys;
  op.script = script;
  op.get_cb = std::move(cb);
  size_t idx = rr_.fetch_add(1, std::memory_order_relaxed) % io_queues_.size();
  EnqueueRead(idx, std::move(op));
}

void RedisClient::EvalWriteAsync(const std::vector<std::string>& keys,
                                 const std::string& script,
                                 std::function<void()> cb) {
  if (!enabled_.load(std::memory_order_relaxed) || stop_) {
    if (cb) {
      schedule_(std::move(cb));
    }
    return;
  }
  Op op;
  op.type = Op::Type::kEvalWrite;
  op.keys = keys;
  op.script = script;
  op.done_cb = std::move(cb);
  size_t idx = rr_.fetch_add(1, std::memory_order_relaxed) % io_queues_.size();
  EnqueueWrite(idx, std::move(op));
}

void RedisClient::SetNxAsync(const std::string& key, int ttl_seconds,
                            std::function<void(bool)> cb) {
  if (!enabled_.load(std::memory_order_relaxed) || stop_) {
    if (cb) {
      schedule_([cb = std::move(cb)]() { cb(true); });
    }
    return;
  }
  Op op;
  op.type = Op::Type::kEvalResult;
  op.keys = {key};
  op.script =
      "if redis.call('SET', KEYS[1], '1', 'NX', 'EX', ARGV[1]) then return 'OK' else return "
      "nil end";
  op.value = std::to_string(ttl_seconds);
  op.result_cb = std::move(cb);
  size_t idx = rr_.fetch_add(1, std::memory_order_relaxed) % io_queues_.size();
  EnqueueRead(idx, std::move(op));
}

void RedisClient::SetAsync(const std::string& key, const std::string& value,
                           std::function<void()> cb) {
  if (!enabled_.load(std::memory_order_relaxed) || stop_) {
    if (cb) {
      schedule_(std::move(cb));
    }
    return;
  }
  Op op;
  op.type = Op::Type::kSet;
  op.key = key;
  op.value = value;
  op.done_cb = std::move(cb);
  size_t idx = rr_.fetch_add(1, std::memory_order_relaxed) % io_queues_.size();
  EnqueueWrite(idx, std::move(op));
}

void RedisClient::DelAsync(const std::string& key, std::function<void()> cb) {
  if (!enabled_.load(std::memory_order_relaxed) || stop_) {
    if (cb) {
      schedule_(std::move(cb));
    }
    return;
  }
  Op op;
  op.type = Op::Type::kDel;
  op.key = key;
  op.done_cb = std::move(cb);
  size_t idx = rr_.fetch_add(1, std::memory_order_relaxed) % io_queues_.size();
  EnqueueWrite(idx, std::move(op));
}

void RedisClient::IncrAsync(const std::string& key, std::function<void(int64_t)> cb) {
  if (!enabled_.load(std::memory_order_relaxed) || stop_) {
    if (cb) {
      schedule_([cb = std::move(cb)]() { cb(-1); });
    }
    return;
  }
  Op op;
  op.type = Op::Type::kIncr;
  op.key = key;
  op.incr_cb = std::move(cb);
  size_t idx = rr_.fetch_add(1, std::memory_order_relaxed) % io_queues_.size();
  EnqueueWrite(idx, std::move(op));
}

}  // namespace dge
