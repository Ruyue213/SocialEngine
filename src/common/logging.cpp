#include "common/logging.h"

#include <chrono>
#include <cstdarg>
#include <cstring>
#include <ctime>

namespace dge {
namespace log {

namespace {
Level g_level = Level::kInfo;
std::mutex g_mu;
}  // namespace

void SetLevel(Level lvl) { g_level = lvl; }

void Write(Level lvl, const char* file, int line, const char* fmt, ...) {
  if (lvl < g_level) {
    return;
  }
  const char* names[] = {"DEBUG", "INFO", "WARN", "ERROR"};
  char ts[32];
  auto now = std::chrono::system_clock::now();
  std::time_t t = std::chrono::system_clock::to_time_t(now);
  auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
  std::tm tm_buf;
  localtime_r(&t, &tm_buf);
  std::strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_buf);

  const char* base = std::strrchr(file, '/');
  base = base ? base + 1 : file;

  char buf[1024];
  va_list ap;
  va_start(ap, fmt);
  int n = vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  if (n < 0) {
    return;
  }
  if (static_cast<size_t>(n) >= sizeof(buf)) {
    n = sizeof(buf) - 1;
  }

  std::lock_guard<std::mutex> lk(g_mu);
  fprintf(stderr, "%s.%03lld [%s] [%s:%d] [tid=%llu] %.*s\n", ts,
          static_cast<long long>(ms.count()), names[static_cast<int>(lvl)], base, line,
          static_cast<unsigned long long>(
              std::hash<std::thread::id>()(std::this_thread::get_id())),
          n, buf);
}

}  // namespace log
}  // namespace dge
