#pragma once

#include <cstdio>
#include <mutex>
#include <thread>

namespace dge {
namespace log {

enum class Level { kDebug = 0, kInfo = 1, kWarn = 2, kError = 3 };

void SetLevel(Level lvl);

void Write(Level lvl, const char* file, int line, const char* fmt, ...)
    __attribute__((format(printf, 4, 5)));

}  // namespace log
}  // namespace dge

#define DGE_LOG_DEBUG(...) dge::log::Write(dge::log::Level::kDebug, __FILE__, __LINE__, __VA_ARGS__)
#define DGE_LOG_INFO(...) dge::log::Write(dge::log::Level::kInfo, __FILE__, __LINE__, __VA_ARGS__)
#define DGE_LOG_WARN(...) dge::log::Write(dge::log::Level::kWarn, __FILE__, __LINE__, __VA_ARGS__)
#define DGE_LOG_ERROR(...) dge::log::Write(dge::log::Level::kError, __FILE__, __LINE__, __VA_ARGS__)
