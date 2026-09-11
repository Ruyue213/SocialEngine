#pragma once

#include <cstddef>
#include <map>
#include <string>

namespace dge {
namespace net {

struct HttpRequest {
  std::string method;
  std::string path;
  std::string query;
  std::map<std::string, std::string> headers;
  std::string body;
};

enum class ParseResult { kNeedMore, kOk, kError };

ParseResult ParseRequest(const char* data, size_t len, HttpRequest& out, size_t& consumed);

std::string BuildResponse(int status, const std::string& content_type, const std::string& body,
                          bool keep_alive);

const char* StatusText(int status);

}  // namespace net
}  // namespace dge
