#include "net/http.h"

#include <cstdio>
#include <cstring>

namespace dge {
namespace net {

const char* StatusText(int status) {
  switch (status) {
    case 200:
      return "OK";
    case 400:
      return "Bad Request";
    case 404:
      return "Not Found";
    case 409:
      return "Conflict";
    case 429:
      return "Too Many Requests";
    case 500:
      return "Internal Server Error";
    case 503:
      return "Service Unavailable";
    default:
      return "Unknown";
  }
}

ParseResult ParseRequest(const char* data, size_t len, HttpRequest& out, size_t& consumed) {
  const char* line_end = static_cast<const char*>(std::memchr(data, '\n', len));
  if (line_end == nullptr) {
    if (len > 8192) {
      return ParseResult::kError;
    }
    return ParseResult::kNeedMore;
  }
  std::string line(data, line_end - data);
  if (!line.empty() && line.back() == '\r') {
    line.pop_back();
  }
  size_t sp1 = line.find(' ');
  size_t sp2 = line.find(' ', sp1 + 1);
  if (sp1 == std::string::npos || sp2 == std::string::npos) {
    return ParseResult::kError;
  }
  out.method = line.substr(0, sp1);
  std::string target = line.substr(sp1 + 1, sp2 - sp1 - 1);
  size_t qpos = target.find('?');
  out.path = target.substr(0, qpos);
  if (qpos != std::string::npos) {
    out.query = target.substr(qpos + 1);
  }

  size_t pos = line_end - data + 1;
  out.headers.clear();
  size_t header_bytes = 0;
  while (true) {
    if (pos >= len) {
      return ParseResult::kNeedMore;
    }
    if (pos + 1 < len && data[pos] == '\r' && data[pos + 1] == '\n') {
      pos += 2;
      break;
    }
    if (data[pos] == '\n') {
      pos += 1;
      break;
    }
    const char* hend = static_cast<const char*>(std::memchr(data + pos, '\n', len - pos));
    if (hend == nullptr) {
      return ParseResult::kNeedMore;
    }
    std::string hl(data + pos, hend - data - pos);
    if (!hl.empty() && hl.back() == '\r') {
      hl.pop_back();
    }
    size_t colon = hl.find(':');
    if (colon != std::string::npos) {
      std::string name = hl.substr(0, colon);
      std::string value = hl.substr(colon + 1);
      while (!value.empty() && value.front() == ' ') {
        value.erase(value.begin());
      }
      out.headers[name] = value;
    }
    header_bytes += hend - (data + pos) + 1;
    if (header_bytes > 16384) {
      return ParseResult::kError;
    }
    pos = hend - data + 1;
  }

  size_t content_length = 0;
  auto it = out.headers.find("Content-Length");
  if (it != out.headers.end()) {
    content_length = static_cast<size_t>(std::strtoull(it->second.c_str(), nullptr, 10));
    if (content_length > 1024 * 1024) {
      return ParseResult::kError;
    }
  }
  if (len - pos < content_length) {
    return ParseResult::kNeedMore;
  }
  out.body.assign(data + pos, content_length);
  consumed = pos + content_length;
  return ParseResult::kOk;
}

std::string BuildResponse(int status, const std::string& content_type, const std::string& body,
                          bool keep_alive) {
  std::string resp;
  resp.reserve(body.size() + 128);
  char line[128];
  std::snprintf(line, sizeof(line), "HTTP/1.1 %d %s\r\n", status, StatusText(status));
  resp += line;
  resp += "Server: DynamicGraphEngine/1.0\r\n";
  if (status == 503) {
    resp += "Retry-After: 1\r\n";
  }
  if (status == 429) {
    resp += "Retry-After: 1\r\n";
  }
  if (!content_type.empty()) {
    resp += "Content-Type: " + content_type + "\r\n";
  }
  std::snprintf(line, sizeof(line), "Content-Length: %zu\r\n", body.size());
  resp += line;
  resp += keep_alive ? "Connection: keep-alive\r\n" : "Connection: close\r\n";
  resp += "\r\n";
  resp += body;
  return resp;
}

}  // namespace net
}  // namespace dge
