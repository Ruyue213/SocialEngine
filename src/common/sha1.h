#pragma once

#include <cstdint>
#include <string>

namespace dge {

struct Sha1Digest {
  uint32_t h[5];
};

Sha1Digest sha1(const void* data, size_t len);

std::string sha1_hex(const std::string& s);

}  // namespace dge
