#pragma once

#include <cstddef>
#include <cstdint>

namespace dge {

inline size_t varint_size(uint64_t v) {
  size_t n = 1;
  while (v >= 0x80) {
    v >>= 7;
    ++n;
  }
  return n;
}

inline uint8_t* put_varint(uint8_t* p, uint64_t v) {
  while (v >= 0x80) {
    *p++ = static_cast<uint8_t>(v | 0x80);
    v >>= 7;
  }
  *p++ = static_cast<uint8_t>(v);
  return p;
}

inline const uint8_t* get_varint(const uint8_t* p, const uint8_t* end, uint64_t& out) {
  out = 0;
  int shift = 0;
  while (p < end && shift < 64) {
    uint8_t b = *p++;
    out |= static_cast<uint64_t>(b & 0x7F) << shift;
    if (!(b & 0x80)) {
      return p;
    }
    shift += 7;
  }
  return nullptr;
}

}  // namespace dge
