#include "common/sha1.h"

#include <cstdio>
#include <cstring>

namespace dge {

namespace {

inline uint32_t rotl32(uint32_t x, int n) { return (x << n) | (x >> (32 - n)); }

}  // namespace

Sha1Digest sha1(const void* data, size_t len) {
  const uint8_t* msg = static_cast<const uint8_t*>(data);
  uint64_t bit_len = static_cast<uint64_t>(len) * 8;
  size_t padded_len = len + 1;
  while (padded_len % 64 != 56) {
    ++padded_len;
  }
  padded_len += 8;
  uint8_t* buf = new uint8_t[padded_len];
  std::memcpy(buf, msg, len);
  buf[len] = 0x80;
  for (size_t i = len + 1; i < padded_len - 8; ++i) {
    buf[i] = 0;
  }
  for (int i = 0; i < 8; ++i) {
    buf[padded_len - 1 - i] = static_cast<uint8_t>((bit_len >> (i * 8)) & 0xFF);
  }

  uint32_t h0 = 0x67452301;
  uint32_t h1 = 0xEFCDAB89;
  uint32_t h2 = 0x98BADCFE;
  uint32_t h3 = 0x10325476;
  uint32_t h4 = 0xC3D2E1F0;

  for (size_t off = 0; off < padded_len; off += 64) {
    uint32_t w[80];
    for (int i = 0; i < 16; ++i) {
      w[i] = (static_cast<uint32_t>(buf[off + i * 4]) << 24) |
             (static_cast<uint32_t>(buf[off + i * 4 + 1]) << 16) |
             (static_cast<uint32_t>(buf[off + i * 4 + 2]) << 8) |
             static_cast<uint32_t>(buf[off + i * 4 + 3]);
    }
    for (int i = 16; i < 80; ++i) {
      w[i] = rotl32(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
    }
    uint32_t a = h0, b = h1, c = h2, d = h3, e = h4;
    for (int i = 0; i < 80; ++i) {
      uint32_t f, k;
      if (i < 20) {
        f = (b & c) | ((~b) & d);
        k = 0x5A827999;
      } else if (i < 40) {
        f = b ^ c ^ d;
        k = 0x6ED9EBA1;
      } else if (i < 60) {
        f = (b & c) | (b & d) | (c & d);
        k = 0x8F1BBCDC;
      } else {
        f = b ^ c ^ d;
        k = 0xCA62C1D6;
      }
      uint32_t tmp = rotl32(a, 5) + f + e + k + w[i];
      e = d;
      d = c;
      c = rotl32(b, 30);
      b = a;
      a = tmp;
    }
    h0 += a;
    h1 += b;
    h2 += c;
    h3 += d;
    h4 += e;
  }
  delete[] buf;
  return {h0, h1, h2, h3, h4};
}

std::string sha1_hex(const std::string& s) {
  Sha1Digest d = sha1(s.data(), s.size());
  char out[48];
  std::snprintf(out, sizeof(out), "%08x%08x%08x%08x%08x", d.h[0], d.h[1], d.h[2], d.h[3],
                d.h[4]);
  return std::string(out);
}

}  // namespace dge
