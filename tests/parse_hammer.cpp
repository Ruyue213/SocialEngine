#include <chrono>
#include <cstdint>
#include <cstdio>
#include <random>
#include <string>
#include <vector>

#include "common/varint.h"
#include "net/http.h"

namespace {

uint64_t NowMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

}  // namespace

int main() {
  std::mt19937_64 rng(12345);
  std::vector<uint8_t> seed = {'P', 'O', 'S', 'T', ' ', '/', 'q', 'u', 'e', 'r', 'y', ' ',
                               'H', 'T', 'T', 'P', '/', '1', '.', '1', '\r', '\n'};
  uint64_t iters = 0;
  uint64_t t0 = NowMs();
  while (NowMs() - t0 < 30000) {
    std::vector<uint8_t> data = seed;
    int mutations = 1 + rng() % 40;
    for (int m = 0; m < mutations; ++m) {
      switch (rng() % 4) {
        case 0:
          if (!data.empty()) {
            data[rng() % data.size()] = static_cast<uint8_t>(rng());
          }
          break;
        case 1:
          if (!data.empty()) {
            data.erase(data.begin() + rng() % data.size());
          }
          break;
        case 2:
          if (data.size() < 4096) {
            data.insert(data.begin() + rng() % (data.size() + 1),
                        static_cast<uint8_t>(rng()));
          }
          break;
        case 3:
          if (data.size() < 4096) {
            data.insert(data.begin() + rng() % (data.size() + 1), 64, 'A');
          }
          break;
      }
    }
    dge::net::HttpRequest req;
    size_t consumed = 0;
    dge::net::ParseRequest(reinterpret_cast<const char*>(data.data()), data.size(), req,
                           consumed);
    uint64_t v = 0;
    dge::get_varint(data.data(), data.data() + data.size(), v);
    uint8_t buf[16];
    uint8_t* end = dge::put_varint(buf, v);
    uint64_t back = 0;
    dge::get_varint(buf, end, back);
    ++iters;
  }
  printf("parse hammer: %llu iterations in %llums, no crash\n",
         static_cast<unsigned long long>(iters),
         static_cast<unsigned long long>(NowMs() - t0));
  return 0;
}
