#pragma once

#include <cstddef>
#include <cstdint>

#if defined(__GNUC__) || defined(__clang__)
#define DGE_RESTRICT __restrict__
#else
#define DGE_RESTRICT
#endif

namespace dge {

inline size_t IntersectSorted(const uint32_t* DGE_RESTRICT a, size_t na,
                              const uint32_t* DGE_RESTRICT b, size_t nb,
                              uint32_t* DGE_RESTRICT out) {
  size_t i = 0, j = 0, k = 0;
  while (i < na && j < nb) {
    if (a[i] < b[j]) {
      ++i;
    } else if (b[j] < a[i]) {
      ++j;
    } else {
      out[k++] = a[i];
      ++i;
      ++j;
    }
  }
  return k;
}

}  // namespace dge
