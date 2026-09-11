#pragma once

#include <cstdint>
#include <utility>
#include <vector>

#include "graph/engine_interface.h"

namespace dge {

void GenerateBarabasiAlbert(IGraphEngine& engine, size_t nodes, uint32_t degree,
                            uint64_t seed, std::vector<std::pair<uint32_t, uint32_t>>& edges);

}  // namespace dge
