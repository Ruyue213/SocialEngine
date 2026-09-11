#pragma once

namespace dge {

unsigned hardware_cores();

void pin_thread_to_core(int core);

void set_thread_low_priority();

}  // namespace dge
