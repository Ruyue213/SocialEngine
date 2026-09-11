#include "common/affinity.h"

#include <thread>

#if defined(__linux__)
#include <pthread.h>
#include <sched.h>
#elif defined(__APPLE__)
#include <pthread.h>
#include <mach/mach.h>
#include <mach/thread_act.h>
#include <mach/thread_policy.h>
#endif

namespace dge {

unsigned hardware_cores() {
  unsigned n = std::thread::hardware_concurrency();
  return n ? n : 1;
}

void pin_thread_to_core(int core) {
#if defined(__linux__)
  cpu_set_t cs;
  CPU_ZERO(&cs);
  CPU_SET(core % CPU_SETSIZE, &cs);
  pthread_setaffinity_np(pthread_self(), sizeof(cs), &cs);
#elif defined(__APPLE__)
  thread_affinity_policy_data_t policy = {core + 1};
  thread_policy_set(pthread_mach_thread_np(pthread_self()), THREAD_AFFINITY_POLICY,
                    reinterpret_cast<thread_policy_t>(&policy),
                    THREAD_AFFINITY_POLICY_COUNT);
#endif
}

void set_thread_low_priority() {
#if defined(__linux__)
  struct sched_param sp;
  sp.sched_priority = 0;
  pthread_setschedparam(pthread_self(), SCHED_BATCH, &sp);
#elif defined(__APPLE__)
  pthread_set_qos_class_self_np(QOS_CLASS_BACKGROUND, 0);
#endif
}

}  // namespace dge
