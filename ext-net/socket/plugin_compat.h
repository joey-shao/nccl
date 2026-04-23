#ifndef NCCL_DPDK_PLUGIN_COMPAT_H_
#define NCCL_DPDK_PLUGIN_COMPAT_H_

#include "net.h"

#include <errno.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#ifndef MAX_COLLNET_SIZE
#define MAX_COLLNET_SIZE (512 * 1024 * 1024L)
#endif

extern ncclDebugLogger_t ncclDpdkLogFunction;

void ncclDpdkSetLogFunction(ncclDebugLogger_t logFunction);
const char* ncclGetEnv(const char* name);
ncclResult_t getRandomData(void* data, size_t size);

static inline void ncclDpdkLoadParam(const char* envName, int64_t defaultVal,
                                     int64_t uninitialized, int64_t* cache) {
  if (__atomic_load_n(cache, __ATOMIC_RELAXED) != uninitialized) return;

  int64_t value = defaultVal;
  const char* env = ncclGetEnv(envName);
  if (env != NULL && env[0] != '\0') {
    errno = 0;
    char* end = NULL;
    int64_t parsed = strtoll(env, &end, 0);
    if (errno == 0 && end != env && *end == '\0') value = parsed;
  }
  __atomic_store_n(cache, value, __ATOMIC_RELAXED);
}

#define NCCL_DPDK_PARAM(name, env, defaultVal)                                 \
  int ncclParam##name() {                                                      \
    constexpr int64_t uninitialized = INT64_MIN;                               \
    static_assert(defaultVal != uninitialized,                                 \
                  "default value cannot be the uninitialized value.");         \
    static int64_t cache = uninitialized;                                      \
    if (__builtin_expect(                                                      \
            __atomic_load_n(&cache, __ATOMIC_RELAXED) == uninitialized,       \
            false)) {                                                          \
      ncclDpdkLoadParam("NCCL_" env, defaultVal, uninitialized, &cache);      \
    }                                                                          \
    return static_cast<int>(cache);                                            \
  }

template <typename T>
static inline ncclResult_t ncclCalloc(T** ptr, size_t nelems = 1) {
  if (ptr == NULL || nelems == 0) return ncclInvalidArgument;
  *ptr = static_cast<T*>(calloc(nelems, sizeof(T)));
  return (*ptr != NULL) ? ncclSuccess : ncclSystemError;
}

#define NCCLCHECK(cmd)                                                        \
  do {                                                                        \
    ncclResult_t _nccl_result = (cmd);                                        \
    if (_nccl_result != ncclSuccess) return _nccl_result;                     \
  } while (0)

#define NCCLCHECKGOTO(cmd, ret, label)                                        \
  do {                                                                        \
    ret = (cmd);                                                               \
    if (ret != ncclSuccess) goto label;                                        \
  } while (0)

#define WARN(...)                                                              \
  do {                                                                        \
    if (ncclDpdkLogFunction) {                                                 \
      ncclDpdkLogFunction(NCCL_LOG_WARN, NCCL_ALL, __FILE__, __LINE__,        \
                          __VA_ARGS__);                                        \
    } else {                                                                   \
      fprintf(stderr, "WARN %s:%d ", __FILE__, __LINE__);                     \
      fprintf(stderr, __VA_ARGS__);                                            \
      fprintf(stderr, "\n");                                                   \
    }                                                                          \
  } while (0)

#define INFO(flags, ...)                                                       \
  do {                                                                        \
    if (ncclDpdkLogFunction) {                                                 \
      ncclDpdkLogFunction(NCCL_LOG_INFO, (flags), __func__, __LINE__,         \
                          __VA_ARGS__);                                        \
    }                                                                          \
  } while (0)

#define TRACE(flags, ...)                                                      \
  do {                                                                        \
    if (ncclDpdkLogFunction) {                                                 \
      ncclDpdkLogFunction(NCCL_LOG_TRACE, (flags), __func__, __LINE__,        \
                          __VA_ARGS__);                                        \
    }                                                                          \
  } while (0)

#endif // NCCL_DPDK_PLUGIN_COMPAT_H_
