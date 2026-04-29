#ifndef NCCL_NET_PLUGIN_TEST_LOG_H_
#define NCCL_NET_PLUGIN_TEST_LOG_H_

#include <algorithm>
#include <cctype>
#include <cstdarg>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

#include "net.h"

static inline const char* pluginTestLogLevelName(ncclDebugLogLevel level) {
  switch (level) {
    case NCCL_LOG_VERSION: return "VERSION";
    case NCCL_LOG_WARN: return "WARN";
    case NCCL_LOG_INFO: return "INFO";
    case NCCL_LOG_ABORT: return "ABORT";
    case NCCL_LOG_TRACE: return "TRACE";
    default: return "NONE";
  }
}

static inline std::string pluginTestUpper(const char* s) {
  std::string out = s ? s : "";
  std::transform(out.begin(), out.end(), out.begin(),
                 [](unsigned char c) { return (char)std::toupper(c); });
  return out;
}

static inline ncclDebugLogLevel pluginTestDebugLevel() {
  const char* env = std::getenv("NCCL_DEBUG");
  std::string level = pluginTestUpper(env);
  if (level == "TRACE") return NCCL_LOG_TRACE;
  if (level == "INFO") return NCCL_LOG_INFO;
  if (level == "WARN") return NCCL_LOG_WARN;
  if (level == "VERSION") return NCCL_LOG_VERSION;
  if (level == "NONE") return NCCL_LOG_NONE;
  return NCCL_LOG_WARN;
}

static inline unsigned long pluginTestSubsysBit(const std::string& token) {
  if (token == "INIT") return NCCL_INIT;
  if (token == "COLL") return NCCL_COLL;
  if (token == "P2P") return NCCL_P2P;
  if (token == "SHM") return NCCL_SHM;
  if (token == "NET") return NCCL_NET;
  if (token == "GRAPH") return NCCL_GRAPH;
  if (token == "TUNING") return NCCL_TUNING;
  if (token == "ENV") return NCCL_ENV;
  if (token == "ALLOC") return NCCL_ALLOC;
  if (token == "CALL") return NCCL_CALL;
  if (token == "PROXY") return NCCL_PROXY;
  if (token == "NVLS") return NCCL_NVLS;
  if (token == "BOOTSTRAP") return NCCL_BOOTSTRAP;
  if (token == "REG") return NCCL_REG;
  if (token == "ALL") return NCCL_ALL;
  return 0;
}

static inline unsigned long pluginTestDebugSubsys() {
  const char* env = std::getenv("NCCL_DEBUG_SUBSYS");
  if (env == nullptr || env[0] == '\0') return NCCL_ALL;

  std::string subsys = pluginTestUpper(env);
  unsigned long mask = 0;
  size_t pos = 0;
  while (pos <= subsys.size()) {
    size_t comma = subsys.find(',', pos);
    std::string token = subsys.substr(pos, comma == std::string::npos
                                           ? std::string::npos
                                           : comma - pos);
    size_t begin = token.find_first_not_of(" \t");
    size_t end = token.find_last_not_of(" \t");
    if (begin != std::string::npos)
      mask |= pluginTestSubsysBit(token.substr(begin, end - begin + 1));
    if (comma == std::string::npos) break;
    pos = comma + 1;
  }
  return mask == 0 ? NCCL_ALL : mask;
}

static inline void pluginTestLogFunction(ncclDebugLogLevel level,
                                         unsigned long flags,
                                         const char* file, int line,
                                         const char* fmt, ...) {
  ncclDebugLogLevel debugLevel = pluginTestDebugLevel();
  if (level > debugLevel) return;
  if (level >= NCCL_LOG_INFO && (flags & pluginTestDebugSubsys()) == 0) return;

  flockfile(stderr);
  std::fprintf(stderr, "NCCL %s %s:%d ", pluginTestLogLevelName(level),
               file ? file : "<unknown>", line);
  va_list args;
  va_start(args, fmt);
  std::vfprintf(stderr, fmt, args);
  va_end(args);
  std::fprintf(stderr, "\n");
  funlockfile(stderr);
}

#endif
