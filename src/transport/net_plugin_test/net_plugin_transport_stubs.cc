#include "debug.h"
#include "param.h"
#include "utils.h"

#include <errno.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int ncclDebugLevel = NCCL_LOG_WARN;
uint64_t ncclDebugMask = NCCL_ALL;
FILE* ncclDebugFile = stderr;
thread_local int ncclDebugNoWarn = 0;
char ncclLastError[1024] = "";

void ncclDebugLog(ncclDebugLogLevel level, unsigned long flags, const char* filefunc, int line,
                  const char* fmt, ...) {
  (void)flags;
  if (ncclDebugLevel >= 0 && level > ncclDebugLevel) return;

  va_list args;
  va_start(args, fmt);
  if (level == NCCL_LOG_WARN) {
    va_list copy;
    va_copy(copy, args);
    (void)vsnprintf(ncclLastError, sizeof(ncclLastError), fmt, copy);
    va_end(copy);
  }

  FILE* out = (level <= NCCL_LOG_WARN) ? stderr : stdout;
  if (filefunc != NULL) {
    (void)fprintf(out, "[%s:%d] ", filefunc, line);
  }
  (void)vfprintf(out, fmt, args);
  (void)fputc('\n', out);
  va_end(args);
}

void ncclSetThreadName(pthread_t thread, const char* fmt, ...) {
  (void)thread;
  (void)fmt;
}

void ncclResetDebugInit() {}

const char* userHomeDir() {
  return getenv("HOME");
}

void setEnvFile(const char* fileName) {
  (void)fileName;
}

void initEnv() {}

const char* ncclGetEnv(const char* name) {
  return getenv(name);
}

void ncclLoadParam(char const* env, int64_t deftVal, int64_t uninitialized, int64_t* cache) {
  if (__atomic_load_n(cache, __ATOMIC_RELAXED) != uninitialized) return;

  int64_t value = deftVal;
  const char* str = ncclGetEnv(env);
  if (str != NULL && str[0] != '\0') {
    errno = 0;
    int64_t parsed = strtoll(str, nullptr, 0);
    if (errno == 0) value = parsed;
  }
  __atomic_store_n(cache, value, __ATOMIC_RELAXED);
}

int parseStringList(const char* string, struct netIf* ifList, int maxList) {
  if (string == NULL) return 0;

  const char* ptr = string;
  int ifNum = 0;
  int ifC = 0;
  char c;
  do {
    c = *ptr;
    if (c == ':') {
      if (ifC > 0) {
        ifList[ifNum].prefix[ifC] = '\0';
        ifList[ifNum].port = atoi(ptr + 1);
        ifNum++;
        ifC = 0;
      }
      while (c != ',' && c != '\0') c = *(++ptr);
    } else if (c == ',' || c == '\0') {
      if (ifC > 0) {
        ifList[ifNum].prefix[ifC] = '\0';
        ifList[ifNum].port = -1;
        ifNum++;
        ifC = 0;
      }
    } else {
      ifList[ifNum].prefix[ifC++] = c;
    }
    ptr++;
  } while (ifNum < maxList && c);

  return ifNum;
}

static bool matchIf(const char* string, const char* ref, bool matchExact) {
  int matchLen = matchExact ? (int)strlen(string) + 1 : (int)strlen(ref);
  return strncmp(string, ref, matchLen) == 0;
}

static bool matchPort(int port1, int port2) {
  if (port1 == -1 || port2 == -1) return true;
  return port1 == port2;
}

bool matchIfList(const char* string, int port, struct netIf* ifList, int listSize, bool matchExact) {
  if (listSize == 0) return true;
  for (int i = 0; i < listSize; i++) {
    if (matchIf(string, ifList[i].prefix, matchExact) && matchPort(port, ifList[i].port)) {
      return true;
    }
  }
  return false;
}
