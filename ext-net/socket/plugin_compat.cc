#include "plugin_compat.h"

#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <random>
#include <unistd.h>

ncclDebugLogger_t ncclDpdkLogFunction = nullptr;

void ncclDpdkSetLogFunction(ncclDebugLogger_t logFunction) {
  ncclDpdkLogFunction = logFunction;
}

const char* ncclGetEnv(const char* name) {
  return getenv(name);
}

ncclResult_t getRandomData(void* data, size_t size) {
  if (data == NULL || size == 0) return ncclInvalidArgument;

  int fd = open("/dev/urandom", O_RDONLY);
  if (fd >= 0) {
    uint8_t* p = static_cast<uint8_t*>(data);
    size_t off = 0;
    while (off < size) {
      ssize_t n = read(fd, p + off, size - off);
      if (n > 0) {
        off += static_cast<size_t>(n);
        continue;
      }
      if (errno == EINTR) continue;
      close(fd);
      return ncclSystemError;
    }
    close(fd);
    return ncclSuccess;
  }

  std::random_device rd;
  uint8_t* p = static_cast<uint8_t*>(data);
  for (size_t i = 0; i < size; ++i) p[i] = static_cast<uint8_t>(rd());
  return ncclSuccess;
}
