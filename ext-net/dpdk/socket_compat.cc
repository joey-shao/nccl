#include "socket_compat.h"

#include "plugin_compat.h"

#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ifaddrs.h>
#include <net/if.h>
#include <netinet/in.h>
#include <time.h>
#include <unistd.h>

#ifndef ENONET
#define ENONET ENETDOWN
#endif

struct netIf {
  char prefix[64];
  int port;
};

static void msleep(unsigned int timeMsec) {
  struct timespec tv;
  tv.tv_sec = timeMsec / 1000;
  tv.tv_nsec = (long)(timeMsec % 1000) * 1000000L;
  nanosleep(&tv, NULL);
}

static int loadSocketParamCached(const char* envName, int defaultValue,
                                 std::atomic<int>& cache) {
  int cached = cache.load(std::memory_order_relaxed);
  if (cached != INT32_MIN) return cached;

  int value = defaultValue;
  const char* env = ncclGetEnv(envName);
  if (env != NULL && env[0] != '\0') {
    errno = 0;
    char* end = NULL;
    long parsed = strtol(env, &end, 0);
    if (errno == 0 && end != env && *end == '\0') value = (int)parsed;
  }
  cache.store(value, std::memory_order_relaxed);
  return value;
}

static int ncclParamRetryCnt() {
  static std::atomic<int> cache(INT32_MIN);
  return loadSocketParamCached("NCCL_SOCKET_RETRY_CNT", 34, cache);
}

static int ncclParamRetryTimeOut() {
  static std::atomic<int> cache(INT32_MIN);
  return loadSocketParamCached("NCCL_SOCKET_RETRY_SLEEP_MSEC", 100, cache);
}

static int ncclParamSocketMaxRecvBuff() {
  static std::atomic<int> cache(INT32_MIN);
  return loadSocketParamCached("NCCL_SOCKET_RCVBUF", -1, cache);
}

static int ncclParamSocketMaxSendBuff() {
  static std::atomic<int> cache(INT32_MIN);
  return loadSocketParamCached("NCCL_SOCKET_SNDBUF", -1, cache);
}

static int parseStringList(const char* string, struct netIf* ifList,
                           int maxList) {
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

static bool matchIfList(const char* string, int port, struct netIf* ifList,
                        int listSize, bool matchExact) {
  if (listSize == 0) return true;
  for (int i = 0; i < listSize; i++) {
    if (matchIf(string, ifList[i].prefix, matchExact) &&
        matchPort(port, ifList[i].port)) {
      return true;
    }
  }
  return false;
}

static ncclResult_t socketProgressOpt(int op, struct ncclSocket* sock, void* ptr,
                                      int size, int* offset, int block,
                                      int* closed) {
  int bytes = 0;
  *closed = 0;
  char* data = (char*)ptr;
  char line[SOCKET_NAME_MAXLEN + 1];
  do {
    if (op == NCCL_SOCKET_RECV)
      bytes = recv(sock->fd, data + (*offset), size - (*offset),
                   block ? 0 : MSG_DONTWAIT);
    if (op == NCCL_SOCKET_SEND)
      bytes = send(sock->fd, data + (*offset), size - (*offset),
                   block ? MSG_NOSIGNAL : MSG_DONTWAIT | MSG_NOSIGNAL);
    if (op == NCCL_SOCKET_RECV && bytes == 0) {
      *closed = 1;
      return ncclSuccess;
    }
    if (bytes == -1) {
      if ((op == NCCL_SOCKET_SEND && errno == EPIPE) ||
          (op == NCCL_SOCKET_RECV && errno == ECONNRESET)) {
        *closed = 1;
        return ncclSuccess;
      }
      if (errno != EINTR && errno != EWOULDBLOCK && errno != EAGAIN) {
        WARN("socketProgressOpt: Call to %s %s failed : %s",
             (op == NCCL_SOCKET_RECV ? "recv from" : "send to"),
             ncclSocketToString(&sock->addr, line), strerror(errno));
        return ncclRemoteError;
      }
      bytes = 0;
    }
    (*offset) += bytes;
    if (sock->abortFlag && __atomic_load_n(sock->abortFlag, __ATOMIC_ACQUIRE)) {
      INFO(NCCL_NET, "socketProgressOpt: abort called");
      return ncclInternalError;
    }
  } while (sock->asyncFlag == 0 && bytes > 0 && (*offset) < size);
  return ncclSuccess;
}

static ncclResult_t socketProgress(int op, struct ncclSocket* sock, void* ptr,
                                   int size, int* offset, int* pclosed = NULL) {
  int closed;
  NCCLCHECK(socketProgressOpt(op, sock, ptr, size, offset, 0, &closed));
  if (closed) {
    if (pclosed) {
      *pclosed = closed;
      return ncclSuccess;
    }
    char line[SOCKET_NAME_MAXLEN + 1];
    WARN("socketProgress: Connection closed by remote peer %s",
         ncclSocketToString(&sock->addr, line, 0));
    return ncclRemoteError;
  }
  return ncclSuccess;
}

static ncclResult_t socketWait(int op, struct ncclSocket* sock, void* ptr,
                               int size, int* offset) {
  while (*offset < size) NCCLCHECK(socketProgress(op, sock, ptr, size, offset));
  return ncclSuccess;
}

const char* ncclSocketToString(const union ncclSocketAddress* addr, char* buf,
                               const int numericHostForm) {
  const struct sockaddr* saddr;
  char host[NI_MAXHOST], service[NI_MAXSERV];
  int flag = NI_NUMERICSERV | (numericHostForm ? NI_NUMERICHOST : 0);
  if (buf == NULL || addr == NULL) goto fail;
  saddr = &addr->sa;
  if (saddr->sa_family != AF_INET && saddr->sa_family != AF_INET6) goto fail;
  if (getnameinfo(saddr, sizeof(union ncclSocketAddress), host, NI_MAXHOST,
                  service, NI_MAXSERV, flag))
    goto fail;
  snprintf(buf, SOCKET_NAME_MAXLEN + 1, "%s<%s>", host, service);
  return buf;
fail:
  if (buf) buf[0] = '\0';
  return buf;
}

static uint16_t socketToPort(union ncclSocketAddress* addr) {
  struct sockaddr* saddr = &addr->sa;
  return ntohs(saddr->sa_family == AF_INET ? addr->sin.sin_port
                                            : addr->sin6.sin6_port);
}

static int envSocketFamily(void) {
  int family = -1;
  const char* env = ncclGetEnv("NCCL_SOCKET_FAMILY");
  if (env == NULL) return family;

  INFO(NCCL_ENV, "NCCL_SOCKET_FAMILY set by environment to %s", env);

  if (strcmp(env, "AF_INET") == 0)
    family = AF_INET;
  else if (strcmp(env, "AF_INET6") == 0)
    family = AF_INET6;
  return family;
}

static ncclResult_t findInterfaces(const char* prefixList, char* names,
                                   union ncclSocketAddress* addrs,
                                   int sockFamily, int maxIfNameSize,
                                   int maxIfs, int* found) {
  char line[SOCKET_NAME_MAXLEN + 1];
  struct netIf userIfs[MAX_IFS];
  bool searchNot = prefixList && prefixList[0] == '^';
  if (searchNot) prefixList++;
  bool searchExact = prefixList && prefixList[0] == '=';
  if (searchExact) prefixList++;
  int nUserIfs = parseStringList(prefixList, userIfs, MAX_IFS);

  *found = 0;
  struct ifaddrs* interfaces;
  if (getifaddrs(&interfaces) != 0) {
    WARN("findInterfaces: getifaddrs failed : %s", strerror(errno));
    return ncclSystemError;
  }
  for (struct ifaddrs* interface = interfaces;
       interface && *found < maxIfs;
       interface = interface->ifa_next) {
    if (interface->ifa_addr == NULL) continue;

    int family = interface->ifa_addr->sa_family;
    if (family != AF_INET && family != AF_INET6) continue;

    if (!(interface->ifa_flags & IFF_RUNNING)) continue;

    TRACE(NCCL_INIT | NCCL_NET, "Found interface %s:%s", interface->ifa_name,
          ncclSocketToString((union ncclSocketAddress*)interface->ifa_addr,
                             line));

    if (sockFamily != -1 && family != sockFamily) continue;

    if (family == AF_INET6) {
      struct sockaddr_in6* sa = (struct sockaddr_in6*)(interface->ifa_addr);
      if (IN6_IS_ADDR_LOOPBACK(&sa->sin6_addr)) continue;
    }

    if (!(matchIfList(interface->ifa_name, -1, userIfs, nUserIfs, searchExact) ^
          searchNot)) {
      continue;
    }

    bool duplicate = false;
    for (int i = 0; i < *found; i++) {
      if (strcmp(interface->ifa_name, names + i * maxIfNameSize) == 0) {
        duplicate = true;
        break;
      }
    }

    if (!duplicate) {
      strncpy(names + (*found) * maxIfNameSize, interface->ifa_name,
              maxIfNameSize);
      int salen = (family == AF_INET) ? sizeof(struct sockaddr_in)
                                      : sizeof(struct sockaddr_in6);
      memset(addrs + *found, '\0', sizeof(*addrs));
      memcpy(addrs + *found, interface->ifa_addr, salen);
      (*found)++;
    }
  }

  freeifaddrs(interfaces);
  return ncclSuccess;
}

static bool matchSubnet(struct ifaddrs localIf,
                        union ncclSocketAddress* remote) {
  int family = localIf.ifa_addr->sa_family;
  if (family != remote->sa.sa_family) return false;

  if (family == AF_INET) {
    struct sockaddr_in* localAddr = (struct sockaddr_in*)(localIf.ifa_addr);
    struct sockaddr_in* mask = (struct sockaddr_in*)(localIf.ifa_netmask);
    struct sockaddr_in& remoteAddr = remote->sin;
    struct in_addr localSubnet, remoteSubnet;
    localSubnet.s_addr = localAddr->sin_addr.s_addr & mask->sin_addr.s_addr;
    remoteSubnet.s_addr = remoteAddr.sin_addr.s_addr & mask->sin_addr.s_addr;
    return (localSubnet.s_addr ^ remoteSubnet.s_addr) ? false : true;
  }

  if (family == AF_INET6) {
    struct sockaddr_in6* localAddr = (struct sockaddr_in6*)(localIf.ifa_addr);
    struct sockaddr_in6* mask = (struct sockaddr_in6*)(localIf.ifa_netmask);
    struct sockaddr_in6& remoteAddr = remote->sin6;
    struct in6_addr& localIn6 = localAddr->sin6_addr;
    struct in6_addr& maskIn6 = mask->sin6_addr;
    struct in6_addr& remoteIn6 = remoteAddr.sin6_addr;
    bool same = true;
    for (int c = 0; c < 16; c++) {
      char c1 = localIn6.s6_addr[c] & maskIn6.s6_addr[c];
      char c2 = remoteIn6.s6_addr[c] & maskIn6.s6_addr[c];
      if (c1 ^ c2) {
        same = false;
        break;
      }
    }
    same &= (localAddr->sin6_scope_id == remoteAddr.sin6_scope_id);
    return same;
  }

  INFO(NCCL_NET, "Net : Unsupported address family type");
  return false;
}

static ncclResult_t ncclFindInterfaceMatchSubnet(
    char* ifName, union ncclSocketAddress* localAddr,
    union ncclSocketAddress* remoteAddr, int ifNameMaxSize, int* found) {
  char line[SOCKET_NAME_MAXLEN + 1];
  char lineA[SOCKET_NAME_MAXLEN + 1];
  *found = 0;
  struct ifaddrs* interfaces;
  if (getifaddrs(&interfaces) != 0) {
    WARN("ncclFindInterfaceMatchSubnet: getifaddrs failed : %s",
         strerror(errno));
    return ncclSystemError;
  }
  for (struct ifaddrs* interface = interfaces;
       interface && !*found;
       interface = interface->ifa_next) {
    if (interface->ifa_addr == NULL) continue;

    int family = interface->ifa_addr->sa_family;
    if (family != AF_INET && family != AF_INET6) continue;

    if (!matchSubnet(*interface, remoteAddr)) continue;

    int salen = (family == AF_INET) ? sizeof(struct sockaddr_in)
                                    : sizeof(struct sockaddr_in6);
    memcpy(localAddr, interface->ifa_addr, salen);

    strncpy(ifName, interface->ifa_name, ifNameMaxSize);

    TRACE(NCCL_INIT | NCCL_NET,
          "NET : Found interface %s:%s in the same subnet as remote address %s",
          interface->ifa_name, ncclSocketToString(localAddr, line),
          ncclSocketToString(remoteAddr, lineA));
    *found = 1;
  }

  freeifaddrs(interfaces);
  return ncclSuccess;
}

static ncclResult_t ncclSocketGetAddrFromString(union ncclSocketAddress* ua,
                                                const char* ipPortPair) {
  if (!(ipPortPair && strlen(ipPortPair) > 1)) {
    WARN("Net : string is null");
    return ncclInvalidArgument;
  }

  bool ipv6 = ipPortPair[0] == '[';
  if (!ipv6) {
    struct netIf ni;
    if (parseStringList(ipPortPair, &ni, 1) != 1) {
      WARN("Net : No valid <IPv4_or_hostname>:<port> pair found");
      return ncclInvalidArgument;
    }

    struct addrinfo hints;
    struct addrinfo* p;
    int rv;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    rv = getaddrinfo(ni.prefix, NULL, &hints, &p);
    if (rv != 0) {
      WARN("Net : error encountered when getting address info : %s",
           gai_strerror(rv));
      return ncclInvalidArgument;
    }

    if (p->ai_family == AF_INET) {
      struct sockaddr_in& sin = ua->sin;
      memcpy(&sin, p->ai_addr, sizeof(struct sockaddr_in));
      sin.sin_family = AF_INET;
      sin.sin_port = htons(ni.port);
    } else if (p->ai_family == AF_INET6) {
      struct sockaddr_in6& sin6 = ua->sin6;
      memcpy(&sin6, p->ai_addr, sizeof(struct sockaddr_in6));
      sin6.sin6_family = AF_INET6;
      sin6.sin6_port = htons(ni.port);
      sin6.sin6_flowinfo = 0;
      sin6.sin6_scope_id = 0;
    } else {
      WARN("Net : unsupported IP family");
      freeaddrinfo(p);
      return ncclInvalidArgument;
    }

    freeaddrinfo(p);

  } else {
    int i;
    int j = -1;
    int len = (int)strlen(ipPortPair);
    for (i = 1; i < len; i++) {
      if (ipPortPair[i] == '%') j = i;
      if (ipPortPair[i] == ']') break;
    }
    if (i == len) {
      WARN("Net : No valid [IPv6]:port pair found");
      return ncclInvalidArgument;
    }
    bool globalScope = (j == -1 ? true : false);

    char ipStr[NI_MAXHOST];
    char portStr[NI_MAXSERV];
    char ifName[IFNAMSIZ];
    memset(ipStr, '\0', sizeof(ipStr));
    memset(portStr, '\0', sizeof(portStr));
    memset(ifName, '\0', sizeof(ifName));
    strncpy(ipStr, ipPortPair + 1, globalScope ? i - 1 : j - 1);
    strncpy(portStr, ipPortPair + i + 2, len - i - 1);
    int port = atoi(portStr);
    if (!globalScope) strncpy(ifName, ipPortPair + j + 1, i - j - 1);

    struct sockaddr_in6& sin6 = ua->sin6;
    sin6.sin6_family = AF_INET6;
    inet_pton(AF_INET6, ipStr, &(sin6.sin6_addr));
    sin6.sin6_port = htons(port);
    sin6.sin6_flowinfo = 0;
    sin6.sin6_scope_id = globalScope ? 0 : if_nametoindex(ifName);
  }
  return ncclSuccess;
}

ncclResult_t ncclFindInterfaces(char* ifNames, union ncclSocketAddress* ifAddrs,
                                int ifNameMaxSize, int maxIfs, int* nIfs) {
  static int shownIfName = 0;
  int sockFamily = envSocketFamily();
  const char* env = ncclGetEnv("NCCL_SOCKET_IFNAME");
  *nIfs = 0;
  if (env && strlen(env) > 1) {
    INFO(NCCL_ENV, "NCCL_SOCKET_IFNAME set by environment to %s", env);
    if (shownIfName++ == 0)
      INFO(NCCL_NET, "NCCL_SOCKET_IFNAME set to %s", env);
    NCCLCHECK(findInterfaces(env, ifNames, ifAddrs, sockFamily, ifNameMaxSize,
                             maxIfs, nIfs));
  } else {
    NCCLCHECK(findInterfaces("ib", ifNames, ifAddrs, sockFamily, ifNameMaxSize,
                             maxIfs, nIfs));
    if (*nIfs == 0) {
      const char* commId = ncclGetEnv("NCCL_COMM_ID");
      if (commId && strlen(commId) > 1) {
        INFO(NCCL_ENV, "NCCL_COMM_ID set by environment to %s", commId);
        union ncclSocketAddress idAddr;
        NCCLCHECK(ncclSocketGetAddrFromString(&idAddr, commId));
        NCCLCHECK(ncclFindInterfaceMatchSubnet(ifNames, ifAddrs, &idAddr,
                                               ifNameMaxSize, nIfs));
      }
    }
    if (*nIfs == 0)
      NCCLCHECK(findInterfaces("^docker,lo,virbr", ifNames, ifAddrs,
                               sockFamily, ifNameMaxSize, maxIfs, nIfs));
    if (*nIfs == 0)
      NCCLCHECK(findInterfaces("docker", ifNames, ifAddrs, sockFamily,
                               ifNameMaxSize, maxIfs, nIfs));
    if (*nIfs == 0)
      NCCLCHECK(findInterfaces("lo", ifNames, ifAddrs, sockFamily,
                               ifNameMaxSize, maxIfs, nIfs));
    if (*nIfs == 0)
      NCCLCHECK(findInterfaces("virbr", ifNames, ifAddrs, sockFamily,
                               ifNameMaxSize, maxIfs, nIfs));
  }
  return ncclSuccess;
}

static ncclResult_t socketSetFlags(struct ncclSocket* sock) {
  const int one = 1;
  if ((sock->asyncFlag || sock->abortFlag) && sock->fd >= 0) {
    int flags = fcntl(sock->fd, F_GETFL);
    if (flags == -1) {
      WARN("socketSetFlags: fcntl(F_GETFL) failed : %s", strerror(errno));
      return ncclSystemError;
    }
    if (fcntl(sock->fd, F_SETFL, flags | O_NONBLOCK) == -1) {
      WARN("socketSetFlags: fcntl(F_SETFL) failed : %s", strerror(errno));
      return ncclSystemError;
    }
  }
  if (setsockopt(sock->fd, IPPROTO_TCP, TCP_NODELAY, (char*)&one,
                 sizeof(int)) == -1) {
    WARN("socketSetFlags: setsockopt TCP_NODELAY failed : %s", strerror(errno));
    return ncclSystemError;
  }

  int rcvBuf = ncclParamSocketMaxRecvBuff();
  int sndBuf = ncclParamSocketMaxSendBuff();
  if (sndBuf > 0 &&
      setsockopt(sock->fd, SOL_SOCKET, SO_SNDBUF, (char*)&sndBuf,
                 sizeof(int)) == -1) {
    WARN("socketSetFlags: setsockopt SO_SNDBUF failed : %s", strerror(errno));
    return ncclSystemError;
  }
  if (rcvBuf > 0 &&
      setsockopt(sock->fd, SOL_SOCKET, SO_RCVBUF, (char*)&rcvBuf,
                 sizeof(int)) == -1) {
    WARN("socketSetFlags: setsockopt SO_RCVBUF failed : %s", strerror(errno));
    return ncclSystemError;
  }
  return ncclSuccess;
}

static void socketResetAccept(struct ncclSocket* sock) {
  char line[SOCKET_NAME_MAXLEN + 1];
  INFO(NCCL_NET | NCCL_INIT,
       "socketFinalizeAccept: didn't receive a valid magic from %s",
       ncclSocketToString(&sock->addr, line));
  (void)close(sock->fd);
  sock->fd = -1;
  sock->state = ncclSocketStateAccepting;
  sock->finalizeCounter = 0;
}

static ncclResult_t socketFinalizeAccept(struct ncclSocket* sock) {
  uint64_t magic;
  enum ncclSocketType type;
  int received;
  char line[SOCKET_NAME_MAXLEN + 1];
  NCCLCHECK(socketSetFlags(sock));

  if (sock->asyncFlag == 0 || sock->finalizeCounter < (int)sizeof(magic)) {
    if (sock->asyncFlag == 0) {
      received = 0;
      if (socketWait(NCCL_SOCKET_RECV, sock, &magic, sizeof(magic), &received) !=
          ncclSuccess) {
        socketResetAccept(sock);
        return ncclSuccess;
      }
    } else {
      int closed = 0;
      received = sock->finalizeCounter;
      NCCLCHECK(socketProgress(NCCL_SOCKET_RECV, sock, sock->finalizeBuffer,
                               sizeof(magic), &received, &closed));
      sock->finalizeCounter = received;
      if (received < (int)sizeof(magic)) {
        if (closed) socketResetAccept(sock);
        return ncclSuccess;
      }
      memcpy(&magic, sock->finalizeBuffer, sizeof(magic));
    }
    if (magic != sock->magic) {
      socketResetAccept(sock);
      return ncclSuccess;
    }
  }

  if (sock->asyncFlag == 0) {
    received = 0;
    NCCLCHECK(socketWait(NCCL_SOCKET_RECV, sock, &type, sizeof(type), &received));
  } else {
    received = sock->finalizeCounter - (int)sizeof(magic);
    NCCLCHECK(socketProgress(NCCL_SOCKET_RECV, sock, sock->finalizeBuffer,
                             sizeof(type), &received));
    sock->finalizeCounter = received + (int)sizeof(magic);
    if (received < (int)sizeof(type)) return ncclSuccess;
    memcpy(&type, sock->finalizeBuffer, sizeof(type));
  }

  if (type != sock->type) {
    WARN("socketFinalizeAccept from %s: wrong type %d != %d",
         ncclSocketToString(&sock->addr, line), type, sock->type);
    sock->state = ncclSocketStateError;
    close(sock->fd);
    sock->fd = -1;
    return ncclInternalError;
  }
  sock->state = ncclSocketStateReady;
  return ncclSuccess;
}

static ncclResult_t socketResetFd(struct ncclSocket* sock) {
  ncclResult_t ret = ncclSuccess;
  int fd = -1;

  fd = socket(sock->addr.sa.sa_family, SOCK_STREAM, 0);
  if (fd == -1) {
    WARN("socketResetFd: socket failed : %s", strerror(errno));
    return ncclSystemError;
  }

  if (sock->fd != -1) {
    if (dup2(fd, sock->fd) == -1) {
      WARN("socketResetFd: dup2 failed : %s", strerror(errno));
      ret = ncclSystemError;
      goto cleanup;
    }
    if (close(fd) == -1) {
      WARN("socketResetFd: close failed : %s", strerror(errno));
      ret = ncclSystemError;
      goto cleanup;
    }
  } else {
    sock->fd = fd;
  }

  NCCLCHECKGOTO(socketSetFlags(sock), ret, exit);
exit:
  return ret;
cleanup:
  if (fd != -1) (void)close(fd);
  goto exit;
}

static ncclResult_t socketConnectCheck(struct ncclSocket* sock, int errCode,
                                       const char funcName[]) {
  char line[SOCKET_NAME_MAXLEN + 1];
  if (errCode == 0) {
    sock->state = ncclSocketStateConnected;
  } else if (errCode == EINPROGRESS) {
    sock->state = ncclSocketStateConnectPolling;
  } else if (errCode == EINTR || errCode == EWOULDBLOCK || errCode == EAGAIN ||
             errCode == ETIMEDOUT || errCode == EHOSTUNREACH ||
             errCode == ECONNREFUSED) {
    if (sock->customRetry == 0) {
      if (sock->errorRetries++ == ncclParamRetryCnt()) {
        sock->state = ncclSocketStateError;
        WARN("%s: connect to %s returned %s, exceeded error retry count after %d attempts",
             funcName, ncclSocketToString(&sock->addr, line), strerror(errCode),
             sock->errorRetries);
        return ncclRemoteError;
      }
      unsigned int sleepTime = (unsigned int)(sock->errorRetries *
                                              ncclParamRetryTimeOut());
      INFO(NCCL_NET | NCCL_INIT,
           "%s: connect to %s returned %s, retrying (%d/%d) after sleep for %u msec",
           funcName, ncclSocketToString(&sock->addr, line), strerror(errCode),
           sock->errorRetries, ncclParamRetryCnt(), sleepTime);
      msleep(sleepTime);
    }
    NCCLCHECK(socketResetFd(sock));
    sock->state = ncclSocketStateConnecting;
  } else {
    sock->state = ncclSocketStateError;
    WARN("%s: connect to %s failed : %s", funcName,
         ncclSocketToString(&sock->addr, line), strerror(errCode));
    return ncclSystemError;
  }
  return ncclSuccess;
}

static ncclResult_t socketStartConnect(struct ncclSocket* sock) {
  int ret = connect(sock->fd, &sock->addr.sa, sock->salen);
  return socketConnectCheck(sock, (ret == -1) ? errno : 0, __func__);
}

static ncclResult_t socketPollConnect(struct ncclSocket* sock) {
  struct pollfd pfd;
  int timeout = 1;
  int ret;
  socklen_t rlen = sizeof(int);
  char line[SOCKET_NAME_MAXLEN + 1];

  memset(&pfd, 0, sizeof(struct pollfd));
  pfd.fd = sock->fd;
  pfd.events = POLLOUT;
  ret = poll(&pfd, 1, timeout);

  if (ret == 0 || (ret < 0 && errno == EINTR)) {
    return ncclSuccess;
  }
  if (ret < 0) {
    WARN("socketPollConnect to %s failed with error %s",
         ncclSocketToString(&sock->addr, line), strerror(errno));
    return ncclSystemError;
  }

  if (getsockopt(sock->fd, SOL_SOCKET, SO_ERROR, (void*)&ret, &rlen) == -1) {
    WARN("socketPollConnect: getsockopt failed : %s", strerror(errno));
    return ncclSystemError;
  }
  return socketConnectCheck(sock, ret, __func__);
}

static ncclResult_t socketFinalizeConnect(struct ncclSocket* sock) {
  int sent;
  if (sock->asyncFlag == 0) {
    sent = 0;
    NCCLCHECK(socketWait(NCCL_SOCKET_SEND, sock, &sock->magic,
                         sizeof(sock->magic), &sent));
    sent = 0;
    NCCLCHECK(socketWait(NCCL_SOCKET_SEND, sock, &sock->type,
                         sizeof(sock->type), &sent));
  } else {
    if (sock->finalizeCounter < (int)sizeof(sock->magic)) {
      sent = sock->finalizeCounter;
      NCCLCHECK(socketProgress(NCCL_SOCKET_SEND, sock, &sock->magic,
                               sizeof(sock->magic), &sent));
      sock->finalizeCounter = sent;
      if (sent < (int)sizeof(sock->magic)) return ncclSuccess;
    }
    sent = sock->finalizeCounter - (int)sizeof(sock->magic);
    NCCLCHECK(socketProgress(NCCL_SOCKET_SEND, sock, &sock->type,
                             sizeof(sock->type), &sent));
    sock->finalizeCounter = sent + (int)sizeof(sock->magic);
    if (sent < (int)sizeof(sock->type)) return ncclSuccess;
  }
  sock->state = ncclSocketStateReady;
  return ncclSuccess;
}

static ncclResult_t socketTryAccept(struct ncclSocket* sock) {
  socklen_t socklen = sizeof(union ncclSocketAddress);
  sock->fd = accept(sock->acceptFd, (struct sockaddr*)&sock->addr, &socklen);
  if (sock->fd != -1) {
    sock->state = ncclSocketStateAccepted;
  } else if (errno == ENETDOWN || errno == EPROTO || errno == ENOPROTOOPT ||
             errno == EHOSTDOWN || errno == ENONET || errno == EHOSTUNREACH ||
             errno == EOPNOTSUPP || errno == ENETUNREACH || errno == EINTR) {
    if (++sock->errorRetries == ncclParamRetryCnt()) {
      WARN("socketTryAccept: exceeded error retry count after %d attempts, %s",
           sock->errorRetries, strerror(errno));
      return ncclSystemError;
    }
    INFO(NCCL_NET | NCCL_INIT, "Call to accept returned %s, retrying",
         strerror(errno));
  } else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
    WARN("socketTryAccept: Accept failed: %s", strerror(errno));
    return ncclSystemError;
  }
  return ncclSuccess;
}

static ncclResult_t socketProgressState(struct ncclSocket* sock) {
  if (sock->state == ncclSocketStateAccepting) {
    NCCLCHECK(socketTryAccept(sock));
  }
  if (sock->state == ncclSocketStateAccepted) {
    NCCLCHECK(socketFinalizeAccept(sock));
  }
  if (sock->state == ncclSocketStateConnecting) {
    NCCLCHECK(socketStartConnect(sock));
  }
  if (sock->state == ncclSocketStateConnectPolling) {
    NCCLCHECK(socketPollConnect(sock));
  }
  if (sock->state == ncclSocketStateConnected) {
    NCCLCHECK(socketFinalizeConnect(sock));
  }
  return ncclSuccess;
}

ncclResult_t ncclSocketInit(struct ncclSocket* sock,
                            const union ncclSocketAddress* addr, uint64_t magic,
                            enum ncclSocketType type,
                            volatile uint32_t* abortFlag, int asyncFlag,
                            int customRetry) {
  ncclResult_t ret = ncclSuccess;

  if (sock == NULL) goto exit;
  sock->errorRetries = 0;
  sock->abortFlag = abortFlag;
  sock->asyncFlag = asyncFlag;
  sock->state = ncclSocketStateInitialized;
  sock->magic = magic;
  sock->type = type;
  sock->fd = -1;
  sock->acceptFd = -1;
  sock->customRetry = customRetry;
  sock->finalizeCounter = 0;
  memset(sock->finalizeBuffer, 0, sizeof(sock->finalizeBuffer));

  if (addr) {
    int family;
    memcpy(&sock->addr, addr, sizeof(union ncclSocketAddress));
    family = sock->addr.sa.sa_family;
    if (family != AF_INET && family != AF_INET6) {
      char line[SOCKET_NAME_MAXLEN + 1];
      WARN("ncclSocketInit: connecting to address %s with family %d is neither AF_INET(%d) nor AF_INET6(%d)",
           ncclSocketToString(&sock->addr, line), family, AF_INET, AF_INET6);
      ret = ncclInternalError;
      goto exit;
    }
    sock->salen =
        (family == AF_INET) ? sizeof(struct sockaddr_in) : sizeof(struct sockaddr_in6);
    NCCLCHECKGOTO(socketResetFd(sock), ret, fail);
  } else {
    memset(&sock->addr, 0, sizeof(union ncclSocketAddress));
    sock->salen = sizeof(struct sockaddr_in);
  }

exit:
  return ret;
fail:
  if (sock->fd != -1) {
    close(sock->fd);
    sock->fd = -1;
  }
  goto exit;
}

ncclResult_t ncclSocketListen(struct ncclSocket* sock) {
  if (sock == NULL) {
    WARN("ncclSocketListen: pass NULL socket");
    return ncclInvalidArgument;
  }
  if (sock->fd == -1) {
    WARN("ncclSocketListen: file descriptor is -1");
    return ncclInvalidArgument;
  }

  if (socketToPort(&sock->addr)) {
    int opt = 1;
    if (setsockopt(sock->fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) ==
        -1) {
      WARN("ncclSocketListen: setsockopt(SO_REUSEADDR) failed : %s",
           strerror(errno));
      return ncclSystemError;
    }
#if defined(SO_REUSEPORT)
    if (setsockopt(sock->fd, SOL_SOCKET, SO_REUSEPORT, &opt, sizeof(opt)) ==
        -1) {
      WARN("ncclSocketListen: setsockopt(SO_REUSEPORT) failed : %s",
           strerror(errno));
      return ncclSystemError;
    }
#endif
  }

  if (bind(sock->fd, &sock->addr.sa, sock->salen) == -1) {
    WARN("ncclSocketListen: bind failed : %s", strerror(errno));
    return ncclSystemError;
  }

  socklen_t size = sock->salen;
  if (getsockname(sock->fd, &sock->addr.sa, &size) == -1) {
    WARN("ncclSocketListen: getsockname failed : %s", strerror(errno));
    return ncclSystemError;
  }

  if (listen(sock->fd, 16384) == -1) {
    WARN("ncclSocketListen: listen failed : %s", strerror(errno));
    return ncclSystemError;
  }

  sock->state = ncclSocketStateReady;
  return ncclSuccess;
}

ncclResult_t ncclSocketGetAddr(struct ncclSocket* sock,
                               union ncclSocketAddress* addr) {
  if (sock == NULL) {
    WARN("ncclSocketGetAddr: pass NULL socket");
    return ncclInvalidArgument;
  }
  if (sock->state != ncclSocketStateReady) return ncclInternalError;
  memcpy(addr, &sock->addr, sizeof(union ncclSocketAddress));
  return ncclSuccess;
}

ncclResult_t ncclSocketConnect(struct ncclSocket* sock) {
  char line[SOCKET_NAME_MAXLEN + 1];

  if (sock == NULL) {
    WARN("ncclSocketConnect: pass NULL socket");
    return ncclInvalidArgument;
  }
  if (sock->fd == -1) {
    WARN("ncclSocketConnect: file descriptor is -1");
    return ncclInvalidArgument;
  }

  if (sock->state != ncclSocketStateInitialized) {
    WARN("ncclSocketConnect: wrong socket state %d", sock->state);
    if (sock->state == ncclSocketStateError) return ncclRemoteError;
    return ncclInternalError;
  }
  TRACE(NCCL_INIT | NCCL_NET, "Connecting to socket %s",
        ncclSocketToString(&sock->addr, line));

  sock->state = ncclSocketStateConnecting;
  sock->finalizeCounter = 0;
  do {
    NCCLCHECK(socketProgressState(sock));
  } while (
      sock->asyncFlag == 0 &&
      (sock->abortFlag == NULL ||
       __atomic_load_n(sock->abortFlag, __ATOMIC_ACQUIRE) == 0) &&
      (sock->state == ncclSocketStateConnecting ||
       sock->state == ncclSocketStateConnectPolling ||
       sock->state == ncclSocketStateConnected));

  if (sock->abortFlag && __atomic_load_n(sock->abortFlag, __ATOMIC_ACQUIRE))
    return ncclInternalError;

  switch (sock->state) {
    case ncclSocketStateConnecting:
    case ncclSocketStateConnectPolling:
    case ncclSocketStateConnected:
    case ncclSocketStateReady:
      return ncclSuccess;
    case ncclSocketStateError:
      return ncclSystemError;
    default:
      WARN("ncclSocketConnect: wrong socket state %d", sock->state);
      return ncclInternalError;
  }
}

ncclResult_t ncclSocketReady(struct ncclSocket* sock, int* running) {
  if (sock == NULL) {
    *running = 0;
    return ncclSuccess;
  }
  if (sock->state == ncclSocketStateError ||
      sock->state == ncclSocketStateClosed) {
    WARN("ncclSocketReady: unexpected socket state %d", sock->state);
    return ncclRemoteError;
  }
  *running = (sock->state == ncclSocketStateReady) ? 1 : 0;
  if (*running == 0) {
    NCCLCHECK(socketProgressState(sock));
    *running = (sock->state == ncclSocketStateReady) ? 1 : 0;
  }
  return ncclSuccess;
}

ncclResult_t ncclSocketAccept(struct ncclSocket* sock,
                              struct ncclSocket* listenSock) {
  ncclResult_t ret = ncclSuccess;

  if (listenSock == NULL || sock == NULL) {
    WARN("ncclSocketAccept: pass NULL socket");
    ret = ncclInvalidArgument;
    goto exit;
  }
  if (listenSock->state != ncclSocketStateReady) {
    WARN("ncclSocketAccept: wrong socket state %d", listenSock->state);
    if (listenSock->state == ncclSocketStateError)
      ret = ncclSystemError;
    else
      ret = ncclInternalError;
    goto exit;
  }

  if (sock->acceptFd == -1) {
    memcpy(sock, listenSock, sizeof(struct ncclSocket));
    sock->acceptFd = listenSock->fd;
    sock->state = ncclSocketStateAccepting;
    sock->finalizeCounter = 0;
  }

  do {
    NCCLCHECKGOTO(socketProgressState(sock), ret, exit);
  } while (
      sock->asyncFlag == 0 &&
      (sock->abortFlag == NULL ||
       __atomic_load_n(sock->abortFlag, __ATOMIC_ACQUIRE) == 0) &&
      (sock->state == ncclSocketStateAccepting ||
       sock->state == ncclSocketStateAccepted));

  if (sock->abortFlag && __atomic_load_n(sock->abortFlag, __ATOMIC_ACQUIRE))
    return ncclInternalError;

  switch (sock->state) {
    case ncclSocketStateAccepting:
    case ncclSocketStateAccepted:
    case ncclSocketStateReady:
      ret = ncclSuccess;
      break;
    case ncclSocketStateError:
      ret = ncclSystemError;
      break;
    default:
      WARN("ncclSocketAccept: wrong socket state %d", sock->state);
      ret = ncclInternalError;
      break;
  }

exit:
  return ret;
}

ncclResult_t ncclSocketProgress(int op, struct ncclSocket* sock, void* ptr,
                                int size, int* offset, int* closed) {
  if (sock == NULL) {
    WARN("ncclSocketProgress: pass NULL socket");
    return ncclInvalidArgument;
  }
  NCCLCHECK(socketProgress(op, sock, ptr, size, offset, closed));
  return ncclSuccess;
}

ncclResult_t ncclSocketClose(struct ncclSocket* sock, bool wait) {
  if (sock != NULL) {
    if (sock->state > ncclSocketStateNone && sock->state < ncclSocketStateNum &&
        sock->fd >= 0) {
      if (wait) {
        char data;
        int closed = 0;
        do {
          int offset = 0;
          if (ncclSocketProgress(NCCL_SOCKET_RECV, sock, &data, sizeof(char),
                                 &offset, &closed) != ncclSuccess)
            break;
        } while (closed == 0);
      }
      (void)shutdown(sock->fd, SHUT_RDWR);
      (void)close(sock->fd);
    }
    sock->state = ncclSocketStateClosed;
    sock->fd = -1;
  }
  return ncclSuccess;
}
