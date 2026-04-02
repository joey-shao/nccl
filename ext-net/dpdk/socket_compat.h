#ifndef NCCL_DPDK_SOCKET_COMPAT_H_
#define NCCL_DPDK_SOCKET_COMPAT_H_

#include "net.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>

#define MAX_IFS 16
#define MAX_IF_NAME_SIZE 16
#define SOCKET_NAME_MAXLEN (NI_MAXHOST + NI_MAXSERV)
#define NCCL_SOCKET_MAGIC 0x564ab9f2fc4b9d6cULL

union ncclSocketAddress {
  struct sockaddr sa;
  struct sockaddr_in sin;
  struct sockaddr_in6 sin6;
};

enum ncclSocketState {
  ncclSocketStateNone = 0,
  ncclSocketStateInitialized = 1,
  ncclSocketStateAccepting = 2,
  ncclSocketStateAccepted = 3,
  ncclSocketStateConnecting = 4,
  ncclSocketStateConnectPolling = 5,
  ncclSocketStateConnected = 6,
  ncclSocketStateReady = 7,
  ncclSocketStateTerminating = 8,
  ncclSocketStateClosed = 9,
  ncclSocketStateError = 10,
  ncclSocketStateNum = 11
};

enum ncclSocketType {
  ncclSocketTypeUnknown = 0,
  ncclSocketTypeBootstrap = 1,
  ncclSocketTypeProxy = 2,
  ncclSocketTypeNetSocket = 3,
  ncclSocketTypeNetIb = 4,
  ncclSocketTypeRasNetwork = 5
};

struct ncclSocket {
  int fd;
  int acceptFd;
  int errorRetries;
  union ncclSocketAddress addr;
  volatile uint32_t* abortFlag;
  int asyncFlag;
  enum ncclSocketState state;
  int salen;
  uint64_t magic;
  enum ncclSocketType type;
  int customRetry;
  int finalizeCounter;
  char finalizeBuffer[sizeof(uint64_t)];
};

const char* ncclSocketToString(const union ncclSocketAddress* addr, char* buf,
                               const int numericHostForm = 1);
ncclResult_t ncclFindInterfaces(char* ifNames, union ncclSocketAddress* ifAddrs,
                                int ifNameMaxSize, int maxIfs, int* nIfs);
ncclResult_t ncclSocketInit(struct ncclSocket* sock,
                            const union ncclSocketAddress* addr = NULL,
                            uint64_t magic = NCCL_SOCKET_MAGIC,
                            enum ncclSocketType type = ncclSocketTypeUnknown,
                            volatile uint32_t* abortFlag = NULL,
                            int asyncFlag = 0, int customRetry = 0);
ncclResult_t ncclSocketListen(struct ncclSocket* sock);
ncclResult_t ncclSocketGetAddr(struct ncclSocket* sock,
                               union ncclSocketAddress* addr);
ncclResult_t ncclSocketConnect(struct ncclSocket* sock);
ncclResult_t ncclSocketReady(struct ncclSocket* sock, int* running);
ncclResult_t ncclSocketAccept(struct ncclSocket* sock,
                              struct ncclSocket* listenSock);

#define NCCL_SOCKET_SEND 0
#define NCCL_SOCKET_RECV 1

ncclResult_t ncclSocketProgress(int op, struct ncclSocket* sock, void* ptr,
                                int size, int* offset, int* closed = NULL);
ncclResult_t ncclSocketClose(struct ncclSocket* sock, bool wait = false);

#endif // NCCL_DPDK_SOCKET_COMPAT_H_
