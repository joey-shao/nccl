/*************************************************************************
 * Copyright (c) 2016-2022, NVIDIA CORPORATION. All rights reserved.
 *
 * See LICENSE.txt for license information
 ************************************************************************/

#include "checks.h"
#include "comm.h"
#include "core.h"
#include "ncdp.h"
#include "net.h"
#include "param.h"
#include "socket.h"
#include "utils.h"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <limits.h>
#include <mutex>
#include <string.h>
#include <string>
#include <unistd.h>
#include <vector>

#include <rte_byteorder.h>
#include <rte_cycles.h>
#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_ether.h>
#include <rte_mbuf.h>

#define DPDK_MAX_DEVS MAX_IFS
#define DPDK_MAX_REQUESTS NCCL_NET_MAX_REQUESTS

#define DPDK_CTRL_MAGIC 0x4e43444bU // "NCDK"
#define DPDK_PROTO_VERSION 2

#define DPDK_RX_BURST 32
#define DPDK_TX_BURST 32
#define DPDK_MBUF_COUNT 8192
#define DPDK_MBUF_CACHE 256
#define DPDK_RX_DESC 1024
#define DPDK_TX_DESC 1024

NCCL_PARAM(DpdkInlineSize, "DPDK_INLINE", 0);
NCCL_PARAM(DpdkAckTimeoutUs, "DPDK_ACK_TIMEOUT_US", 200);
NCCL_PARAM(DpdkFrameWindow, "DPDK_FRAME_WINDOW", 64);
NCCL_PARAM(DpdkTaskFrames, "DPDK_TASK_FRAMES", 0);
NCCL_PARAM(DpdkUdpPort, "DPDK_UDP_PORT", 4789);

enum dpdkCtrlType {
  DPDK_CTRL_SEND = 1,
  DPDK_CTRL_READY = 2,
};

typedef struct __attribute__((packed)) dpdkCtrlMsg {
  uint32_t magic;
  uint16_t version;
  uint16_t type;
  uint32_t reqId;
  uint32_t size;
  uint32_t frameSize;
} dpdkCtrlMsg;

typedef struct __attribute__((packed)) dpdkHelloMsg {
  struct rte_ether_addr mac;
  uint32_t commId;
} dpdkHelloMsg;

struct ncclNetDpdkHandle;
struct ncclNetDpdkComm;

enum dpdkFrameState {
  DPDK_FRAME_EMPTY = 0,
  DPDK_FRAME_READY = 1,
  DPDK_FRAME_SENT = 2,
  DPDK_FRAME_DONE = 3,
};

struct dpdkFrameMeta {
  uint16_t len;
  uint8_t state;
  uint8_t reserved;
  uint64_t lastTxTsc;
};

struct dpdkSendTask {
  uint32_t baseSeq;
  int numFrames;
  int frameSize;
  int window;
  int inflight;
  int completedFrames;
  int timeoutScan;
  int done;
  struct dpdkFrameMeta *frames;
  uint32_t *queue;
  int head;
  int tail;
  int count;
};

enum ncclNetDpdkCommState {
  ncclNetDpdkCommStateStart = 0,
  ncclNetDpdkCommStateConnect = 1,
  ncclNetDpdkCommStateHelloSend = 2,
  ncclNetDpdkCommStateAccept = 3,
  ncclNetDpdkCommStateHelloRecv = 4,
};

struct ncclNetDpdkCommStage {
  enum ncclNetDpdkCommState state;
  struct ncclSocket *sock;
  struct ncclNetDpdkComm *comm;
  int offset;
};

struct ncclNetDpdkHandle {
  union ncclSocketAddress connectAddr;
  uint64_t magic;
  struct rte_ether_addr mac;
  uint32_t commId;
  struct ncclNetDpdkCommStage stage;
};

struct ncclNetDpdkRequest {
  int op;
  void *data;
  int size;
  int used;
  std::atomic<int> state;
  uint32_t reqId;
  int ctrlOffset;
  dpdkCtrlMsg ctrlMsg;
  std::atomic<int> doneFlag;
  struct ncclNetDpdkComm *comm;
  int frameSize;
  int numFrames;
  int framesPerTask;
  int numTasks;
  int completedTasks;
  int nextTask;
  struct dpdkSendTask *tasks;
  struct dpdkFrameMeta *frames;
  int recvCompleted;
};

enum ncclNetDpdkReqState {
  DPDK_REQ_UNUSED = 0,
  DPDK_REQ_SEND_CTRL = 1,
  DPDK_REQ_WAIT_READY = 2,
  DPDK_REQ_SENDING = 3,
  DPDK_REQ_RECV_CTRL = 4,
  DPDK_REQ_SEND_READY = 5,
  DPDK_REQ_RECEIVING = 6,
};

struct ncclNetDpdkListenComm {
  struct ncclSocket sock;
  struct ncclNetDpdkCommStage stage;
  int dev;
  uint32_t commId;
};

struct ncclNetDpdkComm {
  struct ncclSocket ctrlSock;
  int dev;
  int cudaDev;
  int portId;
  struct rte_mempool *pool;
  struct rte_ether_addr localMac;
  struct rte_ether_addr remoteMac;
  uint32_t localIp;
  uint32_t remoteIp;
  uint16_t udpPort;
  struct ncdpEndpoint ncdp;
  uint32_t commId;
  uint32_t remoteCommId;
  int maxPayload;
  uint32_t nextReqId;
  struct ncclNetDpdkRequest requests[DPDK_MAX_REQUESTS];
  std::atomic<int> refCount;
  std::atomic<int> closing;
};

static ncclProfilerCallback_t ncclProfilerFunction;

struct ncclNetDpdkDev {
  union ncclSocketAddress addr;
  char devName[MAX_IF_NAME_SIZE];
  char *pciPath;
  int portId;
  struct rte_mempool *pool;
  struct rte_ether_addr mac;
  int mtu;
};

static std::mutex ncclNetDpdkMutex;
static std::mutex ncclNetDpdkCommMutex;
static std::mutex ncclNetDpdkLcoreMutex;
static int ncclNetDpdkIfs = -1;
static ncclNetDpdkDev ncclNetDpdkDevs[DPDK_MAX_DEVS];
static int dpdkRefCount;
static uint64_t dpdkTscHz;
static std::vector<ncclNetDpdkComm *> dpdkComms;
static unsigned dpdkNextLcore = RTE_MAX_LCORE;

struct dpdkPollThread {
  int portId;
  int dev;
  int refCount;
  int stop;
  int useDpdk;
  unsigned lcoreId;
  std::mutex mutex;
  std::vector<struct ncclNetDpdkComm *> comms;
};

static dpdkPollThread dpdkPollThreads[DPDK_MAX_DEVS];

static void dpdkReleaseRequestFrames(struct ncclNetDpdkRequest *r);
static uint16_t dpdkSelectUdpPort(uint32_t dstCommId, uint32_t srcCommId,
                                  uint32_t reqId, uint32_t taskId);

static ncclResult_t ncclNetDpdkGetPciPath(char *devName, char **pciPath) {
  char devicePath[PATH_MAX];
  snprintf(devicePath, PATH_MAX, "/sys/class/net/%s/device", devName);
  *pciPath = realpath(devicePath, NULL);
  return ncclSuccess;
}

static const char *dpdkLastPathComponent(const char *path) {
  const char *last = strrchr(path, '/');
  return last ? last + 1 : path;
}

static ncclResult_t dpdkInitEal() {
  static bool inited = false;
  if (inited)
    return ncclSuccess;

  std::vector<std::string> args;
  args.emplace_back("nccl_dpdk");
  const char *env = getenv("NCCL_DPDK_EAL");
  if (env && env[0]) {
    std::string eal(env);
    size_t pos = 0;
    while (pos < eal.size()) {
      while (pos < eal.size() && eal[pos] == ' ')
        pos++;
      if (pos >= eal.size())
        break;
      size_t start = pos;
      while (pos < eal.size() && eal[pos] != ' ')
        pos++;
      args.emplace_back(eal.substr(start, pos - start));
    }
  } else {
    args.emplace_back("-l");
    args.emplace_back("0");
    args.emplace_back("-n");
    args.emplace_back("4");
    args.emplace_back("--proc-type=auto");
    args.emplace_back("--file-prefix=nccl_dpdk");
  }

  std::vector<char *> argv;
  argv.reserve(args.size());
  for (auto &a : args) {
    char *dup = strdup(a.c_str());
    if (dup == NULL)
      return ncclSystemError;
    argv.push_back(dup);
  }

  int ret = rte_eal_init((int)argv.size(), argv.data());
  for (auto *p : argv)
    free(p);
  if (ret < 0) {
    WARN("NET/DPDK : rte_eal_init failed");
    return ncclSystemError;
  }
  dpdkTscHz = rte_get_tsc_hz();
  inited = true;
  return ncclSuccess;
}

static ncclResult_t dpdkSetupPort(ncclNetDpdkDev *dev) {
  if (dev->portId < 0)
    return ncclInternalError;
  char poolName[64];
  snprintf(poolName, sizeof(poolName), "nccl_dpdk_mbufs_%d", dev->portId);
  dev->pool = rte_pktmbuf_pool_create(
      poolName, DPDK_MBUF_COUNT, DPDK_MBUF_CACHE, 0, RTE_MBUF_DEFAULT_BUF_SIZE,
      rte_eth_dev_socket_id(dev->portId));
  if (dev->pool == NULL) {
    WARN("NET/DPDK : failed to create mempool for port %d", dev->portId);
    return ncclSystemError;
  }

  struct rte_eth_conf portConf;
  memset(&portConf, 0, sizeof(portConf));

  int ret = rte_eth_dev_configure(dev->portId, 1, 1, &portConf);
  if (ret != 0) {
    WARN("NET/DPDK : rte_eth_dev_configure failed for port %d", dev->portId);
    return ncclSystemError;
  }
  ret = rte_eth_rx_queue_setup(dev->portId, 0, DPDK_RX_DESC,
                               rte_eth_dev_socket_id(dev->portId), NULL,
                               dev->pool);
  if (ret != 0) {
    WARN("NET/DPDK : rte_eth_rx_queue_setup failed for port %d", dev->portId);
    return ncclSystemError;
  }
  ret = rte_eth_tx_queue_setup(dev->portId, 0, DPDK_TX_DESC,
                               rte_eth_dev_socket_id(dev->portId), NULL);
  if (ret != 0) {
    WARN("NET/DPDK : rte_eth_tx_queue_setup failed for port %d", dev->portId);
    return ncclSystemError;
  }
  ret = rte_eth_dev_start(dev->portId);
  if (ret != 0) {
    WARN("NET/DPDK : rte_eth_dev_start failed for port %d", dev->portId);
    return ncclSystemError;
  }
  rte_eth_promiscuous_enable(dev->portId);
  rte_eth_macaddr_get(dev->portId, &dev->mac);
  dev->mtu = 1500;
  uint16_t mtu = 0;
  if (rte_eth_dev_get_mtu(dev->portId, &mtu) == 0 && mtu > 0)
    dev->mtu = (int)mtu;
  return ncclSuccess;
}

static ncclResult_t dpdkInitDevices() {
  if (ncclNetDpdkIfs != -1)
    return ncclSuccess;

  char names[MAX_IF_NAME_SIZE * DPDK_MAX_DEVS];
  union ncclSocketAddress addrs[DPDK_MAX_DEVS];
  int nIfs = 0;
  NCCLCHECK(
      ncclFindInterfaces(names, addrs, MAX_IF_NAME_SIZE, DPDK_MAX_DEVS, &nIfs));
  if (nIfs <= 0) {
    WARN("NET/DPDK : no interface found");
    return ncclInternalError;
  }

  int portCount = rte_eth_dev_count_avail();
  if (portCount <= 0) {
    WARN("NET/DPDK : no DPDK ports available");
    return ncclInternalError;
  }

  std::vector<std::string> portNames;
  portNames.resize(portCount);
  for (int port = 0; port < portCount; ++port) {
    char name[RTE_ETH_NAME_MAX_LEN];
    if (rte_eth_dev_get_name_by_port(port, name) == 0) {
      portNames[port] = name;
    } else {
      portNames[port].clear();
    }
  }

  int matched = 0;
  for (int i = 0; i < nIfs; i++) {
    if (addrs[i].sa.sa_family != AF_INET)
      continue;
    char *devName = names + i * MAX_IF_NAME_SIZE;
    char *pciPath = NULL;
    NCCLCHECK(ncclNetDpdkGetPciPath(devName, &pciPath));
    if (pciPath == NULL)
      continue;
    const char *pciName = dpdkLastPathComponent(pciPath);

    int portId = -1;
    for (int p = 0; p < portCount; p++) {
      if (!portNames[p].empty() && portNames[p] == pciName) {
        portId = p;
        break;
      }
    }
    if (portId < 0) {
      free(pciPath);
      continue;
    }

    ncclNetDpdkDev *dev = ncclNetDpdkDevs + matched;
    memset(dev, 0, sizeof(*dev));
    memcpy(&dev->addr, addrs + i, sizeof(union ncclSocketAddress));
    strncpy(dev->devName, devName, MAX_IF_NAME_SIZE - 1);
    dev->devName[MAX_IF_NAME_SIZE - 1] = '\0';
    dev->pciPath = pciPath;
    dev->portId = portId;
    NCCLCHECK(dpdkSetupPort(dev));
    matched++;
    if (matched >= DPDK_MAX_DEVS)
      break;
  }

  ncclNetDpdkIfs = matched;
  if (ncclNetDpdkIfs <= 0) {
    WARN("NET/DPDK : no DPDK-enabled interface matched");
    return ncclInternalError;
  }

  char line[2048];
  char addrline[SOCKET_NAME_MAXLEN + 1];
  line[0] = '\0';
  addrline[SOCKET_NAME_MAXLEN] = '\0';
  for (int i = 0; i < ncclNetDpdkIfs; i++) {
    snprintf(line + strlen(line), sizeof(line) - strlen(line), " [%d]%s:%s", i,
             ncclNetDpdkDevs[i].devName,
             ncclSocketToString(&ncclNetDpdkDevs[i].addr, addrline));
  }
  INFO(NCCL_INIT | NCCL_NET, "NET/DPDK : Using%s", line);
  return ncclSuccess;
}

static ncclNetDpdkComm *dpdkFindComm(uint32_t commId) {
  for (auto *comm : dpdkComms) {
    if (comm->commId == commId)
      return comm;
  }
  return NULL;
}

static void dpdkCommRetain(struct ncclNetDpdkComm *comm) {
  comm->refCount.fetch_add(1, std::memory_order_relaxed);
}

static void dpdkCommRelease(struct ncclNetDpdkComm *comm) {
  if (comm->refCount.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    for (int i = 0; i < DPDK_MAX_REQUESTS; i++) {
      dpdkReleaseRequestFrames(comm->requests + i);
    }
    delete comm;
  }
}

static void dpdkInitComm(struct ncclNetDpdkComm *comm) {
  // comm is value-initialized by new ncclNetDpdkComm(), so only set non-zero
  // defaults.
  comm->dev = -1;
  comm->cudaDev = -1;
  comm->portId = -1;
  comm->refCount.store(1, std::memory_order_relaxed);
}

static void dpdkUpdateNcdp(struct ncclNetDpdkComm *comm) {
  comm->ncdp.portId = comm->portId;
  comm->ncdp.pool = comm->pool;
  comm->ncdp.localMac = comm->localMac;
  comm->ncdp.remoteMac = comm->remoteMac;
  comm->ncdp.localIp = comm->localIp;
  comm->ncdp.remoteIp = comm->remoteIp;
  comm->ncdp.udpPort = comm->udpPort;
  comm->ncdp.maxPayload = comm->maxPayload;
}

static bool dpdkTrySendFrame(struct ncclNetDpdkComm *comm, uint16_t flags,
                             uint32_t dstCommId, uint32_t srcCommId,
                             uint32_t reqId, uint32_t taskId, uint32_t seq,
                             const void *payload, uint16_t len) {
  uint16_t udpPort = dpdkSelectUdpPort(dstCommId, srcCommId, reqId, taskId);
  return ncdpTrySendFrame(&comm->ncdp, flags, dstCommId, srcCommId, reqId,
                          taskId, seq, payload, len, udpPort);
}

static void dpdkReleaseRequestFrames(struct ncclNetDpdkRequest *r) {
  if (r->tasks) {
    for (int i = 0; i < r->numTasks; i++) {
      if (r->tasks[i].frames)
        free(r->tasks[i].frames);
      if (r->tasks[i].queue)
        free(r->tasks[i].queue);
    }
    free(r->tasks);
    r->tasks = NULL;
  }
  if (r->frames) {
    free(r->frames);
    r->frames = NULL;
  }
  r->frameSize = 0;
  r->numFrames = 0;
  r->framesPerTask = 0;
  r->numTasks = 0;
  r->completedTasks = 0;
  r->nextTask = 0;
  r->recvCompleted = 0;
}

static inline struct ncclNetDpdkRequest *
dpdkFindRequestByReqId(struct ncclNetDpdkComm *comm, uint32_t reqId,
                       enum ncclNetDpdkReqState expectState) {
  for (int i = 0; i < DPDK_MAX_REQUESTS; i++) {
    struct ncclNetDpdkRequest *r = comm->requests + i;
    if (r->state.load(std::memory_order_acquire) == expectState &&
        r->reqId == reqId) {
      return r;
    }
  }
  return NULL;
}

static int dpdkSelectFrameSize(struct ncclNetDpdkComm *comm, int size) {
  int frameSize = comm->maxPayload;
  if (frameSize <= 0)
    frameSize = 1;
  if (size > 0 && frameSize > size)
    frameSize = size;
  return frameSize;
}

static uint32_t dpdkHash32(uint32_t x) {
  x ^= x >> 16;
  x *= 0x7feb352dU;
  x ^= x >> 15;
  x *= 0x846ca68bU;
  x ^= x >> 16;
  return x;
}

static uint16_t dpdkSelectUdpPort(uint32_t dstCommId, uint32_t srcCommId,
                                  uint32_t reqId, uint32_t taskId) {
  uint32_t seed = dstCommId ^ srcCommId ^ reqId ^ (taskId * 0x9e3779b9U);
  uint32_t base = (uint32_t)ncclParamDpdkUdpPort();
  if (base < 1024 || base > 65535)
    base = 1024;
  uint32_t range = 65535 - base + 1;
  uint32_t hash = dpdkHash32(seed);
  return (uint16_t)(base + (hash % range));
}

static ncclResult_t dpdkInitSendTasks(struct ncclNetDpdkRequest *r,
                                      int frameSize, int size) {
  dpdkReleaseRequestFrames(r);
  if (size <= 0 || frameSize <= 0) {
    r->frameSize = frameSize;
    r->numFrames = 0;
    return ncclSuccess;
  }
  r->frameSize = frameSize;
  r->numFrames = (size + frameSize - 1) / frameSize;
  int framesPerTask = ncclParamDpdkTaskFrames();
  if (framesPerTask <= 0)
    framesPerTask = ncclParamDpdkFrameWindow();
  if (framesPerTask <= 0)
    framesPerTask = DPDK_TX_BURST;
  if (framesPerTask > r->numFrames)
    framesPerTask = r->numFrames;
  if (framesPerTask <= 0)
    framesPerTask = 1;
  r->framesPerTask = framesPerTask;
  r->numTasks = (r->numFrames + framesPerTask - 1) / framesPerTask;
  if (r->numTasks > 0xFFFF) {
    dpdkReleaseRequestFrames(r);
    return ncclSystemError;
  }
  NCCLCHECK(ncclCalloc(&r->tasks, r->numTasks));
  int window = ncclParamDpdkFrameWindow();
  if (window <= 0)
    window = DPDK_TX_BURST;
  for (int t = 0; t < r->numTasks; t++) {
    struct dpdkSendTask *task = r->tasks + t;
    task->baseSeq = (uint32_t)(t * framesPerTask);
    int remainingFrames = r->numFrames - (int)task->baseSeq;
    task->numFrames = std::min(framesPerTask, remainingFrames);
    task->frameSize = frameSize;
    task->window = std::min(window, task->numFrames);
    task->inflight = 0;
    task->completedFrames = 0;
    task->timeoutScan = 0;
    task->done = 0;
    NCCLCHECK(ncclCalloc(&task->frames, task->numFrames));
    NCCLCHECK(ncclCalloc(&task->queue, task->numFrames));
    for (int i = 0; i < task->numFrames; i++) {
      uint32_t seq = task->baseSeq + (uint32_t)i;
      int remaining = size - (int)(seq * (uint32_t)frameSize);
      int len = std::min(frameSize, remaining);
      task->frames[i].len = (uint16_t)len;
      task->frames[i].state = DPDK_FRAME_READY;
      task->frames[i].lastTxTsc = 0;
      task->queue[i] = (uint32_t)i;
    }
    task->head = 0;
    task->tail = 0;
    task->count = task->numFrames;
  }
  r->completedTasks = 0;
  r->nextTask = 0;
  return ncclSuccess;
}

static ncclResult_t dpdkInitRecvFrames(struct ncclNetDpdkRequest *r,
                                       int frameSize, int size) {
  dpdkReleaseRequestFrames(r);
  if (size <= 0 || frameSize <= 0) {
    r->frameSize = frameSize;
    r->numFrames = 0;
    return ncclSuccess;
  }
  r->frameSize = frameSize;
  r->numFrames = (size + frameSize - 1) / frameSize;
  NCCLCHECK(ncclCalloc(&r->frames, r->numFrames));
  for (int i = 0; i < r->numFrames; i++) {
    int remaining = size - i * frameSize;
    int len = std::min(frameSize, remaining);
    r->frames[i].len = (uint16_t)len;
    r->frames[i].state = DPDK_FRAME_EMPTY;
    r->frames[i].lastTxTsc = 0;
  }
  r->recvCompleted = 0;
  return ncclSuccess;
}

static inline void dpdkEnqueueTask(struct dpdkSendTask *task,
                                   uint32_t localSeq) {
  if (!task->queue || task->numFrames <= 0)
    return;
  if (task->count >= task->numFrames)
    return;
  task->queue[task->tail] = localSeq;
  task->tail = (task->tail + 1) % task->numFrames;
  task->count++;
}

static inline bool dpdkDequeueTask(struct dpdkSendTask *task,
                                   uint32_t *localSeq) {
  if (!task->queue || task->count <= 0)
    return false;
  *localSeq = task->queue[task->head];
  task->head = (task->head + 1) % task->numFrames;
  task->count--;
  return true;
}

static void dpdkHandleRx(void * /*ctx*/, const ncdpHdr *hdr,
                         const uint8_t *payload, uint16_t len) {
  std::lock_guard<std::mutex> commLock(ncclNetDpdkCommMutex);
  struct ncclNetDpdkComm *comm = dpdkFindComm(hdr->dstCommId);
  if (!comm || comm->closing.load(std::memory_order_acquire))
    return;

  if (hdr->flags & NCDP_FLAG_ACK) {
    struct ncclNetDpdkRequest *req =
        dpdkFindRequestByReqId(comm, hdr->reqId, DPDK_REQ_SENDING);
    if (req && req->reqId == hdr->reqId && req->tasks && req->numTasks > 0) {
      uint32_t seq = hdr->seq;
      if (seq < (uint32_t)req->numFrames) {
        uint32_t taskId = hdr->taskId;
        int taskIndex = (taskId < (uint32_t)req->numTasks) ? (int)taskId : -1;
        if (taskIndex < 0 && req->framesPerTask > 0) {
          taskIndex = (int)(seq / (uint32_t)req->framesPerTask);
        }
        if (taskIndex >= 0 && taskIndex < req->numTasks) {
          struct dpdkSendTask *task = req->tasks + taskIndex;
          uint32_t localSeq = seq - task->baseSeq;
          if (localSeq < (uint32_t)task->numFrames) {
            struct dpdkFrameMeta *meta = task->frames + localSeq;
            if (meta->state != DPDK_FRAME_DONE) {
              meta->state = DPDK_FRAME_DONE;
              if (meta->lastTxTsc != 0 && task->inflight > 0)
                task->inflight--;
              task->completedFrames++;
              if (task->completedFrames >= task->numFrames && !task->done) {
                task->done = 1;
                req->completedTasks++;
                if (req->completedTasks >= req->numTasks) {
                  req->doneFlag.store(1, std::memory_order_release);
                  dpdkReleaseRequestFrames(req);
                }
              }
            }
          }
        }
      }
    }
    return;
  }

  if (hdr->flags & NCDP_FLAG_DATA) {
    struct ncclNetDpdkRequest *req =
        dpdkFindRequestByReqId(comm, hdr->reqId, DPDK_REQ_RECEIVING);
    if (req && req->reqId == hdr->reqId && req->frames && req->numFrames > 0) {
      uint32_t seq = hdr->seq;
      if (seq < (uint32_t)req->numFrames) {
        int offset =
            req->frameSize > 0 ? (int)(seq * (uint32_t)req->frameSize) : 0;
        int copyLen = std::min((int)len, req->size - offset);
        struct dpdkFrameMeta *meta = req->frames + seq;
        if (copyLen > (int)meta->len)
          copyLen = meta->len;
        if (meta->state != DPDK_FRAME_DONE) {
          if (copyLen > 0)
            memcpy((char *)req->data + offset, payload, copyLen);
          meta->state = DPDK_FRAME_DONE;
          req->recvCompleted++;
          if (req->recvCompleted >= req->numFrames) {
            req->doneFlag.store(1, std::memory_order_release);
            dpdkReleaseRequestFrames(req);
          }
        }
        (void)dpdkTrySendFrame(comm, NCDP_FLAG_ACK, hdr->srcCommId,
                               hdr->dstCommId, hdr->reqId, hdr->taskId, seq,
                               NULL, 0);
        return;
      }
    }
  }
}

static int dpdkPollRx(int portId) {
  return ncdpPollRx(portId, 0, dpdkHandleRx, NULL);
}

static int dpdkProgressTask(struct ncclNetDpdkComm *comm,
                            struct ncclNetDpdkRequest *req,
                            struct dpdkSendTask *task, int taskId, int budget) {
  int sent = 0;
  while (budget > 0) {
    uint32_t localSeq = 0;
    if (!dpdkDequeueTask(task, &localSeq))
      break;
    if (localSeq >= (uint32_t)task->numFrames)
      continue;
    struct dpdkFrameMeta *meta = task->frames + localSeq;
    if (meta->state == DPDK_FRAME_DONE)
      continue;
    bool wasSent = meta->lastTxTsc != 0;
    if (!wasSent && task->inflight >= task->window) {
      dpdkEnqueueTask(task, localSeq);
      continue;
    }
    uint32_t seq = task->baseSeq + localSeq;
    uint32_t offset = (uint32_t)(seq * (uint32_t)req->frameSize);
    if (!dpdkTrySendFrame(comm, NCDP_FLAG_DATA, comm->remoteCommId,
                          comm->commId, req->reqId, (uint32_t)taskId, seq,
                          (char *)req->data + offset, meta->len)) {
      dpdkEnqueueTask(task, localSeq);
      break;
    }
    if (!wasSent)
      task->inflight++;
    meta->state = DPDK_FRAME_SENT;
    meta->lastTxTsc = rte_get_tsc_cycles();
    sent++;
    budget--;
  }
  return sent;
}

static void dpdkProgressTaskTimeout(struct dpdkSendTask *task,
                                    int64_t timeoutUs) {
  if (task->numFrames <= 0 || task->frames == NULL)
    return;
  if (timeoutUs <= 0 || dpdkTscHz == 0 || task->inflight == 0)
    return;
  uint64_t now = rte_get_tsc_cycles();
  int scanCount = std::min(task->numFrames, DPDK_TX_BURST);
  for (int i = 0; i < scanCount; i++) {
    int localSeq = task->timeoutScan;
    task->timeoutScan = (localSeq + 1) % task->numFrames;
    struct dpdkFrameMeta *meta = task->frames + localSeq;
    if (meta->state != DPDK_FRAME_SENT)
      continue;
    uint64_t deltaUs = (now - meta->lastTxTsc) * 1000000ULL / dpdkTscHz;
    if (deltaUs <= (uint64_t)timeoutUs)
      continue;
    meta->state = DPDK_FRAME_READY;
    dpdkEnqueueTask(task, (uint32_t)localSeq);
  }
}

static void dpdkProgressSend(struct ncclNetDpdkComm *comm,
                             struct ncclNetDpdkRequest *req) {
  if (req->doneFlag.load(std::memory_order_acquire))
    return;
  if (req->numFrames <= 0) {
    req->doneFlag.store(1, std::memory_order_release);
    return;
  }
  if (req->tasks == NULL || req->numTasks <= 0)
    return;
  if (req->frameSize <= 0)
    return;

  int budget = DPDK_TX_BURST;
  int tasksScanned = 0;
  int numTasks = req->numTasks;
  int64_t timeoutUs = ncclParamDpdkAckTimeoutUs();
  while (budget > 0 && tasksScanned < numTasks) {
    int idx = req->nextTask;
    req->nextTask = (idx + 1) % numTasks;
    tasksScanned++;
    struct dpdkSendTask *task = req->tasks + idx;
    if (task->done) {
      dpdkProgressTaskTimeout(task, timeoutUs);
      continue;
    }
    budget -= dpdkProgressTask(comm, req, task, idx, budget);
    dpdkProgressTaskTimeout(task, timeoutUs);
  }
}

static int dpdkPollThreadMain(void *arg) {
  dpdkPollThread *thread = (dpdkPollThread *)arg;
  while (!thread->stop) {
    int got = dpdkPollRx(thread->portId);
    // Progress TX for comms on this port.
    std::vector<struct ncclNetDpdkComm *> comms;
    {
      std::lock_guard<std::mutex> lock(thread->mutex);
      for (auto *comm : thread->comms) {
        if (comm->closing.load(std::memory_order_acquire))
          continue;
        dpdkCommRetain(comm);
        comms.push_back(comm);
      }
    }
    for (auto *comm : comms) {
      for (int i = 0; i < DPDK_MAX_REQUESTS; i++) {
        struct ncclNetDpdkRequest *req = comm->requests + i;
        if (req->state.load(std::memory_order_acquire) == DPDK_REQ_SENDING) {
          dpdkProgressSend(comm, req);
        }
      }
      dpdkCommRelease(comm);
    }
    if (got == 0)
      rte_pause();
  }
  return 0;
}

static ncclResult_t dpdkAcquirePollThread(int dev) {
  dpdkPollThread *thread = &dpdkPollThreads[dev];
  std::lock_guard<std::mutex> lock(thread->mutex);
  if (thread->refCount++ > 0)
    return ncclSuccess;
  thread->portId = ncclNetDpdkDevs[dev].portId;
  thread->dev = dev;
  thread->stop = 0;
  thread->useDpdk = 0;
  thread->lcoreId = RTE_MAX_LCORE;
  if (rte_lcore_count() <= 1) {
    WARN("NET/DPDK : no available DPDK lcore for poll thread");
    thread->refCount--;
    return ncclSystemError;
  }
  unsigned lcore = RTE_MAX_LCORE;
  {
    std::lock_guard<std::mutex> lcoreLock(ncclNetDpdkLcoreMutex);
    if (dpdkNextLcore == RTE_MAX_LCORE)
      dpdkNextLcore = rte_lcore_id();
    lcore = rte_get_next_lcore(dpdkNextLcore, 1, 1);
    if (lcore == RTE_MAX_LCORE || lcore == rte_lcore_id()) {
      WARN("NET/DPDK : failed to select DPDK lcore for poll thread");
      thread->refCount--;
      return ncclSystemError;
    }
    if (rte_eal_remote_launch(dpdkPollThreadMain, thread, lcore) != 0) {
      WARN("NET/DPDK : rte_eal_remote_launch failed for lcore %u", lcore);
      thread->refCount--;
      return ncclSystemError;
    }
    dpdkNextLcore = lcore;
  }
  thread->useDpdk = 1;
  thread->lcoreId = lcore;
  return ncclSuccess;
}

static void dpdkAddCommToPollThread(struct ncclNetDpdkComm *comm) {
  dpdkPollThread *thread = &dpdkPollThreads[comm->dev];
  std::lock_guard<std::mutex> lock(thread->mutex);
  thread->comms.push_back(comm);
  dpdkCommRetain(comm);
}

static void dpdkRemoveCommFromPollThread(struct ncclNetDpdkComm *comm) {
  dpdkPollThread *thread = &dpdkPollThreads[comm->dev];
  std::lock_guard<std::mutex> lock(thread->mutex);
  auto &v = thread->comms;
  for (size_t i = 0; i < v.size(); ++i) {
    if (v[i] == comm) {
      v[i] = v.back();
      v.pop_back();
      break;
    }
  }
  dpdkCommRelease(comm);
}

static ncclResult_t dpdkRegisterComm(struct ncclNetDpdkComm *comm) {
  std::lock_guard<std::mutex> lock(ncclNetDpdkCommMutex);
  dpdkComms.push_back(comm);
  dpdkAddCommToPollThread(comm);
  return ncclSuccess;
}

static ncclResult_t dpdkUnregisterComm(struct ncclNetDpdkComm *comm) {
  std::lock_guard<std::mutex> lock(ncclNetDpdkCommMutex);
  auto it = std::find(dpdkComms.begin(), dpdkComms.end(), comm);
  if (it != dpdkComms.end())
    dpdkComms.erase(it);
  dpdkRemoveCommFromPollThread(comm);
  return ncclSuccess;
}

static ncclResult_t dpdkReleasePollThread(int dev) {
  dpdkPollThread *thread = &dpdkPollThreads[dev];
  unsigned lcore = RTE_MAX_LCORE;
  int useDpdk = 0;
  {
    std::lock_guard<std::mutex> lock(thread->mutex);
    if (--thread->refCount > 0)
      return ncclSuccess;
    thread->stop = 1;
    useDpdk = thread->useDpdk;
    lcore = thread->lcoreId;
  }
  if (useDpdk && lcore != RTE_MAX_LCORE) {
    rte_eal_wait_lcore(lcore);
  } else {
    WARN("NET/DPDK : poll thread not running on DPDK lcore");
    return ncclSystemError;
  }
  {
    std::lock_guard<std::mutex> lock(thread->mutex);
    thread->useDpdk = 0;
    thread->lcoreId = RTE_MAX_LCORE;
    for (auto *comm : thread->comms)
      dpdkCommRelease(comm);
    thread->comms.clear();
  }
  return ncclSuccess;
}

static ncclResult_t ncclNetDpdkGetRequest(struct ncclNetDpdkComm *comm, int op,
                                          void *data, int size,
                                          struct ncclNetDpdkRequest **req) {
  for (int i = 0; i < DPDK_MAX_REQUESTS; i++) {
    struct ncclNetDpdkRequest *r = comm->requests + i;
    if (r->used == 0) {
      r->op = op;
      r->data = data;
      r->size = size;
      dpdkReleaseRequestFrames(r);
      r->used = 1;
      r->ctrlOffset = 0;
      memset(&r->ctrlMsg, 0, sizeof(r->ctrlMsg));
      r->doneFlag.store(0, std::memory_order_relaxed);
      r->comm = comm;
      r->reqId = (op == NCCL_SOCKET_SEND) ? ++comm->nextReqId : 0;
      r->frameSize =
          (op == NCCL_SOCKET_SEND) ? dpdkSelectFrameSize(comm, size) : 0;
      r->state.store((op == NCCL_SOCKET_SEND) ? DPDK_REQ_SEND_CTRL
                                              : DPDK_REQ_RECV_CTRL,
                     std::memory_order_release);
      *req = r;
      return ncclSuccess;
    }
  }
  WARN("NET/DPDK : unable to allocate requests");
  return ncclInternalError;
}

ncclResult_t ncclNetDpdkInit(void **ctx, uint64_t commId,
                             ncclNetCommConfig_t *config,
                             ncclDebugLogger_t logFunction,
                             ncclProfilerCallback_t profFunction) {
  if (dpdkRefCount++)
    return ncclSuccess;
  ncclProfilerFunction = profFunction;
  std::lock_guard<std::mutex> lock(ncclNetDpdkMutex);
  ncclResult_t ret = dpdkInitEal();
  if (ret != ncclSuccess) {
    dpdkRefCount--;
    return ret;
  }
  ret = dpdkInitDevices();
  if (ret != ncclSuccess) {
    dpdkRefCount--;
    return ret;
  }
  return ret;
}

ncclResult_t ncclNetDpdkDevices(int *ndev) {
  *ndev = ncclNetDpdkIfs;
  return ncclSuccess;
}

static ncclResult_t ncclNetDpdkGetSpeed(char *devName, int *speed) {
  ncclResult_t ret = ncclSuccess;
  *speed = 0;
  char speedPath[PATH_MAX];
  snprintf(speedPath, sizeof(speedPath), "/sys/class/net/%s/speed", devName);
  int fd = -1;
  SYSCHECKSYNC(open(speedPath, O_RDONLY), "open", fd);
  if (fd != -1) {
    char speedStr[] = "        ";
    int n = read(fd, speedStr, sizeof(speedStr) - 1);
    if (n > 0)
      *speed = strtol(speedStr, NULL, 0);
  }
  if (*speed <= 0) {
    INFO(NCCL_NET, "Could not get speed from %s. Defaulting to 10 Gbps.",
         speedPath);
    *speed = 10000;
  }
  if (fd != -1)
    SYSCHECK(close(fd), "close");
  return ret;
}

ncclResult_t ncclNetDpdkGetProperties(int dev, ncclNetProperties_t *props) {
  props->name = ncclNetDpdkDevs[dev].devName;
  props->pciPath = ncclNetDpdkDevs[dev].pciPath;
  props->guid = dev;
  props->ptrSupport = NCCL_PTR_HOST;
  props->regIsGlobal = 0;
  props->forceFlush = 0;
  NCCLCHECK(ncclNetDpdkGetSpeed(props->name, &props->speed));
  props->latency = 0;
  props->port = 0;
  props->maxComms = 65536;
  props->maxRecvs = 1;
  props->netDeviceType = NCCL_NET_DEVICE_HOST;
  props->netDeviceVersion = NCCL_NET_DEVICE_INVALID_VERSION;
  props->maxP2pBytes = NCCL_MAX_NET_SIZE_BYTES;
  props->maxCollBytes = MAX_COLLNET_SIZE;
  props->maxMultiRequestSize = 1;
  return ncclSuccess;
}

ncclResult_t ncclNetDpdkListen(void *ctx, int dev, void *opaqueHandle,
                               void **listenComm) {
  if (dev < 0 || dev >= ncclNetDpdkIfs)
    return ncclInternalError;
  ncclResult_t ret = ncclSuccess;
  struct ncclNetDpdkHandle *handle = (struct ncclNetDpdkHandle *)opaqueHandle;
  memset(handle, 0, sizeof(struct ncclNetDpdkHandle));
  static_assert(sizeof(struct ncclNetDpdkHandle) <= NCCL_NET_HANDLE_MAXSIZE,
                "ncclNetDpdkHandle size too large");

  struct ncclNetDpdkListenComm *comm;
  NCCLCHECK(ncclCalloc(&comm, 1));

  NCCLCHECKGOTO(getRandomData(&handle->magic, sizeof(handle->magic)), ret,
                fail);
  NCCLCHECKGOTO(getRandomData(&handle->commId, sizeof(handle->commId)), ret,
                fail);
  handle->commId |= 1; // avoid zero
  handle->stage.state = ncclNetDpdkCommStateStart;

  NCCLCHECKGOTO(ncclSocketInit(&comm->sock, &ncclNetDpdkDevs[dev].addr,
                               handle->magic, ncclSocketTypeNetSocket, NULL, 1),
                ret, fail);
  NCCLCHECKGOTO(ncclSocketListen(&comm->sock), ret, fail);
  NCCLCHECKGOTO(ncclSocketGetAddr(&comm->sock, &handle->connectAddr), ret,
                fail);

  rte_ether_addr_copy(&ncclNetDpdkDevs[dev].mac, &handle->mac);
  comm->dev = dev;
  comm->commId = handle->commId;
  *listenComm = comm;
  return ncclSuccess;
fail:
  (void)ncclSocketClose(&comm->sock);
  free(comm);
  return ret;
}

ncclResult_t ncclNetDpdkConnect(void *ctx, int dev, void *opaqueHandle,
                                void **sendComm,
                                ncclNetDeviceHandle_t ** /*sendDevComm*/) {
  if (dev < 0 || dev >= ncclNetDpdkIfs)
    return ncclInternalError;

  int ready;
  struct ncclNetDpdkHandle *handle = (struct ncclNetDpdkHandle *)opaqueHandle;
  struct ncclNetDpdkCommStage *stage = &handle->stage;
  struct ncclNetDpdkComm *comm = stage->comm;

  *sendComm = NULL;
  if (stage->state == ncclNetDpdkCommStateConnect)
    goto dpdk_connect_check;
  if (stage->state == ncclNetDpdkCommStateHelloSend)
    goto dpdk_hello_send;

  comm = new ncclNetDpdkComm();
  dpdkInitComm(comm);
  stage->comm = comm;

  comm->dev = dev;
  comm->portId = ncclNetDpdkDevs[dev].portId;
  comm->pool = ncclNetDpdkDevs[dev].pool;
  comm->remoteCommId = handle->commId;
  if (getRandomData(&comm->commId, sizeof(comm->commId)) != ncclSuccess) {
    delete comm;
    stage->comm = NULL;
    return ncclSystemError;
  }
  comm->commId |= 1; // avoid zero
  if (comm->commId == comm->remoteCommId)
    comm->commId ^= 0x1u;
  comm->maxPayload = ncdpComputeMaxPayload(ncclNetDpdkDevs[dev].mtu);
  CUDACHECK(cudaGetDevice(&comm->cudaDev));
  comm->udpPort = (uint16_t)ncclParamDpdkUdpPort();

  if (ncclNetDpdkDevs[dev].addr.sa.sa_family != AF_INET) {
    WARN("NET/DPDK : IPv6 is not supported for data plane");
    delete comm;
    stage->comm = NULL;
    return ncclInternalError;
  }
  comm->localIp = ncclNetDpdkDevs[dev].addr.sin.sin_addr.s_addr;
  if (handle->connectAddr.sa.sa_family != AF_INET) {
    WARN("NET/DPDK : IPv6 peer is not supported for data plane");
    delete comm;
    stage->comm = NULL;
    return ncclInternalError;
  }
  comm->remoteIp = handle->connectAddr.sin.sin_addr.s_addr;

  rte_ether_addr_copy(&ncclNetDpdkDevs[dev].mac, &comm->localMac);
  rte_ether_addr_copy(&handle->mac, &comm->remoteMac);
  dpdkUpdateNcdp(comm);
  NCCLCHECK(dpdkAcquirePollThread(dev));

  NCCLCHECK(ncclSocketInit(&comm->ctrlSock, &handle->connectAddr, handle->magic,
                           ncclSocketTypeNetSocket, NULL, 1));
  stage->sock = &comm->ctrlSock;
  stage->state = ncclNetDpdkCommStateConnect;
  stage->offset = 0;
  NCCLCHECK(ncclSocketConnect(&comm->ctrlSock));

dpdk_connect_check:
  NCCLCHECK(ncclSocketReady(stage->sock, &ready));
  if (!ready)
    return ncclSuccess;
  stage->state = ncclNetDpdkCommStateHelloSend;
  stage->offset = 0;

dpdk_hello_send: {
  dpdkHelloMsg hello;
  hello.mac = comm->localMac;
  hello.commId = comm->commId;
  NCCLCHECK(ncclSocketProgress(NCCL_SOCKET_SEND, stage->sock, &hello,
                               sizeof(hello), &stage->offset));
  if (stage->offset < (int)sizeof(hello))
    return ncclSuccess;
}

  NCCLCHECK(dpdkRegisterComm(comm));
  *sendComm = comm;
  return ncclSuccess;
}

ncclResult_t ncclNetDpdkAccept(void *listenComm, void **recvComm,
                               ncclNetDeviceHandle_t ** /*recvDevComm*/) {
  struct ncclNetDpdkListenComm *lComm =
      (struct ncclNetDpdkListenComm *)listenComm;
  struct ncclNetDpdkCommStage *stage = &lComm->stage;
  struct ncclNetDpdkComm *rComm = stage->comm;
  struct ncclSocket *sock = stage->sock;
  int ready;

  *recvComm = NULL;
  if (stage->state == ncclNetDpdkCommStateAccept)
    goto dpdk_accept_check;
  if (stage->state == ncclNetDpdkCommStateHelloRecv)
    goto dpdk_hello_recv;

  rComm = new ncclNetDpdkComm();
  dpdkInitComm(rComm);
  stage->comm = rComm;
  rComm->dev = lComm->dev;
  rComm->portId = ncclNetDpdkDevs[rComm->dev].portId;
  rComm->pool = ncclNetDpdkDevs[rComm->dev].pool;
  rComm->commId = lComm->commId;
  rComm->maxPayload = ncdpComputeMaxPayload(ncclNetDpdkDevs[rComm->dev].mtu);
  CUDACHECK(cudaGetDevice(&rComm->cudaDev));
  rComm->udpPort = (uint16_t)ncclParamDpdkUdpPort();

  if (ncclNetDpdkDevs[rComm->dev].addr.sa.sa_family != AF_INET) {
    WARN("NET/DPDK : IPv6 is not supported for data plane");
    delete rComm;
    stage->comm = NULL;
    return ncclInternalError;
  }
  rComm->localIp = ncclNetDpdkDevs[rComm->dev].addr.sin.sin_addr.s_addr;
  rte_ether_addr_copy(&ncclNetDpdkDevs[rComm->dev].mac, &rComm->localMac);
  NCCLCHECK(dpdkAcquirePollThread(rComm->dev));

  NCCLCHECK(ncclCalloc(&sock, 1));
  NCCLCHECK(ncclSocketInit(sock));
  stage->sock = sock;
  stage->state = ncclNetDpdkCommStateAccept;
  stage->offset = 0;
  NCCLCHECK(ncclSocketAccept(sock, &lComm->sock));

dpdk_accept_check:
  NCCLCHECK(ncclSocketReady(stage->sock, &ready));
  if (!ready)
    return ncclSuccess;
  stage->state = ncclNetDpdkCommStateHelloRecv;
  stage->offset = 0;

dpdk_hello_recv: {
  dpdkHelloMsg hello;
  NCCLCHECK(ncclSocketProgress(NCCL_SOCKET_RECV, stage->sock, &hello,
                               sizeof(hello), &stage->offset));
  if (stage->offset < (int)sizeof(hello))
    return ncclSuccess;
  rComm->remoteMac = hello.mac;
  rComm->remoteCommId = hello.commId;
  rComm->ctrlSock = *sock;
  free(sock);
  union ncclSocketAddress peerAddr;
  NCCLCHECK(ncclSocketGetAddr(&rComm->ctrlSock, &peerAddr));
  if (peerAddr.sa.sa_family != AF_INET) {
    WARN("NET/DPDK : IPv6 peer is not supported for data plane");
    (void)dpdkReleasePollThread(rComm->dev);
    (void)ncclSocketClose(&rComm->ctrlSock);
    delete rComm;
    stage->state = ncclNetDpdkCommStateStart;
    stage->sock = NULL;
    stage->comm = NULL;
    stage->offset = 0;
    return ncclInternalError;
  }
  rComm->remoteIp = peerAddr.sin.sin_addr.s_addr;
  dpdkUpdateNcdp(rComm);
}

  NCCLCHECK(dpdkRegisterComm(rComm));
  *recvComm = rComm;

  stage->state = ncclNetDpdkCommStateStart;
  stage->sock = NULL;
  stage->comm = NULL;
  stage->offset = 0;
  return ncclSuccess;
}

ncclResult_t ncclNetDpdkRegMr(void *comm, void *data, size_t size, int type,
                              void **mhandle) {
  return (type != NCCL_PTR_HOST) ? ncclInternalError : ncclSuccess;
}

ncclResult_t ncclNetDpdkDeregMr(void *comm, void *mhandle) {
  return ncclSuccess;
}

ncclResult_t ncclNetDpdkIsend(void *sendComm, void *data, size_t size, int tag,
                              void *mhandle, void *phandle, void **request) {
  struct ncclNetDpdkComm *comm = (struct ncclNetDpdkComm *)sendComm;
  NCCLCHECK(ncclNetDpdkGetRequest(comm, NCCL_SOCKET_SEND, data, (int)size,
                                  (struct ncclNetDpdkRequest **)request));
  return ncclSuccess;
}

ncclResult_t ncclNetDpdkIrecv(void *recvComm, int n, void **data, size_t *sizes,
                              int *tags, void **mhandles, void **phandles,
                              void **request) {
  struct ncclNetDpdkComm *comm = (struct ncclNetDpdkComm *)recvComm;
  if (n != 1)
    return ncclInternalError;
  NCCLCHECK(ncclNetDpdkGetRequest(comm, NCCL_SOCKET_RECV, data[0],
                                  (int)sizes[0],
                                  (struct ncclNetDpdkRequest **)request));
  return ncclSuccess;
}

ncclResult_t ncclNetDpdkIflush(void *recvComm, int n, void **data, int *sizes,
                               void **mhandles, void **request) {
  return ncclInternalError;
}

ncclResult_t ncclNetDpdkTest(void *request, int *done, int *size) {
  *done = 0;
  struct ncclNetDpdkRequest *r = (struct ncclNetDpdkRequest *)request;
  if (r == NULL) {
    WARN("NET/DPDK : test called with NULL request");
    return ncclInternalError;
  }
  struct ncclNetDpdkComm *comm = r->comm;

  if (r->op == NCCL_SOCKET_SEND) {
    int state = r->state.load(std::memory_order_acquire);
    if (state == DPDK_REQ_SEND_CTRL) {
      if (r->ctrlOffset == 0) {
        r->ctrlMsg.magic = DPDK_CTRL_MAGIC;
        r->ctrlMsg.version = DPDK_PROTO_VERSION;
        r->ctrlMsg.type = DPDK_CTRL_SEND;
        r->ctrlMsg.reqId = r->reqId;
        r->ctrlMsg.size = r->size;
        r->ctrlMsg.frameSize = (uint32_t)r->frameSize;
      }
      NCCLCHECK(ncclSocketProgress(NCCL_SOCKET_SEND, &comm->ctrlSock,
                                   &r->ctrlMsg, sizeof(r->ctrlMsg),
                                   &r->ctrlOffset));
      if (r->ctrlOffset < (int)sizeof(r->ctrlMsg))
        return ncclSuccess;
      r->state.store(DPDK_REQ_WAIT_READY, std::memory_order_release);
      r->ctrlOffset = 0;
      state = DPDK_REQ_WAIT_READY;
    }
    if (state == DPDK_REQ_WAIT_READY) {
      NCCLCHECK(ncclSocketProgress(NCCL_SOCKET_RECV, &comm->ctrlSock,
                                   &r->ctrlMsg, sizeof(r->ctrlMsg),
                                   &r->ctrlOffset));
      if (r->ctrlOffset < (int)sizeof(r->ctrlMsg))
        return ncclSuccess;
      if (r->ctrlMsg.magic != DPDK_CTRL_MAGIC ||
          r->ctrlMsg.type != DPDK_CTRL_READY || r->ctrlMsg.reqId != r->reqId) {
        WARN("NET/DPDK : invalid ctrl READY");
        return ncclInvalidUsage;
      }
      NCCLCHECK(dpdkInitSendTasks(r, r->frameSize, r->size));
      r->doneFlag.store(0, std::memory_order_relaxed);
      r->state.store(DPDK_REQ_SENDING, std::memory_order_release);
      state = DPDK_REQ_SENDING;
      if (r->numFrames == 0) {
        r->doneFlag.store(1, std::memory_order_release);
      }
    }
    if (state == DPDK_REQ_SENDING) {
      if (r->doneFlag.load(std::memory_order_acquire)) {
        if (size)
          *size = r->size;
        *done = 1;
        r->state.store(DPDK_REQ_UNUSED, std::memory_order_release);
        r->used = 0;
        return ncclSuccess;
      }
    }
    return ncclSuccess;
  }

  if (r->op == NCCL_SOCKET_RECV) {
    int state = r->state.load(std::memory_order_acquire);
    if (state == DPDK_REQ_RECV_CTRL) {
      NCCLCHECK(ncclSocketProgress(NCCL_SOCKET_RECV, &comm->ctrlSock,
                                   &r->ctrlMsg, sizeof(r->ctrlMsg),
                                   &r->ctrlOffset));
      if (r->ctrlOffset < (int)sizeof(r->ctrlMsg))
        return ncclSuccess;
      if (r->ctrlMsg.magic != DPDK_CTRL_MAGIC ||
          r->ctrlMsg.type != DPDK_CTRL_SEND) {
        WARN("NET/DPDK : invalid ctrl SEND");
        return ncclInvalidUsage;
      }
      if ((int)r->ctrlMsg.size > r->size) {
        char line[SOCKET_NAME_MAXLEN + 1];
        union ncclSocketAddress addr;
        NCCLCHECK(ncclSocketGetAddr(&comm->ctrlSock, &addr));
        WARN("NET/DPDK : peer %s message truncated : receiving %d bytes "
             "instead of %d. If you believe your network is healthy, there may "
             "be a mismatch in collective sizes or env settings between ranks",
             ncclSocketToString(&addr, line), (int)r->ctrlMsg.size, r->size);
        return ncclInvalidUsage;
      }
      r->reqId = r->ctrlMsg.reqId;
      r->size = (int)r->ctrlMsg.size;
      r->frameSize = (int)r->ctrlMsg.frameSize;
      if (r->frameSize <= 0)
        r->frameSize = dpdkSelectFrameSize(comm, r->size);
      NCCLCHECK(dpdkInitRecvFrames(r, r->frameSize, r->size));
      r->state.store(DPDK_REQ_SEND_READY, std::memory_order_release);
      r->ctrlOffset = 0;
      state = DPDK_REQ_SEND_READY;
    }
    if (state == DPDK_REQ_SEND_READY) {
      if (r->ctrlOffset == 0) {
        r->ctrlMsg.magic = DPDK_CTRL_MAGIC;
        r->ctrlMsg.version = DPDK_PROTO_VERSION;
        r->ctrlMsg.type = DPDK_CTRL_READY;
        r->ctrlMsg.reqId = r->reqId;
        r->ctrlMsg.size = r->size;
        r->ctrlMsg.frameSize = (uint32_t)r->frameSize;
      }
      NCCLCHECK(ncclSocketProgress(NCCL_SOCKET_SEND, &comm->ctrlSock,
                                   &r->ctrlMsg, sizeof(r->ctrlMsg),
                                   &r->ctrlOffset));
      if (r->ctrlOffset < (int)sizeof(r->ctrlMsg))
        return ncclSuccess;
      r->doneFlag.store(0, std::memory_order_relaxed);
      r->state.store(DPDK_REQ_RECEIVING, std::memory_order_release);
      state = DPDK_REQ_RECEIVING;
      if (r->numFrames == 0) {
        r->doneFlag.store(1, std::memory_order_release);
      }
    }
    if (state == DPDK_REQ_RECEIVING) {
      if (r->doneFlag.load(std::memory_order_acquire)) {
        if (size)
          *size = r->size;
        *done = 1;
        r->state.store(DPDK_REQ_UNUSED, std::memory_order_release);
        r->used = 0;
        return ncclSuccess;
      }
    }
    return ncclSuccess;
  }

  return ncclSuccess;
}

ncclResult_t ncclNetDpdkCloseListen(void *opaqueComm) {
  struct ncclNetDpdkListenComm *comm =
      (struct ncclNetDpdkListenComm *)opaqueComm;
  if (comm) {
    int ready;
    NCCLCHECK(ncclSocketReady(&comm->sock, &ready));
    if (ready)
      NCCLCHECK(ncclSocketClose(&comm->sock));
    free(comm);
  }
  return ncclSuccess;
}

ncclResult_t ncclNetDpdkClose(void *opaqueComm) {
  struct ncclNetDpdkComm *comm = (struct ncclNetDpdkComm *)opaqueComm;
  if (comm) {
    comm->closing.store(1, std::memory_order_release);
    dpdkUnregisterComm(comm);
    NCCLCHECK(dpdkReleasePollThread(comm->dev));
    int ready;
    NCCLCHECK(ncclSocketReady(&comm->ctrlSock, &ready));
    if (ready)
      NCCLCHECK(ncclSocketClose(&comm->ctrlSock));
    dpdkCommRelease(comm);
  }
  return ncclSuccess;
}

ncclResult_t ncclNetDpdkFinalize(void *ctx) {
  dpdkRefCount--;
  return ncclSuccess;
}

ncclNet_t ncclNetDpdkSocket = {
    "DpdkSocket",
    ncclNetDpdkInit,
    ncclNetDpdkDevices,
    ncclNetDpdkGetProperties,
    ncclNetDpdkListen,
    ncclNetDpdkConnect,
    ncclNetDpdkAccept,
    ncclNetDpdkRegMr,
    NULL,
    ncclNetDpdkDeregMr,
    ncclNetDpdkIsend,
    ncclNetDpdkIrecv,
    ncclNetDpdkIflush,
    ncclNetDpdkTest,
    ncclNetDpdkClose,
    ncclNetDpdkClose,
    ncclNetDpdkCloseListen,
    NULL,
    NULL,
    NULL,
    ncclNetDpdkFinalize,
    NULL,
};
