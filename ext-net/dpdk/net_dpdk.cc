/*************************************************************************
 * Copyright (c) 2016-2022, NVIDIA CORPORATION. All rights reserved.
 *
 * See LICENSE.txt for license information
 ************************************************************************/

#include "ncdp.h"
#include "net.h"
#include "plugin_compat.h"
#include "socket_compat.h"

#include <arpa/inet.h>
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <mutex>
#include <stdint.h>
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
#if __has_include(<rte_vdev.h>)
#include <rte_vdev.h>
#elif __has_include(<rte_bus_vdev.h>)
#include <rte_bus_vdev.h>
#endif

#define DPDK_MAX_PDEVS MAX_IFS
#define DPDK_MAX_VDEVS MAX_IFS
#define DPDK_MAX_DEVS (DPDK_MAX_PDEVS + DPDK_MAX_VDEVS)
#define DPDK_MAX_REQUESTS NCCL_NET_MAX_REQUESTS

#define DPDK_CTRL_MAGIC 0x4e43444bU // "NCDK"
#define DPDK_PROTO_VERSION 3

#define DPDK_RX_BURST 32
#define DPDK_TX_BURST 32
#define DPDK_MBUF_COUNT 8192
#define DPDK_MBUF_CACHE 256
#define DPDK_RX_DESC 1024
#define DPDK_TX_DESC 1024
#define DPDK_INVALID_SEQ UINT32_MAX

// Runtime knobs used by this DPDK transport implementation.
// Data plane is DPDK/UDP; control plane remains ncclSocket-based TCP.
NCCL_DPDK_PARAM(DpdkFrameWindow, "DPDK_FRAME_WINDOW", 64)
NCCL_DPDK_PARAM(DpdkMaxTasksPerRequest, "DPDK_MAX_TASKS_PER_REQUEST", 8)
NCCL_DPDK_PARAM(DpdkMinTaskFrames, "DPDK_MIN_TASK_FRAMES", 4096)
NCCL_DPDK_PARAM(DpdkAckEvery, "DPDK_ACK_EVERY", 8)
NCCL_DPDK_PARAM(DpdkAckDelayUs, "DPDK_ACK_DELAY_US", 10)

// Control-plane messages exchanged over ncclSocket.
// SEND announces a new transfer, READY binds the peer request id.
enum dpdkCtrlType {
  DPDK_CTRL_SEND = 1,
  DPDK_CTRL_READY = 2,
};

// Fixed control header carried on the control socket.
typedef struct __attribute__((packed)) dpdkCtrlMsg {
  uint32_t magic;
  uint16_t version;
  uint16_t type;
  uint32_t srcReqId;
  uint32_t dstReqId;
  uint32_t size;
  uint32_t frameSize;
} dpdkCtrlMsg;

// Data-plane identity exchanged once during connect/accept.
typedef struct __attribute__((packed)) dpdkHelloMsg {
  struct rte_ether_addr mac;
  uint32_t commId;
  uint32_t dataIp;
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
  // Local sequence this slot currently represents. UINT32_MAX means free slot.
  uint32_t seq;
  // Payload bytes in this frame slot (last frame may be shorter).
  uint16_t len;
  uint8_t state;
  uint8_t reserved;
};

struct dpdkSendTask {
  // First global frame seq covered by this task.
  uint32_t baseSeq;
  int frameSize;
  // Flow-control window in number of frames.
  int cwnd;
  // TCP-style notation: lowest unacked local seq / next local seq to send.
  uint32_t sndUna;
  uint32_t sndNxt;
  // Number of frames sent but not cumulatively acked yet.
  int inflight;
  // Number of frames marked done (acked) inside this task.
  int completedFrames;
  int isDone;
  int numFrames;
  // Ring window slots for in-flight/send-progress frames.
  int frameSlots;
  // Per-slot metadata indexed by (localSeq % frameSlots).
  struct dpdkFrameMeta *frames;
  // Ack bitmap over ring slots.
  uint64_t *ackedBitmap;
  int ackedBitmapWords;
};

struct dpdkRecvTask {
  // First global frame seq covered by this task.
  uint32_t baseSeq;
  // Task index echoed on data/ack packets.
  uint16_t taskId;
  uint16_t reserved0;
  int frameSize;
  int numFrames;
  int completedFrames;
  int isDone;
  // Ring window slots for in-order contiguous commit + duplicate filtering.
  int frameSlots;
  // Per-slot metadata indexed by (localSeq % frameSlots).
  struct dpdkFrameMeta *frames;
  // Next local seq not cumulatively committed yet.
  uint32_t rcvNxt;
  // Delayed cumulative ACK state for this task.
  uint32_t ackPending;
  uint32_t ackSentSeq;
  uint64_t ackDeadlineTsc;
};

struct dpdkSendRequestState {
  // Round-robin cursor used by send progress.
  int nextTaskCursor;
  struct dpdkSendTask *tasks;
};

struct dpdkRecvRequestState {
  struct dpdkRecvTask *tasks;
  // Number of recv frames already copied/committed.
  int recvDoneFrames;
};

enum ncclNetDpdkCommState {
  // Entry state before any socket progress.
  ncclNetDpdkCommStateStart = 0,
  // Active connect path (client).
  ncclNetDpdkCommStateConnect = 1,
  // Client sends hello payload.
  ncclNetDpdkCommStateHelloSend = 2,
  // Active accept path (server).
  ncclNetDpdkCommStateAccept = 3,
  // Server receives hello payload.
  ncclNetDpdkCommStateHelloRecv = 4,
};

struct ncclNetDpdkCommStage {
  // Incremental connect/accept progress.
  enum ncclNetDpdkCommState state;
  struct ncclSocket *sock;
  struct ncclNetDpdkComm *comm;
  int ioOffset;
};

struct ncclNetDpdkHandle {
  union ncclSocketAddress connectAddr;
  uint64_t magic;
  struct rte_ether_addr mac;
  uint32_t commId;
  uint32_t dataIp;
  struct ncclNetDpdkCommStage stage;
};

struct ncclNetDpdkRequest {
  int op;
  void *data;
  int size;
  // Slot ownership in comm->requests[].
  int inUse;
  std::atomic<int> state;
  uint32_t reqId;
  uint32_t peerReqId;
  // Byte progress for control-plane SEND/RECV of ctrlMsg.
  int ctrlMsgOffset;
  dpdkCtrlMsg ctrlMsg;
  // Completion observed by ncclNetDpdkTest.
  std::atomic<int> isCompleted;
  struct ncclNetDpdkComm *comm;
  // Common transfer geometry shared by send and recv paths.
  int frameSize;
  int numFrames;
  int numTasks;
  int completedTasks;
  // Per-op request state. SEND and RECV do not overlap in a single request.
  union {
    struct dpdkSendRequestState send;
    struct dpdkRecvRequestState recv;
  };
};

enum ncclNetDpdkReqState {
  // Request slot free.
  DPDK_REQ_UNUSED = 0,
  // Sender publishes SEND control message.
  DPDK_REQ_SEND_CTRL = 1,
  // Sender waits for READY reply.
  DPDK_REQ_WAIT_READY = 2,
  // Sender data frames are in flight.
  DPDK_REQ_SENDING = 3,
  // Receiver waits for SEND control message.
  DPDK_REQ_RECV_CTRL = 4,
  // Receiver sends READY reply.
  DPDK_REQ_SEND_READY = 5,
  // Receiver data frames are arriving.
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
  union ncclSocketAddress ctrlAddr;
  char devName[RTE_ETH_NAME_MAX_LEN];
  char *pciPath;
  int portId;
  struct rte_mempool *pool;
  struct rte_ether_addr mac;
  int mtu;
};

struct dpdkPollThread {
  std::mutex mutex;
  int portId;
  int dev;
  // Number of comms using this worker.
  int refCount;
  int stopRequested;
  unsigned lcoreId;
  std::vector<struct ncclNetDpdkComm *> attachedComms;
};

// Process-local transport state.
static int dpdkInitRefCount;
// Guards global init/finalize and device table mutations.
static std::mutex ncclNetDpdkMutex;
// Guards communicator registry (dpdkActiveComms).
static std::mutex ncclNetDpdkCommMutex;
// Guards poll-lcore allocation/launch bookkeeping.
static std::mutex ncclNetDpdkLcoreMutex;
static int ncclNetDpdkIfs;
static int ncclNetDpdkPhysIfs;
static int ncclNetDpdkVirtIfs;
static ncclNetDpdkDev ncclNetDpdkDevs[DPDK_MAX_DEVS];
// Local control-plane bind address selected from Linux interfaces.
static union ncclSocketAddress ncclNetDpdkCtrlAddr;
static uint64_t dpdkTscHz;
// All currently registered send/recv communicators.
static std::vector<ncclNetDpdkComm *> dpdkActiveComms;
// Round-robin cursor for picking next poll thread lcore.
static unsigned dpdkNextPollLcore = RTE_MAX_LCORE;
// Parsed data-plane IPv4 list loaded from config file.
static std::vector<uint32_t> ncclNetDpdkDataIps;
// Per-device polling worker state and attachments.
static dpdkPollThread dpdkPollThreads[DPDK_MAX_DEVS];

static void dpdkReleaseRequestFrames(struct ncclNetDpdkRequest *r);

// Internal static helpers are ordered by responsibility:
// 1) init / device discovery
// 2) communicator and request ownership
// 3) data-path packet + task state handling
// 4) polling thread orchestration

// ---------------------------------------------------------------------
// Device/control-plane initialization helpers
// ---------------------------------------------------------------------

// Best-effort resolve of PCI sysfs path from DPDK port name.
// Missing path is represented as an empty string for safer topology logging.
static ncclResult_t ncclNetDpdkGetPciPath(const char *portName,
                                          char **pciPath) {
  char devicePath[PATH_MAX] = "";
  *pciPath = NULL;
  snprintf(devicePath, PATH_MAX, "/sys/bus/pci/devices/%s", portName);
  *pciPath = realpath(devicePath, NULL);
  if (*pciPath == NULL) {
    // Keep a non-null string so the topology logging path is safe.
    *pciPath = strdup("");
    if (*pciPath == NULL)
      return ncclSystemError;
  }
  return ncclSuccess;
}

// Resolve one Linux interface for control-plane socket traffic.
// This is intentionally separate from DPDK data-plane ports.
static ncclResult_t dpdkInitControlInterface() {
  char ifName[MAX_IF_NAME_SIZE] = {0};
  union ncclSocketAddress ifAddr;
  int nIfs = 0;
  memset(&ifAddr, 0, sizeof(ifAddr));
  NCCLCHECK(ncclFindInterfaces(ifName, &ifAddr, MAX_IF_NAME_SIZE, 1, &nIfs));
  if (nIfs <= 0) {
    WARN("NET/DPDK : no Linux socket interface available for control plane");
    return ncclInternalError;
  }
  memcpy(&ncclNetDpdkCtrlAddr, &ifAddr, sizeof(ifAddr));
  char line[SOCKET_NAME_MAXLEN + 1];
  INFO(NCCL_INIT | NCCL_NET, "NET/DPDK : control plane using %s:%s",
       ifName, ncclSocketToString(&ncclNetDpdkCtrlAddr, line));
  return ncclSuccess;
}

// Load data-plane IPv4 list from a local config file.
// File format:
// - One or multiple IPv4 addresses per line.
// - Separators: spaces, tabs or commas.
// - '#' starts a comment until end-of-line.
// - Device i uses the i-th parsed IPv4 address.
static ncclResult_t dpdkLoadDataIpConfig() {
  const char *confPath = ncclGetEnv("NCCL_DPDK_NET_CONF");
  if (confPath == NULL || confPath[0] == '\0')
    confPath = "dpdknet.conf";
  ncclNetDpdkDataIps.clear();

  FILE *fp = fopen(confPath, "r");
  if (fp == NULL) {
    WARN("NET/DPDK : failed to open data IP config %s : %s", confPath,
         strerror(errno));
    return ncclSystemError;
  }

  char line[1024];
  int lineNo = 0;
  while (fgets(line, sizeof(line), fp) != NULL) {
    lineNo++;
    char *comment = strchr(line, '#');
    if (comment)
      *comment = '\0';
    for (char *p = line; *p; ++p) {
      if (*p == ',' || *p == '\t' || *p == '\r' || *p == '\n')
        *p = ' ';
    }

    char *save = NULL;
    for (char *token = strtok_r(line, " ", &save); token != NULL;
         token = strtok_r(NULL, " ", &save)) {
      struct in_addr addr;
      if (inet_pton(AF_INET, token, &addr) != 1) {
        fclose(fp);
        WARN("NET/DPDK : invalid IPv4 '%s' at %s:%d", token, confPath, lineNo);
        ncclNetDpdkDataIps.clear();
        return ncclInvalidUsage;
      }
      if ((int)ncclNetDpdkDataIps.size() >= DPDK_MAX_DEVS) {
        WARN("NET/DPDK : too many IPs in %s (max %d), ignoring remaining entries",
             confPath, DPDK_MAX_DEVS);
        fclose(fp);
        INFO(NCCL_INIT | NCCL_NET,
             "NET/DPDK : loaded %d data-plane IPs from %s",
             (int)ncclNetDpdkDataIps.size(), confPath);
        return ncclSuccess;
      }
      ncclNetDpdkDataIps.push_back(addr.s_addr);
    }
  }
  fclose(fp);

  if (ncclNetDpdkDataIps.empty()) {
    WARN("NET/DPDK : no data-plane IPv4 found in %s", confPath);
    return ncclInvalidUsage;
  }
  INFO(NCCL_INIT | NCCL_NET, "NET/DPDK : loaded %d data-plane IPs from %s",
       (int)ncclNetDpdkDataIps.size(), confPath);
  return ncclSuccess;
}

// Initialize DPDK EAL from NCCL_DPDK_EAL arguments.
// If the env var is empty, keep defaults minimal and deterministic.
static ncclResult_t dpdkInitEal() {
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
  }

  std::vector<char *> ownedArgv;
  ownedArgv.reserve(args.size());
  for (auto &a : args) {
    char *dup = strdup(a.c_str());
    if (dup == NULL) {
      for (auto *p : ownedArgv)
        free(p);
      return ncclSystemError;
    }
    ownedArgv.push_back(dup);
  }

  // rte_eal_init may rewrite the argv pointer array in-place. Keep one copy
  // for EAL and free only the original strdup-ed pointers.
  std::vector<char *> ealArgv = ownedArgv;
  int ret = rte_eal_init((int)ealArgv.size(), ealArgv.data());
  for (auto *p : ownedArgv)
    free(p);
  if (ret < 0) {
    WARN("NET/DPDK : rte_eal_init failed");
    return ncclSystemError;
  }
  dpdkTscHz = rte_get_tsc_hz();
  if (dpdkTscHz == 0)
    dpdkTscHz = 1000000000ULL;
  return ncclSuccess;
}

// Configure one DPDK data port with a single RX/TX queue pair.
static ncclResult_t dpdkInitDataPort(ncclNetDpdkDev *dev) {
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

// Discover and initialize all physical DPDK ports used for data transfer.
static ncclResult_t dpdkInitDataDevices() {
  if (ncclNetDpdkDataIps.empty()) {
    WARN("NET/DPDK : data IP config is empty");
    return ncclInvalidUsage;
  }

  int matched = 0;
  uint16_t portId = 0;
  RTE_ETH_FOREACH_DEV(portId) {
    if (matched >= DPDK_MAX_PDEVS)
      break;
    char portName[RTE_ETH_NAME_MAX_LEN] = {0};
    if (rte_eth_dev_get_name_by_port(portId, portName) != 0)
      continue;

    char *pciPath = NULL;
    NCCLCHECK(ncclNetDpdkGetPciPath(portName, &pciPath));
    ncclNetDpdkDev *dev = ncclNetDpdkDevs + matched;
    memset(dev, 0, sizeof(*dev));
    if (matched >= (int)ncclNetDpdkDataIps.size()) {
      WARN("NET/DPDK : missing data-plane IPv4 for device %d (need at least %d entries)",
           matched, matched + 1);
      return ncclInvalidUsage;
    }
    dev->addr.sin.sin_family = AF_INET;
    dev->addr.sin.sin_addr.s_addr = ncclNetDpdkDataIps[matched];
    dev->addr.sin.sin_port = 0;
    memcpy(&dev->ctrlAddr, &ncclNetDpdkCtrlAddr, sizeof(dev->ctrlAddr));
    strncpy(dev->devName, portName, sizeof(dev->devName) - 1);
    dev->devName[sizeof(dev->devName) - 1] = '\0';
    dev->pciPath = pciPath;
    dev->portId = (int)portId;
    NCCLCHECK(dpdkInitDataPort(dev));
    matched++;
  }

  if (rte_eth_dev_count_avail() > 0 && matched <= 0) {
    WARN("NET/DPDK : no DPDK data-plane device available");
    return ncclInternalError;
  }
  ncclNetDpdkPhysIfs = matched;
  ncclNetDpdkIfs = matched;

  char line[2048];
  char addrline[SOCKET_NAME_MAXLEN + 1];
  char ctrlline[SOCKET_NAME_MAXLEN + 1];
  line[0] = '\0';
  addrline[SOCKET_NAME_MAXLEN] = '\0';
  ctrlline[SOCKET_NAME_MAXLEN] = '\0';
  for (int i = 0; i < ncclNetDpdkIfs; i++) {
    snprintf(line + strlen(line), sizeof(line) - strlen(line),
             " [%d]%s(port=%d data=%s ctrl=%s)", i,
             ncclNetDpdkDevs[i].devName,
             ncclNetDpdkDevs[i].portId,
             ncclSocketToString(&ncclNetDpdkDevs[i].addr, addrline),
             ncclSocketToString(&ncclNetDpdkDevs[i].ctrlAddr, ctrlline));
  }
  INFO(NCCL_INIT | NCCL_NET, "NET/DPDK : Using%s", line);
  return ncclSuccess;
}

// Query current link speed from DPDK ethdev.
static ncclResult_t ncclNetDpdkGetSpeed(int dev, int *speed) {
  int portId = ncclNetDpdkDevs[dev].portId;

  struct rte_eth_link link;
  memset(&link, 0, sizeof(link));
  rte_eth_link_get((uint16_t)portId, &link);
  int mbps = (int)link.link_speed;

  if (mbps <= 0) {
    INFO(NCCL_NET,
         "NET/DPDK : Could not get speed from port %d (%s). Defaulting to 10 "
         "Gbps.",
         portId, ncclNetDpdkDevs[dev].devName);
    mbps = 10000;
  }
  *speed = mbps;
  return ncclSuccess;
}

// ---------------------------------------------------------------------
// Communicator/request ownership helpers
// ---------------------------------------------------------------------

static void dpdkInitCommDefaults(struct ncclNetDpdkComm *comm) {
  // comm is value-initialized by new ncclNetDpdkComm(), so only set non-zero
  // defaults.
  comm->dev = -1;
  comm->cudaDev = -1;
  comm->portId = -1;
  comm->refCount.store(1, std::memory_order_relaxed);
}

// Lookup active data-plane communicator by destination comm id.
static ncclNetDpdkComm *dpdkFindCommById(uint32_t commId) {
  for (auto *comm : dpdkActiveComms) {
    if (comm->commId == commId)
      return comm;
  }
  return NULL;
}

static void dpdkCommAcquire(struct ncclNetDpdkComm *comm) {
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

// ---------------------------------------------------------------------
// Request/task metadata helpers
// ---------------------------------------------------------------------

// Free dynamic frame/task state and reset request progress fields.
static void dpdkReleaseRequestFrames(struct ncclNetDpdkRequest *r) {
  if (r->op == NCCL_SOCKET_SEND) {
    struct dpdkSendRequestState *s = &r->send;
    if (s->tasks) {
      for (int i = 0; i < r->numTasks; i++) {
        if (s->tasks[i].ackedBitmap)
          free(s->tasks[i].ackedBitmap);
        if (s->tasks[i].frames)
          free(s->tasks[i].frames);
      }
      free(s->tasks);
      s->tasks = NULL;
    }
  } else if (r->op == NCCL_SOCKET_RECV) {
    struct dpdkRecvRequestState *v = &r->recv;
    if (v->tasks) {
      for (int i = 0; i < r->numTasks; i++) {
        if (v->tasks[i].frames)
          free(v->tasks[i].frames);
      }
      free(v->tasks);
      v->tasks = NULL;
    }
  }
  r->send.nextTaskCursor = 0;
  r->send.tasks = NULL;
  r->recv.tasks = NULL;
  r->recv.recvDoneFrames = 0;
  r->frameSize = 0;
  r->numFrames = 0;
  r->numTasks = 0;
  r->completedTasks = 0;
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

// Allocate one free request slot and seed initial control-plane state.
static ncclResult_t dpdkGetRequest(struct ncclNetDpdkComm *comm, int op,
                                     void *data, int size,
                                     struct ncclNetDpdkRequest **req) {
  for (int i = 0; i < DPDK_MAX_REQUESTS; i++) {
    struct ncclNetDpdkRequest *r = comm->requests + i;
    if (r->inUse == 0) {
      dpdkReleaseRequestFrames(r);
      r->op = op;
      r->data = data;
      r->size = size;
      r->inUse = 1;
      r->ctrlMsgOffset = 0;
      memset(&r->ctrlMsg, 0, sizeof(r->ctrlMsg));
      r->isCompleted.store(0, std::memory_order_relaxed);
      r->comm = comm;
      r->reqId = ++comm->nextReqId;
      if (r->reqId == 0)
        r->reqId = ++comm->nextReqId;
      r->peerReqId = 0;
      if (op == NCCL_SOCKET_SEND)
        r->frameSize = dpdkSelectFrameSize(comm, size);
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

// Build request tasks for both SEND and RECV from the same partition rule.
// This guarantees identical task slicing for equal (size, frameSize).
static ncclResult_t dpdkBuildRequestTasks(struct ncclNetDpdkRequest *r,
                                          int frameSize, int size, int op) {
  // Rebuild-safe: clear any stale task/frame allocations first.
  if (size <= 0 || frameSize <= 0) {
    r->numFrames = 0;
    r->numTasks = 0;
    return ncclSuccess;
  }

  r->numFrames = (size + frameSize - 1) / frameSize;

  // Minimum number of frames that should be grouped into one task.
  int minTaskFrames = ncclParamDpdkMinTaskFrames();
  if (minTaskFrames > r->numFrames)
    minTaskFrames = r->numFrames;

  // Feasible task upper-bound under the min-task-frames constraint.
  int maxTasksByMinFrames = r->numFrames / minTaskFrames;
  r->numTasks = std::min((int)ncclParamDpdkMaxTasksPerRequest(), maxTasksByMinFrames);

  // Spread frames as evenly as possible across tasks.
  // Each task has baseTaskFrames frames; first extraTaskFrames tasks have +1.
  const int baseTaskFrames = r->numFrames / r->numTasks;
  const int extraTaskFrames = r->numFrames % r->numTasks;

  auto taskNumFramesFor = [baseTaskFrames, extraTaskFrames](int taskId) -> int {
    return baseTaskFrames + (taskId < extraTaskFrames ? 1 : 0);
  };
  auto taskBaseSeqFor = [baseTaskFrames, extraTaskFrames](int taskId) -> uint32_t {
    return (uint32_t)(taskId * baseTaskFrames +
                      std::min(taskId, extraTaskFrames));
  };

  int window = ncclParamDpdkFrameWindow();

  if (op == NCCL_SOCKET_SEND) {
    struct dpdkSendRequestState *s = &r->send;
    NCCLCHECK(ncclCalloc(&s->tasks, r->numTasks));
    for (int t = 0; t < r->numTasks; t++) {
      struct dpdkSendTask *task = s->tasks + t;
      task->baseSeq = taskBaseSeqFor(t);
      task->numFrames = taskNumFramesFor(t);
      task->frameSize = frameSize;
      task->cwnd = std::min(window, task->numFrames);
      // Keep ring window larger than cwnd when possible so slot reuse never
      // collides with unacked frames.
      task->frameSlots = std::min(task->numFrames, task->cwnd + DPDK_TX_BURST);
      if (task->numFrames > task->cwnd && task->frameSlots <= task->cwnd)
        task->frameSlots = task->cwnd + 1;
      task->sndUna = 0;
      task->sndNxt = 0;
      task->inflight = 0;
      task->completedFrames = 0;
      task->isDone = 0;
      // One bit per ring slot.
      task->ackedBitmapWords = (task->frameSlots + 63) / 64;
      NCCLCHECK(ncclCalloc(&task->frames, task->frameSlots));
      NCCLCHECK(ncclCalloc(&task->ackedBitmap, task->ackedBitmapWords));
      for (int i = 0; i < task->frameSlots; i++) {
        task->frames[i].seq = DPDK_INVALID_SEQ;
        task->frames[i].len = 0;
        task->frames[i].state = DPDK_FRAME_EMPTY;
        task->frames[i].reserved = 0;
      }
    }
    s->nextTaskCursor = 0;
  } else if (op == NCCL_SOCKET_RECV) {
    struct dpdkRecvRequestState *v = &r->recv;
    NCCLCHECK(ncclCalloc(&v->tasks, r->numTasks));
    for (int t = 0; t < r->numTasks; t++) {
      struct dpdkRecvTask *task = v->tasks + t;
      memset(task, 0, sizeof(*task));
      task->baseSeq = taskBaseSeqFor(t);
      task->numFrames = taskNumFramesFor(t);
      task->taskId = (uint16_t)t;
      task->frameSize = frameSize;
      task->completedFrames = 0;
      task->isDone = 0;
      // Keep receive slot count ahead of sender cwnd to absorb mild reordering.
      task->frameSlots = std::min(task->numFrames, window + DPDK_RX_BURST);
      if (task->numFrames > window && task->frameSlots <= window)
        task->frameSlots = window + 1;
      task->rcvNxt = 0;
      task->ackPending = 0;
      task->ackSentSeq = DPDK_INVALID_SEQ;
      task->ackDeadlineTsc = 0;
      NCCLCHECK(ncclCalloc(&task->frames, task->frameSlots));
      for (int i = 0; i < task->frameSlots; i++) {
        task->frames[i].seq = DPDK_INVALID_SEQ;
        task->frames[i].len = 0;
        task->frames[i].state = DPDK_FRAME_EMPTY;
        task->frames[i].reserved = 0;
      }
    }
    v->recvDoneFrames = 0;
  } else {
    WARN("NET/DPDK : invalid op %d in dpdkBuildRequestTasks", op);
    dpdkReleaseRequestFrames(r);
    return ncclInternalError;
  }

  r->completedTasks = 0;
  return ncclSuccess;
}

// ---------------------------------------------------------------------
// Data-plane packet hashing/port helpers
// ---------------------------------------------------------------------

static uint32_t dpdkHash32(uint32_t x) {
  x ^= x >> 16;
  x *= 0x7feb352dU;
  x ^= x >> 15;
  x *= 0x846ca68bU;
  x ^= x >> 16;
  return x;
}

// Deterministic per-flow UDP port selection to spread traffic.
static uint16_t dpdkSelectUdpPort(uint32_t dstCommId, uint32_t srcCommId,
                                  uint32_t srcReqId, uint32_t dstReqId,
                                  uint32_t taskId) {
  static uint32_t base = 6789;
  uint32_t seed = dstCommId ^ srcCommId ^ srcReqId ^ dstReqId ^
                  (taskId * 0x9e3779b9U);
  if (base < 1024 || base > 65535)
    base = 1024;
  uint32_t range = 65535 - base + 1;
  uint32_t hash = dpdkHash32(seed);
  return (uint16_t)(base + (hash % range));
}

// Build one NCDP packet and submit it on the port.
static bool dpdkSendPacket(struct ncclNetDpdkComm *comm, uint16_t flags,
                           uint32_t dstCommId, uint32_t srcCommId,
                           uint32_t srcReqId, uint32_t dstReqId,
                           uint32_t taskId, uint32_t seq,
                           const void *payload, uint16_t len) {
  if (comm == NULL || comm->pool == NULL || comm->portId < 0)
    return false;
  uint16_t udpPort =
      dpdkSelectUdpPort(dstCommId, srcCommId, srcReqId, dstReqId, taskId);
  struct rte_mbuf *mbuf = rte_pktmbuf_alloc(comm->pool);
  if (mbuf == NULL)
    return false;
  if (!ncdpBuildPacket(mbuf, &comm->localMac, &comm->remoteMac, comm->localIp,
                       comm->remoteIp, flags, dstCommId, srcCommId, srcReqId,
                       dstReqId, taskId, seq, payload, len, udpPort)) {
    rte_pktmbuf_free(mbuf);
    return false;
  }
  struct rte_mbuf *txPkts[1] = {mbuf};
  int sent = rte_eth_tx_burst((uint16_t)comm->portId, 0, txPkts, 1);
  if (sent < 1) {
    rte_pktmbuf_free(mbuf);
    return false;
  }
  return true;
}

// ---------------------------------------------------------------------
// Request/frame state machine helpers
// ---------------------------------------------------------------------

static inline int dpdkAckEvery() {
  int ackEvery = ncclParamDpdkAckEvery();
  if (ackEvery <= 0)
    ackEvery = 1;
  if (ackEvery > 256)
    ackEvery = 256;
  return ackEvery;
}

static inline uint64_t dpdkAckDelayCycles() {
  int delayUs = ncclParamDpdkAckDelayUs();
  if (delayUs <= 0)
    return 0;
  uint64_t hz = dpdkTscHz ? dpdkTscHz : rte_get_tsc_hz();
  if (hz == 0)
    return 0;
  uint64_t cycles = (uint64_t)delayUs * hz / 1000000ULL;
  if (cycles == 0)
    cycles = 1;
  return cycles;
}

// Mark recv request done only after all payload is copied and final ACK state
// is fully drained.
static inline void dpdkTryFinishRecvAfterAck(struct ncclNetDpdkRequest *req) {
  struct dpdkRecvRequestState *v = &req->recv;
  if (req->numFrames <= 0 || v->tasks == NULL)
    return;
  if (req->completedTasks < req->numTasks)
    return;
  for (int i = 0; i < req->numTasks; i++) {
    struct dpdkRecvTask *task = v->tasks + i;
    if (task->rcvNxt != (uint32_t)task->numFrames || task->ackPending != 0)
      return;
  }
  req->isCompleted.store(1, std::memory_order_release);
}

// Emit one task-scoped cumulative ACK packet.
// seq field carries global seq for easier sender-side bounds checks.
static inline bool dpdkSendTaskAck(struct ncclNetDpdkComm *comm,
                                   struct ncclNetDpdkRequest *req,
                                   struct dpdkRecvTask *task,
                                   uint32_t ackLocalSeq) {
  if (req->peerReqId == 0 || req->reqId == 0)
    return false;
  uint32_t ackSeq = task->baseSeq + ackLocalSeq;
  if (!dpdkSendPacket(comm, NCDP_FLAG_ACK, comm->remoteCommId, comm->commId,
                        /*srcReqId=*/req->reqId,
                        /*dstReqId=*/req->peerReqId,
                        /*taskId=*/(uint32_t)task->taskId, ackSeq, NULL, 0)) {
    return false;
  }
  task->ackSentSeq = ackSeq;
  task->ackPending = 0;
  task->ackDeadlineTsc = 0;
  dpdkTryFinishRecvAfterAck(req);
  return true;
}

static inline void dpdkMarkTaskAckedRange(struct dpdkSendTask *task,
                                          uint32_t start, uint32_t end) {
  if (task->ackedBitmap == NULL || task->frameSlots <= 0 || start > end)
    return;
  for (uint32_t localSeq = start; localSeq <= end; localSeq++) {
    int slot = (int)(localSeq % (uint32_t)task->frameSlots);
    uint32_t word = (uint32_t)slot >> 6;
    uint32_t bit = (uint32_t)slot & 63u;
    if (task->frames[slot].seq == localSeq)
      task->ackedBitmap[word] |= (1ull << bit);
  }
}

// Consume contiguous acked bits from sndUna and advance the left boundary.
static inline void dpdkAdvanceTaskAckPrefix(struct dpdkSendTask *task) {
  if (task->frames == NULL || task->ackedBitmap == NULL || task->frameSlots <= 0)
    return;
  while (task->sndUna < task->sndNxt) {
    uint32_t localSeq = task->sndUna;
    int slot = (int)(localSeq % (uint32_t)task->frameSlots);
    struct dpdkFrameMeta *meta = task->frames + slot;
    if (meta->seq != localSeq)
      break;
    uint32_t word = (uint32_t)slot >> 6;
    uint32_t bit = (uint32_t)slot & 63u;
    if (((task->ackedBitmap[word] >> bit) & 1ull) == 0ull)
      break;
    task->ackedBitmap[word] &= ~(1ull << bit);
    if (meta->state == DPDK_FRAME_SENT && task->inflight > 0)
      task->inflight--;
    if (meta->state != DPDK_FRAME_DONE)
      task->completedFrames++;
    meta->seq = DPDK_INVALID_SEQ;
    meta->len = 0;
    meta->state = DPDK_FRAME_EMPTY;
    task->sndUna++;
  }
}

// Apply one task-scoped cumulative ACK to sender window state.
static inline void dpdkApplyTaskAck(struct ncclNetDpdkRequest *req,
                                    uint32_t taskId, uint32_t ackSeq) {
  struct dpdkSendRequestState *s = &req->send;
  if (s->tasks == NULL || req->numTasks <= 0 || req->numFrames <= 0)
    return;
  if (taskId >= (uint32_t)req->numTasks)
    return;
  struct dpdkSendTask *task = s->tasks + taskId;
  if (task->isDone)
    return;

  uint32_t taskStart = task->baseSeq;
  if (ackSeq < taskStart)
    return;
  uint32_t taskLast = taskStart + (uint32_t)task->numFrames - 1;
  uint32_t taskAckGlobal = std::min(ackSeq, taskLast);
  if (task->sndNxt == 0)
    return;
  uint32_t taskSentLastLocal = task->sndNxt - 1;
  uint32_t taskAckLocal = taskAckGlobal - taskStart;
  if (taskAckLocal > taskSentLastLocal)
    taskAckLocal = taskSentLastLocal;
  if (taskAckLocal < task->sndUna)
    return;

  dpdkMarkTaskAckedRange(task, task->sndUna, taskAckLocal);
  dpdkAdvanceTaskAckPrefix(task);
  if (task->completedFrames >= task->numFrames && !task->isDone) {
    task->isDone = 1;
    req->completedTasks++;

    if (req->completedTasks >= req->numTasks) {
      req->isCompleted.store(1, std::memory_order_release);
    }
  }
}

// Periodically flush delayed ACKs for each recv task.
static inline void dpdkProgressRecvRequest(struct ncclNetDpdkComm *comm,
                                           struct ncclNetDpdkRequest *req) {
  struct dpdkRecvRequestState *v = &req->recv;

  if (req->state.load(std::memory_order_acquire) != DPDK_REQ_RECEIVING)
    return;
  if (v->tasks == NULL || req->numTasks <= 0) {
    dpdkTryFinishRecvAfterAck(req);
    return;
  }
  uint64_t nowCycles = rte_get_tsc_cycles();
  uint64_t delayCycles = dpdkAckDelayCycles();
  for (int i = 0; i < req->numTasks; i++) {
    struct dpdkRecvTask *task = v->tasks + i;
    if (task->ackPending == 0 || task->rcvNxt == 0)
      continue;
    uint32_t ackSeq = task->baseSeq + (task->rcvNxt - 1);
    if (ackSeq == task->ackSentSeq) {
      task->ackPending = 0;
      task->ackDeadlineTsc = 0;
      continue;
    }
    if (delayCycles > 0 && task->ackDeadlineTsc != 0 &&
        nowCycles < task->ackDeadlineTsc)
      continue;
    if (!dpdkSendTaskAck(comm, req, task, task->rcvNxt - 1)) {
      uint64_t retryDelay = delayCycles;
      if (retryDelay == 0)
        retryDelay = 1;
      task->ackDeadlineTsc = nowCycles + retryDelay;
    }
  }
  dpdkTryFinishRecvAfterAck(req);
}

// Packet demux for the data plane:
// ACK updates sender windows, DATA copies payload and may schedule delayed ACK.
static void dpdkHandleRxPacket(const ncdpHdr *hdr,
                               const uint8_t *payload, uint16_t len) {
  std::lock_guard<std::mutex> commLock(ncclNetDpdkCommMutex);
  struct ncclNetDpdkComm *comm = dpdkFindCommById(hdr->dstCommId);
  if (!comm || comm->closing.load(std::memory_order_acquire))
    return;

  if (hdr->flags & NCDP_FLAG_ACK) {
    struct ncclNetDpdkRequest *req =
        dpdkFindRequestByReqId(comm, hdr->dstReqId, DPDK_REQ_SENDING);
    // ACK seq is cumulative within hdr->taskId.
    if (req)
      dpdkApplyTaskAck(req, hdr->taskId, hdr->seq);
    return;
  }

  if (hdr->flags & NCDP_FLAG_DATA) {
    struct ncclNetDpdkRequest *req =
        dpdkFindRequestByReqId(comm, hdr->dstReqId, DPDK_REQ_RECEIVING);
    struct dpdkRecvRequestState *v = req ? &req->recv : NULL;
    if (!(req && req->reqId == hdr->dstReqId && v->tasks && req->numFrames > 0))
      return;
    uint32_t taskId = hdr->taskId;
    if (taskId >= (uint32_t)req->numTasks)
      return;
    struct dpdkRecvTask *task = v->tasks + taskId;
    if (task->frames == NULL || task->frameSlots <= 0 || task->numFrames <= 0)
      return;

    uint32_t seq = hdr->seq;
    if (seq < task->baseSeq)
      return;
    uint32_t localSeq = seq - task->baseSeq;
    if (localSeq >= (uint32_t)task->numFrames)
      return;
    // Out-of-window packet: drop. Sender window should keep this rare.
    if (localSeq >= task->rcvNxt + (uint32_t)task->frameSlots)
      return;

    int payloadOffset =
        req->frameSize > 0 ? (int)(seq * (uint32_t)req->frameSize) : 0;
    if (payloadOffset < 0 || payloadOffset >= req->size)
      return;
    int remaining = req->size - payloadOffset;
    int expectedLen = std::min(req->frameSize, remaining);
    int copyLen = std::min((int)len, expectedLen);
    if (copyLen < 0)
      copyLen = 0;

    int slot = (int)(localSeq % (uint32_t)task->frameSlots);
    struct dpdkFrameMeta *meta = task->frames + slot;
    bool newlyCompleted = false;
    if (meta->seq == localSeq) {
      if (meta->state != DPDK_FRAME_DONE) {
        if (copyLen > 0)
          memcpy((char *)req->data + payloadOffset, payload, copyLen);
        meta->len = (uint16_t)copyLen;
        meta->state = DPDK_FRAME_DONE;
        newlyCompleted = true;
      }
    } else if (meta->seq == DPDK_INVALID_SEQ || meta->state == DPDK_FRAME_EMPTY) {
      if (copyLen > 0)
        memcpy((char *)req->data + payloadOffset, payload, copyLen);
      meta->seq = localSeq;
      meta->len = (uint16_t)copyLen;
      meta->state = DPDK_FRAME_DONE;
      meta->reserved = 0;
      newlyCompleted = true;
    } else {
      return;
    }

    if (newlyCompleted) {
      task->completedFrames++;
      v->recvDoneFrames++;
      if (task->completedFrames >= task->numFrames && !task->isDone) {
        task->isDone = 1;
        req->completedTasks++;
      }
    }

    uint32_t oldRcvNxt = task->rcvNxt;
    while (task->rcvNxt < (uint32_t)task->numFrames) {
      int nextSlot = (int)(task->rcvNxt % (uint32_t)task->frameSlots);
      struct dpdkFrameMeta *nextMeta = task->frames + nextSlot;
      if (nextMeta->seq != task->rcvNxt || nextMeta->state != DPDK_FRAME_DONE)
        break;
      nextMeta->seq = DPDK_INVALID_SEQ;
      nextMeta->len = 0;
      nextMeta->state = DPDK_FRAME_EMPTY;
      task->rcvNxt++;
    }
    if (task->rcvNxt > oldRcvNxt) {
      task->ackPending += task->rcvNxt - oldRcvNxt;
      bool forceAck = (task->rcvNxt == (uint32_t)task->numFrames) ||
                      (task->ackPending >= (uint32_t)dpdkAckEvery()) ||
                      (dpdkAckDelayCycles() == 0);
      uint64_t nowCycles = rte_get_tsc_cycles();
      if (forceAck) {
        if (!dpdkSendTaskAck(comm, req, task, task->rcvNxt - 1)) {
          uint64_t delayCycles = dpdkAckDelayCycles();
          if (delayCycles == 0)
            delayCycles = 1;
          task->ackDeadlineTsc = nowCycles + delayCycles;
        }
      } else if (task->ackDeadlineTsc == 0) {
        uint64_t delayCycles = dpdkAckDelayCycles();
        if (delayCycles == 0)
          delayCycles = 1;
        task->ackDeadlineTsc = nowCycles + delayCycles;
      }
    }
    dpdkTryFinishRecvAfterAck(req);
    return;
  }
}

static int dpdkProgressSendTask(struct ncclNetDpdkComm *comm,
                                struct ncclNetDpdkRequest *req,
                                struct dpdkSendTask *task, int taskId,
                                int budget) {
  int sent = 0;
  // Drain newly acked prefix before trying to send more.
  dpdkAdvanceTaskAckPrefix(task);
  while (budget > 0) {
    if (task->inflight >= task->cwnd)
      break;
    if (task->sndNxt >= (uint32_t)task->numFrames)
      break;
    if (task->frames == NULL || task->frameSlots <= 0)
      break;
    if (req->peerReqId == 0)
      break;

    uint32_t localSeq = task->sndNxt;
    int slot = (int)(localSeq % (uint32_t)task->frameSlots);
    struct dpdkFrameMeta *meta = task->frames + slot;
    if (meta->seq != localSeq) {
      if (meta->seq != DPDK_INVALID_SEQ)
        break;
      uint32_t seq = task->baseSeq + localSeq;
      int remaining = req->size - (int)(seq * (uint32_t)req->frameSize);
      // Exact bytes to send for this frame (tail frame may be partial).
      int len = std::min(req->frameSize, remaining);
      if (len < 0)
        len = 0;
      meta->seq = localSeq;
      meta->len = (uint16_t)len;
      meta->state = DPDK_FRAME_READY;
      meta->reserved = 0;
      uint32_t word = (uint32_t)slot >> 6;
      uint32_t bit = (uint32_t)slot & 63u;
      task->ackedBitmap[word] &= ~(1ull << bit);
    }
    if (meta->state != DPDK_FRAME_READY)
      break;

    uint32_t seq = task->baseSeq + localSeq;
    uint32_t payloadOffset = (uint32_t)(seq * (uint32_t)req->frameSize);
    if (!dpdkSendPacket(comm, NCDP_FLAG_DATA, comm->remoteCommId,
                        comm->commId,
                        /*srcReqId=*/req->reqId,
                        /*dstReqId=*/req->peerReqId, (uint32_t)taskId, seq,
                        (char *)req->data + payloadOffset, meta->len)) {
      break;
    }
    if (meta->state != DPDK_FRAME_SENT) {
      task->inflight++;
    }
    meta->state = DPDK_FRAME_SENT;
    task->sndNxt++;
    sent++;
    budget--;
  }
  return sent;
}

static void dpdkProgressSendRequest(struct ncclNetDpdkComm *comm,
                                    struct ncclNetDpdkRequest *req) {
  struct dpdkSendRequestState *s = &req->send;
  if (req->isCompleted.load(std::memory_order_acquire))
    return;
  if (req->numFrames <= 0) {
    req->isCompleted.store(1, std::memory_order_release);
    return;
  }
  if (s->tasks == NULL || req->numTasks <= 0)
    return;
  if (req->frameSize <= 0)
    return;

  int budget = DPDK_TX_BURST;
  int tasksScanned = 0;
  int numTasks = req->numTasks;
  while (budget > 0 && tasksScanned < numTasks) {
    int idx = s->nextTaskCursor;
    s->nextTaskCursor = (idx + 1) % numTasks;
    tasksScanned++;
    struct dpdkSendTask *task = s->tasks + idx;
    if (task->isDone)
      continue;
    budget -= dpdkProgressSendTask(comm, req, task, idx, budget);
  }
}

// ---------------------------------------------------------------------
// Polling loop and background progress helpers
// ---------------------------------------------------------------------

static int dpdkPollRx(int portId) {
  struct rte_mbuf *mbufs[DPDK_RX_BURST];
  int nb = rte_eth_rx_burst((uint16_t)portId, 0, mbufs, DPDK_RX_BURST);
  if (nb <= 0)
    return 0;
  for (int i = 0; i < nb; i++) {
    struct rte_mbuf *mbuf = mbufs[i];
    ncdpHdr hdr;
    const uint8_t *payload = NULL;
    uint16_t payloadLen = 0;
    if (ncdpParsePacket(mbuf, &hdr, &payload, &payloadLen))
      dpdkHandleRxPacket(&hdr, payload, payloadLen);
    rte_pktmbuf_free(mbuf);
  }
  return nb;
}

static inline void dpdkProgressComms(struct dpdkPollThread *thread) {
  std::vector<struct ncclNetDpdkComm *> activeComms;
  {
    std::lock_guard<std::mutex> lock(thread->mutex);
    for (auto *comm : thread->attachedComms) {
      if (comm->closing.load(std::memory_order_acquire))
        continue;
      dpdkCommAcquire(comm);
      activeComms.push_back(comm);
    }
  }

  for (auto *comm : activeComms) {
    for (int i = 0; i < DPDK_MAX_REQUESTS; i++) {
      struct ncclNetDpdkRequest *req = comm->requests + i;
      int reqState = req->state.load(std::memory_order_acquire);
      if (reqState == DPDK_REQ_SENDING) {
        dpdkProgressSendRequest(comm, req);
      } else if (reqState == DPDK_REQ_RECEIVING) {
        dpdkProgressRecvRequest(comm, req);
      }
    }
    dpdkCommRelease(comm);
  }
}

// One poll worker per net device:
// receives packets, progresses send tasks, recv tasks(flushes delayed ACKs).
static int dpdkPollThreadMain(void *arg) {
  dpdkPollThread *thread = (dpdkPollThread *)arg;
  while (!thread->stopRequested) {
    int got = dpdkPollRx(thread->portId);
    dpdkProgressComms(thread);
    if (got == 0)
      rte_pause();
  }
  return 0;
}

// Start a device poll worker once.
static ncclResult_t dpdkStartPollThread(int dev) {
  dpdkPollThread *thread = &dpdkPollThreads[dev];
  std::lock_guard<std::mutex> lock(thread->mutex);
  thread->portId = ncclNetDpdkDevs[dev].portId;
  thread->dev = dev;
  thread->refCount = 0;
  thread->stopRequested = 0;
  thread->lcoreId = RTE_MAX_LCORE;
  if (rte_lcore_count() <= 1) {
    WARN("NET/DPDK : no available DPDK lcore for poll thread");
    return ncclSystemError;
  }
  unsigned lcore = RTE_MAX_LCORE;
  {
    std::lock_guard<std::mutex> lcoreLock(ncclNetDpdkLcoreMutex);
    if (dpdkNextPollLcore == RTE_MAX_LCORE)
      dpdkNextPollLcore = rte_lcore_id();
    lcore = rte_get_next_lcore(dpdkNextPollLcore, 1, 1);
    if (lcore == RTE_MAX_LCORE || lcore == rte_lcore_id()) {
      WARN("NET/DPDK : failed to select DPDK lcore for poll thread");
      return ncclSystemError;
    }
    if (rte_eal_remote_launch(dpdkPollThreadMain, thread, lcore) != 0) {
      WARN("NET/DPDK : rte_eal_remote_launch failed for lcore %u", lcore);
      return ncclSystemError;
    }
    dpdkNextPollLcore = lcore;
  }
  thread->lcoreId = lcore;
  return ncclSuccess;
}

// Acquire one user reference to an already-started poll worker.
static ncclResult_t dpdkAcquirePollThread(int dev) {
  dpdkPollThread *thread = &dpdkPollThreads[dev];
  std::lock_guard<std::mutex> lock(thread->mutex);
  thread->refCount++;
  return ncclSuccess;
}

static ncclResult_t dpdkReleasePollThread(int dev) {
  dpdkPollThread *thread = &dpdkPollThreads[dev];
  std::lock_guard<std::mutex> lock(thread->mutex);
  if (thread->refCount <= 0) {
    WARN("NET/DPDK : poll thread refcount underflow on dev %d", dev);
    return ncclInternalError;
  }
  thread->refCount--;
  return ncclSuccess;
}

// Bind/unbind communicators to a device poll thread.
static void dpdkAttachCommToPollThread(struct ncclNetDpdkComm *comm) {
  dpdkPollThread *thread = &dpdkPollThreads[comm->dev];
  std::lock_guard<std::mutex> lock(thread->mutex);
  thread->attachedComms.push_back(comm);
  dpdkCommAcquire(comm);
}

static void dpdkDetachCommFromPollThread(struct ncclNetDpdkComm *comm) {
  dpdkPollThread *thread = &dpdkPollThreads[comm->dev];
  std::lock_guard<std::mutex> lock(thread->mutex);
  auto &v = thread->attachedComms;
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
  dpdkActiveComms.push_back(comm);
  dpdkAttachCommToPollThread(comm);
  return ncclSuccess;
}

static ncclResult_t dpdkUnregisterComm(struct ncclNetDpdkComm *comm) {
  std::lock_guard<std::mutex> lock(ncclNetDpdkCommMutex);
  auto it = std::find(dpdkActiveComms.begin(), dpdkActiveComms.end(), comm);
  if (it != dpdkActiveComms.end())
    dpdkActiveComms.erase(it);
  dpdkDetachCommFromPollThread(comm);
  return ncclSuccess;
}

static ncclResult_t dpdkStopPollThread(int dev) {
  dpdkPollThread *thread = &dpdkPollThreads[dev];
  unsigned lcore = RTE_MAX_LCORE;
  {
    std::lock_guard<std::mutex> lock(thread->mutex);
    if (thread->refCount != 0) {
      WARN("NET/DPDK : cannot stop poll thread for dev %d with %d active references",
           dev, thread->refCount);
      return ncclInvalidUsage;
    }
    thread->stopRequested = 1;
    lcore = thread->lcoreId;
  }
  rte_eal_wait_lcore(lcore);

  {
    std::lock_guard<std::mutex> lock(thread->mutex);
    thread->lcoreId = RTE_MAX_LCORE;
    for (auto *comm : thread->attachedComms)
      dpdkCommRelease(comm);
    thread->attachedComms.clear();
  }
  return ncclSuccess;
}

// ---------------------------------------------------------------------
// NCCL net plugin entry points
// ---------------------------------------------------------------------

ncclResult_t ncclNetDpdkInit(void **ctx, uint64_t commId,
                             ncclNetCommConfig_t *config,
                             ncclDebugLogger_t logFunction,
                             ncclProfilerCallback_t profFunction) {
  ncclDpdkSetLogFunction(logFunction);
  // Shared module init for this process.
  if (dpdkInitRefCount++)
    return ncclSuccess;
  ncclProfilerFunction = profFunction;
  std::lock_guard<std::mutex> lock(ncclNetDpdkMutex);
  NCCLCHECK(dpdkInitControlInterface());
  NCCLCHECK(dpdkInitEal());
  NCCLCHECK(dpdkLoadDataIpConfig());
  NCCLCHECK(dpdkInitDataDevices());
  for (int d = 0; d < ncclNetDpdkIfs; d++) {
    NCCLCHECK(dpdkStartPollThread(d));
  }
  return ncclSuccess;
}

ncclResult_t ncclNetDpdkDevices(int *ndev) {
  *ndev = ncclNetDpdkIfs;
  return ncclSuccess;
}

ncclResult_t ncclNetDpdkMakeVDevice(int *d, ncclNetVDeviceProps_t *props) {
  if (d == NULL || props == NULL)
    return ncclInvalidArgument;

  std::lock_guard<std::mutex> lock(ncclNetDpdkMutex);
  if (ncclNetDpdkIfs < 0 || ncclNetDpdkPhysIfs < 0) {
    WARN("NET/DPDK : makeVDevice called before DPDK devices are initialized");
    return ncclInternalError;
  }
  if (props->ndevs != 0) {
    TRACE(NCCL_NET,
          "NET/DPDK : makeVDevice only supports synthetic vNIC (ndevs=0), got ndevs=%d",
          props->ndevs);
    return ncclInvalidUsage;
  }
  if (ncclNetDpdkVirtIfs >= DPDK_MAX_VDEVS || ncclNetDpdkIfs >= DPDK_MAX_DEVS) {
    WARN("NET/DPDK : Cannot allocate any more virtual devices (%d)", DPDK_MAX_VDEVS);
    return ncclInvalidUsage;
  }
  int devIx = ncclNetDpdkIfs;
  if (devIx >= (int)ncclNetDpdkDataIps.size()) {
    WARN("NET/DPDK : missing data-plane IPv4 for virtual device %d", devIx);
    return ncclInvalidUsage;
  }

  // Synthetic vNIC path: create one DPDK vdev and append it as a new NCCL net dev.
  const char *vdevPrefix = ncclGetEnv("NCCL_DPDK_VDEV_PREFIX");
  if (vdevPrefix == NULL || vdevPrefix[0] == '\0')
    vdevPrefix = "net_ring";
  const char *vdevArgs = ncclGetEnv("NCCL_DPDK_VDEV_ARGS");

  uint16_t existingPorts[DPDK_MAX_DEVS];
  int nExistingPorts = 0;
  uint16_t iterPort = 0;
  RTE_ETH_FOREACH_DEV(iterPort) {
    if (nExistingPorts == DPDK_MAX_DEVS)
      break;
    existingPorts[nExistingPorts++] = iterPort;
  }

  char vdevName[RTE_ETH_NAME_MAX_LEN] = {0};
  snprintf(vdevName, sizeof(vdevName), "%s_nccl_%d", vdevPrefix, ncclNetDpdkVirtIfs);
  int ret = rte_vdev_init(vdevName, (vdevArgs && vdevArgs[0]) ? vdevArgs : NULL);
  if (ret != 0) {
    WARN("NET/DPDK : rte_vdev_init failed for %s (ret=%d)", vdevName, ret);
    return ncclSystemError;
  }

  uint16_t portId = 0;
  if (rte_eth_dev_get_port_by_name(vdevName, &portId) != 0) {
    // Some PMDs cannot resolve by name immediately; fallback to newly added port.
    bool found = false;
    RTE_ETH_FOREACH_DEV(iterPort) {
      bool existed = false;
      for (int i = 0; i < nExistingPorts; i++) {
        if (existingPorts[i] == iterPort) {
          existed = true;
          break;
        }
      }
      if (!existed) {
        portId = iterPort;
        found = true;
        break;
      }
    }
    if (!found) {
      WARN("NET/DPDK : cannot resolve port id for virtual device %s", vdevName);
      return ncclSystemError;
    }
  }

  ncclNetDpdkDev *dev = ncclNetDpdkDevs + devIx;
  memset(dev, 0, sizeof(*dev));
  dev->addr.sin.sin_family = AF_INET;
  dev->addr.sin.sin_addr.s_addr = ncclNetDpdkDataIps[devIx];
  dev->addr.sin.sin_port = 0;
  memcpy(&dev->ctrlAddr, &ncclNetDpdkCtrlAddr, sizeof(dev->ctrlAddr));
  strncpy(dev->devName, vdevName, sizeof(dev->devName) - 1);
  dev->devName[sizeof(dev->devName) - 1] = '\0';
  dev->portId = (int)portId;
  NCCLCHECK(ncclNetDpdkGetPciPath(vdevName, &dev->pciPath));
  NCCLCHECK(dpdkInitDataPort(dev));
  NCCLCHECK(dpdkStartPollThread(devIx));

  *d = devIx;
  ncclNetDpdkIfs++;
  ncclNetDpdkVirtIfs++;
  INFO(NCCL_NET, "NET/DPDK : Made virtual device [%d] name=%s port=%d args=%s",
       *d, dev->devName, dev->portId, (vdevArgs && vdevArgs[0]) ? vdevArgs : "(null)");
  return ncclSuccess;
}

ncclResult_t ncclNetDpdkGetProperties(int dev, ncclNetProperties_t *props) {
  // Host pointer support only: no GPUDirect registration in this transport yet.
  props->name = ncclNetDpdkDevs[dev].devName;
  props->pciPath = ncclNetDpdkDevs[dev].pciPath;
  props->guid = dev;
  props->ptrSupport = NCCL_PTR_HOST;
  props->regIsGlobal = 0;
  props->forceFlush = 0;
  NCCLCHECK(ncclNetDpdkGetSpeed(dev, &props->speed));
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

  // Listen side only prepares control-plane endpoint and exported handle.
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

  NCCLCHECKGOTO(ncclSocketInit(&comm->sock, &ncclNetDpdkDevs[dev].ctrlAddr,
                               handle->magic, ncclSocketTypeNetSocket, NULL, 1),
                ret, fail);
  NCCLCHECKGOTO(ncclSocketListen(&comm->sock), ret, fail);
  NCCLCHECKGOTO(ncclSocketGetAddr(&comm->sock, &handle->connectAddr), ret,
                fail);

  rte_ether_addr_copy(&ncclNetDpdkDevs[dev].mac, &handle->mac);
  handle->dataIp = ncclNetDpdkDevs[dev].addr.sin.sin_addr.s_addr;
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

  // Non-blocking connect state machine:
  // TCP connect -> send hello -> register communicator.
  *sendComm = NULL;
  if (stage->state == ncclNetDpdkCommStateConnect)
    goto dpdk_connect_check;
  if (stage->state == ncclNetDpdkCommStateHelloSend)
    goto dpdk_hello_send;

  comm = new ncclNetDpdkComm();
  dpdkInitCommDefaults(comm);
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
  comm->maxPayload = ncdpComputeMaxPayload(ncclNetDpdkDevs[dev].mtu);
  comm->cudaDev = -1;
  comm->localIp = ncclNetDpdkDevs[dev].addr.sin.sin_addr.s_addr;
  comm->remoteIp = handle->dataIp;
  rte_ether_addr_copy(&ncclNetDpdkDevs[dev].mac, &comm->localMac);
  rte_ether_addr_copy(&handle->mac, &comm->remoteMac);

  NCCLCHECK(ncclSocketInit(&comm->ctrlSock, &handle->connectAddr, handle->magic,
                           ncclSocketTypeNetSocket, NULL, 1));
  stage->sock = &comm->ctrlSock;
  stage->state = ncclNetDpdkCommStateConnect;
  stage->ioOffset = 0;
  NCCLCHECK(ncclSocketConnect(&comm->ctrlSock));

dpdk_connect_check:
  NCCLCHECK(ncclSocketReady(stage->sock, &ready));
  if (!ready)
    return ncclSuccess;
  stage->state = ncclNetDpdkCommStateHelloSend;
  stage->ioOffset = 0;

dpdk_hello_send:
  dpdkHelloMsg hello;
  hello.mac = comm->localMac;
  hello.dataIp = comm->localIp;
  hello.commId = comm->commId;
  NCCLCHECK(ncclSocketProgress(NCCL_SOCKET_SEND, stage->sock, &hello,
                               sizeof(hello), &stage->ioOffset));
  if (stage->ioOffset < (int)sizeof(hello))
    return ncclSuccess;
  NCCLCHECK(dpdkRegisterComm(comm));
  NCCLCHECK(dpdkAcquirePollThread(dev));
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

  // Non-blocking accept state machine:
  // accept socket -> receive hello -> register communicator.
  *recvComm = NULL;
  if (stage->state == ncclNetDpdkCommStateAccept)
    goto dpdk_accept_check;
  if (stage->state == ncclNetDpdkCommStateHelloRecv)
    goto dpdk_hello_recv;

  rComm = new ncclNetDpdkComm();
  dpdkInitCommDefaults(rComm);
  stage->comm = rComm;
  rComm->dev = lComm->dev;
  rComm->portId = ncclNetDpdkDevs[rComm->dev].portId;
  rComm->pool = ncclNetDpdkDevs[rComm->dev].pool;
  rComm->commId = lComm->commId;
  rComm->maxPayload = ncdpComputeMaxPayload(ncclNetDpdkDevs[rComm->dev].mtu);
  rComm->cudaDev = -1;
  rComm->localIp = ncclNetDpdkDevs[rComm->dev].addr.sin.sin_addr.s_addr;
  rte_ether_addr_copy(&ncclNetDpdkDevs[rComm->dev].mac, &rComm->localMac);

  NCCLCHECK(ncclCalloc(&sock, 1));
  NCCLCHECK(ncclSocketInit(sock));
  stage->sock = sock;
  stage->state = ncclNetDpdkCommStateAccept;
  stage->ioOffset = 0;
  NCCLCHECK(ncclSocketAccept(sock, &lComm->sock));

dpdk_accept_check:
  NCCLCHECK(ncclSocketReady(stage->sock, &ready));
  if (!ready)
    return ncclSuccess;
  stage->state = ncclNetDpdkCommStateHelloRecv;
  stage->ioOffset = 0;

dpdk_hello_recv:
  dpdkHelloMsg hello;
  NCCLCHECK(ncclSocketProgress(NCCL_SOCKET_RECV, stage->sock, &hello,
                               sizeof(hello), &stage->ioOffset));
  if (stage->ioOffset < (int)sizeof(hello))
    return ncclSuccess;
  rComm->remoteMac = hello.mac;
  rComm->remoteIp = hello.dataIp;
  rComm->remoteCommId = hello.commId;
  rComm->ctrlSock = *sock;
  free(sock);
  NCCLCHECK(dpdkRegisterComm(rComm));
  NCCLCHECK(dpdkAcquirePollThread(rComm->dev));
  *recvComm = rComm;

  stage->state = ncclNetDpdkCommStateStart;
  stage->sock = NULL;
  stage->comm = NULL;
  stage->ioOffset = 0;
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
  NCCLCHECK(dpdkGetRequest(comm, NCCL_SOCKET_SEND, data, (int)size,
                             (struct ncclNetDpdkRequest **)request));
  return ncclSuccess;
}

ncclResult_t ncclNetDpdkIrecv(void *recvComm, int n, void **data, size_t *sizes,
                              int *tags, void **mhandles, void **phandles,
                              void **request) {
  struct ncclNetDpdkComm *comm = (struct ncclNetDpdkComm *)recvComm;
  if (n != 1)
    return ncclInternalError;
  NCCLCHECK(dpdkGetRequest(comm, NCCL_SOCKET_RECV, data[0],
                             (int)sizes[0],
                             (struct ncclNetDpdkRequest **)request));
  return ncclSuccess;
}

ncclResult_t ncclNetDpdkIflush(void *recvComm, int n, void **data, int *sizes,
                               void **mhandles, void **request) {
  return ncclInternalError;
}

ncclResult_t ncclNetDpdkTest(void *request, int *done, int *size) {
  // Control-plane progression entrypoint called by NCCL:
  // SEND: SEND_CTRL -> WAIT_READY -> SENDING
  // RECV: RECV_CTRL -> SEND_READY -> RECEIVING
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
      if (r->ctrlMsgOffset == 0) {
        r->ctrlMsg.magic = DPDK_CTRL_MAGIC;
        r->ctrlMsg.version = DPDK_PROTO_VERSION;
        r->ctrlMsg.type = DPDK_CTRL_SEND;
        r->ctrlMsg.srcReqId = r->reqId;
        r->ctrlMsg.dstReqId = 0;
        r->ctrlMsg.size = r->size;
        r->ctrlMsg.frameSize = (uint32_t)r->frameSize;
      }
      NCCLCHECK(ncclSocketProgress(NCCL_SOCKET_SEND, &comm->ctrlSock,
                                   &r->ctrlMsg, sizeof(r->ctrlMsg),
                                   &r->ctrlMsgOffset));
      if (r->ctrlMsgOffset < (int)sizeof(r->ctrlMsg))
        return ncclSuccess;
      r->state.store(DPDK_REQ_WAIT_READY, std::memory_order_release);
      r->ctrlMsgOffset = 0;
      state = DPDK_REQ_WAIT_READY;
    }
    if (state == DPDK_REQ_WAIT_READY) {
      NCCLCHECK(ncclSocketProgress(NCCL_SOCKET_RECV, &comm->ctrlSock,
                                   &r->ctrlMsg, sizeof(r->ctrlMsg),
                                   &r->ctrlMsgOffset));
      if (r->ctrlMsgOffset < (int)sizeof(r->ctrlMsg))
        return ncclSuccess;
      if (r->ctrlMsg.magic != DPDK_CTRL_MAGIC ||
          r->ctrlMsg.version != DPDK_PROTO_VERSION ||
          r->ctrlMsg.type != DPDK_CTRL_READY ||
          r->ctrlMsg.dstReqId != r->reqId) {
        WARN("NET/DPDK : invalid ctrl READY");
        return ncclInvalidUsage;
      }
      r->peerReqId = r->ctrlMsg.srcReqId;
      NCCLCHECK(dpdkBuildRequestTasks(r, r->frameSize, r->size,
                                      NCCL_SOCKET_SEND));
      r->isCompleted.store(0, std::memory_order_relaxed);
      if (r->numFrames == 0) {
        r->isCompleted.store(1, std::memory_order_release);
      }
      r->state.store(DPDK_REQ_SENDING, std::memory_order_release);
      state = DPDK_REQ_SENDING;
    }
    if (state == DPDK_REQ_SENDING) {
      if (r->isCompleted.load(std::memory_order_acquire)) {
        if (size)
          *size = r->size;
        *done = 1;
        r->state.store(DPDK_REQ_UNUSED, std::memory_order_release);
        r->inUse = 0;
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
                                   &r->ctrlMsgOffset));
      if (r->ctrlMsgOffset < (int)sizeof(r->ctrlMsg))
        return ncclSuccess;
      if (r->ctrlMsg.magic != DPDK_CTRL_MAGIC ||
          r->ctrlMsg.version != DPDK_PROTO_VERSION) {
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
      r->peerReqId = r->ctrlMsg.srcReqId;
      r->size = (int)r->ctrlMsg.size;
      r->frameSize = (int)r->ctrlMsg.frameSize;
      NCCLCHECK(dpdkBuildRequestTasks(r, r->frameSize, r->size,
                                      NCCL_SOCKET_RECV));
      r->state.store(DPDK_REQ_SEND_READY, std::memory_order_release);
      r->ctrlMsgOffset = 0;
      state = DPDK_REQ_SEND_READY;
    }
    if (state == DPDK_REQ_SEND_READY) {
      if (r->ctrlMsgOffset == 0) {
        r->ctrlMsg.magic = DPDK_CTRL_MAGIC;
        r->ctrlMsg.version = DPDK_PROTO_VERSION;
        r->ctrlMsg.type = DPDK_CTRL_READY;
        r->ctrlMsg.srcReqId = r->reqId;
        r->ctrlMsg.dstReqId = r->peerReqId;
        r->ctrlMsg.size = r->size;
        r->ctrlMsg.frameSize = (uint32_t)r->frameSize;
      }
      NCCLCHECK(ncclSocketProgress(NCCL_SOCKET_SEND, &comm->ctrlSock,
                                   &r->ctrlMsg, sizeof(r->ctrlMsg),
                                   &r->ctrlMsgOffset));
      if (r->ctrlMsgOffset < (int)sizeof(r->ctrlMsg))
        return ncclSuccess;
      r->isCompleted.store(0, std::memory_order_relaxed);
      if (r->numFrames == 0) {
        r->isCompleted.store(1, std::memory_order_release);
      }
      r->state.store(DPDK_REQ_RECEIVING, std::memory_order_release);
      state = DPDK_REQ_RECEIVING;
    }
    if (state == DPDK_REQ_RECEIVING) {
      if (r->isCompleted.load(std::memory_order_acquire)) {
        if (size)
          *size = r->size;
        *done = 1;
        r->state.store(DPDK_REQ_UNUSED, std::memory_order_release);
        r->inUse = 0;
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
  std::lock_guard<std::mutex> lock(ncclNetDpdkMutex);
  if (dpdkInitRefCount <= 0)
    return ncclSuccess;
  if (--dpdkInitRefCount > 0)
    return ncclSuccess;
  for (int d = 0; d < ncclNetDpdkIfs; d++) {
    NCCLCHECK(dpdkStopPollThread(d));
  }
  return ncclSuccess;
}

ncclNet_t ncclNetDpdk = {
    "Dpdk",
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
    ncclNetDpdkMakeVDevice,
    ncclNetDpdkFinalize,
    NULL,
};

// External plugin entry point expected by NCCL plugin loader.
extern "C" __attribute__((visibility("default"))) ncclNet_t ncclNetPlugin_v11 = {
    "Dpdk",
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
    ncclNetDpdkMakeVDevice,
    ncclNetDpdkFinalize,
    NULL,
};
