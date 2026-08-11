/*************************************************************************
 * Copyright (c) 2016-2022, NVIDIA CORPORATION. All rights reserved.
 *
 * See LICENSE.txt for license information
 ************************************************************************/

#include "ncdp.h"
#include "net.h"
#include "net_dpdk_utils.h"
#include "plugin_compat.h"
#include "socket_compat.h"

#include <arpa/inet.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <errno.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <limits.h>
#include <mutex>
#include <net/if.h>
#include <netinet/in.h>
#include <stddef.h>
#include <stdint.h>
#include <string.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>

#include <rte_byteorder.h>
#include <rte_cycles.h>
#include <rte_eal.h>
#include <rte_ethdev.h>
#include <rte_ether.h>
#include <rte_mbuf.h>
#include <rte_mempool.h>
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
#define DPDK_FAULT_MAGIC 0x4e434446U // "NCDF"
#define DPDK_PROTO_VERSION 5

#define DPDK_RX_BURST 32
#define DPDK_TX_BURST 32
#define DPDK_MBUF_COUNT 8192
#define DPDK_MBUF_CACHE 256
#define DPDK_RX_DESC 1024
#define DPDK_TX_DESC 1024
#define DPDK_INVALID_SEQ UINT32_MAX
#define DPDK_INVALID_DEV -1
#define DPDK_MAX_CTRL_LANES 2
#define DPDK_MAX_CTRL_PAIRS 2

#ifndef NCCL_DPDK_FAULT_INJECT
#define NCCL_DPDK_FAULT_INJECT 0
#endif

// Runtime knobs used by this DPDK transport implementation.
// Data plane is DPDK/UDP; control plane remains ncclSocket-based TCP.
NCCL_DPDK_PARAM(DpdkFrameWindow, "DPDK_FRAME_WINDOW", 64)
NCCL_DPDK_PARAM(DpdkMaxTasksPerRequest, "DPDK_MAX_TASKS_PER_REQUEST", 8)
NCCL_DPDK_PARAM(DpdkMinTaskFrames, "DPDK_MIN_TASK_FRAMES", 4096)
NCCL_DPDK_PARAM(DpdkAckEvery, "DPDK_ACK_EVERY", 32)
NCCL_DPDK_PARAM(DpdkAckDelayUs, "DPDK_ACK_DELAY_US", 1000)
NCCL_DPDK_PARAM(DpdkRetxTimeoutUs, "DPDK_RETX_TIMEOUT_US", 500000)
NCCL_DPDK_PARAM(DpdkLbBalance, "DPDK_LB_BALANCE", 0)
NCCL_DPDK_PARAM(DpdkLbBusyTasks, "DPDK_LB_BUSY_TASKS", 8)
NCCL_DPDK_PARAM(DpdkLbMinTasks, "DPDK_LB_MIN_TASKS", 4)
NCCL_DPDK_PARAM(DpdkFaultCheckMs, "DPDK_FAULT_CHECK_MS", 100)
NCCL_DPDK_PARAM(DpdkFaultDownCount, "DPDK_FAULT_DOWN_COUNT", 3)
NCCL_DPDK_PARAM(DpdkFaultSendTimeoutMs, "DPDK_FAULT_SEND_TIMEOUT_MS", 50)
NCCL_DPDK_PARAM(DpdkFaultManagerPollMs, "DPDK_FAULT_MANAGER_POLL_MS", 100)
#if NCCL_DPDK_FAULT_INJECT
NCCL_DPDK_PARAM(DpdkFaultInjectDev, "DPDK_FAULT_INJECT_DEV", 0)
NCCL_DPDK_PARAM(DpdkFaultInjectIntervalMs, "DPDK_FAULT_INJECT_INTERVAL_MS",
                100)
NCCL_DPDK_PARAM(DpdkFaultInjectSleepMs, "DPDK_FAULT_INJECT_SLEEP_MS", 100)
#endif

// Control-plane messages exchanged over ncclSocket.
// SEND announces a new transfer, READY binds the peer request id.
enum dpdkCtrlType {
  DPDK_CTRL_SEND = 1,
  DPDK_CTRL_READY = 2,
};

enum dpdkHelloChannel {
  DPDK_HELLO_CHANNEL_CTRL = 1,
  DPDK_HELLO_CHANNEL_FAULT = 2,
};

enum dpdkFaultType {
  DPDK_FAULT_NOTIFY = 1,
};

typedef struct __attribute__((packed)) dpdkCtrlLane {
  uint16_t dev;
  uint16_t busyQ16;
  uint32_t speedMbps;
  struct rte_ether_addr mac;
  uint32_t dataIp;
} dpdkCtrlLane;

typedef struct __attribute__((packed)) dpdkCtrlLanePair {
  uint8_t sendLane;
  uint8_t recvLane;
  uint16_t weight;
} dpdkCtrlLanePair;

// Fixed control header carried on the control socket.
typedef struct __attribute__((packed)) dpdkCtrlMsg {
  uint32_t magic;
  uint16_t version;
  uint16_t type;
  uint32_t srcReqId;
  uint32_t dstReqId;
  uint32_t size;
  uint32_t frameSize;
  uint32_t numTasks;
  uint16_t laneCount;
  uint16_t pairCount;
  dpdkCtrlLane lanes[DPDK_MAX_CTRL_LANES];
  dpdkCtrlLanePair pairs[DPDK_MAX_CTRL_PAIRS];
  uint32_t pairSeed;
} dpdkCtrlMsg;

// Data-plane identity exchanged once during connect/accept.
typedef struct __attribute__((packed)) dpdkHelloMsg {
  struct rte_ether_addr mac;
  uint32_t commId;
  uint32_t dataIp;
  uint16_t channel;
  uint16_t reserved;
} dpdkHelloMsg;

typedef struct __attribute__((packed)) dpdkFaultMsg {
  uint32_t magic;
  uint16_t version;
  uint16_t type;
  uint32_t srcCommId;
  uint32_t dstCommId;
  uint32_t srcReqId;
  uint32_t dstReqId;
  uint32_t op;
  uint32_t failedDev;
  uint32_t newDev;
  uint64_t epoch;
  uint64_t taskMask;
  dpdkCtrlLane newLane;
} dpdkFaultMsg;

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
  // Last successful TX timestamp for timeout-based retransmission.
  uint64_t lastTxTsc;
  // Number of successful TX submissions for this frame, including retransmits.
  uint32_t txCount;
  // Payload bytes in this frame slot (last frame may be shorter).
  uint16_t len;
  uint16_t state;
};

struct dpdkTaskEndpoint {
  int dev;
  int portId;
  struct rte_mempool *pool;
  struct rte_ether_addr localMac;
  struct rte_ether_addr remoteMac;
  uint32_t localIp;
  uint32_t remoteIp;
};

struct dpdkSendTask {
  uint32_t taskId;
  int attachedDev;
  int isAttached;
  struct dpdkTaskEndpoint endpoint;
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
  // Task-local cache of app-owned mbufs used to build DATA bursts.
  struct rte_mbuf **txMbufs;
  int txMbufCap;
  int txMbufCount;
};

struct dpdkRecvTask {
  // First global frame seq covered by this task.
  uint32_t baseSeq;
  // Task index echoed on data/ack packets.
  uint32_t taskId;
  int attachedDev;
  int isAttached;
  struct dpdkTaskEndpoint endpoint;
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
  // Active connect path (client) for ctrl socket.
  ncclNetDpdkCommStateConnectCtrl = 1,
  // Client sends ctrl hello payload.
  ncclNetDpdkCommStateHelloSendCtrl = 2,
  // Active connect path (client) for fault socket.
  ncclNetDpdkCommStateConnectFault = 3,
  // Client sends fault hello payload.
  ncclNetDpdkCommStateHelloSendFault = 4,
  // Active accept path (server).
  ncclNetDpdkCommStateAccept = 5,
  // Server receives hello payload.
  ncclNetDpdkCommStateHelloRecv = 6,
};

struct ncclNetDpdkCommStage {
  // Incremental connect/accept progress.
  enum ncclNetDpdkCommState state;
  struct ncclSocket *sock;
  struct ncclNetDpdkComm *comm;
  int ioOffset;
  int acceptedChannelMask;
  dpdkHelloMsg hello;
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
  // Async terminal error observed by ncclNetDpdkTest.
  std::atomic<int> errorCode;
  struct ncclNetDpdkComm *comm;
  // Common transfer geometry shared by send and recv paths.
  int frameSize;
  int numFrames;
  int numTasks;
  std::atomic<int> completedTasks;
  uint16_t localLaneCount;
  uint16_t peerLaneCount;
  uint16_t pairCount;
  uint16_t pairWeightSum;
  uint32_t pairSeed;
  dpdkCtrlLane localLanes[DPDK_MAX_CTRL_LANES];
  dpdkCtrlLane peerLanes[DPDK_MAX_CTRL_LANES];
  dpdkCtrlLanePair pairs[DPDK_MAX_CTRL_PAIRS];
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
  struct ncclSocket faultSock;
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
  int faultRxOffset;
  uint64_t faultLastEpoch;
  dpdkFaultMsg faultRxMsg;
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
  int speedMbps;
};

struct dpdkSendTaskRef {
  struct ncclNetDpdkRequest *req;
  struct dpdkSendTask *task;
};

struct dpdkRecvTaskRef {
  struct ncclNetDpdkRequest *req;
  struct dpdkRecvTask *task;
};

struct dpdkPollThread {
  std::mutex mutex;
  int portId;
  int dev;
  // Number of tasks attached to this worker.
  int refCount;
  int stopRequested;
  unsigned lcoreId;
  std::vector<struct dpdkSendTaskRef> sendTasks;
  std::vector<struct dpdkRecvTaskRef> recvTasks;
  uint32_t busyQ16;
  uint64_t txAttempts;
  uint64_t txDrops;
  uint64_t rxPkts;
  uint64_t lastLoadTsc;
  uint64_t lastLinkCheckTsc;
  int linkDownCount;
  int faultReported;
#if NCCL_DPDK_FAULT_INJECT
  uint64_t lastFaultInjectTsc;
#endif
};

struct dpdkFaultEvent {
  int failedDev;
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
// Optional explicit control-plane IPv4 loaded from dpdknet.conf.
static uint32_t ncclNetDpdkControlIp;
static uint64_t dpdkTscHz;
// All currently registered send/recv communicators.
static std::vector<ncclNetDpdkComm *> dpdkActiveComms;
// Round-robin cursor for picking next poll thread lcore.
static unsigned dpdkNextPollLcore = RTE_MAX_LCORE;
// Parsed data-plane IPv4 list loaded from config file.
static std::vector<uint32_t> ncclNetDpdkDataIps;
// Per-device polling worker state and attachments.
static dpdkPollThread dpdkPollThreads[DPDK_MAX_DEVS];
// Local fault events raised by poll threads.
static std::mutex dpdkFaultMutex;
static std::condition_variable dpdkFaultCv;
static std::vector<dpdkFaultEvent> dpdkFaultEvents;
static std::thread dpdkFaultManagerThread;
static std::atomic<int> dpdkFaultManagerRunning(0);
static std::atomic<int> dpdkFaultStop(0);
static std::atomic<uint64_t> dpdkFaultEpoch(1);

static void dpdkReleaseRequestFrames(struct ncclNetDpdkRequest *r);
static void dpdkDetachRequestTasks(struct ncclNetDpdkRequest *req);
static ncclResult_t ncclNetDpdkGetSpeed(int dev, int *speed);
static ncclResult_t dpdkStartFaultManager();
static void dpdkStopFaultManager();
static void dpdkRaiseLocalFaultEvent(int failedDev);
static bool dpdkIsDevLinkUp(int dev);

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
  char ifName[MAX_IF_NAME_SIZE + 1] = {0};

  if (ncclNetDpdkControlIp != 0) {
    struct ifaddrs *interfaces = NULL;
    if (getifaddrs(&interfaces) != 0) {
      WARN("NET/DPDK : getifaddrs failed for control plane : %s",
           strerror(errno));
      return ncclSystemError;
    }

    for (struct ifaddrs *interface = interfaces; interface != NULL;
         interface = interface->ifa_next) {
      if (interface->ifa_addr == NULL ||
          interface->ifa_addr->sa_family != AF_INET)
        continue;
      if (!(interface->ifa_flags & IFF_RUNNING))
        continue;

      struct sockaddr_in *addr =
          (struct sockaddr_in *)interface->ifa_addr;
      if (addr->sin_addr.s_addr != ncclNetDpdkControlIp)
        continue;

      memset(&ncclNetDpdkCtrlAddr, 0, sizeof(ncclNetDpdkCtrlAddr));
      memcpy(&ncclNetDpdkCtrlAddr.sin, addr, sizeof(*addr));
      ncclNetDpdkCtrlAddr.sin.sin_port = 0;
      strncpy(ifName, interface->ifa_name, MAX_IF_NAME_SIZE);
      ifName[MAX_IF_NAME_SIZE] = '\0';
      freeifaddrs(interfaces);

      char line[SOCKET_NAME_MAXLEN + 1];
      char ipLine[INET_ADDRSTRLEN];
      INFO(NCCL_INIT | NCCL_NET,
           "NET/DPDK : control plane using %s:%s (control_ip=%s)", ifName,
           ncclSocketToString(&ncclNetDpdkCtrlAddr, line),
           dpdkIpv4ToString(ncclNetDpdkControlIp, ipLine, sizeof(ipLine)));
      return ncclSuccess;
    }

    char ipLine[INET_ADDRSTRLEN];
    WARN("NET/DPDK : configured control_ip=%s not found on any running Linux IPv4 interface",
         dpdkIpv4ToString(ncclNetDpdkControlIp, ipLine, sizeof(ipLine)));
    freeifaddrs(interfaces);
    return ncclInvalidUsage;
  }

  int nIfs = 0;
  memset(&ncclNetDpdkCtrlAddr, 0, sizeof(ncclNetDpdkCtrlAddr));
  NCCLCHECK(ncclFindInterfaces(ifName, &ncclNetDpdkCtrlAddr, MAX_IF_NAME_SIZE,
                               1, &nIfs));
  if (nIfs <= 0) {
    WARN("NET/DPDK : no Linux socket interface available for control plane");
    return ncclInternalError;
  }

  char line[SOCKET_NAME_MAXLEN + 1];
  INFO(NCCL_INIT | NCCL_NET, "NET/DPDK : control plane using %s:%s (default)",
       ifName, ncclSocketToString(&ncclNetDpdkCtrlAddr, line));
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

    struct rte_eth_link link;
    rte_eth_link_get(portId, &link);

    if (link.link_status != RTE_ETH_LINK_UP) {
      INFO(NCCL_INIT | NCCL_NET,
           "NET/DPDK : ignoring port %d (%s) because link is down",
           portId, portName);
      continue;
    }

    if (matched >= (int)ncclNetDpdkDataIps.size()) {
      WARN("NET/DPDK : missing data-plane IPv4 for device %d (need at least %d entries)",
           matched, matched + 1);
      return ncclInvalidUsage;
    }

    char *pciPath = NULL;
    NCCLCHECK(ncclNetDpdkGetPciPath(portName, &pciPath));
    ncclNetDpdkDev *dev = ncclNetDpdkDevs + matched;
    memset(dev, 0, sizeof(*dev));
    dev->addr.sin.sin_family = AF_INET;
    dev->addr.sin.sin_addr.s_addr = ncclNetDpdkDataIps[matched];
    dev->addr.sin.sin_port = 0;
    memcpy(&dev->ctrlAddr, &ncclNetDpdkCtrlAddr, sizeof(dev->ctrlAddr));
    strncpy(dev->devName, portName, sizeof(dev->devName) - 1);
    dev->devName[sizeof(dev->devName) - 1] = '\0';
    dev->pciPath = pciPath;
    dev->portId = (int)portId;
    NCCLCHECK(dpdkInitDataPort(dev));
    NCCLCHECK(ncclNetDpdkGetSpeed(matched, &dev->speedMbps));
    matched++;
  }

  if (rte_eth_dev_count_avail() > 0 && matched <= 0) {
    WARN("NET/DPDK : no DPDK data-plane device with link up available");
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
  comm->ctrlSock.fd = -1;
  comm->faultSock.fd = -1;
  comm->faultRxOffset = 0;
  comm->faultLastEpoch = 0;
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
        if (s->tasks[i].txMbufs) {
          for (int m = 0; m < s->tasks[i].txMbufCount; m++) {
            if (s->tasks[i].txMbufs[m])
              rte_pktmbuf_free(s->tasks[i].txMbufs[m]);
          }
          free(s->tasks[i].txMbufs);
        }
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
  r->errorCode.store(0, std::memory_order_relaxed);
  r->frameSize = 0;
  r->numFrames = 0;
  r->numTasks = 0;
  r->completedTasks.store(0, std::memory_order_relaxed);
  r->localLaneCount = 0;
  r->peerLaneCount = 0;
  r->pairCount = 0;
  r->pairWeightSum = 0;
  r->pairSeed = 0;
  memset(r->localLanes, 0, sizeof(r->localLanes));
  memset(r->peerLanes, 0, sizeof(r->peerLanes));
  memset(r->pairs, 0, sizeof(r->pairs));
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

static inline void dpdkInitTaskEndpointFromDev(struct dpdkTaskEndpoint *ep,
                                               int localDev,
                                               const struct rte_ether_addr *remoteMac,
                                               uint32_t remoteIp) {
  memset(ep, 0, sizeof(*ep));
  if (localDev < 0 || localDev >= ncclNetDpdkIfs) {
    ep->dev = DPDK_INVALID_DEV;
    ep->portId = -1;
    return;
  }
  ep->dev = localDev;
  ep->portId = ncclNetDpdkDevs[localDev].portId;
  ep->pool = ncclNetDpdkDevs[localDev].pool;
  ep->localMac = ncclNetDpdkDevs[localDev].mac;
  ep->localIp = ncclNetDpdkDevs[localDev].addr.sin.sin_addr.s_addr;
  ep->remoteIp = remoteIp;
  if (remoteMac)
    ep->remoteMac = *remoteMac;
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
      r->errorCode.store(0, std::memory_order_relaxed);
      r->comm = comm;
      r->reqId = ++comm->nextReqId;
      if (r->reqId == 0)
        r->reqId = ++comm->nextReqId;
      r->peerReqId = 0;
      r->localLaneCount = 0;
      r->peerLaneCount = 0;
      r->pairCount = 0;
      r->pairWeightSum = 0;
      r->pairSeed = 0;
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
      memset(task, 0, sizeof(*task));
      task->taskId = t;
      task->attachedDev = r->comm ? r->comm->dev : DPDK_INVALID_DEV;
      task->isAttached = 0;
      if (r->comm)
        dpdkInitTaskEndpointFromDev(&task->endpoint, r->comm->dev,
                                    &r->comm->remoteMac, r->comm->remoteIp);
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
      task->txMbufCap = std::max(1, std::min(task->frameSlots, DPDK_TX_BURST * 4));
      task->txMbufCount = 0;
      NCCLCHECK(ncclCalloc(&task->frames, task->frameSlots));
      NCCLCHECK(ncclCalloc(&task->ackedBitmap, task->ackedBitmapWords));
      NCCLCHECK(ncclCalloc(&task->txMbufs, task->txMbufCap));
      for (int i = 0; i < task->frameSlots; i++) {
        task->frames[i].seq = DPDK_INVALID_SEQ;
        task->frames[i].lastTxTsc = 0;
        task->frames[i].txCount = 0;
        task->frames[i].len = 0;
        task->frames[i].state = DPDK_FRAME_EMPTY;
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
      task->taskId = t;
      task->attachedDev = r->comm ? r->comm->dev : DPDK_INVALID_DEV;
      task->isAttached = 0;
      if (r->comm)
        dpdkInitTaskEndpointFromDev(&task->endpoint, r->comm->dev,
                                    &r->comm->remoteMac, r->comm->remoteIp);
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
        task->frames[i].lastTxTsc = 0;
        task->frames[i].txCount = 0;
        task->frames[i].len = 0;
        task->frames[i].state = DPDK_FRAME_EMPTY;
      }
    }
    v->recvDoneFrames = 0;
  } else {
    WARN("NET/DPDK : invalid op %d in dpdkBuildRequestTasks", op);
    dpdkReleaseRequestFrames(r);
    return ncclInternalError;
  }

  r->completedTasks.store(0, std::memory_order_relaxed);
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

static inline void dpdkRecordTxResult(int dev, bool success) {
  if (dev < 0 || dev >= ncclNetDpdkIfs)
    return;
  dpdkPollThread *thread = &dpdkPollThreads[dev];
  __atomic_fetch_add(&thread->txAttempts, 1ull, __ATOMIC_RELAXED);
  if (!success)
    __atomic_fetch_add(&thread->txDrops, 1ull, __ATOMIC_RELAXED);
}

// Build one NCDP packet and submit it on the port.
static bool dpdkSendPacket(const struct dpdkTaskEndpoint *ep, uint16_t flags,
                           uint32_t dstCommId, uint32_t srcCommId,
                           uint32_t srcReqId, uint32_t dstReqId,
                           uint32_t taskId, uint32_t seq,
                           const void *payload, uint16_t len) {
  if (ep == NULL || ep->pool == NULL || ep->portId < 0 || ep->dev < 0)
    return false;
  uint16_t udpPort =
      dpdkSelectUdpPort(dstCommId, srcCommId, srcReqId, dstReqId, taskId);
  struct rte_mbuf *mbuf = rte_pktmbuf_alloc(ep->pool);
  if (mbuf == NULL)
    return false;
  if (!ncdpBuildPacket(mbuf, &ep->localMac, &ep->remoteMac, ep->localIp,
                       ep->remoteIp, flags, dstCommId, srcCommId, srcReqId,
                       dstReqId, taskId, seq, payload, len, udpPort)) {
    rte_pktmbuf_free(mbuf);
    dpdkRecordTxResult(ep->dev, false);
    return false;
  }
  struct rte_mbuf *txPkts[1] = {mbuf};
  int sent = rte_eth_tx_burst((uint16_t)ep->portId, 0, txPkts, 1);
  if (sent < 1) {
    rte_pktmbuf_free(mbuf);
    dpdkRecordTxResult(ep->dev, false);
    return false;
  }
  dpdkRecordTxResult(ep->dev, true);
  return true;
}

static int dpdkRefillTaskTxMbufs(struct dpdkSendTask *task, int want) {
  if (task == NULL || task->endpoint.pool == NULL || task->txMbufs == NULL ||
      task->txMbufCap <= 0)
    return 0;
  int room = task->txMbufCap - task->txMbufCount;
  if (room <= 0)
    return 0;
  want = std::min(want, room);
  if (want <= 0)
    return 0;

  struct rte_mbuf **dst = task->txMbufs + task->txMbufCount;
  if (rte_pktmbuf_alloc_bulk(task->endpoint.pool, dst, (unsigned)want) == 0) {
    task->txMbufCount += want;
    return want;
  }

  int got = 0;
  while (got < want) {
    struct rte_mbuf *mbuf = rte_pktmbuf_alloc(task->endpoint.pool);
    if (mbuf == NULL)
      break;
    task->txMbufs[task->txMbufCount++] = mbuf;
    got++;
  }
  return got;
}

static inline struct rte_mbuf *dpdkGetTaskTxMbuf(struct dpdkSendTask *task) {
  if (task == NULL)
    return NULL;
  if (task->txMbufCount <= 0)
    dpdkRefillTaskTxMbufs(task, std::min(task->txMbufCap, DPDK_TX_BURST));
  if (task->txMbufCount <= 0)
    return NULL;
  return task->txMbufs[--task->txMbufCount];
}

static inline void dpdkReturnTaskTxMbuf(struct dpdkSendTask *task,
                                        struct rte_mbuf *mbuf) {
  if (mbuf == NULL)
    return;
  if (task != NULL && task->txMbufs != NULL &&
      task->txMbufCount < task->txMbufCap) {
    task->txMbufs[task->txMbufCount++] = mbuf;
  } else {
    rte_pktmbuf_free(mbuf);
  }
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

static inline uint64_t dpdkUsToCycles(int us) {
  if (us <= 0)
    return 0;
  uint64_t hz = dpdkTscHz ? dpdkTscHz : rte_get_tsc_hz();
  if (hz == 0)
    return 0;
  uint64_t cycles = (uint64_t)us * hz / 1000000ULL;
  return cycles == 0 ? 1 : cycles;
}

static inline uint64_t dpdkAckDelayCycles() {
  return dpdkUsToCycles(ncclParamDpdkAckDelayUs());
}

static inline uint64_t dpdkRetxDelayCycles() {
  return dpdkUsToCycles(ncclParamDpdkRetxTimeoutUs());
}

// Mark recv request done only after all payload is copied and final ACK state
// is fully drained.
static inline void dpdkTryFinishRecvAfterAck(struct ncclNetDpdkRequest *req) {
  struct dpdkRecvRequestState *v = &req->recv;
  if (req->numFrames <= 0 || v->tasks == NULL)
    return;
  if (req->completedTasks.load(std::memory_order_acquire) < req->numTasks)
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
static inline bool dpdkSendTaskAck(struct ncclNetDpdkRequest *req,
                                   struct dpdkRecvTask *task,
                                   uint32_t ackLocalSeq) {
  if (req->peerReqId == 0 || req->reqId == 0 || req->comm == NULL)
    return false;
  struct ncclNetDpdkComm *comm = req->comm;
  uint32_t ackSeq = task->baseSeq + ackLocalSeq;
  if (!dpdkSendPacket(&task->endpoint, NCDP_FLAG_ACK, comm->remoteCommId,
                      comm->commId,
                      /*srcReqId=*/req->reqId,
                      /*dstReqId=*/req->peerReqId,
                      /*taskId=*/task->taskId, ackSeq, NULL, 0)) {
    return false;
  }
  task->ackSentSeq = ackSeq;
  task->ackPending = 0;
  task->ackDeadlineTsc = 0;
  dpdkTryFinishRecvAfterAck(req);
  return true;
}

static inline void dpdkDeferTaskAck(struct dpdkRecvTask *task,
                                    uint64_t nowCycles) {
  uint64_t delayCycles = dpdkAckDelayCycles();
  if (delayCycles == 0)
    delayCycles = 1;
  task->ackPending = std::max<uint32_t>(task->ackPending, 1);
  task->ackSentSeq = DPDK_INVALID_SEQ;
  task->ackDeadlineTsc = nowCycles + delayCycles;
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

static inline bool dpdkTaskFrameAcked(const struct dpdkSendTask *task,
                                      uint32_t localSeq) {
  if (task == NULL || task->ackedBitmap == NULL || task->frames == NULL ||
      task->frameSlots <= 0)
    return false;
  int slot = (int)(localSeq % (uint32_t)task->frameSlots);
  if (task->frames[slot].seq != localSeq)
    return false;
  uint32_t word = (uint32_t)slot >> 6;
  uint32_t bit = (uint32_t)slot & 63u;
  return ((task->ackedBitmap[word] >> bit) & 1ull) != 0ull;
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
    meta->lastTxTsc = 0;
    meta->txCount = 0;
    meta->len = 0;
    meta->state = DPDK_FRAME_EMPTY;
    task->sndUna++;
  }
}

// Apply one task-scoped cumulative ACK to sender window state.
static inline void dpdkApplyTaskAck(struct ncclNetDpdkRequest *req,
                                    uint32_t taskId, uint32_t ackSeq,
                                    int rxDev) {
  struct dpdkSendRequestState *s = &req->send;
  if (s->tasks == NULL || req->numTasks <= 0 || req->numFrames <= 0)
    return;
  if (taskId >= (uint32_t)req->numTasks)
    return;
  struct dpdkSendTask *task = s->tasks + taskId;
  if (task->attachedDev != rxDev)
    return;
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
    int doneTasks =
        req->completedTasks.fetch_add(1, std::memory_order_acq_rel) + 1;

    if (doneTasks >= req->numTasks) {
      req->isCompleted.store(1, std::memory_order_release);
    }
  }
}

// Periodically flush delayed ACKs for each recv task.
static inline void dpdkProgressRecvTask(struct ncclNetDpdkRequest *req,
                                        struct dpdkRecvTask *task) {
  if (req->state.load(std::memory_order_acquire) != DPDK_REQ_RECEIVING ||
      task == NULL)
    return;
  uint64_t nowCycles = rte_get_tsc_cycles();
  uint64_t delayCycles = dpdkAckDelayCycles();
  if (task->ackPending == 0 || task->rcvNxt == 0)
    return;
  uint32_t ackSeq = task->baseSeq + (task->rcvNxt - 1);
  if (ackSeq == task->ackSentSeq) {
    task->ackPending = 0;
    task->ackDeadlineTsc = 0;
    return;
  }
  if (delayCycles > 0 && task->ackDeadlineTsc != 0 &&
      nowCycles < task->ackDeadlineTsc)
    return;
  if (!dpdkSendTaskAck(req, task, task->rcvNxt - 1))
    dpdkDeferTaskAck(task, nowCycles);
}

// Packet demux for the data plane:
// ACK updates sender windows, DATA copies payload and may schedule delayed ACK.
static void dpdkHandleRxPacket(int rxDev, const ncdpHdr *hdr,
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
      dpdkApplyTaskAck(req, hdr->taskId, hdr->seq, rxDev);
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
    if (task->attachedDev != rxDev)
      return;
    if (task->frames == NULL || task->frameSlots <= 0 || task->numFrames <= 0)
      return;

    uint32_t seq = hdr->seq;
    if (seq < task->baseSeq)
      return;
    uint32_t localSeq = seq - task->baseSeq;
    if (localSeq >= (uint32_t)task->numFrames)
      return;
    if (localSeq < task->rcvNxt) {
      // Duplicate retransmission for data already committed. Re-ACK the
      // current contiguous prefix so the sender can recover from ACK loss.
      if (task->rcvNxt > 0) {
        uint64_t nowCycles = rte_get_tsc_cycles();
        if (!dpdkSendTaskAck(req, task, task->rcvNxt - 1))
          dpdkDeferTaskAck(task, nowCycles);
      }
      return;
    }
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
      newlyCompleted = true;
    } else {
      return;
    }

    if (newlyCompleted) {
      task->completedFrames++;
      v->recvDoneFrames++;
      if (task->completedFrames >= task->numFrames && !task->isDone) {
        task->isDone = 1;
        req->completedTasks.fetch_add(1, std::memory_order_acq_rel);
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
        if (!dpdkSendTaskAck(req, task, task->rcvNxt - 1))
          dpdkDeferTaskAck(task, nowCycles);
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

static inline bool dpdkBuildTaskDataMbuf(struct ncclNetDpdkRequest *req,
                                         struct dpdkSendTask *task,
                                         uint32_t localSeq,
                                         struct rte_mbuf *mbuf) {
  if (req == NULL || task == NULL || req->comm == NULL || task->frames == NULL ||
      task->frameSlots <= 0 || mbuf == NULL)
    return false;
  int slot = (int)(localSeq % (uint32_t)task->frameSlots);
  struct dpdkFrameMeta *meta = task->frames + slot;
  if (meta->seq != localSeq)
    return false;

  struct ncclNetDpdkComm *comm = req->comm;
  uint32_t seq = task->baseSeq + localSeq;
  uint64_t payloadOffset = (uint64_t)seq * (uint64_t)req->frameSize;
  if (payloadOffset > (uint64_t)req->size)
    return false;

  uint16_t udpPort = dpdkSelectUdpPort(comm->remoteCommId, comm->commId,
                                       req->reqId, req->peerReqId,
                                       task->taskId);
  return ncdpBuildPacket(mbuf, &task->endpoint.localMac,
                         &task->endpoint.remoteMac, task->endpoint.localIp,
                         task->endpoint.remoteIp, NCDP_FLAG_DATA,
                         comm->remoteCommId, comm->commId,
                         /*srcReqId=*/req->reqId,
                         /*dstReqId=*/req->peerReqId,
                         task->taskId, seq,
                         (char *)req->data + (size_t)payloadOffset, meta->len,
                         udpPort);
}

static int dpdkSendTaskDataBurst(struct ncclNetDpdkRequest *req,
                                 struct dpdkSendTask *task,
                                 const uint32_t *localSeqs, int count,
                                 uint64_t nowCycles) {
  if (req == NULL || task == NULL || task->endpoint.portId < 0 ||
      task->endpoint.dev < 0 || localSeqs == NULL || count <= 0)
    return 0;
  if (count > DPDK_TX_BURST)
    count = DPDK_TX_BURST;

  struct rte_mbuf *pkts[DPDK_TX_BURST];
  struct dpdkFrameMeta *metas[DPDK_TX_BURST];
  int prepared = 0;
  for (int i = 0; i < count; i++) {
    uint32_t localSeq = localSeqs[i];
    int slot = (int)(localSeq % (uint32_t)task->frameSlots);
    struct dpdkFrameMeta *meta = task->frames + slot;
    struct rte_mbuf *mbuf = dpdkGetTaskTxMbuf(task);
    if (mbuf == NULL)
      break;
    if (!dpdkBuildTaskDataMbuf(req, task, localSeq, mbuf)) {
      dpdkReturnTaskTxMbuf(task, mbuf);
      break;
    }
    pkts[prepared] = mbuf;
    metas[prepared] = meta;
    prepared++;
  }
  if (prepared == 0)
    return 0;

  int sent = rte_eth_tx_burst((uint16_t)task->endpoint.portId, 0, pkts,
                              (uint16_t)prepared);
  for (int i = 0; i < sent; i++) {
    metas[i]->lastTxTsc = nowCycles != 0 ? nowCycles : rte_get_tsc_cycles();
    metas[i]->txCount++;
    dpdkRecordTxResult(task->endpoint.dev, true);
  }
  for (int i = sent; i < prepared; i++) {
    dpdkReturnTaskTxMbuf(task, pkts[i]);
    dpdkRecordTxResult(task->endpoint.dev, false);
  }
  return sent;
}

static int dpdkRetransmitSendTaskWindow(struct ncclNetDpdkRequest *req,
                                        struct dpdkSendTask *task,
                                        int budget,
                                        uint64_t nowCycles) {
  uint64_t retxDelayCycles = dpdkRetxDelayCycles();
  if (retxDelayCycles == 0 || budget <= 0 || task == NULL ||
      task->frames == NULL || task->frameSlots <= 0 ||
      task->sndUna >= task->sndNxt)
    return 0;

  uint32_t localSeqs[DPDK_TX_BURST];
  int count = 0;
  for (uint32_t localSeq = task->sndUna;
       localSeq < task->sndNxt && count < budget; localSeq++) {
    int slot = (int)(localSeq % (uint32_t)task->frameSlots);
    struct dpdkFrameMeta *meta = task->frames + slot;
    if (meta->seq != localSeq || meta->state != DPDK_FRAME_SENT)
      continue;
    if (dpdkTaskFrameAcked(task, localSeq))
      continue;
    if (meta->lastTxTsc != 0 &&
        nowCycles - meta->lastTxTsc < retxDelayCycles)
      continue;

    localSeqs[count++] = localSeq;
  }
  return dpdkSendTaskDataBurst(req, task, localSeqs, count, nowCycles);
}

static int dpdkProgressSendTask(struct ncclNetDpdkRequest *req,
                                struct dpdkSendTask *task) {
  if (req == NULL || task == NULL || req->comm == NULL)
    return 0;
  int sent = 0;
  int budget = DPDK_TX_BURST;
  uint64_t nowCycles = rte_get_tsc_cycles();
  // Drain newly acked prefix before trying to send more.
  dpdkAdvanceTaskAckPrefix(task);

  sent += dpdkRetransmitSendTaskWindow(req, task, budget, nowCycles);
  budget -= sent;

  while (budget > 0) {
    if (task->frames == NULL || task->frameSlots <= 0 || req->peerReqId == 0)
      break;

    uint32_t localSeqs[DPDK_TX_BURST];
    struct dpdkFrameMeta *metas[DPDK_TX_BURST];
    int count = 0;
    while (count < budget) {
      if (task->inflight + count >= task->cwnd)
        break;
      uint32_t localSeq = task->sndNxt + (uint32_t)count;
      if (localSeq >= (uint32_t)task->numFrames)
        break;
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
        meta->lastTxTsc = 0;
        meta->txCount = 0;
        meta->len = (uint16_t)len;
        meta->state = DPDK_FRAME_READY;
        uint32_t word = (uint32_t)slot >> 6;
        uint32_t bit = (uint32_t)slot & 63u;
        task->ackedBitmap[word] &= ~(1ull << bit);
      }
      if (meta->state != DPDK_FRAME_READY)
        break;
      localSeqs[count] = localSeq;
      metas[count] = meta;
      count++;
    }
    if (count == 0)
      break;

    int sentNow = dpdkSendTaskDataBurst(req, task, localSeqs, count, nowCycles);
    if (sentNow <= 0)
      break;
    for (int i = 0; i < sentNow; i++) {
      task->inflight++;
      metas[i]->state = DPDK_FRAME_SENT;
      task->sndNxt++;
    }
    sent += sentNow;
    budget -= sentNow;
    if (sentNow < count)
      break;
  }
  return sent;
}

// ---------------------------------------------------------------------
// Polling loop and background progress helpers
// ---------------------------------------------------------------------

static inline int dpdkGetThreadTaskCount(int dev) {
  if (dev < 0 || dev >= ncclNetDpdkIfs)
    return 0;
  dpdkPollThread *thread = &dpdkPollThreads[dev];
  std::lock_guard<std::mutex> lock(thread->mutex);
  return (int)thread->sendTasks.size() + (int)thread->recvTasks.size();
}

static inline uint16_t dpdkGetThreadBusyQ16(int dev) {
  if (dev < 0 || dev >= ncclNetDpdkIfs)
    return 0;
  return __atomic_load_n(&dpdkPollThreads[dev].busyQ16, __ATOMIC_RELAXED);
}

static inline bool dpdkLbBalanceEnabled() {
  return ncclParamDpdkLbBalance() != 0;
}

static inline void dpdkBuildCtrlLaneFromDev(int dev, dpdkCtrlLane *lane) {
  memset(lane, 0, sizeof(*lane));
  if (dev < 0 || dev >= ncclNetDpdkIfs)
    return;
  lane->dev = (uint16_t)dev;
  lane->busyQ16 = dpdkGetThreadBusyQ16(dev);
  lane->speedMbps = (uint32_t)ncclNetDpdkDevs[dev].speedMbps;
  lane->mac = ncclNetDpdkDevs[dev].mac;
  lane->dataIp = ncclNetDpdkDevs[dev].addr.sin.sin_addr.s_addr;
}

static int dpdkCollectLocalLanes(struct ncclNetDpdkComm *comm, int numTasks,
                                 dpdkCtrlLane *lanes, int maxLanes) {
  if (comm == NULL || lanes == NULL || maxLanes <= 0 || comm->dev < 0 ||
      comm->dev >= ncclNetDpdkIfs) {
    return 0;
  }
  int count = 0;
  dpdkBuildCtrlLaneFromDev(comm->dev, lanes + count++);
  if (!dpdkLbBalanceEnabled())
    return count;
  if (count >= maxLanes || ncclNetDpdkIfs <= 1)
    return count;
  if (numTasks < std::max(1, ncclParamDpdkLbMinTasks()))
    return count;

  int primaryLoad = dpdkGetThreadTaskCount(comm->dev);
  if (primaryLoad < std::max(1, ncclParamDpdkLbBusyTasks()))
    return count;

  int bestDev = -1;
  uint16_t bestBusy = UINT16_MAX;
  for (int dev = 0; dev < ncclNetDpdkIfs; dev++) {
    if (dev == comm->dev)
      continue;
    uint16_t busy = dpdkGetThreadBusyQ16(dev);
    if (bestDev < 0 || busy < bestBusy) {
      bestDev = dev;
      bestBusy = busy;
    }
  }
  if (bestDev >= 0)
    dpdkBuildCtrlLaneFromDev(bestDev, lanes + count++);
  return count;
}

static inline void dpdkCopyCtrlLanesFromMsg(const dpdkCtrlMsg *msg,
                                            dpdkCtrlLane *lanes,
                                            int *count) {
  int n = (int)msg->laneCount;
  if (n > DPDK_MAX_CTRL_LANES)
    n = DPDK_MAX_CTRL_LANES;
  for (int i = 0; i < n; i++)
    lanes[i] = msg->lanes[i];
  *count = n;
}

static inline bool dpdkBuildTaskEndpointFromLanes(const dpdkCtrlLane *localLane,
                                                  const dpdkCtrlLane *remoteLane,
                                                  struct dpdkTaskEndpoint *ep,
                                                  int *attachedDev) {
  if (localLane == NULL || remoteLane == NULL || ep == NULL || attachedDev == NULL)
    return false;
  int dev = (int)localLane->dev;
  if (dev < 0 || dev >= ncclNetDpdkIfs)
    return false;
  // remoteLane is packed in ctrl message layout; copy fields to aligned locals
  // before taking addresses to avoid unaligned-pointer UB/warnings.
  struct rte_ether_addr remoteMac;
  uint32_t remoteIp = 0;
  memcpy(&remoteMac, &remoteLane->mac, sizeof(remoteMac));
  memcpy(&remoteIp, &remoteLane->dataIp, sizeof(remoteIp));
  dpdkInitTaskEndpointFromDev(ep, dev, &remoteMac, remoteIp);
  if (ep->dev < 0 || ep->pool == NULL || ep->portId < 0)
    return false;
  *attachedDev = dev;
  return true;
}

static int dpdkBuildLanePairPlan(const dpdkCtrlLane *sendLanes, int sendCount,
                                 const dpdkCtrlLane *recvLanes, int recvCount,
                                 dpdkCtrlLanePair *pairs, int maxPairs,
                                 uint16_t *weightSum) {
  struct PairCandidate {
    uint64_t score;
    uint8_t sendIx;
    uint8_t recvIx;
  };
  PairCandidate cand[DPDK_MAX_CTRL_LANES * DPDK_MAX_CTRL_LANES];
  int candCount = 0;

  for (int s = 0; s < sendCount; s++) {
    for (int r = 0; r < recvCount; r++) {
      uint64_t txCap =
          (uint64_t)std::max(1u, sendLanes[s].speedMbps) *
          (uint64_t)(65535u - sendLanes[s].busyQ16);
      uint64_t rxCap =
          (uint64_t)std::max(1u, recvLanes[r].speedMbps) *
          (uint64_t)(65535u - recvLanes[r].busyQ16);
      uint64_t score = std::min(txCap, rxCap);
      if (score == 0)
        score = 1;
      cand[candCount++] = {score, (uint8_t)s, (uint8_t)r};
    }
  }

  if (candCount == 0 || maxPairs <= 0) {
    *weightSum = 0;
    return 0;
  }

  std::sort(cand, cand + candCount, [](const PairCandidate &a,
                                       const PairCandidate &b) {
    return a.score > b.score;
  });

  int pairCount = std::min(maxPairs, candCount);
  uint64_t selectedScore = 0;
  for (int i = 0; i < pairCount; i++)
    selectedScore += cand[i].score;

  uint16_t sum = 0;
  for (int i = 0; i < pairCount; i++) {
    uint16_t w = 1;
    if (selectedScore > 0) {
      w = (uint16_t)std::max<uint64_t>(
          1ull, (cand[i].score * 256ull) / selectedScore);
    }
    pairs[i].sendLane = cand[i].sendIx;
    pairs[i].recvLane = cand[i].recvIx;
    pairs[i].weight = w;
    sum = (uint16_t)(sum + w);
  }
  *weightSum = sum;
  return pairCount;
}

static int dpdkSelectPairForTask(const struct ncclNetDpdkRequest *req,
                                 uint32_t taskId) {
  if (req->pairCount <= 1)
    return 0;
  uint16_t sum = req->pairWeightSum;
  if (sum == 0)
    return 0;
  uint32_t pick = dpdkHash32(req->pairSeed ^ taskId) % sum;
  uint32_t acc = 0;
  for (int i = 0; i < req->pairCount; i++) {
    acc += std::max<int>(1, req->pairs[i].weight);
    if (pick < acc)
      return i;
  }
  return req->pairCount - 1;
}

static void dpdkApplySendTaskPairing(struct ncclNetDpdkRequest *req) {
  if (req == NULL || req->send.tasks == NULL)
    return;
  for (int i = 0; i < req->numTasks; i++) {
    struct dpdkSendTask *task = req->send.tasks + i;
    bool ok = false;
    if (req->pairCount > 0 && req->localLaneCount > 0 && req->peerLaneCount > 0) {
      int pairIx = dpdkSelectPairForTask(req, task->taskId);
      const dpdkCtrlLanePair *pair = &req->pairs[pairIx];
      if (pair->sendLane < req->localLaneCount &&
          pair->recvLane < req->peerLaneCount) {
        ok = dpdkBuildTaskEndpointFromLanes(&req->localLanes[pair->sendLane],
                                            &req->peerLanes[pair->recvLane],
                                            &task->endpoint, &task->attachedDev);
      }
    }
    if (!ok && req->comm) {
      task->attachedDev = req->comm->dev;
      dpdkInitTaskEndpointFromDev(&task->endpoint, req->comm->dev,
                                  &req->comm->remoteMac, req->comm->remoteIp);
    }
  }
}

static void dpdkApplyRecvTaskPairing(struct ncclNetDpdkRequest *req) {
  if (req == NULL || req->recv.tasks == NULL)
    return;
  for (int i = 0; i < req->numTasks; i++) {
    struct dpdkRecvTask *task = req->recv.tasks + i;
    bool ok = false;
    if (req->pairCount > 0 && req->localLaneCount > 0 && req->peerLaneCount > 0) {
      int pairIx = dpdkSelectPairForTask(req, task->taskId);
      const dpdkCtrlLanePair *pair = &req->pairs[pairIx];
      if (pair->recvLane < req->localLaneCount &&
          pair->sendLane < req->peerLaneCount) {
        ok = dpdkBuildTaskEndpointFromLanes(&req->localLanes[pair->recvLane],
                                            &req->peerLanes[pair->sendLane],
                                            &task->endpoint, &task->attachedDev);
      }
    }
    if (!ok && req->comm) {
      task->attachedDev = req->comm->dev;
      dpdkInitTaskEndpointFromDev(&task->endpoint, req->comm->dev,
                                  &req->comm->remoteMac, req->comm->remoteIp);
    }
  }
}

static void dpdkReportLbAssignment(const struct ncclNetDpdkRequest *req,
                                   const char *role) {
  if (!dpdkLbBalanceEnabled() || req == NULL || role == NULL)
    return;

  int pairTasks[DPDK_MAX_CTRL_PAIRS] = {};
  int devTasks[DPDK_MAX_DEVS] = {};
  struct dpdkLbPairLog pairLogs[DPDK_MAX_CTRL_PAIRS] = {};
  struct dpdkLbDevTaskLog devLogs[DPDK_MAX_DEVS] = {};

  for (int i = 0; i < req->numTasks; i++) {
    uint32_t taskId = 0;
    int attachedDev = DPDK_INVALID_DEV;
    if (req->op == NCCL_SOCKET_SEND && req->send.tasks != NULL) {
      const dpdkSendTask *task = req->send.tasks + i;
      taskId = task->taskId;
      attachedDev = task->attachedDev;
    } else if (req->op == NCCL_SOCKET_RECV && req->recv.tasks != NULL) {
      const dpdkRecvTask *task = req->recv.tasks + i;
      taskId = task->taskId;
      attachedDev = task->attachedDev;
    } else {
      continue;
    }

    if (req->pairCount > 0) {
      int pairIx = dpdkSelectPairForTask(req, taskId);
      if (pairIx >= 0 && pairIx < DPDK_MAX_CTRL_PAIRS)
        pairTasks[pairIx]++;
    }
    if (attachedDev >= 0 && attachedDev < DPDK_MAX_DEVS)
      devTasks[attachedDev]++;
  }

  bool localIsSend = req->op == NCCL_SOCKET_SEND;
  for (int i = 0; i < req->pairCount && i < DPDK_MAX_CTRL_PAIRS; i++) {
    const dpdkCtrlLanePair *pair = req->pairs + i;
    const dpdkCtrlLane *sendLanes =
        localIsSend ? req->localLanes : req->peerLanes;
    const dpdkCtrlLane *recvLanes =
        localIsSend ? req->peerLanes : req->localLanes;
    int sendLaneCount = localIsSend ? req->localLaneCount : req->peerLaneCount;
    int recvLaneCount = localIsSend ? req->peerLaneCount : req->localLaneCount;
    uint16_t sendDev = UINT16_MAX;
    uint16_t recvDev = UINT16_MAX;
    if (pair->sendLane < sendLaneCount)
      sendDev = sendLanes[pair->sendLane].dev;
    if (pair->recvLane < recvLaneCount)
      recvDev = recvLanes[pair->recvLane].dev;
    pairLogs[i].sendDev = (unsigned)sendDev;
    pairLogs[i].recvDev = (unsigned)recvDev;
    pairLogs[i].taskCount = pairTasks[i];
  }

  int devLogCount = 0;
  for (int dev = 0; dev < ncclNetDpdkIfs && dev < DPDK_MAX_DEVS; dev++) {
    if (devTasks[dev] == 0)
      continue;
    devLogs[devLogCount].dev = dev;
    devLogs[devLogCount].taskCount = devTasks[dev];
    devLogCount++;
  }

  struct dpdkLbAssignmentLog log = {
      role,
      req->reqId,
      req->peerReqId,
      req->numTasks,
      (unsigned)std::min<int>(req->pairCount, DPDK_MAX_CTRL_PAIRS),
      pairLogs,
      devLogCount,
      devLogs,
  };
  dpdkLogLbAssignment(&log);
}

static inline void dpdkUpdatePollThreadLoad(struct dpdkPollThread *thread) {
  uint64_t now = rte_get_tsc_cycles();
  uint64_t hz = dpdkTscHz ? dpdkTscHz : rte_get_tsc_hz();
  uint64_t updateEvery = hz / 10000ULL; // ~100us
  if (updateEvery == 0)
    updateEvery = 1;
  if (thread->lastLoadTsc != 0 && now - thread->lastLoadTsc < updateEvery)
    return;

  int activeSend = 0;
  int activeRecv = 0;
  {
    std::lock_guard<std::mutex> lock(thread->mutex);
    activeSend = (int)thread->sendTasks.size();
    activeRecv = (int)thread->recvTasks.size();
  }

  uint64_t attempts = __atomic_exchange_n(&thread->txAttempts, 0ull,
                                          __ATOMIC_RELAXED);
  uint64_t drops = __atomic_exchange_n(&thread->txDrops, 0ull,
                                       __ATOMIC_RELAXED);
  uint64_t rxPkts = __atomic_exchange_n(&thread->rxPkts, 0ull,
                                        __ATOMIC_RELAXED);
  uint32_t txFailQ16 = 0;
  if (attempts > 0)
    txFailQ16 = (uint32_t)std::min<uint64_t>(65535ull, (drops * 65535ull) / attempts);
  uint32_t activeTasksQ16 =
      (uint32_t)std::min<uint64_t>(65535ull, (uint64_t)(activeSend + activeRecv) * 4096ull);
  uint32_t rxPressureQ16 =
      (uint32_t)std::min<uint64_t>(65535ull, (rxPkts * 65535ull) / DPDK_RX_BURST);

  uint32_t rawBusy = (txFailQ16 * 6 + activeTasksQ16 * 3 + rxPressureQ16) / 10;
  uint32_t smooth = ((uint32_t)thread->busyQ16 * 7 + rawBusy) / 8;
  __atomic_store_n(&thread->busyQ16, (uint16_t)smooth, __ATOMIC_RELAXED);
  thread->lastLoadTsc = now;
}

#if NCCL_DPDK_FAULT_INJECT
static inline void dpdkMaybeInjectPollThreadFault(struct dpdkPollThread *thread,
                                                  uint64_t now,
                                                  uint64_t hz) {
  int injectDev = ncclParamDpdkFaultInjectDev();
  if (injectDev >= 0 && injectDev != thread->dev)
    return;

  bool hasTasks = false;
  {
    std::lock_guard<std::mutex> lock(thread->mutex);
    hasTasks = !thread->sendTasks.empty() || !thread->recvTasks.empty();
  }
  if (!hasTasks) {
    thread->lastFaultInjectTsc = 0;
    return;
  }

  int intervalMs = ncclParamDpdkFaultInjectIntervalMs();
  uint64_t injectEvery = ((uint64_t)intervalMs * hz) / 1000ULL;
  if (injectEvery == 0)
    injectEvery = 1;
  if (thread->lastFaultInjectTsc == 0) {
    thread->lastFaultInjectTsc = now;
    return;
  }
  if (now - thread->lastFaultInjectTsc < injectEvery)
    return;
  thread->lastFaultInjectTsc = now;

  int sleepMs = ncclParamDpdkFaultInjectSleepMs();

  INFO(NCCL_NET,
       "NET/DPDK : test fault injection dev=%d port=%d name=%s intervalMs=%d sleepMs=%d, scheduling failover",
       thread->dev, thread->portId, ncclNetDpdkDevs[thread->dev].devName,
       intervalMs, sleepMs);
  dpdkRaiseLocalFaultEvent(thread->dev);
  if (sleepMs > 0) {
    uint64_t sleepUs = (uint64_t)sleepMs * 1000ULL;
    if (sleepUs > UINT_MAX)
      sleepUs = UINT_MAX;
    rte_delay_us_sleep((unsigned int)sleepUs);
  }
}
#endif

static inline void dpdkCheckPollThreadLink(struct dpdkPollThread *thread) {
  if (thread == NULL || thread->dev < 0 || thread->dev >= ncclNetDpdkIfs)
    return;
  uint64_t hz = dpdkTscHz ? dpdkTscHz : rte_get_tsc_hz();
  uint64_t now = rte_get_tsc_cycles();
  int checkMs = ncclParamDpdkFaultCheckMs();
  if (checkMs <= 0)
    checkMs = 1;
  uint64_t checkEvery = ((uint64_t)checkMs * hz) / 1000ULL;
  if (checkEvery == 0)
    checkEvery = 1;
  if (thread->lastLinkCheckTsc != 0 &&
      now - thread->lastLinkCheckTsc < checkEvery)
    return;
  thread->lastLinkCheckTsc = now;

  if (dpdkIsDevLinkUp(thread->dev)) {
    thread->linkDownCount = 0;
    thread->faultReported = 0;
#if NCCL_DPDK_FAULT_INJECT
    dpdkMaybeInjectPollThreadFault(thread, now, hz);
#endif
    return;
  }

  thread->linkDownCount++;
  int downNeed = ncclParamDpdkFaultDownCount();
  if (downNeed <= 0)
    downNeed = 1;
  if (thread->linkDownCount >= downNeed && !thread->faultReported) {
    thread->faultReported = 1;
    INFO(NCCL_NET,
         "NET/DPDK : fault detected dev=%d port=%d name=%s downCount=%d, scheduling failover",
         thread->dev, thread->portId, ncclNetDpdkDevs[thread->dev].devName,
         thread->linkDownCount);
    dpdkRaiseLocalFaultEvent(thread->dev);
  }
}

static int dpdkPollRx(struct dpdkPollThread *thread) {
  struct rte_mbuf *mbufs[DPDK_RX_BURST];
  int nb = rte_eth_rx_burst((uint16_t)thread->portId, 0, mbufs, DPDK_RX_BURST);
  if (nb <= 0)
    return 0;
  __atomic_fetch_add(&thread->rxPkts, (uint64_t)nb, __ATOMIC_RELAXED);
  for (int i = 0; i < nb; i++) {
    struct rte_mbuf *mbuf = mbufs[i];
    ncdpHdr hdr;
    const uint8_t *payload = NULL;
    uint16_t payloadLen = 0;
    if (ncdpParsePacket(mbuf, &hdr, &payload, &payloadLen))
      dpdkHandleRxPacket(thread->dev, &hdr, payload, payloadLen);
    rte_pktmbuf_free(mbuf);
  }
  return nb;
}

static inline void dpdkProgressTasks(struct dpdkPollThread *thread) {
  std::vector<struct dpdkSendTaskRef> sendTasks;
  std::vector<struct dpdkRecvTaskRef> recvTasks;
  {
    std::lock_guard<std::mutex> lock(thread->mutex);
    sendTasks = thread->sendTasks;
    recvTasks = thread->recvTasks;
  }

  for (auto &ref : sendTasks) {
    if (ref.req == NULL || ref.task == NULL)
      continue;
    if (ref.task->attachedDev != thread->dev)
      continue;
    if (ref.req->state.load(std::memory_order_acquire) != DPDK_REQ_SENDING)
      continue;
    if (ref.task->isDone)
      continue;
    dpdkProgressSendTask(ref.req, ref.task);
  }

  for (auto &ref : recvTasks) {
    if (ref.req == NULL || ref.task == NULL)
      continue;
    if (ref.task->attachedDev != thread->dev)
      continue;
    if (ref.req->state.load(std::memory_order_acquire) != DPDK_REQ_RECEIVING)
      continue;
    dpdkProgressRecvTask(ref.req, ref.task);
    dpdkTryFinishRecvAfterAck(ref.req);
  }
}

// One poll worker per net device:
// receives packets, progresses send tasks, recv tasks(flushes delayed ACKs).
static int dpdkPollThreadMain(void *arg) {
  dpdkPollThread *thread = (dpdkPollThread *)arg;
  while (!thread->stopRequested) {
    int got = dpdkPollRx(thread);
    dpdkProgressTasks(thread);
    dpdkUpdatePollThreadLoad(thread);
    dpdkCheckPollThreadLink(thread);
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
  thread->sendTasks.clear();
  thread->recvTasks.clear();
  thread->busyQ16 = 0;
  thread->txAttempts = 0;
  thread->txDrops = 0;
  thread->rxPkts = 0;
  thread->lastLoadTsc = 0;
  thread->lastLinkCheckTsc = 0;
  thread->linkDownCount = 0;
  thread->faultReported = 0;
#if NCCL_DPDK_FAULT_INJECT
  thread->lastFaultInjectTsc = 0;
#endif
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
  if (dev < 0 || dev >= ncclNetDpdkIfs)
    return ncclInvalidArgument;
  dpdkPollThread *thread = &dpdkPollThreads[dev];
  std::lock_guard<std::mutex> lock(thread->mutex);
  thread->refCount++;
  return ncclSuccess;
}

static ncclResult_t dpdkReleasePollThread(int dev) {
  if (dev < 0 || dev >= ncclNetDpdkIfs)
    return ncclInvalidArgument;
  dpdkPollThread *thread = &dpdkPollThreads[dev];
  std::lock_guard<std::mutex> lock(thread->mutex);
  if (thread->refCount <= 0) {
    WARN("NET/DPDK : poll thread refcount underflow on dev %d", dev);
    return ncclInternalError;
  }
  thread->refCount--;
  return ncclSuccess;
}

static ncclResult_t dpdkAttachSendTask(struct ncclNetDpdkRequest *req,
                                       struct dpdkSendTask *task) {
  if (req == NULL || task == NULL || req->comm == NULL)
    return ncclInvalidArgument;
  if (task->isAttached)
    return ncclSuccess;
  int dev = task->attachedDev;
  if (task->txMbufs != NULL && task->txMbufCount == 0)
    (void)dpdkRefillTaskTxMbufs(task, task->txMbufCap);
  NCCLCHECK(dpdkAcquirePollThread(dev));
  dpdkPollThread *thread = &dpdkPollThreads[dev];
  {
    std::lock_guard<std::mutex> lock(thread->mutex);
    thread->sendTasks.push_back({req, task});
  }
  task->isAttached = 1;
  dpdkCommAcquire(req->comm);
  return ncclSuccess;
}

static void dpdkDetachSendTask(struct ncclNetDpdkRequest *req,
                               struct dpdkSendTask *task) {
  if (req == NULL || task == NULL || req->comm == NULL || !task->isAttached)
    return;
  int dev = task->attachedDev;
  bool releaseThreadRef = false;
  if (dev >= 0 && dev < ncclNetDpdkIfs) {
    dpdkPollThread *thread = &dpdkPollThreads[dev];
    {
      std::lock_guard<std::mutex> lock(thread->mutex);
      auto &v = thread->sendTasks;
      for (size_t i = 0; i < v.size(); ++i) {
        if (v[i].task == task && v[i].req == req) {
          v[i] = v.back();
          v.pop_back();
          releaseThreadRef = true;
          break;
        }
      }
    }
    if (releaseThreadRef)
      (void)dpdkReleasePollThread(dev);
  }
  task->isAttached = 0;
  dpdkCommRelease(req->comm);
}

static ncclResult_t dpdkAttachRecvTask(struct ncclNetDpdkRequest *req,
                                       struct dpdkRecvTask *task) {
  if (req == NULL || task == NULL || req->comm == NULL)
    return ncclInvalidArgument;
  if (task->isAttached)
    return ncclSuccess;
  int dev = task->attachedDev;
  NCCLCHECK(dpdkAcquirePollThread(dev));
  dpdkPollThread *thread = &dpdkPollThreads[dev];
  {
    std::lock_guard<std::mutex> lock(thread->mutex);
    thread->recvTasks.push_back({req, task});
  }
  task->isAttached = 1;
  dpdkCommAcquire(req->comm);
  return ncclSuccess;
}

static void dpdkDetachRecvTask(struct ncclNetDpdkRequest *req,
                               struct dpdkRecvTask *task) {
  if (req == NULL || task == NULL || req->comm == NULL || !task->isAttached)
    return;
  int dev = task->attachedDev;
  bool releaseThreadRef = false;
  if (dev >= 0 && dev < ncclNetDpdkIfs) {
    dpdkPollThread *thread = &dpdkPollThreads[dev];
    {
      std::lock_guard<std::mutex> lock(thread->mutex);
      auto &v = thread->recvTasks;
      for (size_t i = 0; i < v.size(); ++i) {
        if (v[i].task == task && v[i].req == req) {
          v[i] = v.back();
          v.pop_back();
          releaseThreadRef = true;
          break;
        }
      }
    }
    if (releaseThreadRef)
      (void)dpdkReleasePollThread(dev);
  }
  task->isAttached = 0;
  dpdkCommRelease(req->comm);
}

static ncclResult_t dpdkAttachRequestTasks(struct ncclNetDpdkRequest *req) {
  if (req == NULL)
    return ncclInvalidArgument;
  if (req->op == NCCL_SOCKET_SEND) {
    if (req->send.tasks == NULL)
      return ncclSuccess;
    for (int i = 0; i < req->numTasks; i++) {
      ncclResult_t ret = dpdkAttachSendTask(req, req->send.tasks + i);
      if (ret != ncclSuccess) {
        dpdkDetachRequestTasks(req);
        return ret;
      }
    }
  } else if (req->op == NCCL_SOCKET_RECV) {
    if (req->recv.tasks == NULL)
      return ncclSuccess;
    for (int i = 0; i < req->numTasks; i++) {
      ncclResult_t ret = dpdkAttachRecvTask(req, req->recv.tasks + i);
      if (ret != ncclSuccess) {
        dpdkDetachRequestTasks(req);
        return ret;
      }
    }
  }
  return ncclSuccess;
}

static void dpdkDetachRequestTasks(struct ncclNetDpdkRequest *req) {
  if (req == NULL)
    return;
  if (req->op == NCCL_SOCKET_SEND) {
    if (req->send.tasks == NULL)
      return;
    for (int i = 0; i < req->numTasks; i++)
      dpdkDetachSendTask(req, req->send.tasks + i);
  } else if (req->op == NCCL_SOCKET_RECV) {
    if (req->recv.tasks == NULL)
      return;
    for (int i = 0; i < req->numTasks; i++)
      dpdkDetachRecvTask(req, req->recv.tasks + i);
  }
}

static void dpdkMarkRequestFailed(struct ncclNetDpdkRequest *req,
                                  ncclResult_t err) {
  if (req == NULL)
    return;
  req->errorCode.store((int)err, std::memory_order_release);
  req->isCompleted.store(1, std::memory_order_release);
  req->state.store(DPDK_REQ_UNUSED, std::memory_order_release);
  dpdkDetachRequestTasks(req);
}

static bool dpdkIsDevLinkUp(int dev) {
  if (dev < 0 || dev >= ncclNetDpdkIfs)
    return false;
  int portId = ncclNetDpdkDevs[dev].portId;
  if (portId < 0)
    return false;
  struct rte_eth_link link;
  memset(&link, 0, sizeof(link));
  rte_eth_link_get_nowait((uint16_t)portId, &link);
  return link.link_status != RTE_ETH_LINK_DOWN;
}

static int dpdkSelectFailoverDev(int failedDev) {
  int bestDev = -1;
  uint16_t bestBusy = UINT16_MAX;
  for (int dev = 0; dev < ncclNetDpdkIfs; dev++) {
    if (dev == failedDev)
      continue;
    if (!dpdkIsDevLinkUp(dev))
      continue;
    uint16_t busy = dpdkGetThreadBusyQ16(dev);
    if (bestDev < 0 || busy < bestBusy) {
      bestDev = dev;
      bestBusy = busy;
    }
  }
  return bestDev;
}

static inline const char *dpdkOpName(int op) {
  if (op == NCCL_SOCKET_SEND)
    return "SEND";
  if (op == NCCL_SOCKET_RECV)
    return "RECV";
  return "UNKNOWN";
}

static void dpdkSwitchCommPrimaryDev(struct ncclNetDpdkComm *comm, int newDev) {
  if (comm == NULL || newDev < 0 || newDev >= ncclNetDpdkIfs)
    return;
  int oldDev = comm->dev;
  comm->dev = newDev;
  comm->portId = ncclNetDpdkDevs[newDev].portId;
  comm->pool = ncclNetDpdkDevs[newDev].pool;
  comm->maxPayload = ncdpComputeMaxPayload(ncclNetDpdkDevs[newDev].mtu);
  comm->localIp = ncclNetDpdkDevs[newDev].addr.sin.sin_addr.s_addr;
  rte_ether_addr_copy(&ncclNetDpdkDevs[newDev].mac, &comm->localMac);
  INFO(NCCL_NET,
       "NET/DPDK : fault failover switched commId=%u primary dev %d->%d port=%d",
       comm->commId, oldDev, newDev, comm->portId);
}

static void dpdkResetSendTaskWindow(struct dpdkSendTask *task) {
  if (task == NULL)
    return;
  task->sndNxt = task->sndUna;
  task->inflight = 0;
  if (task->frames && task->frameSlots > 0) {
    for (int i = 0; i < task->frameSlots; i++) {
      task->frames[i].seq = DPDK_INVALID_SEQ;
      task->frames[i].lastTxTsc = 0;
      task->frames[i].txCount = 0;
      task->frames[i].len = 0;
      task->frames[i].state = DPDK_FRAME_EMPTY;
    }
  }
  if (task->ackedBitmap && task->ackedBitmapWords > 0)
    memset(task->ackedBitmap, 0, task->ackedBitmapWords * sizeof(uint64_t));
}

static ncclResult_t dpdkMigrateSendTaskLocal(struct ncclNetDpdkRequest *req,
                                             struct dpdkSendTask *task,
                                             int newDev) {
  if (req == NULL || task == NULL || req->comm == NULL || newDev < 0 ||
      newDev >= ncclNetDpdkIfs)
    return ncclInvalidArgument;
  if (task->attachedDev == newDev)
    return ncclSuccess;

  int oldDev = task->attachedDev;
  struct rte_ether_addr remoteMac = task->endpoint.remoteMac;
  uint32_t remoteIp = task->endpoint.remoteIp;
  if (task->isAttached)
    dpdkDetachSendTask(req, task);
  task->attachedDev = newDev;
  dpdkInitTaskEndpointFromDev(&task->endpoint, newDev, &remoteMac, remoteIp);
  task->attachedDev = newDev;
  NCCLCHECK(dpdkAttachSendTask(req, task));
  dpdkResetSendTaskWindow(task);
  INFO(NCCL_NET,
       "NET/DPDK : migrated SEND task commId=%u reqId=%u peerReqId=%u taskId=%u dev %d->%d sndUna=%u sndNxt=%u completed=%d/%d",
       req->comm->commId, req->reqId, req->peerReqId, task->taskId, oldDev,
       newDev, task->sndUna, task->sndNxt, task->completedFrames,
       task->numFrames);
  return ncclSuccess;
}

static ncclResult_t dpdkMigrateRecvTaskLocal(struct ncclNetDpdkRequest *req,
                                             struct dpdkRecvTask *task,
                                             int newDev) {
  if (req == NULL || task == NULL || req->comm == NULL || newDev < 0 ||
      newDev >= ncclNetDpdkIfs)
    return ncclInvalidArgument;
  if (task->attachedDev == newDev)
    return ncclSuccess;

  int oldDev = task->attachedDev;
  struct rte_ether_addr remoteMac = task->endpoint.remoteMac;
  uint32_t remoteIp = task->endpoint.remoteIp;
  if (task->isAttached)
    dpdkDetachRecvTask(req, task);
  task->attachedDev = newDev;
  dpdkInitTaskEndpointFromDev(&task->endpoint, newDev, &remoteMac, remoteIp);
  task->attachedDev = newDev;
  NCCLCHECK(dpdkAttachRecvTask(req, task));
  INFO(NCCL_NET,
       "NET/DPDK : migrated RECV task commId=%u reqId=%u peerReqId=%u taskId=%u dev %d->%d rcvNxt=%u completed=%d/%d",
       req->comm->commId, req->reqId, req->peerReqId, task->taskId, oldDev,
       newDev, task->rcvNxt, task->completedFrames, task->numFrames);
  return ncclSuccess;
}

static bool dpdkSendFaultMsg(struct ncclNetDpdkComm *comm,
                             const dpdkFaultMsg *msg) {
  if (comm == NULL || msg == NULL || comm->faultSock.fd < 0)
    return false;
  int timeoutMs = ncclParamDpdkFaultSendTimeoutMs();
  if (timeoutMs <= 0)
    timeoutMs = 1;
  int offset = 0;
  int closed = 0;
  auto start = std::chrono::steady_clock::now();
  while (offset < (int)sizeof(*msg) &&
         !dpdkFaultStop.load(std::memory_order_relaxed)) {
    ncclResult_t ret =
        ncclSocketProgress(NCCL_SOCKET_SEND, &comm->faultSock, (void *)msg,
                           sizeof(*msg), &offset, &closed);
    if (ret != ncclSuccess || closed)
      return false;
    if (offset < (int)sizeof(*msg)) {
      auto now = std::chrono::steady_clock::now();
      int elapsed = (int)std::chrono::duration_cast<std::chrono::milliseconds>(
                        now - start)
                        .count();
      if (elapsed >= timeoutMs)
        return false;
      usleep(200);
    }
  }
  return offset == (int)sizeof(*msg);
}

static inline void dpdkStoreFaultLane(dpdkFaultMsg *msg,
                                      const dpdkCtrlLane *lane) {
  memcpy((char *)msg + offsetof(dpdkFaultMsg, newLane), lane, sizeof(*lane));
}

static inline void dpdkLoadFaultLane(const dpdkFaultMsg *msg,
                                     dpdkCtrlLane *lane) {
  memcpy(lane, (const char *)msg + offsetof(dpdkFaultMsg, newLane),
         sizeof(*lane));
}

static void dpdkFillFaultMsg(struct ncclNetDpdkComm *comm,
                             struct ncclNetDpdkRequest *req, int failedDev,
                             int newDev, uint64_t taskMask,
                             dpdkFaultMsg *msg) {
  memset(msg, 0, sizeof(*msg));
  msg->magic = DPDK_FAULT_MAGIC;
  msg->version = DPDK_PROTO_VERSION;
  msg->type = DPDK_FAULT_NOTIFY;
  msg->srcCommId = comm->commId;
  msg->dstCommId = comm->remoteCommId;
  msg->srcReqId = req->reqId;
  msg->dstReqId = req->peerReqId;
  msg->op = (uint32_t)req->op;
  msg->failedDev = (uint32_t)std::max(0, failedDev);
  msg->newDev = (uint32_t)std::max(0, newDev);
  msg->epoch = dpdkFaultEpoch.fetch_add(1, std::memory_order_relaxed);
  msg->taskMask = taskMask;

  dpdkCtrlLane lane;
  dpdkBuildCtrlLaneFromDev(newDev, &lane);
  dpdkStoreFaultLane(msg, &lane);
  INFO(NCCL_NET,
       "NET/DPDK : prepared fault notify commId=%u remoteCommId=%u op=%s reqId=%u peerReqId=%u failedDev=%d newDev=%d taskMask=0x%016llx epoch=%llu",
       comm->commId, comm->remoteCommId, dpdkOpName(req->op), req->reqId,
       req->peerReqId, failedDev, newDev, (unsigned long long)taskMask,
       (unsigned long long)msg->epoch);
}

static void dpdkApplyRemoteFault(struct ncclNetDpdkComm *comm,
                                 const dpdkFaultMsg *msg) {
  if (comm == NULL || msg == NULL)
    return;
  std::lock_guard<std::mutex> lock(ncclNetDpdkCommMutex);
  if (comm->closing.load(std::memory_order_acquire))
    return;
  if (msg->magic != DPDK_FAULT_MAGIC || msg->version != DPDK_PROTO_VERSION ||
      msg->type != DPDK_FAULT_NOTIFY || msg->dstCommId != comm->commId ||
      msg->srcCommId != comm->remoteCommId) {
    return;
  }
  if (msg->epoch <= comm->faultLastEpoch)
    return;
  comm->faultLastEpoch = msg->epoch;

  dpdkCtrlLane lane;
  dpdkLoadFaultLane(msg, &lane);
  comm->remoteMac = lane.mac;
  comm->remoteIp = lane.dataIp;

  INFO(NCCL_NET,
       "NET/DPDK : applying remote fault commId=%u remoteCommId=%u op=%s localReqId=%u remoteReqId=%u failedDev=%u newDev=%u taskMask=0x%016llx epoch=%llu",
       comm->commId, comm->remoteCommId, dpdkOpName((int)msg->op),
       msg->dstReqId, msg->srcReqId, msg->failedDev, msg->newDev,
       (unsigned long long)msg->taskMask, (unsigned long long)msg->epoch);

  if (msg->taskMask == 0)
    return;

  if ((int)msg->op == NCCL_SOCKET_SEND) {
    struct ncclNetDpdkRequest *req =
        dpdkFindRequestByReqId(comm, msg->dstReqId, DPDK_REQ_RECEIVING);
    if (req == NULL || req->recv.tasks == NULL)
      return;
    for (int i = 0; i < req->numTasks && i < 64; i++) {
      if (((msg->taskMask >> i) & 1ull) == 0ull)
        continue;
      struct dpdkRecvTask *task = req->recv.tasks + i;
      task->endpoint.remoteMac = lane.mac;
      task->endpoint.remoteIp = lane.dataIp;
      INFO(NCCL_NET,
           "NET/DPDK : remote fault updated RECV task commId=%u reqId=%u peerReqId=%u taskId=%u remoteDev=%u newRemoteDev=%u rcvNxt=%u completed=%d/%d",
           comm->commId, req->reqId, req->peerReqId, task->taskId,
           msg->failedDev, msg->newDev, task->rcvNxt, task->completedFrames,
           task->numFrames);
    }
    return;
  }

  if ((int)msg->op == NCCL_SOCKET_RECV) {
    struct ncclNetDpdkRequest *req =
        dpdkFindRequestByReqId(comm, msg->dstReqId, DPDK_REQ_SENDING);
    if (req == NULL || req->send.tasks == NULL)
      return;
    for (int i = 0; i < req->numTasks && i < 64; i++) {
      if (((msg->taskMask >> i) & 1ull) == 0ull)
        continue;
      struct dpdkSendTask *task = req->send.tasks + i;
      task->endpoint.remoteMac = lane.mac;
      task->endpoint.remoteIp = lane.dataIp;
      dpdkResetSendTaskWindow(task);
      INFO(NCCL_NET,
           "NET/DPDK : remote fault retargeted SEND task commId=%u reqId=%u peerReqId=%u taskId=%u remoteDev=%u newRemoteDev=%u sndUna=%u sndNxt=%u completed=%d/%d",
           comm->commId, req->reqId, req->peerReqId, task->taskId,
           msg->failedDev, msg->newDev, task->sndUna, task->sndNxt,
           task->completedFrames, task->numFrames);
    }
  }
}

static void dpdkProgressFaultRx(struct ncclNetDpdkComm *comm) {
  if (comm == NULL || comm->faultSock.fd < 0 ||
      comm->closing.load(std::memory_order_acquire))
    return;

  for (int i = 0; i < 8; i++) {
    int closed = 0;
    ncclResult_t ret = ncclSocketProgress(
        NCCL_SOCKET_RECV, &comm->faultSock, &comm->faultRxMsg,
        sizeof(comm->faultRxMsg), &comm->faultRxOffset, &closed);
    if (ret != ncclSuccess || closed) {
      WARN("NET/DPDK : fault socket closed/errored for commId=%u",
           comm->commId);
      (void)ncclSocketClose(&comm->faultSock);
      comm->faultRxOffset = 0;
      return;
    }
    if (comm->faultRxOffset < (int)sizeof(comm->faultRxMsg))
      break;
    dpdkApplyRemoteFault(comm, &comm->faultRxMsg);
    comm->faultRxOffset = 0;
  }
}

static void dpdkCollectActiveComms(std::vector<ncclNetDpdkComm *> *out) {
  if (out == NULL)
    return;
  std::lock_guard<std::mutex> lock(ncclNetDpdkCommMutex);
  out->clear();
  out->reserve(dpdkActiveComms.size());
  for (auto *comm : dpdkActiveComms) {
    dpdkCommAcquire(comm);
    out->push_back(comm);
  }
}

static void dpdkReleaseActiveComms(std::vector<ncclNetDpdkComm *> *comms) {
  if (comms == NULL)
    return;
  for (auto *comm : *comms)
    dpdkCommRelease(comm);
  comms->clear();
}

static void dpdkHandleLocalFaultEvent(const dpdkFaultEvent &ev) {
  std::vector<ncclNetDpdkComm *> comms;
  dpdkCollectActiveComms(&comms);
  for (auto *comm : comms) {
    if (comm == NULL || comm->closing.load(std::memory_order_acquire))
      continue;

    int backupDev = dpdkSelectFailoverDev(ev.failedDev);
    if (backupDev >= 0 && comm->dev == ev.failedDev)
      dpdkSwitchCommPrimaryDev(comm, backupDev);

    for (int ri = 0; ri < DPDK_MAX_REQUESTS; ri++) {
      struct ncclNetDpdkRequest *req = comm->requests + ri;
      if (!req->inUse || req->peerReqId == 0)
        continue;
      int state = req->state.load(std::memory_order_acquire);
      if (state != DPDK_REQ_SENDING && state != DPDK_REQ_RECEIVING)
        continue;

      bool hasAffectedTask = false;
      if (state == DPDK_REQ_SENDING && req->send.tasks) {
        for (int ti = 0; ti < req->numTasks && ti < 64; ti++) {
          struct dpdkSendTask *task = req->send.tasks + ti;
          if (!task->isDone && task->attachedDev == ev.failedDev) {
            hasAffectedTask = true;
            break;
          }
        }
      } else if (state == DPDK_REQ_RECEIVING && req->recv.tasks) {
        for (int ti = 0; ti < req->numTasks && ti < 64; ti++) {
          struct dpdkRecvTask *task = req->recv.tasks + ti;
          if (!task->isDone && task->attachedDev == ev.failedDev) {
            hasAffectedTask = true;
            break;
          }
        }
      }
      if (!hasAffectedTask)
        continue;

      if (backupDev < 0) {
        WARN("NET/DPDK : no backup device found for failed dev %d",
             ev.failedDev);
        dpdkMarkRequestFailed(req, ncclSystemError);
        continue;
      }

      INFO(NCCL_NET,
           "NET/DPDK : handling local fault commId=%u remoteCommId=%u op=%s reqId=%u peerReqId=%u failedDev=%d backupDev=%d state=%d",
           comm->commId, comm->remoteCommId, dpdkOpName(req->op), req->reqId,
           req->peerReqId, ev.failedDev, backupDev, state);

      uint64_t taskMask = 0;
      ncclResult_t ret = ncclSuccess;
      if (state == DPDK_REQ_SENDING && req->send.tasks) {
        for (int ti = 0; ti < req->numTasks && ti < 64; ti++) {
          struct dpdkSendTask *task = req->send.tasks + ti;
          if (task->isDone || task->attachedDev != ev.failedDev)
            continue;
          ret = dpdkMigrateSendTaskLocal(req, task, backupDev);
          if (ret != ncclSuccess)
            break;
          taskMask |= (1ull << ti);
        }
      } else if (state == DPDK_REQ_RECEIVING && req->recv.tasks) {
        for (int ti = 0; ti < req->numTasks && ti < 64; ti++) {
          struct dpdkRecvTask *task = req->recv.tasks + ti;
          if (task->isDone || task->attachedDev != ev.failedDev)
            continue;
          ret = dpdkMigrateRecvTaskLocal(req, task, backupDev);
          if (ret != ncclSuccess)
            break;
          taskMask |= (1ull << ti);
        }
      }

      if (ret != ncclSuccess) {
        WARN("NET/DPDK : task migration failed for reqId=%u", req->reqId);
        dpdkMarkRequestFailed(req, ret);
        continue;
      }
      if (taskMask == 0)
        continue;

      dpdkFaultMsg msg;
      dpdkFillFaultMsg(comm, req, ev.failedDev, backupDev, taskMask, &msg);
      if (!dpdkSendFaultMsg(comm, &msg)) {
        WARN("NET/DPDK : failed to notify remote fault handler commId=%u",
             comm->commId);
        dpdkMarkRequestFailed(req, ncclRemoteError);
      } else {
        INFO(NCCL_NET,
             "NET/DPDK : sent fault notify commId=%u remoteCommId=%u op=%s reqId=%u peerReqId=%u failedDev=%d backupDev=%d taskMask=0x%016llx",
             comm->commId, comm->remoteCommId, dpdkOpName(req->op),
             req->reqId, req->peerReqId, ev.failedDev, backupDev,
             (unsigned long long)taskMask);
      }
    }
  }
  dpdkReleaseActiveComms(&comms);
}

static void dpdkFaultManagerMain() {
  std::vector<dpdkFaultEvent> events;
  std::vector<ncclNetDpdkComm *> comms;
  int pollMs = ncclParamDpdkFaultManagerPollMs();
  if (pollMs <= 0)
    pollMs = 1;
  while (!dpdkFaultStop.load(std::memory_order_acquire)) {
    {
      std::unique_lock<std::mutex> lock(dpdkFaultMutex);
      dpdkFaultCv.wait_for(lock, std::chrono::milliseconds(pollMs), [] {
        return dpdkFaultStop.load(std::memory_order_acquire) ||
               !dpdkFaultEvents.empty();
      });
      events.swap(dpdkFaultEvents);
    }

    for (const auto &ev : events)
      dpdkHandleLocalFaultEvent(ev);
    events.clear();

    dpdkCollectActiveComms(&comms);
    for (auto *comm : comms)
      dpdkProgressFaultRx(comm);
    dpdkReleaseActiveComms(&comms);
  }
}

static ncclResult_t dpdkStartFaultManager() {
  if (dpdkFaultManagerRunning.load(std::memory_order_acquire))
    return ncclSuccess;
  dpdkFaultStop.store(0, std::memory_order_release);
  try {
    dpdkFaultManagerThread = std::thread(dpdkFaultManagerMain);
  } catch (...) {
    WARN("NET/DPDK : failed to start fault manager thread");
    return ncclSystemError;
  }
  dpdkFaultManagerRunning.store(1, std::memory_order_release);
  return ncclSuccess;
}

static void dpdkStopFaultManager() {
  if (!dpdkFaultManagerRunning.load(std::memory_order_acquire))
    return;
  dpdkFaultStop.store(1, std::memory_order_release);
  dpdkFaultCv.notify_all();
  if (dpdkFaultManagerThread.joinable())
    dpdkFaultManagerThread.join();
  dpdkFaultManagerRunning.store(0, std::memory_order_release);
  {
    std::lock_guard<std::mutex> lock(dpdkFaultMutex);
    dpdkFaultEvents.clear();
  }
}

static void dpdkRaiseLocalFaultEvent(int failedDev) {
  if (failedDev < 0 || failedDev >= ncclNetDpdkIfs)
    return;
  {
    std::lock_guard<std::mutex> lock(dpdkFaultMutex);
    for (const auto &ev : dpdkFaultEvents) {
      if (ev.failedDev == failedDev)
        return;
    }
    dpdkFaultEvents.push_back({failedDev});
  }
  dpdkFaultCv.notify_one();
}

static ncclResult_t dpdkRegisterComm(struct ncclNetDpdkComm *comm) {
  std::lock_guard<std::mutex> lock(ncclNetDpdkCommMutex);
  dpdkActiveComms.push_back(comm);
  return ncclSuccess;
}

static ncclResult_t dpdkUnregisterComm(struct ncclNetDpdkComm *comm) {
  std::lock_guard<std::mutex> lock(ncclNetDpdkCommMutex);
  auto it = std::find(dpdkActiveComms.begin(), dpdkActiveComms.end(), comm);
  if (it != dpdkActiveComms.end())
    dpdkActiveComms.erase(it);
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
    thread->sendTasks.clear();
    thread->recvTasks.clear();
    thread->busyQ16 = 0;
    thread->txAttempts = 0;
    thread->txDrops = 0;
    thread->rxPkts = 0;
    thread->lastLoadTsc = 0;
    thread->lastLinkCheckTsc = 0;
    thread->linkDownCount = 0;
    thread->faultReported = 0;
#if NCCL_DPDK_FAULT_INJECT
    thread->lastFaultInjectTsc = 0;
#endif
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
  NCCLCHECK(dpdkLoadDataIpConfig(&ncclNetDpdkDataIps, &ncclNetDpdkControlIp,
                                  DPDK_MAX_DEVS));
  NCCLCHECK(dpdkInitControlInterface());
  NCCLCHECK(dpdkInitEal());
  NCCLCHECK(dpdkInitDataDevices());
  for (int d = 0; d < ncclNetDpdkIfs; d++) {
    NCCLCHECK(dpdkStartPollThread(d));
  }
  NCCLCHECK(dpdkStartFaultManager());
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
  NCCLCHECK(ncclNetDpdkGetSpeed(devIx, &dev->speedMbps));
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
  props->speed = ncclNetDpdkDevs[dev].speedMbps;
  props->guid = dev;
  props->ptrSupport = NCCL_PTR_HOST;
  props->regIsGlobal = 0;
  props->forceFlush = 0;
  props->latency = 0;
  props->port = 0;
  props->maxComms = 65536;
  props->maxRecvs = 1;
  props->netDeviceType = NCCL_NET_DEVICE_HOST;
  props->netDeviceVersion = NCCL_NET_DEVICE_INVALID_VERSION;
  props->maxP2pBytes = NCCL_MAX_NET_SIZE_BYTES;
  props->maxCollBytes = MAX_COLLNET_SIZE;
  props->maxMultiRequestSize = 1;

  // Only support synthetic vNIC
  props->vProps = {0};
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

  // Non-blocking dual-channel state machine:
  // TCP(ctrl) -> hello(ctrl) -> TCP(fault) -> hello(fault) -> register.
  *sendComm = NULL;
  if (stage->state == ncclNetDpdkCommStateConnectCtrl)
    goto dpdk_connect_ctrl_check;
  if (stage->state == ncclNetDpdkCommStateHelloSendCtrl)
    goto dpdk_hello_ctrl_send;
  if (stage->state == ncclNetDpdkCommStateConnectFault)
    goto dpdk_connect_fault_check;
  if (stage->state == ncclNetDpdkCommStateHelloSendFault)
    goto dpdk_hello_fault_send;

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
  stage->state = ncclNetDpdkCommStateConnectCtrl;
  stage->ioOffset = 0;
  stage->acceptedChannelMask = 0;
  memset(&stage->hello, 0, sizeof(stage->hello));
  NCCLCHECK(ncclSocketConnect(&comm->ctrlSock));

dpdk_connect_ctrl_check:
  NCCLCHECK(ncclSocketReady(stage->sock, &ready));
  if (!ready)
    return ncclSuccess;
  stage->state = ncclNetDpdkCommStateHelloSendCtrl;
  stage->ioOffset = 0;

dpdk_hello_ctrl_send:
  if (stage->ioOffset == 0) {
    memset(&stage->hello, 0, sizeof(stage->hello));
    stage->hello.mac = comm->localMac;
    stage->hello.dataIp = comm->localIp;
    stage->hello.commId = comm->commId;
    stage->hello.channel = DPDK_HELLO_CHANNEL_CTRL;
  }
  NCCLCHECK(ncclSocketProgress(NCCL_SOCKET_SEND, stage->sock, &stage->hello,
                               sizeof(stage->hello), &stage->ioOffset));
  if (stage->ioOffset < (int)sizeof(stage->hello))
    return ncclSuccess;

  NCCLCHECK(ncclSocketInit(&comm->faultSock, &handle->connectAddr, handle->magic,
                           ncclSocketTypeNetSocket, NULL, 1));
  stage->sock = &comm->faultSock;
  stage->state = ncclNetDpdkCommStateConnectFault;
  stage->ioOffset = 0;
  NCCLCHECK(ncclSocketConnect(&comm->faultSock));

dpdk_connect_fault_check:
  NCCLCHECK(ncclSocketReady(stage->sock, &ready));
  if (!ready)
    return ncclSuccess;
  stage->state = ncclNetDpdkCommStateHelloSendFault;
  stage->ioOffset = 0;

dpdk_hello_fault_send:
  if (stage->ioOffset == 0) {
    memset(&stage->hello, 0, sizeof(stage->hello));
    stage->hello.mac = comm->localMac;
    stage->hello.dataIp = comm->localIp;
    stage->hello.commId = comm->commId;
    stage->hello.channel = DPDK_HELLO_CHANNEL_FAULT;
  }
  NCCLCHECK(ncclSocketProgress(NCCL_SOCKET_SEND, stage->sock, &stage->hello,
                               sizeof(stage->hello), &stage->ioOffset));
  if (stage->ioOffset < (int)sizeof(stage->hello))
    return ncclSuccess;

  NCCLCHECK(dpdkRegisterComm(comm));
  *sendComm = comm;
  stage->state = ncclNetDpdkCommStateStart;
  stage->sock = NULL;
  stage->comm = NULL;
  stage->ioOffset = 0;
  stage->acceptedChannelMask = 0;
  memset(&stage->hello, 0, sizeof(stage->hello));
  return ncclSuccess;
}

ncclResult_t ncclNetDpdkAccept(void *listenComm, void **recvComm,
                               ncclNetDeviceHandle_t ** /*recvDevComm*/) {
  struct ncclNetDpdkListenComm *lComm =
      (struct ncclNetDpdkListenComm *)listenComm;
  struct ncclNetDpdkCommStage *stage = &lComm->stage;
  struct ncclNetDpdkComm *rComm = stage->comm;
  struct ncclSocket *sock = stage->sock;
  ncclResult_t ret = ncclSuccess;
  int ready;

  // Non-blocking dual-channel accept:
  // accept twice -> classify by hello.channel -> bind ctrl/fault sockets.
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
  stage->acceptedChannelMask = 0;
  memset(&stage->hello, 0, sizeof(stage->hello));

  NCCLCHECKGOTO(ncclCalloc(&sock, 1), ret, fail);
  NCCLCHECKGOTO(ncclSocketInit(sock), ret, fail);
  stage->sock = sock;
  stage->state = ncclNetDpdkCommStateAccept;
  stage->ioOffset = 0;
  NCCLCHECKGOTO(ncclSocketAccept(sock, &lComm->sock), ret, fail);

dpdk_accept_check:
  NCCLCHECKGOTO(ncclSocketReady(stage->sock, &ready), ret, fail);
  if (!ready)
    return ncclSuccess;
  stage->state = ncclNetDpdkCommStateHelloRecv;
  stage->ioOffset = 0;
  memset(&stage->hello, 0, sizeof(stage->hello));

dpdk_hello_recv:
  NCCLCHECKGOTO(ncclSocketProgress(NCCL_SOCKET_RECV, stage->sock, &stage->hello,
                                   sizeof(stage->hello), &stage->ioOffset),
                ret, fail);
  if (stage->ioOffset < (int)sizeof(stage->hello))
    return ncclSuccess;

  if (stage->hello.channel != DPDK_HELLO_CHANNEL_CTRL &&
      stage->hello.channel != DPDK_HELLO_CHANNEL_FAULT) {
    WARN("NET/DPDK : invalid hello channel %u", stage->hello.channel);
    ret = ncclInvalidUsage;
    goto fail;
  }

  if (rComm->remoteCommId == 0) {
    rComm->remoteCommId = stage->hello.commId;
    rComm->remoteIp = stage->hello.dataIp;
    rComm->remoteMac = stage->hello.mac;
  } else if (rComm->remoteCommId != stage->hello.commId) {
    WARN("NET/DPDK : mismatched hello commId %u != %u",
         rComm->remoteCommId, stage->hello.commId);
    ret = ncclInvalidUsage;
    goto fail;
  }

  if (stage->hello.channel == DPDK_HELLO_CHANNEL_CTRL) {
    if (stage->acceptedChannelMask & 0x1) {
      WARN("NET/DPDK : duplicate ctrl channel accept");
      ret = ncclInvalidUsage;
      goto fail;
    }
    rComm->ctrlSock = *sock;
    stage->acceptedChannelMask |= 0x1;
  } else {
    if (stage->acceptedChannelMask & 0x2) {
      WARN("NET/DPDK : duplicate fault channel accept");
      ret = ncclInvalidUsage;
      goto fail;
    }
    rComm->faultSock = *sock;
    stage->acceptedChannelMask |= 0x2;
  }
  free(sock);
  sock = NULL;
  stage->sock = NULL;

  if (stage->acceptedChannelMask != 0x3) {
    sock = NULL;
    NCCLCHECKGOTO(ncclCalloc(&sock, 1), ret, fail);
    NCCLCHECKGOTO(ncclSocketInit(sock), ret, fail);
    stage->sock = sock;
    stage->state = ncclNetDpdkCommStateAccept;
    stage->ioOffset = 0;
    NCCLCHECKGOTO(ncclSocketAccept(sock, &lComm->sock), ret, fail);
    return ncclSuccess;
  }

  NCCLCHECKGOTO(dpdkRegisterComm(rComm), ret, fail);
  *recvComm = rComm;

  stage->state = ncclNetDpdkCommStateStart;
  stage->sock = NULL;
  stage->comm = NULL;
  stage->ioOffset = 0;
  stage->acceptedChannelMask = 0;
  memset(&stage->hello, 0, sizeof(stage->hello));
  return ncclSuccess;

fail:
  if (sock) {
    (void)ncclSocketClose(sock);
    free(sock);
  }
  if (stage->sock && stage->sock != sock) {
    (void)ncclSocketClose(stage->sock);
    free(stage->sock);
  }
  stage->sock = NULL;
  if (stage->comm) {
    if (stage->comm->ctrlSock.fd >= 0)
      (void)ncclSocketClose(&stage->comm->ctrlSock);
    if (stage->comm->faultSock.fd >= 0)
      (void)ncclSocketClose(&stage->comm->faultSock);
    delete stage->comm;
    stage->comm = NULL;
  }
  stage->state = ncclNetDpdkCommStateStart;
  stage->ioOffset = 0;
  stage->acceptedChannelMask = 0;
  memset(&stage->hello, 0, sizeof(stage->hello));
  return ret;
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
  // Handshake ownership:
  // 1) Sender publishes SEND metadata + local lane candidates.
  // 2) Receiver builds pair plan and replies READY with lane pairing.
  // 3) Both sides map tasks to attachedDev/endpoint and attach tasks to poll threads.
  *done = 0;
  struct ncclNetDpdkRequest *r = (struct ncclNetDpdkRequest *)request;
  if (r == NULL) {
    WARN("NET/DPDK : test called with NULL request");
    return ncclInternalError;
  }
  int reqErr = r->errorCode.load(std::memory_order_acquire);
  if (reqErr != 0) {
    r->state.store(DPDK_REQ_UNUSED, std::memory_order_release);
    dpdkDetachRequestTasks(r);
    r->inUse = 0;
    return (ncclResult_t)reqErr;
  }
  struct ncclNetDpdkComm *comm = r->comm;

  if (r->op == NCCL_SOCKET_SEND) {
    int state = r->state.load(std::memory_order_acquire);
    if (state == DPDK_REQ_SEND_CTRL) {
      if (r->ctrlMsgOffset == 0) {
        // Build deterministic task geometry first so receiver can pair by taskId.
        if (r->send.tasks == NULL && r->size > 0 && r->frameSize > 0) {
          NCCLCHECK(dpdkBuildRequestTasks(r, r->frameSize, r->size,
                                          NCCL_SOCKET_SEND));
        }
        // Advertise sender-side lane candidates to peer.
        r->localLaneCount = (uint16_t)dpdkCollectLocalLanes(
            comm, r->numTasks, r->localLanes, DPDK_MAX_CTRL_LANES);
        r->ctrlMsg.magic = DPDK_CTRL_MAGIC;
        r->ctrlMsg.version = DPDK_PROTO_VERSION;
        r->ctrlMsg.type = DPDK_CTRL_SEND;
        r->ctrlMsg.srcReqId = r->reqId;
        r->ctrlMsg.dstReqId = 0;
        r->ctrlMsg.size = r->size;
        r->ctrlMsg.frameSize = (uint32_t)r->frameSize;
        r->ctrlMsg.numTasks = (uint16_t)std::max(0, r->numTasks);
        r->ctrlMsg.laneCount = r->localLaneCount;
        memset(r->ctrlMsg.lanes, 0, sizeof(r->ctrlMsg.lanes));
        for (int i = 0; i < r->localLaneCount; i++)
          r->ctrlMsg.lanes[i] = r->localLanes[i];
        r->ctrlMsg.pairCount = 0;
        memset(r->ctrlMsg.pairs, 0, sizeof(r->ctrlMsg.pairs));
        r->ctrlMsg.pairSeed = 0;
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
      // Consume READY, import receiver lane/pair decision, then attach send tasks.
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
      int peerCount = 0;
      dpdkCopyCtrlLanesFromMsg(&r->ctrlMsg, r->peerLanes, &peerCount);
      r->peerLaneCount = (uint16_t)peerCount;
      r->pairCount = std::min((uint16_t)DPDK_MAX_CTRL_PAIRS, r->ctrlMsg.pairCount);
      r->pairWeightSum = 0;
      for (int i = 0; i < r->pairCount; i++) {
        r->pairs[i] = r->ctrlMsg.pairs[i];
        r->pairWeightSum = (uint16_t)(r->pairWeightSum + std::max<int>(1, r->pairs[i].weight));
      }
      r->pairSeed = r->ctrlMsg.pairSeed;
      // If load balance is disabled or peer returns no pair, force single-lane fallback.
      if (!dpdkLbBalanceEnabled()) {
        r->pairCount = 1;
        r->pairs[0].sendLane = 0;
        r->pairs[0].recvLane = 0;
        r->pairs[0].weight = 1;
        r->pairWeightSum = 1;
      } else if (r->pairCount == 0) {
        r->pairCount = 1;
        r->pairs[0].sendLane = 0;
        r->pairs[0].recvLane = 0;
        r->pairs[0].weight = 1;
        r->pairWeightSum = 1;
      }
      // Materialize per-task attachedDev/endpoint and register tasks to poll threads.
      dpdkApplySendTaskPairing(r);
      dpdkReportLbAssignment(r, "SEND");
      NCCLCHECK(dpdkAttachRequestTasks(r));
      r->isCompleted.store(0, std::memory_order_relaxed);
      if (r->numFrames == 0) {
        r->isCompleted.store(1, std::memory_order_release);
      }
      r->state.store(DPDK_REQ_SENDING, std::memory_order_release);
      state = DPDK_REQ_SENDING;
    }
    if (state == DPDK_REQ_SENDING) {
      if (r->isCompleted.load(std::memory_order_acquire)) {
        // Request is done: detach all task-thread bindings before slot reuse.
        r->state.store(DPDK_REQ_UNUSED, std::memory_order_release);
        dpdkDetachRequestTasks(r);
        if (size)
          *size = r->size;
        *done = 1;
        r->inUse = 0;
        return ncclSuccess;
      }
    }
    return ncclSuccess;
  }

  if (r->op == NCCL_SOCKET_RECV) {
    int state = r->state.load(std::memory_order_acquire);
    if (state == DPDK_REQ_RECV_CTRL) {
      // Receive SEND, validate metadata, then compute receiver-side pair plan.
      NCCLCHECK(ncclSocketProgress(NCCL_SOCKET_RECV, &comm->ctrlSock,
                                   &r->ctrlMsg, sizeof(r->ctrlMsg),
                                   &r->ctrlMsgOffset));
      if (r->ctrlMsgOffset < (int)sizeof(r->ctrlMsg))
        return ncclSuccess;
      if (r->ctrlMsg.magic != DPDK_CTRL_MAGIC ||
          r->ctrlMsg.version != DPDK_PROTO_VERSION ||
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
      r->peerReqId = r->ctrlMsg.srcReqId;
      r->size = (int)r->ctrlMsg.size;
      r->frameSize = (int)r->ctrlMsg.frameSize;
      int peerCount = 0;
      dpdkCopyCtrlLanesFromMsg(&r->ctrlMsg, r->peerLanes, &peerCount);
      r->peerLaneCount = (uint16_t)peerCount;
      NCCLCHECK(dpdkBuildRequestTasks(r, r->frameSize, r->size,
                                      NCCL_SOCKET_RECV));
      // Receiver chooses local candidates and computes lane-pair weights.
      r->localLaneCount = (uint16_t)dpdkCollectLocalLanes(
          comm, r->numTasks, r->localLanes, DPDK_MAX_CTRL_LANES);
      if (dpdkLbBalanceEnabled()) {
        r->pairCount = (uint16_t)dpdkBuildLanePairPlan(
            r->peerLanes, r->peerLaneCount, r->localLanes, r->localLaneCount,
            r->pairs, DPDK_MAX_CTRL_PAIRS, &r->pairWeightSum);
      } else {
        r->pairCount = 1;
        r->pairWeightSum = 1;
        r->pairs[0].sendLane = 0;
        r->pairs[0].recvLane = 0;
        r->pairs[0].weight = 1;
      }
      if (r->pairCount == 0) {
        r->pairCount = 1;
        r->pairs[0].sendLane = 0;
        r->pairs[0].recvLane = 0;
        r->pairs[0].weight = 1;
        r->pairWeightSum = 1;
      }
      // Pair plan is now fixed; map recv tasks and attach them before READY reply.
      r->pairSeed = dpdkHash32(r->reqId ^ (r->peerReqId * 0x9e3779b9U) ^
                               (uint32_t)r->size);
      dpdkApplyRecvTaskPairing(r);
      dpdkReportLbAssignment(r, "RECV");
      NCCLCHECK(dpdkAttachRequestTasks(r));
      r->state.store(DPDK_REQ_SEND_READY, std::memory_order_release);
      r->ctrlMsgOffset = 0;
      state = DPDK_REQ_SEND_READY;
    }
    if (state == DPDK_REQ_SEND_READY) {
      if (r->ctrlMsgOffset == 0) {
        // Reply READY with receiver-selected lane candidates + pair plan.
        r->ctrlMsg.magic = DPDK_CTRL_MAGIC;
        r->ctrlMsg.version = DPDK_PROTO_VERSION;
        r->ctrlMsg.type = DPDK_CTRL_READY;
        r->ctrlMsg.srcReqId = r->reqId;
        r->ctrlMsg.dstReqId = r->peerReqId;
        r->ctrlMsg.size = r->size;
        r->ctrlMsg.frameSize = (uint32_t)r->frameSize;
        r->ctrlMsg.numTasks = (uint16_t)std::max(0, r->numTasks);
        r->ctrlMsg.laneCount = r->localLaneCount;
        memset(r->ctrlMsg.lanes, 0, sizeof(r->ctrlMsg.lanes));
        for (int i = 0; i < r->localLaneCount; i++)
          r->ctrlMsg.lanes[i] = r->localLanes[i];
        r->ctrlMsg.pairCount = r->pairCount;
        memset(r->ctrlMsg.pairs, 0, sizeof(r->ctrlMsg.pairs));
        for (int i = 0; i < r->pairCount; i++)
          r->ctrlMsg.pairs[i] = r->pairs[i];
        r->ctrlMsg.pairSeed = r->pairSeed;
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
        // Recv request is done: detach task-thread bindings before slot reuse.
        r->state.store(DPDK_REQ_UNUSED, std::memory_order_release);
        dpdkDetachRequestTasks(r);
        if (size)
          *size = r->size;
        *done = 1;
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
    for (int i = 0; i < DPDK_MAX_REQUESTS; i++) {
      struct ncclNetDpdkRequest *req = comm->requests + i;
      if (req->inUse) {
        req->state.store(DPDK_REQ_UNUSED, std::memory_order_release);
        dpdkDetachRequestTasks(req);
      }
    }
    dpdkUnregisterComm(comm);
    int ready;
    NCCLCHECK(ncclSocketReady(&comm->ctrlSock, &ready));
    if (ready)
      NCCLCHECK(ncclSocketClose(&comm->ctrlSock));
    if (comm->faultSock.fd >= 0)
      (void)ncclSocketClose(&comm->faultSock);
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
  dpdkStopFaultManager();
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
