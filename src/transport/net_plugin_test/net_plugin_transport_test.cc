#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>
#include <vector>

#include "net.h"

// Link-time binding to one internal transport plugin object selected at build time.
#if defined(NETTEST_USE_DPDK)
extern ncclNet_t ncclNetDpdkSocket;
static ncclNet_t* selectedNet() { return &ncclNetDpdkSocket; }
static const char* selectedNetSymbol() { return "ncclNetDpdkSocket"; }
#else
extern ncclNet_t ncclNetSocket;
static ncclNet_t* selectedNet() { return &ncclNetSocket; }
static const char* selectedNetSymbol() { return "ncclNetSocket"; }
#endif

struct Options {
  int dev = -1;         // -1 => auto choose dev 0
  int timeoutMs = 10000;
  bool verbose = false;
};

static void usage(const char* prog) {
  std::cerr
      << "Usage: " << prog << " [--dev <idx>] [--timeout-ms <ms>] [--verbose]\n"
      << "Examples:\n"
      << "  " << prog << "\n"
      << "  " << prog << " --dev 0 --timeout-ms 15000 --verbose\n";
}

static bool parseInt(const char* s, int* out) {
  char* end = nullptr;
  long v = strtol(s, &end, 10);
  if (s == end || *end != '\0') return false;
  if (v < INT32_MIN || v > INT32_MAX) return false;
  *out = (int)v;
  return true;
}

static bool parseArgs(int argc, char** argv, Options* opt) {
  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    if (a == "--dev" && i + 1 < argc) {
      if (!parseInt(argv[++i], &opt->dev)) return false;
    } else if (a == "--timeout-ms" && i + 1 < argc) {
      if (!parseInt(argv[++i], &opt->timeoutMs)) return false;
    } else if (a == "--verbose") {
      opt->verbose = true;
    } else if (a == "-h" || a == "--help") {
      return false;
    } else {
      std::cerr << "Unknown arg: " << a << "\n";
      return false;
    }
  }
  return opt->timeoutMs > 0;
}

static void fillPattern(std::vector<uint8_t>& buf, int seed) {
  for (size_t i = 0; i < buf.size(); i++) {
    buf[i] = (uint8_t)((i * 131u + seed * 17u + 23u) & 0xff);
  }
}

static bool equalPrefix(const std::vector<uint8_t>& a, const std::vector<uint8_t>& b, size_t n) {
  if (a.size() < n || b.size() < n) return false;
  return std::memcmp(a.data(), b.data(), n) == 0;
}

struct StepStatus {
  bool ok = false;
  std::string err;
};

// connect()/accept() are non-blocking in the plugin contract.
static StepStatus connectWorker(ncclNet_t* net, void* ctx, int dev, void* handle,
                                int timeoutMs, std::atomic<bool>* listenReady,
                                void** sendComm) {
  StepStatus st;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
  while (!listenReady->load(std::memory_order_acquire) && std::chrono::steady_clock::now() < deadline) {
    usleep(200);
  }
  if (!listenReady->load(std::memory_order_acquire)) {
    st.err = "connect timeout waiting for listen";
    return st;
  }

  while (*sendComm == nullptr && std::chrono::steady_clock::now() < deadline) {
    ncclResult_t r = net->connect(ctx, dev, handle, sendComm, nullptr);
    if (r != ncclSuccess) {
      st.err = "connect failed rc=" + std::to_string((int)r);
      return st;
    }
    if (*sendComm == nullptr) usleep(1000);
  }
  if (*sendComm == nullptr) {
    st.err = "connect timeout";
    return st;
  }
  st.ok = true;
  return st;
}

static StepStatus listenAcceptWorker(ncclNet_t* net, void* ctx, int dev, void* handle,
                                     int timeoutMs, std::atomic<bool>* listenReady,
                                     void** listenComm, void** recvComm) {
  StepStatus st;
  ncclResult_t r = net->listen(ctx, dev, handle, listenComm);
  if (r != ncclSuccess || *listenComm == nullptr) {
    st.err = "listen failed rc=" + std::to_string((int)r);
    return st;
  }
  listenReady->store(true, std::memory_order_release);

  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
  while (*recvComm == nullptr && std::chrono::steady_clock::now() < deadline) {
    r = net->accept(*listenComm, recvComm, nullptr);
    if (r != ncclSuccess) {
      st.err = "accept failed rc=" + std::to_string((int)r);
      return st;
    }
    if (*recvComm == nullptr) usleep(1000);
  }
  if (*recvComm == nullptr) {
    st.err = "accept timeout";
    return st;
  }
  st.ok = true;
  return st;
}

static StepStatus recvWorker(ncclNet_t* net, void* recvComm, std::vector<uint8_t>& recvBuf,
                             size_t size, int tag, int timeoutMs) {
  StepStatus st;
  void* recvReq = nullptr;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
  while (std::chrono::steady_clock::now() < deadline) {
    if (recvReq == nullptr) {
      void* data = size ? (void*)recvBuf.data() : nullptr;
      size_t len = size;
      int rtag = tag;
      void* mr = nullptr;
      ncclResult_t r = net->irecv(recvComm, 1, &data, &len, &rtag, &mr, nullptr, &recvReq);
      if (r != ncclSuccess) {
        st.err = "irecv failed rc=" + std::to_string((int)r);
        return st;
      }
    }
    int done = 0;
    int got = -1;
    ncclResult_t r = net->test(recvReq, &done, &got);
    if (r != ncclSuccess) {
      st.err = "test(recv) failed rc=" + std::to_string((int)r);
      return st;
    }
    if (done) {
      if (got >= 0 && (size_t)got != size) {
        st.err = "recv size mismatch expected=" + std::to_string(size) +
                 " got=" + std::to_string(got);
        return st;
      }
      st.ok = true;
      return st;
    }
    usleep(200);
  }
  st.err = "recv timeout";
  return st;
}

static StepStatus sendWorker(ncclNet_t* net, void* sendComm, std::vector<uint8_t>& sendBuf,
                             size_t size, int tag, int timeoutMs) {
  StepStatus st;
  void* sendReq = nullptr;
  auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
  while (std::chrono::steady_clock::now() < deadline) {
    if (sendReq == nullptr) {
      void* data = size ? (void*)sendBuf.data() : nullptr;
      ncclResult_t r = net->isend(sendComm, data, size, tag, nullptr, nullptr, &sendReq);
      if (r != ncclSuccess) {
        st.err = "isend failed rc=" + std::to_string((int)r);
        return st;
      }
    }
    int done = 0;
    int sent = -1;
    ncclResult_t r = net->test(sendReq, &done, &sent);
    if (r != ncclSuccess) {
      st.err = "test(send) failed rc=" + std::to_string((int)r);
      return st;
    }
    if (done) {
      if (sent >= 0 && (size_t)sent != size) {
        st.err = "send size mismatch expected=" + std::to_string(size) +
                 " sent=" + std::to_string(sent);
        return st;
      }
      st.ok = true;
      return st;
    }
    usleep(200);
  }
  st.err = "send timeout";
  return st;
}

struct TransferTask {
  size_t size;
  int tag;
  bool verbose;
};

static StepStatus sendThreadMain(ncclNet_t* net, void* ctx, int dev, void* handle,
                                 int timeoutMs, std::atomic<bool>* listenReady,
                                 const std::vector<TransferTask>& tasks, size_t maxBytes) {
  StepStatus st;
  void* sendComm = nullptr;
  StepStatus conn = connectWorker(net, ctx, dev, handle, timeoutMs, listenReady, &sendComm);
  if (!conn.ok) return conn;

  std::vector<uint8_t> sendBuf(maxBytes ? maxBytes : 1);
  for (const auto& task : tasks) {
    fillPattern(sendBuf, task.tag);
    StepStatus tx = sendWorker(net, sendComm, sendBuf, task.size, task.tag, timeoutMs);
    if (!tx.ok) {
      tx.err += ", size=" + std::to_string(task.size) + " tag=" + std::to_string(task.tag);
      if (sendComm) net->closeSend(sendComm);
      return tx;
    }
  }

  net->closeSend(sendComm);
  st.ok = true;
  return st;
}

static StepStatus recvThreadMain(ncclNet_t* net, void* ctx, int dev, void* handle,
                                 int timeoutMs, std::atomic<bool>* listenReady,
                                 const std::vector<TransferTask>& tasks, size_t maxBytes) {
  StepStatus st;
  void* listenComm = nullptr;
  void* recvComm = nullptr;
  StepStatus conn = listenAcceptWorker(net, ctx, dev, handle, timeoutMs,
                                       listenReady, &listenComm, &recvComm);
  if (!conn.ok) return conn;

  std::vector<uint8_t> recvBuf(maxBytes ? maxBytes : 1);
  std::vector<uint8_t> expectBuf(maxBytes ? maxBytes : 1);
  for (const auto& task : tasks) {
    std::memset(recvBuf.data(), 0, task.size);
    StepStatus rx = recvWorker(net, recvComm, recvBuf, task.size, task.tag, timeoutMs);
    if (!rx.ok) {
      rx.err += ", size=" + std::to_string(task.size) + " tag=" + std::to_string(task.tag);
      if (recvComm) net->closeRecv(recvComm);
      if (listenComm) net->closeListen(listenComm);
      return rx;
    }
    fillPattern(expectBuf, task.tag);
    if (!equalPrefix(expectBuf, recvBuf, task.size)) {
      st.err = "payload mismatch, size=" + std::to_string(task.size) +
               " tag=" + std::to_string(task.tag);
      if (recvComm) net->closeRecv(recvComm);
      if (listenComm) net->closeListen(listenComm);
      return st;
    }
    if (task.verbose) std::cout << "  [OK] size=" << task.size << " tag=" << task.tag << "\n";
  }

  net->closeRecv(recvComm);
  net->closeListen(listenComm);
  st.ok = true;
  return st;
}

int main(int argc, char** argv) {
  Options opt;
  if (!parseArgs(argc, argv, &opt)) {
    usage(argv[0]);
    return 2;
  }

  ncclNet_t* net = selectedNet();
  std::cout << "Using internal transport plugin object directly (" << selectedNetSymbol() << "): "
            << (net->name ? net->name : "<null>") << "\n";

  void* ctx = nullptr;
  ncclNetCommConfig_t cfg{};
  cfg.trafficClass = NCCL_NET_TRAFFIC_CLASS_UNDEF;
  ncclResult_t r = net->init(&ctx, /*commId=*/0x12345678ULL, &cfg, nullptr, nullptr);
  if (r != ncclSuccess) {
    std::cerr << "init failed, rc=" << (int)r << "\n";
    return 1;
  }

  int ndev = 0;
  r = net->devices(&ndev);
  if (r != ncclSuccess || ndev <= 0) {
    std::cerr << "devices failed or no device, rc=" << (int)r << " ndev=" << ndev << "\n";
    if (net->finalize) net->finalize(ctx);
    return 1;
  }
  int dev = (opt.dev >= 0) ? opt.dev : 0;
  if (dev < 0 || dev >= ndev) {
    std::cerr << "invalid dev index " << dev << ", available [0.." << (ndev - 1) << "]\n";
    if (net->finalize) net->finalize(ctx);
    return 1;
  }

  ncclNetProperties_t props{};
  r = net->getProperties(dev, &props);
  if (r != ncclSuccess) {
    std::cerr << "getProperties failed, rc=" << (int)r << "\n";
    if (net->finalize) net->finalize(ctx);
    return 1;
  }
  std::cout << "Using dev " << dev << " name=" << (props.name ? props.name : "<null>")
            << " ptrSupport=" << props.ptrSupport
            << " maxRecvs=" << props.maxRecvs
            << " maxP2pBytes=" << props.maxP2pBytes
            << "\n";

  size_t maxBytes = 1 << 20;
  if (props.maxP2pBytes > 0 && props.maxP2pBytes < maxBytes) maxBytes = props.maxP2pBytes;
  std::vector<size_t> sizes = {0, 1, 7, 64, 4096, 65536, maxBytes};
  if (maxBytes < 65536) {
    sizes = {0, 1, 7, 64, maxBytes};
  }

  std::vector<TransferTask> tasks;
  int tag = 100;
  for (size_t sz : sizes) {
    tasks.push_back({sz, tag, opt.verbose});
    tag++;
  }

  // Back-to-back stress for a medium payload.
  size_t stress = std::min<size_t>(32768, maxBytes);
  for (int i = 0; i < 32; i++) {
    tasks.push_back({stress, tag + i, false});
  }

  char handle[NCCL_NET_HANDLE_MAXSIZE];
  std::memset(handle, 0, sizeof(handle));
  std::atomic<bool> listenReady(false);
  StepStatus sendStatus;
  StepStatus recvStatus;
  std::thread sendThread([&]() {
    sendStatus = sendThreadMain(net, ctx, dev, handle, opt.timeoutMs, &listenReady, tasks, maxBytes);
  });
  std::thread recvThread([&]() {
    recvStatus = recvThreadMain(net, ctx, dev, handle, opt.timeoutMs, &listenReady, tasks, maxBytes);
  });
  sendThread.join();
  recvThread.join();

  bool ok = sendStatus.ok && recvStatus.ok;
  if (!sendStatus.ok) std::cerr << sendStatus.err << "\n";
  if (!recvStatus.ok) std::cerr << recvStatus.err << "\n";

  if (net->finalize) net->finalize(ctx);

  if (ok) {
    std::cout << "[PASS] net transport integration test passed.\n";
    return 0;
  }
  std::cout << "[FAIL] net transport integration test failed.\n";
  return 1;
}
