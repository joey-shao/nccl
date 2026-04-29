#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "net.h"
#include "test_log.h"

static const char* kPluginSymbol = "ncclNetPlugin_v11";

struct Options {
  int dev = -1;       // set both sendDev/recvDev if unspecified
  int sendDev = -1;   // -1 => auto choose dev 0
  int recvDev = -1;   // -1 => auto choose dev 0
  int timeoutMs = 10000;
  int taskBytes = 1 << 25; // bytes per task
  int taskCount = 1;     // number of tasks
  bool verbose = false;
  std::string pluginPath;
};

static void usage(const char* prog) {
  std::cerr
      << "Usage: " << prog
      << " --plugin <path> [--dev <idx>] [--send-dev <idx>] [--recv-dev <idx>] "
         "[--timeout-ms <ms>] "
         "[--task-bytes <bytes>] [--task-count <count>] "
         "[--verbose]\n"
      << "Examples:\n"
      << "  " << prog
      << " --plugin ext-net/dpdk/build/libnccl-net-dpdk.so --dev 0 "
         "--timeout-ms 15000 --task-bytes 1048576 "
         "--task-count 64 --verbose\n"
      << "  " << prog
      << " --plugin ext-net/dpdk/build/libnccl-net-dpdk.so "
         "--send-dev 0 --recv-dev 1 --timeout-ms 20000 "
         "--task-count 128 --verbose\n";
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
    if (a == "--plugin" && i + 1 < argc) {
      opt->pluginPath = argv[++i];
    } else if (a == "--dev" && i + 1 < argc) {
      if (!parseInt(argv[++i], &opt->dev)) return false;
    } else if (a == "--send-dev" && i + 1 < argc) {
      if (!parseInt(argv[++i], &opt->sendDev)) return false;
    } else if (a == "--recv-dev" && i + 1 < argc) {
      if (!parseInt(argv[++i], &opt->recvDev)) return false;
    } else if (a == "--timeout-ms" && i + 1 < argc) {
      if (!parseInt(argv[++i], &opt->timeoutMs)) return false;
    } else if (a == "--task-bytes" && i + 1 < argc) {
      if (!parseInt(argv[++i], &opt->taskBytes)) return false;
    } else if (a == "--task-count" && i + 1 < argc) {
      if (!parseInt(argv[++i], &opt->taskCount)) return false;
    } else if (a == "--verbose") {
      opt->verbose = true;
    } else if (a == "-h" || a == "--help") {
      return false;
    } else {
      std::cerr << "Unknown arg: " << a << "\n";
      return false;
    }
  }
  return !opt->pluginPath.empty() && opt->timeoutMs > 0 &&
         opt->taskBytes > 0 && opt->taskCount > 0;
}

static void fillPattern(std::vector<uint8_t>& buf, int seed, size_t n) {
  size_t len = std::min(buf.size(), n);
  for (size_t i = 0; i < len; i++) {
    buf[i] = (uint8_t)((i * 131u + seed * 17u + 23u) & 0xff);
  }
}

static double bytesToMB(size_t bytes) {
  return (double)bytes / (1024.0 * 1024.0);
}

static double throughputMBps(size_t bytes, double elapsedMs) {
  double sec = elapsedMs / 1000.0;
  return (sec > 0.0) ? (bytesToMB(bytes) / sec) : 0.0;
}

static bool equalPrefix(const std::vector<uint8_t>& a,
                        const std::vector<uint8_t>& b, size_t n) {
  if (a.size() < n || b.size() < n) return false;
  return std::memcmp(a.data(), b.data(), n) == 0;
}

struct StepStatus {
  bool ok = false;
  std::string err;
  double transferMs = 0.0; // worker execution duration
};

// Reusable 2-party barrier to align send/recv task launch points.
struct TwoThreadBarrier {
  std::mutex mu;
  std::condition_variable cv;
  int arrived = 0;
  uint64_t generation = 0;
  bool broken = false;

  bool wait(int timeoutMs) {
    auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
    std::unique_lock<std::mutex> lock(mu);
    if (broken) return false;
    uint64_t gen = generation;
    arrived++;
    if (arrived == 2) {
      arrived = 0;
      generation++;
      cv.notify_all();
      return true;
    }
    while (!broken && generation == gen) {
      if (cv.wait_until(lock, deadline) == std::cv_status::timeout) {
        if (!broken && generation == gen) {
          broken = true;
          cv.notify_all();
          return false;
        }
      }
    }
    return !broken;
  }

  void breakAll() {
    std::lock_guard<std::mutex> lock(mu);
    broken = true;
    cv.notify_all();
  }
};

struct LoadedPlugin {
  void* handle = nullptr;
  ncclNet_t* net = nullptr;
};

static StepStatus loadPlugin(const std::string& path, LoadedPlugin* plugin) {
  StepStatus st;
  plugin->handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (plugin->handle == nullptr) {
    st.err = std::string("dlopen failed: ") +
             (dlerror() ? dlerror() : "unknown error");
    return st;
  }
  void* sym = dlsym(plugin->handle, kPluginSymbol);
  if (sym == nullptr) {
    st.err = std::string("dlsym(") + kPluginSymbol + ") failed: " +
             (dlerror() ? dlerror() : "unknown error");
    dlclose(plugin->handle);
    plugin->handle = nullptr;
    return st;
  }
  plugin->net = reinterpret_cast<ncclNet_t*>(sym);
  st.ok = true;
  return st;
}

static void unloadPlugin(LoadedPlugin* plugin) {
  plugin->net = nullptr;
  if (plugin->handle) {
    dlclose(plugin->handle);
    plugin->handle = nullptr;
  }
}


// connect()/accept() are non-blocking in the plugin contract.
static StepStatus connectWorker(ncclNet_t* net, void* ctx, int dev, void* handle,
                                int timeoutMs, std::atomic<bool>* listenReady,
                                void** sendComm) {
  StepStatus st;
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
  while (!listenReady->load(std::memory_order_acquire) &&
         std::chrono::steady_clock::now() < deadline) {
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

static StepStatus listenAcceptWorker(ncclNet_t* net, void* ctx, int dev,
                                     void* handle, int timeoutMs,
                                     std::atomic<bool>* listenReady,
                                     void** listenComm, void** recvComm) {
  StepStatus st;
  ncclResult_t r = net->listen(ctx, dev, handle, listenComm);
  if (r != ncclSuccess || *listenComm == nullptr) {
    st.err = "listen failed rc=" + std::to_string((int)r);
    return st;
  }
  listenReady->store(true, std::memory_order_release);

  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
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

static StepStatus recvWorker(ncclNet_t* net, void* recvComm,
                             std::vector<uint8_t>& recvBuf, size_t size, int tag,
                             int timeoutMs) {
  StepStatus st;
  auto workerStart = std::chrono::steady_clock::now();
  void* recvReq = nullptr;
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
  while (std::chrono::steady_clock::now() < deadline) {
    if (recvReq == nullptr) {
      void* data = size ? (void*)recvBuf.data() : nullptr;
      size_t len = size;
      int rtag = tag;
      void* mr = nullptr;
      ncclResult_t r =
          net->irecv(recvComm, 1, &data, &len, &rtag, &mr, nullptr, &recvReq);
      if (r != ncclSuccess) {
        st.err = "irecv failed rc=" + std::to_string((int)r);
        st.transferMs = std::chrono::duration<double, std::milli>(
                            std::chrono::steady_clock::now() - workerStart)
                            .count();
        return st;
      }
    }
    int done = 0;
    int got = -1;
    ncclResult_t r = net->test(recvReq, &done, &got);
    if (r != ncclSuccess) {
      st.err = "test(recv) failed rc=" + std::to_string((int)r);
      st.transferMs = std::chrono::duration<double, std::milli>(
                          std::chrono::steady_clock::now() - workerStart)
                          .count();
      return st;
    }
    if (done) {
      if (got >= 0 && (size_t)got != size) {
        st.err = "recv size mismatch expected=" + std::to_string(size) +
                 " got=" + std::to_string(got);
        st.transferMs = std::chrono::duration<double, std::milli>(
                            std::chrono::steady_clock::now() - workerStart)
                            .count();
        return st;
      }
      st.ok = true;
      st.transferMs = std::chrono::duration<double, std::milli>(
                          std::chrono::steady_clock::now() - workerStart)
                          .count();
      return st;
    }
    usleep(200);
  }
  st.err = "recv timeout";
  st.transferMs = std::chrono::duration<double, std::milli>(
                      std::chrono::steady_clock::now() - workerStart)
                      .count();
  return st;
}

static StepStatus sendWorker(ncclNet_t* net, void* sendComm,
                             std::vector<uint8_t>& sendBuf, size_t size, int tag,
                             int timeoutMs) {
  StepStatus st;
  auto workerStart = std::chrono::steady_clock::now();
  void* sendReq = nullptr;
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
  while (std::chrono::steady_clock::now() < deadline) {
    if (sendReq == nullptr) {
      void* data = size ? (void*)sendBuf.data() : nullptr;
      ncclResult_t r = net->isend(sendComm, data, size, tag, nullptr, nullptr,
                                  &sendReq);
      if (r != ncclSuccess) {
        st.err = "isend failed rc=" + std::to_string((int)r);
        st.transferMs = std::chrono::duration<double, std::milli>(
                            std::chrono::steady_clock::now() - workerStart)
                            .count();
        return st;
      }
    }
    int done = 0;
    int sent = -1;
    ncclResult_t r = net->test(sendReq, &done, &sent);
    if (r != ncclSuccess) {
      st.err = "test(send) failed rc=" + std::to_string((int)r);
      st.transferMs = std::chrono::duration<double, std::milli>(
                          std::chrono::steady_clock::now() - workerStart)
                          .count();
      return st;
    }
    if (done) {
      if (sent >= 0 && (size_t)sent != size) {
        st.err = "send size mismatch expected=" + std::to_string(size) +
                 " sent=" + std::to_string(sent);
        st.transferMs = std::chrono::duration<double, std::milli>(
                            std::chrono::steady_clock::now() - workerStart)
                            .count();
        return st;
      }
      st.ok = true;
      st.transferMs = std::chrono::duration<double, std::milli>(
                          std::chrono::steady_clock::now() - workerStart)
                          .count();
      return st;
    }
    usleep(200);
  }
  st.err = "send timeout";
  st.transferMs = std::chrono::duration<double, std::milli>(
                      std::chrono::steady_clock::now() - workerStart)
                      .count();
  return st;
}

struct TransferTask {
  size_t size;
  int tag;
  bool verbose;
};

static std::mutex gTaskPrintMutex;

static void printTaskPerf(const char* role, size_t taskIndex,
                          const TransferTask& task, double elapsedMs) {
  std::lock_guard<std::mutex> lock(gTaskPrintMutex);
  std::cout << std::fixed << std::setprecision(3)
            << "  [" << role << "_TASK] idx=" << taskIndex
            << " tag=" << task.tag
            << " sizeMB=" << bytesToMB(task.size)
            << " elapsed_ms=" << elapsedMs
            << " throughput_MBps=" << throughputMBps(task.size, elapsedMs)
            << std::defaultfloat << "\n";
}

static size_t maxTaskBytes(const std::vector<TransferTask>& tasks) {
  size_t maxBytes = 0;
  for (const auto& task : tasks) maxBytes = std::max(maxBytes, task.size);
  return std::max<size_t>(1, maxBytes);
}

static std::vector<TransferTask> buildUniformTasks(size_t taskBytes,
                                                   int taskCount, int tagBase,
                                                   bool verbose) {
  std::vector<TransferTask> tasks;
  tasks.reserve((size_t)taskCount);
  for (int i = 0; i < taskCount; i++) {
    bool taskVerbose = verbose && i < 8;
    tasks.push_back({taskBytes, tagBase + i, taskVerbose});
  }
  return tasks;
}

static StepStatus sendThreadMain(ncclNet_t* net, void* ctx, int dev, void* handle,
                                 int timeoutMs, std::atomic<bool>* listenReady,
                                 const std::vector<TransferTask>& tasks,
                                 TwoThreadBarrier* taskStartBarrier) {
  StepStatus st;
  double sendWorkerTotalMs = 0.0;
  void* sendComm = nullptr;
  StepStatus conn =
      connectWorker(net, ctx, dev, handle, timeoutMs, listenReady, &sendComm);
  if (!conn.ok) {
    if (taskStartBarrier) taskStartBarrier->breakAll();
    return conn;
  }

  std::vector<uint8_t> sendBuf(maxTaskBytes(tasks));
  for (size_t taskIndex = 0; taskIndex < tasks.size(); taskIndex++) {
    const auto& task = tasks[taskIndex];
    fillPattern(sendBuf, task.tag, task.size);
    if (taskStartBarrier && !taskStartBarrier->wait(timeoutMs)) {
      st.err = "task start barrier failed in send path";
      if (sendComm) net->closeSend(sendComm);
      if (taskStartBarrier) taskStartBarrier->breakAll();
      st.transferMs = sendWorkerTotalMs;
      return st;
    }
    StepStatus tx =
        sendWorker(net, sendComm, sendBuf, task.size, task.tag, timeoutMs);
    sendWorkerTotalMs += tx.transferMs;
    if (!tx.ok) {
      tx.err += ", size=" + std::to_string(task.size) +
                " tag=" + std::to_string(task.tag);
      if (sendComm) net->closeSend(sendComm);
      if (taskStartBarrier) taskStartBarrier->breakAll();
      tx.transferMs = sendWorkerTotalMs;
      return tx;
    }
    printTaskPerf("SEND", taskIndex, task, tx.transferMs);
  }

  net->closeSend(sendComm);
  st.ok = true;
  st.transferMs = sendWorkerTotalMs;
  return st;
}

static StepStatus recvThreadMain(ncclNet_t* net, void* ctx, int dev, void* handle,
                                 int timeoutMs, std::atomic<bool>* listenReady,
                                 const std::vector<TransferTask>& tasks,
                                 TwoThreadBarrier* taskStartBarrier) {
  StepStatus st;
  double recvWorkerTotalMs = 0.0;
  void* listenComm = nullptr;
  void* recvComm = nullptr;
  StepStatus conn = listenAcceptWorker(net, ctx, dev, handle, timeoutMs,
                                       listenReady, &listenComm, &recvComm);
  if (!conn.ok) {
    if (taskStartBarrier) taskStartBarrier->breakAll();
    return conn;
  }

  std::vector<uint8_t> recvBuf(maxTaskBytes(tasks));
  std::vector<uint8_t> expectBuf(maxTaskBytes(tasks));
  for (size_t taskIndex = 0; taskIndex < tasks.size(); taskIndex++) {
    const auto& task = tasks[taskIndex];
    std::memset(recvBuf.data(), 0, task.size);
    if (taskStartBarrier && !taskStartBarrier->wait(timeoutMs)) {
      st.err = "task start barrier failed in recv path";
      if (recvComm) net->closeRecv(recvComm);
      if (listenComm) net->closeListen(listenComm);
      if (taskStartBarrier) taskStartBarrier->breakAll();
      st.transferMs = recvWorkerTotalMs;
      return st;
    }
    StepStatus rx =
        recvWorker(net, recvComm, recvBuf, task.size, task.tag, timeoutMs);
    recvWorkerTotalMs += rx.transferMs;
    if (!rx.ok) {
      rx.err += ", size=" + std::to_string(task.size) +
                " tag=" + std::to_string(task.tag);
      if (recvComm) net->closeRecv(recvComm);
      if (listenComm) net->closeListen(listenComm);
      if (taskStartBarrier) taskStartBarrier->breakAll();
      rx.transferMs = recvWorkerTotalMs;
      return rx;
    }
    fillPattern(expectBuf, task.tag, task.size);
    if (!equalPrefix(expectBuf, recvBuf, task.size)) {
      st.err = "payload mismatch, size=" + std::to_string(task.size) +
               " tag=" + std::to_string(task.tag);
      if (recvComm) net->closeRecv(recvComm);
      if (listenComm) net->closeListen(listenComm);
      if (taskStartBarrier) taskStartBarrier->breakAll();
      st.transferMs = recvWorkerTotalMs;
      return st;
    }
    printTaskPerf("RECV", taskIndex, task, rx.transferMs);
  }
  net->closeRecv(recvComm);
  net->closeListen(listenComm);
  st.ok = true;
  st.transferMs = recvWorkerTotalMs;
  return st;
}

int main(int argc, char** argv) {
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-h") == 0 || strcmp(argv[i], "--help") == 0) {
      usage(argv[0]);
      return 0;
    }
  }

  Options opt;
  if (!parseArgs(argc, argv, &opt)) {
    std::cerr << "missing or invalid arguments. '--plugin <path>' is required.\n";
    usage(argv[0]);
    return 2;
  }

  LoadedPlugin plugin;
  StepStatus loaded = loadPlugin(opt.pluginPath, &plugin);
  if (!loaded.ok) {
    std::cerr << "failed to load plugin '" << opt.pluginPath
              << "': " << loaded.err << "\n";
    return 1;
  }
  ncclNet_t* net = plugin.net;
  if (net == nullptr) {
    std::cerr << "selected plugin symbol is null: " << kPluginSymbol << "\n";
    unloadPlugin(&plugin);
    return 1;
  }

  std::cout << "Using external plugin symbol (" << kPluginSymbol << ") from "
            << opt.pluginPath << ": " << (net->name ? net->name : "<null>")
            << "\n";

  void* ctx = nullptr;
  bool initialized = false;
  auto cleanup = [&]() {
    if (initialized && net->finalize) net->finalize(ctx);
    unloadPlugin(&plugin);
  };

  ncclNetCommConfig_t cfg{};
  cfg.trafficClass = NCCL_NET_TRAFFIC_CLASS_UNDEF;
  ncclResult_t r = net->init(&ctx, /*commId=*/0x12345678ULL, &cfg,
                             pluginTestLogFunction, nullptr);
  if (r != ncclSuccess) {
    std::cerr << "init failed, rc=" << (int)r << "\n";
    cleanup();
    return 1;
  }
  initialized = true;

  int ndev = 0;
  r = net->devices(&ndev);
  if (r != ncclSuccess || ndev <= 0) {
    std::cerr << "devices failed or no device, rc=" << (int)r
              << " ndev=" << ndev << "\n";
    cleanup();
    return 1;
  }
  int defaultDev = (opt.dev >= 0) ? opt.dev : 0;
  int sendDev = (opt.sendDev >= 0) ? opt.sendDev : defaultDev;
  int recvDev = (opt.recvDev >= 0) ? opt.recvDev : defaultDev;
  if (sendDev < 0 || sendDev >= ndev) {
    std::cerr << "invalid send dev index " << sendDev << ", available [0.."
              << (ndev - 1) << "]\n";
    cleanup();
    return 1;
  }
  if (recvDev < 0 || recvDev >= ndev) {
    std::cerr << "invalid recv dev index " << recvDev << ", available [0.."
              << (ndev - 1) << "]\n";
    cleanup();
    return 1;
  }

  ncclNetProperties_t sendProps{};
  ncclNetProperties_t recvProps{};
  r = net->getProperties(sendDev, &sendProps);
  if (r != ncclSuccess) {
    std::cerr << "getProperties(sendDev) failed, rc=" << (int)r << "\n";
    cleanup();
    return 1;
  }
  r = net->getProperties(recvDev, &recvProps);
  if (r != ncclSuccess) {
    std::cerr << "getProperties(recvDev) failed, rc=" << (int)r << "\n";
    cleanup();
    return 1;
  }
  std::cout << std::fixed << std::setprecision(3)
            << "Using sendDev=" << sendDev
            << " name=" << (sendProps.name ? sendProps.name : "<null>")
            << " maxP2pMB=" << bytesToMB(sendProps.maxP2pBytes)
            << std::defaultfloat << "\n";
  std::cout << std::fixed << std::setprecision(3)
            << "Using recvDev=" << recvDev
            << " name=" << (recvProps.name ? recvProps.name : "<null>")
            << " maxP2pMB=" << bytesToMB(recvProps.maxP2pBytes)
            << std::defaultfloat << "\n";

  size_t maxBytes = 1 << 31;
  if (sendProps.maxP2pBytes > 0 && sendProps.maxP2pBytes < maxBytes) {
    maxBytes = sendProps.maxP2pBytes;
  }
  if (recvProps.maxP2pBytes > 0 && recvProps.maxP2pBytes < maxBytes) {
    maxBytes = recvProps.maxP2pBytes;
  }
  size_t taskBytes = std::min<size_t>((size_t)opt.taskBytes, maxBytes);
  int taskCount = opt.taskCount;
  if ((size_t)opt.taskBytes > maxBytes) {
    std::cout << std::fixed << std::setprecision(3)
              << "Requested taskMB=" << bytesToMB((size_t)opt.taskBytes)
              << " exceeds maxP2pMB limit, clamp to " << bytesToMB(taskBytes)
              << std::defaultfloat << "\n";
  }
  std::vector<TransferTask> tasks =
      buildUniformTasks(taskBytes, taskCount, /*tagBase=*/100, opt.verbose);
  std::cout << std::fixed << std::setprecision(3)
            << "Transfer config: taskCount=" << taskCount
            << " taskMB=" << bytesToMB(taskBytes)
            << " totalMB=" << bytesToMB(taskBytes * (size_t)taskCount)
            << std::defaultfloat << "\n";

  auto runScenario = [&](const std::string& name,
                         const std::vector<TransferTask>& scenarioTasks
                         ) -> bool {
    std::cout << "[TEST] " << name << "\n";
    char handle[NCCL_NET_HANDLE_MAXSIZE];
    std::memset(handle, 0, sizeof(handle));
    std::atomic<bool> listenReady(false);
    TwoThreadBarrier taskStartBarrier;
    StepStatus sendStatus;
    StepStatus recvStatus;

    std::thread sendThread([&]() {
      sendStatus = sendThreadMain(net, ctx, sendDev, handle, opt.timeoutMs,
                                  &listenReady, scenarioTasks,
                                  &taskStartBarrier);
    });
    std::thread recvThread([&]() {
      recvStatus = recvThreadMain(net, ctx, recvDev, handle, opt.timeoutMs,
                                  &listenReady, scenarioTasks,
                                  &taskStartBarrier);
    });

    sendThread.join();
    recvThread.join();

    size_t totalBytes = 0;
    for (const auto& t : scenarioTasks) totalBytes += t.size;
    size_t bytesPerTask = scenarioTasks.empty() ? 0 : scenarioTasks.front().size;
    double sendWorkerMs = sendStatus.transferMs;
    double recvWorkerMs = recvStatus.transferMs;
    double totalTransferMs = std::max(sendWorkerMs, recvWorkerMs);
    double seconds = totalTransferMs / 1000.0;
    double taskMB = bytesToMB(bytesPerTask);
    double totalMB = bytesToMB(totalBytes);
    double mbps = (seconds > 0.0) ? (totalMB / seconds) : 0.0;

    bool ok = sendStatus.ok && recvStatus.ok;
    if (!sendStatus.ok) std::cerr << "  send: " << sendStatus.err << "\n";
    if (!recvStatus.ok) std::cerr << "  recv: " << recvStatus.err << "\n";
    if (ok) {
      std::cout << "  [OK] " << name << "\n";
      std::cout << std::fixed << std::setprecision(3)
                << "  [PERF] task_count=" << scenarioTasks.size()
                << " task_mb=" << taskMB
                << " total_mb=" << totalMB
                << " send_worker_ms=" << sendWorkerMs
                << " recv_worker_ms=" << recvWorkerMs
                << " transfer_elapsed_ms=" << totalTransferMs
                << " throughput_MBps=" << mbps
                << std::defaultfloat << "\n";
    } else {
      std::cout << "  [FAIL] " << name << "\n";
    }
    return ok;
  };

  bool ok = runScenario(
      "performance transfer", tasks
  );

  cleanup();

  if (ok) {
    std::cout << "[PASS] net plugin .so integration test passed.\n";
    return 0;
  }
  std::cout << "[FAIL] net plugin .so integration test failed.\n";
  return 1;
}
