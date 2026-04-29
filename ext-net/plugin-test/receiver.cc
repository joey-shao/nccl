#include <unistd.h>

#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <cerrno>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include <fcntl.h>
#include <sys/socket.h>

#include "net.h"
#include "test_log.h"

static const char* kPluginSymbol = "ncclNetPlugin_v11";
static const uint32_t kHandleMagic = 0x4e48544cU; // "NHTL"

struct Options {
  std::string pluginPath;
  std::string netConfPath = "net.conf";
  int dev = 0;
  int timeoutMs = 10000;
  int taskBytes = 1 << 25;
  int taskCount = 1;
  bool verbose = false;
};

struct NetConf {
  std::string senderIp;
  std::string receiverIp;
  int port = 50000;
};

struct TransferTask {
  size_t size;
  int tag;
  bool verbose;
};

struct HandlePacketHeader {
  uint32_t magic;
  uint32_t payloadBytes;
};

struct LoadedPlugin {
  void* so = nullptr;
  ncclNet_t* net = nullptr;
};

static std::string trim(const std::string& in) {
  size_t b = 0;
  while (b < in.size() && (in[b] == ' ' || in[b] == '\t' || in[b] == '\r' ||
                           in[b] == '\n')) {
    b++;
  }
  size_t e = in.size();
  while (e > b && (in[e - 1] == ' ' || in[e - 1] == '\t' || in[e - 1] == '\r' ||
                   in[e - 1] == '\n')) {
    e--;
  }
  return in.substr(b, e - b);
}

static bool parseInt(const char* s, int* out) {
  char* end = nullptr;
  long v = strtol(s, &end, 10);
  if (s == end || *end != '\0') return false;
  if (v < std::numeric_limits<int>::min() || v > std::numeric_limits<int>::max())
    return false;
  *out = (int)v;
  return true;
}

static bool isValidIpv4(const std::string& ip) {
  struct in_addr addr;
  return inet_pton(AF_INET, ip.c_str(), &addr) == 1;
}

static void usage(const char* prog) {
  std::cerr
      << "Usage: " << prog
      << " --plugin <path> [--net-conf <path>] [--dev <idx>] "
         "[--timeout-ms <ms>] [--task-bytes <bytes>] "
         "[--task-count <count>] [--verbose]\n"
      << "Example:\n"
      << "  " << prog
      << " --plugin ../dpdk/build/libnccl-net-dpdk.so "
         "--net-conf ./net.conf --dev 0 --task-bytes 1048576 --task-count 128\n";
}

static bool parseArgs(int argc, char** argv, Options* opt) {
  for (int i = 1; i < argc; i++) {
    std::string a = argv[i];
    if (a == "--plugin" && i + 1 < argc) {
      opt->pluginPath = argv[++i];
    } else if (a == "--net-conf" && i + 1 < argc) {
      opt->netConfPath = argv[++i];
    } else if (a == "--dev" && i + 1 < argc) {
      if (!parseInt(argv[++i], &opt->dev)) return false;
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
  return !opt->pluginPath.empty() && opt->timeoutMs > 0 && opt->taskBytes > 0 &&
         opt->taskCount > 0 && opt->dev >= 0;
}

static bool parseNetConf(const std::string& path, NetConf* conf, std::string* err) {
  std::ifstream in(path);
  if (!in.is_open()) {
    *err = "open net.conf failed: " + path;
    return false;
  }
  std::string line;
  int lineno = 0;
  while (std::getline(in, line)) {
    lineno++;
    size_t sharp = line.find('#');
    if (sharp != std::string::npos) line.resize(sharp);
    line = trim(line);
    if (line.empty()) continue;
    size_t eq = line.find('=');
    if (eq == std::string::npos) {
      *err = "invalid line " + std::to_string(lineno) + " in net.conf";
      return false;
    }
    std::string key = trim(line.substr(0, eq));
    std::string val = trim(line.substr(eq + 1));
    if (key == "sender_ip" || key == "sender" || key == "send_ip") {
      conf->senderIp = val;
    } else if (key == "receiver_ip" || key == "receiver" || key == "recv_ip") {
      conf->receiverIp = val;
    } else if (key == "port" || key == "control_port") {
      int port = 0;
      if (!parseInt(val.c_str(), &port) || port <= 0 || port > 65535) {
        *err = "invalid port in net.conf line " + std::to_string(lineno);
        return false;
      }
      conf->port = port;
    }
  }
  if (!isValidIpv4(conf->senderIp) || !isValidIpv4(conf->receiverIp)) {
    *err = "net.conf needs valid IPv4 sender_ip and receiver_ip";
    return false;
  }
  return true;
}

static bool loadPlugin(const std::string& path, LoadedPlugin* plugin,
                       std::string* err) {
  plugin->so = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
  if (plugin->so == nullptr) {
    *err = std::string("dlopen failed: ") +
           (dlerror() ? dlerror() : "unknown error");
    return false;
  }
  void* sym = dlsym(plugin->so, kPluginSymbol);
  if (sym == nullptr) {
    *err = std::string("dlsym(") + kPluginSymbol + ") failed: " +
           (dlerror() ? dlerror() : "unknown error");
    dlclose(plugin->so);
    plugin->so = nullptr;
    return false;
  }
  plugin->net = reinterpret_cast<ncclNet_t*>(sym);
  return true;
}

static void unloadPlugin(LoadedPlugin* plugin) {
  plugin->net = nullptr;
  if (plugin->so) {
    dlclose(plugin->so);
    plugin->so = nullptr;
  }
}

static bool sendAll(int fd, const void* buf, size_t bytes) {
  const uint8_t* p = static_cast<const uint8_t*>(buf);
  size_t off = 0;
  while (off < bytes) {
    ssize_t n = send(fd, p + off, bytes - off, 0);
    if (n <= 0) return false;
    off += (size_t)n;
  }
  return true;
}

static bool waitForSenderAndSendHandle(const NetConf& conf, int timeoutMs,
                                       const void* handleBuf, size_t handleBytes,
                                       std::string* err) {
  int listenFd = socket(AF_INET, SOCK_STREAM, 0);
  if (listenFd < 0) {
    *err = "socket() failed";
    return false;
  }
  int opt = 1;
  setsockopt(listenFd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  struct sockaddr_in bindAddr {};
  bindAddr.sin_family = AF_INET;
  bindAddr.sin_port = htons((uint16_t)conf.port);
  if (inet_pton(AF_INET, conf.receiverIp.c_str(), &bindAddr.sin_addr) != 1) {
    close(listenFd);
    *err = "invalid receiver_ip";
    return false;
  }
  if (bind(listenFd, reinterpret_cast<struct sockaddr*>(&bindAddr),
           sizeof(bindAddr)) != 0) {
    close(listenFd);
    *err = "bind receiver_ip failed";
    return false;
  }
  if (listen(listenFd, 1) != 0) {
    close(listenFd);
    *err = "listen() failed";
    return false;
  }
  int flags = fcntl(listenFd, F_GETFL, 0);
  if (flags >= 0) fcntl(listenFd, F_SETFL, flags | O_NONBLOCK);

  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
  int connFd = -1;
  while (std::chrono::steady_clock::now() < deadline && connFd < 0) {
    connFd = accept(listenFd, nullptr, nullptr);
    if (connFd < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        usleep(100 * 1000);
      } else {
        close(listenFd);
        *err = "accept() failed";
        return false;
      }
    }
  }
  close(listenFd);
  if (connFd < 0) {
    *err = "accept sender control connection timeout";
    return false;
  }

  HandlePacketHeader hdr {};
  hdr.magic = kHandleMagic;
  hdr.payloadBytes = (uint32_t)handleBytes;
  bool ok = sendAll(connFd, &hdr, sizeof(hdr)) &&
            sendAll(connFd, handleBuf, handleBytes);
  close(connFd);
  if (!ok) {
    *err = "send handle failed";
    return false;
  }
  return true;
}

static bool acceptPluginComm(ncclNet_t* net, void* listenComm, int timeoutMs,
                             void** recvComm, std::string* err) {
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
  *recvComm = nullptr;
  while (*recvComm == nullptr && std::chrono::steady_clock::now() < deadline) {
    ncclResult_t r = net->accept(listenComm, recvComm, nullptr);
    if (r != ncclSuccess) {
      *err = "net->accept failed rc=" + std::to_string((int)r);
      return false;
    }
    if (*recvComm == nullptr) usleep(1000);
  }
  if (*recvComm == nullptr) {
    *err = "net->accept timeout";
    return false;
  }
  return true;
}

static void fillPattern(std::vector<uint8_t>& buf, int seed, size_t n) {
  for (size_t i = 0; i < n && i < buf.size(); i++)
    buf[i] = (uint8_t)((i * 131u + seed * 17u + 23u) & 0xff);
}

static bool equalPrefix(const std::vector<uint8_t>& a,
                        const std::vector<uint8_t>& b, size_t n) {
  if (a.size() < n || b.size() < n) return false;
  return std::memcmp(a.data(), b.data(), n) == 0;
}

static bool waitRecvDone(ncclNet_t* net, void* recvReq, size_t expectSize,
                         int timeoutMs, std::string* err,
                         double* waitElapsedMs) {
  auto waitStart = std::chrono::steady_clock::now();
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
  while (std::chrono::steady_clock::now() < deadline) {
    int done = 0;
    int got = -1;
    ncclResult_t r = net->test(recvReq, &done, &got);
    if (r != ncclSuccess) {
      *err = "net->test(recv) failed rc=" + std::to_string((int)r);
      return false;
    }
    if (done) {
      if (got >= 0 && (size_t)got != expectSize) {
        *err = "recv size mismatch expected=" + std::to_string(expectSize) +
               " got=" + std::to_string(got);
        return false;
      }
      if (waitElapsedMs) {
        *waitElapsedMs = std::chrono::duration<double, std::milli>(
                             std::chrono::steady_clock::now() - waitStart)
                             .count();
      }
      return true;
    }
    usleep(200);
  }
  if (waitElapsedMs) {
    *waitElapsedMs = std::chrono::duration<double, std::milli>(
                         std::chrono::steady_clock::now() - waitStart)
                         .count();
  }
  *err = "wait recv completion timeout";
  return false;
}

static std::vector<TransferTask> buildTasks(size_t taskBytes, int taskCount,
                                            bool verbose) {
  std::vector<TransferTask> tasks;
  tasks.reserve((size_t)taskCount);
  for (int i = 0; i < taskCount; i++) {
    tasks.push_back({taskBytes, 100 + i, verbose && i < 8});
  }
  return tasks;
}

static double bytesToMB(size_t bytes) {
  return (double)bytes / (1024.0 * 1024.0);
}

static double throughputMBps(size_t bytes, double elapsedMs) {
  double sec = elapsedMs / 1000.0;
  return (sec > 0.0) ? (bytesToMB(bytes) / sec) : 0.0;
}

int main(int argc, char** argv) {
  Options opt;
  if (!parseArgs(argc, argv, &opt)) {
    usage(argv[0]);
    return 2;
  }

  NetConf conf;
  std::string err;
  if (!parseNetConf(opt.netConfPath, &conf, &err)) {
    std::cerr << "[ERR] " << err << "\n";
    return 1;
  }

  LoadedPlugin plugin;
  if (!loadPlugin(opt.pluginPath, &plugin, &err)) {
    std::cerr << "[ERR] " << err << "\n";
    return 1;
  }
  ncclNet_t* net = plugin.net;
  if (net == nullptr) {
    std::cerr << "[ERR] plugin symbol is null: " << kPluginSymbol << "\n";
    unloadPlugin(&plugin);
    return 1;
  }

  std::cout << "Role=receiver plugin=" << opt.pluginPath << " net="
            << (net->name ? net->name : "<null>") << "\n";
  std::cout << "Control channel: sender_ip=" << conf.senderIp
            << " receiver_ip=" << conf.receiverIp << " port=" << conf.port
            << "\n";

  void* ctx = nullptr;
  bool initialized = false;
  auto cleanup = [&]() {
    if (initialized && net->finalize) net->finalize(ctx);
    unloadPlugin(&plugin);
  };

  ncclNetCommConfig_t cfg {};
  cfg.trafficClass = NCCL_NET_TRAFFIC_CLASS_UNDEF;
  ncclResult_t r = net->init(&ctx, /*commId=*/0x12345678ULL, &cfg,
                             pluginTestLogFunction, nullptr);
  if (r != ncclSuccess) {
    std::cerr << "[ERR] net->init failed rc=" << (int)r << "\n";
    cleanup();
    return 1;
  }
  initialized = true;

  int ndev = 0;
  r = net->devices(&ndev);
  if (r != ncclSuccess || ndev <= 0 || opt.dev >= ndev) {
    std::cerr << "[ERR] invalid device, rc=" << (int)r << " ndev=" << ndev
              << " dev=" << opt.dev << "\n";
    cleanup();
    return 1;
  }

  ncclNetProperties_t props {};
  r = net->getProperties(opt.dev, &props);
  if (r != ncclSuccess) {
    std::cerr << "[ERR] getProperties failed rc=" << (int)r << "\n";
    cleanup();
    return 1;
  }

  size_t taskBytes = (size_t)opt.taskBytes;
  if (props.maxP2pBytes > 0 && taskBytes > props.maxP2pBytes)
    taskBytes = props.maxP2pBytes;

  std::vector<TransferTask> tasks = buildTasks(taskBytes, opt.taskCount, opt.verbose);
  size_t totalBytes = taskBytes * (size_t)opt.taskCount;
  std::cout << std::fixed << std::setprecision(3)
            << "Using dev=" << opt.dev
            << " name=" << (props.name ? props.name : "<null>")
            << " task_count=" << opt.taskCount
            << " task_mb=" << bytesToMB(taskBytes)
            << " total_mb=" << bytesToMB(totalBytes) << std::defaultfloat
            << "\n";

  char handle[NCCL_NET_HANDLE_MAXSIZE];
  memset(handle, 0, sizeof(handle));
  void* listenComm = nullptr;
  r = net->listen(ctx, opt.dev, handle, &listenComm);
  if (r != ncclSuccess || listenComm == nullptr) {
    std::cerr << "[ERR] net->listen failed rc=" << (int)r << "\n";
    cleanup();
    return 1;
  }
  std::cout << "Plugin listen ready, waiting sender to fetch handle...\n";

  if (!waitForSenderAndSendHandle(conf, opt.timeoutMs, handle, sizeof(handle), &err)) {
    std::cerr << "[ERR] " << err << "\n";
    net->closeListen(listenComm);
    cleanup();
    return 1;
  }
  std::cout << "Handle sent to sender.\n";

  void* recvComm = nullptr;
  if (!acceptPluginComm(net, listenComm, opt.timeoutMs, &recvComm, &err)) {
    std::cerr << "[ERR] " << err << "\n";
    net->closeListen(listenComm);
    cleanup();
    return 1;
  }
  std::cout << "Plugin accept established.\n";

  std::vector<uint8_t> recvBuf(taskBytes);
  std::vector<uint8_t> expectBuf(taskBytes);
  double totalWaitRecvMs = 0.0;
  for (size_t taskIndex = 0; taskIndex < tasks.size(); taskIndex++) {
    const auto& task = tasks[taskIndex];
    memset(recvBuf.data(), 0, task.size);
    void* req = nullptr;
    void* data = recvBuf.data();
    size_t size = task.size;
    int tag = task.tag;
    void* mr = nullptr;
    r = net->irecv(recvComm, 1, &data, &size, &tag, &mr, nullptr, &req);
    if (r != ncclSuccess) {
      std::cerr << "[ERR] net->irecv failed rc=" << (int)r
                << " tag=" << task.tag << "\n";
      net->closeRecv(recvComm);
      net->closeListen(listenComm);
      cleanup();
      return 1;
    }
    double taskWaitMs = 0.0;
    if (!waitRecvDone(net, req, task.size, opt.timeoutMs, &err,
                      &taskWaitMs)) {
      std::cerr << "[ERR] " << err << " tag=" << task.tag << "\n";
      net->closeRecv(recvComm);
      net->closeListen(listenComm);
      cleanup();
      return 1;
    }
    totalWaitRecvMs += taskWaitMs;

    fillPattern(expectBuf, task.tag, task.size);
    if (!equalPrefix(expectBuf, recvBuf, task.size)) {
      std::cerr << "[ERR] payload mismatch tag=" << task.tag << "\n";
      net->closeRecv(recvComm);
      net->closeListen(listenComm);
      cleanup();
      return 1;
    }
    std::cout << std::fixed << std::setprecision(3)
              << "  [RECV_TASK] idx=" << taskIndex
              << " tag=" << task.tag
              << " sizeMB=" << bytesToMB(task.size)
              << " elapsed_ms=" << taskWaitMs
              << " throughput_MBps=" << throughputMBps(task.size, taskWaitMs)
              << std::defaultfloat << "\n";
  }

  net->closeRecv(recvComm);
  net->closeListen(listenComm);

  double sec = totalWaitRecvMs / 1000.0;
  double mbps = (sec > 0.0) ? (bytesToMB(totalBytes) / sec) : 0.0;
  std::cout << std::fixed << std::setprecision(3)
            << "[RECV_PERF] total_mb=" << bytesToMB(totalBytes)
            << " wait_recv_ms_sum=" << totalWaitRecvMs
            << " throughput_MBps=" << mbps
            << std::defaultfloat << "\n";

  cleanup();
  std::cout << "[PASS] receiver finished.\n";
  return 0;
}
