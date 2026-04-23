#include <unistd.h>

#include <arpa/inet.h>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <dlfcn.h>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include <sys/socket.h>

#include "net.h"

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

static bool recvAll(int fd, void* buf, size_t bytes) {
  uint8_t* p = static_cast<uint8_t*>(buf);
  size_t off = 0;
  while (off < bytes) {
    ssize_t n = recv(fd, p + off, bytes - off, 0);
    if (n <= 0) return false;
    off += (size_t)n;
  }
  return true;
}

static int connectControlAndFetchHandle(const NetConf& conf, int timeoutMs,
                                        void* handleBuf, size_t handleBytes,
                                        std::string* err) {
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
  while (std::chrono::steady_clock::now() < deadline) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      *err = "socket() failed";
      return -1;
    }

    struct sockaddr_in localAddr {};
    localAddr.sin_family = AF_INET;
    localAddr.sin_port = htons(0);
    if (inet_pton(AF_INET, conf.senderIp.c_str(), &localAddr.sin_addr) != 1) {
      close(fd);
      *err = "invalid sender_ip";
      return -1;
    }
    if (bind(fd, reinterpret_cast<struct sockaddr*>(&localAddr),
             sizeof(localAddr)) != 0) {
      close(fd);
      *err = "bind sender_ip failed";
      return -1;
    }

    struct sockaddr_in remoteAddr {};
    remoteAddr.sin_family = AF_INET;
    remoteAddr.sin_port = htons((uint16_t)conf.port);
    inet_pton(AF_INET, conf.receiverIp.c_str(), &remoteAddr.sin_addr);
    if (connect(fd, reinterpret_cast<struct sockaddr*>(&remoteAddr),
                sizeof(remoteAddr)) != 0) {
      close(fd);
      usleep(200 * 1000);
      continue;
    }

    HandlePacketHeader hdr {};
    if (!recvAll(fd, &hdr, sizeof(hdr))) {
      close(fd);
      *err = "recv handle header failed";
      return -1;
    }
    if (hdr.magic != kHandleMagic || hdr.payloadBytes != handleBytes) {
      close(fd);
      *err = "invalid handle header";
      return -1;
    }
    if (!recvAll(fd, handleBuf, handleBytes)) {
      close(fd);
      *err = "recv handle payload failed";
      return -1;
    }
    close(fd);
    return 0;
  }
  *err = "connect control channel timeout";
  return -1;
}

static bool connectPluginComm(ncclNet_t* net, void* ctx, int dev, void* handle,
                              int timeoutMs, void** sendComm, std::string* err) {
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
  *sendComm = nullptr;
  while (*sendComm == nullptr && std::chrono::steady_clock::now() < deadline) {
    ncclResult_t r = net->connect(ctx, dev, handle, sendComm, nullptr);
    if (r != ncclSuccess) {
      *err = "net->connect failed rc=" + std::to_string((int)r);
      return false;
    }
    if (*sendComm == nullptr) usleep(1000);
  }
  if (*sendComm == nullptr) {
    *err = "net->connect timeout";
    return false;
  }
  return true;
}

static void fillPattern(std::vector<uint8_t>& buf, int seed, size_t n) {
  for (size_t i = 0; i < n && i < buf.size(); i++)
    buf[i] = (uint8_t)((i * 131u + seed * 17u + 23u) & 0xff);
}

static bool waitSendDone(ncclNet_t* net, void* sendReq, size_t expectSize,
                         int timeoutMs, std::string* err,
                         double* waitElapsedMs) {
  auto waitStart = std::chrono::steady_clock::now();
  auto deadline =
      std::chrono::steady_clock::now() + std::chrono::milliseconds(timeoutMs);
  while (std::chrono::steady_clock::now() < deadline) {
    int done = 0;
    int sent = -1;
    ncclResult_t r = net->test(sendReq, &done, &sent);
    if (r != ncclSuccess) {
      *err = "net->test(send) failed rc=" + std::to_string((int)r);
      return false;
    }
    if (done) {
      if (sent >= 0 && (size_t)sent != expectSize) {
        *err = "send size mismatch expected=" + std::to_string(expectSize) +
               " sent=" + std::to_string(sent);
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
  *err = "wait send completion timeout";
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

  std::cout << "Role=sender plugin=" << opt.pluginPath << " net="
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
  ncclResult_t r = net->init(&ctx, /*commId=*/0x12345678ULL, &cfg, nullptr,
                             nullptr);
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
  if (connectControlAndFetchHandle(conf, opt.timeoutMs, handle, sizeof(handle),
                                   &err) != 0) {
    std::cerr << "[ERR] " << err << "\n";
    cleanup();
    return 1;
  }
  std::cout << "Received handle bytes=" << sizeof(handle) << "\n";

  void* sendComm = nullptr;
  if (!connectPluginComm(net, ctx, opt.dev, handle, opt.timeoutMs, &sendComm,
                         &err)) {
    std::cerr << "[ERR] " << err << "\n";
    cleanup();
    return 1;
  }
  std::cout << "Plugin connect established.\n";

  std::vector<uint8_t> sendBuf(taskBytes);
  double totalWaitSendMs = 0.0;
  for (size_t taskIndex = 0; taskIndex < tasks.size(); taskIndex++) {
    const auto& task = tasks[taskIndex];
    fillPattern(sendBuf, task.tag, task.size);
    void* req = nullptr;
    r = net->isend(sendComm, sendBuf.data(), task.size, task.tag, nullptr,
                   nullptr, &req);
    if (r != ncclSuccess) {
      std::cerr << "[ERR] net->isend failed rc=" << (int)r
                << " tag=" << task.tag << "\n";
      net->closeSend(sendComm);
      cleanup();
      return 1;
    }
    double taskWaitMs = 0.0;
    if (!waitSendDone(net, req, task.size, opt.timeoutMs, &err,
                      &taskWaitMs)) {
      std::cerr << "[ERR] " << err << " tag=" << task.tag << "\n";
      net->closeSend(sendComm);
      cleanup();
      return 1;
    }
    totalWaitSendMs += taskWaitMs;
    std::cout << std::fixed << std::setprecision(3)
              << "  [SEND_TASK] idx=" << taskIndex
              << " tag=" << task.tag
              << " sizeMB=" << bytesToMB(task.size)
              << " elapsed_ms=" << taskWaitMs
              << " throughput_MBps=" << throughputMBps(task.size, taskWaitMs)
              << std::defaultfloat << "\n";
  }
  net->closeSend(sendComm);

  double sec = totalWaitSendMs / 1000.0;
  double mbps = (sec > 0.0) ? (bytesToMB(totalBytes) / sec) : 0.0;
  std::cout << std::fixed << std::setprecision(3)
            << "[SEND_PERF] total_mb=" << bytesToMB(totalBytes)
            << " wait_send_ms_sum=" << totalWaitSendMs
            << " throughput_MBps=" << mbps
            << std::defaultfloat << "\n";

  cleanup();
  std::cout << "[PASS] sender finished.\n";
  return 0;
}
