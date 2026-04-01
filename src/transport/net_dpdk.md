# NET/DPDK 设计与使用说明

本文档说明 `src/transport/net_dpdk.cc` 的当前实现设计与使用方法，目标是让开发者快速理解：

1. 该插件在 NCCL 网络栈中的定位。
2. 控制面/数据面的工作机制。
3. 关键参数、构建方式与联调方法。

## 1. 定位与目标

`net_dpdk` 是 NCCL 内置网络插件之一（插件名 `DpdkSocket`），采用“控制面走 Socket，数据面走 DPDK”模式：

- 控制面：使用 `ncclSocket`（TCP）进行 listen/connect/accept 和 SEND/READY 握手。
- 数据面：使用 DPDK 收发 `Ethernet + IPv4 + UDP + NCDP` 报文。
- 可靠性：由插件内部 frame/task 状态机 + ACK 机制实现。

代码入口：

- 插件对象：`ncclNetDpdkSocket`
- 源文件：`src/transport/net_dpdk.cc`
- 协议封装：`src/transport/ncdp.h`、`src/transport/ncdp.cc`

## 2. 总体架构

### 2.1 组件分层

- 设备初始化层
  - 控制面网卡选择：`dpdkInitControlInterface()`
  - DPDK EAL 启动：`dpdkInitEal()`
  - 数据面端口发现与配置：`dpdkInitDataDevices()` / `dpdkInitDataPort()`
- 连接管理层
  - listen/connect/accept 非阻塞状态机
  - 通过 `dpdkHelloMsg` 交换 `MAC/commId/dataIp`
- 请求管理层
  - `dpdkAllocRequest()` 分配请求槽
  - `dpdkBuildRequestTasks()` 将请求切分为 task/frame
- 数据面轮询层
  - 每个 net device 一个 `dpdkPollThread`
  - `dpdkPollRx()` 收包，`dpdkProgressSendRequest()` 推进发送，`dpdkProgressRecvRequest()` 刷 ACK

### 2.2 并发模型

- 全局锁
  - `ncclNetDpdkMutex`：初始化、设备/虚拟设备管理
  - `ncclNetDpdkCommMutex`：活跃 comm 表与收包分发
  - `ncclNetDpdkLcoreMutex`：DPDK lcore 分配
- 每设备线程
  - `dpdkPollThreads[dev]` 维护线程生命周期和附着 comm 列表
  - 线程主循环：`dpdkPollThreadMain()`

## 3. 初始化流程

入口函数：`ncclNetDpdkInit()`。

执行顺序：

1. `dpdkInitControlInterface()`
   - 用 `ncclFindInterfaces` 找到 Linux 控制面网卡。
2. `dpdkInitEal()`
   - 从环境变量 `NCCL_DPDK_EAL` 解析参数并调用 `rte_eal_init`。
3. `dpdkLoadDataIpConfig()`
   - 从 `dpdknet.conf`（或 `NCCL_DPDK_NET_CONF` 指定路径）读取数据面 IPv4 列表。
4. `dpdkInitDataDevices()`
   - 枚举物理 DPDK 端口并调用 `dpdkInitDataPort()` 完成配置。
5. `dpdkStartPollThread()`
   - 为每个已发现 device 启动一个常驻 poll thread。

`dpdkInitDataPort()` 关键动作：

- 创建 mbuf pool（`rte_pktmbuf_pool_create`）
- 配置 1 RX / 1 TX queue
- `rte_eth_dev_start`
- 打开混杂模式，获取 MAC 和 MTU

`dpdknet.conf` 文件格式：

- 每行可写 1 个或多个 IPv4。
- 分隔符支持空格、Tab、逗号。
- `#` 开头或行内 `#` 之后视为注释。
- 第 `i` 个解析出的 IP 会分配给第 `i` 个 net device（包含后续创建的 vdevice）。

示例：

```conf
# physical devices
192.168.10.11
192.168.10.12

# optional vdevices
192.168.10.101, 192.168.10.102
```

## 4. 连接与握手状态机

### 4.1 Connect 侧

`ncclNetDpdkConnect()` 状态机：

1. `ncclNetDpdkCommStateConnect`
   - 建立控制面 TCP 连接。
2. `ncclNetDpdkCommStateHelloSend`
   - 发送 `dpdkHelloMsg`（本端 MAC、commId、dataIp）。
3. 注册 comm 并获取 poll thread 引用（线程已在 init 时启动）。

### 4.2 Accept 侧

`ncclNetDpdkAccept()` 状态机：

1. `ncclNetDpdkCommStateAccept`
   - 接受控制面 TCP 连接。
2. `ncclNetDpdkCommStateHelloRecv`
   - 接收 `dpdkHelloMsg`，填充对端标识。
3. 注册 comm 并获取 poll thread 引用（线程已在 init 时启动）。

## 5. 请求与传输设计

### 5.1 控制面消息

控制消息结构：`dpdkCtrlMsg`。

- `DPDK_CTRL_SEND`
  - 发送侧声明 `srcReqId/size/frameSize`。
- `DPDK_CTRL_READY`
  - 接收侧返回 `srcReqId`，并用 `dstReqId` 绑定发送请求。

### 5.2 请求状态机

发送请求：

- `DPDK_REQ_SEND_CTRL` -> `DPDK_REQ_WAIT_READY` -> `DPDK_REQ_SENDING` -> `DPDK_REQ_UNUSED`

接收请求：

- `DPDK_REQ_RECV_CTRL` -> `DPDK_REQ_SEND_READY` -> `DPDK_REQ_RECEIVING` -> `DPDK_REQ_UNUSED`

进度由 `ncclNetDpdkTest()` 驱动控制面状态推进，数据面的真正收发在 poll thread 中进行。

### 5.3 task/frame 切分

`dpdkBuildRequestTasks()` 先按 `frameSize` 切分请求，再按如下规则确定 task：

- 先计算 `numFrames = ceil(size / frameSize)`。
- 由 `NCCL_DPDK_MAX_TASKS_PER_REQUEST` 给出 request 的 task 上限。
- 由 `NCCL_DPDK_MIN_TASK_FRAMES` 约束每个 task 的最小 frame 数。
- 先确定 `numTasks`，再把 `numFrames` 尽量均匀分配到各 task（前若干 task 多 1 帧）。
- `NCCL_DPDK_FRAME_WINDOW` 不参与 task 数计算，只用于每个 task 的发送窗口（`cwnd`）和收发 ring slot 大小。

- 发送侧 `dpdkSendTask`
  - `sndUna/sndNxt/inflight/cwnd`
  - `ackedBitmap` + ring slot 追踪 ACK 前缀
- 接收侧 `dpdkRecvTask`
  - `rcvNxt/ackPending/ackDeadlineTsc`
  - 支持乱序缓存与连续前缀提交

### 5.4 ACK 策略

- ACK 不是每包必回，可延迟聚合。
- 由两个参数控制：
  - `NCCL_DPDK_ACK_EVERY`
  - `NCCL_DPDK_ACK_DELAY_US`
- `dpdkProgressRecvRequest()` 周期性刷新 delayed ACK。

### 5.5 UDP 端口选择

`dpdkSelectUdpPort()` 基于 `dstCommId/srcCommId/srcReqId/dstReqId/taskId` 哈希，映射到 `[base, 65535]`，其中 `base = NCCL_DPDK_UDP_PORT`。

## 6. 虚拟设备（VDevice）

插件实现了 `makeVDevice`：`ncclNetDpdkMakeVDevice()`。

当前语义：

- 仅支持 synthetic vNIC（`props->ndevs == 0`）。
- 默认 PMD 前缀 `net_ring`，可通过环境变量调整：
  - `NCCL_DPDK_VDEV_PREFIX`
  - `NCCL_DPDK_VDEV_ARGS`
- 内部使用 `rte_vdev_init()` 创建 vdev，并把新端口追加到 NCCL net device 列表。
- vdev 的数据面 IPv4 也来自 `dpdknet.conf`，按设备索引顺序取下一个可用 IP。

说明：插件不会在 `init` 中自动创建 vdev，需要由上层调用 `makeVDevice`。

## 7. 关键环境变量

### 7.1 选择插件

- `NCCL_NET=DpdkSocket`

### 7.2 DPDK/EAL

- `NCCL_DPDK_EAL`
  - 直接传给 `rte_eal_init` 的参数字符串。
- `NCCL_DPDK_NET_CONF`
  - 数据面 IP 配置文件路径，默认 `dpdknet.conf`。

### 7.3 传输行为参数

- `NCCL_DPDK_UDP_PORT`（默认 4789）
- `NCCL_DPDK_FRAME_WINDOW`（默认 64）
  - 每个 task 的窗口参数（`cwnd`）和 ring slot 计算基准，不决定 task 数量。
- `NCCL_DPDK_MAX_TASKS_PER_REQUEST`（默认 8）
  - 单个 request 允许的最大 task 数。
- `NCCL_DPDK_MIN_TASK_FRAMES`（默认 1024）
  - 单个 task 的最小 frame 数；用于约束 task 数，避免 task 粒度过碎。
- `NCCL_DPDK_ACK_EVERY`（默认 8）
- `NCCL_DPDK_ACK_DELAY_US`（默认 10）
- `NCCL_DPDK_INLINE`（保留参数，当前实现未形成独立路径）

### 7.4 vdev 相关

- `NCCL_DPDK_VDEV_PREFIX`（默认 `net_ring`）
- `NCCL_DPDK_VDEV_ARGS`（可选）

## 8. 构建与测试

`src/Makefile` 内置了 net transport 集成测试目标 `ncclnettest`。

### 8.1 构建 socket 版本测试

```bash
make -C src nettest NETTEST_IMPL=socket
```

### 8.2 构建 dpdk 版本测试

```bash
make -C src nettest NETTEST_IMPL=dpdk
```

如果 `pkg-config` 不能自动找到 DPDK，可显式传：

```bash
make -C src nettest NETTEST_IMPL=dpdk \
  DPDK_CFLAGS="$(pkg-config --cflags libdpdk)" \
  DPDK_LIBS="$(pkg-config --static --libs libdpdk)"
```

可执行文件输出：`build/bin/ncclnettest`。

### 8.3 运行示例

```bash
NCCL_NET=DpdkSocket \
NCCL_DPDK_NET_CONF=/etc/nccl/dpdknet.conf \
NCCL_DPDK_EAL="-l 1-2 -n 4" \
./build/bin/ncclnettest --dev 0 --timeout-ms 15000 --verbose
```

如需测试 vdevice 创建：

```bash
./build/bin/ncclnettest --dev 0 --vdevice-num 2 --verbose
```

## 9. 当前限制

- 仅支持 IPv4 数据面。
- `dpdknet.conf` 中的 IP 数量必须覆盖要使用的 net device（物理网卡 + vdevice）。
- `regMr` 仅支持 `NCCL_PTR_HOST`。
- `iflush` 未实现（返回 `ncclInternalError`）。
- 每端口固定使用 queue 0（RX/TX 各 1 队列）。
- 生命周期上 `finalize` 会停止 poll threads，但仍未做完整 DPDK 全局反初始化（例如 EAL 级别资源回收）。

## 10. 排障建议

1. `rte_eal_init failed`
   - 检查 hugepage、EAL 参数、绑定驱动与权限。
2. `no DPDK ports available`
   - 确认 DPDK 端口已被正确绑定并可被 EAL 发现。
3. `no Linux socket interface available for control plane`
   - 控制面网卡未就绪，检查 IP 接口与网络命名空间。
4. `invalid ctrl SEND/READY`
   - 通常是双端配置不一致或请求状态错位，先对齐环境变量与消息大小。
5. `makeVDevice` 失败
   - 检查 PMD 名称/参数（`NCCL_DPDK_VDEV_PREFIX`、`NCCL_DPDK_VDEV_ARGS`）与 DPDK PMD 支持情况。

## 11. 代码索引

- 主实现：`src/transport/net_dpdk.cc`
- 协议实现：`src/transport/ncdp.h`、`src/transport/ncdp.cc`
- 插件装配：`src/plugin/net.cc`
- 测试程序：`src/transport/net_plugin_test/net_plugin_transport_test.cc`
- 构建规则：`src/Makefile`（目标 `nettest`）
