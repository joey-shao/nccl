# DPDK Net + NCDP 设计说明

本文档描述 `src/transport/net_dpdk.cc` 与 `src/transport/ncdp.{h,cc}` 的当前实现设计。

## 1. 目标与范围

- 目标：在 NCCL `net` 插件中提供一条基于 DPDK 的用户态数据通路，绕过内核 TCP/IP 数据面。
- 控制面：仍使用 `ncclSocket`（TCP）做连接协商与请求协商。
- 数据面：使用 DPDK 直接收发以太网帧，承载 `NCDP` 报文（IPv4 + UDP + 自定义头）。
- 可靠性：由上层协议实现（ACK + 超时重传 + frame/task 状态机），不依赖 UDP 自身可靠性。

## 2. 模块关系

- `net_dpdk.cc`
  - 实现 NCCL net 插件接口：`listen/connect/accept/isend/irecv/test/close`。
  - 管理 comm/request 生命周期、控制面状态机、发送任务调度。
  - 在 DPDK lcore 上运行轮询线程（`rte_eal_remote_launch`）。
- `ncdp.h` / `ncdp.cc`
  - 提供极简数据面收发原语：`ncdpTrySendFrame` / `ncdpPollRx`。
  - 负责封装/解析 `Ether + IPv4 + UDP + NCDP header`。

## 3. 协议分层

### 3.1 控制面（TCP Socket）

控制消息结构：`dpdkCtrlMsg`

- `SEND`：发送方发起请求元信息（`reqId`, `size`, `frameSize`）。
- `READY`：接收方确认已准备接收该请求。

连接建立阶段还会通过 `dpdkHelloMsg` 交换：

- 对端 MAC 地址（数据面 L2 转发需要）。
- 对端 `commId`（数据面请求命中需要）。

### 3.2 数据面（NCDP over UDP over IPv4）

NCDP 头（见 `ncdpHdr`）：

- `magic`
- `flags`（`DATA` / `ACK`）
- `dstCommId`
- `srcCommId`
- `reqId`
- `taskId`
- `seq`
- `len`

要点：

- `reqId` 是 send/recv 双方协商后的共同请求标识。
- `taskId` 是请求内子任务标识（当前代码仍保留 `u32` 头字段）。
- `seq` 是 frame 序号，用于乱序定位、去重和 ACK 对应。

## 4. 关键状态机

### 4.1 请求状态（`ncclNetDpdkRequest.state`）

发送侧：

`DPDK_REQ_SEND_CTRL -> DPDK_REQ_WAIT_READY -> DPDK_REQ_SENDING -> DPDK_REQ_UNUSED`

接收侧：

`DPDK_REQ_RECV_CTRL -> DPDK_REQ_SEND_READY -> DPDK_REQ_RECEIVING -> DPDK_REQ_UNUSED`

`ncclNetDpdkTest` 负责驱动控制面握手与完成回收（完成后置 `state=UNUSED, used=0`）。

### 4.2 Frame/Task 状态

- frame：`EMPTY/READY/SENT/DONE`
- task：维护独立窗口与队列
  - `window`：每 task 允许的最大 in-flight frame 数
  - `inflight`：当前已发未 ACK
  - `queue`：可发送/重传 frame 的循环队列

## 5. 发送与接收流程

### 5.1 发送路径

1. `isend` 申请 request，写入 comm 的请求池。
2. `test` 发送 `SEND` 控制消息，等待 `READY`。
3. 进入 `SENDING` 后按 `frameSize` 切分，构建 task/frame 元数据。
4. 轮询线程遍历所有 `state == DPDK_REQ_SENDING` 的 request，推进发送。
5. 收到 ACK 后按 `reqId + taskId + seq` 更新 frame/task 完成度。
6. 所有 task 完成后置 `doneFlag`，`test` 返回完成并回收 request。

### 5.2 接收路径

1. `irecv` 申请 request 并进入 `RECV_CTRL`。
2. `test` 等待 `SEND` 控制消息，拿到 `reqId/size/frameSize`。
3. 发送 `READY`，进入 `RECEIVING`。
4. 轮询线程收到 `DATA`：
   - 按 `dstCommId` 找 comm；
   - 按 `reqId + state(RECEIVING)` 找 request；
   - 按 `seq` 定位目标偏移并拷贝 payload；
   - 立即回 ACK（携带原 `reqId/taskId/seq`）。
5. 全部 frame 完成后置 `doneFlag`，`test` 返回完成并回收 request。

## 6. 调度与并发模型

- 每个网卡设备一个 `dpdkPollThread`，绑定一个 DPDK lcore。
- 轮询线程主循环：
  - `ncdpPollRx` 收包；
  - 遍历该设备上所有 comm；
  - 遍历 comm 内所有 request，推进 `SENDING` 请求。
- 锁设计：
  - `thread->mutex`：保护 poll thread 的 comm 列表与线程状态。
  - `ncclNetDpdkCommMutex`：保护全局 comm 表与 RX 命中处理中的 comm 查找。

## 7. UDP 端口策略

数据面包的 UDP 端口不是固定单值，而是基于：

- `dstCommId`
- `srcCommId`
- `reqId`
- `taskId`

做哈希后映射到 `[DPDK_UDP_PORT, 65535]` 区间。

目的：

- 避免所有包都落在同一 5-tuple；
- 提升网络设备/LAG/ECMP 对流量的负载分担效果。

## 8. 参数与可调项

来自 `net_dpdk.cc` 的参数：

- `NCCL_DPDK_EAL`：DPDK EAL 启动参数（未设置时使用默认参数）。
- `DPDK_UDP_PORT`：UDP 端口哈希基值（默认 `4789`）。
- `DPDK_ACK_TIMEOUT_US`：ACK 超时重传阈值（默认 `200us`）。
- `DPDK_FRAME_WINDOW`：task 发送窗口（默认 `64`）。
- `DPDK_TASK_FRAMES`：每 task 包含 frame 数（默认 `0`，回退到窗口/默认 burst）。
- `DPDK_INLINE`：当前代码保留参数定义，尚未形成明显独立策略分支。

插件选择：

- 内置插件名：`DpdkSocket`
- 可通过 NCCL 的 net 插件选择机制指定使用该插件。

## 9. 当前限制

- 仅支持 IPv4 数据面（IPv6 会报错并退出）。
- `regMr` 仅接受 `NCCL_PTR_HOST`。
- `iflush` 未实现（返回 `ncclInternalError`）。
- 当前收发固定使用 DPDK queue `0`。
- 可靠性为轻量实现（ACK + 超时重发），未实现拥塞控制与复杂丢包恢复策略。

## 10. 代码索引

- 主插件实现：`src/transport/net_dpdk.cc`
- 数据面协议：`src/transport/ncdp.h`
- 数据面实现：`src/transport/ncdp.cc`
- 传输源编译入口：`src/transport/CMakeLists.txt`
- 内置插件注册：`src/plugin/net.cc`
