# NCCL Net Plugin 双机测试

- `sender.cc`：发送端程序，运行在 sender 节点
- `receiver.cc`：接收端程序，运行在 receiver 节点

两个程序都独立加载插件并初始化网络，`handle` 通过控制面 TCP 通道传输。

## 编译

```bash
make build
```

生成二进制：

- `build/nccl-net-plugin-sender`
- `build/nccl-net-plugin-receiver`

## net.conf 格式

系统需要提供 `net.conf`，用于指定控制通道 IP/端口（用于传输 `handle`）：

```ini
sender_ip=10.10.10.11
receiver_ip=10.10.10.12
port=50000
```

## 运行方式

先在 receiver 节点启动：

```bash
sudo -E ./build/nccl-net-plugin-receiver \
  --plugin ../dpdk/build/libnccl-net-dpdk.so \
  --net-conf ./net.conf \
  --dev 0 \
  --task-bytes 1048576 \
  --task-count 1 \
  --timeout-ms 1000000 \
  --verbose

./build/nccl-net-plugin-receiver \
  --plugin ../socket/build/libnccl-net-socket.so \
  --net-conf ./net.conf \
  --dev 0 \
  --task-bytes 1048576 \
  --task-count 1 \
  --verbose
```

再在 sender 节点启动：

```bash
sudo -E ./build/nccl-net-plugin-sender \
  --plugin ../dpdk/build/libnccl-net-dpdk.so \
  --net-conf ./net.conf \
  --dev 0 \
  --task-bytes 1048576 \
  --task-count 1 \
  --verbose

./build/nccl-net-plugin-sender \
  --plugin ../socket/build/libnccl-net-socket.so \
  --net-conf ./net.conf \
  --dev 0 \
  --task-bytes 1048576 \
  --task-count 1 \
  --verbose
```

## 批量扫大小测试

脚本从 `1024` bytes 到 `1073741824` bytes 按 2 倍递增测试，每档运行一次
sender/receiver 二进制，并把完整输出追加到日志文件。

DPDK 测试，先在 receiver 节点运行：

```bash
sudo -E ./run_receiver_sweep.sh \
  --plugin-type dpdk \
  --out ./results/receiver-dpdk.log
```

再在 sender 节点运行：

```bash
sudo -E ./run_sender_sweep.sh \
  --plugin-type dpdk \
  --out ./results/sender-dpdk.log
```

Socket 测试只需要把插件类型改成 `socket`：

```bash
./run_receiver_sweep.sh --plugin-type socket --out ./results/receiver-socket.log
./run_sender_sweep.sh --plugin-type socket --out ./results/sender-socket.log
```

常用参数：

- `--plugin-type dpdk|socket`：选择默认插件路径。
- `--plugin <path>`：覆盖插件 `.so` 路径。
- `--min-bytes <bytes>` / `--max-bytes <bytes>`：调整测试范围。
- `--factor <n>`：调整每档大小倍数，默认 `4`。
- `--task-count <count>`：每个 size 的 task 数。
- `--out <path>`：结果日志路径。
