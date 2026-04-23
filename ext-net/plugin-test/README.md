# NCCL Net Plugin `.so` 测试说明

本目录提供外部 NCCL net 插件（`dlopen + dlsym`）的独立集成测试，用于验证已构建的 `.so`（例如 `ext-net/dpdk/build/libnccl-net-dpdk.so`）。

## 编译

```bash
make build
```

## 运行

必须提供参数 `--plugin <path>`。

会生成以下测试二进制：

- `./build/nccl-net-plugin-perf-test`：性能测试

对应源码：

- `net_plugin_so_perf_test.cc`

### 性能测试

基础运行示例：

```bash
sudo -E ./build/nccl-net-plugin-perf-test \
  --plugin ../dpdk/build/libnccl-net-dpdk.so \
  --dev 0 --vdevice-num 1 --verbose
```

收发设备分离运行示例：

```bash
./build/nccl-net-plugin-perf-test \
  --plugin ../dpdk/build/libnccl-net-dpdk.so \
  --send-dev 0 --recv-dev 1 --verbose
```

说明：

- 性能测试在单个场景内使用等大小 task。
- `--task-bytes` 与 `--task-count` 用于控制工作负载。
