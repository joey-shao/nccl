# NCCL Net Plugin `.so` Test

This directory contains a standalone integration test for external NCCL net
plugins (`dlopen + dlsym`), intended to validate a built `.so` (for example
`ext-net/dpdk/build/libnccl-net-dpdk.so`).

## Build

```bash
make build
```

## Run

`--plugin <path>` is required.

Examples:

```bash
./build/nccl-net-plugin-test \
  --plugin ../dpdk/build/libnccl-net-dpdk.so \
  --dev 0 --vdevice-num 1 --timeout-ms 15000 --verbose
```
