/*************************************************************************
 * Copyright (c) 2016-2022, NVIDIA CORPORATION. All rights reserved.
 *
 * See LICENSE.txt for license information
 ************************************************************************/

#ifndef NCCL_NET_DPDK_UTILS_H_
#define NCCL_NET_DPDK_UTILS_H_

#include "net.h"

#include <stddef.h>
#include <stdint.h>
#include <vector>

ncclResult_t dpdkLoadDataIpConfig(std::vector<uint32_t> *dataIps,
                                  uint32_t *controlIp, int maxDataIps);
const char *dpdkIpv4ToString(uint32_t ip, char *buf, size_t len);

struct dpdkLbPairLog {
  unsigned sendDev;
  unsigned recvDev;
  int taskCount;
};

struct dpdkLbDevTaskLog {
  int dev;
  int taskCount;
};

struct dpdkLbAssignmentLog {
  const char *role;
  uint32_t reqId;
  uint32_t peerReqId;
  int numTasks;
  unsigned pairCount;
  const struct dpdkLbPairLog *pairs;
  int devTaskCount;
  const struct dpdkLbDevTaskLog *devTasks;
};

void dpdkLogLbAssignment(const struct dpdkLbAssignmentLog *assignment);

#endif // NCCL_NET_DPDK_UTILS_H_
