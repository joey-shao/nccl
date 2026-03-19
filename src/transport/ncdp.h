/*************************************************************************
 * Copyright (c) 2016-2022, NVIDIA CORPORATION. All rights reserved.
 *
 * See LICENSE.txt for license information
 ************************************************************************/

#ifndef NCCL_NCDP_H_
#define NCCL_NCDP_H_

#include <rte_ether.h>
#include <stdint.h>

struct rte_mempool;

enum ncdpFlags {
  NCDP_FLAG_DATA = 1,
  NCDP_FLAG_ACK = 2,
};

typedef struct __attribute__((packed)) ncdpHdr {
  uint32_t magic;
  uint16_t flags;
  uint16_t reserved0;
  uint32_t dstCommId;
  uint32_t srcCommId;
  uint32_t reqId;
  uint32_t taskId;
  uint32_t seq;
  uint16_t len;
  uint16_t reserved2;
} ncdpHdr;

struct ncdpEndpoint {
  int portId;
  struct rte_mempool *pool;
  struct rte_ether_addr localMac;
  struct rte_ether_addr remoteMac;
  uint32_t localIp;
  uint32_t remoteIp;
  uint16_t udpPort;
  int maxPayload;
};

typedef void (*ncdpRxCallback)(void *ctx, const ncdpHdr *hdr,
                               const uint8_t *payload, uint16_t len);

int ncdpComputeMaxPayload(int mtu);
bool ncdpTrySendFrame(const ncdpEndpoint *ep, uint16_t flags,
                      uint32_t dstCommId, uint32_t srcCommId, uint32_t reqId,
                      uint32_t taskId, uint32_t seq, const void *payload,
                      uint16_t len, uint16_t udpPort);
int ncdpPollRx(int portId, uint16_t udpPort, ncdpRxCallback cb, void *ctx);

#endif
