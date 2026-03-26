/*************************************************************************
 * Copyright (c) 2016-2022, NVIDIA CORPORATION. All rights reserved.
 *
 * See LICENSE.txt for license information
 ************************************************************************/

#ifndef NCCL_NCDP_H_
#define NCCL_NCDP_H_

#include <rte_ether.h>
#include <stdint.h>

struct rte_mbuf;

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
  uint32_t srcReqId;
  uint32_t dstReqId;
  // Task id within one request.
  uint32_t taskId;
  // Global frame sequence inside one request.
  uint32_t seq;
  // Payload bytes following ncdpHdr.
  uint16_t len;
  uint16_t reserved2;
} ncdpHdr;

int ncdpComputeMaxPayload(int mtu);
bool ncdpBuildPacket(struct rte_mbuf *mbuf,
                     const struct rte_ether_addr *localMac,
                     const struct rte_ether_addr *remoteMac, uint32_t localIp,
                     uint32_t remoteIp, uint16_t flags,
                     uint32_t dstCommId, uint32_t srcCommId,
                     uint32_t srcReqId, uint32_t dstReqId, uint32_t taskId,
                     uint32_t seq, const void *payload,
                     uint16_t len, uint16_t udpPort);
bool ncdpParsePacket(struct rte_mbuf *mbuf, ncdpHdr *outHdr,
                     const uint8_t **payload, uint16_t *payloadLen);

#endif
