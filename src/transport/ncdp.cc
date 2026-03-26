/*************************************************************************
 * Copyright (c) 2016-2022, NVIDIA CORPORATION. All rights reserved.
 *
 * See LICENSE.txt for license information
 ************************************************************************/

#include "ncdp.h"

#include <netinet/in.h>
#include <rte_byteorder.h>
#include <rte_ether.h>
#include <rte_ip.h>
#include <rte_mbuf.h>
#include <rte_udp.h>
#include <string.h>

#define NCDP_MAGIC 0x4e434450U // "NCDP"

int ncdpComputeMaxPayload(int mtu) {
  int payload = mtu - (int)sizeof(struct rte_ipv4_hdr) -
                (int)sizeof(struct rte_udp_hdr) - (int)sizeof(ncdpHdr);
  int maxMbufPayload =
      (int)RTE_MBUF_DEFAULT_BUF_SIZE - (int)sizeof(struct rte_ether_hdr) -
      (int)sizeof(struct rte_ipv4_hdr) - (int)sizeof(struct rte_udp_hdr) -
      (int)sizeof(ncdpHdr);
  if (payload > maxMbufPayload)
    payload = maxMbufPayload;
  if (payload < 64)
    payload = 64;
  return payload;
}

bool ncdpBuildPacket(struct rte_mbuf *mbuf,
                     const struct rte_ether_addr *localMac,
                     const struct rte_ether_addr *remoteMac, uint32_t localIp,
                     uint32_t remoteIp, uint16_t flags,
                     uint32_t dstCommId, uint32_t srcCommId,
                     uint32_t srcReqId, uint32_t dstReqId, uint32_t taskId,
                     uint32_t seq, const void *payload, uint16_t len,
                     uint16_t udpPort) {
  if (mbuf == NULL || localMac == NULL || remoteMac == NULL)
    return false;
  rte_pktmbuf_reset(mbuf);

  size_t l4Len = sizeof(struct rte_udp_hdr) + sizeof(ncdpHdr) + len;
  size_t l3Len = sizeof(struct rte_ipv4_hdr) + l4Len;
  size_t total = sizeof(struct rte_ether_hdr) + l3Len;
  char *data = (char *)rte_pktmbuf_append(mbuf, total);
  if (!data) {
    return false;
  }

  struct rte_ether_hdr *eth = (struct rte_ether_hdr *)data;
  rte_ether_addr_copy(remoteMac, &eth->dst_addr);
  rte_ether_addr_copy(localMac, &eth->src_addr);
  eth->ether_type = rte_cpu_to_be_16(RTE_ETHER_TYPE_IPV4);

  struct rte_ipv4_hdr *ip =
      (struct rte_ipv4_hdr *)(data + sizeof(struct rte_ether_hdr));
  ip->version_ihl = (4 << 4) | (sizeof(struct rte_ipv4_hdr) / 4);
  ip->type_of_service = 0;
  ip->total_length = rte_cpu_to_be_16((uint16_t)l3Len);
  ip->packet_id = 0;
  ip->fragment_offset = rte_cpu_to_be_16(RTE_IPV4_HDR_DF_FLAG);
  ip->time_to_live = 64;
  ip->next_proto_id = IPPROTO_UDP;
  ip->hdr_checksum = 0;
  ip->src_addr = localIp;
  ip->dst_addr = remoteIp;
  ip->hdr_checksum = rte_ipv4_cksum(ip);

  struct rte_udp_hdr *udp =
      (struct rte_udp_hdr *)((char *)ip + sizeof(struct rte_ipv4_hdr));
  udp->src_port = rte_cpu_to_be_16(udpPort);
  udp->dst_port = rte_cpu_to_be_16(udpPort);
  udp->dgram_len = rte_cpu_to_be_16((uint16_t)l4Len);
  udp->dgram_cksum = 0;

  ncdpHdr *wireHdr = (ncdpHdr *)((char *)udp + sizeof(struct rte_udp_hdr));
  wireHdr->magic = rte_cpu_to_be_32(NCDP_MAGIC);
  wireHdr->flags = rte_cpu_to_be_16(flags);
  wireHdr->reserved0 = 0;
  wireHdr->dstCommId = rte_cpu_to_be_32(dstCommId);
  wireHdr->srcCommId = rte_cpu_to_be_32(srcCommId);
  wireHdr->srcReqId = rte_cpu_to_be_32(srcReqId);
  wireHdr->dstReqId = rte_cpu_to_be_32(dstReqId);
  wireHdr->taskId = rte_cpu_to_be_32(taskId);
  wireHdr->seq = rte_cpu_to_be_32(seq);
  wireHdr->len = rte_cpu_to_be_16(len);
  wireHdr->reserved2 = 0;

  if (len > 0 && payload) {
    memcpy((char *)wireHdr + sizeof(ncdpHdr), payload, len);
  }
  return true;
}

bool ncdpParsePacket(struct rte_mbuf *mbuf, ncdpHdr *hdr,
                     const uint8_t **payload, uint16_t *payloadLen) {
  if (mbuf == NULL || hdr == NULL || payload == NULL || payloadLen == NULL)
    return false;
  int dataLen = rte_pktmbuf_data_len(mbuf);
  if (dataLen <
      (int)(sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) +
            sizeof(struct rte_udp_hdr) + sizeof(ncdpHdr)))
    return false;

  char *data = rte_pktmbuf_mtod(mbuf, char *);
  struct rte_ether_hdr *eth = (struct rte_ether_hdr *)data;
  if (rte_be_to_cpu_16(eth->ether_type) != RTE_ETHER_TYPE_IPV4)
    return false;

  struct rte_ipv4_hdr *ip =
      (struct rte_ipv4_hdr *)(data + sizeof(struct rte_ether_hdr));
  if ((ip->version_ihl >> 4) != 4)
    return false;
  int ipHdrLen = (ip->version_ihl & 0x0f) * 4;
  if (ipHdrLen < (int)sizeof(struct rte_ipv4_hdr))
    return false;
  if (dataLen < (int)(sizeof(struct rte_ether_hdr) + ipHdrLen +
                      sizeof(struct rte_udp_hdr) + sizeof(ncdpHdr)))
    return false;
  if (ip->next_proto_id != IPPROTO_UDP)
    return false;

  struct rte_udp_hdr *udp = (struct rte_udp_hdr *)((char *)ip + ipHdrLen);
  uint16_t udpLen = rte_be_to_cpu_16(udp->dgram_len);
  if (udpLen < sizeof(struct rte_udp_hdr) + sizeof(ncdpHdr))
    return false;

  const ncdpHdr *wireHdr = (const ncdpHdr *)((char *)udp + sizeof(struct rte_udp_hdr));
  if (rte_be_to_cpu_32(wireHdr->magic) != NCDP_MAGIC)
    return false;
  uint16_t len = rte_be_to_cpu_16(wireHdr->len);
  if ((int)len >
      (int)udpLen - (int)sizeof(struct rte_udp_hdr) - (int)sizeof(ncdpHdr))
    return false;

  hdr->magic = rte_be_to_cpu_32(wireHdr->magic);
  hdr->flags = rte_be_to_cpu_16(wireHdr->flags);
  hdr->reserved0 = rte_be_to_cpu_16(wireHdr->reserved0);
  hdr->dstCommId = rte_be_to_cpu_32(wireHdr->dstCommId);
  hdr->srcCommId = rte_be_to_cpu_32(wireHdr->srcCommId);
  hdr->srcReqId = rte_be_to_cpu_32(wireHdr->srcReqId);
  hdr->dstReqId = rte_be_to_cpu_32(wireHdr->dstReqId);
  hdr->taskId = rte_be_to_cpu_32(wireHdr->taskId);
  hdr->seq = rte_be_to_cpu_32(wireHdr->seq);
  hdr->len = len;
  hdr->reserved2 = rte_be_to_cpu_16(wireHdr->reserved2);

  *payload = (const uint8_t *)wireHdr + sizeof(ncdpHdr);
  *payloadLen = len;
  return true;
}
