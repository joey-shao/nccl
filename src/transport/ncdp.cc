/*************************************************************************
 * Copyright (c) 2016-2022, NVIDIA CORPORATION. All rights reserved.
 *
 * See LICENSE.txt for license information
 ************************************************************************/

#include "ncdp.h"

#include <netinet/in.h>
#include <rte_byteorder.h>
#include <rte_ethdev.h>
#include <rte_ether.h>
#include <rte_ip.h>
#include <rte_mbuf.h>
#include <rte_udp.h>
#include <string.h>

#define NCDP_MAGIC 0x4e434450U // "NCDP"
#define NCDP_RX_BURST 32

typedef struct __attribute__((packed)) ncdpWireHdr {
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
} ncdpWireHdr;

int ncdpComputeMaxPayload(int mtu) {
  int payload = mtu - (int)sizeof(struct rte_ipv4_hdr) -
                (int)sizeof(struct rte_udp_hdr) - (int)sizeof(ncdpWireHdr);
  int maxMbufPayload =
      (int)RTE_MBUF_DEFAULT_BUF_SIZE - (int)sizeof(struct rte_ether_hdr) -
      (int)sizeof(struct rte_ipv4_hdr) - (int)sizeof(struct rte_udp_hdr) -
      (int)sizeof(ncdpWireHdr);
  if (payload > maxMbufPayload)
    payload = maxMbufPayload;
  if (payload < 64)
    payload = 64;
  return payload;
}

bool ncdpTrySendFrame(const ncdpEndpoint *ep, uint16_t flags,
                      uint32_t dstCommId, uint32_t srcCommId, uint32_t reqId,
                      uint32_t taskId, uint32_t seq, const void *payload,
                      uint16_t len, uint16_t udpPort) {
  if (ep == NULL || ep->pool == NULL)
    return false;
  struct rte_mbuf *mbuf = rte_pktmbuf_alloc(ep->pool);
  if (!mbuf)
    return false;

  size_t l4Len = sizeof(struct rte_udp_hdr) + sizeof(ncdpWireHdr) + len;
  size_t l3Len = sizeof(struct rte_ipv4_hdr) + l4Len;
  size_t total = sizeof(struct rte_ether_hdr) + l3Len;
  char *data = (char *)rte_pktmbuf_append(mbuf, total);
  if (!data) {
    rte_pktmbuf_free(mbuf);
    return false;
  }

  struct rte_ether_hdr *eth = (struct rte_ether_hdr *)data;
  rte_ether_addr_copy(&ep->remoteMac, &eth->dst_addr);
  rte_ether_addr_copy(&ep->localMac, &eth->src_addr);
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
  ip->src_addr = ep->localIp;
  ip->dst_addr = ep->remoteIp;
  ip->hdr_checksum = rte_ipv4_cksum(ip);

  struct rte_udp_hdr *udp =
      (struct rte_udp_hdr *)((char *)ip + sizeof(struct rte_ipv4_hdr));
  uint16_t port = udpPort ? udpPort : ep->udpPort;
  udp->src_port = rte_cpu_to_be_16(port);
  udp->dst_port = rte_cpu_to_be_16(port);
  udp->dgram_len = rte_cpu_to_be_16((uint16_t)l4Len);
  udp->dgram_cksum = 0;

  ncdpWireHdr *hdr = (ncdpWireHdr *)((char *)udp + sizeof(struct rte_udp_hdr));
  hdr->magic = rte_cpu_to_be_32(NCDP_MAGIC);
  hdr->flags = rte_cpu_to_be_16(flags);
  hdr->reserved0 = 0;
  hdr->dstCommId = rte_cpu_to_be_32(dstCommId);
  hdr->srcCommId = rte_cpu_to_be_32(srcCommId);
  hdr->reqId = rte_cpu_to_be_32(reqId);
  hdr->taskId = rte_cpu_to_be_32(taskId);
  hdr->seq = rte_cpu_to_be_32(seq);
  hdr->len = rte_cpu_to_be_16(len);
  hdr->reserved2 = 0;

  if (len > 0 && payload) {
    memcpy((char *)hdr + sizeof(ncdpWireHdr), payload, len);
  }

  struct rte_mbuf *txPkts[1] = {mbuf};
  int sent = rte_eth_tx_burst(ep->portId, 0, txPkts, 1);
  if (sent < 1) {
    rte_pktmbuf_free(mbuf);
    return false;
  }
  return true;
}

static bool ncdpParsePacket(struct rte_mbuf *mbuf, uint16_t udpPort,
                            ncdpHdr *outHdr, const uint8_t **payload,
                            uint16_t *payloadLen) {
  int dataLen = rte_pktmbuf_data_len(mbuf);
  if (dataLen <
      (int)(sizeof(struct rte_ether_hdr) + sizeof(struct rte_ipv4_hdr) +
            sizeof(struct rte_udp_hdr) + sizeof(ncdpWireHdr)))
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
                      sizeof(struct rte_udp_hdr) + sizeof(ncdpWireHdr)))
    return false;
  if (ip->next_proto_id != IPPROTO_UDP)
    return false;

  struct rte_udp_hdr *udp = (struct rte_udp_hdr *)((char *)ip + ipHdrLen);
  if (udpPort != 0 && rte_be_to_cpu_16(udp->dst_port) != udpPort)
    return false;
  uint16_t udpLen = rte_be_to_cpu_16(udp->dgram_len);
  if (udpLen < sizeof(struct rte_udp_hdr) + sizeof(ncdpWireHdr))
    return false;

  ncdpWireHdr *hdr = (ncdpWireHdr *)((char *)udp + sizeof(struct rte_udp_hdr));
  if (rte_be_to_cpu_32(hdr->magic) != NCDP_MAGIC)
    return false;
  uint16_t len = rte_be_to_cpu_16(hdr->len);
  if ((int)len >
      (int)udpLen - (int)sizeof(struct rte_udp_hdr) - (int)sizeof(ncdpWireHdr))
    return false;

  outHdr->magic = rte_be_to_cpu_32(hdr->magic);
  outHdr->flags = rte_be_to_cpu_16(hdr->flags);
  outHdr->reserved0 = rte_be_to_cpu_16(hdr->reserved0);
  outHdr->dstCommId = rte_be_to_cpu_32(hdr->dstCommId);
  outHdr->srcCommId = rte_be_to_cpu_32(hdr->srcCommId);
  outHdr->reqId = rte_be_to_cpu_32(hdr->reqId);
  outHdr->taskId = rte_be_to_cpu_32(hdr->taskId);
  outHdr->seq = rte_be_to_cpu_32(hdr->seq);
  outHdr->len = len;
  outHdr->reserved2 = rte_be_to_cpu_16(hdr->reserved2);

  *payload = (const uint8_t *)hdr + sizeof(ncdpWireHdr);
  *payloadLen = len;
  return true;
}

int ncdpPollRx(int portId, uint16_t udpPort, ncdpRxCallback cb, void *ctx) {
  struct rte_mbuf *mbufs[NCDP_RX_BURST];
  int nb = rte_eth_rx_burst(portId, 0, mbufs, NCDP_RX_BURST);
  if (nb <= 0)
    return 0;

  for (int i = 0; i < nb; i++) {
    struct rte_mbuf *mbuf = mbufs[i];
    ncdpHdr hdr;
    const uint8_t *payload = NULL;
    uint16_t payloadLen = 0;
    if (ncdpParsePacket(mbuf, udpPort, &hdr, &payload, &payloadLen)) {
      if (cb)
        cb(ctx, &hdr, payload, payloadLen);
    }
    rte_pktmbuf_free(mbuf);
  }
  return nb;
}
