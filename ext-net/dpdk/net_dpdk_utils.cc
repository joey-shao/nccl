/*************************************************************************
 * Copyright (c) 2016-2022, NVIDIA CORPORATION. All rights reserved.
 *
 * See LICENSE.txt for license information
 ************************************************************************/

#include "net_dpdk_utils.h"

#include "plugin_compat.h"

#include <arpa/inet.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <string.h>

static bool dpdkConfigIsSpace(char c) {
  return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

static char *dpdkConfigTrim(char *text) {
  while (*text && dpdkConfigIsSpace(*text))
    text++;
  char *end = text + strlen(text);
  while (end > text && dpdkConfigIsSpace(end[-1]))
    *--end = '\0';
  return text;
}

static void dpdkConfigNormalizeIpSeparators(char *text) {
  for (char *p = text; *p; ++p) {
    if (*p == ',' || *p == '\t' || *p == '\r' || *p == '\n')
      *p = ' ';
  }
}

static bool dpdkConfigIsControlKey(const char *key) {
  return strcmp(key, "control_ip") == 0;
}

static bool dpdkConfigIsDataKey(const char *key) {
  return strcmp(key, "data_ip") == 0;
}

const char *dpdkIpv4ToString(uint32_t ip, char *buf, size_t len) {
  struct in_addr addr;
  addr.s_addr = ip;
  if (inet_ntop(AF_INET, &addr, buf, len) == NULL)
    snprintf(buf, len, "<invalid>");
  return buf;
}

static ncclResult_t dpdkParseIpv4Config(const char *token, uint32_t *ip,
                                        const char *field,
                                        const char *confPath, int lineNo) {
  struct in_addr addr;
  if (inet_pton(AF_INET, token, &addr) != 1) {
    WARN("NET/DPDK : invalid %s IPv4 '%s' at %s:%d", field, token, confPath,
         lineNo);
    return ncclInvalidUsage;
  }
  *ip = addr.s_addr;
  return ncclSuccess;
}

static ncclResult_t dpdkParseControlIpConfig(char *value,
                                             const char *confPath, int lineNo,
                                             uint32_t *controlIp) {
  dpdkConfigNormalizeIpSeparators(value);
  char *save = NULL;
  char *token = strtok_r(value, " ", &save);
  if (token == NULL) {
    WARN("NET/DPDK : empty control_ip at %s:%d", confPath, lineNo);
    return ncclInvalidUsage;
  }

  uint32_t ip = 0;
  NCCLCHECK(dpdkParseIpv4Config(token, &ip, "control", confPath, lineNo));
  if (strtok_r(NULL, " ", &save) != NULL) {
    WARN("NET/DPDK : control_ip expects exactly one IPv4 at %s:%d", confPath,
         lineNo);
    return ncclInvalidUsage;
  }
  *controlIp = ip;
  return ncclSuccess;
}

static ncclResult_t dpdkParseDataIpConfig(char *value, const char *confPath,
                                          int lineNo, int *dataFull,
                                          std::vector<uint32_t> *dataIps,
                                          int maxDataIps) {
  dpdkConfigNormalizeIpSeparators(value);
  char *save = NULL;
  for (char *token = strtok_r(value, " ", &save); token != NULL;
       token = strtok_r(NULL, " ", &save)) {
    uint32_t ip = 0;
    NCCLCHECK(dpdkParseIpv4Config(token, &ip, "data", confPath, lineNo));
    if ((int)dataIps->size() >= maxDataIps) {
      if (!*dataFull) {
        WARN("NET/DPDK : too many data IPs in %s (max %d), ignoring remaining entries",
             confPath, maxDataIps);
        *dataFull = 1;
      }
      return ncclSuccess;
    }
    dataIps->push_back(ip);
  }
  return ncclSuccess;
}

enum dpdkIpConfigSection {
  DPDK_IP_CONFIG_NONE = 0,
  DPDK_IP_CONFIG_CONTROL,
  DPDK_IP_CONFIG_DATA,
};

ncclResult_t dpdkLoadDataIpConfig(std::vector<uint32_t> *dataIps,
                                  uint32_t *controlIp, int maxDataIps) {
  if (dataIps == NULL || controlIp == NULL || maxDataIps <= 0)
    return ncclInvalidArgument;

  const char *confPath = ncclGetEnv("NCCL_DPDK_NET_CONF");
  if (confPath == NULL || confPath[0] == '\0')
    confPath = "dpdknet.conf";
  dataIps->clear();
  *controlIp = 0;

  FILE *fp = fopen(confPath, "r");
  if (fp == NULL) {
    WARN("NET/DPDK : failed to open data IP config %s : %s", confPath,
         strerror(errno));
    return ncclSystemError;
  }

  char line[1024];
  int lineNo = 0;
  int dataFull = 0;
  int sawControlSection = 0;
  int sawDataSection = 0;
  int controlValueSeen = 0;
  enum dpdkIpConfigSection section = DPDK_IP_CONFIG_NONE;
  ncclResult_t ret = ncclSuccess;
  while (fgets(line, sizeof(line), fp) != NULL) {
    lineNo++;
    char *comment = strchr(line, '#');
    if (comment)
      *comment = '\0';

    char *text = dpdkConfigTrim(line);
    if (*text == '\0')
      continue;

    if (dpdkConfigIsControlKey(text)) {
      if (sawControlSection) {
        WARN("NET/DPDK : duplicate control_ip section at %s:%d", confPath,
             lineNo);
        ret = ncclInvalidUsage;
        goto fail;
      }
      sawControlSection = 1;
      section = DPDK_IP_CONFIG_CONTROL;
      continue;
    }

    if (dpdkConfigIsDataKey(text)) {
      if (!sawControlSection) {
        WARN("NET/DPDK : data_ip section must appear after control_ip in %s:%d",
             confPath, lineNo);
        ret = ncclInvalidUsage;
        goto fail;
      }
      if (sawDataSection) {
        WARN("NET/DPDK : duplicate data_ip section at %s:%d", confPath,
             lineNo);
        ret = ncclInvalidUsage;
        goto fail;
      }
      sawDataSection = 1;
      section = DPDK_IP_CONFIG_DATA;
      continue;
    }

    if (section == DPDK_IP_CONFIG_NONE) {
      WARN("NET/DPDK : expected control_ip section before '%s' at %s:%d",
           text, confPath, lineNo);
      ret = ncclInvalidUsage;
      goto fail;
    }

    if (section == DPDK_IP_CONFIG_CONTROL) {
      if (controlValueSeen) {
        WARN("NET/DPDK : control_ip expects at most one IPv4 value at %s:%d",
             confPath, lineNo);
        ret = ncclInvalidUsage;
        goto fail;
      }
      NCCLCHECKGOTO(
          dpdkParseControlIpConfig(text, confPath, lineNo, controlIp), ret,
          fail);
      controlValueSeen = 1;
      continue;
    }

    NCCLCHECKGOTO(
        dpdkParseDataIpConfig(text, confPath, lineNo, &dataFull, dataIps,
                              maxDataIps),
        ret, fail);
  }
  fclose(fp);

  if (!sawControlSection) {
    WARN("NET/DPDK : missing required control_ip section in %s", confPath);
    dataIps->clear();
    *controlIp = 0;
    return ncclInvalidUsage;
  }
  if (!sawDataSection) {
    WARN("NET/DPDK : missing required data_ip section in %s", confPath);
    dataIps->clear();
    *controlIp = 0;
    return ncclInvalidUsage;
  }
  if (dataIps->empty()) {
    WARN("NET/DPDK : no data-plane IPv4 found in %s", confPath);
    *controlIp = 0;
    return ncclInvalidUsage;
  }
  if (*controlIp != 0) {
    char ipLine[INET_ADDRSTRLEN];
    INFO(NCCL_INIT | NCCL_NET,
         "NET/DPDK : loaded %d data-plane IPs and control_ip=%s from %s",
         (int)dataIps->size(),
         dpdkIpv4ToString(*controlIp, ipLine, sizeof(ipLine)), confPath);
  } else {
    INFO(NCCL_INIT | NCCL_NET,
         "NET/DPDK : loaded %d data-plane IPs from %s", (int)dataIps->size(),
         confPath);
  }
  return ncclSuccess;

fail:
  fclose(fp);
  dataIps->clear();
  *controlIp = 0;
  return ret;
}

static void dpdkAppendf(char *buf, size_t len, size_t *off,
                        const char *fmt, ...) {
  if (buf == NULL || len == 0 || off == NULL || *off >= len)
    return;
  va_list args;
  va_start(args, fmt);
  int n = vsnprintf(buf + *off, len - *off, fmt, args);
  va_end(args);
  if (n <= 0)
    return;
  if ((size_t)n >= len - *off)
    *off = len - 1;
  else
    *off += (size_t)n;
}

void dpdkLogLbAssignment(const struct dpdkLbAssignmentLog *assignment) {
  if (assignment == NULL || assignment->role == NULL)
    return;
  unsigned pairCount = assignment->pairs == NULL ? 0 : assignment->pairCount;
  int devTaskCount =
      assignment->devTasks == NULL ? 0 : assignment->devTaskCount;

  char line[2048];
  size_t off = 0;
  line[0] = '\0';
  dpdkAppendf(line, sizeof(line), &off,
              "NET/DPDK/LB role=%s req=%u peerReq=%u tasks=%d pairCount=%u",
              assignment->role, assignment->reqId, assignment->peerReqId,
              assignment->numTasks, pairCount);

  dpdkAppendf(line, sizeof(line), &off, " pairs=");
  for (unsigned i = 0; i < pairCount; i++) {
    const struct dpdkLbPairLog *pair = assignment->pairs + i;
    dpdkAppendf(line, sizeof(line), &off,
                "%spair%u:sendDev%u->recvDev%u tasks=%d",
                i == 0 ? "" : ",", i, pair->sendDev, pair->recvDev,
                pair->taskCount);
  }
  if (pairCount == 0)
    dpdkAppendf(line, sizeof(line), &off, "<none>");

  dpdkAppendf(line, sizeof(line), &off, " localDevTasks=");
  for (int i = 0; i < devTaskCount; i++) {
    const struct dpdkLbDevTaskLog *devTask = assignment->devTasks + i;
    dpdkAppendf(line, sizeof(line), &off, "%sdev%d:%d", i == 0 ? "" : ",",
                devTask->dev, devTask->taskCount);
  }
  if (devTaskCount == 0)
    dpdkAppendf(line, sizeof(line), &off, "<none>");

  INFO(NCCL_NET, "%s", line);
}
