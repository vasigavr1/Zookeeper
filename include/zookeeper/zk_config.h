//
// Created by vasilis on 23/06/2020.
//

#ifndef ZOOKEEPER_CONFIG_H
#define ZOOKEEPER_CONFIG_H

#include "top.h"
#include "zk_opcodes.h"

// CORE CONFIGURATION
#define R_CREDITS 5
#define MAX_READ_SIZE 300 //300 in terms of bytes for Reads
#define W_CREDITS 6
#define MAX_W_COALESCE 6
#define PREPARE_CREDITS 6
#define MAX_PREP_COALESCE 9
#define COMMIT_CREDITS 30
#define FEED_FROM_TRACE 0

#define DISABLE_GID_ORDERING 1
#define DISABLE_UPDATING_KVS 0

#define FOLLOWERS_PER_MACHINE (WORKERS_PER_MACHINE)
#define LEADERS_PER_MACHINE (WORKERS_PER_MACHINE)
#define FOLLOWER_MACHINE_NUM (MACHINE_NUM - 1)
#define LEADER_MACHINE 0 // which machine is the leader
#define FOLLOWER_NUM (FOLLOWERS_PER_MACHINE * FOLLOWER_MACHINE_NUM)
#define MAKE_FOLLOWERS_PASSIVE 0
// QUORUM INFO
#define Q_INFO_NUM_SEND_WRS 2
#define Q_INFO_CREDIT_TARGETS 2


#define MICA_VALUE_SIZE (VALUE_SIZE + (FIND_PADDING_CUST_ALIGN(VALUE_SIZE, 32)))
#define MICA_OP_SIZE_  (20 + ((MICA_VALUE_SIZE)))
#define MICA_OP_PADDING_SIZE  (FIND_PADDING(MICA_OP_SIZE_))

#define MICA_OP_SIZE  (MICA_OP_SIZE_ + MICA_OP_PADDING_SIZE)
struct mica_op {
  // Cache-line -1
  uint8_t value[MICA_VALUE_SIZE];
  // Cache-line -2
  struct key key;
  seqlock_t seqlock;
  uint32_t key_id; // strictly for debug
  uint8_t padding[MICA_OP_PADDING_SIZE];
};

// MULTICAST
#define MCAST_FLOW_NUM 2 // prepares and commits
#define PREPARE_FLOW 0
#define COMMIT_FLOW 1
#define MCAST_LDR_RECV_QP_NUM 0 // leader does not receive
#define MCAST_FLR_RECV_QP_NUM 2 // follower receives in both flows
#define MCAST_LDR_SEND_QP_NUM 2 // leader sends in both flows
#define MCAST_FLR_SEND_QP_NUM 0 // follower does not send
#define MCAST_GROUPS_PER_FLOW 2
#define PREP_MCAST_QP 0
#define COM_MCAST_QP 1 //





#endif //KITE_CONFIG_H
