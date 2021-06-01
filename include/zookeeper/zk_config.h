//
// Created by vasilis on 23/06/2020.
//

#ifndef ZOOKEEPER_CONFIG_H
#define ZOOKEEPER_CONFIG_H

#include "od_top.h"
#include "zk_opcodes.h"

// CORE CONFIGURATION
#define R_CREDITS 5
#define R_COALESCE 20
//#define MAX_READ_SIZE 300 //300 in terms of bytes for Reads
#define W_CREDITS 2
#define MAX_W_COALESCE 16
#define PREPARE_CREDITS 10
#define MAX_PREP_COALESCE 16
#define COMMIT_CREDITS 400
#define FEED_FROM_TRACE 0

// BQR Settings
//#define ZK_ENABLE_BQR           // comment out to disable
#define BQR_MAX_READ_BUFFER_SIZE (100 * 64)
//#define BQR_ENABLE_ASSERTS // comment out to disable

#define MAKE_FOLLOWERS_PASSIVE 0
#define ENABLE_GIDS 1 // should ldr tag writes with unique ids // this is useful for remote reads
#define ENABLE_GID_ORDERING (ENABLE_GIDS ? 1 : 0) // should global write ordering occur
#define DISABLE_UPDATING_KVS 0
#define USE_LIN_READS (ENABLE_GIDS ? 0 : 0)
#define ENABLE_LIN_READ_LATENCY 0

#define FOLLOWERS_PER_MACHINE (WORKERS_PER_MACHINE)
#define LEADERS_PER_MACHINE (WORKERS_PER_MACHINE)
#define FOLLOWER_MACHINE_NUM (MACHINE_NUM - 1)
#define LEADER_MACHINE 0 // which machine is the leader
#define FOLLOWER_NUM (FOLLOWERS_PER_MACHINE * FOLLOWER_MACHINE_NUM)



#define MICA_VALUE_SIZE (VALUE_SIZE + (FIND_PADDING_CUST_ALIGN(VALUE_SIZE, 32)))
#define MICA_OP_SIZE_  (28 + ((MICA_VALUE_SIZE)))
#define MICA_OP_PADDING_SIZE  (FIND_PADDING(MICA_OP_SIZE_))

#define MICA_OP_SIZE  (MICA_OP_SIZE_ + MICA_OP_PADDING_SIZE)
struct mica_op {

  struct key key;
  seqlock_t seqlock;
  uint64_t g_id;
  uint32_t key_id; // strictly for debug
  uint8_t value[MICA_VALUE_SIZE];
  uint8_t padding[MICA_OP_PADDING_SIZE];
};

// MULTICAST
#define PREP_MCAST_QP 0
#define COM_MCAST_QP 1 //





#endif //KITE_CONFIG_H
