#ifndef ZOOKEEPER_MAIN_H
#define ZOOKEEPER_MAIN_H

#include <od_rdma_gen_util.h>
#include "od_fifo.h"
#include <od_network_context.h>
#include "od_top.h"


#include "od_city.h"
#include "od_hrd.h"
#include "zk_bqr.h"


#define ENABLE_CACHE_STATS 0
#define DUMP_STATS_2_FILE 0


/*-------------------------------------------------
-----------------DEBUGGING-------------------------
--------------------------------------------------*/

#define REMOTE_LATENCY_MARK 100 // mark a remote request for measurement by attaching this to the imm_data of the wr
#define USE_A_SINGLE_KEY 0
#define DISABLE_HYPERTHREADING 0 // do not shcedule two threads on the same core
#define DEFAULT_SL 0 //default service level


/* --------------------------------------------------------------------------------
 * -----------------------------ZOOKEEPER---------------------------------------
 * --------------------------------------------------------------------------------
 * --------------------------------------------------------------------------------*/
typedef enum {FOLLOWER = 1, LEADER} protocol_t;

#define LDR_CREDIT_DIVIDER (1)
#define LDR_CREDITS_IN_MESSAGE (W_CREDITS / LDR_CREDIT_DIVIDER)
#define FLR_CREDIT_DIVIDER (2)
#define FLR_CREDITS_IN_MESSAGE (COMMIT_CREDITS / FLR_CREDIT_DIVIDER)

// if this is smaller than MAX_BCAST_BATCH + 2 it will deadlock because the signaling messaged is polled before actually posted
#define COM_BCAST_SS_BATCH MAX((MIN_SS_BATCH / (FOLLOWER_MACHINE_NUM)), (MAX_BCAST_BATCH + 2))
#define PREP_BCAST_SS_BATCH MAX((MIN_SS_BATCH / (FOLLOWER_MACHINE_NUM)), (MAX_BCAST_BATCH + 2))


// post some extra receives to avoid spurious out_of_buffer errors
#define RECV_WR_SAFETY_MARGIN 2

// -------ACKS-------------
#define USE_QUORUM 1
#define QUORUM_NUM ((MACHINE_NUM / 2) + 1)
#define LDR_QUORUM_OF_ACKS (USE_QUORUM == 1 ? (QUORUM_NUM - 1): FOLLOWER_MACHINE_NUM) //FOLLOWER_MACHINE_NUM //


#define FLR_ACK_SEND_SIZE (16) // a local id and its metadata
#define LDR_ACK_RECV_SIZE (GRH_SIZE + (FLR_ACK_SEND_SIZE))


// -- COMMITS-----
#define COMMIT_FIFO_SIZE 1 //((COM_ENABLE_INLINING == 1) ? (COMMIT_CREDITS) : (COM_BCAST_SS_BATCH))

//---WRITES---
#define W_MES_HEADER 1
#define W_SIZE (VALUE_SIZE + KEY_SIZE + 7)
#define FLR_W_SEND_SIZE (W_MES_HEADER + (MAX_W_COALESCE * W_SIZE))
#define LDR_W_RECV_SIZE (GRH_SIZE + FLR_W_SEND_SIZE)
#define FLR_W_ENABLE_INLINING ((FLR_W_SEND_SIZE > MAXIMUM_INLINE_SIZE) ?  0 : 1)


//---READS---
#define R_MES_HEADER (10) // local id + coalesce num + m_id
//#define EFFECTIVE_MAX_R_SIZE (MAX_READ_SIZE - R_MES_HEADER)
#define R_SIZE (17) // key +g_id + opcode
//#define R_COALESCE (EFFECTIVE_MAX_R_SIZE / R_SIZE)
#define R_MES_SIZE (R_MES_HEADER + (R_SIZE * R_COALESCE))
#define R_SEND_SIZE R_MES_SIZE
#define MAX_READ_COALESCE R_COALESCE
#define LDR_R_RECV_SIZE (GRH_SIZE + R_MES_SIZE)
#define FLR_R_ENABLE_INLINING ((R_SEND_SIZE > MAXIMUM_INLINE_SIZE) ?  0 : 1)

#define R_SEND_SIDE_PADDING FIND_PADDING(R_SEND_SIZE)
#define ALIGNED_R_SEND_SIDE (R_SEND_SIZE + R_SEND_SIDE_PADDING)

#define MAX_RECV_R_WRS ((R_CREDITS * FOLLOWER_MACHINE_NUM) + RECV_WR_SAFETY_MARGIN)
#define MAX_INCOMING_R (MAX_RECV_R_WRS * MAX_READ_COALESCE)
#define MAX_R_WRS (R_CREDITS)
#define R_ENABLE_INLINING ((R_SEND_SIZE > MAXIMUM_INLINE_SIZE) ?  0 : 1)
#define R_RECV_SIZE (GRH_SIZE + ALIGNED_R_SEND_SIDE)



// READ REPLIES
#define R_REP_MES_HEADER (9) //l_id 8 , coalesce_num 1
#define R_REP_BIG_SIZE (VALUE_SIZE + 8 + 1) // g_id + opcode
#define R_REP_SMALL_SIZE 1
#define R_REP_MES_SIZE (R_REP_MES_HEADER + (R_COALESCE * R_REP_BIG_SIZE))
#define MAX_R_REP_MES_SIZE R_REP_MES_SIZE
#define R_REP_SEND_SIZE MIN(MAX_R_REP_MES_SIZE, MTU)

#define MAX_R_REP_COALESCE R_COALESCE
#define MAX_REPS_IN_REP MAX_R_REP_COALESCE

#define R_REP_SEND_SIDE_PADDING FIND_PADDING(R_REP_SEND_SIZE)
#define ALIGNED_R_REP_SEND_SIDE (R_REP_SEND_SIZE + R_REP_SEND_SIDE_PADDING)
#define R_REP_RECV_SIZE (GRH_SIZE + ALIGNED_R_REP_SEND_SIDE)

#define MAX_RECV_R_REP_WRS (R_CREDITS) // a follower recvs r_Reps only from on machine (the leader)
#define MAX_R_REP_WRS (R_CREDITS * FOLLOWER_MACHINE_NUM) // leader sends r_reps_to all

#define R_REP_ENABLE_INLINING ((R_REP_SEND_SIZE > MAXIMUM_INLINE_SIZE) ?  0 : 1)
#define R_REP_FIFO_SIZE (MAX_RECV_R_WRS * MAX_READ_COALESCE)


#define PREP_MES_HEADER 10 // opcode(1), coalesce_num(1) l_id (8)
#define PREP_SIZE (KEY_SIZE + VALUE_SIZE + 13) // Size of a write
#define LDR_PREP_SEND_SIZE (PREP_MES_HEADER + (MAX_PREP_COALESCE * PREP_SIZE))
#define FLR_PREP_RECV_SIZE (GRH_SIZE + LDR_PREP_SEND_SIZE)

#define LEADER_PREPARE_ENABLE_INLINING ((LDR_PREP_SEND_SIZE > MAXIMUM_INLINE_SIZE) ?  0 : 1)

#define LDR_MAX_INCOMING_R (MAX_RECV_R_WRS * MAX_READ_COALESCE)


//-- LEADER

#define LEADER_ACK_BUF_SLOTS ( FOLLOWER_MACHINE_NUM * PREPARE_CREDITS)
#define LEADER_ACK_BUF_SIZE (LDR_ACK_RECV_SIZE * LEADER_ACK_BUF_SLOTS)
#define LEADER_W_BUF_SLOTS (2 * FOLLOWER_MACHINE_NUM * W_CREDITS)
#define LEADER_W_BUF_SIZE (LDR_W_RECV_SIZE * LEADER_W_BUF_SLOTS)
#define LEADER_R_BUF_SLOTS (2 * FOLLOWER_MACHINE_NUM * R_CREDITS)
#define LEADER_R_BUF_SIZE (LDR_R_RECV_SIZE * LEADER_R_BUF_SLOTS)


#define LEADER_BUF_SIZE (LEADER_W_BUF_SIZE + LEADER_ACK_BUF_SIZE + LEADER_R_BUF_SIZE)
#define LEADER_BUF_SLOTS (LEADER_W_BUF_SLOTS + LEADER_ACK_BUF_SLOTS + LEADER_R_BUF_SLOTS)

#define LEADER_REMOTE_W_SLOTS (FOLLOWER_MACHINE_NUM * W_CREDITS * MAX_W_COALESCE)
#define LEADER_PENDING_WRITES (SESSIONS_PER_THREAD + LEADER_REMOTE_W_SLOTS + 1)
#define PREP_FIFO_SIZE (LEADER_PENDING_WRITES)

//---------LEADER-----------------------
// PREP_ACK_QP_ID 0: send Prepares -- receive ACKs
#define LDR_MAX_PREP_WRS (MESSAGES_IN_BCAST_BATCH)
#define LDR_MAX_RECV_ACK_WRS LEADER_ACK_BUF_SLOTS //(3 * FOLLOWER_MACHINE_NUM * PREPARE_CREDITS)
// COMMIT_W_QP_ID 1: send Commits  -- receive Writes
#define LDR_MAX_COM_WRS (MESSAGES_IN_BCAST_BATCH)
#define LDR_MAX_RECV_W_WRS (FOLLOWER_MACHINE_NUM * W_CREDITS)
// Credits WRs
#define LDR_MAX_CREDIT_WRS ((W_CREDITS / LDR_CREDITS_IN_MESSAGE ) * FOLLOWER_MACHINE_NUM)
#define LDR_MAX_CREDIT_RECV ((COMMIT_CREDITS / FLR_CREDITS_IN_MESSAGE ) * FOLLOWER_MACHINE_NUM)



//--FOLLOWER
#define FLR_PREP_BUF_SLOTS (3 * PREPARE_CREDITS)
#define FLR_PREP_BUF_SIZE (FLR_PREP_RECV_SIZE * FLR_PREP_BUF_SLOTS)
#define FLR_COM_BUF_SLOTS (COMMIT_CREDITS)
#define FLR_COM_BUF_SIZE (CTX_COM_RECV_SIZE * FLR_COM_BUF_SLOTS)
#define FLR_R_REP_BUF_SLOTS (R_CREDITS)
#define FLR_R_REP_BUF_SIZE (R_REP_RECV_SIZE * FLR_R_REP_BUF_SLOTS)

#define FLR_BUF_SIZE (FLR_PREP_BUF_SIZE + FLR_COM_BUF_SIZE + FLR_R_REP_BUF_SIZE)
#define FLR_BUF_SLOTS (FLR_PREP_BUF_SLOTS + FLR_COM_BUF_SLOTS + FLR_R_REP_BUF_SLOTS)
#define W_FIFO_SIZE (SESSIONS_PER_THREAD + 1)
#define MAX_PREP_BUF_SLOTS_TO_BE_POLLED (2 * PREPARE_CREDITS)
#define FLR_PENDING_WRITES (2 * PREPARE_CREDITS * MAX_PREP_COALESCE) // 2/3 of the buffer
#define FLR_DISALLOW_OUT_OF_ORDER_PREPARES 1


//--------FOLLOWER--------------
// // PREP_ACK_QP_ID 0: receive Prepares -- send ACKs
#define FLR_MAX_ACK_WRS (2)
#define FLR_MAX_RECV_PREP_WRS (3 * PREPARE_CREDITS) // if not enough prep messges get lost
#define ACK_SEND_BUF_SIZE (2)
// COMMIT_W_QP_ID 1: send Writes  -- receive Commits
#define FLR_MAX_W_WRS (W_CREDITS)
#define FLR_MAX_RECV_COM_WRS (COMMIT_CREDITS)
// Credits WRs
#define FLR_MAX_CREDIT_WRS 1 //(COMMIT_CREDITS / FLR_CREDITS_IN_MESSAGE )
#define FLR_MAX_CREDIT_RECV (W_CREDITS / LDR_CREDITS_IN_MESSAGE)
#define ACK_SEND_SS_BATCH MAX(MIN_SS_BATCH, (FLR_MAX_ACK_WRS + 2))
#define R_SS_BATCH MAX(MIN_SS_BATCH, (MAX_R_WRS + 2))
#define R_FIFO_SIZE (SESSIONS_PER_THREAD + 1)

#define FLR_PENDING_READS (SESSIONS_PER_THREAD + 1)
#define MAX_LIDS_IN_A_COMMIT MIN(FLR_PENDING_WRITES, LEADER_PENDING_WRITES)



#define ZK_TRACE_BATCH SESSIONS_PER_THREAD
#define ZK_UPDATE_BATCH  MAX (FLR_PENDING_WRITES, LEADER_PENDING_WRITES)
/*-------------------------------------------------
-----------------QUEUE DEPTHS-------------------------
--------------------------------------------------*/

#define COM_CREDIT_SS_BATCH MAX(MIN_SS_BATCH, (FLR_MAX_CREDIT_WRS + 1))
#define WRITE_SS_BATCH MAX(MIN_SS_BATCH, (FLR_MAX_W_WRS + 1))
#define R_REP_SS_BATCH MAX(MIN_SS_BATCH, (MAX_R_REP_WRS + 1))


#define QP_NUM 3
#define PREP_ACK_QP_ID 0
#define COMMIT_W_QP_ID 1
#define R_QP_ID 2

/*
 * -------LEADER-------------
 * 1st Dgram send Prepares -- receive ACKs
 * 2nd Dgram send Commits  -- receive Writes
 * 3rd Dgram  -- receive reads -- send r_reps
 *
 * ------FOLLOWER-----------
 * 1st Dgram receive prepares -- send Acks
 * 2nd Dgram receive Commits  -- send Writes
 * 3rd Dgram send Reads receive R_Reps
 * */


// DEBUG

#define FLR_CHECK_DBG_COUNTERS 1




/* SHM key for the 1st request region created by master. ++ for other RRs.*/
#define MASTER_SHM_KEY 24


//Defines for parsing the trace
#define _200_K 200000
#define MAX_TRACE_SIZE _200_K
#define TRACE_SIZE K_128 // used only when manufacturing a trace
#define NOP 0



/*
 *  SENT means we sent the prepare message // OR an ack has been sent
 *  READY means all acks have been gathered // OR a commit has been received
 *  SEND_COMMITS menas it has been propagated to the
 *  cache and commits should be sent out
 * */
typedef enum op_state {INVALID, VALID, SENT, READY, SEND_COMMITTS} w_state_t;



typedef struct zk_prepare {
	uint8_t flr_id;
	uint8_t val_len;
  uint16_t sess_id;
	uint64_t g_id;
	mica_key_t key;
	uint8_t opcode; //override opcode
	uint8_t value[VALUE_SIZE];
} __attribute__((__packed__)) zk_prepare_t;

// prepare message
typedef struct zk_prep_message {
	uint8_t opcode;
	uint8_t coalesce_num;
	uint64_t l_id; // send the bottom half of the lid
	zk_prepare_t prepare[MAX_PREP_COALESCE];
} __attribute__((__packed__)) zk_prep_mes_t;

typedef struct zk_prep_message_ud_req {
	uint8_t grh[GRH_SIZE];
	zk_prep_mes_t prepare;
} zk_prep_mes_ud_t;


typedef struct zk_write {
  uint8_t flr_id;
	uint8_t opcode;
	uint8_t val_len;
  uint32_t sess_id;
  mica_key_t key;	/* 8B */
  uint8_t value[VALUE_SIZE];
} __attribute__((__packed__)) zk_write_t;

typedef struct zk_w_message {
	uint8_t coalesce_num;
  zk_write_t write[MAX_W_COALESCE];
} __attribute__((__packed__)) zk_w_mes_t;


typedef struct zk_w_message_ud_req {
  uint8_t unused[GRH_SIZE];
  zk_w_mes_t w_mes;
} zk_w_mes_ud_t;

//-------------------------
// Reads and Read Replies
//-------------------------

//
typedef struct zk_read {
	uint8_t opcode;
  struct key key;
  uint64_t g_id;
} __attribute__((__packed__)) zk_read_t;

typedef struct zk_r_message {
  uint8_t coalesce_num;
  uint8_t m_id;
  uint64_t l_id ;
  zk_read_t read[R_COALESCE];
} __attribute__((__packed__)) zk_r_mes_t;


typedef struct zk_r_message_ud_req {
  uint8_t unused[GRH_SIZE];
  uint8_t r_mes[ALIGNED_R_SEND_SIDE];
} zk_r_mes_ud_t;


typedef struct zk_r_rep_small {
	uint8_t opcode;
}__attribute__((__packed__)) zk_r_rep_small_t;


typedef struct zk_r_rep_big {
  uint8_t opcode;
	uint64_t g_id;
  uint8_t value[VALUE_SIZE];
}__attribute__((__packed__)) zk_r_rep_big_t;

typedef struct r_rep_message {
  uint64_t l_id;
	uint8_t coalesce_num;
	zk_r_rep_big_t r_rep[MAX_R_REP_COALESCE];
} __attribute__((__packed__)) zk_r_rep_mes_t;


typedef struct zk_r_rep_message_ud_req {
  uint8_t unused[GRH_SIZE];
  uint8_t r_rep_mes[ALIGNED_R_REP_SEND_SIDE];
} zk_r_rep_mes_ud_t;

/*------------TEMPLATES----------*/
struct zk_r_rep_message_template {
  uint8_t unused[ALIGNED_R_REP_SEND_SIDE];
};

struct zk_r_message_template {
  uint8_t unused[ALIGNED_R_SEND_SIDE];
};




typedef struct ptrs_to_reads {
	uint16_t polled_reads;
	zk_read_t **ptr_to_ops;
	zk_r_mes_t **ptr_to_r_mes;
	bool *coalesce_r_rep;
} ptrs_to_r_t;




typedef struct zk_resp {
  uint8_t type;
} zk_resp_t;

typedef struct r_rob {
	bool seen_larger_g_id;
	uint8_t opcode;
	mica_key_t key;
	uint8_t value[VALUE_SIZE]; //
	uint8_t *value_to_read;
	uint32_t state;
	uint32_t log_no;
	uint32_t val_len;
	uint32_t sess_id;
	uint64_t g_id;
	uint64_t l_id;
} r_rob_t ;


typedef struct w_rob {
	uint32_t session_id;
	uint64_t g_id;
	w_state_t w_state;
	uint8_t flr_id;
	uint8_t acks_seen;
	//uint32_t index_to_req_array;
	bool is_local;
	zk_prepare_t *ptr_to_op;

} w_rob_t;

// A data structute that keeps track of the outstanding writes
typedef struct zk_ctx {
  // reorder buffers
  fifo_t *r_rob;
	fifo_t *w_rob;

	trace_t *trace;
	uint32_t trace_iter;
  uint16_t last_session;

  ctx_trace_op_t *ops;
  zk_resp_t *resp;

	ptrs_to_r_t *ptrs_to_r;
	uint64_t local_w_id;
  uint64_t local_r_id;

	uint32_t unordered_ptr;
  uint64_t highest_g_id_taken;

  uint32_t *index_to_req_array; // [SESSIONS_PER_THREAD]
	bool *stalled;

	bool all_sessions_stalled;
  protocol_t protocol;

	uint32_t wait_for_gid_dbg_counter;
  uint32_t stalled_sessions_dbg_counter;

#ifdef ZK_ENABLE_BQR
  bqr_ctx b_ctx;
#endif
} zk_ctx_t;








typedef struct thread_stats { // 2 cache lines
	long long total_reqs;
	long long remotes_per_client;
	long long locals_per_client;

	long long preps_sent;
	long long acks_sent;
	long long coms_sent;
  long long writes_sent;
	uint64_t reads_sent;

  long long prep_sent_mes_num;
  long long acks_sent_mes_num;
  long long coms_sent_mes_num;
  long long writes_sent_mes_num;
	uint64_t reads_sent_mes_num;


  long long received_coms;
	long long received_acks;
	long long received_preps;
  long long received_writes;

  long long received_coms_mes_num;
  long long received_acks_mes_num;
  long long received_preps_mes_num;
  long long received_writes_mes_num;


	uint64_t batches_per_thread; // Leader only
  uint64_t total_writes; // Leader only

	uint64_t stalled_gid;
  uint64_t stalled_ack_prep;
  uint64_t stalled_com_credit;
	//long long unused[3]; // padding to avoid false sharing
} thread_stats_t;

//extern remote_qp_t remote_follower_qp[FOLLOWER_MACHINE_NUM][FOLLOWERS_PER_MACHINE][FOLLOWER_QP_NUM];
//extern remote_qp_t remote_leader_qp[LEADERS_PER_MACHINE][LEADER_QP_NUM];
extern thread_stats_t t_stats[WORKERS_PER_MACHINE];
struct mica_op;
extern atomic_uint_fast64_t global_w_id, committed_global_w_id;


void print_latency_stats(void);





#endif
