#ifndef ZK_UTILS_H
#define ZK_UTILS_H

#include "kvs.h"
#include "zk_main.h"
#include "init_connect.h"



extern uint64_t seed;

void zk_print_parameters_in_the_start();
void zk_static_assert_compile_parameters();
void zk_init_globals();

/* ---------------------------------------------------------------------------
------------------------------STATS --------------------------------------
---------------------------------------------------------------------------*/
struct stats {
  double batch_size_per_thread[WORKERS_PER_MACHINE];
  double com_batch_size[WORKERS_PER_MACHINE];
  double prep_batch_size[WORKERS_PER_MACHINE];
  double ack_batch_size[WORKERS_PER_MACHINE];
  double write_batch_size[WORKERS_PER_MACHINE];
	double stalled_gid[WORKERS_PER_MACHINE];
  double stalled_ack_prep[WORKERS_PER_MACHINE];
  double stalled_com_credit[WORKERS_PER_MACHINE];


	double cache_hits_per_thread[WORKERS_PER_MACHINE];


	double preps_sent[WORKERS_PER_MACHINE];
	double acks_sent[WORKERS_PER_MACHINE];
	double coms_sent[WORKERS_PER_MACHINE];

	double received_coms[WORKERS_PER_MACHINE];
	double received_acks[WORKERS_PER_MACHINE];
	double received_preps[WORKERS_PER_MACHINE];

	double write_ratio_per_client[WORKERS_PER_MACHINE];
};
void dump_stats_2_file(struct stats* st);
void print_latency_stats(void);


/* ---------------------------------------------------------------------------
------------------------------INITIALIZATION --------------------------------------
---------------------------------------------------------------------------*/

// Set up a struct that stores pending writes
p_writes_t* set_up_pending_writes(uint32_t size, struct ibv_send_wr*,
																	struct ibv_send_wr*, uint16_t credits[][MACHINE_NUM],
																	protocol_t);

/* ---------------------------------------------------------------------------
------------------------------LEADER--------------------------------------
---------------------------------------------------------------------------*/
// construct a prep_message-- max_size must be in bytes
void init_fifo(struct fifo **fifo, uint32_t max_size, uint32_t);



// Set up all leader WRs
void set_up_ldr_WRs(struct ibv_send_wr*, struct ibv_sge*,
                    struct ibv_send_wr*, struct ibv_sge*,
                    uint16_t, uint16_t, struct ibv_mr*,
                    struct ibv_mr*, mcast_cb_t*);
// Set up all Follower WRs
void set_up_follower_WRs(struct ibv_send_wr *ack_send_wr, struct ibv_sge *ack_send_sgl,
                         struct ibv_recv_wr *prep_recv_wr, struct ibv_sge *prep_recv_sgl,
                         struct ibv_send_wr *w_send_wr, struct ibv_sge *w_send_sgl,
                         struct ibv_recv_wr *com_recv_wr, struct ibv_sge *com_recv_sgl,
                         uint16_t remote_thread,
                         hrd_ctrl_blk_t *cb, struct ibv_mr *w_mr,
                         mcast_cb_t *mcast);

// Follower sends credits for commits
void flr_set_up_credit_WRs(struct ibv_send_wr* credit_send_wr, struct ibv_sge* credit_send_sgl,
                           hrd_ctrl_blk_t *cb, uint8_t flr_id, uint32_t max_credt_wrs, uint16_t);

// Post receives for the coherence traffic in the init phase
void pre_post_recvs(uint32_t*, struct ibv_qp *, uint32_t lkey, void*,
                    uint32_t, uint32_t, uint16_t, uint32_t);
// set up some basic leader buffers
zk_com_fifo_t* set_up_ldr_ops(zk_resp_t*, uint16_t);
// Set up the memory registrations required in the leader if there is no Inlining
void set_up_ldr_mrs(struct ibv_mr**, void*, struct ibv_mr**, void*,
                    hrd_ctrl_blk_t*);
// Set up the credits for leader
void ldr_set_up_credits_and_WRs(uint16_t credits[][MACHINE_NUM], struct ibv_recv_wr *credit_recv_wr,
                                struct ibv_sge *credit_recv_sgl, hrd_ctrl_blk_t *cb,
                                uint32_t max_credit_recvs);

//Set up the depths of all QPs
void set_up_queue_depths_ldr_flr(int**, int**, int);

/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/
// check if the given protocol is invalid
void check_protocol(int);


void print_latency_stats(void);


static mcast_cb_t* zk_init_multicast(uint16_t t_id, hrd_ctrl_blk_t *cb, int protocol)
{
	check_protocol(protocol);
	uint32_t *recv_q_depth = (uint32_t *) malloc(MCAST_QP_NUM * sizeof(int));
	recv_q_depth[0] = protocol == FOLLOWER ? FLR_RECV_PREP_Q_DEPTH : 1;
	recv_q_depth[1] = protocol == FOLLOWER ? FLR_RECV_COM_Q_DEPTH : 1;
	return create_mcast_cb(MCAST_GROUPS_NUM, MCAST_QP_NUM,
												 MACHINE_NUM, recv_q_depth, (void *) cb->dgram_buf,
												 (size_t) FLR_BUF_SIZE, t_id);
}


#endif /* ZK_UTILS_H */
