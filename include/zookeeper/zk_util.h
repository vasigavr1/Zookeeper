#ifndef ZK_UTILS_H
#define ZK_UTILS_H

#include "../../../shared/include/multicast/multicast.h"
#include "kvs.h"
#include "zk_main.h"
#include <network_context.h>
//#include "init_connect.h"



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
zk_ctx_t *set_up_pending_writes(context_t *ctx, protocol_t);

/* ---------------------------------------------------------------------------
------------------------------LEADER--------------------------------------
---------------------------------------------------------------------------*/
// construct a prep_message-- max_size must be in bytes
void init_fifo(struct fifo **fifo, uint32_t max_size, uint32_t);

// Post receives for the coherence traffic in the init phase
void pre_post_recvs(uint32_t*, struct ibv_qp *, uint32_t lkey, void*,
                    uint32_t, uint32_t, uint16_t, uint32_t);
// set up some basic leader buffers

void zk_init_send_fifos(context_t *ctx);

/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/
// check if the given protocol is invalid
void check_protocol(int);


void print_latency_stats(void);


static mcast_cb_t* zk_init_multicast(uint16_t t_id, void* recv_buffer, int protocol)
{
	check_protocol(protocol);
	uint32_t *recv_q_depth = NULL;
  uint16_t *group_to_send_to = NULL;
  bool *recvs_from_flow =  NULL;

	uint16_t recv_qp_num = MCAST_LDR_RECV_QP_NUM, send_num = MCAST_LDR_SEND_QP_NUM;

	if (protocol == FOLLOWER) {
		recv_q_depth = (uint32_t *) malloc(MCAST_FLR_RECV_QP_NUM * sizeof(int));
		recv_q_depth[0] = FLR_RECV_PREP_Q_DEPTH;
		recv_q_depth[1] = FLR_RECV_COM_Q_DEPTH;
		recv_qp_num = MCAST_FLR_RECV_QP_NUM;
		send_num = MCAST_FLR_SEND_QP_NUM;
    recvs_from_flow = (bool *) malloc(MCAST_FLOW_NUM * sizeof(bool));
    for (int i = 0; i < MCAST_FLOW_NUM; ++i) {
      recvs_from_flow[i] = true;
    }
	}
  else {
    group_to_send_to = (uint16_t *) malloc(MCAST_FLOW_NUM * (sizeof(uint16_t)));
    for (int i = 0; i < MCAST_FLOW_NUM; ++i) {
      group_to_send_to[i] = 0; // Which group you want to send to in that flow
    }
  }


  uint16_t *groups_per_flow = (uint16_t *) calloc(MCAST_FLOW_NUM, sizeof(uint16_t));
	groups_per_flow[PREPARE_FLOW] = 1;
	groups_per_flow[COMMIT_FLOW] = 1;





	return create_mcast_cb(MCAST_FLOW_NUM, recv_qp_num, send_num,
												 groups_per_flow, recv_q_depth,
                         group_to_send_to,
                         recvs_from_flow,
                         local_ip,
                         recv_buffer,
												 (size_t) FLR_BUF_SIZE, t_id);
}


#endif /* ZK_UTILS_H */
