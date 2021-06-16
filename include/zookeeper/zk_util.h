#ifndef ZK_UTILS_H
#define ZK_UTILS_H

//#include <init_func.h>
#include "od_multicast.h"
#include "zk_main.h"
#include "../../../odlib/include/network_api/od_network_context.h"
#include <od_init_func.h>
#include "od_kvs.h"


extern uint64_t seed;

void zk_print_parameters_in_the_start();
void zk_static_assert_compile_parameters();
void zk_init_globals();

/* ---------------------------------------------------------------------------
------------------------------STATS --------------------------------------
---------------------------------------------------------------------------*/
typedef struct stats {
  double batch_size_per_thread[WORKERS_PER_MACHINE];
  double com_batch_size[WORKERS_PER_MACHINE];
  double prep_batch_size[WORKERS_PER_MACHINE];
  double ack_batch_size[WORKERS_PER_MACHINE];
  double write_batch_size[WORKERS_PER_MACHINE];
	double stalled_gid[WORKERS_PER_MACHINE];
  double stalled_ack_prep[WORKERS_PER_MACHINE];
  double stalled_com_credit[WORKERS_PER_MACHINE];


	double total_reqs[WORKERS_PER_MACHINE];


	double preps_sent[WORKERS_PER_MACHINE];
	double acks_sent[WORKERS_PER_MACHINE];
	double coms_sent[WORKERS_PER_MACHINE];

	double received_coms[WORKERS_PER_MACHINE];
	double received_acks[WORKERS_PER_MACHINE];
	double received_preps[WORKERS_PER_MACHINE];

	double write_ratio_per_client[WORKERS_PER_MACHINE];
} all_stats_t;

void zk_stats(stats_ctx_t *ctx);
void dump_stats_2_file(struct stats* st);
void print_latency_stats(void);


/* ---------------------------------------------------------------------------
------------------------------INITIALIZATION --------------------------------------
---------------------------------------------------------------------------*/
void zk_init_qp_meta(context_t *ctx);

// Set up a struct that stores pending writes
zk_ctx_t *set_up_zk_ctx(context_t *ctx);


/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/

void print_latency_stats(void);



static void zk_init_functionality(int argc, char *argv[])
{
  zk_print_parameters_in_the_start();
  od_generic_static_assert_compile_parameters();
  zk_static_assert_compile_parameters();
  od_generic_init_globals(QP_NUM);
  zk_init_globals();
  od_handle_program_inputs(argc, argv);
}


#endif /* ZK_UTILS_H */
