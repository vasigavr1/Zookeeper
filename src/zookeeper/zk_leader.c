#include "zk_util.h"
#include "zk_inline_util.h"
#include "init_connect.h"
#include "rdma_gen_util.h"
#include "trace_util.h"


void *leader(void *arg)
{
	struct thread_params params = *(struct thread_params *) arg;
	uint16_t t_id = (uint16_t) params.id;
  uint32_t g_id = get_gid((uint8_t) machine_id, t_id);

	if (ENABLE_MULTICAST == 1 && t_id == 0)
		my_printf(cyan, "MULTICAST IS ENABLED\n");


  context_t *ctx = create_ctx((uint8_t) machine_id,
                              (uint16_t) params.id,
                              (uint16_t) LEADER_QP_NUM,
                              local_ip);

  per_qp_meta_t *qp_meta = ctx->qp_meta;

  ///
  create_per_qp_meta(&qp_meta[PREP_ACK_QP_ID], LDR_MAX_PREP_WRS,
                     LDR_MAX_RECV_ACK_WRS, SEND_BCAST_LDR_RECV_UNI,  RECV_REPLY,
                     FOLLOWER_MACHINE_NUM, FOLLOWER_MACHINE_NUM, LEADER_ACK_BUF_SLOTS,
                     LDR_ACK_RECV_SIZE, LDR_PREP_SEND_SIZE, ENABLE_MULTICAST, false,
                     PREP_MCAST_QP, LEADER_MACHINE, PREP_FIFO_SIZE,
                     PREPARE_CREDITS, PREP_MES_HEADER, ack_handler, NULL,
                     "send preps", "recv acks");
  ///
  create_per_qp_meta(&qp_meta[COMMIT_W_QP_ID], LDR_MAX_COM_WRS,
                     LDR_MAX_RECV_W_WRS, SEND_BCAST_LDR_RECV_UNI, RECV_REQ,
                     FOLLOWER_MACHINE_NUM, FOLLOWER_MACHINE_NUM, LEADER_W_BUF_SLOTS,
                     LDR_W_RECV_SIZE, LDR_COM_SEND_SIZE, ENABLE_MULTICAST, false,
                     COM_MCAST_QP, LEADER_MACHINE, COMMIT_FIFO_SIZE,
                     COMMIT_CREDITS, 0, write_handler, NULL,
                     "send commits", "recv writes");
  ///
  create_per_qp_meta(&qp_meta[FC_QP_ID], 0, LDR_MAX_CREDIT_RECV, RECV_CREDITS, RECV_REPLY,
                     0, FOLLOWER_MACHINE_NUM, 0,
                     0, 0, false, false,
                     0, LEADER_MACHINE, 0, 0, 0, NULL, NULL,
                     NULL, "recv credits");
  ///
  create_per_qp_meta(&qp_meta[R_QP_ID], MAX_R_REP_WRS, MAX_RECV_R_WRS, SEND_UNI_REP_LDR_RECV_UNI_REQ, RECV_REQ,
                     FOLLOWER_MACHINE_NUM, FOLLOWER_MACHINE_NUM, LEADER_R_BUF_SLOTS,
                     R_RECV_SIZE, R_REP_SEND_SIZE, false, false,
                     0, LEADER_MACHINE, R_REP_FIFO_SIZE, 0, R_REP_MES_HEADER, r_handler, send_r_reps_helper,
                     "send r_Reps", "recv reads");


  set_up_ctx(ctx);

	/* -----------------------------------------------------
	--------------CONNECT WITH FOLLOWERS-----------------------
	---------------------------------------------------------*/
  setup_connections_and_spawn_stats_thread(g_id, ctx->cb, t_id);
  init_ctx_send_wrs(ctx);

	/* -----------------------------------------------------
	--------------DECLARATIONS------------------------------
	---------------------------------------------------------*/

	uint32_t trace_iter = 0;


	latency_info_t latency_info = {
			.measured_req_flag = NO_REQ,
			.measured_sess_id = 0,
	};

	zk_trace_op_t *ops = (zk_trace_op_t *) calloc((size_t) ZK_TRACE_BATCH, sizeof(zk_trace_op_t));
  zk_resp_t *resp = (zk_resp_t*) calloc((size_t) ZK_TRACE_BATCH, sizeof(zk_resp_t));

  zk_com_mes_t *commits = (zk_com_mes_t *) qp_meta[COMMIT_W_QP_ID].send_fifo->fifo;
  for(int i = 0; i <  ZK_TRACE_BATCH; i++) resp[i].type = EMPTY;
  for(int i = 0; i <  COMMIT_FIFO_SIZE; i++) {
    commits[i].opcode = KVS_OP_PUT;
  }

  zk_prep_mes_t *preps = (zk_prep_mes_t *) qp_meta[PREP_ACK_QP_ID].send_fifo->fifo;
  for (int i = 0; i < PREP_FIFO_SIZE; i++) {
    preps[i].opcode = KVS_OP_PUT;
    for (uint16_t j = 0; j < MAX_PREP_COALESCE; j++) {
      preps[i].prepare[j].opcode = KVS_OP_PUT;
      preps[i].prepare[j].val_len = VALUE_SIZE >> SHIFT_BITS;
    }
  }

  zk_ctx_t *zk_ctx = set_up_pending_writes(ctx, LEADER_PENDING_WRITES,
                                               LEADER);
  zk_ctx->prep_fifo = qp_meta[PREP_ACK_QP_ID].send_fifo;



  // There are no explicit credits and therefore we need to represent the remote prepare buffer somehow,
  // such that we can interpret the incoming acks correctly
  struct fifo *remote_prep_buf;
  init_fifo(&remote_prep_buf, FLR_PREP_BUF_SLOTS * sizeof(uint16_t), FOLLOWER_MACHINE_NUM);
  uint16_t *fifo = (uint16_t *)remote_prep_buf[FOLLOWER_MACHINE_NUM -1].fifo;
  assert(fifo[FLR_PREP_BUF_SLOTS -1] == 0);
  qp_meta[PREP_ACK_QP_ID].mirror_remote_recv_fifo = remote_prep_buf;


	// TRACE
	trace_t *trace = NULL;
  if (!ENABLE_CLIENTS)
    trace = trace_init(t_id);

	/* ---------------------------------------------------------------------------
	------------------------------LATENCY AND DEBUG-----------------------------------
	---------------------------------------------------------------------------*/
  uint16_t last_session = 0;
  uint32_t wait_for_gid_dbg_counter = 0, wait_for_acks_dbg_counter = 0;
  uint32_t credit_debug_cnt[LDR_VC_NUM] = {0}, time_out_cnt[LDR_VC_NUM] = {0};
  uint32_t outstanding_prepares = 0;
	struct timespec start, end;
  if (t_id == 0) my_printf(green, "Leader %d  reached the loop \n", t_id);

	/* ---------------------------------------------------------------------------
	------------------------------START LOOP--------------------------------
	---------------------------------------------------------------------------*/
	while(true) {

     if (ENABLE_ASSERTIONS)
       ldr_check_debug_cntrs(credit_debug_cnt, &wait_for_acks_dbg_counter,
                             &wait_for_gid_dbg_counter, zk_ctx, t_id);

		/* ---------------------------------------------------------------------------
		------------------------------ POLL FOR ACKS--------------------------------
		---------------------------------------------------------------------------*/
    poll_incoming_messages(ctx, zk_ctx, PREP_ACK_QP_ID);

/* ---------------------------------------------------------------------------
		------------------------------ PROPAGATE UPDATES--------------------------
		---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      /* After propagating the acked messages we push their l_id to a prep_message buffer
       * to send the commits and clear the p_write buffer space. The reason behind that
       * is that we do not want to wait for the commit broadcast to happen to clear the
       * buffer space for new writes*/
      ldr_propagate_updates(ctx, zk_ctx, &latency_info, &wait_for_gid_dbg_counter);


    /* ---------------------------------------------------------------------------
		------------------------------ BROADCAST COMMITS--------------------------
		---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      broadcast_commits(ctx, zk_ctx, time_out_cnt);
    /* ---------------------------------------------------------------------------
    ------------------------------PROBE THE CACHE--------------------------------------
    ---------------------------------------------------------------------------*/


    // Get a new batch from the trace, pass it through the cache and create
    // the appropriate prepare messages
		trace_iter = zk_batch_from_trace_to_KVS(ctx, trace_iter, t_id, trace, ops,
                                            (uint8_t) FOLLOWER_MACHINE_NUM, zk_ctx, resp,
                                            &latency_info,  &last_session);

    /* ---------------------------------------------------------------------------
		------------------------------POLL FOR REMOTE WRITES--------------------------
		---------------------------------------------------------------------------*/
    // get local and remote writes back to back to increase the write batch
    poll_incoming_messages(ctx, zk_ctx, COMMIT_W_QP_ID);



    /* ---------------------------------------------------------------------------
		------------------------------GET GLOBAL WRITE IDS--------------------------
		---------------------------------------------------------------------------*/
    // Assign a global write  id to each new write
    if (WRITE_RATIO > 0) zk_get_g_ids(zk_ctx, t_id);

    if (ENABLE_ASSERTIONS) check_ldr_p_states(zk_ctx, t_id);
		/* ---------------------------------------------------------------------------
		------------------------------BROADCASTS--------------------------------------
		---------------------------------------------------------------------------*/
		if (WRITE_RATIO > 0)
			/* Poll for credits - Perform broadcasts
				 Post the appropriate number of credit receives before sending anything */
      broadcast_prepares(ctx, zk_ctx, remote_prep_buf, time_out_cnt, &outstanding_prepares);
    if (ENABLE_ASSERTIONS) {
      assert(zk_ctx->w_size <= LEADER_PENDING_WRITES);
      for (uint16_t i = 0; i < LEADER_PENDING_WRITES - zk_ctx->w_size; i++) {
        uint16_t ptr = (zk_ctx->w_push_ptr + i) % LEADER_PENDING_WRITES;
        assert (zk_ctx->w_state[ptr] == INVALID);
      }
    }

    /* ---------------------------------------------------------------------------
		------------------------------READ_REPLIES------------------------------------
		---------------------------------------------------------------------------*/
    poll_incoming_messages(ctx, zk_ctx, R_QP_ID);
    send_unicasts(ctx, zk_ctx, R_QP_ID);


	}
	return NULL;
}

