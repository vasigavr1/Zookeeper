#include "zk_util.h"
#include "zk_inline_util.h"
#include "init_connect.h"
#include "trace_util.h"
#include "rdma_gen_util.h"

void *follower(void *arg)
{
  struct thread_params params = *(struct thread_params *) arg;
  uint32_t g_id = (uint32_t)( machine_id > LEADER_MACHINE ? ((machine_id - 1) * FOLLOWERS_PER_MACHINE) + params.id :
                  (machine_id * FOLLOWERS_PER_MACHINE) + params.id);
  uint8_t flr_id = (uint8_t) machine_id; // (machine_id > LEADER_MACHINE ? (machine_id - 1) : machine_id);
  uint16_t t_id = (uint16_t) params.id;
  if (t_id == 0) my_printf(yellow, "FOLLOWER-id %d \n", flr_id);
  uint16_t remote_ldr_thread = t_id;

  protocol_t protocol = FOLLOWER;
  context_t *ctx = calloc(1, sizeof(context_t));
  ctx->t_id = (uint16_t) params.id;
  ctx->m_id = (uint8_t) machine_id;
  ctx->qp_num = FOLLOWER_QP_NUM;

  ctx->qp_meta = calloc(FOLLOWER_QP_NUM, sizeof(per_qp_meta_t));
  per_qp_meta_t *qp_meta = ctx->qp_meta;
  ///
  create_per_qp_meta(&qp_meta[PREP_ACK_QP_ID], FLR_MAX_ACK_WRS,
                     FLR_MAX_RECV_PREP_WRS, UNICAST_TO_LDR, 1, 1, 3 * PREPARE_CREDITS,
                     FLR_PREP_RECV_SIZE, FLR_ACK_SEND_SIZE, false, 0, LEADER_MACHINE, 0);
  ///
  create_per_qp_meta(&qp_meta[COMMIT_W_QP_ID], FLR_MAX_W_WRS,
                     FLR_MAX_RECV_COM_WRS, UNICAST_TO_LDR, 1, 1, COMMIT_CREDITS,
                     FLR_COM_RECV_SIZE, FLR_W_SEND_SIZE, false, 0, LEADER_MACHINE, W_FIFO_SIZE);
  ///
  create_per_qp_meta(&qp_meta[FC_QP_ID], FLR_MAX_CREDIT_WRS, 0, CREDIT, 1,
                     0, 0, 0, 0, false, 0, LEADER_MACHINE, 0);
  ///
  create_per_qp_meta(&qp_meta[R_QP_ID], MAX_R_WRS, MAX_RECV_R_REP_WRS, UNICAST_TO_LDR, 1, 1,
                     FLR_R_REP_BUF_SLOTS, R_REP_RECV_SIZE, R_SEND_SIZE, false, 0, LEADER_MACHINE, R_FIFO_SIZE);


  //int *recv_q_depths, *send_q_depths;
  //
  //set_up_queue_depths_ldr_flr(&recv_q_depths, &send_q_depths, protocol);
  hrd_ctrl_blk_t *cb = hrd_ctrl_blk_init(t_id,	/* local_hid */
                                         0, -1, /* port_index, numa_node_id */
                                         0, 0,	/* #conn qps, uc */
                                         NULL, 0, -1,	/* prealloc conn recv_buf, recv_buf capacity, key */
                                         FOLLOWER_QP_NUM, FLR_BUF_SIZE,	/* num_dgram_qps, dgram_buf_size */
                                         MASTER_SHM_KEY + t_id, /* key */
                                         get_recv_q_depths(qp_meta, FOLLOWER_QP_NUM),
                                         get_send_q_depths(qp_meta, FOLLOWER_QP_NUM)); /* Depth of the dgram RECV Q*/

  ctx->cb = cb;
  init_ctx_mrs(ctx);
  set_per_qp_meta_recv_fifos(ctx);
  //uint32_t prep_push_ptr = 0, prep_pull_ptr = 0,
  //  com_push_ptr = 0, com_pull_ptr = 0,
  //  r_rep_push_ptr = 0, r_rep_pull_ptr = 0;
  //
  //qp_meta[PREP_ACK_QP_ID].recv_buf = (void *) (cb->dgram_buf);
  //qp_meta[COMMIT_W_QP_ID].recv_buf = (qp_meta[PREP_ACK_QP_ID].recv_buf + qp_meta[PREP_ACK_QP_ID].recv_buf_slot_num);
  //qp_meta[R_QP_ID].recv_buf = (qp_meta[COMMIT_W_QP_ID].recv_buf + qp_meta[COMMIT_W_QP_ID].recv_buf_slot_num);
  //
  //volatile zk_prep_mes_ud_t *prep_buffer = (volatile zk_prep_mes_ud_t *)(cb->dgram_buf);
  //zk_com_mes_ud_t *com_buffer = (zk_com_mes_ud_t *)(cb->dgram_buf + FLR_PREP_BUF_SIZE);
  //volatile struct  r_rep_message_ud_req *r_rep_buffer =
  //  (volatile r_rep_mes_ud_t *)(cb->dgram_buf + FLR_PREP_BUF_SIZE + FLR_COM_BUF_SIZE);

  /* ---------------------------------------------------------------------------
  ------------------------------MULTICAST SET UP-------------------------------
  ---------------------------------------------------------------------------*/

  // need to init mcast_cb before sync, such that we can post recvs
  if (ENABLE_MULTICAST == 1) {
    ctx->mcast_cb = zk_init_multicast(t_id, cb, protocol);
  }


  set_up_ctx_qps(ctx);
  init_ctx_recv_infos(ctx);

  post_recvs_with_recv_info(qp_meta[PREP_ACK_QP_ID].recv_info,
                            qp_meta[PREP_ACK_QP_ID].recv_wr_num);

  post_recvs_with_recv_info(qp_meta[COMMIT_W_QP_ID].recv_info,
                            qp_meta[COMMIT_W_QP_ID].recv_wr_num);

  /* -----------------------------------------------------
  --------------CONNECT WITH LEADER-----------------------
  ---------------------------------------------------------*/
  setup_connections_and_spawn_stats_thread(g_id, cb, t_id);

  /* -----------------------------------------------------
  --------------DECLARATIONS------------------------------
  ---------------------------------------------------------*/

  uint32_t credit_debug_cnt = 0, outstanding_writes = 0, trace_iter = 0;
  //uint64_t sent_ack_tx = 0, credit_tx = 0, w_tx = 0;

  latency_info_t latency_info = {
    .measured_req_flag = NO_REQ,
    .measured_sess_id = 0,
  };


  zk_resp_t *resp = (zk_resp_t *) calloc(ZK_TRACE_BATCH, sizeof(zk_resp_t));
  zk_trace_op_t *ops = (zk_trace_op_t *) calloc(ZK_TRACE_BATCH, sizeof(zk_trace_op_t));
  p_writes_t *p_writes = set_up_pending_writes(FLR_PENDING_WRITES, NULL, NULL, NULL, protocol);
  p_acks_t *p_acks = (p_acks_t *) calloc(1, sizeof(p_acks_t));
  zk_ack_mes_t *ack = (zk_ack_mes_t *) calloc(1, sizeof(zk_ack_mes_t));


  p_writes->w_fifo = qp_meta[COMMIT_W_QP_ID].send_fifo;
  zk_w_mes_t *writes = (zk_w_mes_t *) p_writes->w_fifo->fifo;
  for (uint16_t i = 0; i < W_FIFO_SIZE; i++) {
    for (uint16_t j = 0; j < MAX_W_COALESCE; j++) {
      writes[i].write[j].opcode = KVS_OP_PUT;
      writes[i].write[j].val_len = VALUE_SIZE >> SHIFT_BITS;
    }
  }
  qp_meta[COMMIT_W_QP_ID].credits = W_CREDITS;
  //if (!FLR_W_ENABLE_INLINING)
  //  w_mr = register_buffer(cb->pd, p_writes->w_fifo->fifo, W_FIFO_SIZE * sizeof(zk_w_mes_t));

  struct fifo *remote_w_buf;
  init_fifo(&remote_w_buf, LEADER_W_BUF_SLOTS * sizeof(uint16_t), 1);
  struct fifo *prep_buf_mirror;
  init_fifo(&prep_buf_mirror, FLR_PREP_BUF_SLOTS * sizeof(uint16_t), 1);

  /* ---------------------------------------------------------------------------
  ------------------------------INITIALIZE STATIC STRUCTURES--------------------
    ---------------------------------------------------------------------------*/
  init_ctx_send_wrs(ctx);

  //// SEND AND RECEIVE WRs
  //set_up_follower_WRs(ack_send_wr, ack_send_sgl, prep_recv_wr, prep_recv_sgl, w_send_wr, w_send_sgl,
  //                    com_recv_wr, com_recv_sgl, remote_ldr_thread, cb, w_mr, mcast_cb);
  //flr_set_up_credit_WRs(credit_send_wr, &credit_send_sgl, cb, flr_id, FLR_MAX_CREDIT_WRS, t_id);
  // TRACE
  trace_t *trace;
  if (!ENABLE_CLIENTS)
    trace = trace_init(t_id);

  /* ---------------------------------------------------------------------------
  ------------------------------LATENCY AND DEBUG-----------------------------------
  ---------------------------------------------------------------------------*/
  uint32_t wait_for_gid_dbg_counter = 0, completed_but_not_polled_coms = 0,
    completed_but_not_polled_preps = 0,
    wait_for_prepares_dbg_counter = 0, wait_for_coms_dbg_counter = 0;
  uint16_t last_session = 0;
  struct timespec start, end;
  uint16_t debug_ptr = 0;
  if (t_id == 0) my_printf(green, "Follower %d  reached the loop %u \n", t_id, p_acks->acks_to_send);
  /* ---------------------------------------------------------------------------
  ------------------------------START LOOP--------------------------------
  ---------------------------------------------------------------------------*/
  while(true) {
    if (t_stats[t_id].received_preps_mes_num > 0 && FLR_CHECK_DBG_COUNTERS)
      flr_check_debug_cntrs(&credit_debug_cnt, &wait_for_coms_dbg_counter,
                            &wait_for_prepares_dbg_counter,
                            &wait_for_gid_dbg_counter,
                            (volatile zk_prep_mes_ud_t *)
                            ctx->qp_meta[PREP_ACK_QP_ID].recv_fifo->fifo,
                            ctx->qp_meta[PREP_ACK_QP_ID].recv_fifo->pull_ptr,
                            p_writes, t_id);



    if (PUT_A_MACHINE_TO_SLEEP && (machine_id == MACHINE_THAT_SLEEPS) &&
        (t_stats[WORKERS_PER_MACHINE -1].cache_hits_per_thread > 4 * MILLION)) {
      if (t_id == 0) my_printf(yellow, "Machine performs scheduled failure\n");
      exit(0);
    }
  /* ---------------------------------------------------------------------------
  ------------------------------ POLL FOR PREPARES--------------------------
  ---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      flr_poll_for_prepares(ctx, p_writes, p_acks, prep_buf_mirror,
                            &completed_but_not_polled_preps,
                            &wait_for_prepares_dbg_counter);


  /* ---------------------------------------------------------------------------
  ------------------------------SEND ACKS-------------------------------------
  ---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      send_acks_to_ldr(ctx, p_writes, ack, p_acks);

    /* ---------------------------------------------------------------------------
    ------------------------------POLL FOR COMMITS---------------------------------
    ---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      poll_for_coms(ctx, p_writes,
                    remote_w_buf, &completed_but_not_polled_coms,
                    &wait_for_coms_dbg_counter);

    /* ---------------------------------------------------------------------------
    ------------------------------PROPAGATE UPDATES---------------------------------
    ---------------------------------------------------------------------------*/
    if (WRITE_RATIO > 0)
      flr_propagate_updates(p_writes, p_acks, resp, prep_buf_mirror,
                            &latency_info, t_id, &wait_for_gid_dbg_counter);


  /* ---------------------------------------------------------------------------
  ------------------------------PROBE THE CACHE--------------------------------------
  ---------------------------------------------------------------------------*/

  // Propagate the updates before probing the cache
    trace_iter = zk_batch_from_trace_to_KVS(trace_iter, t_id, trace, ops, flr_id,
                                            p_writes, resp, &latency_info, &last_session, protocol);



  /* ---------------------------------------------------------------------------
  ------------------------------SEND WRITES TO THE LEADER---------------------------
  ---------------------------------------------------------------------------*/
  if (WRITE_RATIO > 0)
    send_writes_to_the_ldr(ctx, p_writes, remote_w_buf,
                           &outstanding_writes);
  }
  return NULL;
}
