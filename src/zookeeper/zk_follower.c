#include "zk_util.h"
#include "zk_inline_util.h"
#include "init_connect.h"
#include "trace_util.h"

void *follower(void *arg)
{
  struct thread_params params = *(struct thread_params *) arg;
  uint32_t g_id = (uint32_t)( machine_id > LEADER_MACHINE ? ((machine_id - 1) * FOLLOWERS_PER_MACHINE) + params.id :
                  (machine_id * FOLLOWERS_PER_MACHINE) + params.id);
  uint8_t flr_id = (uint8_t) machine_id;
  uint16_t t_id = (uint16_t) params.id;
  if (t_id == 0) my_printf(yellow, "FOLLOWER-id %d \n", flr_id);

  protocol_t protocol = FOLLOWER;
  context_t *ctx = create_ctx((uint8_t) machine_id,
                              (uint16_t) params.id,
                              (uint16_t) FOLLOWER_QP_NUM,
                              local_ip);



  per_qp_meta_t *qp_meta = ctx->qp_meta;
  ///
  create_per_qp_meta(&qp_meta[PREP_ACK_QP_ID], FLR_MAX_ACK_WRS,
                     FLR_MAX_RECV_PREP_WRS, SEND_UNI_REP_RECV_LDR_BCAST, 1, 1, 3 * PREPARE_CREDITS,
                     FLR_PREP_RECV_SIZE, FLR_ACK_SEND_SIZE, false, ENABLE_MULTICAST, PREP_MCAST_QP,
                     LEADER_MACHINE, 0, 0);
  ///
  create_per_qp_meta(&qp_meta[COMMIT_W_QP_ID], FLR_MAX_W_WRS,
                     FLR_MAX_RECV_COM_WRS, SEND_UNI_REP_RECV_LDR_BCAST, 1, 1, COMMIT_CREDITS,
                     FLR_COM_RECV_SIZE, FLR_W_SEND_SIZE, false, ENABLE_MULTICAST, COM_MCAST_QP,
                     LEADER_MACHINE, W_FIFO_SIZE, W_CREDITS);
  ///
  create_per_qp_meta(&qp_meta[FC_QP_ID], FLR_MAX_CREDIT_WRS, 0, SEND_CREDITS_LDR_RECV_NONE, 1,
                     0, 0, 0, 0, false, false, 0, LEADER_MACHINE, 0, 0);
  ///
  create_per_qp_meta(&qp_meta[R_QP_ID], MAX_R_WRS, MAX_RECV_R_REP_WRS, SEND_UNI_REQ_RECV_LDR_REP, 1, 1,
                     R_CREDITS, R_REP_RECV_SIZE, R_SEND_SIZE, false, false, 0, LEADER_MACHINE, R_FIFO_SIZE, R_CREDITS);

  set_up_ctx(ctx);

  /* ---------------------------------------------------------------------------
  ------------------------------PREPOST_RECVS-------------------------------
  ---------------------------------------------------------------------------*/

  post_recvs_with_recv_info(qp_meta[PREP_ACK_QP_ID].recv_info,
                            qp_meta[PREP_ACK_QP_ID].recv_wr_num);

  post_recvs_with_recv_info(qp_meta[COMMIT_W_QP_ID].recv_info,
                            qp_meta[COMMIT_W_QP_ID].recv_wr_num);

  /* -----------------------------------------------------
  --------------CONNECT WITH LEADER-----------------------
  ---------------------------------------------------------*/
  setup_connections_and_spawn_stats_thread(g_id, ctx->cb, t_id);
  // We can set up the send work requests now that
  // we have address handles for remote machines
  init_ctx_send_wrs(ctx);

  /* -----------------------------------------------------
  --------------DECLARATIONS------------------------------
  ---------------------------------------------------------*/

  uint32_t credit_debug_cnt = 0, outstanding_writes = 0, trace_iter = 0;

  latency_info_t latency_info = {
    .measured_req_flag = NO_REQ,
    .measured_sess_id = 0,
  };


  zk_resp_t *resp = (zk_resp_t *) calloc(ZK_TRACE_BATCH, sizeof(zk_resp_t));
  zk_trace_op_t *ops = (zk_trace_op_t *) calloc(ZK_TRACE_BATCH, sizeof(zk_trace_op_t));
  p_writes_t *p_writes = set_up_pending_writes(ctx, FLR_PENDING_WRITES, protocol);
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

  struct fifo *remote_w_buf;
  init_fifo(&remote_w_buf, LEADER_W_BUF_SLOTS * sizeof(uint16_t), 1);
  struct fifo *prep_buf_mirror;
  init_fifo(&prep_buf_mirror, FLR_PREP_BUF_SLOTS * sizeof(uint16_t), 1);

  /* ---------------------------------------------------------------------------
  ------------------------------INITIALIZE STATIC STRUCTURES--------------------
    ---------------------------------------------------------------------------*/

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
    trace_iter = zk_batch_from_trace_to_KVS(ctx, trace_iter, t_id, trace, ops, flr_id,
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
