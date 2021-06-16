#include <od_rdma_gen_util.h>
#include "../../../odlib/include/trace/od_trace_util.h"
#include <zk_inline_util.h>
#include "zk_util.h"

atomic_uint_fast64_t global_w_id, committed_global_w_id;


void zk_print_parameters_in_the_start()
{
  emphatic_print(green, "ZOOKEEPER");
  if (ENABLE_ASSERTIONS) {
    my_printf(green, "COMMIT: commit message %lu/%d, commit message ud req %llu/%d\n",
              sizeof(ctx_com_mes_t), CTX_COM_SEND_SIZE,
              sizeof(ctx_com_mes_ud_t), CTX_COM_RECV_SIZE);
    my_printf(cyan, "ACK: ack message %lu/%d, ack message ud req %llu/%d\n",
              sizeof(ctx_ack_mes_t), FLR_ACK_SEND_SIZE,
              sizeof(ctx_ack_mes_ud_t), LDR_ACK_RECV_SIZE);
    my_printf(yellow, "PREPARE: prepare %lu/%d, prep message %lu/%d, prep message ud req %llu/%d\n",
              sizeof(zk_prepare_t), PREP_SIZE,
              sizeof(zk_prep_mes_t), LDR_PREP_SEND_SIZE,
              sizeof(zk_prep_mes_ud_t), FLR_PREP_RECV_SIZE);
    my_printf(cyan, "Write: write %lu/%d, write message %lu/%d, write message ud req %llu/%d\n",
              sizeof(zk_write_t), W_SIZE,
              sizeof(zk_w_mes_t), FLR_W_SEND_SIZE,
              sizeof(zk_w_mes_ud_t), LDR_W_RECV_SIZE);

    my_printf(green, "LEADER PREPARE INLINING %d, LEADER PENDING WRITES %d \n",
              LEADER_PREPARE_ENABLE_INLINING, LEADER_PENDING_WRITES);
    my_printf(green, "FOLLOWER WRITE INLINING %d, FOLLOWER WRITE FIFO SIZE %d \n",
              FLR_W_ENABLE_INLINING, W_FIFO_SIZE);
    my_printf(cyan, "PREPARE CREDITS %d, FLR PREPARE BUF SLOTS %d, FLR PREPARE BUF SIZE %d\n",
              PREPARE_CREDITS, FLR_PREP_BUF_SLOTS, FLR_PREP_BUF_SIZE);

    my_printf(yellow, "Using Quorom %d , Quorum Machines %d \n", USE_QUORUM, LDR_QUORUM_OF_ACKS);
  }
}

void zk_static_assert_compile_parameters()
{
  static_assert(COMPILED_SYSTEM == zookeeper_sys, " ");
  static_assert(sizeof(zk_w_mes_t) == FLR_W_SEND_SIZE, " ");
  static_assert(sizeof(zk_r_mes_t) == R_MES_SIZE, " ");
  static_assert(sizeof(zk_read_t) == R_SIZE, " ");
  static_assert(sizeof(zk_r_mes_t) == R_MES_HEADER + (R_SIZE * R_COALESCE), " ");
  static_assert(sizeof(zk_write_t) == W_SIZE, "");
  static_assert(sizeof(zk_prepare_t) == PREP_SIZE, "");

  static_assert(sizeof(zk_r_rep_big_t) == R_REP_BIG_SIZE, " ");

  //if (ENABLE_MULTICAST) assert(MCAST_FLR_RECV_QP_NUM == MCAST_GROUPS_PER_FLOW);
  assert(LEADER_MACHINE < MACHINE_NUM);
  assert(LEADER_PENDING_WRITES >= SESSIONS_PER_THREAD);
  assert(sizeof(struct key) == KEY_SIZE);
  assert(LEADERS_PER_MACHINE == FOLLOWERS_PER_MACHINE); // hopefully temporary restriction
  assert((W_CREDITS % LDR_CREDIT_DIVIDER) == 0); // division better be perfect
  assert((COMMIT_CREDITS % FLR_CREDIT_DIVIDER) == 0); // division better be perfect
  assert(sizeof(ctx_ack_mes_ud_t) == LDR_ACK_RECV_SIZE);
  assert(sizeof(ctx_com_mes_ud_t) == CTX_COM_RECV_SIZE);
  assert(sizeof(zk_prep_mes_ud_t) == FLR_PREP_RECV_SIZE);
  assert(sizeof(zk_w_mes_ud_t) == LDR_W_RECV_SIZE);
  assert(SESSIONS_PER_THREAD < M_16);
  assert(FLR_MAX_RECV_COM_WRS >= FLR_CREDITS_IN_MESSAGE);
  if (write_ratio > 0) assert(ZK_UPDATE_BATCH >= LEADER_PENDING_WRITES);

  if (PUT_A_MACHINE_TO_SLEEP) assert(MACHINE_THAT_SLEEPS != LEADER_MACHINE);


//
//  my_printf(yellow, "WRITE: capacity of write recv slot %d capacity of w_message %lu , "
//           "value capacity %d, capacity of cache op %lu , sizeof udreq w message %lu \n",
//         LDR_W_RECV_SIZE, sizeof(zk_w_mes_t), VALUE_SIZE,
//         sizeof(struct cache_op), sizeof(zk_w_mes_ud_t));
  assert(sizeof(zk_w_mes_ud_t) == LDR_W_RECV_SIZE);
  assert(sizeof(zk_w_mes_t) == FLR_W_SEND_SIZE);
}

void zk_init_globals()
{
  global_w_id = 1; // DO not start from 0, because when checking for acks there is a non-zero test
  committed_global_w_id = 0;
}


void dump_stats_2_file(struct stats* st){
    uint8_t typeNo = LEADER;
    assert(typeNo >=0 && typeNo <=3);
    int i = 0;
    char filename[128];
    FILE *fp;
    double total_MIOPS;
    char* path = "../../results/scattered-results";

    sprintf(filename, "%s/%s_s_%d__v_%d_m_%d_l_%d_f_%d_r_%d-%d.csv", path,
            "ZK",
            SESSIONS_PER_THREAD,
            USE_BIG_OBJECTS == 1 ? ((EXTRA_CACHE_LINES * 64) + BASE_VALUE_SIZE): BASE_VALUE_SIZE,
            MACHINE_NUM, LEADERS_PER_MACHINE,
            FOLLOWERS_PER_MACHINE, write_ratio,
            machine_id);
    printf("%s\n", filename);
    fp = fopen(filename, "w"); // "w" means that we are going to write on this file
    fprintf(fp, "machine_id: %d\n", machine_id);

    fprintf(fp, "comment: thread ID, total MIOPS,"
            "preps sent, coms sent, acks sent, "
            "received preps, received coms, received acks\n");
    for(i = 0; i < WORKERS_PER_MACHINE; ++i){
        total_MIOPS = st->total_reqs[i];
        fprintf(fp, "client: %d, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f\n",
                i, total_MIOPS, st->total_reqs[i], st->preps_sent[i],
                st->coms_sent[i], st->acks_sent[i],
                st->received_preps[i],st->received_coms[i],
                st->received_acks[i]);
    }

    fclose(fp);
}


void zk_ldr_qp_meta_init(per_qp_meta_t *qp_meta)
{
  ///
  create_per_qp_meta(&qp_meta[PREP_ACK_QP_ID], LDR_MAX_PREP_WRS,
                     LDR_MAX_RECV_ACK_WRS, SEND_BCAST_LDR_RECV_UNI, RECV_REPLY,
                     PREP_ACK_QP_ID,
                     FOLLOWER_MACHINE_NUM, FOLLOWER_MACHINE_NUM, LEADER_ACK_BUF_SLOTS,
                     LDR_ACK_RECV_SIZE, LDR_PREP_SEND_SIZE, ENABLE_MULTICAST, false,
                     PREP_MCAST_QP, LEADER_MACHINE, PREP_FIFO_SIZE,
                     PREPARE_CREDITS, PREP_MES_HEADER,
                     "send preps", "recv acks");
  ///
  create_per_qp_meta(&qp_meta[COMMIT_W_QP_ID], LDR_MAX_COM_WRS,
                     LDR_MAX_RECV_W_WRS, SEND_BCAST_LDR_RECV_UNI, RECV_REQ,
                     COMMIT_W_QP_ID,
                     FOLLOWER_MACHINE_NUM, FOLLOWER_MACHINE_NUM, LEADER_W_BUF_SLOTS,
                     LDR_W_RECV_SIZE, CTX_COM_SEND_SIZE, ENABLE_MULTICAST, false,
                     COM_MCAST_QP, LEADER_MACHINE, COMMIT_FIFO_SIZE,
                     COMMIT_CREDITS, CTX_COM_SEND_SIZE,
                     "send commits", "recv writes");
  ///
  create_per_qp_meta(&qp_meta[R_QP_ID], MAX_R_REP_WRS, MAX_RECV_R_WRS, SEND_UNI_REP_LDR_RECV_UNI_REQ, RECV_REQ,
                     R_QP_ID,
                     FOLLOWER_MACHINE_NUM, FOLLOWER_MACHINE_NUM, LEADER_R_BUF_SLOTS,
                     R_RECV_SIZE, R_REP_SEND_SIZE, false, false,
                     0, LEADER_MACHINE, R_REP_FIFO_SIZE, 0, R_REP_MES_HEADER,
                     "send r_Reps", "recv reads");
}


void zk_flr_qp_meta_init(per_qp_meta_t *qp_meta)
{

  ///
  create_per_qp_meta(&qp_meta[PREP_ACK_QP_ID], FLR_MAX_ACK_WRS,
                     FLR_MAX_RECV_PREP_WRS, SEND_UNI_REP_RECV_LDR_BCAST, RECV_REQ,
                     PREP_ACK_QP_ID,
                     1, 1, 3 * PREPARE_CREDITS,
                     FLR_PREP_RECV_SIZE, FLR_ACK_SEND_SIZE, false, ENABLE_MULTICAST, PREP_MCAST_QP,
                     LEADER_MACHINE, ACK_SEND_BUF_SIZE, 0, 0, "send acks", "recv preps");
  ///
  create_per_qp_meta(&qp_meta[COMMIT_W_QP_ID], FLR_MAX_W_WRS,
                     FLR_MAX_RECV_COM_WRS, SEND_UNI_REP_RECV_LDR_BCAST, RECV_SEC_ROUND,
                     COMMIT_W_QP_ID,
                     1, 1, COMMIT_CREDITS,
                     CTX_COM_RECV_SIZE, FLR_W_SEND_SIZE, false, ENABLE_MULTICAST, COM_MCAST_QP,
                     LEADER_MACHINE, W_FIFO_SIZE, W_CREDITS, W_MES_HEADER,
                     "send writes", "recv commits");
  ///
  create_per_qp_meta(&qp_meta[R_QP_ID], MAX_R_WRS, MAX_RECV_R_REP_WRS, SEND_UNI_REQ_RECV_LDR_REP, RECV_REPLY,
                     R_QP_ID, 1, 1,
                     R_CREDITS, R_REP_RECV_SIZE, R_SEND_SIZE, false, false, 0, LEADER_MACHINE, R_FIFO_SIZE,
                     R_CREDITS, R_MES_HEADER,
                     "send reads", "recv read_replies");
}



void zk_ldr_qp_meta_mfs(context_t *ctx)
{
  mf_t *mfs = calloc(QP_NUM, sizeof(mf_t));

  mfs[PREP_ACK_QP_ID].recv_handler = ack_handler;
  mfs[PREP_ACK_QP_ID].send_helper = send_prepares_helper;
  mfs[PREP_ACK_QP_ID].insert_helper = insert_prep_help;
  mfs[PREP_ACK_QP_ID].polling_debug = zk_debug_info_bookkeep;

  mfs[COMMIT_W_QP_ID].recv_handler = write_handler;
  mfs[COMMIT_W_QP_ID].send_helper = send_commits_helper;
  mfs[COMMIT_W_QP_ID].polling_debug = zk_debug_info_bookkeep;

  mfs[R_QP_ID].recv_handler = r_handler;
  mfs[R_QP_ID].send_helper = send_r_reps_helper;
  mfs[R_QP_ID].recv_kvs = zk_KVS_batch_op_reads;
  mfs[R_QP_ID].insert_helper = insert_r_rep_help;
  mfs[R_QP_ID].polling_debug = zk_debug_info_bookkeep;



  ctx_set_qp_meta_mfs(ctx, mfs);
  free(mfs);
}


void zk_flr_qp_meta_mfs(context_t *ctx)
{
  mf_t *mfs = calloc(QP_NUM, sizeof(mf_t));

  mfs[PREP_ACK_QP_ID].send_helper = send_acks_helper;
  mfs[PREP_ACK_QP_ID].recv_handler = prepare_handler;
  mfs[PREP_ACK_QP_ID].polling_debug = zk_debug_info_bookkeep;

  mfs[COMMIT_W_QP_ID].recv_handler = commit_handler;
  mfs[COMMIT_W_QP_ID].send_helper = send_writes_helper;
  mfs[COMMIT_W_QP_ID].insert_helper = insert_write_help;
  mfs[COMMIT_W_QP_ID].polling_debug = zk_debug_info_bookkeep;

  mfs[R_QP_ID].recv_handler = r_rep_handler;
  mfs[R_QP_ID].send_helper = send_reads_helper;
  mfs[R_QP_ID].insert_helper = insert_read_help;
  mfs[R_QP_ID].polling_debug = zk_debug_info_bookkeep;


  ctx_set_qp_meta_mfs(ctx, mfs);
  free(mfs);
}

void zk_init_ldr_send_fifos(context_t *ctx)
{
  fifo_t *send_fifo = ctx->qp_meta[COMMIT_W_QP_ID].send_fifo;
  ctx_com_mes_t *commits = (ctx_com_mes_t *) send_fifo->fifo;

  for (uint32_t i = 0; i < COMMIT_FIFO_SIZE; i++) {
    commits[i].opcode = KVS_OP_PUT;
  }

  zk_prep_mes_t *preps = (zk_prep_mes_t *) ctx->qp_meta[PREP_ACK_QP_ID].send_fifo->fifo;
  for (int i = 0; i < PREP_FIFO_SIZE; i++) {
    preps[i].opcode = KVS_OP_PUT;
    for (uint16_t j = 0; j < MAX_PREP_COALESCE; j++) {
      preps[i].prepare[j].opcode = KVS_OP_PUT;
      preps[i].prepare[j].val_len = VALUE_SIZE >> SHIFT_BITS;
    }
  }
}

void zk_init_flr_send_fifos(context_t *ctx)
{
  zk_w_mes_t *writes = (zk_w_mes_t *) ctx->qp_meta[COMMIT_W_QP_ID].send_fifo->fifo;
  for (uint16_t i = 0; i < W_FIFO_SIZE; i++) {
    for (uint16_t j = 0; j < MAX_W_COALESCE; j++) {
      writes[i].write[j].opcode = KVS_OP_PUT;
      writes[i].write[j].val_len = VALUE_SIZE >> SHIFT_BITS;
    }
  }

  zk_r_mes_t *r_mes = (zk_r_mes_t *) ctx->qp_meta[R_QP_ID].send_fifo->fifo;
  for (uint16_t i = 0; i < R_FIFO_SIZE; i++) {
    for (uint16_t j = 0; j < R_COALESCE; j++) {
      r_mes[i].read[j].opcode = KVS_OP_GET;
      r_mes[i].m_id = ctx->m_id;
    }
  }
  per_qp_meta_t *prep_ack_qp_meta = &ctx->qp_meta[PREP_ACK_QP_ID];
  ctx_ack_mes_t * acks = (ctx_ack_mes_t *) prep_ack_qp_meta->send_fifo->fifo; //calloc(1, sizeof(ctx_ack_mes_t));
  for (int m_id = 0; m_id < prep_ack_qp_meta->send_wr_num; ++m_id) {
    acks[m_id].opcode = OP_ACK;
    acks[m_id].m_id = ctx->m_id;
    prep_ack_qp_meta->send_sgl[m_id].addr = (uintptr_t) &acks[m_id];
  }
}

void zk_init_qp_meta(context_t *ctx)
{
  protocol_t protocol = machine_id == LEADER_MACHINE ? LEADER : FOLLOWER;
  per_qp_meta_t *qp_meta = ctx->qp_meta;
  switch (protocol) {
    case FOLLOWER:
      zk_flr_qp_meta_init(qp_meta);
      zk_flr_qp_meta_mfs(ctx);
      zk_init_flr_send_fifos(ctx);
      ctx_qp_meta_mirror_buffers(&qp_meta[COMMIT_W_QP_ID],
                                 LEADER_W_BUF_SLOTS, 1);
      /// We are mirroring the local prep buffer because
      /// we want to use it as the buffer to be sent to the KVS,
      /// We can only go to the KVS after a commit has been received.
      /// the problem is that at that point we don't know how many of the writes are coalesced,
      /// i.e. we dont know how many recv-buffer-slots are actually being freed.
      /// We need that information because in order to avoid overwriting the recv-buffer we need to stop polling
      /// when we reach a threshold, so that we wont send any more acks.
      ctx_qp_meta_mirror_buffers(&qp_meta[PREP_ACK_QP_ID],
                                 FLR_PREP_BUF_SLOTS, 1);

      break;
    case LEADER:
      zk_ldr_qp_meta_init(qp_meta);
      zk_ldr_qp_meta_mfs(ctx);
      zk_init_ldr_send_fifos(ctx);
      break;
    default: assert(false);
  }


}

// Set up a struct that stores pending writes
zk_ctx_t *set_up_zk_ctx(context_t *ctx)
{
  per_qp_meta_t *prep_ack_qp_meta = &ctx->qp_meta[PREP_ACK_QP_ID];
  protocol_t protocol = machine_id == LEADER_MACHINE ? LEADER : FOLLOWER;
  uint32_t i;
  zk_ctx_t* zk_ctx = (zk_ctx_t*) calloc(1,sizeof(zk_ctx_t));
  uint32_t size = protocol == LEADER ? LEADER_PENDING_WRITES : FLR_PENDING_WRITES;
  zk_ctx->protocol = protocol;

  zk_ctx->w_rob = fifo_constructor(size, sizeof(w_rob_t), false, 0, 1);
  zk_ctx->index_to_req_array = (uint32_t *) calloc(SESSIONS_PER_THREAD, sizeof(uint32_t));

  zk_ctx->stalled = (bool *) malloc(SESSIONS_PER_THREAD * sizeof(bool));

  zk_ctx->ops = (ctx_trace_op_t *) calloc((size_t) ZK_TRACE_BATCH, sizeof(ctx_trace_op_t));
  zk_ctx->resp = (zk_resp_t*) calloc((size_t) ZK_TRACE_BATCH, sizeof(zk_resp_t));
  for(i = 0; i <  ZK_TRACE_BATCH; i++) zk_ctx->resp[i].type = EMPTY;

  for (i = 0; i < SESSIONS_PER_THREAD; i++) zk_ctx->stalled[i] = false;
  for (i = 0; i < size; i++) {
    w_rob_t *w_rob = (w_rob_t *) get_fifo_slot(zk_ctx->w_rob, i);
    w_rob->w_state = INVALID;
    w_rob->g_id = 0;
  }
  if (protocol == LEADER) {
    zk_ctx->ptrs_to_r = calloc(1, sizeof(ptrs_to_r_t));
    zk_ctx->ptrs_to_r->ptr_to_ops = malloc(LDR_MAX_INCOMING_R * sizeof(zk_read_t*));
    zk_ctx->ptrs_to_r->ptr_to_r_mes = malloc(LDR_MAX_INCOMING_R * sizeof(zk_r_mes_t*));
    zk_ctx->ptrs_to_r->coalesce_r_rep = malloc(LDR_MAX_INCOMING_R * sizeof(bool));

  }
  else { // PROTOCOL == FOLLOWER
    zk_ctx->r_rob = fifo_constructor(FLR_PENDING_READS, sizeof(r_rob_t), false, 0, 1);
  }

  if (!ENABLE_CLIENTS)
    zk_ctx->trace = trace_init(ctx->t_id);


  return zk_ctx;
}







