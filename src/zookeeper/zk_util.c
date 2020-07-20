#include <rdma_gen_util.h>
#include "zk_util.h"


void zk_print_parameters_in_the_start()
{
  my_printf(green, "---------------------------------------------------------- \n");
  my_printf(green, "------------------------ZOOKEEPER------------------------- \n");
  my_printf(green, "---------------------------------------------------------- \n");
  if (ENABLE_ASSERTIONS) {
    my_printf(green, "COMMIT: commit message %lu/%d, commit message ud req %llu/%d\n",
              sizeof(zk_com_mes_t), LDR_COM_SEND_SIZE,
              sizeof(zk_com_mes_ud_t), FLR_COM_RECV_SIZE);
    my_printf(cyan, "ACK: ack message %lu/%d, ack message ud req %llu/%d\n",
              sizeof(zk_ack_mes_t), FLR_ACK_SEND_SIZE,
              sizeof(zk_ack_mes_ud_t), LDR_ACK_RECV_SIZE);
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
  static_assert(sizeof(zk_w_mes_t) == FLR_W_SEND_SIZE, " ");
  static_assert(sizeof(zk_r_mes_t) == R_MES_SIZE, " ");
  static_assert(sizeof(zk_read_t) == R_SIZE, " ");
  static_assert(sizeof(zk_r_mes_t) == R_MES_HEADER + (R_SIZE * R_COALESCE), " ");

  static_assert(sizeof(zk_r_rep_big_t) == R_REP_BIG_SIZE, " ");

  //if (ENABLE_MULTICAST) assert(MCAST_FLR_RECV_QP_NUM == MCAST_GROUPS_PER_FLOW);
  assert(LEADER_MACHINE < MACHINE_NUM);
  assert(LEADER_PENDING_WRITES >= SESSIONS_PER_THREAD);
  assert(sizeof(struct key) == KEY_SIZE);
  assert(LEADERS_PER_MACHINE == FOLLOWERS_PER_MACHINE); // hopefully temporary restriction
  assert((W_CREDITS % LDR_CREDIT_DIVIDER) == 0); // division better be perfect
  assert((COMMIT_CREDITS % FLR_CREDIT_DIVIDER) == 0); // division better be perfect
  assert(sizeof(zk_ack_mes_ud_t) == LDR_ACK_RECV_SIZE);
  assert(sizeof(zk_com_mes_ud_t) == FLR_COM_RECV_SIZE);
  assert(sizeof(zk_prep_mes_ud_t) == FLR_PREP_RECV_SIZE);
  assert(sizeof(zk_w_mes_ud_t) == LDR_W_RECV_SIZE);
  assert(SESSIONS_PER_THREAD < M_16);
  assert(FLR_MAX_RECV_COM_WRS >= FLR_CREDITS_IN_MESSAGE);
  if (WRITE_RATIO > 0) assert(ZK_UPDATE_BATCH >= LEADER_PENDING_WRITES);

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
            FOLLOWERS_PER_MACHINE, WRITE_RATIO,
            machine_id);
    printf("%s\n", filename);
    fp = fopen(filename, "w"); // "w" means that we are going to write on this file
    fprintf(fp, "machine_id: %d\n", machine_id);

    fprintf(fp, "comment: thread ID, total MIOPS,"
            "preps sent, coms sent, acks sent, "
            "received preps, received coms, received acks\n");
    for(i = 0; i < WORKERS_PER_MACHINE; ++i){
        total_MIOPS = st->cache_hits_per_thread[i];
        fprintf(fp, "client: %d, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f\n",
                i, total_MIOPS, st->cache_hits_per_thread[i], st->preps_sent[i],
                st->coms_sent[i], st->acks_sent[i],
                st->received_preps[i],st->received_coms[i],
                st->received_acks[i]);
    }

    fclose(fp);
}


/* ---------------------------------------------------------------------------
------------------------------LEADER --------------------------------------
---------------------------------------------------------------------------*/
void init_fifo(struct fifo **fifo, uint32_t max_size, uint32_t fifos_num)
{
  (*fifo) = (struct fifo *) malloc(fifos_num * sizeof(struct fifo));
  memset((*fifo), 0, fifos_num *  sizeof(struct fifo));
  for (int i = 0; i < fifos_num; ++i) {
    (*fifo)[i].fifo = malloc(max_size);
    memset((*fifo)[i].fifo, 0, max_size);
  }
}

// Initialize the quorum info that contains the system configuration
quorum_info_t* set_up_q_info(context_t *ctx)
{
  quorum_info_t * q_info = (quorum_info_t *) calloc(1, sizeof(quorum_info_t));
  q_info->active_num = REM_MACH_NUM;
  q_info->first_active_rm_id = 0;
  q_info->last_active_rm_id = REM_MACH_NUM - 1;
  for (uint8_t i = 0; i < REM_MACH_NUM; i++) {
    uint8_t m_id = i < machine_id ? i : (uint8_t) (i + 1);
    q_info->active_ids[i] = m_id;
    q_info->send_vector[i] = true;
  }

  q_info->num_of_send_wrs = Q_INFO_NUM_SEND_WRS;
  q_info->send_wrs_ptrs = (struct ibv_send_wr **) malloc(Q_INFO_NUM_SEND_WRS * sizeof(struct ibv_send_wr *));
  q_info->send_wrs_ptrs[0] = ctx->qp_meta[PREP_ACK_QP_ID].send_wr;
  q_info->send_wrs_ptrs[1] = ctx->qp_meta[COMMIT_W_QP_ID].send_wr;

  q_info->num_of_credit_targets = Q_INFO_CREDIT_TARGETS;
  q_info->targets = malloc (q_info->num_of_credit_targets * sizeof(uint16_t));
  q_info->targets[0] = W_CREDITS;
  q_info->targets[1] = COMMIT_CREDITS;
  q_info->credit_ptrs = malloc(q_info->num_of_credit_targets * sizeof(uint16_t*));
  q_info->credit_ptrs[0] = ctx->qp_meta[PREP_ACK_QP_ID].credits;
  q_info->credit_ptrs[1] = ctx->qp_meta[COMMIT_W_QP_ID].credits;
  return q_info;

}


// Set up a struct that stores pending writes
zk_ctx_t *set_up_pending_writes(context_t *ctx, protocol_t protocol)
{

  int i;
  zk_ctx_t* zk_ctx = (zk_ctx_t*) calloc(1,sizeof(zk_ctx_t));
  zk_ctx->q_info = protocol == LEADER ? set_up_q_info(ctx) : NULL;
  uint32_t size = protocol == LEADER ? LEADER_PENDING_WRITES : FLR_PENDING_WRITES;
  zk_ctx->protocol = protocol;


  zk_ctx->g_id = (uint64_t *) malloc(size * sizeof(uint64_t));
  zk_ctx->w_state = (enum op_state *) malloc(size * sizeof(enum op_state));
  zk_ctx->session_id = (uint32_t *) calloc(size, sizeof(uint32_t));
  zk_ctx->acks_seen = (uint8_t *) calloc(size, sizeof(uint8_t));
  zk_ctx->index_to_req_array = (uint32_t *) calloc(SESSIONS_PER_THREAD, sizeof(uint32_t));

  zk_ctx->flr_id = (uint8_t *) malloc(size * sizeof(uint8_t));
  zk_ctx->is_local = (bool *) malloc(size * sizeof(bool));
  zk_ctx->stalled = (bool *) malloc(SESSIONS_PER_THREAD * sizeof(bool));
  zk_ctx->ptrs_to_ops = (zk_prepare_t **) malloc(size * sizeof(zk_prepare_t *));
  //if (protocol == FOLLOWER) init_fifo(&(zk_ctx->w_fifo), W_FIFO_SIZE * sizeof(zk_w_mes_t), 1);
  memset(zk_ctx->g_id, 0, size * sizeof(uint64_t));
  //zk_ctx->prep_fifo = (zk_prep_fifo_t *) calloc(1, sizeof(zk_prep_fifo_t));
  //  zk_ctx->prep_fifo->prep_message =
  //  (zk_prep_mes_t *) calloc(PREP_FIFO_SIZE, sizeof(zk_prep_mes_t));
  //assert(zk_ctx->prep_fifo != NULL);
  zk_ctx->ops = (zk_trace_op_t *) calloc((size_t) ZK_TRACE_BATCH, sizeof(zk_trace_op_t));
  zk_ctx->resp = (zk_resp_t*) calloc((size_t) ZK_TRACE_BATCH, sizeof(zk_resp_t));
  for(int i = 0; i <  ZK_TRACE_BATCH; i++) zk_ctx->resp[i].type = EMPTY;

  for (i = 0; i < SESSIONS_PER_THREAD; i++) zk_ctx->stalled[i] = false;
  for (i = 0; i < size; i++) {
    zk_ctx->w_state[i] = INVALID;
  }
  if (protocol == LEADER) {
    zk_ctx->ptrs_to_r = calloc(1, sizeof(ptrs_to_r_t));
    zk_ctx->ptrs_to_r->ptr_to_ops = malloc(LDR_MAX_INCOMING_R * sizeof(zk_read_t*));
    zk_ctx->ptrs_to_r->ptr_to_r_mes = malloc(LDR_MAX_INCOMING_R * sizeof(zk_r_mes_t*));
    zk_ctx->ptrs_to_r->coalesce_r_rep = malloc(LDR_MAX_INCOMING_R * sizeof(bool));
  }
  else { // PROTOCOL == FOLLOWER
    zk_ctx->ack = (zk_ack_mes_t *) calloc(1, sizeof(zk_ack_mes_t));
    zk_ctx->p_acks = (p_acks_t *) calloc(1, sizeof(p_acks_t));
    zk_ctx->r_meta = fifo_constructor(FLR_PENDING_READS, sizeof(r_meta_t), false, 0);
  }
  return zk_ctx;
}


void zk_init_send_fifos(context_t *ctx)
{

  zk_ctx_t * zk_ctx = (zk_ctx_t *)ctx->appl_ctx;
  if (zk_ctx->protocol == LEADER) {
    zk_com_mes_t *commits = (zk_com_mes_t *) ctx->qp_meta[COMMIT_W_QP_ID].send_fifo->fifo;

    for (int i = 0; i < COMMIT_FIFO_SIZE; i++) {
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
  else {
    zk_ctx->w_fifo = ctx->qp_meta[COMMIT_W_QP_ID].send_fifo;
    zk_w_mes_t *writes = (zk_w_mes_t *) zk_ctx->w_fifo->fifo;
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
  }
}





// Prepost Receives on the Leader Side
// Post receives for the coherence traffic in the init phase
void pre_post_recvs(uint32_t* push_ptr, struct ibv_qp *recv_qp, uint32_t lkey, void* buf,
                    uint32_t max_reqs, uint32_t number_of_recvs, uint16_t QP_ID, uint32_t message_size)
{
  uint32_t i;//, j;
  for(i = 0; i < number_of_recvs; i++) {
        hrd_post_dgram_recv(recv_qp,	(buf + *push_ptr * message_size),
                            message_size, lkey);
      MOD_INCR(*push_ptr, max_reqs);
  }
}




void check_protocol(int protocol)
{
    if (protocol != FOLLOWER && protocol != LEADER) {
        my_printf(red, "Wrong protocol specified when setting up the queue depths %d \n", protocol);
        assert(false);
    }
}





