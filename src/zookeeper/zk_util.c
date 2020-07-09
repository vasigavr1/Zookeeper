#include <rdma_gen_util.h>
#include "zk_util.h"


void zk_print_parameters_in_the_start()
{
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

void zk_static_assert_compile_parameters()
{
  //static_assert(!ENABLE_CLIENTS, " ");

  if (ENABLE_MULTICAST) assert(MCAST_QP_NUM == MCAST_GROUPS_NUM);
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
//  my_printf(yellow, "WRITE: size of write recv slot %d size of w_message %lu , "
//           "value size %d, size of cache op %lu , sizeof udreq w message %lu \n",
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
// construct a prep_message-- max_size must be in bytes
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
quorum_info_t* set_up_q_info(struct ibv_send_wr *prep_send_wr,
                             struct ibv_send_wr *com_send_wr,
                             uint16_t credits[][MACHINE_NUM])
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
  q_info->send_wrs_ptrs[0] = prep_send_wr;
  q_info->send_wrs_ptrs[1] = com_send_wr;

  q_info->num_of_credit_targets = Q_INFO_CREDIT_TARGETS;
  q_info->targets = malloc (q_info->num_of_credit_targets * sizeof(uint16_t));
  q_info->targets[0] = W_CREDITS;
  q_info->targets[1] = COMMIT_CREDITS;
  q_info->credit_ptrs = malloc(q_info->num_of_credit_targets * sizeof(uint16_t*));
  q_info->credit_ptrs[0] = credits[PREP_VC];
  q_info->credit_ptrs[1] = credits[COMM_VC];
  return q_info;

}


// Set up a struct that stores pending writes
p_writes_t* set_up_pending_writes(uint32_t size, struct ibv_send_wr *prep_send_wr,
                                  struct ibv_send_wr *com_send_wr,
                                  uint16_t credits[][MACHINE_NUM],
                                  protocol_t protocol)
{
  int i;
  p_writes_t* p_writes = (p_writes_t*) calloc(1,sizeof(p_writes_t));
  p_writes->q_info = protocol == LEADER ? set_up_q_info(prep_send_wr, com_send_wr, credits) : NULL;


  p_writes->g_id = (uint64_t *) malloc(size * sizeof(uint64_t));
  p_writes->w_state = (enum write_state *) malloc(size * sizeof(enum write_state));
  p_writes->session_id = (uint32_t *) calloc(size, sizeof(uint32_t));
  p_writes->acks_seen = (uint8_t *) calloc(size, sizeof(uint8_t));
  p_writes->w_index_to_req_array = (uint32_t *) calloc(SESSIONS_PER_THREAD, sizeof(uint32_t));

  p_writes->flr_id = (uint8_t *) malloc(size * sizeof(uint8_t));
  p_writes->is_local = (bool *) malloc(size * sizeof(bool));
  p_writes->stalled = (bool *) malloc(SESSIONS_PER_THREAD * sizeof(bool));
  p_writes->ptrs_to_ops = (zk_prepare_t **) malloc(size * sizeof(zk_prepare_t *));
  if (protocol == FOLLOWER) init_fifo(&(p_writes->w_fifo), W_FIFO_SIZE * sizeof(zk_w_mes_t), 1);
  memset(p_writes->g_id, 0, size * sizeof(uint64_t));
  p_writes->prep_fifo = (zk_prep_fifo_t *) calloc(1, sizeof(zk_prep_fifo_t));
    p_writes->prep_fifo->prep_message =
    (zk_prep_mes_t *) calloc(PREP_FIFO_SIZE, sizeof(zk_prep_mes_t));
  assert(p_writes->prep_fifo != NULL);
  for (i = 0; i < SESSIONS_PER_THREAD; i++) p_writes->stalled[i] = false;
  for (i = 0; i < size; i++) {
    p_writes->w_state[i] = INVALID;
  }
  if (protocol == LEADER) {
    zk_prep_mes_t *preps = p_writes->prep_fifo->prep_message;
    for (i = 0; i < PREP_FIFO_SIZE; i++) {
      preps[i].opcode = KVS_OP_PUT;
      for (uint16_t j = 0; j < MAX_PREP_COALESCE; j++) {
        preps[i].prepare[j].opcode = KVS_OP_PUT;
        preps[i].prepare[j].val_len = VALUE_SIZE >> SHIFT_BITS;
      }
    }
  } else { // PROTOCOL == FOLLOWER
    zk_w_mes_t *writes = (zk_w_mes_t *) p_writes->w_fifo->fifo;
    for (i = 0; i < W_FIFO_SIZE; i++) {
      for (uint16_t j = 0; j < MAX_W_COALESCE; j++) {
        writes[i].write[j].opcode = KVS_OP_PUT;
        writes[i].write[j].val_len = VALUE_SIZE >> SHIFT_BITS;
      }
    }
  }
  return p_writes;
}



// set the different queue depths for client's queue pairs
void set_up_queue_depths_ldr_flr(int** recv_q_depths, int** send_q_depths, int protocol)
{
  /* -------LEADER-------------
  * 1st Dgram send Prepares -- receive ACKs
  * 2nd Dgram send Commits  -- receive Writes
  * 3rd Dgram  receive Credits
  *
    * ------FOLLOWER-----------
  * 1st Dgram receive prepares -- send Acks
  * 2nd Dgram receive Commits  -- send Writes
  * 3rd Dgram  send Credits
  * */
  if (protocol == FOLLOWER) {
    *send_q_depths = malloc(FOLLOWER_QP_NUM * sizeof(int));
    *recv_q_depths = malloc(FOLLOWER_QP_NUM * sizeof(int));
    (*recv_q_depths)[PREP_ACK_QP_ID] = ENABLE_MULTICAST == 1? 1 : FLR_RECV_PREP_Q_DEPTH;
    (*recv_q_depths)[COMMIT_W_QP_ID] = ENABLE_MULTICAST == 1? 1 : FLR_RECV_COM_Q_DEPTH;
    (*recv_q_depths)[FC_QP_ID] = FLR_RECV_CR_Q_DEPTH;
    (*send_q_depths)[PREP_ACK_QP_ID] = FLR_SEND_ACK_Q_DEPTH;
    (*send_q_depths)[COMMIT_W_QP_ID] = FLR_SEND_W_Q_DEPTH;
    (*send_q_depths)[FC_QP_ID] = FLR_SEND_CR_Q_DEPTH;
  }
  else if (protocol == LEADER) {
    *send_q_depths = malloc(LEADER_QP_NUM * sizeof(int));
    *recv_q_depths = malloc(LEADER_QP_NUM * sizeof(int));
    (*recv_q_depths)[PREP_ACK_QP_ID] = LDR_RECV_ACK_Q_DEPTH;
    (*recv_q_depths)[COMMIT_W_QP_ID] = LDR_RECV_W_Q_DEPTH;
    (*recv_q_depths)[FC_QP_ID] = LDR_RECV_CR_Q_DEPTH;
    (*send_q_depths)[PREP_ACK_QP_ID] = LDR_SEND_PREP_Q_DEPTH;
    (*send_q_depths)[COMMIT_W_QP_ID] = LDR_SEND_COM_Q_DEPTH;
    (*send_q_depths)[FC_QP_ID] = LDR_SEND_CR_Q_DEPTH;
  }
  else check_protocol(protocol);
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


// set up some basic leader buffers
zk_com_fifo_t *set_up_ldr_ops(zk_resp_t *resp,  uint16_t t_id)
{
  int i;
  assert(resp != NULL);
  zk_com_fifo_t *com_fifo = calloc(1, sizeof(zk_com_fifo_t));
  com_fifo->commits = (zk_com_mes_t *)
    calloc(COMMIT_FIFO_SIZE, sizeof(zk_com_mes_t));
  for(i = 0; i <  ZK_TRACE_BATCH; i++) resp[i].type = EMPTY;
  for(i = 0; i <  COMMIT_FIFO_SIZE; i++) {
      com_fifo->commits[i].opcode = KVS_OP_PUT;
  }
  assert(com_fifo->push_ptr == 0 && com_fifo->pull_ptr == 0 && com_fifo->size == 0);
 return com_fifo;
}

// Set up the memory registrations required in the leader if there is no Inlining
void set_up_ldr_mrs(struct ibv_mr **prep_mr, void *prep_buf,
                    struct ibv_mr **com_mr, void *com_buf,
                    hrd_ctrl_blk_t *cb)
{
  if (!LEADER_PREPARE_ENABLE_INLINING) {
   *prep_mr = register_buffer(cb->pd, (void*)prep_buf, PREP_FIFO_SIZE * sizeof(zk_prep_mes_t));
  }
  if (!COM_ENABLE_INLINING) *com_mr = register_buffer(cb->pd, com_buf,
                                                      COMMIT_CREDITS * sizeof(zk_com_mes_t));
}

// Set up all leader WRs
void set_up_ldr_WRs(struct ibv_send_wr *prep_send_wr, struct ibv_sge *prep_send_sgl,
                    struct ibv_send_wr *com_send_wr, struct ibv_sge *com_send_sgl,
                    uint16_t t_id, uint16_t remote_thread,
                    struct ibv_mr *prep_mr, struct ibv_mr *com_mr,
                    mcast_cb_t *mcast_cb) {
  uint16_t i, j;
  //BROADCAST WRs and credit Receives
  for (j = 0; j < MAX_BCAST_BATCH; j++) { // Number of Broadcasts
    if (LEADER_PREPARE_ENABLE_INLINING == 0) prep_send_sgl[j].lkey = prep_mr->lkey;
    if (!COM_ENABLE_INLINING) com_send_sgl[j].lkey = com_mr->lkey;

    for (i = 0; i < MESSAGES_IN_BCAST; i++) {
      uint16_t rm_id = (uint16_t) (i < LEADER_MACHINE ? i : i + 1);
      assert(rm_id != LEADER_MACHINE);
      assert(rm_id < MACHINE_NUM);
      uint16_t index = (uint16_t) ((j * MESSAGES_IN_BCAST) + i);
      assert (index < MESSAGES_IN_BCAST_BATCH);
      assert(index < LDR_MAX_PREP_WRS);
      assert(index < LDR_MAX_COM_WRS);
      bool last = (i == MESSAGES_IN_BCAST - 1);
      set_up_wr(&prep_send_wr[index], &prep_send_sgl[j], LEADER_PREPARE_ENABLE_INLINING,
                last, rm_id, remote_thread, PREP_ACK_QP_ID,
                ENABLE_MULTICAST, mcast_cb, PREP_MCAST_QP);
      set_up_wr(&com_send_wr[index], &com_send_sgl[j], COM_ENABLE_INLINING,
                last, rm_id, remote_thread, COMMIT_W_QP_ID,
                ENABLE_MULTICAST, mcast_cb, COM_MCAST_QP);
    }
  }
}


/* ---------------------------------------------------------------------------
------------------------------FOLLOWER --------------------------------------
---------------------------------------------------------------------------*/

// Set up all Follower WRs
void set_up_follower_WRs(struct ibv_send_wr *ack_send_wr, struct ibv_sge *ack_send_sgl,
                         struct ibv_recv_wr *prep_recv_wr, struct ibv_sge *prep_recv_sgl,
                         struct ibv_send_wr *w_send_wr, struct ibv_sge *w_send_sgl,
                         struct ibv_recv_wr *com_recv_wr, struct ibv_sge *com_recv_sgl,
                         uint16_t remote_thread,
                         hrd_ctrl_blk_t *cb, struct ibv_mr *w_mr,
                         mcast_cb_t *mcast)
{
  uint16_t i;
    // ACKS
    ack_send_wr->wr.ud.ah = rem_qp[LEADER_MACHINE][remote_thread][PREP_ACK_QP_ID].ah;
    ack_send_wr->wr.ud.remote_qpn = (uint32) rem_qp[LEADER_MACHINE][remote_thread][PREP_ACK_QP_ID].qpn;
    ack_send_wr->wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
    ack_send_wr->opcode = IBV_WR_SEND;
    ack_send_wr->send_flags = IBV_SEND_INLINE;
    ack_send_sgl->length = FLR_ACK_SEND_SIZE;
    ack_send_wr->num_sge = 1;
    ack_send_wr->sg_list = ack_send_sgl;
    ack_send_wr->next = NULL;
    // WRITES
    for (i = 0; i < FLR_MAX_W_WRS; ++i) {
        w_send_wr[i].wr.ud.ah = rem_qp[LEADER_MACHINE][remote_thread][COMMIT_W_QP_ID].ah;
        w_send_wr[i].wr.ud.remote_qpn = (uint32) rem_qp[LEADER_MACHINE][remote_thread][COMMIT_W_QP_ID].qpn;
        if (FLR_W_ENABLE_INLINING) w_send_wr[i].send_flags = IBV_SEND_INLINE;
        else {
            w_send_sgl[i].lkey = w_mr->lkey;
            w_send_wr[i].send_flags = 0;
        }
        w_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
        w_send_wr[i].opcode = IBV_WR_SEND;
        w_send_wr[i].num_sge = 1;
        w_send_wr[i].sg_list = &w_send_sgl[i];
    }
}


void flr_set_up_credit_WRs(struct ibv_send_wr* credit_send_wr, struct ibv_sge* credit_send_sgl,
                           hrd_ctrl_blk_t *cb, uint8_t flr_id, uint32_t max_credt_wrs, uint16_t t_id)
{
  // Credit WRs
  for (uint32_t i = 0; i < max_credt_wrs; i++) {
    credit_send_sgl->length = 0;
    credit_send_wr[i].opcode = IBV_WR_SEND_WITH_IMM;
    credit_send_wr[i].num_sge = 0;
    credit_send_wr[i].sg_list = credit_send_sgl;
    credit_send_wr[i].imm_data = (uint32) flr_id;
    credit_send_wr[i].wr.ud.remote_qkey = HRD_DEFAULT_QKEY;
    credit_send_wr[i].next = NULL;
    credit_send_wr[i].send_flags = IBV_SEND_INLINE;
    credit_send_wr[i].wr.ud.ah = rem_qp[LEADER_MACHINE][t_id][FC_QP_ID].ah;
    credit_send_wr[i].wr.ud.remote_qpn = (uint32_t) rem_qp[LEADER_MACHINE][t_id][FC_QP_ID].qpn;
  }
}



/* ---------------------------------------------------------------------------
------------------------------UTILITY --------------------------------------
---------------------------------------------------------------------------*/
void check_protocol(int protocol)
{
    if (protocol != FOLLOWER && protocol != LEADER) {
        my_printf(red, "Wrong protocol specified when setting up the queue depths %d \n", protocol);
        assert(false);
    }
}

/* ---------------------------------------------------------------------------
------------------------------MULTICAST --------------------------------------
---------------------------------------------------------------------------*/



