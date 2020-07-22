//
// Created by vasilis on 01/07/20.
//

#ifndef KITE_ZK_RESERVATION_STATIONS_UTIL_H_H
#define KITE_ZK_RESERVATION_STATIONS_UTIL_H_H

#include <inline_util.h>
#include "latency_util.h"
#include "generic_inline_util.h"
#include "zk_debug_util.h"



static inline uint32_t get_size_from_opcode(uint8_t opcode)
{
  switch (opcode) {
    case KVS_OP_GET:
      return (uint32_t) R_SIZE;
    case G_ID_TOO_SMALL:
      return (uint32_t) R_REP_BIG_SIZE;
    case G_ID_EQUAL:
      return (uint32_t) R_REP_SMALL_SIZE;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}

static inline uint32_t get_resp_size_from_opcode(uint8_t opcode)
{
  switch (opcode) {
    case KVS_OP_GET:
      return (uint32_t) R_REP_BIG_SIZE;
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}

/*---------------------------------------------------------------------
 * -----------------------POLL COMMITS------------------------------
 * ---------------------------------------------------------------------*/

static inline void flr_increases_write_credits(context_t *ctx,
                                               zk_ctx_t *zk_ctx,
                                               uint16_t com_ptr,
                                               fifo_t *remote_w_buf)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COMMIT_W_QP_ID];
  w_rob_t *w_rob = (w_rob_t *) get_fifo_slot(zk_ctx->w_rob, com_ptr);
  if (w_rob->flr_id == ctx->m_id) {
    assert(qp_meta->mirror_remote_recv_fifo == remote_w_buf);
    (*qp_meta->credits) += remove_from_the_mirrored_buffer(remote_w_buf,
                                                  1, ctx->t_id, 0,
                                                  LEADER_W_BUF_SLOTS);
    if (DEBUG_WRITES)
      my_printf(yellow, "Received a credit, credits: %u \n", *qp_meta->credits);
  }
  if (ENABLE_ASSERTIONS) assert(*qp_meta->credits <= W_CREDITS);
}

static inline bool zk_write_not_ready(zk_com_mes_t *com,
                                      uint16_t com_ptr,
                                      uint16_t com_i,
                                      uint16_t com_num,
                                      zk_ctx_t *zk_ctx,
                                      uint16_t t_id)
{

  // it may be that a commit refers to a subset of writes that
  // we have seen and acked, and a subset not yet seen or acked,
  // We need to commit the seen subset to avoid a deadlock
  bool wrap_around = com->l_id + com_i - zk_ctx->local_w_id >= FLR_PENDING_WRITES;
  if (wrap_around) assert(com_ptr == zk_ctx->w_rob->pull_ptr);
  w_rob_t *w_rob = (w_rob_t *) get_fifo_slot(zk_ctx->w_rob, com_ptr);

  if (wrap_around || w_rob->w_state != SENT) {
    com->com_num -= com_i;
    com->l_id += com_i;
    if (ENABLE_STAT_COUNTING)  t_stats[t_id].received_coms += com_i;
    uint16_t imaginary_com_ptr = (uint16_t) ((zk_ctx->w_rob->pull_ptr +
                                 (com->l_id - zk_ctx->local_w_id)) % FLR_PENDING_WRITES);
    if(com_ptr != imaginary_com_ptr) {
      printf("com ptr %u/%u, com->l_id %lu, pull_lid %lu, w_pull_ptr %u, flr pending writes %d \n",
             com_ptr, imaginary_com_ptr, com->l_id, zk_ctx->local_w_id, zk_ctx->w_rob->pull_ptr, FLR_PENDING_WRITES);
      assert(false);
    }
    return true;
  }
  if (DEBUG_COMMITS)
    printf("Flr %d valid com %u/%u write at ptr %d with g_id %lu is ready \n",
           t_id, com_i, com_num,  com_ptr, ((w_rob_t *) get_fifo_slot(zk_ctx->w_rob, com_ptr))->g_id);

  return false;
}

/*---------------------------------------------------------------------
 * -----------------------POLL PREPARES------------------------------
 * ---------------------------------------------------------------------*/

static inline void fill_zk_ctx_entry(zk_ctx_t *zk_ctx,
                                     zk_prepare_t *prepare, uint8_t flr_id,
                                     uint16_t t_id)
{
  w_rob_t *w_rob = (w_rob_t *) get_fifo_push_slot(zk_ctx->w_rob);
  w_rob->ptr_to_op = prepare;
  w_rob->g_id = prepare->g_id;
  w_rob->flr_id = prepare->flr_id;
  w_rob->is_local = prepare->flr_id == flr_id;
  w_rob->session_id = prepare->sess_id; //not useful if not local
  w_rob->w_state = VALID;
}

/*---------------------------------------------------------------------
 * -----------------------PROPAGATING UPDATES------------------------------
 * ---------------------------------------------------------------------*/



static inline void
flr_increase_counter_if_waiting_for_commit(zk_ctx_t *zk_ctx,
                                           uint64_t committed_g_id,
                                           uint16_t t_id)
{
  w_rob_t * w_rob = (w_rob_t *) get_fifo_pull_slot(zk_ctx->w_rob);
  if (ENABLE_STAT_COUNTING) {
    if ((w_rob->g_id == committed_g_id + 1) &&
        (w_rob->w_state == SENT))
    t_stats[t_id].stalled_com_credit++;
  }
}


static inline bool is_expected_g_id_ready(zk_ctx_t *zk_ctx,
                                          uint64_t *committed_g_id,
                                          uint16_t *update_op_i,
                                          uint16_t t_id)
{
  w_rob_t * w_rob = (w_rob_t *) get_fifo_pull_slot(zk_ctx->w_rob);
  if (ENABLE_GID_ORDERING) {
    if (w_rob->g_id != (*committed_g_id) + 1) {
      if (ENABLE_ASSERTIONS) {
        if((*committed_g_id) >= w_rob->g_id) {
          my_printf(red, "Committed g_id/expected %lu/%lu \n",
                    (*committed_g_id), w_rob->g_id);
          assert(false);
        }
        zk_ctx->wait_for_gid_dbg_counter++;

        //if (*wait_for_reps_ctr % MILLION == 0)
        //  my_printf(yellow, "%s %u expecting/reading %u/%u \n",
        //            prot_to_str(protocol),
        //            t_id, zk_ctx->g_id[zk_ctx->w_rob->pull_ptr], committed_g_id);
      }
      if (ENABLE_STAT_COUNTING) t_stats[t_id].stalled_gid++;
      return false;
    }
  }
  w_rob->w_state = INVALID;
  if (ENABLE_ASSERTIONS) zk_ctx->wait_for_gid_dbg_counter = 0;
  fifo_incr_pull_ptr(zk_ctx->w_rob);
  (*update_op_i)++;
  (*committed_g_id)++;

  return true;
}


static inline void zk_signal_completion_and_bookkeepfor_writes(zk_ctx_t *zk_ctx,
                                                               uint16_t update_op_i,
                                                               uint32_t start_pull_ptr,
                                                               protocol_t protocol,
                                                               uint16_t t_id)
{
  if (ENABLE_ASSERTIONS)
    assert(start_pull_ptr ==
           (zk_ctx->w_rob->max_size + zk_ctx->w_rob->pull_ptr - update_op_i) % zk_ctx->w_rob->max_size);

  for (int w_i = 0; w_i < update_op_i; ++w_i) {
    w_rob_t * w_rob = (w_rob_t *) get_fifo_slot(zk_ctx->w_rob, start_pull_ptr);
    if (protocol == LEADER) w_rob->acks_seen = 0;

    if (w_rob->is_local) {
      uint32_t sess_id = w_rob->session_id;
      signal_completion_to_client(w_rob->session_id,
                                  zk_ctx->index_to_req_array[sess_id], t_id);
      //if (DEBUG_WRITES)
      //  my_printf(cyan, "Found a local req freeing session %d \n", zk_ctx->session_id[w_pull_ptr]);
      zk_ctx->stalled[sess_id] = false;
      zk_ctx->all_sessions_stalled = false;
      w_rob->is_local = false;
    }
    MOD_INCR(start_pull_ptr, zk_ctx->w_rob->max_size);
  }

}


/* ---------------------------------------------------------------------------
//------------------------------ BROADCASTS -----------------------------
//---------------------------------------------------------------------------*/


// Poll for credits and increment the credits according to the protocol
static inline void ldr_poll_credits(context_t *ctx)

{
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *cred_qp_meta = &ctx->qp_meta[FC_QP_ID];
  quorum_info_t *q_info = zk_ctx->q_info;
  uint16_t *credits = ctx->qp_meta[COMMIT_W_QP_ID].credits;
  bool poll_for_credits = false;
  for (uint8_t j = 0; j < q_info->active_num; j++) {
    if (credits[q_info->active_ids[j]] == 0) {
      poll_for_credits = true;
      break;
    }
  }
  if (!poll_for_credits) return;

  int credits_found = 0;
  credits_found = ibv_poll_cq(cred_qp_meta->recv_cq,
                              LDR_MAX_CREDIT_RECV,
                              cred_qp_meta->recv_wc);
  if(credits_found > 0) {
    if(unlikely(cred_qp_meta->recv_wc[credits_found - 1].status != 0)) {
      fprintf(stderr, "Bad wc status when polling for credits to send a broadcast %d\n",
              cred_qp_meta->recv_wc[credits_found -1].status);
      assert(false);
    }
    cred_qp_meta->recv_info->posted_recvs -= credits_found;
    for (uint32_t j = 0; j < credits_found; j++) {
      credits[cred_qp_meta->recv_wc[j].imm_data]+= FLR_CREDITS_IN_MESSAGE;
    }

    post_recvs_with_recv_info(cred_qp_meta->recv_info,
                              credits_found);

  }
  else if(unlikely(credits_found < 0)) {
    printf("ERROR In the credit CQ\n"); exit(0);
  }


}


// Form Broadcast work requests for the leader
static inline void forge_commit_wrs(context_t *ctx, zk_com_mes_t *com_mes,
                                    quorum_info_t *q_info, uint16_t br_i)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COMMIT_W_QP_ID];
  qp_meta->send_sgl[br_i].addr = (uint64_t) (uintptr_t) com_mes;
  qp_meta->send_sgl[br_i].length = LDR_COM_SEND_SIZE;
  if (ENABLE_ASSERTIONS) {
    assert(qp_meta->send_sgl[br_i].length <= LDR_COM_SEND_SIZE);
    if (!USE_QUORUM)
      assert(com_mes->com_num <= LEADER_PENDING_WRITES);
  }
  //my_printf(green, "Leader %d : I BROADCAST a message with %d commits, opcode %d credits: %d, l_id %lu \n",
  //             t_id, com_mes->com_num, com_mes->opcode, credits[COMM_VC][1], com_mes->l_id);
  form_bcast_links(&qp_meta->sent_tx, qp_meta->ss_batch, q_info, br_i,
                   qp_meta->send_wr, qp_meta->send_cq, "forging commits", ctx->t_id);
}


// Form the Broadcast work request for the prepare
static inline void forge_bcast_wr(context_t *ctx,
                                  uint16_t qp_id,
                                  uint16_t br_i)
{
  zk_ctx_t * zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_id];
  struct ibv_sge *send_sgl = qp_meta->send_sgl;

  fifo_t *send_fifo = qp_meta->send_fifo;
  send_sgl[br_i].length = get_fifo_slot_meta_pull(send_fifo)->byte_size;
  send_sgl[br_i].addr = (uintptr_t) get_fifo_pull_slot(send_fifo);
  form_bcast_links(&qp_meta->sent_tx, qp_meta->ss_batch, zk_ctx->q_info, br_i,
                   qp_meta->send_wr, qp_meta->send_cq, qp_meta->send_string, ctx->t_id);
}


/* ---------------------------------------------------------------------------
//------------------------------ UNICASTS --------------------------------
//---------------------------------------------------------------------------*/
//
static inline void forge_unicast_wr(context_t *ctx,
                                    uint16_t qp_id,
                                    uint16_t mes_i)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_id];
  struct ibv_sge *send_sgl = qp_meta->send_sgl;
  struct ibv_send_wr *send_wr = qp_meta->send_wr;

  //printf("%u/%u \n", mes_i, MAX_R_REP_WRS);
  send_sgl[mes_i].length = get_fifo_slot_meta_pull(qp_meta->send_fifo)->byte_size;
  send_sgl[mes_i].addr = (uintptr_t) get_fifo_pull_slot(qp_meta->send_fifo);

  if (qp_meta->receipient_num > 1) {
    uint8_t rm_id = get_fifo_slot_meta_pull(qp_meta->send_fifo)->rm_id;
    send_wr[mes_i].wr.ud.ah = rem_qp[rm_id][ctx->t_id][qp_id].ah;
    send_wr[mes_i].wr.ud.remote_qpn = (uint32_t) rem_qp[rm_id][ctx->t_id][qp_id].qpn;
  }


  selective_signaling_for_unicast(&qp_meta->sent_tx, qp_meta->ss_batch, send_wr,
                                  mes_i, qp_meta->send_cq, qp_meta->enable_inlining,
                                  qp_meta->send_string, ctx->t_id);
  // Have the last message point to the current message
  if (mes_i > 0) send_wr[mes_i - 1].next = &send_wr[mes_i];

}



// Add the acked gid to the appropriate commit message
static inline void zk_insert_commit(context_t *ctx,
                                    uint16_t update_op_i)
{
  zk_ckecks_when_creating_commits(ctx, update_op_i);
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  fifo_t *send_fifo = ctx->qp_meta[COMMIT_W_QP_ID].send_fifo;
  zk_com_mes_t *commit = (zk_com_mes_t *) get_fifo_push_prev_slot(send_fifo);

  if (send_fifo->capacity > 0)
    commit->com_num += update_op_i;
  else { //otherwise push a new commit
    commit->l_id = zk_ctx->local_w_id;
    commit->com_num = update_op_i;
    fifo_incr_capacity(send_fifo);
  }
  send_fifo->net_capacity += update_op_i;
  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  slot_meta->coalesce_num += update_op_i;


}

/* ---------------------------------------------------------------------------
//------------------------------INSERTS --------------------------------
//---------------------------------------------------------------------------*/

static inline void fill_prep(zk_prepare_t *prep, mica_key_t key, uint8_t opcode, uint8_t val_len,
                             uint8_t *value, uint8_t flr_id, uint16_t session_id)
{
  prep->key = key;
  prep->opcode = opcode;
  prep->val_len = val_len;
  memcpy(prep->value, value, (size_t) VALUE_SIZE);
  prep->flr_id = flr_id;
  prep->sess_id = session_id;
}

static inline void fill_write(zk_write_t *write, mica_key_t key, uint8_t opcode, uint8_t val_len,
                              uint8_t *value, uint8_t flr_id, uint32_t session_id)
{
  write->key = key;
  write->opcode = opcode;
  write->val_len = val_len;
  memcpy(write->value, value, (size_t) VALUE_SIZE);
  write->flr_id = flr_id;
  write->sess_id = session_id;
}


static inline void fill_r_rep(zk_read_t *read,
                              zk_r_rep_big_t *r_rep,
                              mica_op_t *kv_ptr,
                              uint16_t t_id)
{
  r_rep->opcode = G_ID_TOO_SMALL;
  uint32_t debug_cntr = 0;
  uint64_t tmp_lock = read_seqlock_lock_free(&kv_ptr->seqlock);
  do {
    debug_stalling_on_lock(&debug_cntr, "filling r_rep", t_id);
    if (read->g_id == kv_ptr->g_id) r_rep->opcode = G_ID_EQUAL;
    else {
      memcpy(r_rep->value, kv_ptr->value, (size_t) VALUE_SIZE);
      r_rep->g_id = kv_ptr->g_id;
    }
  } while (!(check_seqlock_lock_free(&kv_ptr->seqlock, &tmp_lock)));
}


static inline uint16_t fill_prepare_based_on_source(context_t *ctx,
                                                    zk_prepare_t *prep,
                                                    void *source,
                                                    source_t source_flag)
{

  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  if (source_flag == LOCAL_PREP) {
    zk_trace_op_t *op = (zk_trace_op_t *) source;
    fill_prep(prep, op->key, op->opcode, op->val_len, op->value,
              ctx->m_id, (uint16_t)  op->session_id);
    zk_ctx->index_to_req_array[op->session_id] = op->index_to_req_array;
    return op->session_id;
  }
  else {
    if (ENABLE_ASSERTIONS) assert(source_flag == REMOTE_WRITE);
    zk_write_t *rem_write = (zk_write_t *) source;
    fill_prep(prep, rem_write->key, rem_write->opcode,
              rem_write->val_len, rem_write->value,
              rem_write->flr_id, (uint16_t) rem_write->sess_id);
    return (uint16_t) rem_write->sess_id;
  }

}


static inline void fill_read(context_t *ctx, zk_read_t *read)
{
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  r_rob_t *r_meta = (r_rob_t *) get_fifo_push_slot(zk_ctx->r_rob);
  read->opcode = KVS_OP_GET;
  read->key = r_meta->key;
  read->g_id = r_meta->g_id;
}


static inline void insert_r_rep_help(context_t *ctx, void *r_rep_ptr,
                                     void *source, uint32_t op_i)
{
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[R_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  ptrs_to_r_t *ptrs_to_r = zk_ctx->ptrs_to_r;
  zk_read_t *read = ptrs_to_r->ptr_to_ops[op_i];


  zk_r_rep_big_t *r_rep = (zk_r_rep_big_t *) r_rep_ptr;

  fill_r_rep(read, r_rep, (mica_op_t *) source, ctx->t_id);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  if (r_rep->opcode == G_ID_TOO_SMALL)
    slot_meta->byte_size += (R_REP_BIG_SIZE - R_REP_SMALL_SIZE);

  zk_r_rep_mes_t *r_rep_mes = (zk_r_rep_mes_t *) get_fifo_push_slot(send_fifo);
  if (slot_meta->coalesce_num == 1) {
    r_rep_mes->l_id = ptrs_to_r->ptr_to_r_mes[op_i]->l_id;
    slot_meta->rm_id = ptrs_to_r->ptr_to_r_mes[op_i]->m_id;
  }
}

static inline void insert_write_help(context_t *ctx, void *w_ptr,
                                     void *source, uint32_t source_flag)
{
  zk_write_t *write = (zk_write_t *) w_ptr;
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;

  zk_trace_op_t *op = (zk_trace_op_t *) source;
  fill_write(write, op->key, op->opcode, op->val_len, op->value,
             ctx->m_id, op->session_id);
  zk_ctx->index_to_req_array[op->session_id] = op->index_to_req_array;

}

// Insert a new local or remote write to the leader pending writes
static inline void insert_prep_help(context_t *ctx, void* prep_ptr,
                                    void *source, uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_ACK_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  w_rob_t * w_rob = (w_rob_t *) get_fifo_push_slot(zk_ctx->w_rob);

  zk_prepare_t *prep = (zk_prepare_t *) prep_ptr;
  uint32_t session_id = fill_prepare_based_on_source(ctx, prep, source, (source_t) source_flag);

  // link it with zk_ctx to lead it later it to the KVS
  w_rob->ptr_to_op = prep;

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  zk_prep_mes_t *prep_mes = (zk_prep_mes_t *) get_fifo_push_slot(send_fifo);
  // If it's the first message give it an lid
  if (slot_meta->coalesce_num == 1) {
    prep_mes->l_id = (uint64_t) (zk_ctx->local_w_id + zk_ctx->w_rob->capacity);
    fifo_set_push_backward_ptr(send_fifo, zk_ctx->w_rob->push_ptr);
  }



  //checks_when_leader_creates_write((zk_prep_mes_t *) send_fifo->fifo, send_fifo->push_ptr,
  //                                 (uint32_t) slot_meta->coalesce_num - 1,
  //                                 zk_ctx, w_ptr, ctx->t_id);

  // Bookkeeping
  w_rob->w_state = VALID;
  w_rob->is_local = source_flag == LOCAL_PREP;
  w_rob->session_id = (uint32_t) session_id;
  fifo_incr_push_ptr(zk_ctx->w_rob);
  fifo_incr_capacity(zk_ctx->w_rob);
}

static inline void insert_read_help(context_t *ctx, void *r_ptr,
                                    void *source, uint32_t source_flag)
{
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  r_rob_t *r_meta = (r_rob_t *) get_fifo_push_slot(zk_ctx->r_rob);
  fill_read(ctx, (zk_read_t *) r_ptr);

  per_qp_meta_t *qp_meta = &ctx->qp_meta[R_QP_ID];
  fifo_t* send_fifo = qp_meta->send_fifo;
  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  zk_r_mes_t *r_mes = (zk_r_mes_t *) get_fifo_push_slot(send_fifo);
  if (ENABLE_ASSERTIONS) {
    assert(r_ptr == (void *)&r_mes->read[slot_meta->coalesce_num - 1]);
  }

  if (slot_meta->coalesce_num == 1) {
    r_mes->l_id = r_meta->l_id;
    fifo_set_push_backward_ptr(send_fifo, zk_ctx->r_rob->push_ptr);
  }
  fifo_incr_push_ptr(zk_ctx->r_rob);
}

//static inline void insert_com_help(context_t *ctx, void *r_ptr,
//                                   void *source, uint32_t source_flag)
//{
//  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
//  per_qp_meta_t *qp_meta = &ctx->qp_meta[COMMIT_W_QP_ID];
//  fifo_t* send_fifo = qp_meta->send_fifo;
//
//
//
//}

static inline void insert_mes(context_t *ctx,
                              uint16_t qp_id,
                              uint32_t send_size,
                              uint32_t recv_size,
                              bool break_message,
                              void* source,
                              uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_id];
  fifo_t* send_fifo = qp_meta->send_fifo;
  void *ptr = get_send_fifo_ptr(send_fifo, send_size, recv_size,
                                break_message, ctx->t_id);

  qp_meta->mfs->insert_helper(ctx, ptr, source, source_flag);

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);


  if (slot_meta->coalesce_num == 1)
    fifo_incr_capacity(send_fifo);

  fifo_incr_net_capacity(send_fifo);
}


/* ---------------------------------------------------------------------------
//------------------------------TRACE --------------------------------
//---------------------------------------------------------------------------*/

static inline void zk_fill_trace_op(context_t *ctx,
                                    trace_t *trace_op,
                                    zk_trace_op_t *op,
                                    int working_session)
{
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  create_inputs_of_op(&op->value_to_write, &op->value_to_read, &op->real_val_len,
                      &op->opcode, &op->index_to_req_array,
                      &op->key, op->value, trace_op, working_session, ctx->t_id);

  zk_check_op(op);

  if (ENABLE_ASSERTIONS) assert(op->opcode != NOP);
  bool is_update = op->opcode == KVS_OP_PUT;
  if (WRITE_RATIO >= 1000) assert(is_update);
   op->val_len = is_update ? (uint8_t) (VALUE_SIZE >> SHIFT_BITS) : (uint8_t) 0;

  op->session_id = (uint16_t) working_session;


  zk_ctx->stalled[working_session] =
    (is_update) || (USE_REMOTE_READS && zk_ctx->protocol == FOLLOWER);

  if (ENABLE_CLIENTS) {
    signal_in_progress_to_client(op->session_id, op->index_to_req_array, ctx->t_id);
    if (ENABLE_ASSERTIONS) assert(interface[ctx->t_id].wrkr_pull_ptr[working_session] == op->index_to_req_array);
    MOD_INCR(interface[ctx->t_id].wrkr_pull_ptr[working_session], PER_SESSION_REQ_NUM);
  }

  if (ENABLE_ASSERTIONS == 1) {
    assert(WRITE_RATIO > 0 || is_update == 0);
    if (is_update) assert(op->val_len > 0);
  }
}


#endif //KITE_ZK_RESERVATION_STATIONS_UTIL_H_H

