//
// Created by vasilis on 01/07/20.
//

#ifndef KITE_ZK_RESERVATION_STATIONS_UTIL_H_H
#define KITE_ZK_RESERVATION_STATIONS_UTIL_H_H

#include <od_inline_util.h>
#include "od_latency_util.h"
#include "od_generic_inline_util.h"
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

static inline bool zk_write_not_ready(ctx_com_mes_t *com,
                                      uint16_t com_ptr,
                                      uint16_t com_i,
                                      uint32_t com_num,
                                      zk_ctx_t *zk_ctx,
                                      uint16_t t_id)
{

  // it may be that a commit refers to a subset of writes that
  // we have seen and acked, and a subset not yet seen or acked,
  // We need to commit the seen subset to avoid a deadlock
  bool wrap_around = com->l_id + com_i - zk_ctx->local_w_id >= FLR_PENDING_WRITES;
  if (wrap_around) assert(com_ptr == zk_ctx->w_rob->pull_ptr);
  w_rob_t *w_rob = (w_rob_t *) get_fifo_slot(zk_ctx->w_rob, com_ptr);

  if (wrap_around || w_rob->w_state != VALID) {
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
        (w_rob->w_state == VALID))
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


static inline void zk_signal_completion_and_bookkeep_for_writes(zk_ctx_t *zk_ctx,
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
    if (protocol == LEADER)
    {
      w_rob->acks_seen = 0;
    }

    if (w_rob->is_local) {
      uint32_t sess_id = w_rob->session_id;
      signal_completion_to_client(w_rob->session_id,
                                  zk_ctx->index_to_req_array[sess_id], t_id);
      //if (DEBUG_WRITES)
#ifdef ZK_ENABLE_BQR
      bqr_rb_inc_last_completed_ts(&zk_ctx->b_ctx);
#endif
      //  my_printf(cyan, "Found a local req freeing session %d \n", zk_ctx->session_id[w_pull_ptr]);
      zk_ctx->stalled[sess_id] = false;
      zk_ctx->all_sessions_stalled = false;
      w_rob->is_local = false;
    }
    MOD_INCR(start_pull_ptr, zk_ctx->w_rob->max_size);
  }

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
    ctx_trace_op_t *op = (ctx_trace_op_t *) source;
    fill_prep(prep, op->key, op->opcode, op->val_len, op->value_to_write,
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
  r_rob_t *r_rob = (r_rob_t *) get_fifo_push_slot(zk_ctx->r_rob);
  read->opcode = KVS_OP_GET;
  read->key = r_rob->key;
  read->g_id = r_rob->g_id;
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

  ctx_trace_op_t *op = (ctx_trace_op_t *) source;
  fill_write(write, op->key, op->opcode, op->val_len, op->value_to_write,
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
  fifo_increm_capacity(zk_ctx->w_rob);
}

static inline void insert_read_help(context_t *ctx, void *r_ptr,
                                    void *source, uint32_t source_flag)
{
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  r_rob_t *r_rob = (r_rob_t *) get_fifo_push_slot(zk_ctx->r_rob);
  fill_read(ctx, (zk_read_t *) r_ptr);

  per_qp_meta_t *qp_meta = &ctx->qp_meta[R_QP_ID];
  fifo_t* send_fifo = qp_meta->send_fifo;
  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  zk_r_mes_t *r_mes = (zk_r_mes_t *) get_fifo_push_slot(send_fifo);
  if (ENABLE_ASSERTIONS) {
    assert(r_ptr == (void *)&r_mes->read[slot_meta->coalesce_num - 1]);
  }

  if (slot_meta->coalesce_num == 1) {
    r_mes->l_id = r_rob->l_id;
    fifo_set_push_backward_ptr(send_fifo, zk_ctx->r_rob->push_ptr);
  }
  fifo_incr_push_ptr(zk_ctx->r_rob);
  fifo_increm_capacity(zk_ctx->r_rob);
}


/* ---------------------------------------------------------------------------
//------------------------------ -----------------------------
//---------------------------------------------------------------------------*/
static inline void zk_apply_acks(uint32_t ack_num, uint32_t ack_ptr,
                                 uint64_t l_id, zk_ctx_t *zk_ctx,
                                 uint64_t pull_lid, uint32_t *outstanding_prepares,
                                 uint16_t t_id)
{
  for (uint16_t ack_i = 0; ack_i < ack_num; ack_i++) {

    if (ENABLE_ASSERTIONS && (ack_ptr == zk_ctx->w_rob->push_ptr)) {
      uint32_t origin_ack_ptr = (uint32_t) (ack_ptr - ack_i + LEADER_PENDING_WRITES) % LEADER_PENDING_WRITES;
      my_printf(red, "Origin ack_ptr %u/%u, acks %u/%u, w_pull_ptr %u, w_push_ptr % u, capacity %u \n",
                origin_ack_ptr,  (zk_ctx->w_rob->pull_ptr + (l_id - pull_lid)) % LEADER_PENDING_WRITES,
                ack_i, ack_num, zk_ctx->w_rob->pull_ptr, zk_ctx->w_rob->push_ptr, zk_ctx->w_rob->capacity);
    }

    w_rob_t *w_rob = (w_rob_t *) get_fifo_slot(zk_ctx->w_rob, ack_ptr);
    w_rob->acks_seen++;
    if (w_rob->acks_seen == LDR_QUORUM_OF_ACKS) {
      if (ENABLE_ASSERTIONS) (*outstanding_prepares)--;
//        printf("Leader %d valid ack %u/%u write at ptr %d with g_id %lu is ready \n",
//               t_id, ack_i, ack_num,  ack_ptr, zk_ctx->g_id[ack_ptr]);
      w_rob->w_state = READY;

    }
    MOD_INCR(ack_ptr, LEADER_PENDING_WRITES);
  }
}





#endif //KITE_ZK_RESERVATION_STATIONS_UTIL_H_H

