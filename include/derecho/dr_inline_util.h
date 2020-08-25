//
// Created by vasilis on 20/08/20.
//

#include "../../../odlib/include/network_api/network_context.h"
#include "dr_reserve_stations.h"
#include "dr_kvs_util.h"

#ifndef ODYSSEY_DR_INLINE_UTIL_H
#define ODYSSEY_DR_INLINE_UTIL_H

/* ---------------------------------------------------------------------------
//------------------------------TRACE --------------------------------
//---------------------------------------------------------------------------*/


static inline void dr_batch_from_trace_to_KVS(context_t *ctx)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  dr_trace_op_t *ops = dr_ctx->ops;
  dr_resp_t *resp = dr_ctx->resp;
  trace_t *trace = dr_ctx->trace;

  uint16_t op_i = 0;
  int working_session = -1;
  // if there are clients the "all_sessions_stalled" flag is not used,
  // so we need not bother checking it
  if (!ENABLE_CLIENTS && dr_ctx->all_sessions_stalled) {
    return;
  }
  for (uint16_t i = 0; i < SESSIONS_PER_THREAD; i++) {
    uint16_t sess_i = (uint16_t)((dr_ctx->last_session + i) % SESSIONS_PER_THREAD);
    if (pull_request_from_this_session(dr_ctx->stalled[sess_i], sess_i, ctx->t_id)) {
      working_session = sess_i;
      break;
    }
  }
  if (ENABLE_CLIENTS) {
    if (working_session == -1) return;
  }
  else if (ENABLE_ASSERTIONS) assert(working_session != -1);

  bool passed_over_all_sessions = false;

  /// main loop
  while (op_i < DR_TRACE_BATCH && !passed_over_all_sessions) {

    dr_fill_trace_op(ctx, &trace[dr_ctx->trace_iter], &ops[op_i], working_session);
    while (!pull_request_from_this_session(dr_ctx->stalled[working_session],
                                           (uint16_t) working_session, ctx->t_id)) {

      MOD_INCR(working_session, SESSIONS_PER_THREAD);
      if (working_session == dr_ctx->last_session) {
        passed_over_all_sessions = true;
        // If clients are used the condition does not guarantee that sessions are stalled
        if (!ENABLE_CLIENTS) dr_ctx->all_sessions_stalled = true;
        break;
      }
    }
    resp[op_i].type = EMPTY;
    if (!ENABLE_CLIENTS) {
      dr_ctx->trace_iter++;
      if (trace[dr_ctx->trace_iter].opcode == NOP) dr_ctx->trace_iter = 0;
    }
    op_i++;
  }
  //printf("Session %u pulled: ops %u, req_array ptr %u \n",
  //       working_session, op_i, ops[0].index_to_req_array);
  dr_ctx->last_session = (uint16_t) working_session;
  t_stats[ctx->t_id].cache_hits_per_thread += op_i;
  dr_KVS_batch_op_trace(dr_ctx, op_i, ctx->t_id);

  for (uint16_t i = 0; i < op_i; i++) {
    // my_printf(green, "After: OP_i %u -> session %u \n", i, *(uint32_t *) &ops[i]);
    if (resp[i].type == KVS_MISS)  {
      my_printf(green, "KVS %u: bkt %u, server %u, tag %u \n", i,
                ops[i].key.bkt, ops[i].key.server, ops[i].key.tag);
      assert(false);
      continue;
    }
    // Local reads
    else if (resp[i].type == KVS_GET_SUCCESS) {
      if (ENABLE_ASSERTIONS) {
        assert(USE_REMOTE_READS);
      }
      //ctx_insert_mes(ctx, R_QP_ID, R_SIZE, (uint32_t) R_REP_BIG_SIZE, false, NULL, NOT_USED);
    }
    else if (resp[i].type == KVS_LOCAL_GET_SUCCESS) {
      signal_completion_to_client(ops[i].session_id, ops[i].index_to_req_array, ctx->t_id);
    }
    else { // WRITE
      ctx_insert_mes(ctx, PREP_QP_ID, (uint32_t) PREP_SIZE, 1, false, &ops[i], LOCAL_PREP);
    }
  }

}

//
///* ---------------------------------------------------------------------------
////------------------------------UNICASTS -----------------------------
////---------------------------------------------------------------------------*/


///* ---------------------------------------------------------------------------
////------------------------------ GID HANDLING -----------------------------
////---------------------------------------------------------------------------*/

// Propagates Updates that have seen all acks to the KVS
static inline void dr_propagate_updates(context_t *ctx)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  fifo_t *prep_buf_mirror = ctx->qp_meta[PREP_QP_ID].mirror_remote_recv_fifo;

  uint16_t update_op_i = 0;
  // remember the starting point to use it when writing the KVS
  uint32_t starting_pull_ptr = dr_ctx->w_rob->pull_ptr;
  uint64_t committed_g_id = atomic_load_explicit(&committed_global_w_id, memory_order_relaxed);
  dr_increase_counter_if_waiting_for_commit(dr_ctx, committed_g_id, ctx->t_id);
  while(!dr_ctx->gid_rob_arr->empty) {
    w_rob_t *w_rob = get_w_rob_from_g_rob_pull(dr_ctx);
    if (ENABLE_ASSERTIONS) assert(w_rob->w_state == VALID || w_rob->w_state == READY);
    if (w_rob->w_state != READY) break;



  }

  //while(((w_rob_t *) get_fifo_pull_slot(dr_ctx->w_rob))->w_state == READY) {
  //  if (!is_expected_g_id_ready(dr_ctx, &committed_g_id, &update_op_i,
  //                              ctx->t_id))
  //    break;
  //}
  //
  //if (update_op_i > 0) {
  //  if (dr_ctx->protocol == FOLLOWER) {
  //    remove_from_the_mirrored_buffer(prep_buf_mirror, update_op_i,
  //                                    ctx->t_id, 0, FLR_PREP_BUF_SLOTS);
  //    if (ENABLE_ASSERTIONS)
  //      assert(dr_ctx->p_acks->slots_ahead >= update_op_i);
  //    dr_ctx->p_acks->slots_ahead -= update_op_i;
  //
  //  }
  //  else {
  //    dr_insert_commit(ctx, update_op_i);
  //  }
  //
  //  dr_ctx->committed_w_id += update_op_i; // this must happen after inserting commits
  //  fifo_decrease_capacity(dr_ctx->w_rob, update_op_i);
  //  dr_KVS_batch_op_updates((uint16_t) update_op_i, dr_ctx, starting_pull_ptr,
  //                          ctx->t_id);
  //
  //  if (ENABLE_GIDS)
  //    atomic_store_explicit(&committed_global_w_id, committed_g_id, memory_order_relaxed);
  //  dr_signal_completion_and_bookkeep_for_writes(dr_ctx, update_op_i, starting_pull_ptr,
  //                                               dr_ctx->protocol, ctx->t_id);
  //}
}



///* ---------------------------------------------------------------------------
////------------------------------POLL HANDLERS -----------------------------
////---------------------------------------------------------------------------*/



static inline void dr_apply_acks(context_t *ctx,
                                 ack_mes_t *ack,
                                 uint32_t ack_num, uint32_t ack_ptr)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
  uint64_t pull_lid = dr_ctx->committed_w_id;
  for (uint16_t ack_i = 0; ack_i < ack_num; ack_i++) {

    if (ENABLE_ASSERTIONS && (ack_ptr == dr_ctx->w_rob->push_ptr)) {
      uint32_t origin_ack_ptr = (uint32_t) (ack_ptr - ack_i + DR_PENDING_WRITES) % DR_PENDING_WRITES;
      my_printf(red, "Origin ack_ptr %u/%u, acks %u/%u, w_pull_ptr %u, w_push_ptr % u, capacity %u \n",
                origin_ack_ptr,  (dr_ctx->w_rob->pull_ptr + (ack->l_id - pull_lid)) % DR_PENDING_WRITES,
                ack_i, ack_num, dr_ctx->w_rob->pull_ptr, dr_ctx->w_rob->push_ptr, dr_ctx->w_rob->capacity);
    }

    w_rob_t *w_rob = (w_rob_t *) get_fifo_slot(dr_ctx->w_rob, ack_ptr);
    w_rob->acks_seen++;
    if (w_rob->acks_seen == QUORUM_NUM) {
      if (ENABLE_ASSERTIONS) qp_meta->outstanding_messages--;
//        printf("Worker %d valid ack %u/%u write at ptr %d with g_id %lu is ready \n",
//               t_id, ack_i, ack_num,  ack_ptr, dr_ctx->g_id[ack_ptr]);
      w_rob->w_state = READY;

    }
    MOD_INCR(ack_ptr, DR_PENDING_WRITES);
  }
}

//
static inline bool ack_handler(context_t *ctx)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[ACK_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile ack_mes_ud_t *incoming_acks = (volatile ack_mes_ud_t *) recv_fifo->fifo;
  ack_mes_t *ack = (ack_mes_t *) &incoming_acks[recv_fifo->pull_ptr].ack;
  uint32_t ack_num = ack->ack_num;
  uint64_t l_id = ack->l_id;
  uint64_t pull_lid = dr_ctx->committed_w_id; // l_id at the pull pointer
  uint32_t ack_ptr; // a pointer in the FIFO, from where ack should be added
  //dr_check_polled_ack_and_print(ack, ack_num, pull_lid, recv_fifo->pull_ptr, ctx->t_id);
  //dr_increase_prep_credits(qp_meta->credits, ack, qp_meta->mirror_remote_recv_fifo, ctx->t_id);

  ctx_increase_credits_on_polling_ack(ctx, ACK_QP_ID, ack);

  if ((dr_ctx->w_rob->capacity == 0 ) ||
      (pull_lid >= l_id && (pull_lid - l_id) >= ack_num))
    return true;

  dr_check_ack_l_id_is_small_enough(ctx, ack);
  ack_ptr = ctx_find_when_the_ack_points_acked(ack, dr_ctx->w_rob, pull_lid, &ack_num);

  // Apply the acks that refer to stored writes
  dr_apply_acks(ctx, ack, ack_num, ack_ptr);

  return true;
}


//
static inline bool prepare_handler(context_t *ctx)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile dr_prep_mes_ud_t *incoming_preps = (volatile dr_prep_mes_ud_t *) recv_fifo->fifo;
  dr_prep_mes_t *prep_mes = (dr_prep_mes_t *) &incoming_preps[recv_fifo->pull_ptr].prepare;

  uint8_t coalesce_num = prep_mes->coalesce_num;

  if (qp_meta->mirror_remote_recv_fifo->capacity == MAX_PREP_BUF_SLOTS_TO_BE_POLLED) return false;
  if (dr_ctx->w_rob->capacity + coalesce_num > DR_PENDING_WRITES) return false;
  dr_check_polled_prep_and_print(ctx, prep_mes);

  ctx_ack_insert(ctx, ACK_QP_ID, coalesce_num,  prep_mes->l_id, prep_mes->m_id);
  add_to_the_mirrored_buffer(qp_meta->mirror_remote_recv_fifo,
                             coalesce_num, 1, ctx->q_info);

  for (uint8_t prep_i = 0; prep_i < coalesce_num; prep_i++) {
    dr_check_prepare_and_print(ctx, prep_mes, prep_i);
    fill_dr_ctx_entry(ctx, prep_mes, prep_i);
    fifo_incr_push_ptr(dr_ctx->w_rob);
    fifo_incr_capacity(dr_ctx->w_rob);
  }

  if (ENABLE_ASSERTIONS) prep_mes->opcode = 0;

  return true;
}


//
static inline void send_prepares_helper(context_t *ctx)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  if (DEBUG_PREPARES)
    printf("Wrkr %d has %u bcasts to send credits %d\n", ctx->t_id,
           send_fifo->net_capacity, qp_meta->credits[1]);
  // Create the broadcast messages
  dr_prep_mes_t *prep_buf = (dr_prep_mes_t *) qp_meta->send_fifo->fifo;
  dr_prep_mes_t *prep = &prep_buf[send_fifo->pull_ptr];

  slot_meta_t *slot_meta = get_fifo_slot_meta_pull(send_fifo);
  uint8_t coalesce_num = (uint8_t) slot_meta->coalesce_num;
  prep->coalesce_num = (uint8_t) slot_meta->coalesce_num;
  uint32_t backward_ptr = fifo_get_pull_backward_ptr(send_fifo);

  for (uint16_t i = 0; i < coalesce_num; i++) {
    w_rob_t *w_rob = (w_rob_t *) get_fifo_slot_mod(dr_ctx->w_rob, backward_ptr + i);
    w_rob->w_state = SENT;
    if (DEBUG_PREPARES)
      printf("Prepare %d, g_id %lu, total message capacity %d\n", i, prep->prepare[i].g_id,
             slot_meta->byte_size);
  }

  if (DEBUG_PREPARES)
    my_printf(green, "Wrkr %d : I BROADCAST a prepare message %d of "
                "%u prepares with total w_size %u,  with  credits: %d, lid: %lu  \n",
              ctx->t_id, prep->opcode, coalesce_num, slot_meta->byte_size,
              qp_meta->credits[0], prep->l_id);
  dr_checks_and_stats_on_bcasting_prepares(ctx, coalesce_num);
}





static inline void main_loop(context_t *ctx)
{
  my_printf(yellow, "Derecho main loop \n");
  while(true) {

    dr_batch_from_trace_to_KVS(ctx);

    ctx_send_broadcasts(ctx, PREP_QP_ID);

    ctx_poll_incoming_messages(ctx, PREP_QP_ID);

    ctx_send_acks(ctx, ACK_QP_ID);

    ctx_poll_incoming_messages(ctx, ACK_QP_ID);

  }
}

#endif //ODYSSEY_DR_INLINE_UTIL_H
