//
// Created by vasilis on 21/08/20.
//

#ifndef ODYSSEY_DR_DEBUG_UTIL_H
#define ODYSSEY_DR_DEBUG_UTIL_H

#include "dr_config.h"
#include "../../../odlib/include/network_api/network_context.h"
#include "dr_generic_util.h"

static inline char* dr_w_state_to_str(w_state_t state)
{
  switch (state) {
    case INVALID:return "INVALID";
    case VALID:return "VALID";
    case SENT:return "SENT";
    case READY:return "READY";
    case SEND_COMMITTS:return "SEND_COMMITTS";
    default: assert(false);
  }
}

static inline void print_g_id_entry(context_t *ctx,
                                    uint32_t rob_id,
                                    uint32_t entry_i)
{
  assert(rob_id < GID_ROB_NUM);
  assert(entry_i < GID_ROB_SIZE);
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  gid_rob_t *gid_rob = &dr_ctx->gid_rob_arr->gid_rob[rob_id];
  uint32_t w_ptr = gid_rob->w_rob_ptr[entry_i];
  w_rob_t* w_rob = get_fifo_slot(dr_ctx->w_rob, w_ptr);
  my_printf(cyan, "Entry %u: %s ---> %u (state %s, g_id %lu)",
            entry_i, gid_rob->valid[entry_i] ? "Valid" : "Invalid",
            dr_w_state_to_str(w_rob->w_state), w_rob->g_id);
}

static inline void print_g_id_rob(context_t *ctx, uint32_t rob_id)
{
  assert(rob_id < GID_ROB_NUM);
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  gid_rob_t *gid_rob = &dr_ctx->gid_rob_arr->gid_rob[rob_id];
  my_printf(cyan, "~~~~~~~Gid_rob %u~~~~~~~~~", gid_rob->rob_id);
  my_printf(cyan, "Ranging from %lu to %lu", gid_rob->base_gid, gid_rob->base_gid + GID_ROB_SIZE);
  for (int i = 0; i < GID_ROB_SIZE; ++i) {
    print_g_id_entry(ctx, rob_id, i);
  }

}

static inline void print_all_gid_robs(context_t *ctx)
{
  for (uint32_t i = 0; i < GID_ROB_NUM; ++i) {
    print_g_id_rob(ctx, i);
  }
}



static inline void print_gid_rob_with_gid(context_t *ctx,
                                          uint64_t g_id)
{
  print_g_id_rob(ctx, get_g_id_rob(ctx, g_id));
}


static inline void dr_check_op(dr_trace_op_t *op)
{
  if (ENABLE_ASSERTIONS) {
    check_state_with_allowed_flags(3, op->opcode, KVS_OP_PUT, KVS_OP_GET);
    assert(op->real_val_len > 0);
    assert(op->index_to_req_array < PER_SESSION_REQ_NUM);
    assert(op->session_id < SESSIONS_PER_THREAD);
    assert(op->key.bkt > 0);
  }
}

static inline void dr_checks_and_stats_on_bcasting_prepares(context_t *ctx,
                                                            uint8_t coalesce_num)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  
  if (ENABLE_ASSERTIONS) {
    assert(send_fifo->net_capacity >= coalesce_num);
    qp_meta->outstanding_messages += coalesce_num;
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].preps_sent +=
      coalesce_num;
    t_stats[ctx->t_id].preps_sent_mes_num++;
  }
}


static inline void dr_check_polled_prep_and_print(context_t *ctx,
                                                  dr_prep_mes_t* prep_mes)
{

  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  if (DEBUG_PREPARES)
    my_printf(green, "Wrkr %d sees a prep_mes message "
                "with %d prepares at index %u l_id %u \n",
              ctx->t_id, prep_mes->coalesce_num, recv_fifo->pull_ptr,
              prep_mes->l_id);
  if (ENABLE_ASSERTIONS) {
    assert(prep_mes->opcode == KVS_OP_PUT);
    assert(prep_mes->coalesce_num > 0 && prep_mes->coalesce_num <= PREP_COALESCE);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].received_preps += prep_mes->coalesce_num;
    t_stats[ctx->t_id].received_preps_mes_num++;
  }
}

static inline void dr_check_prepare_and_print(context_t *ctx,
                                              dr_prep_mes_t *prep_mes,
                                              uint8_t prep_i)
{
  if (ENABLE_ASSERTIONS) {
    dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
    dr_prepare_t *prepare = &prep_mes->prepare[prep_i];
    assert(prepare->g_id >= committed_global_w_id);
    //assert(prepare->val_len == VALUE_SIZE >> SHIFT_BITS);
    assert(((w_rob_t *) get_fifo_push_slot(dr_ctx->w_rob))->w_state == INVALID);


    if (DEBUG_PREPARES)
      my_printf(green, "Wrkr %u, prep_i %u new write at "
                  "ptr %u with g_id %lu and m_id %u \n",
                ctx->t_id, prep_i, dr_ctx->w_rob->push_ptr,
                prepare->g_id, prep_mes->m_id);
  }
}


static inline void dr_check_ack_l_id_is_small_enough(context_t *ctx,
                                                     ack_mes_t *ack)
{
  if (ENABLE_ASSERTIONS) {
    dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
    uint64_t pull_lid = dr_ctx->committed_w_id;
    assert(ack->l_id + ack->ack_num <= pull_lid + dr_ctx->w_rob->capacity);
    if ((ack->l_id + ack->ack_num < pull_lid) && (!USE_QUORUM)) {
      my_printf(red, "l_id %u, ack_num %u, pull_lid %u \n", ack->l_id, ack->ack_num, pull_lid);
      assert(false);
    }
  }
}


static inline void
dr_increase_counter_if_waiting_for_commit(dr_ctx_t *dr_ctx,
                                          uint64_t committed_g_id,
                                          uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) assert(dr_ctx->gid_rob_arr->pull_ptr < GID_ROB_NUM);
  gid_rob_t *gid_rob = get_g_id_rob_pull(dr_ctx);
  if (gid_rob->empty) return;
  w_rob_t *w_rob = get_w_rob_from_g_rob_pull(dr_ctx);
  if (ENABLE_STAT_COUNTING) {
    if ((gid_rob->base_gid == committed_g_id) &&
        (w_rob->w_state == VALID))
      t_stats[t_id].stalled_com_credit++;
  }
}

#endif //ODYSSEY_DR_DEBUG_UTIL_H
