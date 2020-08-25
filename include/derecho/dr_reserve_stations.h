//
// Created by vasilis on 21/08/20.
//

#ifndef ODYSSEY_DR_RESERVE_STATIONS_H
#define ODYSSEY_DR_RESERVE_STATIONS_H



//#include "latency_util.h"
#include "generic_inline_util.h"
#include "dr_debug_util.h"

#include <inline_util.h>




static inline void fill_dr_ctx_entry(context_t *ctx,
                                     dr_prep_mes_t *prep_mes,
                                     uint8_t prep_i)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  dr_prepare_t *prepare = &prep_mes->prepare[prep_i];
  w_rob_t *w_rob = (w_rob_t *) get_fifo_push_slot(dr_ctx->w_rob);
  w_rob->ptr_to_op = prepare;
  w_rob->g_id = prepare->g_id;
  w_rob->m_id = prep_mes->m_id;
  w_rob->w_state = VALID;
  set_gid_rob_entry(ctx, w_rob->g_id, dr_ctx->w_rob->push_ptr);
}

static inline void fill_prep(context_t *ctx,
                             dr_prepare_t *prep,
                             dr_trace_op_t *op)
{
  prep->key = op->key;
  //prep->opcode = op->opcode;
  //prep->val_len = op->val_len;
  memcpy(prep->value, op->value, (size_t) VALUE_SIZE);
  //prep->m_id = ctx->m_id;
  //prep->sess_id = op->session_id;
  prep->g_id = assign_new_g_id(ctx);
  printf("Wrkr %u assigned g_id %lu \n", ctx->t_id, prep->g_id);
}



// Insert a new local or remote write to the leader pending writes
static inline void insert_prep_help(context_t *ctx, void* prep_ptr,
                                    void *source, uint32_t source_flag)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  w_rob_t *w_rob = (w_rob_t *) get_fifo_push_slot(dr_ctx->w_rob);

  dr_prepare_t *prep = (dr_prepare_t *) prep_ptr;


  dr_trace_op_t *op = (dr_trace_op_t *) source;
  fill_prep(ctx, prep, op);
  dr_ctx->index_to_req_array[op->session_id] = op->index_to_req_array;


  // link it with dr_ctx to lead it later it to the KVS
  w_rob->ptr_to_op = prep;

  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);
  dr_prep_mes_t *prep_mes = (dr_prep_mes_t *) get_fifo_push_slot(send_fifo);
  // If it's the first message give it an lid
  if (slot_meta->coalesce_num == 1) {
    prep_mes->l_id = dr_ctx->inserted_w_id;
    fifo_set_push_backward_ptr(send_fifo, dr_ctx->w_rob->push_ptr);
  }

  // Bookkeeping
  w_rob->w_state = VALID;
  w_rob->is_local = source_flag == LOCAL_PREP;
  w_rob->session_id = op->session_id;
  w_rob->g_id = prep->g_id;
  set_gid_rob_entry(ctx, w_rob->g_id, dr_ctx->w_rob->push_ptr);
  dr_ctx->inserted_w_id++;
  fifo_incr_push_ptr(dr_ctx->w_rob);
  fifo_incr_capacity(dr_ctx->w_rob);
}




static inline void dr_fill_trace_op(context_t *ctx,
                                    trace_t *trace_op,
                                    dr_trace_op_t *op,
                                    int working_session)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  create_inputs_of_op(&op->value_to_write, &op->value_to_read, &op->real_val_len,
                      &op->opcode, &op->index_to_req_array,
                      &op->key, op->value, trace_op, working_session, ctx->t_id);

  dr_check_op(op);

  if (ENABLE_ASSERTIONS) assert(op->opcode != NOP);
  bool is_update = op->opcode == KVS_OP_PUT;
  if (WRITE_RATIO >= 1000) assert(is_update);
  op->val_len = is_update ? (uint8_t) (VALUE_SIZE >> SHIFT_BITS) : (uint8_t) 0;

  op->session_id = (uint16_t) working_session;


  dr_ctx->stalled[working_session] =
    (is_update) || (USE_REMOTE_READS);

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

#endif //ODYSSEY_DR_RESERVE_STATIONS_H
