#ifndef INLINE_UTILS_H
#define INLINE_UTILS_H


#include "zk_kvs_util.h"

/* ---------------------------------------------------------------------------
//------------------------------TRACE --------------------------------
//---------------------------------------------------------------------------*/


#define MAX_BQR_READS_FOR_SINGLE_INSERT (bqr_is_remote ? 256 : 64)
// Both Leader and Followers use this to read the trace, propagate reqs to the cache and maintain their prepare/write fifos
static inline uint16_t zk_find_trace_ops(context_t *ctx)
{
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  ctx_trace_op_t *ops = zk_ctx->ops;
  zk_resp_t *resp = zk_ctx->resp;
  trace_t *trace = zk_ctx->trace;

  if (zk_ctx->protocol == FOLLOWER && MAKE_FOLLOWERS_PASSIVE) return 0;
  uint16_t kvs_op_i = 0;
  int working_session = -1;
  if (all_sessions_are_stalled(ctx, zk_ctx->all_sessions_stalled,
                               &zk_ctx->stalled_sessions_dbg_counter))
    return 0;
  if (!od_find_starting_session(ctx, zk_ctx->last_session,
                                zk_ctx->stalled, &working_session)) return 0;

  bool passed_over_all_sessions = false;

#ifdef ZK_ENABLE_BQR
  uint16_t bqr_inserts = 0;
#endif
  /// main loop
  while (kvs_op_i < ZK_TRACE_BATCH && !passed_over_all_sessions) {

#ifdef ZK_ENABLE_BQR
    if(trace[zk_ctx->trace_iter].opcode == KVS_OP_GET){
      ctx_trace_op_t *op_to_fill = bqr_rb_next_to_push(&zk_ctx->b_ctx);
      bool rb_is_full = op_to_fill == NULL;
      if(rb_is_full) break;
      ctx_fill_trace_op(ctx, &trace[zk_ctx->trace_iter], op_to_fill, working_session);

      zk_ctx->trace_iter++;
      if (trace[zk_ctx->trace_iter].opcode == NOP) zk_ctx->trace_iter = 0;

      if(++bqr_inserts == MAX_BQR_READS_FOR_SINGLE_INSERT) break;
      continue;
    }
#endif

    od_fill_trace_op(ctx, &trace[zk_ctx->trace_iter], &ops[kvs_op_i], working_session);
    zk_ctx->stalled[working_session] =
      ops[kvs_op_i].opcode == KVS_OP_PUT || (USE_LIN_READS && zk_ctx->protocol == FOLLOWER);

      if (USE_LIN_READS && ENABLE_LIN_READ_LATENCY &&
          zk_ctx->protocol == FOLLOWER && ctx->t_id == 0 &&
          kvs_op_i == 0 && ops[kvs_op_i].opcode == KVS_OP_GET)
      {
          start_latency_measurement(&lt_timer);
      }

    passed_over_all_sessions =
        od_find_next_working_session(ctx, &working_session,
                                     zk_ctx->stalled,
                                     zk_ctx->last_session,
                                     &zk_ctx->all_sessions_stalled);
    resp[kvs_op_i].type = EMPTY;
    if (!ENABLE_CLIENTS) {
      zk_ctx->trace_iter++;
      if (trace[zk_ctx->trace_iter].opcode == NOP) zk_ctx->trace_iter = 0;
    }
    kvs_op_i++;

  }
  //printf("Session %u pulled: ops %u, req_array ptr %u \n",
  //       working_session, kvs_op_i, ops[0].index_to_req_array);
  zk_ctx->last_session = (uint16_t) working_session;
  t_stats[ctx->t_id].total_reqs += kvs_op_i;
  return kvs_op_i;
}


static inline void zk_batch_from_trace_to_KVS(context_t *ctx)
{
  uint16_t kvs_op_i = zk_find_trace_ops(ctx);
  if (kvs_op_i > 0 )
    zk_KVS_batch_op_trace(ctx, kvs_op_i);
}

/* ---------------------------------------------------------------------------
//------------------------------UNICASTS -----------------------------
//---------------------------------------------------------------------------*/

static inline void send_writes_helper(context_t *ctx)
{
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COMMIT_W_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  zk_w_mes_t *w_mes = (zk_w_mes_t *) get_fifo_pull_slot(send_fifo);
  uint16_t coalesce_num = get_fifo_slot_meta_pull(send_fifo)->coalesce_num;
  w_mes->coalesce_num = (uint8_t) coalesce_num;
    checks_and_stats_when_sending_unicasts(ctx, COMMIT_W_QP_ID, coalesce_num);
  zk_checks_and_print_when_forging_unicast(ctx, COMMIT_W_QP_ID);

#ifdef ZK_ENABLE_BQR
  bqr_rb_inc_last_issued_ts(&zk_ctx->b_ctx, coalesce_num);
#endif

  add_to_the_mirrored_buffer(qp_meta->mirror_remote_recv_fifo,
                             (uint8_t) coalesce_num, 1, ctx->q_info);
}


static inline void send_reads_helper(context_t *ctx)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[R_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  zk_r_mes_t *r_mes = (zk_r_mes_t *) get_fifo_pull_slot(send_fifo);
  uint16_t coalesce_num = get_fifo_slot_meta_pull(send_fifo)->coalesce_num;
  r_mes->coalesce_num = (uint8_t) coalesce_num;
  checks_and_stats_when_sending_unicasts(ctx, R_QP_ID, coalesce_num);
  zk_checks_and_print_when_forging_unicast(ctx, R_QP_ID);
}


static inline void send_r_reps_helper(context_t *ctx)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[R_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  zk_r_rep_mes_t *r_rep_mes = (zk_r_rep_mes_t *) get_fifo_pull_slot(send_fifo);
  uint16_t coalesce_num = get_fifo_slot_meta_pull(send_fifo)->coalesce_num;

  r_rep_mes->coalesce_num = (uint8_t) coalesce_num;

  if (DEBUG_READ_REPS)
    my_printf(yellow, "Wrkr %u SENDING R_REP: coalesce_num %u, l_id %lu, to m_id %u\n",
              ctx->t_id, r_rep_mes->coalesce_num, r_rep_mes->l_id,
              get_fifo_slot_meta_pull(send_fifo)->rm_id);


}

static inline void send_acks_helper(context_t *ctx)
{
  ctx_refill_recvs(ctx, COMMIT_W_QP_ID);
}

/* ---------------------------------------------------------------------------
//------------------------------ GID HANDLING -----------------------------
//---------------------------------------------------------------------------*/

// Propagates Updates that have seen all acks to the KVS
static inline void propagate_updates(context_t *ctx)
{
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  fifo_t *prep_buf_mirror = ctx->qp_meta[PREP_ACK_QP_ID].mirror_remote_recv_fifo;

  uint16_t update_op_i = 0;
  // remember the starting point to use it when writing the KVS
  uint32_t starting_pull_ptr = zk_ctx->w_rob->pull_ptr;
  uint64_t committed_g_id = atomic_load_explicit(&committed_global_w_id, memory_order_relaxed);
  flr_increase_counter_if_waiting_for_commit(zk_ctx, committed_g_id, ctx->t_id);
  while(((w_rob_t *) get_fifo_pull_slot(zk_ctx->w_rob))->w_state == READY) {
    if (!is_expected_g_id_ready(zk_ctx, &committed_g_id, &update_op_i,
                                ctx->t_id))
      break;
  }

  if (update_op_i > 0) {
    if (zk_ctx->protocol == FOLLOWER) {
      remove_from_the_mirrored_buffer(prep_buf_mirror, update_op_i,
                                      ctx->t_id, 0, FLR_PREP_BUF_SLOTS);
    }
    else ctx_insert_commit(ctx, COMMIT_W_QP_ID, update_op_i, zk_ctx->local_w_id);

    zk_ctx->local_w_id += update_op_i; // this must happen after inserting commits
    fifo_decrease_capacity(zk_ctx->w_rob, update_op_i);
    zk_KVS_batch_op_updates((uint16_t) update_op_i, zk_ctx, starting_pull_ptr,
                            ctx->t_id);

    if (ENABLE_GID_ORDERING)
      atomic_store_explicit(&committed_global_w_id, committed_g_id, memory_order_relaxed);
    zk_signal_completion_and_bookkeep_for_writes(zk_ctx, update_op_i, starting_pull_ptr,
                                                 zk_ctx->protocol, ctx->t_id);
  }
}


// Leader calls this to handout global ids to pending writes
static inline void zk_get_g_ids(context_t *ctx)
{
  if (!ENABLE_GIDS) return;
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
	uint16_t unordered_writes_num = (uint16_t) ((LEADER_PENDING_WRITES + zk_ctx->w_rob->push_ptr - zk_ctx->unordered_ptr)
																	% LEADER_PENDING_WRITES);
  if (unordered_writes_num == 0) return;
	uint64_t id = atomic_fetch_add_explicit(&global_w_id, (uint64_t) unordered_writes_num, memory_order_relaxed);
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].batches_per_thread++;
    t_stats[ctx->t_id].total_writes += unordered_writes_num;
  }

	for (uint16_t i = 0; i < unordered_writes_num; ++i) {
    assert(zk_ctx->unordered_ptr == ((LEADER_PENDING_WRITES + zk_ctx->w_rob->push_ptr - unordered_writes_num + i)
                                      % LEADER_PENDING_WRITES));
    w_rob_t *w_rob = (w_rob_t *) get_fifo_slot(zk_ctx->w_rob, zk_ctx->unordered_ptr);
		w_rob->g_id = id + i;
		zk_prepare_t *prep = w_rob->ptr_to_op;
		prep->g_id = w_rob->g_id;
    MOD_INCR(zk_ctx->unordered_ptr, LEADER_PENDING_WRITES);
	}
  zk_ctx->highest_g_id_taken = id + unordered_writes_num - 1;


	if (ENABLE_ASSERTIONS)
    assert(zk_ctx->unordered_ptr == zk_ctx->w_rob->push_ptr);

}

/* ---------------------------------------------------------------------------
//------------------------------POLL HANDLERS -----------------------------
//---------------------------------------------------------------------------*/


static inline bool ack_handler(context_t *ctx)
{
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_ACK_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile ctx_ack_mes_ud_t *incoming_acks = (volatile ctx_ack_mes_ud_t *) recv_fifo->fifo;
  ctx_ack_mes_t *ack = (ctx_ack_mes_t *) &incoming_acks[recv_fifo->pull_ptr].ack;
  uint32_t ack_num = ack->ack_num;
  uint64_t l_id = ack->l_id;
  uint64_t pull_lid = zk_ctx->local_w_id; // l_id at the pull pointer
  uint32_t ack_ptr; // a pointer in the FIFO, from where ack should be added
  zk_check_polled_ack_and_print(ack, ack_num, pull_lid, recv_fifo->pull_ptr, ctx->t_id);
  ctx_increase_credits_on_polling_ack(ctx, PREP_ACK_QP_ID, ack);

  // increasing COMMIT credits
  per_qp_meta_t *com_qp_meta = &ctx->qp_meta[COMMIT_W_QP_ID];
  com_qp_meta->credits[ack->m_id] = com_qp_meta->max_credits;

  if ((zk_ctx->w_rob->capacity == 0 ) ||
      (pull_lid >= l_id && (pull_lid - l_id) >= ack_num))
    return true;

  zk_check_ack_l_id_is_small_enough(ack_num, l_id, zk_ctx, pull_lid, ctx->t_id);
  ack_ptr = ctx_find_when_the_ack_points_acked(ack, zk_ctx->w_rob, pull_lid, &ack_num);

  // Apply the acks that refer to stored writes
  zk_apply_acks(ack_num, ack_ptr, l_id, zk_ctx, pull_lid,
                &qp_meta->outstanding_messages, ctx->t_id);

  return true;
}

static inline bool write_handler(context_t *ctx)
{
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  if (ENABLE_ASSERTIONS) assert(zk_ctx->w_rob->capacity < LEADER_PENDING_WRITES);
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COMMIT_W_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile zk_w_mes_ud_t *incoming_ws = (volatile zk_w_mes_ud_t *) qp_meta->recv_fifo->fifo;
  zk_w_mes_t *w_mes = (zk_w_mes_t *) &incoming_ws[recv_fifo->pull_ptr].w_mes;
  if (DEBUG_WRITES) printf("Leader sees a write Opcode %d at offset %d  \n",
                           w_mes->write[0].opcode, recv_fifo->pull_ptr);

  uint8_t w_num = w_mes->coalesce_num;
  if (zk_ctx->w_rob->capacity + w_num > LEADER_PENDING_WRITES) {
    return false;
  }
  for (uint16_t i = 0; i < w_num; i++) {
    zk_write_t *write = &w_mes->write[i];
    if (ENABLE_ASSERTIONS) if(write->opcode != KVS_OP_PUT)
        my_printf(red, "Opcode %u, i %u/%u \n", write->opcode, i, w_num);
    if (DEBUG_WRITES)
      printf("Poll for writes passes session id %u \n", write->sess_id);
    od_insert_mes(ctx, PREP_ACK_QP_ID, (uint32_t) PREP_SIZE, 1,
                  false, (void *) write, REMOTE_WRITE, 0);
    if (ENABLE_ASSERTIONS) write->opcode = 3;
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].received_writes += w_num;
    t_stats[ctx->t_id].received_writes_mes_num++;
  }
  return true;
}

static inline bool r_rep_handler(context_t *ctx)
{

  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[R_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile zk_r_rep_mes_ud_t *incoming_r_reps = (volatile zk_r_rep_mes_ud_t *) recv_fifo->fifo;
  zk_r_rep_mes_t *r_rep_mes = (zk_r_rep_mes_t *) &incoming_r_reps[recv_fifo->pull_ptr].r_rep_mes;
  fifo_t *r_rob_fifo = zk_ctx->r_rob;
  if (DEBUG_READ_REPS)
    my_printf(cyan, "WRKR %u: RECEIVING R_REP: l_id %u/%lu, coalesce_num %u \n",
              ctx->t_id, r_rep_mes->l_id, zk_ctx->local_r_id, r_rep_mes->coalesce_num);

  assert(r_rob_fifo->capacity > 0);
  uint64_t l_id = r_rep_mes->l_id;
  (*qp_meta->credits)++;
  uint16_t byte_ptr = R_REP_MES_HEADER;
  for (int r_rep_i = 0; r_rep_i < r_rep_mes->coalesce_num; ++r_rep_i) {
    r_rob_t *r_rob = (r_rob_t *) get_fifo_pull_slot(r_rob_fifo);

    zk_r_rep_big_t *r_rep = (zk_r_rep_big_t *) (((void *) r_rep_mes) + byte_ptr);

    if (DEBUG_READ_REPS)
      my_printf(yellow, "Wrkr: %u R_rep %u/%u opcode %u, session %u\n",
                ctx->t_id, r_rep_i, r_rep_mes->coalesce_num, r_rep->opcode, r_rob->sess_id);

    assert(r_rob->state == VALID);
    assert(r_rob->l_id == r_rep_mes->l_id + r_rep_i);
    byte_ptr += get_size_from_opcode(r_rep->opcode);
    uint8_t *value_to_read =  r_rep->opcode == G_ID_EQUAL ? r_rob->value : r_rep->value;
    memcpy(r_rob->value_to_read, value_to_read, VALUE_SIZE);
    zk_ctx->local_r_id++;

      if (USE_LIN_READS && ENABLE_LIN_READ_LATENCY &&
          zk_ctx->protocol == FOLLOWER && ctx->t_id == 0 &&
          zk_ctx->index_to_req_array[r_rob->sess_id] == 0)
      {
          stop_latency_measurement(&lt_cnt, &lt_timer);
      }

    zk_ctx->stalled[r_rob->sess_id] = false;
    zk_ctx->all_sessions_stalled = false;
    r_rob->state = INVALID;
    fifo_incr_pull_ptr(r_rob_fifo);
    fifo_decrem_capacity(r_rob_fifo);
  }

  return true;

}

static inline bool r_handler(context_t *ctx)
{

  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;

  per_qp_meta_t *qp_meta = &ctx->qp_meta[R_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile zk_r_mes_ud_t *incoming_reads = (volatile zk_r_mes_ud_t *) recv_fifo->fifo;
  zk_r_mes_t *r_mes = (zk_r_mes_t *) &incoming_reads[recv_fifo->pull_ptr].r_mes;
  uint8_t r_num = r_mes->coalesce_num;
  uint16_t byte_ptr = R_MES_HEADER;
  if (DEBUG_READS)
      my_printf(green, "WRKR %u RECEIVING READ MESSAGE: lid %u, coalesce num %u from %u \n",
                ctx->t_id, r_mes->l_id, r_mes->coalesce_num, r_mes->m_id);

  ptrs_to_r_t *ptrs_to_r = zk_ctx->ptrs_to_r;
  if (qp_meta->polled_messages == 0) ptrs_to_r->polled_reads = 0;
  for (uint16_t r_i = 0; r_i < r_num; r_i++) {
    zk_read_t *read = (zk_read_t*)(((void *) r_mes) + byte_ptr);
    //printf("Receiving read opcode %u \n", read->opcode);

    if (DEBUG_READS)
      my_printf(yellow, "wrkr %u Read %u, opcode %u, g_id %lu, key.bkt %u\n",
                ctx->t_id, r_i, read->opcode, read->g_id, read->key.bkt);

    if (ENABLE_ASSERTIONS) {
      assert(read->opcode == KVS_OP_GET);
      assert(read->key.bkt > 0);
    }
    ptrs_to_r->ptr_to_ops[ptrs_to_r->polled_reads] = read;
    ptrs_to_r->ptr_to_r_mes[ptrs_to_r->polled_reads] = r_mes;
    ptrs_to_r->coalesce_r_rep[ptrs_to_r->polled_reads] = r_i > 0;
    ptrs_to_r->polled_reads++;
    byte_ptr += R_SIZE;
  }

  return true;
}

static inline bool prepare_handler(context_t *ctx)
{
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_ACK_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile zk_prep_mes_ud_t *incoming_preps = (volatile zk_prep_mes_ud_t *) recv_fifo->fifo;
  zk_prep_mes_t *prep_mes = (zk_prep_mes_t *) &incoming_preps[recv_fifo->pull_ptr].prepare;

  uint8_t coalesce_num = prep_mes->coalesce_num;
  zk_prepare_t *prepare = prep_mes->prepare;
  uint64_t incoming_l_id = prep_mes->l_id;
  uint64_t expected_l_id = zk_ctx->local_w_id + zk_ctx->w_rob->capacity;

  if (qp_meta->mirror_remote_recv_fifo->capacity == MAX_PREP_BUF_SLOTS_TO_BE_POLLED) return false;
  if (zk_ctx->w_rob->capacity + coalesce_num > FLR_PENDING_WRITES) return false;
  zk_check_polled_prep_and_print(prep_mes, zk_ctx, coalesce_num, recv_fifo->pull_ptr,
                                 incoming_l_id, expected_l_id, incoming_preps, ctx->t_id);

  //zk_ctx->p_acks->acks_to_send+= coalesce_num; // lids are in order so ack them
  ctx_ack_insert(ctx, PREP_ACK_QP_ID, coalesce_num,  incoming_l_id, LEADER_MACHINE);

  add_to_the_mirrored_buffer(qp_meta->mirror_remote_recv_fifo,
                             coalesce_num, 1, ctx->q_info);
  ///Loop through prepares inside the message
  for (uint8_t prep_i = 0; prep_i < coalesce_num; prep_i++) {
    zk_check_prepare_and_print(&prepare[prep_i], zk_ctx, prep_i, ctx->t_id);
    fill_zk_ctx_entry(zk_ctx, &prepare[prep_i], ctx->m_id, ctx->t_id);
    fifo_incr_push_ptr(zk_ctx->w_rob);
    fifo_increm_capacity(zk_ctx->w_rob);
  } ///

  if (ENABLE_ASSERTIONS) prep_mes->opcode = 0;

  return true;
}

static inline bool commit_handler(context_t *ctx)
{
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COMMIT_W_QP_ID];
  fifo_t *recv_fifo = qp_meta->recv_fifo;
  volatile ctx_com_mes_ud_t *incoming_coms = (volatile ctx_com_mes_ud_t *) recv_fifo->fifo;

  ctx_com_mes_t *com = (ctx_com_mes_t *) &incoming_coms[recv_fifo->pull_ptr].com;
  uint32_t com_num = com->com_num;
  uint64_t l_id = com->l_id;
  uint64_t pull_lid = zk_ctx->local_w_id; // l_id at the pull pointer
  zk_check_polled_commit_and_print(com, zk_ctx, recv_fifo->pull_ptr,
                                   l_id, pull_lid, com_num, ctx->t_id);
  // This must always hold: l_id >= pull_lid,
  // because we need the commit to advance the pull_lid
  uint16_t com_ptr = (uint16_t)
    ((zk_ctx->w_rob->pull_ptr + (l_id - pull_lid)) % FLR_PENDING_WRITES);
  /// loop through each commit
  for (uint16_t com_i = 0; com_i < com_num; com_i++) {
    if (zk_write_not_ready(com, com_ptr, com_i, com_num, zk_ctx, ctx->t_id)) {
      return false;
    }

    assert(l_id + com_i - pull_lid < FLR_PENDING_WRITES);
    w_rob_t *w_rob = (w_rob_t *) get_fifo_slot(zk_ctx->w_rob, com_ptr);
    w_rob->w_state = READY;
    flr_increases_write_credits(ctx, zk_ctx, com_ptr, qp_meta->mirror_remote_recv_fifo);
    MOD_INCR(com_ptr, FLR_PENDING_WRITES);
  } ///

  if (ENABLE_ASSERTIONS) com->opcode = 0;

  if (ENABLE_STAT_COUNTING) {
    t_stats[ctx->t_id].received_coms += com_num;
    t_stats[ctx->t_id].received_coms_mes_num++;
  }
  if (recv_fifo->pull_ptr % FLR_CREDITS_IN_MESSAGE == 0)
    ;//send_credits_for_commits(ctx, 1);

  return true;
}


/* ---------------------------------------------------------------------------
//------------------------------ BROADCASTS -----------------------------
//---------------------------------------------------------------------------*/
static inline void send_commits_helper(context_t *ctx)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[COMMIT_W_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  ctx_com_mes_t *com_mes = (ctx_com_mes_t *) get_fifo_pull_slot(send_fifo);

  if (DEBUG_COMMITS)
    my_printf(green, "Wrkr %u, Broadcasting commit %u, lid %lu, com_num %u \n",
              ctx->t_id, com_mes->opcode, com_mes->l_id, com_mes->com_num);

  zk_checks_and_stats_on_bcasting_commits(send_fifo, com_mes, ctx->t_id);
}


static inline void send_prepares_helper(context_t *ctx)
{
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[PREP_ACK_QP_ID];
  fifo_t *send_fifo = qp_meta->send_fifo;
  if (DEBUG_PREPARES)
    printf("LDR %d has %u bcasts to send credits %d\n", ctx->t_id,
           send_fifo->net_capacity, qp_meta->credits[1]);
  // Create the broadcast messages
  zk_prep_mes_t *prep_buf = (zk_prep_mes_t *) qp_meta->send_fifo->fifo;
  zk_prep_mes_t *prep = &prep_buf[send_fifo->pull_ptr];

  slot_meta_t *slot_meta = get_fifo_slot_meta_pull(send_fifo);
  uint8_t coalesce_num = (uint8_t) slot_meta->coalesce_num;
  prep->coalesce_num = (uint8_t) slot_meta->coalesce_num;
  uint32_t backward_ptr = fifo_get_pull_backward_ptr(send_fifo);

  for (uint16_t i = 0; i < coalesce_num; i++) {
    w_rob_t *w_rob = (w_rob_t *) get_fifo_slot_mod(zk_ctx->w_rob, backward_ptr + i);
    w_rob->w_state = SENT;

#ifdef ZK_ENABLE_BQR
    if(w_rob->is_local) bqr_rb_inc_last_issued_ts(&zk_ctx->b_ctx, 1);
#endif
    if (DEBUG_PREPARES)
      printf("Prepare %d, val-len %u, total message capacity %d\n", i, prep->prepare[i].val_len,
             slot_meta->byte_size);
    if (ENABLE_ASSERTIONS) {
      assert(prep->prepare[i].val_len == VALUE_SIZE >> SHIFT_BITS);
      assert(prep->prepare[i].opcode == KVS_OP_PUT);
    }
  }

  if (DEBUG_PREPARES)
    my_printf(green, "Leader %d : I BROADCAST a prepare message %d of "
                "%u prepares with total w_size %u,  with  credits: %d, lid: %lu  \n",
              ctx->t_id, prep->opcode, coalesce_num, slot_meta->byte_size,
              qp_meta->credits[0], prep->l_id);

  zk_checks_and_stats_on_bcasting_prepares(send_fifo, coalesce_num,
                                           &qp_meta->outstanding_messages, ctx->t_id);
}

/* ---------------------------------------------------------------------------
//------------------------------ MAIN LOOPS -----------------------------
//---------------------------------------------------------------------------*/

static inline void flr_main_loop(context_t *ctx)
{

  if (PUT_A_MACHINE_TO_SLEEP && (machine_id == MACHINE_THAT_SLEEPS) &&
      (t_stats[WORKERS_PER_MACHINE - 1].total_reqs > 4 * MILLION)) {
    if (ctx->t_id == 0) my_printf(yellow, "Machine performs scheduled failure\n");
    exit(0);
  }

  ctx_poll_incoming_messages(ctx, PREP_ACK_QP_ID);

  //send_acks_to_ldr(ctx);
  od_send_acks(ctx, PREP_ACK_QP_ID);


  ctx_poll_incoming_messages(ctx, COMMIT_W_QP_ID);


  propagate_updates(ctx);


  ctx_poll_incoming_messages(ctx, R_QP_ID);


  zk_batch_from_trace_to_KVS(ctx);


  ctx_send_unicasts(ctx, COMMIT_W_QP_ID);


  ctx_send_unicasts(ctx, R_QP_ID);

#ifdef ZK_ENABLE_BQR
    zk_KVS_batch_bqr_reads(ctx, MAX_BQR_READS_FOR_SINGLE_INSERT);
#endif

}


static inline void ldr_main_loop(context_t *ctx)
{

  //printf ("loop \n");

  ldr_check_debug_cntrs(ctx);

  ctx_poll_incoming_messages(ctx, PREP_ACK_QP_ID);

  propagate_updates(ctx);
  check_ldr_p_states(ctx);

  ctx_send_broadcasts(ctx, COMMIT_W_QP_ID);

  // Get a new batch from the trace, pass it through the cache and create
  // the appropriate prepare messages
  zk_batch_from_trace_to_KVS(ctx);

  // get local and remote writes back to back to increase the write batch
  ctx_poll_incoming_messages(ctx, COMMIT_W_QP_ID);

  // Assign a global write  id to each new write
  zk_get_g_ids(ctx);


  ctx_send_broadcasts(ctx, PREP_ACK_QP_ID);


  ctx_poll_incoming_messages(ctx, R_QP_ID);


  ctx_send_unicasts(ctx, R_QP_ID);

#ifdef ZK_ENABLE_BQR
    zk_KVS_batch_bqr_reads(ctx, MAX_BQR_READS_FOR_SINGLE_INSERT);
#endif
}

static inline void zk_main_loop(context_t *ctx)
{
  zk_ctx_t * zk_ctx =(zk_ctx_t *) ctx->appl_ctx;

#ifdef ZK_ENABLE_BQR
    bqr_ctx_init(&zk_ctx->b_ctx, ctx->t_id);
#endif

  while (true) {
    switch (zk_ctx->protocol) {
      case FOLLOWER:
        flr_main_loop(ctx);
        break;
      case LEADER:
        ldr_main_loop(ctx);
        break;
    }
  }
}

#endif /* INLINE_UTILS_H */
