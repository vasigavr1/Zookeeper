//
// Created by vasilis on 30/06/20.
//

#ifndef KITE_ZK_DEBUG_UTIL_H
#define KITE_ZK_DEBUG_UTIL_H

#include "../../../odlib/include/network_api/od_network_context.h"
#include "zk_main.h"
/* ---------------------------------------------------------------------------
//------------------------------ ZOOKEEPER DEBUGGING -----------------------------
//---------------------------------------------------------------------------*/
static inline char* w_state_to_str(w_state_t state)
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


static inline char* prot_to_str(protocol_t protocol)
{
  switch (protocol){
    case FOLLOWER:
      return "FOLLOWER";
    case LEADER:
      return "LEADER";
    default: if (ENABLE_ASSERTIONS) assert(false);
  }
}

static inline void print_ldr_stats (uint16_t t_id)
{

  my_printf(yellow, "Prepares sent %ld/%ld \n", t_stats[t_id].prep_sent_mes_num, t_stats[t_id].preps_sent );
  my_printf(yellow, "Acks Received %ld/%ld/%ld \n",
            t_stats[t_id].received_acks_mes_num, t_stats[t_id].received_acks,
            t_stats[t_id].received_acks / FOLLOWER_MACHINE_NUM);
  my_printf(yellow, "Commits sent %ld/%ld \n", t_stats[t_id].coms_sent_mes_num, t_stats[t_id].coms_sent );
}

static inline void print_flr_stats (uint16_t t_id)
{

  my_printf(yellow, "Prepares received %ld/%ld \n", t_stats[t_id].received_preps_mes_num, t_stats[t_id].received_preps );
  my_printf(yellow, "Acks sent %ld/%ld \n", t_stats[t_id].acks_sent_mes_num, t_stats[t_id].acks_sent );
  my_printf(yellow, "Commits received %ld/%ld \n", t_stats[t_id].received_coms_mes_num, t_stats[t_id].received_coms );
}


static inline void zk_print_error_message(const char *mes, protocol_t protocol,
                                          zk_ctx_t *zk_ctx, uint16_t t_id)
{
  uint64_t anticipated_g_id = 0;
  w_rob_t * w_rob = (w_rob_t *) get_fifo_pull_slot(zk_ctx->w_rob);

  if (w_rob->w_state == READY)
    anticipated_g_id = w_rob->g_id;

  my_printf(red, "%s %u %s, committed g_id %lu, ",
            prot_to_str(protocol), t_id, mes,
            committed_global_w_id);
  if (protocol == LEADER)
    my_printf(red, "highest g_id %u, ",
            zk_ctx->highest_g_id_taken);

  if (anticipated_g_id > 0)
    my_printf(red, "anticipated g_id %u",
              anticipated_g_id);
  my_printf(red, "\n");
}

// Leader checks its debug counters
static inline void ldr_check_debug_cntrs(context_t *ctx)
{
  if (!ENABLE_ASSERTIONS) return;
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  uint32_t patience =  M_32;
  if (unlikely((zk_ctx->wait_for_gid_dbg_counter) > patience)) {
    zk_print_error_message("waits for the g_id", LEADER, zk_ctx, ctx->t_id);
    print_ldr_stats(ctx->t_id);
    zk_ctx->wait_for_gid_dbg_counter = 0;
  }
  for (int qp_i = 0; qp_i < ctx->qp_num; ++qp_i) {
    per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_i];
    if (qp_meta->wait_for_reps_ctr > patience) {
      zk_print_error_message(qp_meta->send_string, LEADER, zk_ctx, ctx->t_id);
      qp_meta->wait_for_reps_ctr = 0;
    }

    if (qp_meta->time_out_cnt > 0 && qp_meta->time_out_cnt % patience == 0) {
      zk_print_error_message(qp_meta->send_string, LEADER, zk_ctx, ctx->t_id);
      print_ldr_stats(ctx->t_id);
    }
  }

  zk_prep_mes_t *preps = (zk_prep_mes_t *) ctx->qp_meta[PREP_ACK_QP_ID].send_fifo->fifo;
  for (int i = 0; i < PREP_FIFO_SIZE; i++) {
    assert(preps[i].opcode == KVS_OP_PUT);
    for (uint16_t j = 0; j < MAX_PREP_COALESCE; j++) {
      assert(preps[i].prepare[j].opcode == KVS_OP_PUT);
      assert(preps[i].prepare[j].val_len == VALUE_SIZE >> SHIFT_BITS);
    }
  }
}

// Follower checks its debug counters
static inline void flr_check_debug_cntrs(uint32_t *credit_debug_cnt, uint32_t *wait_for_coms_dbg_counter,
                                         uint32_t *wait_for_preps_dbg_counter,
                                         uint32_t *wait_for_gid_dbg_counter, volatile zk_prep_mes_ud_t *prep_buf,
                                         uint32_t pull_ptr, zk_ctx_t *zk_ctx, uint16_t t_id)
{
  uint32_t waiting_time = M_256;
  if (unlikely((*wait_for_preps_dbg_counter) > waiting_time)) {
    my_printf(red, "Follower %d waits for preps, committed g_id %lu \n", t_id, committed_global_w_id);
    zk_prepare_t *prep = (zk_prepare_t *)&prep_buf[pull_ptr].prepare.prepare;
    uint64_t l_id = prep_buf[pull_ptr].prepare.l_id;
    uint64_t g_id = prep->g_id;
    uint8_t message_opc = prep_buf[pull_ptr].prepare.opcode;
    my_printf(cyan, "Flr %d, polling on index %u,polled opc %u, 1st write opcode: %u, l_id %u, first g_id %u, expected l_id %u\n",
              t_id, pull_ptr, message_opc, prep->opcode, l_id, g_id, zk_ctx->local_w_id);
    MOD_INCR(pull_ptr, FLR_PREP_BUF_SLOTS);
    prep = (zk_prepare_t *)&prep_buf[pull_ptr].prepare.prepare;
    l_id = prep_buf[pull_ptr].prepare.l_id;
    g_id = prep->g_id;
    message_opc = prep_buf[pull_ptr].prepare.opcode;
    my_printf(cyan, "Next index %u,polled opc %u, 1st write opcode: %u, l_id %u, first g_id %u, expected l_id %u\n",
              pull_ptr, message_opc, prep->opcode, l_id, g_id, zk_ctx->local_w_id);
    for (int i = 0; i < FLR_PREP_BUF_SLOTS; ++i) {
      if (prep_buf[i].prepare.opcode == KVS_OP_PUT) {
        my_printf(green, "GOOD OPCODE in index %d, l_id %lu \n", i, prep_buf[i].prepare.l_id);
      }
      else my_printf(red, "BAD OPCODE in index %d, l_id %lu \n", i, prep_buf[i].prepare.l_id);

    }

    print_flr_stats(t_id);
    (*wait_for_preps_dbg_counter) = 0;
//    exit(0);
  }
  if (unlikely((*wait_for_gid_dbg_counter) > waiting_time)) {
    zk_print_error_message("waits for the g_id", FOLLOWER, zk_ctx, t_id);
    print_flr_stats(t_id);
    (*wait_for_gid_dbg_counter) = 0;
  }
  if (unlikely((*wait_for_coms_dbg_counter) > waiting_time)) {
    zk_print_error_message("waits for coms", FOLLOWER, zk_ctx, t_id);
    print_flr_stats(t_id);
    (*wait_for_coms_dbg_counter) = 0;
  }
  if (unlikely((*credit_debug_cnt) > waiting_time)) {
    zk_print_error_message("acks write credits", FOLLOWER, zk_ctx, t_id);
    print_flr_stats(t_id);
    (*credit_debug_cnt) = 0;
  }
}

// Check the states of pending writes
static inline void check_ldr_p_states(context_t *ctx)
{

  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  assert(zk_ctx->w_rob->capacity <= LEADER_PENDING_WRITES);
  for (uint32_t w_i = 0; w_i < LEADER_PENDING_WRITES - zk_ctx->w_rob->capacity; w_i++) {
    uint32_t ptr = (zk_ctx->w_rob->push_ptr + w_i);
    w_rob_t *w_rob = (w_rob_t *) get_fifo_slot_mod(zk_ctx->w_rob, ptr);
    if (w_rob->w_state != INVALID) {
      my_printf(red, "LDR %d push ptr %u, pull ptr %u, capacity %u, state %d at ptr %u \n",
                ctx->t_id, zk_ctx->w_rob->push_ptr, zk_ctx->w_rob->pull_ptr,
                zk_ctx->w_rob->capacity, w_rob->w_state, ptr);
      print_ldr_stats(ctx->t_id);
      assert(false);
    }
  }
}





/* ---------------------------------------------------------------------------
//------------------------------ POLLNG ACKS -----------------------------
//---------------------------------------------------------------------------*/

static inline void zk_check_polled_ack_and_print(ctx_ack_mes_t *ack, uint32_t ack_num,
                                                 uint64_t pull_lid, uint32_t buf_ptr, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert (ack->opcode == OP_ACK);
    assert(ack_num > 0 && ack_num <= FLR_PENDING_WRITES);
    assert(ack->m_id < MACHINE_NUM);
    assert(ack->m_id != machine_id);
  }
  if (DEBUG_ACKS)
    my_printf(yellow, "Leader %d ack opcode %d with %d acks for l_id %lu, oldest lid %lu, at offset %d from flr %u \n",
              t_id, ack->opcode, ack_num, ack->l_id, pull_lid, buf_ptr, ack->m_id);
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].received_acks += ack_num;
    t_stats[t_id].received_acks_mes_num++;
  }

}

static inline void zk_check_ack_l_id_is_small_enough(uint32_t ack_num,
                                                     uint64_t l_id, zk_ctx_t *zk_ctx,
                                                     uint64_t pull_lid, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(l_id + ack_num <= pull_lid + zk_ctx->w_rob->capacity);
    if ((l_id + ack_num < pull_lid) && (!USE_QUORUM)) {
      my_printf(red, "l_id %u, ack_num %u, pull_lid %u \n", l_id, ack_num, pull_lid);
      assert(false);
    }
  }
}

static inline void zk_debug_info_bookkeep(context_t *ctx,
                                          uint16_t qp_id,
                                          int completed_messages)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_id];
  if (qp_meta->recv_type == RECV_REPLY) {
    if (qp_meta->polled_messages > 0) {
      if (ENABLE_ASSERTIONS) qp_meta->wait_for_reps_ctr = 0;
    }
    else {
      if (qp_meta->outstanding_messages > 0) {
        if (ENABLE_ASSERTIONS) qp_meta->wait_for_reps_ctr++;
        if (ENABLE_STAT_COUNTING && qp_id == PREP_ACK_QP_ID) t_stats[ctx->t_id].stalled_ack_prep++;
      }
    }
  }
  if (ENABLE_ASSERTIONS) {
    assert(qp_meta->recv_info->posted_recvs >= qp_meta->polled_messages);
    //assert(qp_meta->recv_info->posted_recvs <= qp_meta->recv_wr_num);
  }
}

/* ---------------------------------------------------------------------------
//------------------------------ POLLNG COMMITS -----------------------------
//---------------------------------------------------------------------------*/


static inline void zk_check_polled_commit_and_print(ctx_com_mes_t *com,
                                                    zk_ctx_t *zk_ctx,
                                                    uint32_t buf_ptr,
                                                    uint64_t l_id,
                                                    uint64_t pull_lid,
                                                    uint32_t com_num,
                                                    uint16_t t_id)
{
  if (DEBUG_COMMITS)
    my_printf(yellow, "Flr %d com opcode %d with %d coms for l_id %lu, "
              "oldest lid %lu, at offset %d at address %p \n",
              t_id, com->opcode, com_num, l_id, pull_lid, buf_ptr, (void*) com);
  if (ENABLE_ASSERTIONS) {
    assert(com->opcode == KVS_OP_PUT);
    if ((pull_lid > l_id) ||
       ((l_id + com_num > pull_lid + zk_ctx->w_rob->capacity) &&
       (!USE_QUORUM))) {
      my_printf(red, "Flr %d, COMMIT: received lid %lu, com_num %u, pull_lid %lu, zk_ctx capacity  %u \n",
                t_id, l_id, com_num, pull_lid, zk_ctx->w_rob->capacity);
      print_ldr_stats(t_id);
      assert(false);
    }
    assert(com_num > 0 && com_num <= MAX_LIDS_IN_A_COMMIT);
  }
}


static inline void zk_checks_after_polling_commits(uint32_t *dbg_counter,
                                                   uint32_t polled_messages,
                                                   recv_info_t *com_recv_info)
{
  if (polled_messages > 0) {
    if (ENABLE_ASSERTIONS) (*dbg_counter) = 0;
  }
  if (ENABLE_ASSERTIONS) assert(com_recv_info->posted_recvs >= polled_messages);
}

/* ---------------------------------------------------------------------------
//------------------------------ POLLNG PREPARES -----------------------------
//---------------------------------------------------------------------------*/


static inline void zk_increment_wait_for_preps_cntr(zk_ctx_t *zk_ctx,
                                                    uint32_t *wait_for_prepares_dbg_counter)
{
  if (ENABLE_ASSERTIONS && zk_ctx->w_rob->capacity == 0)
    (*wait_for_prepares_dbg_counter)++;
}


static inline void zk_check_polled_prep_and_print(zk_prep_mes_t* prep_mes,
                                                  zk_ctx_t *zk_ctx,
                                                  uint8_t coalesce_num,
                                                  uint32_t buf_ptr,
                                                  uint64_t incoming_l_id,
                                                  uint64_t expected_l_id,
                                                  volatile zk_prep_mes_ud_t *incoming_preps,
                                                  uint16_t t_id)
{
  if (DEBUG_PREPARES)
    my_printf(green, "Flr %d sees a prep_mes message with %d prepares at index %u l_id %u, expected lid %lu \n",
              t_id, coalesce_num, index, incoming_l_id, expected_l_id);
  if (FLR_DISALLOW_OUT_OF_ORDER_PREPARES && ENABLE_ASSERTIONS) {
    if (expected_l_id != (uint64_t) incoming_l_id) {
      my_printf(red, "flr %u expected l_id  %lu and received %u \n",
                t_id, expected_l_id, incoming_l_id);
      uint32_t dbg = B_4_ ;
      flr_check_debug_cntrs(&dbg, &dbg, &dbg, &dbg, incoming_preps, buf_ptr, zk_ctx, t_id);
      //print_flr_stats(t_id);
      assert(false);
    }
  }
  if (ENABLE_ASSERTIONS) {
    assert(prep_mes->opcode == KVS_OP_PUT);
    assert(expected_l_id <= (uint64_t) incoming_l_id);
    assert(coalesce_num > 0 && coalesce_num <= MAX_PREP_COALESCE);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].received_preps += coalesce_num;
    t_stats[t_id].received_preps_mes_num++;
  }
}


static inline void
zk_check_prepare_and_print(zk_prepare_t *prepare,
                           zk_ctx_t* zk_ctx,
                           uint8_t prep_i,
                           uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(prepare->sess_id < SESSIONS_PER_THREAD);
    assert(prepare->flr_id <= MACHINE_NUM);
    if (ENABLE_GID_ORDERING)
      assert(prepare->g_id > committed_global_w_id);
    assert(prepare->val_len == VALUE_SIZE >> SHIFT_BITS);
    assert(((w_rob_t *) get_fifo_push_slot(zk_ctx->w_rob))->w_state == INVALID);

  }
  if (DEBUG_PREPARES)
    my_printf(green, "Flr %u, prep_i %u new write at ptr %u with g_id %lu and flr id %u, value_len %u \n",
              t_id, prep_i, zk_ctx->w_rob->push_ptr, prepare->g_id, prepare->flr_id, prepare[prep_i].val_len);
}


static inline void zk_checks_after_polling_prepares(zk_ctx_t *zk_ctx,
                                                    uint32_t *wait_for_prepares_dbg_counter,
                                                    uint32_t polled_messages,
                                                    recv_info_t *prep_recv_info,
                                                    uint16_t t_id)
{


  if (polled_messages > 0) {
    if (ENABLE_ASSERTIONS) (*wait_for_prepares_dbg_counter) = 0;
  }
  if (ENABLE_STAT_COUNTING && zk_ctx->w_rob->capacity == 0) t_stats[t_id].stalled_ack_prep++;
  if (ENABLE_ASSERTIONS) assert(prep_recv_info->posted_recvs >= polled_messages);
  if (ENABLE_ASSERTIONS) assert(prep_recv_info->posted_recvs <= FLR_MAX_RECV_PREP_WRS);
}


/* ---------------------------------------------------------------------------
//------------------------------ BROADCASTS -----------------------------
//---------------------------------------------------------------------------*/



static inline void zk_ckecks_when_creating_commits(context_t *ctx,
                                                   uint16_t update_op_i)
{
  if (!ENABLE_ASSERTIONS) return;

  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  fifo_t *send_fifo = ctx->qp_meta[COMMIT_W_QP_ID].send_fifo;
  ctx_com_mes_t *commit = (ctx_com_mes_t *) get_fifo_push_prev_slot(send_fifo);
  slot_meta_t *slot_meta = get_fifo_slot_meta_push(send_fifo);

  assert(update_op_i > 0);
  assert(commit->opcode == KVS_OP_PUT);
  assert(send_fifo->mes_header == CTX_COM_SEND_SIZE);
  assert(slot_meta->byte_size == CTX_COM_SEND_SIZE);

  if (send_fifo->capacity > 0) {
    if (ENABLE_ASSERTIONS) {
      assert(commit->l_id == zk_ctx->local_w_id);
      assert(commit->com_num > 0);
    }
    if (ENABLE_ASSERTIONS) assert(commit->com_num + update_op_i <= MAX_LIDS_IN_A_COMMIT);
  }




}

static inline void zk_checks_and_stats_on_bcasting_prepares(fifo_t *send_fifo,
                                                            uint8_t coalesce_num,
                                                            uint32_t *outstanding_prepares,
                                                            uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(send_fifo->net_capacity >= coalesce_num);
    (*outstanding_prepares) +=
      coalesce_num;
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].preps_sent +=
      coalesce_num;
    t_stats[t_id].prep_sent_mes_num++;
  }
}


static inline void zk_checks_and_stats_on_bcasting_commits(fifo_t *send_fifo,
                                                           ctx_com_mes_t *com_mes,
                                                           uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(com_mes->com_num == get_fifo_slot_meta_pull(send_fifo)->coalesce_num);
    assert(send_fifo->net_capacity >= com_mes->com_num);

    assert(send_fifo != NULL);
    if (send_fifo->capacity > COMMIT_FIFO_SIZE)
      printf("com fifo capacity %u/%d \n", send_fifo->capacity, COMMIT_FIFO_SIZE);
    assert(send_fifo->capacity <= COMMIT_FIFO_SIZE);
    assert(com_mes->com_num > 0 && com_mes->com_num <= MAX_LIDS_IN_A_COMMIT);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].coms_sent += com_mes->com_num;
    t_stats[t_id].coms_sent_mes_num++;
  }
}





/* ---------------------------------------------------------------------------
//------------------------------ UNICASTS------- -----------------------------
//---------------------------------------------------------------------------*/


static inline void check_stats_prints_when_sending_acks(ctx_ack_mes_t *ack,
                                                        zk_ctx_t *zk_ctx,
                                                        uint64_t l_id_to_send, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(ack->l_id == l_id_to_send);
    //assert (p_acks->slots_ahead <= zk_ctx->w_rob->capacity);
  }
  if (ENABLE_STAT_COUNTING) {
    t_stats[t_id].acks_sent += ack->ack_num;
    t_stats[t_id].acks_sent_mes_num++;
  }

  if (DEBUG_ACKS)
    my_printf(yellow, "Flr %d is sending an ack for lid %lu and ack num %d and flr id %d, zk_ctx capacity %u/%d \n",
              t_id, l_id_to_send, ack->ack_num, ack->m_id, zk_ctx->w_rob->capacity, FLR_PENDING_WRITES);
  if (ENABLE_ASSERTIONS) assert(ack->ack_num > 0 && ack->ack_num <= FLR_PENDING_WRITES);
}

static inline void checks_and_prints_posting_recvs_for_preps(recv_info_t *prep_recv_info,
                                                             uint32_t recvs_to_post_num,
                                                             uint16_t t_id)
{
  //printf("FLR %d posting %u recvs and has a total of %u recvs for prepares \n",
  //       t_id, recvs_to_post_num,  prep_recv_info->posted_recvs);
  if (ENABLE_ASSERTIONS) {
    assert(recvs_to_post_num <= FLR_MAX_RECV_PREP_WRS);
    assert(prep_recv_info->posted_recvs <= FLR_MAX_RECV_PREP_WRS);
  }
}


static inline void zk_checks_and_print_when_forging_unicast(context_t *ctx, uint16_t qp_id)
{
  if (!ENABLE_ASSERTIONS) return;
  per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_id];
  fifo_t *send_fifo = qp_meta->send_fifo;


  uint16_t coalesce_num = get_fifo_slot_meta_pull(send_fifo)->coalesce_num;
  uint32_t length = get_fifo_slot_meta_pull(send_fifo)->byte_size;
  if (qp_id == COMMIT_W_QP_ID) {
    zk_w_mes_t *w_mes = (zk_w_mes_t *) get_fifo_pull_slot(send_fifo);
    assert(coalesce_num == w_mes->coalesce_num);
    for (uint16_t i = 0; i < coalesce_num; i++) {
      if (DEBUG_WRITES)
        printf("Write %d, session id %u, val-len %u, message capacity %d\n", i,
               w_mes->write[i].sess_id,
               w_mes->write[i].val_len,
               length);
      if (ENABLE_ASSERTIONS) {
        assert(w_mes->write[i].val_len == VALUE_SIZE >> SHIFT_BITS);
        assert(w_mes->write[i].opcode == KVS_OP_PUT);
      }
    }
    if (DEBUG_WRITES)
      my_printf(green, "Follower %d : I sent a write message %d of %u writes with capacity %u,  with  credits: %d \n",
                ctx->t_id, w_mes->write->opcode, coalesce_num, length, *qp_meta->credits);
  }
  else {
    zk_r_mes_t *r_mes = (zk_r_mes_t *) get_fifo_pull_slot(send_fifo);
    assert(coalesce_num == r_mes->coalesce_num);
    for (uint16_t i = 0; i < coalesce_num; i++) {
      if (DEBUG_READS)
        printf("READ %d/%u, g_id id %lu, for_key %u, message capacity %d\n",
               i,
               coalesce_num,
               r_mes->read[i].g_id,
               r_mes->read[i].key.bkt,
               length);
      if (ENABLE_ASSERTIONS) {
        /// it is possible for the read to have a slightly bigger g_id
        if (ENABLE_GID_ORDERING) {
          uint64_t max_allowed_g_id = committed_global_w_id + THOUSAND;// + (5 * THOUSAND);
          //assert(r_mes->read[i].g_id <= committed_global_w_id + (5 * THOUSAND));
          if (r_mes->read[i].g_id >= max_allowed_g_id) {
            my_printf(red, "Sending a read with a g_id %lu/%lu \n",
                      r_mes->read[i].g_id, max_allowed_g_id);
          }
          //assert(r_mes->read[i].g_id <= committed_global_w_id);
        }
        assert(r_mes->read[i].key.bkt > 0);
      }
    }
    assert(r_mes->m_id == ctx->m_id);
    if (DEBUG_READS)
      my_printf(green, "Follower %d : I sent a read message, coalesce num %u,  lid %lu , byte-size %u,   credits: %d \n",
                ctx->t_id, coalesce_num, r_mes->l_id,  length, *qp_meta->credits);
  }
}


static inline void checks_and_stats_when_sending_unicasts(context_t *ctx,
                                                          uint16_t qp_id,
                                                          uint16_t coalesce_num)
{
  per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_id];
  if (ENABLE_ASSERTIONS) {

    assert(qp_meta->send_fifo->net_capacity >= coalesce_num);
    qp_meta->outstanding_messages += coalesce_num;
  }

  if (qp_id == COMMIT_W_QP_ID) {
    if (DEBUG_WRITES)
      printf("FLR %d has %u writes to send credits %d\n", ctx->t_id,
             qp_meta->send_fifo->net_capacity, *qp_meta->credits);

    if (ENABLE_STAT_COUNTING) {
      t_stats[ctx->t_id].writes_sent += coalesce_num;
      t_stats[ctx->t_id].writes_sent_mes_num++;
    }
  }
  else {
    if (DEBUG_READS)
      printf("FLR %d has %u reads to send, credits %d\n", ctx->t_id,
             qp_meta->send_fifo->net_capacity, *qp_meta->credits);

    if (ENABLE_STAT_COUNTING) {
      t_stats[ctx->t_id].reads_sent += coalesce_num;
      t_stats[ctx->t_id].reads_sent_mes_num++;
    }
  }
}


static inline void check_unicast_before_send(context_t *ctx,
                                             uint8_t rm_id,
                                             uint16_t qp_id)
{
  if (ENABLE_ASSERTIONS) {
    per_qp_meta_t *qp_meta = &ctx->qp_meta[qp_id];
    assert(qp_meta->send_qp == ctx->cb->dgram_qp[qp_id]);
    assert(qp_meta->send_wr[0].sg_list == &qp_meta->send_sgl[0]);
    if (qp_meta->receipient_num == 0)
      assert(qp_meta->send_wr[0].wr.ud.ah == rem_qp[rm_id][ctx->t_id][qp_id].ah);
    assert(qp_meta->send_wr[0].opcode == IBV_WR_SEND);
    assert(qp_meta->send_wr[0].num_sge == 1);
    if (!qp_meta->enable_inlining) {
      //assert(qp_meta->send_wr[0].sg_list->lkey == qp_meta->send_mr->lkey);
      //assert(qp_meta->send_wr[0].send_flags == IBV_SEND_SIGNALED);
      assert(!qp_meta->enable_inlining);
    }
  }
}


/* ---------------------------------------------------------------------------
//------------------------------TRACE --------------------------------
//---------------------------------------------------------------------------*/


static inline void checks_when_leader_creates_write(zk_prep_mes_t *preps, uint32_t prep_ptr,
                                                    uint32_t inside_prep_ptr, zk_ctx_t *zk_ctx,
                                                    uint32_t w_ptr, uint16_t t_id)
{
  if (ENABLE_ASSERTIONS) {
    if (inside_prep_ptr == 0) {
      uint64_t message_l_id = preps[prep_ptr].l_id;
      if (message_l_id > MAX_PREP_COALESCE) {
        uint32_t prev_prep_ptr = (prep_ptr + PREP_FIFO_SIZE - 1) % PREP_FIFO_SIZE;
        uint64_t prev_l_id = preps[prev_prep_ptr].l_id;
        uint8_t prev_coalesce = preps[prev_prep_ptr].coalesce_num;
        if (message_l_id != prev_l_id + prev_coalesce) {
          my_printf(red, "Current message l_id %u, previous message l_id %u , previous coalesce %u\n",
                    message_l_id, prev_l_id, prev_coalesce);
        }
      }
    }
    w_rob_t * w_rob = (w_rob_t *) get_fifo_slot(zk_ctx->w_rob, w_ptr);

    if (w_rob->w_state != INVALID)
      my_printf(red, "Leader %u w_state %d at w_ptr %u, g_id %lu, cache hits %lu, capacity %u \n",
                t_id, w_rob->w_state, w_ptr, w_rob->g_id,
                t_stats[t_id].total_reqs, zk_ctx->w_rob->capacity);
    //printf("Sent %d, Valid %d, Ready %d \n", SENT, VALID, READY);
    assert(w_rob->w_state == INVALID);

  }
}


#endif //KITE_ZK_DEBUG_UTIL_H
