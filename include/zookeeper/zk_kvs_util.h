//
// Created by vasilis on 23/06/2020.
//

#ifndef Z_KVS_UTIL_H
#define Z_KVS_UTIL_H

#include "kvs.h"
#include "zk_main.h"
#include "generic_inline_util.h"
#include "debug_util.h"
#include "zk_generic_util.h"
#include "zk_reservation_stations_util.h"



static inline void zk_KVS_remote_read(zk_ctx_t *zk_ctx,
                                      mica_op_t *kv_ptr,
                                      ctx_trace_op_t *op,
                                      zk_resp_t *resp,
                                      uint32_t *r_push_ptr_,
                                      uint16_t t_id)
{
  uint32_t r_push_ptr = *r_push_ptr_;
  r_rob_t *r_rob = (r_rob_t *) get_fifo_slot(zk_ctx->r_rob, r_push_ptr);

  uint32_t debug_cntr = 0;
  uint64_t tmp_lock = read_seqlock_lock_free(&kv_ptr->seqlock);
  do {
    debug_stalling_on_lock(&debug_cntr, "remote read", t_id);
    memcpy(r_rob->value, kv_ptr->value, (size_t) VALUE_SIZE);
    r_rob->g_id = kv_ptr->g_id;
  } while (!(check_seqlock_lock_free(&kv_ptr->seqlock, &tmp_lock)));

  r_rob->key = op->key;
  r_rob->val_len = op->val_len;
  r_rob->value_to_read = op->value_to_read;
  r_rob->sess_id = op->session_id;
  if (ENABLE_ASSERTIONS) assert(zk_ctx->stalled[r_rob->sess_id]);
  r_rob->state = VALID;
  r_rob->l_id = zk_ctx->local_r_id + zk_ctx->r_rob->capacity ;
  resp->type = KVS_GET_SUCCESS;
  MOD_INCR(r_push_ptr, FLR_PENDING_READS);
  (*r_push_ptr_) =  r_push_ptr;
  fifo_increm_capacity(zk_ctx->r_rob);
}

/* The leader and follower send their local requests to this, reads get served
 * But writes do not get served, writes are only propagated here to see whether their keys exist */
static inline void zk_KVS_batch_op_trace(zk_ctx_t *zk_ctx, uint16_t op_num,
                                         ctx_trace_op_t *op, zk_resp_t *resp,
                                         uint16_t t_id)
{
  uint16_t op_i;
 if (ENABLE_ASSERTIONS) {
   assert(op != NULL);
   assert(op_num > 0 && op_num <= ZK_TRACE_BATCH);
   assert(resp != NULL);
 }

  unsigned int bkt[ZK_TRACE_BATCH];
  struct mica_bkt *bkt_ptr[ZK_TRACE_BATCH];
  unsigned int tag[ZK_TRACE_BATCH];
  mica_op_t *kv_ptr[ZK_TRACE_BATCH];	/* Ptr to KV item in log */
  /*
   * We first lookup the key in the datastore. The first two @op_i loops work
   * for both GETs and PUTs.
   */
  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_locate_one_bucket(op_i, bkt, &op[op_i].key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  uint32_t r_push_ptr = zk_ctx->protocol == FOLLOWER ? zk_ctx->r_rob->push_ptr : 0;
  // the following variables used to validate atomicity between a lock-free read of an object
  for(op_i = 0; op_i < op_num; op_i++) {
    if (ENABLE_ASSERTIONS && kv_ptr[op_i] == NULL) assert(false);
    bool key_found = memcmp(&kv_ptr[op_i]->key, &op[op_i].key, KEY_SIZE) == 0;
    if (unlikely(ENABLE_ASSERTIONS && !key_found)) {
      my_printf(red, "Kvs miss %u\n", op_i);
      cust_print_key("Op", &op[op_i].key);
      cust_print_key("KV_ptr", &kv_ptr[op_i]->key);
      resp[op_i].type = KVS_MISS;
      assert(false);
    }
    if (op[op_i].opcode == KVS_OP_GET ) {
      if (!USE_LIN_READS || machine_id == LEADER_MACHINE) {
        KVS_local_read(kv_ptr[op_i], op[op_i].value_to_read, &resp[op_i].type, t_id);
      }
      else {
        zk_KVS_remote_read(zk_ctx, kv_ptr[op_i], &op[op_i],
                           &resp[op_i], &r_push_ptr, t_id);
      }

    }
    else if (op[op_i].opcode == KVS_OP_PUT) {
      resp[op_i].type = KVS_PUT_SUCCESS;
    }
    else if (ENABLE_ASSERTIONS) {
      my_printf(red, "wrong Opcode in cache: %d, req %d \n", op[op_i].opcode, op_i);
      assert(0);
    }
  }
}

///* The leader and follower send the writes to be committed with this function*/
static inline void zk_KVS_batch_op_updates(uint16_t op_num, zk_ctx_t *zk_ctx,
                                           uint32_t pull_ptr,
                                           uint16_t t_id)
{

  if (DISABLE_UPDATING_KVS) return;
  uint16_t op_i;  /* op_i is batch index */
  if (ENABLE_ASSERTIONS) {
    //assert(preps != NULL);
    assert(op_num > 0 && op_num <= ZK_UPDATE_BATCH);
  }

  unsigned int bkt[ZK_UPDATE_BATCH];
  struct mica_bkt *bkt_ptr[ZK_UPDATE_BATCH];
  unsigned int tag[ZK_UPDATE_BATCH];
  mica_op_t *kv_ptr[ZK_UPDATE_BATCH];	/* Ptr to KV item in log */

  for(op_i = 0; op_i < op_num; op_i++) {
    w_rob_t *w_rob = (w_rob_t *) get_fifo_slot_mod(zk_ctx->w_rob, pull_ptr+ op_i);
    zk_prepare_t *op = w_rob->ptr_to_op;
    KVS_locate_one_bucket(op_i, bkt, &op->key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  for(op_i = 0; op_i < op_num; op_i++) {
    w_rob_t *w_rob = (w_rob_t *) get_fifo_slot_mod(zk_ctx->w_rob, pull_ptr + op_i);
    zk_prepare_t *op = w_rob->ptr_to_op;
    if (ENABLE_ASSERTIONS && kv_ptr[op_i] == NULL) {
      my_printf(red, "Kptr  is null %u\n", op_i);
      cust_print_key("Op", &op->key);
      assert(false);
    }

    bool key_found = memcmp(&kv_ptr[op_i]->key, &op->key, KEY_SIZE) == 0;
    if (unlikely(ENABLE_ASSERTIONS && !key_found)) {
      my_printf(red, "Kvs update miss %u\n", op_i);
      cust_print_key("Op", &op->key);
      cust_print_key("KV_ptr", &kv_ptr[op_i]->key);
      assert(false);
    }
    if (op->opcode == KVS_OP_PUT) {
      if (ENABLE_GID_ORDERING) // only if global ordering is enforced the assertion stands
        assert(op->g_id >= committed_global_w_id);
      lock_seqlock(&kv_ptr[op_i]->seqlock);
      if (ENABLE_GIDS) kv_ptr[op_i]->g_id = op->g_id;
      memcpy(kv_ptr[op_i]->value, op->value, (size_t) VALUE_SIZE);
      unlock_seqlock(&kv_ptr[op_i]->seqlock);
    }
    else {
      my_printf(red, "wrong Opcode to an update in kvs: %d, req %d, flr_id %u, val_len %u, g_id %lu , \n",
                 op->opcode, op_i, op->flr_id, op->val_len, op->g_id);
      assert(0);
    }
  }
}


///* The leader and follower send the writes to be committed with this function*/
static inline void zk_KVS_batch_op_reads(context_t *ctx)
{
  zk_ctx_t *zk_ctx = (zk_ctx_t *) ctx->appl_ctx;
  uint16_t op_i;  /* op_i is batch index */
  ptrs_to_r_t *ptrs_to_r = zk_ctx->ptrs_to_r;
  uint16_t op_num =  ptrs_to_r->polled_reads;
  if (op_num == 0) return;
  if (ENABLE_ASSERTIONS) {
    assert(op_num > 0 && op_num <= LDR_MAX_INCOMING_R);
  }

  unsigned int bkt[LDR_MAX_INCOMING_R];
  struct mica_bkt *bkt_ptr[LDR_MAX_INCOMING_R];
  unsigned int tag[LDR_MAX_INCOMING_R];
  mica_op_t *kv_ptr[LDR_MAX_INCOMING_R];	/* Ptr to KV item in log */

  for(op_i = 0; op_i < op_num; op_i++) {
    zk_read_t *read = ptrs_to_r->ptr_to_ops[op_i];
    KVS_locate_one_bucket(op_i, bkt, &read->key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  for(op_i = 0; op_i < op_num; op_i++) {
    zk_read_t *read = ptrs_to_r->ptr_to_ops[op_i];
    if (ENABLE_ASSERTIONS && kv_ptr[op_i] == NULL) {
      my_printf(red, "Kptr  is null %u\n", op_i);
      cust_print_key("Op", &read->key);
      assert(false);
    }

    bool key_found = memcmp(&kv_ptr[op_i]->key, &read->key, KEY_SIZE) == 0;
    if (unlikely(ENABLE_ASSERTIONS && !key_found)) {
      my_printf(red, "Kvs update miss %u\n", op_i);
      cust_print_key("Op", &read->key);
      cust_print_key("KV_ptr", &kv_ptr[op_i]->key);
      assert(false);
    }
    if (read->opcode == KVS_OP_GET) {
      ctx_insert_mes(ctx, R_QP_ID, R_REP_SMALL_SIZE, 0,
                 !ptrs_to_r->coalesce_r_rep[op_i],
                 (void *) kv_ptr[op_i], op_i);
      //ldr_insert_r_rep(ctx, zk_ctx, kv_ptr[op_i], op_i);
    }
    else {
      my_printf(red, "wrong Opcode to a read in kvs: %d, req %d, flr_id %u,  g_id %lu , \n",
                read->opcode, op_i, ptrs_to_r->ptr_to_r_mes[op_i]->m_id,  read->g_id);
      assert(0);
    }

  }
}

#endif //Z_KVS_UTIL_H
