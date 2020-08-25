//
// Created by vasilis on 21/08/20.
//

#ifndef ODYSSEY_DR_KVS_UTIL_H
#define ODYSSEY_DR_KVS_UTIL_H

#include "kvs.h"

static inline void dr_KVS_batch_op_trace(dr_ctx_t *dr_ctx, uint16_t op_num,
                                         uint16_t t_id)
{
  dr_trace_op_t *op = dr_ctx->ops;
  dr_resp_t *resp = dr_ctx->resp;
  uint16_t op_i;
  if (ENABLE_ASSERTIONS) {
    assert(op != NULL);
    assert(op_num > 0 && op_num <= DR_TRACE_BATCH);
    assert(resp != NULL);
  }

  unsigned int bkt[DR_TRACE_BATCH];
  struct mica_bkt *bkt_ptr[DR_TRACE_BATCH];
  unsigned int tag[DR_TRACE_BATCH];
  mica_op_t *kv_ptr[DR_TRACE_BATCH];	/* Ptr to KV item in log */
  /*
   * We first lookup the key in the datastore. The first two @op_i loops work
   * for both GETs and PUTs.
   */
  for(op_i = 0; op_i < op_num; op_i++) {
    KVS_locate_one_bucket(op_i, bkt, &op[op_i].key, bkt_ptr, tag, kv_ptr, KVS);
  }
  KVS_locate_all_kv_pairs(op_num, tag, bkt_ptr, kv_ptr, KVS);

  //uint32_t r_push_ptr = dr_ctx->r_rob->push_ptr;
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
      if (!USE_REMOTE_READS) {
        KVS_local_read(kv_ptr[op_i], op[op_i].value_to_read, &resp[op_i].type, t_id);
      }
      else {
        assert(false);
        //dr_KVS_remote_read(dr_ctx, kv_ptr[op_i], &op[op_i],
        //                   &resp[op_i], &r_push_ptr, t_id);
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


#endif //ODYSSEY_DR_KVS_UTIL_H
