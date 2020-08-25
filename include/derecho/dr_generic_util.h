//
// Created by vasilis on 25/08/20.
//

#ifndef ODYSSEY_DR_GENERIC_UTIL_H
#define ODYSSEY_DR_GENERIC_UTIL_H

#include "dr_config.h"
#include "../../../odlib/include/network_api/network_context.h"

static inline void print_g_id_rob(context_t *ctx, uint32_t rob_id);

static inline uint32_t get_g_id_rob(context_t *ctx,
                                    uint64_t g_id)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  for (uint32_t i = 0; i < GID_ROB_NUM; ++i) {
    gid_rob_t *gid_rob = &dr_ctx->gid_rob_arr->gid_rob[i];
    if (g_id >= gid_rob->base_gid && g_id < gid_rob->base_gid + GID_ROB_SIZE) {
      return i;
    }
  }
  if (ENABLE_ASSERTIONS)
    my_printf(red, "Wrkr %u Trying to get gid rob for g_id %lu \n", ctx->t_id, g_id);
  assert(false);
}


static inline gid_rob_t *get_g_id_rob_pull(dr_ctx_t *dr_ctx)
{
  return &dr_ctx->gid_rob_arr->gid_rob[dr_ctx->gid_rob_arr->pull_ptr];
}


static inline w_rob_t *get_w_rob_from_g_rob_first_valid(dr_ctx_t *dr_ctx,
                                                        uint32_t rob_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(rob_id < GID_ROB_NUM);
    assert(dr_ctx->gid_rob_arr->gid_rob[rob_id].first_valid < GID_ROB_SIZE);
  }
  uint32_t entry_id = dr_ctx->gid_rob_arr->gid_rob[rob_id].first_valid;
  uint32_t w_ptr = dr_ctx->gid_rob_arr->gid_rob[rob_id].w_rob_ptr[entry_id];
  return (w_rob_t *) get_fifo_slot(dr_ctx->w_rob, w_ptr);
}

static inline w_rob_t *get_w_rob_from_g_rob(dr_ctx_t *dr_ctx,
                                            uint32_t rob_id,
                                            uint32_t entry_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(rob_id < GID_ROB_NUM);
    assert(entry_id < GID_ROB_SIZE);
    assert(!dr_ctx->gid_rob_arr->empty);
  }
  uint32_t w_ptr = dr_ctx->gid_rob_arr->gid_rob[rob_id].w_rob_ptr[entry_id];
  return (w_rob_t *) get_fifo_slot(dr_ctx->w_rob, w_ptr);
}

static inline w_rob_t *get_w_rob_from_g_rob_pull(dr_ctx_t *dr_ctx)
{

  return get_w_rob_from_g_rob_first_valid(dr_ctx, dr_ctx->gid_rob_arr->pull_ptr);

}



static inline void set_gid_rob_entry(context_t *ctx,
                                     uint64_t g_id,
                                     uint32_t w_rob_ptr)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  uint32_t rob_id = get_g_id_rob(ctx, g_id);
  gid_rob_t *gid_rob = &dr_ctx->gid_rob_arr->gid_rob[rob_id];
  uint32_t entry_i = (uint32_t) (g_id % GID_ROB_SIZE);

  if (ENABLE_ASSERTIONS) {
    if (gid_rob->valid[entry_i]) {
      my_printf(red, "Wrkr %u Trying to insert (new_gid %lu -- new w_rob_ptr %u ) "
        "in \n", ctx->t_id, g_id, w_rob_ptr);
      print_g_id_rob(ctx, rob_id);
      assert(false);
    }
    w_rob_t *w_rob = (w_rob_t *) get_fifo_slot(dr_ctx->w_rob, w_rob_ptr);
    assert(w_rob->g_id == g_id);
  }
  gid_rob->w_rob_ptr[entry_i] = w_rob_ptr;
  gid_rob->valid[entry_i] = true;
}



static inline uint64_t assign_new_g_id(context_t *ctx)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;

  uint32_t thread_offset = (uint32_t) ((ctx->m_id * PER_MACHINE_G_ID_BATCH) +
                                       (ctx->t_id * PER_THREAD_G_ID_BATCH));
  uint32_t batch_id = (uint32_t) (dr_ctx->inserted_w_id / PER_THREAD_G_ID_BATCH);

  uint32_t fixed_offset = (uint32_t) TOTAL_G_ID_BATCH;

  uint32_t in_batch_offset = (uint32_t) (dr_ctx->inserted_w_id % PER_THREAD_G_ID_BATCH);

  return (fixed_offset * batch_id) + (thread_offset + in_batch_offset);
}
#endif //ODYSSEY_DR_GENERIC_UTIL_H
