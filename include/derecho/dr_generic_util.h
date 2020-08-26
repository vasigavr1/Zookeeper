//
// Created by vasilis on 25/08/20.
//

#ifndef ODYSSEY_DR_GENERIC_UTIL_H
#define ODYSSEY_DR_GENERIC_UTIL_H

#include "dr_config.h"
#include "../../../odlib/include/network_api/network_context.h"

static inline void print_g_id_rob(context_t *ctx, uint32_t rob_id);


static inline bool gid_rob_is_empty(gid_rob_t *gid_rob)
{
  bool zero_mem[GID_ROB_SIZE] = {0};
  return memcmp(gid_rob->valid, zero_mem, GID_ROB_SIZE) == 0;
}

static inline bool all_gid_robs_are_empty(dr_ctx_t *dr_ctx)
{
  for (int i = 0; i < GID_ROB_NUM; ++i) {
    if (!dr_ctx->gid_rob_arr->gid_rob[i].empty) return false;
  }
  return true;
}

static inline gid_rob_t *get_g_id_rob_priv(context_t *ctx,
                                          uint64_t g_id)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  for (uint64_t  i = 0; i < GID_ROB_NUM; ++i) {
    gid_rob_t *gid_rob = &dr_ctx->gid_rob_arr->gid_rob[i];
    if (ENABLE_ASSERTIONS) assert(i == gid_rob->rob_id);
    if (g_id >= gid_rob->base_gid && g_id < gid_rob->base_gid + GID_ROB_SIZE) {
      return gid_rob;
    }
  }
  if (ENABLE_ASSERTIONS)
    my_printf(red, "Wrkr %u Trying to get gid rob for g_id %lu \n", ctx->t_id, g_id);
  assert(false);
}

static inline uint32_t get_g_id_rob_id(context_t *ctx,
                                       uint64_t g_id)
{
  return get_g_id_rob_priv(ctx, g_id)->rob_id;
}

static inline gid_rob_t *get_g_id_rob_ptr(context_t *ctx,
                                          uint64_t g_id)
{
  return get_g_id_rob_priv(ctx, g_id);
}


static inline gid_rob_t *get_g_id_rob_pull(dr_ctx_t *dr_ctx)
{
  return &dr_ctx->gid_rob_arr->gid_rob[dr_ctx->gid_rob_arr->pull_ptr];
}




/*-----------------------------------------------------
 * -----------------GET W_ROB-------------------------
 * --------------------------------------------------*/

static inline uint32_t get_w_rob_slot(gid_rob_t *gid_rob,
                                      uint64_t g_id)
{
  if (ENABLE_ASSERTIONS) {
    assert(gid_rob != NULL);
  }
  uint32_t w_rob_ptr = (uint32_t) ((gid_rob->rob_id * GID_ROB_SIZE) + (g_id %  GID_ROB_SIZE));
  if (ENABLE_ASSERTIONS) assert(w_rob_ptr < W_ROB_SIZE);
}

static inline w_rob_t *get_w_rob_ptr(dr_ctx_t *dr_ctx,
                                    gid_rob_t *gid_rob,
                                    uint64_t g_id)
{

  return &dr_ctx->w_rob[get_w_rob_slot(gid_rob, g_id)];
}


static inline w_rob_t *find_w_rob(context_t *ctx, uint64_t g_id)
{
  dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  return &dr_ctx->w_rob[get_w_rob_slot(get_g_id_rob_ptr(ctx, g_id), g_id)];
}


static inline w_rob_t *get_w_rob_from_g_rob_first_valid(dr_ctx_t *dr_ctx,
                                                        uint32_t rob_id)
{
  if (ENABLE_ASSERTIONS) assert(rob_id < GID_ROB_NUM);
  gid_rob_t *gid_rob = &dr_ctx->gid_rob_arr->gid_rob[rob_id];
  if (ENABLE_ASSERTIONS) assert(gid_rob->first_valid < GID_ROB_SIZE);

  return get_w_rob_ptr(dr_ctx, gid_rob, (uint64_t) gid_rob->first_valid);
}

static inline w_rob_t *get_w_rob_from_g_rob_pull(dr_ctx_t *dr_ctx)
{

  return get_w_rob_from_g_rob_first_valid(dr_ctx, dr_ctx->gid_rob_arr->pull_ptr);

}

static inline void set_gid_rob_entry_priv(context_t *ctx,
                                          gid_rob_t *gid_rob,
                                          uint64_t g_id)
{
  uint32_t entry_i = (uint32_t) (g_id % GID_ROB_SIZE);

  if (ENABLE_ASSERTIONS) {
    if (gid_rob->valid[entry_i]) {
      my_printf(red, "Wrkr %u Trying to insert (new_gid %lu) "
        "in \n", ctx->t_id, g_id);
      print_g_id_rob(ctx, gid_rob->rob_id);
      assert(false);
    }

  }
  gid_rob->valid[entry_i] = true;
  gid_rob->empty = false;

  if (entry_i < gid_rob->first_valid) {
    gid_rob->first_valid = entry_i;
  }

  //dr_ctx_t *dr_ctx = (dr_ctx_t *) ctx->appl_ctx;
  //dr_ctx->gid_rob_arr->empty = false;
}


static inline void set_gid_rob_entry(context_t *ctx,
                                     uint64_t g_id)
{
  set_gid_rob_entry_priv(ctx, get_g_id_rob_ptr(ctx, g_id), g_id);
}

static inline gid_rob_t *get_and_set_gid_entry(context_t *ctx,
                                               uint64_t g_id)
{
  gid_rob_t *gid_rob = get_g_id_rob_ptr(ctx, g_id);
  set_gid_rob_entry_priv(ctx, gid_rob, g_id);
  return gid_rob;
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
