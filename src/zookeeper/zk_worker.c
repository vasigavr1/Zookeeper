#include "zk_util.h"
#include "zk_inline_util.h"
#include "init_connect.h"

void *zk_worker(void *arg)
{
  struct thread_params params = *(struct thread_params *) arg;
  uint16_t t_id = (uint16_t) params.id;
  bool is_leader = machine_id == LEADER_MACHINE;
  if (t_id == 0) {
    my_printf(yellow, "%s-id %d \n",
              is_leader ? "Leader" : "Follower",
              machine_id);
    if (ENABLE_MULTICAST) my_printf(cyan, "MULTICAST IS ENABLED \n");
  }





  context_t *ctx = create_ctx((uint8_t) machine_id,
                              (uint16_t) params.id,
                              (uint16_t) QP_NUM,
                              local_ip);

  zk_init_qp_meta(ctx, is_leader ? LEADER : FOLLOWER);
  set_up_ctx(ctx);

  /* -----------------------------------------------------
  --------------CONNECT -----------------------
  ---------------------------------------------------------*/
  setup_connections_and_spawn_stats_thread(ctx->cb, t_id);
  // We can set up the send work requests now that
  // we have address handles for remote machines
  init_ctx_send_wrs(ctx);
  ctx->appl_ctx = (void*) set_up_zk_ctx(ctx, is_leader ? LEADER : FOLLOWER);


  if (t_id == 0)
    my_printf(green, "%s %d  reached the loop \n",
              is_leader ? "Leader" : "Follower", t_id);


  main_loop(ctx);


  return NULL;
};