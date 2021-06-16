#include "zk_util.h"

struct timespec lt_timer;
latency_counters_t lt_cnt;

static void zk_file_name(char *filename, bool is_xput)
{
    char* path = is_xput
            ? "../build/results/xput/per-node"
            : "../build/results/latency";

    uint16_t qr = 0;
    char* conf = USE_LIN_READS ? "LinR" : "";
    if(USE_LIN_READS && !ENABLE_GIDS) conf = "NoGIDLinR";

#ifdef ZK_ENABLE_BQR
    qr = bqr_read_buffer_size;
    conf = bqr_is_remote ? "RemoteBqr" : "LocalBqr";
#endif

    sprintf(filename, "%s/ZAB%s_xPut_m_%d_wr_%.1f_rmw_%.1f_wk_%d_b_%d_qr_%d%s-%d.txt",
            path, conf,
            MACHINE_NUM, write_ratio/10.0, RMW_RATIO/10.0, WORKERS_PER_MACHINE,
            ZK_TRACE_BATCH, qr, "_uni", machine_id);
}

//assuming microsecond latency
static void dump_xput_stats(double xput_in_miops)
{
    static uint8_t no_func_calls = 0; ///WARNING this is not thread safe.

    assert(no_func_calls < 250);

    FILE *xput_stats_fd;
    char filename[256];
    zk_file_name(filename, 1);

    const char* open_mode = no_func_calls == 0 ? "w" : "a";
    xput_stats_fd = fopen(filename, open_mode);
    assert(xput_stats_fd != NULL);

    fprintf(xput_stats_fd, "node%d_miops-%d: %.2f\n", machine_id, no_func_calls, xput_in_miops);

    fclose(xput_stats_fd);
    no_func_calls++;
}

void zk_stats(stats_ctx_t *ctx)
{
  double seconds = ctx-> seconds;
  uint16_t print_count = ctx->print_count;
  t_stats_t *curr_w_stats = ctx->curr_w_stats;
  t_stats_t *prev_w_stats = ctx->prev_w_stats;
  all_stats_t all_stats;


  double total_throughput = 0;
  uint64_t all_clients_cache_hits = 0;
  seconds *= MILLION; // compute only MIOPS
  for (int i = 0; i < num_threads; i++) {
    all_clients_cache_hits += curr_w_stats[i].total_reqs - prev_w_stats[i].total_reqs;
    all_stats.total_reqs[i] =
      (curr_w_stats[i].total_reqs - prev_w_stats[i].total_reqs) / seconds;

    all_stats.stalled_gid[i] = (curr_w_stats[i].stalled_gid - prev_w_stats[i].stalled_gid) / seconds;
    all_stats.stalled_ack_prep[i] = (curr_w_stats[i].stalled_ack_prep - prev_w_stats[i].stalled_ack_prep) / seconds;
    all_stats.stalled_com_credit[i] =
      (curr_w_stats[i].stalled_com_credit - prev_w_stats[i].stalled_com_credit) / seconds;

    all_stats.preps_sent[i] = (curr_w_stats[i].preps_sent - prev_w_stats[i].preps_sent) / seconds;
    all_stats.coms_sent[i] = (curr_w_stats[i].coms_sent - prev_w_stats[i].coms_sent) / seconds;
    all_stats.acks_sent[i] = (curr_w_stats[i].acks_sent - prev_w_stats[i].acks_sent) / seconds;
    all_stats.received_coms[i] = (curr_w_stats[i].received_coms - prev_w_stats[i].received_coms) / seconds;
    all_stats.received_preps[i] = (curr_w_stats[i].received_preps - prev_w_stats[i].received_preps) / seconds;
    all_stats.received_acks[i] = (curr_w_stats[i].received_acks - prev_w_stats[i].received_acks) / seconds;
    if (machine_id == LEADER_MACHINE) {
      all_stats.batch_size_per_thread[i] = (curr_w_stats[i].total_writes - prev_w_stats[i].total_writes) /
                                           (double) (curr_w_stats[i].batches_per_thread -
                                                     prev_w_stats[i].batches_per_thread);
      all_stats.com_batch_size[i] = (curr_w_stats[i].coms_sent - prev_w_stats[i].coms_sent) /
                                    (double) (curr_w_stats[i].coms_sent_mes_num -
                                              prev_w_stats[i].coms_sent_mes_num);
      all_stats.prep_batch_size[i] = (curr_w_stats[i].preps_sent - prev_w_stats[i].preps_sent) /
                                     (double) (curr_w_stats[i].prep_sent_mes_num -
                                               prev_w_stats[i].prep_sent_mes_num);
    }
    else {
      all_stats.ack_batch_size[i] = (curr_w_stats[i].acks_sent - prev_w_stats[i].acks_sent) /
                                    (double) (curr_w_stats[i].acks_sent_mes_num -
                                              prev_w_stats[i].acks_sent_mes_num);
      all_stats.write_batch_size[i] = (curr_w_stats[i].writes_sent - prev_w_stats[i].writes_sent) /
                                      (double) (curr_w_stats[i].writes_sent_mes_num -
                                                prev_w_stats[i].writes_sent_mes_num);
    }
  }

  memcpy(prev_w_stats, curr_w_stats, num_threads * (sizeof(struct thread_stats)));
  total_throughput = (all_clients_cache_hits) / seconds;

  if (!SHOW_AGGREGATE_STATS) {
    printf("---------------PRINT %d time elapsed %.2f---------------\n", print_count, seconds / MILLION);
    my_printf(green, "SYSTEM MIOPS: %.2f \n", total_throughput);
    for (int i = 0; i < num_threads; i++) {
      my_printf(cyan, "T%d: ", i);
      my_printf(yellow, "%.2f MIOPS, STALL: GID: %.2f/s, ACK/PREP %.2f/s, COM/CREDIT %.2f/s", i,
                all_stats.total_reqs[i],
                all_stats.stalled_gid[i],
                all_stats.stalled_ack_prep[i],
                all_stats.stalled_com_credit[i]);
      if (machine_id == LEADER_MACHINE) {
        my_printf(yellow, ", BATCHES: GID %.2f, Coms %.2f, Preps %.2f ",
                  all_stats.batch_size_per_thread[i],
                  all_stats.com_batch_size[i],
                  all_stats.prep_batch_size[i]);
      }
      else {
        my_printf(yellow, ", BATCHES: Acks %.2f, Ws %.2f ",
                  all_stats.ack_batch_size[i],
                  all_stats.write_batch_size[i]);
      }
      //if (i > 0 && i % 2 == 0)
      printf("\n");
    }
    printf("\n");
    printf("---------------------------------------\n");
  }
  my_printf(green, "%u. SYSTEM MIOPS: %.2f \n", print_count, total_throughput);

  if(DUMP_STATS_2_FILE){
      dump_xput_stats(total_throughput);
      if(EXIT_ON_PRINT && print_count == PRINT_NUM - 1){
          char filename[256];
          zk_file_name(filename, 1);
          printf("xPut stats (of this node) saved at: \n\t%s\n", filename);
          zk_file_name(filename, 0);
          dump_latency_stats2file(&lt_cnt, filename, "reads");
      }
  }
}


