#ifndef ZK_TIME_H
#define ZK_TIME_H
#include <stdint.h> /* for uint64_t */
#include <time.h>  /* for struct timespec */
#include <stdio.h>
#include <assert.h>

// LATENCY
#define ZK_MAX_LATENCY 1000 //in us
#define ZK_LATENCY_BUCKETS 1000
#define ZK_LATENCY_PRECISION (ZK_MAX_LATENCY / ZK_LATENCY_BUCKETS) //latency granularity in us

typedef struct {
    uint64_t histogram[ZK_LATENCY_BUCKETS + 1];
    int max_latency;
    long long total_measurements;
}latency_counters_t;

extern struct timespec lt_timer;
extern latency_counters_t lt_cnt;

static inline void
latency_counters_init(latency_counters_t* lt_ct)
{
    lt_ct->max_latency;
    lt_ct->total_measurements;
    for(int i = 0; i < ZK_LATENCY_BUCKETS + 1; i++){
       lt_ct->histogram[i] = 0;
    }
}

static inline void
bkkeep_latency(latency_counters_t* lt_ct, int useconds)
{
    lt_ct->total_measurements++;

    if (useconds > ZK_MAX_LATENCY){
        lt_ct->histogram[ZK_LATENCY_BUCKETS]++;
    }else {
        lt_ct->histogram[useconds / ZK_LATENCY_PRECISION]++;
    }

    if(lt_ct->max_latency < useconds){
        lt_ct->max_latency = useconds;
    }
}


// Necessary bookkeeping to initiate the latency measurement
static inline void
start_latency_measurement(struct timespec *start)
{
    clock_gettime(CLOCK_MONOTONIC, start);
}

static inline void
stop_latency_measurement(latency_counters_t* lt_ct, struct timespec *start)
{
    struct timespec end;
    clock_gettime(CLOCK_MONOTONIC, &end);
    int useconds = (int) (((end.tv_sec - start->tv_sec) * 1000000) +
                          ((end.tv_nsec - start->tv_nsec) / 1000));
//	printf("Latency of Read %u us\n", useconds);
    bkkeep_latency(lt_ct, useconds);
}

static inline void
dump_latency_stats2file(latency_counters_t* lt_ct, char *filename, char* prefix)
{
    FILE *latency_stats_fd = fopen(filename, "w");
    fprintf(latency_stats_fd, "#---------------- %s Latency --------------\n", prefix);
    for(int i = 0; i < ZK_LATENCY_BUCKETS; ++i)
        fprintf(latency_stats_fd, "%s: %d, %ld\n", prefix, i * ZK_LATENCY_PRECISION, lt_ct->histogram[i]);
    fprintf(latency_stats_fd, "%s: -1, %ld\n", prefix, lt_ct->histogram[ZK_LATENCY_BUCKETS]); //print outliers
    fprintf(latency_stats_fd, "%s-hl: %d\n", prefix, lt_ct->max_latency); //print max read latency

    fclose(latency_stats_fd);

    printf("Latency stats saved at %s\n", filename);
}

#endif //ZK_TIME_H
