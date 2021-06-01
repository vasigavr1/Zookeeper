#ifndef ZK_BQR_H
#define ZK_BQR_H

#include <od_network_context.h>
#include "zk_config.h"
#include "zk_latency.h"

#ifdef ZK_ENABLE_BQR

#define BQR_ENABLE_LATENCY 1

// TODO 1 optimize
// TODO 2 measure latency
// TODO 3 may implement lazy for leader
// TODO 4 merge with hermes (lazy) and make it generic

#define BQR_LAST_READ_BUFFER_SLOT (bqr_read_buffer_size - 1)

#ifdef BQR_ENABLE_ASSERTS
# define bqr_assert(expr) assert((expr))
#else
# define bqr_assert(expr) ((void)0)
#endif

typedef uint64_t bqr_ts_t;
typedef ctx_trace_op_t bqr_op_t;

// FIFO Ring buffer for reads
typedef struct {
    bqr_ts_t next_r_ts;
    bqr_ts_t last_issued_ts;
    bqr_ts_t last_completed_ts;

    uint16_t head;
    uint16_t size;
    bqr_ts_t          ts[BQR_MAX_READ_BUFFER_SIZE];
    bqr_op_t read_memory[BQR_MAX_READ_BUFFER_SIZE];
} read_buf_t;

typedef struct {
    bool is_lazy; // if not -> eager
    bool is_remote;
    uint8_t t_id;

    struct timespec time;
    bqr_ts_t ts_for_completion; // if 0 there no pending request
    latency_counters_t *lt_cnt;

    bool measure_latency_local;
    bool measure_latency_remote;

    read_buf_t rb;
} bqr_ctx;



static inline void bqr_ctx_init(bqr_ctx *b_ctx, uint8_t thread_id){
    b_ctx->is_lazy = 0;
    b_ctx->t_id = thread_id;
    b_ctx->is_remote = bqr_is_remote != 0;
    read_buf_t* rb = &b_ctx->rb;

    bool measure_latency = BQR_ENABLE_LATENCY && b_ctx->t_id == 0;
    b_ctx->measure_latency_local = measure_latency && !b_ctx->is_remote;
    b_ctx->measure_latency_remote = measure_latency && b_ctx->is_remote;

    if(thread_id == 0) {
        b_ctx->lt_cnt = &lt_cnt;
        latency_counters_init(b_ctx->lt_cnt);
    }

    if(bqr_read_buffer_size == 0){
        bqr_read_buffer_size = BQR_MAX_READ_BUFFER_SIZE;
    }
    assert(bqr_read_buffer_size <= BQR_MAX_READ_BUFFER_SIZE);

    rb->size = 0;
    rb->head = 0;
    rb->next_r_ts = 0;
    rb->last_issued_ts = 0;
    rb->last_completed_ts = !b_ctx->is_remote && write_ratio == 0 ? 1 : 0;
    for(int i = 0; i < BQR_MAX_READ_BUFFER_SIZE; i++) rb->ts[i] = 0;

    if(b_ctx->is_remote && thread_id == 0) {
        my_printf(cyan, "Bqr Remote: read_buf len %d\n", bqr_read_buffer_size);
    }else if(thread_id == 0){
        my_printf(cyan, "Bqr Local: read_buf len %d\n", bqr_read_buffer_size);
    }
}

static inline bool bqr_rb_is_empty(bqr_ctx* b_ctx){
    return b_ctx->rb.size == 0;
}
static inline bool bqr_rb_is_full(bqr_ctx* b_ctx){
    return b_ctx->rb.size == bqr_read_buffer_size;
}
static inline bool bqr_rb_needs_higher_ts(bqr_ctx* b_ctx){
    return b_ctx->rb.next_r_ts > b_ctx->rb.last_completed_ts;
}

static inline void bqr_rb_inc_last_completed_ts(bqr_ctx* b_ctx){
    b_ctx->rb.last_completed_ts++;

    if(b_ctx->measure_latency_remote && b_ctx->ts_for_completion > 0 &&
       b_ctx->ts_for_completion < b_ctx->rb.last_completed_ts)
    {
        stop_latency_measurement(b_ctx->lt_cnt, &b_ctx->time);
        b_ctx->ts_for_completion = 0;
    }
    bqr_assert(!b_ctx->is_remote || b_ctx->rb.last_issued_ts >= b_ctx->rb.last_completed_ts);
}
static inline void bqr_rb_inc_last_issued_ts(bqr_ctx* b_ctx, bqr_ts_t nums_to_inc){
    if(b_ctx->is_remote){
        b_ctx->rb.last_issued_ts += nums_to_inc;
        bqr_assert(b_ctx->rb.last_issued_ts > b_ctx->rb.last_completed_ts);
    }
}

static inline void bqr_rb_pop(bqr_ctx* b_ctx){
    bqr_assert(!bqr_rb_is_empty(b_ctx));

    read_buf_t* rb = &b_ctx->rb;
    if(b_ctx->measure_latency_local && rb->head == 0){
        stop_latency_measurement(b_ctx->lt_cnt, &b_ctx->time);
    }

    rb->size--;
    rb->head = rb->head == BQR_LAST_READ_BUFFER_SLOT ? 0 : rb->head + 1;
}

// returns null if full
/// WARNING assumes that the slot will be filled -> must have called last_issued_ts before!
static inline bqr_op_t* bqr_rb_next_to_push(bqr_ctx* b_ctx){
    if(bqr_rb_is_full(b_ctx)) return NULL;

    read_buf_t* rb = &b_ctx->rb;
    uint16_t last_idx = (rb->head + rb->size) % bqr_read_buffer_size;
    rb->size++;

    rb->ts[last_idx] = rb->last_issued_ts + 1;
    if(BQR_ENABLE_LATENCY && b_ctx->t_id == 0 && last_idx == 0){
        start_latency_measurement(&b_ctx->time);
        b_ctx->ts_for_completion = rb->ts[last_idx];
    }
    return &rb->read_memory[last_idx];
}

// returns null if empty / needs higher ts
static inline bqr_op_t* bqr_rb_peak(bqr_ctx* b_ctx){
    if(bqr_rb_needs_higher_ts(b_ctx)) return NULL;

    if(bqr_rb_is_empty(b_ctx)) return NULL;

    read_buf_t* rb = &b_ctx->rb;
    if(rb->ts[rb->head] > rb->last_completed_ts){
        rb->next_r_ts = rb->ts[rb->head];
        return NULL;
    }

    return &rb->read_memory[rb->head];
}

// returns null if was last / needs higher ts
static inline bqr_op_t* bqr_rb_get_next(bqr_ctx* b_ctx, bqr_op_t* next_from){
    read_buf_t* rb = &b_ctx->rb;

    bqr_assert(!bqr_rb_is_empty(b_ctx));
    bqr_assert(next_from >= rb->read_memory);

    uint16_t nf_idx   = next_from - rb->read_memory;
    uint16_t last_idx = (rb->head + (rb->size - 1)) % bqr_read_buffer_size;
    if(nf_idx == last_idx) return NULL;

#ifdef BQR_ENABLE_ASSERTS
    if(nf_idx < rb->head){
           assert(rb->size > bqr_read_buffer_size - rb->head + nf_idx);
    } else assert(rb->size > nf_idx - rb->head);
#endif

    uint16_t next_idx = nf_idx == BQR_LAST_READ_BUFFER_SLOT ? 0 : nf_idx + 1;

    if(rb->ts[next_idx] > rb->last_completed_ts) {
        rb->next_r_ts = rb->ts[rb->head];
        return NULL;
    }

    return &rb->read_memory[next_idx];
}

#endif
#endif //ZK_BQR_H