#ifndef ZK_BQR_H
#define ZK_BQR_H

#include <network_context.h>
#include <assert.h>


// TODO 1 optimize
// TODO 2 measure latency
// TODO 3 may implement lazy for leader
// TODO 4 merge with hermes (lazy) and make it generic

#define BQR_LAST_READ_BUFFER_SLOT (BQR_MAX_READ_BUFFER_SIZE - 1)

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
    uint8_t is_lazy; // if not -> eager
    read_buf_t rb;
} bqr_ctx;



static inline void bqr_rb_init(read_buf_t* rb){
    rb->size = 0;
    rb->head = 0;
    rb->next_r_ts = 0;
    rb->last_issued_ts = 0;
    rb->last_completed_ts = 0;
    for(int i = 0; i < BQR_MAX_READ_BUFFER_SIZE; i++) rb->ts[i] = 0;
}

static inline bool bqr_rb_is_empty(bqr_ctx* b_ctx){
    return b_ctx->rb.size == 0;
}
static inline bool bqr_rb_is_full(bqr_ctx* b_ctx){
    return b_ctx->rb.size == BQR_MAX_READ_BUFFER_SIZE;
}
static inline bool bqr_rb_needs_higher_ts(bqr_ctx* b_ctx){
    return b_ctx->rb.next_r_ts > b_ctx->rb.last_completed_ts;
}

static inline void bqr_rb_inc_last_completed_ts(bqr_ctx* b_ctx){
    b_ctx->rb.last_completed_ts++;
#ifndef BQR_ENABLE_LOCAL_READS
    bqr_assert(b_ctx->rb.last_issued_ts >= b_ctx->rb.last_completed_ts);
#endif
}
static inline void bqr_rb_inc_last_issued_ts(bqr_ctx* b_ctx, bqr_ts_t nums_to_inc){
#ifndef BQR_ENABLE_LOCAL_READS
    b_ctx->rb.last_issued_ts += nums_to_inc;
    bqr_assert(b_ctx->rb.last_issued_ts > b_ctx->rb.last_completed_ts);
#endif
}

static inline void bqr_rb_pop(bqr_ctx* b_ctx){
    bqr_assert(!bqr_rb_is_empty(b_ctx));

    read_buf_t* rb = &b_ctx->rb;
    rb->size--;
    rb->head = rb->head == BQR_LAST_READ_BUFFER_SLOT ? 0 : rb->head + 1;
}

// returns null if full
/// WARNING assumes that the slot will be filled -> must have called last_issued_ts before!
static inline bqr_op_t* bqr_rb_next_to_push(bqr_ctx* b_ctx){
    if(bqr_rb_is_full(b_ctx)) return NULL;

    read_buf_t* rb = &b_ctx->rb;
    uint16_t last_idx = (rb->head + rb->size) % BQR_MAX_READ_BUFFER_SIZE;
    rb->size++;

    rb->ts[last_idx] = rb->last_issued_ts + 1;
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
    uint16_t last_idx = (rb->head + (rb->size - 1)) % BQR_MAX_READ_BUFFER_SIZE;
    if(nf_idx == last_idx) return NULL;

#ifdef BQR_ENABLE_ASSERTS
    if(nf_idx < rb->head){
           assert(rb->size > BQR_MAX_READ_BUFFER_SIZE - rb->head + nf_idx);
    } else assert(rb->size > nf_idx - rb->head);
#endif

    uint16_t next_idx = nf_idx == BQR_LAST_READ_BUFFER_SLOT ? 0 : nf_idx + 1;

    if(rb->ts[next_idx] > rb->last_completed_ts) {
        rb->next_r_ts = rb->ts[rb->head];
        return NULL;
    }

    return &rb->read_memory[next_idx];
}

#endif //ZK_BQR_H