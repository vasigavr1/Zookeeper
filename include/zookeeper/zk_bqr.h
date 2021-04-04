#ifndef ZK_BQR_H
#define ZK_BQR_H

#include "zk_main.h"


// TODO 1 Implement eager for all ZAB nodes (even leader)
//     --> stall reads until a later write is completed
//     --> once done unstall and push to the KVS
//     --> buffer reads somewhere outside the KVS batch op buffs
// TODO 2 measure latency
// TODO 3 optimize
// TODO 4 merge with hermes (lazy) and make it generic

#define EMPTY_ASYNC_TS 0
#define MAX_READ_BUFFER_SIZE 512
#define BQR_LAST_READ_BUFFER_SLOT (MAX_READ_BUFFER_SIZE - 1)

#define BQR_ENABLE_ASSERTS
#ifdef BQR_ENABLE_ASSERTS
# define bqr_assert(x) (assert(x))
#else
# define bqr_assert(x) ()
#endif

typedef uint64_t bqr_ts_t;
typedef ctx_trace_op_t bqr_op_t;

// FIFO Ring buffer for reads
typedef struct {
    uint16_t head;
    uint16_t size;
    bqr_ts_t next_r_ts;
    bqr_ts_t last_issued_ts;
    bqr_ts_t last_completed_ts;
    bqr_ts_t          ts[MAX_READ_BUFFER_SIZE];
    bqr_op_t read_memory[MAX_READ_BUFFER_SIZE];
} read_buf_t;

typedef struct {
    uint8_t is_lazy; // if not it is eager
    read_buf_t rb;
} bqr_ctx;

static inline void bqr_r_buf_init(read_buf_t* rb){
    rb->size = 0;
    rb->head = 0;
    rb->next_r_ts = 0;
    rb->last_issued_ts = 0;
    rb->last_completed_ts = 0;
    for(int i = 0; i < MAX_READ_BUFFER_SIZE; i++) {
        rb->ts = EMPTY_ASYNC_TS;
    }
}

static inline bool bqr_rb_is_empty(bqr_ctx* b_ctx){
    return b_ctx->rb.size == 0;
}
static inline bool bqr_rb_is_full(bqr_ctx* b_ctx){
    return b_ctx->rb.size == MAX_READ_BUFFER_SIZE;
}
static inline bool bqr_rb_needs_higher_ts(){
    return rb->next_r_ts > rb->last_completed_ts;
}

static inline void bqr_rb_set_last_issued_ts(bqr_ctx* b_ctx, bqr_ts_t ts){
    b_ctx->rb.last_issued_ts = ts;
}
static inline void bqr_rb_set_last_completed_ts(bqr_ctx* b_ctx, bqr_ts_t ts){
    b_ctx->rb.last_completed_ts = ts;
}


static inline void bqr_rb_pop(bqr_ctx* b_ctx){
    bqr_assert(!bqr_r_buf_is_empty(b_ctx));

    read_buf_t* rb = b_ctx->rb;
    rb->size--;
    rb->head = rb->head == BQR_LAST_READ_BUFFER_SLOT
               ? 0
               : rb->head + 1;
}

// returns null if full
/// WARNING assumes that the slot will be filled --> must have called last_issued_ts before!
static inline bqr_op_t* bqr_rb_next_to_push(bqr_ctx* b_ctx){
    if(bqr_r_buf_is_full(b_ctx)) return NULL;

    read_buf_t* rb = b_ctx->rb;
    uint16_t last_idx = rb->head + rb->size % MAX_READ_BUFFER_SIZE;
    rb->size++;

    &rb->ts[last_idx] = rb->last_issued_ts + 1;
    return &rb->read_memory[last_idx];
}

// returns null if empty / needs higher ts
static inline bqr_op_t* bqr_rb_peak(bqr_ctx* b_ctx){
    if(bqr_r_buf_needs_higher_ts(b_ctx)) return NULL;

    if(bqr_r_buf_is_empty(b_ctx)) return NULL;

    read_buf_t* rb = b_ctx->rb;
    if(rb->ts[rb->head] > rb->last_completed_ts){
        rb->next_r_ts = rb->ts[rb->head];
        return NULL;
    }

    return &rb->read_memory[rb->head];
}

// returns null if was last / needs higher ts
static inline bqr_op_t* bqr_rb_get_next(bqr_ctx* b_ctx, bqr_op_t* next_from){
    bqr_assert(!bqr_r_buf_is_empty(b_ctx));

    read_buf_t* rb = b_ctx->rb;
    uint16_t last_idx = rb->head + rb->size % MAX_READ_BUFFER_SIZE;
    if(next_from == last_idx) return NULL;

    uint16_t nf_idx = next_from - rb;
#ifdef BQR_ENABLE_ASSERTS
    if(nf_idx < rb->head){
        assert(rb->size > MAX_READ_BUFFER_SIZE - rb->head + nf_idx);
    }else{
        assert(rb->size > nf_idx - rb->head);
    }
#endif

    unint16_t next_idx = nf_idx == BQR_LAST_READ_BUFFER_SLOT
            ? 0
            : nf_idx + 1;

    if(rb->ts[next_idx] > rb->last_completed_ts) return NULL;

    return &rb->read_memory[next_idx];
}
/// How to iterate and pop bqr_rb:
// 1. bqr_rb_set_last_completed_ts(ts);
// ...
// bqr_op_t* op_ptr = bqr_r_buf_peak(bqr_ctx);
//while(op_ptr != null){
//    if completed pop!
//      bqr_rb_pop(bqr_ctx)
//      tmp = bqr_r_buf_peak(bqr_ctx);
//    else
//      tmp = bqr_r_buf_get_next(bqr_ctx, op_ptr);
//}

/// How to push: 0. make sure you will push for sure
/// (can be easily done but bqr_rb_next_to_push is not reversable op at the moment)
// 1. bqr_rb_set_last_issued_ts(ts);
// ..
// 2. bqr_op_t* op_ptr = bqr_rb_next_to_push(bqr_ctx);
// 3. fill op_ptr;

#endif //ZK_BQR_H