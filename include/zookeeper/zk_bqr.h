#ifndef ZK_BQR_H
#define ZK_BQR_H

#include "zk_main.h"

#define MAX_READ_BUFFER_SIZE 512

// TODO 1 Implement eager for all ZAB nodes (even leader)
//     --> stall reads until a later write is completed
//     --> once done unstall and push to the KVS
//     --> buffer reads somewhere outside the KVS batch op buffs
// TODO 2 measure latency
// TODO 3 optimize
// TODO 4 merge with hermes (lazy) and make it generic

//#define ENABLE_READ_BUFFER
//#define ENABLE_RD_BUF_COALESCING

#define EMPTY_ASYNC_TS 0

//#define BQR_ENABLE_HERMES
#ifdef BQR_ENABLE_HERMES
#define MAX_MACHINE_NUM  ???
#define MAX_BATCH_KVS_OPS_SIZE ???

typedef spacetime_op_t bqr_op_t;
#define BQR_MACHINE_NUM machine_num
#define BQR_BATCH_SIZE  max_batch_size

#define BQR_OP_STATE op_meta.state
#define BQR_OP_OPCODE op_meta.op_code
#define BQR_OP_READ_OPCODE ST_OP_GET
#define BQR_OP_RESET_OPCODE ST_MISS
#define BQR_OP_UNSENT_UPD_TYPE1 ST_PUT_SUCCESS
#define BQR_OP_UNSENT_UPD_TYPE2 ST_RMW_SUCCESS
#define BQR_OP_COMPLETE_READ_OPCODE ST_GET_COMPLETE
#define BQR_OP_ASYNC_COMPLETE_READ_OPCODE ST_GET_ASYNC_COMPLETE

#else
typedef ctx_trace_op_t bqr_op_t;
#define MAX_MACHINE_NUM MACHINE_NUM
#define MAX_BATCH_KVS_OPS_SIZE ZK_TRACE_BATCH

#define BQR_MACHINE_NUM MACHINE_NUM
#define BQR_BATCH_SIZE  ZK_TRACE_BATCH

#define BQR_OP_STATE opcode???
#define BQR_OP_OPCODE opcode
#define BQR_OP_READ_OPCODE KVS_OP_GET
#define BQR_OP_RESET_OPCODE  ???
#define BQR_OP_UNSENT_UPD_TYPE1 ???
#define BQR_OP_UNSENT_UPD_TYPE2  ???
#define BQR_OP_COMPLETE_READ_OPCODE ???
#define BQR_OP_ASYNC_COMPLETE_READ_OPCODE ???

//zk_resp.type --> I think not used
#endif

static_assert(MAX_MACHINE_NUM > 2, "");



typedef uint64_t async_ts;

typedef struct {
    async_ts ts;
    uint16_t cnt;
    bqr_op_t* ptr;
} read_buf_slot_ptr;

typedef struct {
    uint16_t curr_len;
    read_buf_slot_ptr     ptrs[MAX_READ_BUFFER_SIZE];
    bqr_op_t       read_memory[MAX_READ_BUFFER_SIZE];
} read_buf_t;

static void read_buf_init(read_buf_t* rb){
    rb->curr_len = 0;
    for(int i = 0; i < MAX_READ_BUFFER_SIZE; i++){
        rb->ptrs[i].ts = 0;
        rb->ptrs[i].cnt = 0;
        rb->ptrs[i].ptr = &rb->read_memory[i];
    }
}

typedef struct {
    uint8_t is_lazy; // if not it is eager
    uint16_t worker_lid;
    zk_ctx_t *zk_ctx;
    thread_stats_t *t_stats;
    async_ts curr_ts;
    async_ts majority_ts;
    uint8_t  has_given_ts;
    uint8_t  has_issued_upd;
    async_ts ts_array[MAX_MACHINE_NUM];
    async_ts op_async_ts[MAX_BATCH_KVS_OPS_SIZE];
    read_buf_t rb;
} async_ctx;

static inline void complete_reads_from_read_buf(async_ctx *a_ctx);

/*
 *   Helpers
 */
static inline uint8_t* bqr_op_resp_ptr(bqr_op_t* op){
#ifdef BQR_ENABLE_HERMES
    return &op->op_meta.state;
#else
    return &op->opcode;
#endif
}

static inline uint8_t bqr_op_resp(bqr_op_t* op){
#ifdef BQR_ENABLE_HERMES
    return op->op_meta.state;
#else
    return op->opcode;
#endif
}

static inline uint8_t bqr_op_opcode(bqr_op_t* op){
#ifdef BQR_ENABLE_HERMES
    return op->op_meta.op_code;
#else
    return op->opcode;
#endif
}


/*
 *  BQR
 */
static void async_init(async_ctx* a_ctx,
                       uint8_t is_lazy,
                       uint16_t worker_lid,
                       thread_stats_t *t_stats,
                       zk_ctx_t *zk_ctx)
{
    a_ctx->zk_ctx = zk_ctx;
    a_ctx->is_lazy = is_lazy;
    a_ctx->t_stats = t_stats;
    a_ctx->worker_lid = worker_lid;

//    a_ctx->worker_completed_ops = worker_completed_ops;
    a_ctx->curr_ts     = EMPTY_ASYNC_TS;
    a_ctx->majority_ts = EMPTY_ASYNC_TS;
    a_ctx->has_issued_upd = 0;
    for(int i = 0; i < MAX_MACHINE_NUM; ++i){
        a_ctx->ts_array[i]    = EMPTY_ASYNC_TS;
    }
    for(int i = 0; i < MAX_BATCH_KVS_OPS_SIZE; ++i){
        a_ctx->op_async_ts[i] = EMPTY_ASYNC_TS;
    }

    read_buf_init(&a_ctx->rb);
}

static inline void async_start_cycle(async_ctx* a_ctx)
{
    a_ctx->curr_ts++;
    a_ctx->has_given_ts = 0;
    a_ctx->has_issued_upd = 0;
}

static inline void async_end_cycle(async_ctx* a_ctx)
{
    // WARNING we need to ensure there is at least one write per cycle that contains a read
//    assert(!a_ctx->has_given_ts || a_ctx->has_issued_upd);
}

static inline void async_reset_op_ts(async_ctx* a_ctx, uint16_t op_idx)
{
    a_ctx->op_async_ts[op_idx] = EMPTY_ASYNC_TS;
}

static inline void async_set_op_ts(async_ctx* a_ctx, uint16_t op_idx, uint8_t is_upd)
{
    a_ctx->op_async_ts[op_idx] = a_ctx->curr_ts;
    a_ctx->has_given_ts = 1;
    a_ctx->has_issued_upd |= is_upd;
}

static inline void async_set_ops_ts(async_ctx* a_ctx, bqr_op_t *ops){
    async_start_cycle(a_ctx);

    uint8_t resp = bqr_op_resp(&ops[i]);
    // completed read or pending update
    for(int i = 0; i < BQR_BATCH_SIZE; i++) {
        uint8_t just_completed_read =
        resp == BQR_OP_COMPLETE_READ_OPCODE &&
        a_ctx->op_async_ts[i] == EMPTY_ASYNC_TS;
        uint8_t unsent_upd =
        resp == BQR_OP_UNSENT_UPD_TYPE1 ||
        resp == BQR_OP_UNSENT_UPD_TYPE2;

        if(just_completed_read || unsent_upd){
            async_set_op_ts(a_ctx, i, unsent_upd);
        }
    }

    async_end_cycle(a_ctx);
}

static inline void async_complete_read(async_ctx* a_ctx, bqr_op_t *op, uint16_t op_id)
{
    uint8_t resp = bqr_op_resp(op);
    if(resp == BQR_OP_COMPLETE_READ_OPCODE &&
    a_ctx->op_async_ts[op_id] <= a_ctx->majority_ts)
    {
        uint8_t* resp_ptr = bqr_op_resp_ptr(op);
        *resp_ptr = BQR_OP_ASYNC_COMPLETE_READ_OPCODE;
    }
}

static inline void async_complete_reads(async_ctx* a_ctx, bqr_op_t *ops)
{
    for(int i = 0; i < BQR_BATCH_SIZE; i++) {
        async_complete_read(a_ctx, &ops[i], i);
    }
}

static inline void async_set_ack_ts(async_ctx* a_ctx, bqr_op_t *ops,
                                    int op_idx, uint8_t node_id,
                                    const uint8_t set_read_completion)
{
    async_ts ts = a_ctx->op_async_ts[op_idx];

    if(a_ctx->ts_array[node_id] < ts){
        a_ctx->ts_array[node_id] = ts;
    }

    if(a_ctx->majority_ts >= ts) return;

    // calculate majority ts
    uint8_t majority = BQR_MACHINE_NUM / 2; // + 1 (the local replica)
    for(int i = 0; i < BQR_MACHINE_NUM; ++i){
        if(a_ctx->ts_array[i] >= ts){
            majority--;
            if(majority == 0){
                a_ctx->majority_ts = ts;
                if(set_read_completion){ // set read completion if flagged
                    async_complete_reads(a_ctx, ops);
                }
#ifdef ENABLE_READ_BUFFER
                complete_reads_from_read_buf(a_ctx);
#endif
                return;
            }
        }
    }
}


static inline void complete_reads_from_read_buf(async_ctx *a_ctx)
{
    read_buf_t *rb = &a_ctx->rb;
    uint16_t total_completed = 0;
    async_ts mj_ts = a_ctx->majority_ts;
    read_buf_slot_ptr *ptrs = rb->ptrs;

    for(int i = 0; i < rb->curr_len; i++){
        if(ptrs[i].ts <= mj_ts){
            // account for completed req
            total_completed += ptrs[i].cnt;

            // remove from buffer
            if(i == rb->curr_len - 1){
                rb->curr_len--;
                break; // break if last item
            }
            // remove by swapping
            read_buf_slot_ptr last_ptr = ptrs[rb->curr_len - 1];
            ptrs[rb->curr_len - 1].ptr = ptrs[i].ptr;
            ptrs[i] = last_ptr;
            rb->curr_len--;
            i--; //recheck same idx since we swapped with last ptr
        }
    }

//    *a_ctx->worker_completed_ops =
//            *a_ctx->worker_completed_ops + total_completed;
}

static inline void try_add_op_to_read_buf(async_ctx *a_ctx,
                                          bqr_op_t* src,
                                          uint16_t src_op_idx)
{
    uint8_t resp = bqr_op_resp(src);
    // return if not appropriate state(op) or if buf is full
    if(resp != BQR_OP_COMPLETE_READ_OPCODE) return;
    assert(src->BQR_OP_OPCODE  == BQR_OP_READ_OPCODE);

    uint16_t curr_len = a_ctx->rb.curr_len;
    async_ts op_ts = a_ctx->op_async_ts[src_op_idx];

#ifdef ENABLE_RD_BUF_COALESCING
    for(int i = 0; i < curr_len; ++i){
        if(a_ctx->rb.ptrs[i].ts == op_ts){
            a_ctx->rb.ptrs[i].cnt++;
            src->op_meta.state = ST_MISS; // Making it a MISS will be resetted afterwards
            return;
        }
    }
#endif

    if(curr_len == MAX_READ_BUFFER_SIZE - 1) return;

    read_buf_slot_ptr *rb_slot = &a_ctx->rb.ptrs[curr_len];
    *rb_slot->ptr = *src;
    rb_slot->cnt = 1;
    rb_slot->ts = op_ts;
    a_ctx->rb.curr_len++;

    uint8_t* resp_ptr = bqr_op_resp_ptr(src);
    *resp_ptr = BQR_OP_RESET_OPCODE; // Making it a MISS will be resetted afterwards
}

#endif //ZK_BQR_H
