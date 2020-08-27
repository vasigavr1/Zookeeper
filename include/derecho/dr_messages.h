//
// Created by vasilis on 21/08/20.
//

#ifndef ODYSSEY_DR_MESSAGES_H
#define ODYSSEY_DR_MESSAGES_H

#include "top.h"


/// PREP_QP_ID
#define PREP_CREDITS 10
#define MAX_PREP_SIZE 500

#define MAX_PREP_WRS (MESSAGES_IN_BCAST_BATCH)

#define MAX_PREP_BUF_SLOTS_TO_BE_POLLED ( PREP_CREDITS * REM_MACH_NUM)
#define MAX_RECV_PREP_WRS (PREP_CREDITS * REM_MACH_NUM)
#define PREP_BUF_SLOTS (MAX_RECV_PREP_WRS)

#define PREP_MES_HEADER 12 // opcode(1), coalesce_num(1) l_id (8)
#define EFFECTIVE_MAX_PREP_SIZE (MAX_PREP_SIZE - PREP_MES_HEADER)
#define PREP_SIZE (16 + VALUE_SIZE)
#define PREP_COALESCE (EFFECTIVE_MAX_PREP_SIZE / PREP_SIZE)
#define PREP_SEND_SIZE (PREP_MES_HEADER + (PREP_COALESCE * PREP_SIZE))
#define PREP_RECV_SIZE (GRH_SIZE + PREP_SEND_SIZE)

#define PREP_FIFO_SIZE (SESSIONS_PER_THREAD + 1)


typedef struct dr_prepare {
  uint64_t g_id;
  mica_key_t key;
  //uint8_t val_len;
  //uint8_t opcode; //override opcode
  uint8_t value[VALUE_SIZE];
} __attribute__((__packed__)) dr_prepare_t;

// prepare message
typedef struct dr_prep_message {
  uint64_t l_id;
  uint8_t opcode;
  uint8_t coalesce_num;
  uint8_t m_id;
  uint8_t unused;
  dr_prepare_t prepare[PREP_COALESCE];
} __attribute__((__packed__)) dr_prep_mes_t;

typedef struct dr_prep_message_ud_req {
  uint8_t grh[GRH_SIZE];
  dr_prep_mes_t prepare;
} dr_prep_mes_ud_t;


#define COM_CREDITS 5
#define COM_WRS MESSAGES_IN_BCAST_BATCH
#define RECV_COM_WRS (REM_MACH_NUM * COM_CREDITS)
#define COM_BUF_SLOTS RECV_COM_WRS



#endif //ODYSSEY_DR_MESSAGES_H
