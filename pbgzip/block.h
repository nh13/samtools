#ifndef BLOCK_H_
#define BLOCK_H_

typedef struct {
    uint8_t *buffer;
    int32_t block_length;
    int64_t id; // Used by the queue
    int32_t mem;
} block_t;

block_t*
block_init();

void
block_destroy(block_t *block);

#endif
