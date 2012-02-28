#ifndef BLOCK_H_
#define BLOCK_H_

typedef struct {
    int8_t *buffer;
    int32_t block_length;
    int32_t block_offset; // used by bgzf_write and bgzf_flush
    int64_t id; // Used by the queue
    int32_t mem;
} block_t;

block_t*
block_init();

void
block_destroy(block_t *block);

#endif
