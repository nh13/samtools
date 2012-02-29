#ifndef READER_H_
#define READER_H_

typedef struct {
    BGZF *fp_bgzf;
    int fd_file;
    queue_t *input;
    uint8_t is_done;
    uint8_t is_closed;
    uint8_t compress;
    block_pool_t *pool;
} reader_t;

#define READER_BLOCK_POOL_NUM 1000

reader_t*
reader_init(int fd, queue_t *input, uint8_t compress, block_pool_t *pool);

void*
reader_run(void *arg);

void
reader_destroy(reader_t *r);

#endif
