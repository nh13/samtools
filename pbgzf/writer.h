#ifndef WRITER_H_
#define WRITER_H_

typedef struct {
    FILE *fp;
    queue_t *output;
    uint8_t is_done;
    uint8_t is_closed;
} writer_t;

writer_t*
writer_init(int fd, queue_t *output);

void*
writer_run(void *arg);

void
writer_destroy(writer_t *r);

#endif
