#ifndef READER_H_
#define READER_H_

typedef struct {
    BGZF *fp;
    queue_t *input;
    uint8_t is_done;
    uint8_t is_closed;
} reader_t;

reader_t*
reader_init(const char *path, queue_t *input);

void*
reader_run(void *arg);

void
reader_destroy(reader_t *r);

#endif
