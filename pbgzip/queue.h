#ifndef QUEUE_H_
#define QUEUE_H_

typedef struct {
    block_t **queue;
    int32_t mem;
    int32_t head;
    int32_t tail;
    int32_t n; 
    int32_t length;
    int64_t id;
    int8_t eof;
    int8_t ordered;
    pthread_mutex_t *mut;
    pthread_cond_t *not_full;
    pthread_cond_t *not_empty;
} queue_t;

queue_t*
queue_init(int32_t capacity, int8_t ordered);

int8_t
queue_add(queue_t *q, block_t *b, int8_t wait);

block_t*
queue_get(queue_t *q, int8_t wait);

void
queue_close(queue_t *q);

void
queue_destroy(queue_t *q);


#endif
