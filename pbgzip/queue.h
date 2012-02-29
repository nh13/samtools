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
    int32_t num_adders;
    int32_t num_getters;
    pthread_mutex_t *mut;
    pthread_cond_t *not_full;
    pthread_cond_t *not_empty;
    pthread_cond_t *is_empty;
} queue_t;

queue_t*
queue_init(int32_t capacity, int8_t ordered, int32_t num_adders, int32_t num_getters);

int8_t
queue_add(queue_t *q, block_t *b, int8_t wait);

block_t*
queue_get(queue_t *q, int8_t wait);

void
queue_wait_until_empty(queue_t *q);

void
queue_close(queue_t *q);

void
queue_destroy(queue_t *q);

void
queue_reset(queue_t *q);

#endif
