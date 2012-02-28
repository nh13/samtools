#ifndef CONSUMER_H_
#define CONSUMER_H_

typedef struct {
    queue_t *input;
    queue_t *output;
    reader_t *reader;
    
    int16_t cid;
    uint8_t *buffer;
    int8_t is_done;
} consumer_t;

consumer_t*
consumer_init(queue_t *input,
              queue_t *output,
              reader_t *reader,
              int32_t cid);

void*
consumer_run(void *arg);

void
consumer_destroy(consumer_t *c);

#endif
