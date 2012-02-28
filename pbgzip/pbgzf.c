#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>

#include "../bgzf.h"
#include "util.h"
#include "block.h"
#include "queue.h"
#include "reader.h"
#include "writer.h"
#include "consumer.h"

typedef struct {
    pthread_attr_t attr;
    pthread_t *threads;
    int32_t n;
    consumer_t **c;
} consumers_t;

typedef struct {
    pthread_attr_t attr;
    pthread_t thread;
    reader_t *r;
} producer_t;

typedef struct {
    pthread_attr_t attr;
    pthread_t thread;
    writer_t *w;
} outputter_t;

consumers_t*
consumers_init(int32_t n, queue_t *input, queue_t *output, reader_t *reader, 
               int32_t compress, int32_t compress_level)
{
  consumers_t *c = NULL;
  int32_t i;

  c = calloc(1, sizeof(consumers_t));
  c->threads = calloc(n, sizeof(pthread_t));
  c->n = n;
  c->c = calloc(n, sizeof(consumer_t*));

  for(i=0;i<n;i++) {
      c->c[i] = consumer_init(input, output, reader, compress, compress_level, i);
  }

  pthread_attr_init(&c->attr);
  pthread_attr_setdetachstate(&c->attr, PTHREAD_CREATE_JOINABLE);

  return c;
}

void
consumers_destroy(consumers_t *c)
{
  int32_t i;
  free(c->threads);
  for(i=0;i<c->n;i++) {
      consumer_destroy(c->c[i]);
  }
  free(c->c);
  free(c);
}

void
consumers_run(consumers_t *c)
{
  int32_t i;

  for(i=0;i<c->n;i++) {
      if(0 != pthread_create(&c->threads[i], &c->attr, consumer_run, c->c[i])) {
          fprintf(stderr, "failed to create threads");
          exit(1);
      }
  }
}

void
consumers_join(consumers_t *c)
{
  int32_t i;

  for(i=0;i<c->n;i++) {
      if(0 != pthread_join(c->threads[i], NULL)) {
          fprintf(stderr, "failed to join threads");
          exit(1);
      }
  }
  // close the output queue
  queue_close(c->c[0]->output);
}

producer_t*
producer_init(reader_t *r)
{
  producer_t *p;

  p = calloc(1, sizeof(producer_t));
  p->r = r;

  pthread_attr_init(&p->attr);
  pthread_attr_setdetachstate(&p->attr, PTHREAD_CREATE_JOINABLE);

  return p;
}

void
producer_destroy(producer_t *p)
{
  free(p);
}

void
producer_run(producer_t *p)
{
  if(0 != pthread_create(&p->thread, &p->attr, reader_run, p->r)) {
      fprintf(stderr, "failed to create threads");
      exit(1);
  }
}

void
producer_join(producer_t *p)
{
  if(0 != pthread_join(p->thread, NULL)) {
      fprintf(stderr, "failed to join threads");
      exit(1);
  }
  // close the input queue
  queue_close(p->r->input);
}

outputter_t*
outputter_init(writer_t *w)
{
  outputter_t *o;

  o = calloc(1, sizeof(outputter_t));
  o->w = w;

  pthread_attr_init(&o->attr);
  pthread_attr_setdetachstate(&o->attr, PTHREAD_CREATE_JOINABLE);

  return o;
}

void
outputter_destroy(outputter_t *o)
{
  free(o);
}

void
outputter_run(outputter_t *o)
{
  if(0 != pthread_create(&o->thread, &o->attr, writer_run, o->w)) {
      fprintf(stderr, "failed to create threads");
      exit(1);
  }
}

void
outputter_join(outputter_t *o)
{
  if(0 != pthread_join(o->thread, NULL)) {
      fprintf(stderr, "failed to join threads");
      exit(1);
  }
}

void
pbgzf_run(int f_src, int f_dst, int compress, int compress_level, int queue_size, int n_threads)
{
  queue_t *input = NULL;
  queue_t *output = NULL;
  reader_t *r = NULL;
  writer_t *w = NULL;
  consumers_t *c = NULL;
  producer_t *p = NULL;
  outputter_t *o = NULL;

  input = queue_init(queue_size, 0);
  output = queue_init(queue_size, 1);

  r = reader_init(f_src, input, compress);
  w = writer_init(f_dst, output, compress, compress_level);
  c = consumers_init(n_threads, input, output, r, compress, compress_level);
  p = producer_init(r);
  o = outputter_init(w);

  producer_run(p);
  consumers_run(c);
  outputter_run(o);

  producer_join(p);
  consumers_join(c);
  outputter_join(o);

  consumers_destroy(c);
  producer_destroy(p);
  outputter_destroy(o);

  queue_destroy(input);
  queue_destroy(output);
  reader_destroy(r);
  writer_destroy(w);
}
