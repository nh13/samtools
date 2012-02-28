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
consumers_init(int32_t n, queue_t *input, queue_t *output, reader_t *reader)
{
  consumers_t *c = NULL;
  int32_t i;

  c = calloc(1, sizeof(consumers_t));
  c->threads = calloc(n, sizeof(pthread_t));
  c->n = n;
  c->c = calloc(n, sizeof(consumer_t*));

  for(i=0;i<n;i++) {
      c->c[i] = consumer_init(input, output, reader);
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

int 
write_open(const char *fn, int is_forced)
{
  int fd = -1;
  char c;
  if (!is_forced) {
      if ((fd = open(fn, O_WRONLY | O_CREAT | O_TRUNC | O_EXCL, 0666)) < 0 && errno == EEXIST) {
          fprintf(stderr, "[bgzip] %s already exists; do you wish to overwrite (y or n)? ", fn);
          scanf("%c", &c);
          if (c != 'Y' && c != 'y') {
              fprintf(stderr, "[bgzip] not overwritten\n");
              exit(1);
          }
      }
  }
  if (fd < 0) {
      if ((fd = open(fn, O_WRONLY | O_CREAT | O_TRUNC, 0666)) < 0) {
          fprintf(stderr, "[bgzip] %s: Fail to write\n", fn);
          exit(1);
      }
  }
  return fd;
}

static int 
pbgzip_main_usage()
{
  fprintf(stderr, "\n");
  fprintf(stderr, "Usage:   pbgzip [options] [file] ...\n\n");
  fprintf(stderr, "Options: -c      write on standard output, keep original files unchanged\n");
  fprintf(stderr, "         -d      decompress\n");
  fprintf(stderr, "         -f      overwrite files without asking\n");
  fprintf(stderr, "         -n      number of threads [%d]\n", detect_cpus());
  fprintf(stderr, "         -h      give this help\n");
  fprintf(stderr, "\n");
  return 1;
}


int
main(int argc, char *argv[])
{
  queue_t *input = NULL;
  queue_t *output = NULL;
  reader_t *r = NULL;
  writer_t *w = NULL;
  consumers_t *c = NULL;
  producer_t *p = NULL;
  outputter_t *o = NULL;

  int opt, f_dst;
  int32_t compress, pstdout, is_forced, n_threads;

  compress = 1; pstdout = 0; is_forced = 0; n_threads = detect_cpus();
  while((opt = getopt(argc, argv, "cdhfn:")) >= 0){
      switch(opt){
        case 'h': return pbgzip_main_usage();
        case 'd': compress = 0; break;
        case 'c': pstdout = 1; break;
        case 'f': is_forced = 1; break;
        case 'n': n_threads = atoi(optarg); break;
      }
  }

  if(argc <= 1) return pbgzip_main_usage();

  if(1 == compress) {
      fprintf(stderr, "compression is not currently supported\n");
      return 1;
  }

  if(pstdout) {
      f_dst = fileno(stdout);
  }
  else {
      char *name = strdup(argv[optind]);
      name[strlen(name) - 3] = '\0';
      f_dst = write_open(name, is_forced);
      free(name);
  }

  input = queue_init(100, 0);
  output = queue_init(100, 1);

  r = reader_init(argv[optind], input);
  c = consumers_init(1, input, output, r);
  w = writer_init(f_dst, output);
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

  if(!pstdout) unlink(argv[1]);
  return 0;
}
