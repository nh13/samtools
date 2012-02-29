#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>

#include "../bgzf.h"
#include "util.h"
#include "block.h"
#include "queue.h"
#include "reader.h"
#include "writer.h"
#include "consumer.h"
#include "pbgzf.h"


static consumers_t*
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

static void
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

static void
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

static void
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

static producer_t*
producer_init(reader_t *r)
{
  producer_t *p;

  p = calloc(1, sizeof(producer_t));
  p->r = r;

  pthread_attr_init(&p->attr);
  pthread_attr_setdetachstate(&p->attr, PTHREAD_CREATE_JOINABLE);

  return p;
}

static void
producer_destroy(producer_t *p)
{
  free(p);
}

static void
producer_run(producer_t *p)
{
  if(0 != pthread_create(&p->thread, &p->attr, reader_run, p->r)) {
      fprintf(stderr, "failed to create threads");
      exit(1);
  }
}

static void
producer_join(producer_t *p)
{
  if(0 != pthread_join(p->thread, NULL)) {
      fprintf(stderr, "failed to join threads");
      exit(1);
  }
  // close the input queue
  queue_close(p->r->input);
}

static outputter_t*
outputter_init(writer_t *w)
{
  outputter_t *o;

  o = calloc(1, sizeof(outputter_t));
  o->w = w;

  pthread_attr_init(&o->attr);
  pthread_attr_setdetachstate(&o->attr, PTHREAD_CREATE_JOINABLE);

  return o;
}

static void
outputter_destroy(outputter_t *o)
{
  free(o);
}

static void
outputter_run(outputter_t *o)
{
  if(0 != pthread_create(&o->thread, &o->attr, writer_run, o->w)) {
      fprintf(stderr, "failed to create threads");
      exit(1);
  }
}

static void
outputter_join(outputter_t *o)
{
  if(0 != pthread_join(o->thread, NULL)) {
      fprintf(stderr, "failed to join threads");
      exit(1);
  }
}


static inline
int
pbgzf_min(int x, int y)
{
      return (x < y) ? x : y;
}

static void
pbgzf_run(PBGZF *fp)
{
  if(NULL != fp->p) producer_run(fp->p);
  if(NULL != fp->c) consumers_run(fp->c);
  if(NULL != fp->o) outputter_run(fp->o);
}

static void
pbgzf_join(PBGZF *fp)
{
  // join
  if(NULL != fp->p) producer_join(fp->p);
  if(NULL != fp->c) consumers_join(fp->c);
  if(NULL != fp->o) outputter_join(fp->o);
}

static void
pbgzf_signal_and_join(PBGZF *fp)
{
  // signal other threads to finish
  pthread_cond_signal(fp->input->not_full);
  pthread_cond_signal(fp->input->not_empty);
  
  // join
  pbgzf_join(fp);
}

  
static PBGZF*
pbgzf_init(int fd, const char* __restrict mode)
{
  int i, compress_level = -1;
  char open_mode;
  PBGZF *fp = NULL;

  // set compress_level
  for (i = 0; mode[i]; ++i)
    if (mode[i] >= '0' && mode[i] <= '9') break;
  if (mode[i]) compress_level = (int)mode[i] - '0';
  if (strchr(mode, 'u')) compress_level = 0;

  // set read/write
  if (strchr(mode, 'r') || strchr(mode, 'R')) { /* The reading mode is preferred. */
      open_mode = 'r';
  } else if (strchr(mode, 'w') || strchr(mode, 'W')) {
      open_mode = 'w';
  }
  else {
      return NULL;
  }
  
  fp = calloc(1, sizeof(PBGZF));

  // queues
  fp->open_mode = open_mode;
  fp->num_threads = detect_cpus(); // TODO: do we want to use all the threads?
  //fp->num_threads = 1;
  fp->queue_size = PBGZF_QUEUE_SIZE;
  fp->input = queue_init(fp->queue_size, 0);
  fp->output = queue_init(fp->queue_size, 1);
  
  fp->pool = block_pool_init(PBGZF_BLOCKS_POOL_NUM);

  if('w' == open_mode) { // write to a compressed file
      fp->r = NULL; // do not read
      fp->p = NULL; // do not produce data
      fp->c = consumers_init(fp->num_threads, fp->input, fp->output, fp->r, 1, compress_level); // deflate/compress
      fp->w = writer_init(fd, fp->output, 1, compress_level, fp->pool); // write data
      fp->o = outputter_init(fp->w);
  }
  else { // read from a compressed file
      fp->r = reader_init(fd, fp->input, 0, fp->pool); // read the compressed file
      fp->p = producer_init(fp->r);
      fp->c = consumers_init(fp->num_threads, fp->input, fp->output, fp->r, 0, compress_level); // inflate
      fp->w = NULL;
      fp->o = NULL; // do not write
  
      fp->eof_ok = bgzf_check_EOF(fp->r->fp_bgzf); 
  }

  pbgzf_run(fp);

  return fp;
}

PBGZF* pbgzf_fdopen(int fd, const char* __restrict mode)
{
  return pbgzf_init(fd, mode);
}

PBGZF* pbgzf_open(const char* path, const char* __restrict mode)
{
  int fd;
  if (strchr(mode, 'r') || strchr(mode, 'R')) { /* The reading mode is preferred. */
#ifdef _WIN32
      fd = open(path, O_RDONLY | O_BINARY); 
#else 
      fd = open(path, O_RDONLY);
#endif
  }
  else { // read from a compressed file
#ifdef _WIN32
      fd = open(path, O_WRONLY | O_CREAT | O_TRUNC | O_BINARY); 
#else
      fd = open(path, O_WRONLY | O_CREAT | O_TRUNC); 
#endif
  }
  return pbgzf_fdopen(fd, mode);
}

int 
pbgzf_close(PBGZF* fp)
{
  queue_close(fp->input);
  if('r' == fp->open_mode) {
      fp->r->is_done = 1; // force reader to shut down
      queue_close(fp->output);
  }

  pbgzf_signal_and_join(fp);

  // destroy
  if(NULL != fp->c) consumers_destroy(fp->c);
  if(NULL != fp->p) producer_destroy(fp->p);
  if(NULL != fp->o) outputter_destroy(fp->o);
  if(NULL != fp->input) queue_destroy(fp->input);
  if(NULL != fp->output) queue_destroy(fp->output);
  if(NULL != fp->r) reader_destroy(fp->r);
  if(NULL != fp->w) writer_destroy(fp->w);
  
  block_pool_destroy(fp->pool);
  free(fp);

  return 0;
}

int 
pbgzf_read(PBGZF* fp, void* data, int length)
{
  if(length <= 0) {
      return 0;
  }
  if(fp->open_mode != 'r') {
      fprintf(stderr, "file not open for reading\n");
      return -1;
  }

  int bytes_read = 0;
  bgzf_byte_t* output = data;
  while(bytes_read < length) {
      int copy_length, available;

      available = (NULL == fp->block) ? 0 : (fp->block->block_length - fp->block->block_offset);
      if(0 == available) {
          if(NULL != fp->block) block_destroy(fp->block);
          fp->block = queue_get(fp->output, 1);
      }
      available = (NULL == fp->block) ? 0 : (fp->block->block_length - fp->block->block_offset);
      if(available <= 0) {
          break;
      }

      bgzf_byte_t *buffer;
      copy_length = pbgzf_min(length-bytes_read, available);
      buffer = (bgzf_byte_t*)fp->block->buffer;
      memcpy(output, buffer + fp->block->block_offset, copy_length);
      fp->block->block_offset += copy_length;
      output += copy_length;
      bytes_read += copy_length;
  }

  return bytes_read;
}

int 
pbgzf_write(PBGZF* fp, const void* data, int length)
{
  const bgzf_byte_t *input = data;
  int block_length, bytes_written;

  if(fp->open_mode != 'w') {
      fprintf(stderr, "file not open for writing\n");
      return -1;
  }

  if(NULL == fp->block) {
      fp->block = block_init();
  }

  input = data;
  block_length = fp->block->block_length;
  bytes_written = 0;
  while (bytes_written < length) {
      int copy_length = pbgzf_min(block_length - fp->block->block_offset, length - bytes_written);
      bgzf_byte_t* buffer = fp->block->buffer;
      memcpy(buffer + fp->block->block_offset, input, copy_length);
      fp->block->block_offset += copy_length;
      input += copy_length;
      bytes_written += copy_length;
      fp->block_offset += copy_length;
      if (fp->block->block_offset == block_length) {
          // add to the queue
          if(!queue_add(fp->input, fp->block, 1)) {
              fprintf(stderr, "reader queue_add: bug encountered\n");
              exit(1);
          }
          fp->block = NULL;
          fp->block = block_init();
          fp->block_offset = 0;
      }
  }
  return bytes_written;
}

int64_t 
pbgzf_seek(PBGZF* fp, int64_t pos, int where)
{
  if(fp->open_mode != 'r') {
      fprintf(stderr, "file not open for read\n");
      return -1;
  }
  if (where != SEEK_SET) {
      fprintf(stderr, "unimplemented seek option\n");
      return -1;
  }

  // signal and join
  pbgzf_signal_and_join(fp);

  // seek
  pos = bgzf_seek(fp->r->fp_bgzf, pos, where);

  // reset the queues
  queue_reset(fp->input);
  queue_reset(fp->output);

  // restart threads
  pbgzf_run(fp);

  return pos;
}

int 
pbgzf_check_EOF(PBGZF *fp)
{
  if('r' != fp->open_mode) {
      fprintf(stderr, "file not open for reading\n");
      exit(1);
  }
  return fp->eof_ok;
}

int 
pbgzf_flush(PBGZF* fp)
{
  int ret;

  if('w' != fp->open_mode) {
      fprintf(stderr, "file not open for writing\n");
      exit(1);
  }

  // close the input queue 
  queue_close(fp->input);
  
  // join
  pbgzf_join(fp);

  // flush the underlying stream
  ret = bgzf_flush(fp->w->fp_bgzf);
  if(0 != ret) return ret;

  // reset the queues
  queue_reset(fp->input);
  queue_reset(fp->output);

  // restart threads
  pbgzf_run(fp);

  return 0;
}

int 
pbgzf_flush_try(PBGZF *fp, int size)
{
  if (fp->block_offset + size > MAX_BLOCK_SIZE) 
    return pbgzf_flush(fp);
  return -1;
}

void pbgzf_set_cache_size(PBGZF *fp, int cache_size)
{
  if(fp && 'r' == fp->open_mode) bgzf_set_cache_size(fp->r->fp_bgzf, cache_size);
}

void
pbgzf_main(int f_src, int f_dst, int compress, int compress_level, int queue_size, int num_threads)
{
  // NB: this gives us greater control over queue size and the like
  queue_t *input = NULL;
  queue_t *output = NULL;
  reader_t *r = NULL;
  writer_t *w = NULL;
  consumers_t *c = NULL;
  producer_t *p = NULL;
  outputter_t *o = NULL;
  block_pool_t *pool = NULL;

  pool = block_pool_init(PBGZF_BLOCKS_POOL_NUM);
  input = queue_init(queue_size, 0);
  output = queue_init(queue_size, 1);

  r = reader_init(f_src, input, compress, pool);
  w = writer_init(f_dst, output, compress, compress_level, pool);
  c = consumers_init(num_threads, input, output, r, compress, compress_level);
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
  block_pool_destroy(pool);
}
