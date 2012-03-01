#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>

#include "../bgzf.h"
#include "block.h"
#include "queue.h"
#include "writer.h"

writer_t*
writer_init(int fd, queue_t *output, uint8_t compress, int32_t compress_level, block_pool_t *pool)
{
  writer_t *w = calloc(1, sizeof(writer_t));

  if(0 == compress) {
      if(NULL == (w->fp_file = fdopen(fd, "wb"))) {
          fprintf(stderr, "writer fdopen: bug encountered\n");
          exit(1);
      }
  }
  else {
      compress_level = compress_level < 0? Z_DEFAULT_COMPRESSION : compress_level; // Z_DEFAULT_COMPRESSION==-1
      char mode[3]="w";
      if(0 <= compress_level) {
          if(9 <= compress_level) compress_level = 9;
          mode[1] = '0' + compress_level;
          mode[2] = '\0';
      }
      if(NULL == (w->fp_bgzf = bgzf_fdopen(fd, mode))) {
          fprintf(stderr, "writer open_write: bug encountered\n");
          exit(1);
      }
      // NB: do not need w->fp_bgzf->compressed_block; 
  }
  w->output = output;
  w->compress = compress;
  w->pool_fp= pool;
  w->pool_local = block_pool_init2(WRITER_BLOCK_POOL_NUM);

  return w;
}

static size_t
writer_write_block1(FILE* fp, block_t *b)
{
  return fwrite(b->buffer, sizeof(uint8_t), b->block_length, fp);
}

static size_t
writer_write_block2(BGZF* fp, block_t *b)
{
  size_t count;
#ifdef _USE_KNETFILE
  count = fwrite(b->buffer, 1, b->block_length, fp->x.fpw);
#else
  count = fwrite(b->buffer, 1, b->block_length, fp->file);
#endif
  if (count != b->block_length) {
      fprintf(stderr, "writer fwrite: bug encountered\n");
      return -1;
  }
  fp->block_offset = 0; // NB: important to update this!
  fp->block_address += b->block_length;
  return count;
}

void*
writer_run(void *arg)
{
  writer_t *w = (writer_t*)arg;
  block_t *b = NULL;
  uint64_t n = 0;

  while(!w->is_done) {
      while(w->pool_local->n < w->pool_local->m) { // more to read from the output queue
          b = queue_get(w->output, (0 == w->pool_local->n) ? 1 : 0);
          if(NULL == b) {
              if(0 == w->pool_local->n && 0 == w->output->eof) {
                  fprintf(stderr, "writer queue_get: bug encountered\n");
                  exit(1);
              }
              else {
                  break;
              }
          }
          if(0 == block_pool_add(w->pool_local, b)) {
              fprintf(stderr, "writer block_pool_add: bug encountered\n");
              exit(1);
          }
          b = NULL;
      }

      if(0 == w->pool_local->n && 1 == w->output->eof) { // TODO: does this need to be synced?
          break;
      }

      while(0 < w->pool_local->n) { // write all the blocks
          b = block_pool_get(w->pool_local);
          if(NULL == b) {
              fprintf(stderr, "writer block_pool_get: bug encountered\n");
              exit(1);
          }
          if(0 == w->compress) {
              if(writer_write_block1(w->fp_file, b) != b->block_length) {
                  fprintf(stderr, "writer writer_write_block: bug encountered\n");
                  exit(1);
              }
          }
          else {
              if(writer_write_block2(w->fp_bgzf, b) != b->block_length) {
                  fprintf(stderr, "writer writer_write_block: bug encountered\n");
                  exit(1);
              }
          }
          if(0 == block_pool_add(w->pool_fp, b)) {
              fprintf(stderr, "writer block_pool_add: bug encountered\n");
              exit(1);
          }
          n++;
      }
  }

  w->is_done = 1;
  //fprintf(stderr, "writer written %llu blocks\n", n);

  w->output->num_getters--;
  // no need to signal, no one depends on me :(

  return arg;
}

void
writer_destroy(writer_t *w)
{
  if(NULL == w) return;
  if(0 == w->compress) {
      if(fclose(w->fp_file) < 0) {
          fprintf(stderr, "writer bzf_close: bug encountered\n");
          exit(1);
      }
  }
  else {
      if(bgzf_close(w->fp_bgzf) < 0) {
          fprintf(stderr, "writer bzf_close: bug encountered\n");
          exit(1);
      }
      // TODO
  }
  block_pool_destroy(w->pool_local);
  free(w);
}

void
writer_reset(writer_t *w)
{
  w->is_done = w->is_closed = 0;
  block_pool_reset(w->pool_local);
}
