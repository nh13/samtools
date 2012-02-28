#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <zlib.h>
#include <pthread.h>

#include "../bgzf.h"
#include "block.h"
#include "queue.h"
#include "reader.h"
#include "consumer.h"

consumer_t*
consumer_init(queue_t *input,
              queue_t *output,
              reader_t *reader,
              int32_t cid)
{
  consumer_t *c = calloc(1, sizeof(consumer_t));

  c->input = input;
  c->output = output;
  c->reader = reader;
  c->cid = cid;

  c->buffer = malloc(sizeof(uint8_t)*MAX_BLOCK_SIZE);

  return c;
}

static int
consumer_inflate_block(consumer_t *c, block_t *block)
{
  z_stream zs;
  int status;
  
  // copy compressed buffer into consumer buffer
  memcpy(c->buffer, block->buffer, block->block_length);

  zs.zalloc = NULL;
  zs.zfree = NULL;
  zs.next_in = c->buffer + 18;
  zs.avail_in = block->block_length - 16;
  zs.next_out = block->buffer;
  zs.avail_out = MAX_BLOCK_SIZE;

  status = inflateInit2(&zs, GZIP_WINDOW_BITS);
  if (status != Z_OK) {
      fprintf(stderr, "inflate init failed");
      return -1;
  }

  status = inflate(&zs, Z_FINISH);
  if (status != Z_STREAM_END) {
      inflateEnd(&zs);
      fprintf(stderr, "inflate failed");
      return -1;
  }

  status = inflateEnd(&zs);
  if (status != Z_OK) {
      fprintf(stderr, "inflate end failed");
      return -1;
  }

  return zs.total_out;
}

void*
consumer_run(void *arg)
{
  consumer_t *c = (consumer_t*)arg;
  block_t *b = NULL;
  uint64_t n = 0;

  //fprintf(stderr, "consumer starting\n");
  while(1) {
      b = queue_get(c->input, 1);
      if(NULL == b) {
          if(1 == c->reader->is_done) { // TODO: does this need to be synced?
              break;
          }
          else {
              fprintf(stderr, "consumer queue_get: bug encountered");
              exit(1);
          }
      }

      if((b->block_length = consumer_inflate_block(c, b)) < 0) {
          fprintf(stderr, "Error decompressing");
          exit(1);
      }

      if(!queue_add(c->output, b, 1)) {
          fprintf(stderr, "consumer queue_add: bug encountered");
          exit(1);
      }
      
      n++;
  }

  c->is_done = 1;
  //fprintf(stderr, "Consumer #%d processed %llu blocks\n", c->cid, n);

  // signal other threads
  pthread_cond_signal(c->input->not_full);
  pthread_cond_signal(c->input->not_empty);
  pthread_cond_signal(c->output->not_full);
  pthread_cond_signal(c->output->not_empty);

  return arg;
}

void
consumer_destroy(consumer_t *c)
{
  if(NULL == c) return;
  free(c->buffer);
  free(c);
}
