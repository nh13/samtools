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
writer_init(int fd, queue_t *output)
{
  writer_t *w = calloc(1, sizeof(writer_t));

  if(NULL == (w->fp = fdopen(fd, "wb"))) {
      fprintf(stderr, "writer fdopen: bug encountered");
      exit(1);
  }
  w->output = output;

  return w;
}

static size_t
writer_write_block(FILE* fp, block_t *b)
{
  return fwrite(b->buffer, sizeof(uint8_t), b->block_length, fp);
}


void*
writer_run(void *arg)
{
  writer_t *w = (writer_t*)arg;
  block_t *b = NULL;
  uint64_t n = 0;

  while(!w->is_done) {
      b = queue_get(w->output, 1);
      if(NULL == b) {
          if(1 == w->output->eof) { // TODO: does this need to be synced?
              break;
          }
          else {
              fprintf(stderr, "writer queue_get: bug encountered");
              exit(1);
          }
      }

      if(writer_write_block(w->fp, b) != b->block_length) {
          fprintf(stderr, "writer writer_write_block: bug encountered");
          exit(1);
      }
      block_destroy(b);

      n++;
  }

  w->is_done = 1;
  //fprintf(stderr, "writer written %llu blocks\n", n);

  // no need to signal, no one depends on me :(

  return arg;
}

void
writer_destroy(writer_t *w)
{
  if(NULL == w) return;
  fclose(w->fp);
  free(w);
}
