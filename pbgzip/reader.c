#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <pthread.h>

#include "../bgzf.h"
#include "block.h"
#include "queue.h"
#include "reader.h"

reader_t*
reader_init(const char *path, queue_t *input)
{
  reader_t *r= calloc(1, sizeof(reader_t));

  r->fp = bgzf_open(path, "r");
  r->input = input;

  return r;
}

static int
reader_read_block(BGZF* fp, block_t *b)
{
  bgzf_byte_t header[BLOCK_HEADER_LENGTH];
  int count, size = 0, remaining;
#ifdef _USE_KNETFILE
  int64_t block_address = knet_tell(fp->x.fpr);
  //if (load_block_from_cache(fp, block_address)) return 0;
  count = knet_read(fp->x.fpr, header, sizeof(header));
#else
  int64_t block_address = ftello(fp->file);
  //if (load_block_from_cache(fp, block_address)) return 0;
  count = fread(header, 1, sizeof(header), fp->file);
#endif
  if (count == 0) {
      fp->block_length = b->block_length = 0;
      return 0;
  }
  size = count;
  if (count != sizeof(header)) {
      fprintf(stderr, "read failed");
      return -1;
  }
  if (!bgzf_check_header(header)) {
      fprintf(stderr, "invalid block header");
      return -1;
  }
  b->block_length = unpackInt16((uint8_t*)&header[16]) + 1;
  bgzf_byte_t* compressed_block = (bgzf_byte_t*) b->buffer;
  memcpy(compressed_block, header, BLOCK_HEADER_LENGTH);
  remaining = b->block_length - BLOCK_HEADER_LENGTH;
#ifdef _USE_KNETFILE
  count = knet_read(fp->x.fpr, &compressed_block[BLOCK_HEADER_LENGTH], remaining);
#else
  count = fread(&compressed_block[BLOCK_HEADER_LENGTH], 1, remaining, fp->file);
#endif
  if (count != remaining) {
      fprintf(stderr, "read failed");
      return -1;
  }
  size += count;
  /*
  count = inflate_block(fp, block_length);
  if (count < 0) return -1;
  if (fp->block_length != 0) {
      // Do not reset offset if this read follows a seek.
      fp->block_offset = 0;
  }
  */
  fp->block_address = block_address;
  /*
  fp->block_length = count;
  cache_block(fp, size);
  */
  return 0;
}


void*
reader_run(void *arg)
{
  reader_t *r = (reader_t*)arg;
  block_t *b = NULL;
  uint64_t n = 0;

  while(!r->is_done) {
      // read block
      b = block_init(); // TODO: memory pool for blocks
      if(reader_read_block(r->fp, b) < 0) {
          fprintf(stderr, "reader reader_read_block: bug encountered");
          exit(1);
      }
      if(NULL == b || 0 == b->block_length) {
          block_destroy(b);
          b = NULL;
          break;
      }

      // add to the queue
      if(!queue_add(r->input, b, 1)) {
          fprintf(stderr, "reader queue_add: bug encountered");
          exit(1);
      }
      n++;
  }

  r->is_done = 1;
  //fprintf(stderr, "reader read %llu blocks\n", n);

  // signal other threads
  pthread_cond_signal(r->input->not_full);
  pthread_cond_signal(r->input->not_empty);

  return arg;
}

void
reader_destroy(reader_t *r)
{
  if(NULL == r) return;
  if(bgzf_close(r->fp) < 0) {
      fprintf(stderr, "reader bgzf_close: bug encountered");
      exit(1);
  }
  free(r);
}
