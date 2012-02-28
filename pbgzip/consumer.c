#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <zlib.h>
#include <pthread.h>

#include "../bgzf.h"
#include "util.h"
#include "block.h"
#include "queue.h"
#include "reader.h"
#include "consumer.h"

consumer_t*
consumer_init(queue_t *input,
              queue_t *output,
              reader_t *reader,
              int8_t compress,
              int32_t compress_level,
              int32_t cid)
{
  consumer_t *c = calloc(1, sizeof(consumer_t));

  c->input = input;
  c->output = output;
  c->reader = reader;
  c->compress = compress;
  c->compress_level = compress_level < 0? Z_DEFAULT_COMPRESSION : compress_level; // Z_DEFAULT_COMPRESSION==-1
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
  zs.next_out = (void*)block->buffer;
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

static int
consumer_deflate_block(consumer_t *c, block_t *b)
{
  // Deflate the block in fp->uncompressed_block into fp->compressed_block.
  // Also adds an extra field that stores the compressed block length.
  int32_t block_length;

  memcpy(c->buffer, b->buffer, b->block_length); // copy uncompressed to compressed 

  // Notes:
  // fp->compressed_block is now b->buffer
  // fp->uncompressed_block is now c->buffer
  // block_length is now b->block_length

  //bgzf_byte_t* buffer = fp->compressed_block;
  //int buffer_size = fp->compressed_block_size;
  bgzf_byte_t* buffer = b->buffer; // destination
  int buffer_size = MAX_BLOCK_SIZE;
  block_length = b->block_length;

  // Init gzip header
  buffer[0] = GZIP_ID1;
  buffer[1] = GZIP_ID2;
  buffer[2] = CM_DEFLATE;
  buffer[3] = FLG_FEXTRA;
  buffer[4] = 0; // mtime
  buffer[5] = 0;
  buffer[6] = 0;
  buffer[7] = 0;
  buffer[8] = 0;
  buffer[9] = OS_UNKNOWN;
  buffer[10] = BGZF_XLEN;
  buffer[11] = 0;
  buffer[12] = BGZF_ID1;
  buffer[13] = BGZF_ID2;
  buffer[14] = BGZF_LEN;
  buffer[15] = 0;
  buffer[16] = 0; // placeholder for block length
  buffer[17] = 0;

  // loop to retry for blocks that do not compress enough
  int input_length = block_length;
  int compressed_length = 0;
  while (1) {
      z_stream zs;
      zs.zalloc = NULL;
      zs.zfree = NULL;
      //zs.next_in = fp->uncompressed_block;
      zs.next_in = (void*)c->buffer;
      zs.avail_in = input_length;
      zs.next_out = (void*)&buffer[BLOCK_HEADER_LENGTH];
      zs.avail_out = buffer_size - BLOCK_HEADER_LENGTH - BLOCK_FOOTER_LENGTH;

      //int status = deflateInit2(&zs, fp->compress_level, Z_DEFLATED,
      int status = deflateInit2(&zs, c->compress_level, Z_DEFLATED,
                                GZIP_WINDOW_BITS, Z_DEFAULT_MEM_LEVEL, Z_DEFAULT_STRATEGY);
      if (status != Z_OK) {
          fprintf(stderr, "deflate init failed\n");
          return -1;
      }
      status = deflate(&zs, Z_FINISH);
      if (status != Z_STREAM_END) {
          deflateEnd(&zs);
          if (status == Z_OK) {
              // Not enough space in buffer.
              // Can happen in the rare case the input doesn't compress enough.
              // Reduce the amount of input until it fits.
              input_length -= 1024;
              if (input_length <= 0) {
                  // should never happen
                  fprintf(stderr, "input reduction failed\n");
                  return -1;
              }
              continue;
          }
          fprintf(stderr, "deflate failed\n");
          return -1;
      }
      status = deflateEnd(&zs);
      if (status != Z_OK) {
          fprintf(stderr, "deflate end failed\n");
          return -1;
      }
      compressed_length = zs.total_out;
      compressed_length += BLOCK_HEADER_LENGTH + BLOCK_FOOTER_LENGTH;
      if (compressed_length > MAX_BLOCK_SIZE) {
          // should never happen
          fprintf(stderr, "deflate overflow\n");
          return -1;
      }
      break;
  }

  packInt16((uint8_t*)&buffer[16], compressed_length-1);
  uint32_t crc = crc32(0L, NULL, 0L);
  //crc = crc32(crc, fp->uncompressed_block, input_length);
  crc = crc32(crc, c->buffer, input_length);
  packInt32((uint8_t*)&buffer[compressed_length-8], crc);
  packInt32((uint8_t*)&buffer[compressed_length-4], input_length);

  int remaining = block_length - input_length;
  // since we read by blocks, we should have none remaining
  if (0 != remaining) {
      fprintf(stderr, "remaining bytes\n");
      exit(1);
  }
  //fp->block_offset = remaining;
  b->block_offset = remaining;

  return compressed_length;
}

void*
consumer_run(void *arg)
{
  consumer_t *c = (consumer_t*)arg;
  block_t *b = NULL;
  uint64_t n = 0;

  //fprintf(stderr, "consumer starting\n");
  while(1) {
      // get block
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

      // inflate/deflate
      if(0 == c->compress) {
          if((b->block_length = consumer_inflate_block(c, b)) < 0) {
              fprintf(stderr, "Error decompressing");
              exit(1);
          }
      }
      else {
          if((b->block_length = consumer_deflate_block(c, b)) < 0) {
              fprintf(stderr, "Error decompressing");
              exit(1);
          }
      }

      // put back a block
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
