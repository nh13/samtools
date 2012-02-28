#include <stdlib.h>
#include <stdint.h>
#include "../bgzf.h"
#include "block.h"

block_t*
block_init()
{
  block_t *b = calloc(1, sizeof(block_t));
  b->buffer = malloc(sizeof(uint8_t) * MAX_BLOCK_SIZE);
  return b;
}

void
block_destroy(block_t *block)
{
  if(NULL == block) return;
  free(block->buffer);
  free(block);
}
