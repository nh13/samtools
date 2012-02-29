#include <stdio.h>
#include <stdlib.h>
#include "bgzf.h"
#include "bam.h"

#define BUF_SIZE 0x10000

int bam_reheader(bamFile in, const bam_header_t *h, int fd)
{
	bamFile fp;
	bam_header_t *old;
	int len;
	uint8_t *buf;
	if (in->open_mode != 'r') return -1;
	buf = malloc(BUF_SIZE);
	old = bam_header_read(in);
	fp = bam_dopen(fd, "w");
	bam_header_write(fp, h);
        bam_flush(fp); // flush after the header
        /*
	if (in->block_offset < in->block_length) {
		bgzf_write(fp, in->uncompressed_block + in->block_offset, in->block_length - in->block_offset);
		bgzf_flush(fp);
	}
        */
#ifdef PBGZF_USE
        BGZF *fp_bgzf = in->r->fp_bgzf;
#else
        BGZF *fp_bgzf = fp;
#endif
#ifdef _USE_KNETFILE
	while ((len = knet_read(fp_bgzf->x.fpr, buf, BUF_SIZE)) > 0)
		fwrite(buf, 1, len, fp_bgzf->x.fpw);
#else
	while (!feof(fp_bgzf->file) && (len = fread(buf, 1, BUF_SIZE, fp_bgzf->file)) > 0)
		fwrite(buf, 1, len, fp_bgzf->file);
#endif
	free(buf);
	bam_close(fp);
	return 0;
}

int main_reheader(int argc, char *argv[])
{
	bam_header_t *h;
	bamFile in;
	if (argc != 3) {
		fprintf(stderr, "Usage: samtools reheader <in.header.sam> <in.bam>\n");
		return 1;
	}
	{ // read the header
		tamFile fph = sam_open(argv[1]);
		if (fph == 0) {
			fprintf(stderr, "[%s] fail to read the header from %s.\n", __func__, argv[1]);
			return 1;
		}
		h = sam_header_read(fph);
		sam_close(fph);
	}
	in = strcmp(argv[2], "-")? bam_open(argv[2], "r") : bam_dopen(fileno(stdin), "r");
	if (in == 0) {
		fprintf(stderr, "[%s] fail to open file %s.\n", __func__, argv[2]);
		return 1;
	}
	bam_reheader(in, h, fileno(stdout));
	bam_close(in);
	return 0;
}
