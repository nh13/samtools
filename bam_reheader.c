#include <stdio.h>
#include <stdlib.h>
#include "knetfile.h"
#include "bgzf.h"
#include "bam.h"

#define BUF_SIZE 0x10000

static bamFile
bam_reheader_open_input(const char *fn_in, const char* __restrict mode)
{
	bamFile in = strcmp(fn_in, "-")? bam_open(fn_in, mode) : bam_dopen(fileno(stdin), mode);
	if (in == 0) {
		return NULL;
	}
	return in;
}

int bam_reheader(const char *fn_in, const bam_header_t *h, int fd)
{
	bamFile in, out;
	bam_header_t *old;
	int len;
        uint8_t *buf;

	in = bam_reheader_open_input(fn_in, "r");
	if (NULL == in) {
		fprintf(stderr, "[%s] fail to open file %s.\n", __func__, fn_in);
		return 1;
	}
	if (in->is_write) return -1;
	buf = malloc(BUF_SIZE);
	old = bam_header_read(in);
	out = bam_dopen(fd, "w");
	bam_header_write(out, h);
        bam_flush(out); // flush after the header
        /*
	if (in->block_offset < in->block_length) {
		bgzf_write(out, in->uncompressed_block + in->block_offset, in->block_length - in->block_offset);
		bgzf_flush(out);
	}
        */
#ifdef _PBGZF_USE
	int64_t pos = bam_tell(in);
	bam_close(in);
	in = NULL;
	BGZF *fp_bgzf_in = bgzf_open(fn_in, "r");
	if(NULL == fp_bgzf_in) {
		fprintf(stderr, "[%s] fail to open file %s.\n", __func__, fn_in);
		return 1;
	}
	if(bgzf_seek(fp_bgzf_in, pos, SEEK_SET) < 0) {
		fprintf(stderr, "[%s] fail to seek.\n", __func__);
		return 1;
	}
	BGZF *fp_bgzf_out = out->w->fp_bgzf;
#else
	BGZF *fp_bgzf_in = in;
	BGZF *fp_bgzf_out = out;
#endif
	while ((len = bgzf_read(fp_bgzf_in->fp, buf, BUF_SIZE)) > 0)
		fwrite(buf, 1, len, fp_bgzf_out->fp);
	free(buf);
	bam_close(out);
#ifdef _PBGZF_USE
	bgzf_close(fp_bgzf_in);
#endif
	bam_close(in);
	return 0;
}

int main_reheader(int argc, char *argv[])
{
	bam_header_t *h;
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
	return bam_reheader(argv[2], h, fileno(stdout));
}
