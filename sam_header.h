#ifndef __SAM_HEADER_H__
#define __SAM_HEADER_H__

#define SAM_HEADER_CURRENT_VERSION "1.4"

// NB: SAM header merging not supported

#ifdef __cplusplus
extern "C" {
#endif
    enum {
        SAM_HEADER_TYPE_NONE = -1,
        SAM_HEADER_TYPE_HD = 0,
        SAM_HEADER_TYPE_SQ = 1,
        SAM_HEADER_TYPE_RG = 2,
        SAM_HEADER_TYPE_PG = 3,
        SAM_HEADER_TYPE_CO = 4,
        SAM_HEADER_TYPE_NUM = 5
    };
    /*
       extern const char *SAM_HEADER_TYPE_TAGS[];

       enum {
       SAM_HEADER_HD_NONE = -1,
       SAM_HEADER_HD_VN = 0,
       SAM_HEADER_HD_SO = 1,
       SAM_HEADER_HD_GO = 2
       };

       extern const char *SAM_HEADER_HD_TAGS[];

       enum {
       SAM_HEADER_SQ_NONE = -1,
       SAM_HEADER_SQ_SN = 0,
       SAM_HEADER_SQ_LN = 1,
       SAM_HEADER_SQ_AS = 2,
       SAM_HEADER_SQ_M5 = 3,
       SAM_HEADER_SQ_SP = 4,
       SAM_HEADER_SQ_UR = 5
       };
       extern const char *SAM_HEADER_SQ_TAGS[];

       enum {
       SAM_HEADER_RG_NONE = -1,
       SAM_HEADER_RG_ID = 0,
       SAM_HEADER_RG_CN = 1,
       SAM_HEADER_RG_DS = 2,
       SAM_HEADER_RG_DT = 3,
       SAM_HEADER_RG_FO = 4,
       SAM_HEADER_RG_KS = 5,
       SAM_HEADER_RG_LB = 6,
       SAM_HEADER_RG_PG = 7,
       SAM_HEADER_RG_PI = 8,
       SAM_HEADER_RG_PL = 9,
       SAM_HEADER_RG_PU = 10,
       SAM_HEADER_RG_SM = 11
       };
       extern const char *SAM_HEADER_RG_TAGS[];

       enum {
       SAM_HEADER_PG_NONE = -1,
       SAM_HEADER_PG_ID = 0,
       SAM_HEADER_PG_PN = 1,
       SAM_HEADER_PG_CL = 2,
       SAM_HEADER_PG_PP = 3,
       SAM_HEADER_PG_VN = 4
       };
       extern const char *SAM_HEADER_PG_TAGS[];

       extern const char **SAM_HEADER_TAGS[];
       */

    typedef struct {
        void *hash;
        int32_t type;
        char tag[2];
    } sam_header_record_t;

    typedef struct {
        sam_header_record_t **records;
        int32_t n;
        int32_t type;
        char tag[2];
    } sam_header_records_t;

    typedef struct {
        void *hash;
    } sam_header_t;

    sam_header_record_t*
      sam_header_record_init(const char tag[2]);

    void
      sam_header_record_destroy(sam_header_record_t *r);

    int32_t
      sam_header_record_add(sam_header_record_t *r, const char *key, const char *value);

    int32_t
      sam_header_record_set(sam_header_record_t *r, const char *tag, const char *value);

    char*
      sam_header_record_get(sam_header_record_t *r, const char *tag);

    int32_t
      sam_header_record_check(sam_header_record_t *record);

    sam_header_record_t*
      sam_header_record_parse(const char *buf);

    sam_header_record_t*
      sam_header_record_clone(sam_header_record_t *src);

    sam_header_records_t*
      sam_header_records_init(const char tag[2]);

    void
      sam_header_records_destroy(sam_header_records_t *records);

    // NB: shallow copy
    void
      sam_header_records_add(sam_header_records_t *records, sam_header_record_t *record);

    sam_header_t*
      sam_header_init();

    void
      sam_header_destroy(sam_header_t *h);

    sam_header_records_t*
      sam_header_get_records(const sam_header_t *h, const char type_tag[2]);

    // NB: shallow copy
    int32_t
      sam_header_add_record(sam_header_t *h, sam_header_record_t *record, const char type[2]);

    sam_header_t*
      sam_header_clone(const sam_header_t *h);

    // NB: shallow copy
    char **
      sam_header_list(const sam_header_t *h, const char type_tag[2], const char key_tag[2], int *n);

    // NB: shallow copy
    void*
      sam_header_table(const sam_header_t *h, char type_tag[2], char key_tag[2], char value_tag[2]);

    const char *
      sam_tbl_get(void *h, const char *key);

    int 
      sam_tbl_size(void *h);

    void 
      sam_tbl_destroy(void *h);

    // NB: deep copy
    // NB: not implemented
    int32_t
      sam_header_merge_into(sam_header_t *dst, const sam_header_t *src);

    // NB: not implemented
    sam_header_t*
      sam_header_merge(int n, const sam_header_t **headers);

    sam_header_t*
      sam_header_parse2(const char *text);

    char*
      sam_header_write(const sam_header_t *h);

#ifdef __cplusplus
}
#endif

#endif
