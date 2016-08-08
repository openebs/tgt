
#ifndef _CFS_IMPL_H_
#define _CFS_IMPL_H_

#include <sys/types.h>
#include <stdint.h>
#include <pthread.h>

#include <linux/limits.h>
#include <stdio.h>

#include "cfs.h"

#ifdef  __cplusplus
extern "C" {
#endif

typedef enum io_ops {
	IO_READ,
	IO_WRITE,
	IO_DELETE
} io_ops_t;

typedef struct write_log {
        uint64_t        wl_type;
		uint64_t        wl_fd;
        uint64_t        wl_offset;
        uint64_t        wl_size;
} write_log_t;

#define MIN(A,B)  ((A) > (B) ? (B) : (A))

enum state { READING, DIRTY, FLUSHING, WRITING, WRITTEN };

typedef struct buf
{
     uint8_t *buffer;
     uint8_t *buffer_end;
     uint8_t *head;
     uint8_t *tail;
     uint64_t count;
     uint64_t size;
     uint64_t use;
	 struct cfs_vol *vd;
     uint16_t rolla;
     uint16_t rollb;
     int8_t  cleanup;
} cyclic_buffer_t;

typedef struct buf_chunk
{
     char * buffer;
     char * buffer_end;
     uint64_t inuse;
     uint64_t size;
	 uint8_t *fmap;
	 struct cfs_vol *vd;
     int8_t  cleanup;
} chunk_buffer_t;

/*
 * Each block has a 256-bit checksum  BLAKE2B
 */
typedef struct blk_cksum {
	    uint64_t    bc_word[4];
} blk_cksum_t;

/*
 * on disk chunk header, this is followed by data of 'ch_size' bytes
 */
typedef struct chunk_header {
        uint8_t         ch_state;
		uint8_t			ch_misc[7]; //unused now
        uint64_t        ch_uniq;
		uint64_t		ch_fd;
        uint64_t        ch_off;
        uint64_t        ch_size;
        uint64_t        ch_c1;
        uint64_t        ch_c2;
        uint64_t        ch_bindx;
        blk_cksum_t     ch_cksm;
        struct chunk_header *ch_next_ptr;   //if there is are multiple copies of the same chunk,
											//they needs to be in this list
} chunk_header_t;

typedef struct cfs_chunk {
    union {
        uint64_t      cc_flags;
        struct {
            uint64_t  cc_inited:1;
            uint64_t  cc_fdopen:1;
            uint64_t  cc_dirty:1;
            uint64_t  cc_in_ssd:1;
        };
    };
    int              cc_ccnt; //refcount
    int              cfd;  //chunkfd

    void             *cc_start_ptr; //chunk_start_ptr in the chunk buffer

    struct timespec  catime;
    struct timespec  cwtime;

    pthread_mutex_t  cmtx;

} cfs_chunk_t;

/* uin8_t for now to debug */
typedef struct cb_bits {
    union {
        uint8_t      cbs;
        struct {
            uint8_t  read:1;
            uint8_t  write:1;
            uint8_t  usecnt:6;
        };
    };
} cbx_t; 

typedef struct cb_bitmap {
	cbx_t cbx[1024][1024];
} cbm_t; 

struct cfs_commiter;
struct cfs_evicter;

typedef struct cfs_vol {
	uint64_t	indx; //index into the global volume list
	uint64_t	holds;
	uint64_t	loglvl;
	pthread_mutex_t lck;
    FILE		*logfile;

	int			verr;

	uint64_t    size;     // total size of the volume

	uint64_t    blksize;  // physical blocksize
	uint64_t    blkshift;

	uint64_t 	dshift;   // first level chunks
	uint64_t 	dsz;      // fisrt level chunks
	uint64_t 	cshift;   // leaf chunks
	uint64_t 	csz;      // leaf chunk size

	struct cfs_commiter *commiter;
	struct cfs_evicter  *evicter;

	cyclic_buffer_t *nvram_buffer;
	chunk_buffer_t  *chunk_buffer;

    char		*rootdir;
	char		tname[64];     //smaller name, for easy tracking in logs
	char		volname[256];

	uint8_t     d[1024];       //just to track sub-directory presence
    cfs_chunk_t c[1024][1024]; //chunks: 1024 1GB directories; each having 1024 1MB files

	volatile int cur;       // current index into the cbm bitmap array
	cbm_t		cbm[3];    // access bitmaps for the last 3 time slabs
	cbm_t		cbg;       // global access bitmap
} cfs_vol_t;

#define CB_DATA vd 
#define CB_VOLNAME vd->volname
#define CB_VOL vd->c
#define CB_VOLB vd->d

#define CB_CUR  vd->cur
#define CB_BITS vd->cbm
#define CB_BITG vd->cbg

int curloglvl(struct cfs_vol *vd);
FILE *logfile(struct cfs_vol *vd);

cyclic_buffer_t *buf_init(cfs_vol_t *vd, int64_t count, uint64_t avgrsize);
void buf_free(cyclic_buffer_t *cb);

int is_buf_full(cyclic_buffer_t *cb);
int is_buf_empty(cyclic_buffer_t *cb);

int buf_read(cyclic_buffer_t *cb, write_log_t **wl, uint8_t **data);
int buf_insert(cyclic_buffer_t *cb, write_log_t *wl, uint8_t *data, pthread_mutex_t *lock);

chunk_buffer_t *chunk_buf_init(cfs_vol_t *vd, int64_t count, uint64_t chunksize);
void chunk_buf_free(chunk_buffer_t *cb);

int chunk_find_and_evict(chunk_buffer_t *cb);

#endif /* _CFS_IMPL_H_ */
