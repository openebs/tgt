#include "cfs_impl.h"
#include "commiter.h"
#include "evict.h"

#include <limits.h>
#include <bits/time.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

#include "log.h"
#include "cfs.h"

chunk_buffer_t *chunk_buf_init(struct cfs_vol *vd, int64_t count, uint64_t chunksize)
{
    chunk_buffer_t *cb = malloc(sizeof(chunk_buffer_t));
	uint64_t chunkbufsz = (sizeof(chunk_header_t) + chunksize) * count;
    cb->buffer = malloc(chunkbufsz);
    cb->buffer_end = cb->buffer + chunkbufsz; 
    cb->size = count;
    cb->inuse = 0;
	cb->fmap = malloc(count * sizeof(uint8_t)); //debug-ability now, will move to real-bitmap later
	cb->vd = vd;
    cb->cleanup = 0;
	if (cb->buffer == NULL || cb->fmap == NULL) {
		log_crit("chunk_buf_init %p/%p \n", cb->buffer, cb->fmap);
		if (cb->buffer != NULL)
			free(cb->buffer);
		if (cb->fmap != NULL)
			free(cb->fmap);
		free(cb);
		return NULL;
	}
	bzero(cb->fmap, count);
	log_notice("chunk_buf_init %p-%p  sz:%lu  [%lu chunks of size:%lu]\n",
			cb->buffer, cb->buffer_end, chunkbufsz, count, chunksize);
	return cb;
}


void chunk_buf_free(chunk_buffer_t *cb)
{
    free(cb->buffer);
    free(cb->fmap);
}

extern mode_t fmode;
int g_bitmapissue = 0;
uint64_t g_freethreshold = 10000000000;
int g_freeskip = 0;

int cfs_open_chunk(cfs_vol_t *vd, uint64_t dn, uint64_t cn, int *err);

int 
chunk_buf_evict(chunk_buffer_t *cb, chunk_header_t *ch, uint8_t *data)
{
	struct cfs_vol *vd = cb->vd;
	int err;
	int ret;
	uint64_t c1 = ch->ch_c1;
	uint64_t c2 = ch->ch_c2;

	pthread_mutex_lock(&(CB_VOL[c1][c2].cmtx));
	if (CB_VOL[c1][c2].cfd == -1)
		CB_VOL[c1][c2].cfd = cfs_open_chunk(vd, c1, c2, &err);

	if (CB_VOL[c1][c2].cfd == -1) {
		pthread_mutex_unlock(&(CB_VOL[c1][c2].cmtx));
		log_crit("evict-open-failed %d  %lu:%ld/%ld  [off:%ld sz:%ld]\n",
				err, ch->ch_fd, c1, c2, ch->ch_off, ch->ch_size);
		return 1;
	}
	ret = pwrite(CB_VOL[c1][c2].cfd, ch, sizeof(chunk_header_t) + ch->ch_size, 0);
	pthread_mutex_unlock(&(CB_VOL[c1][c2].cmtx));

	log_debug("evict-write:  %ld:%ld/%ld  [off:%ld sz:%ld + %ld] wrote:%d\n",
			ch->ch_fd, c1, c2, ch->ch_off, sizeof(chunk_header_t), ch->ch_size, ret);

	return ret;
}

int chunk_find_and_evict(chunk_buffer_t *cb)
{
	struct cfs_vol *vd = cb->vd;
	chunk_header_t *ch;
	uint8_t *data;
	int processed = 0;
	uint64_t free;
	int i, j;
	int oldest, prev, cur = CB_CUR;
	if (cur == 2) {
		oldest = 0; prev = 1;
	} else if (cur == 1) {
		oldest = 2; prev = 0;
	} else {
		oldest = 1; prev = 2;
	}
	free = cb->size - cb->inuse;

	if (free > g_freethreshold) {
		++g_freeskip;
		return 0;  //no need to evict
	}
	
	// evict the oldest unused buffer
	// non-dirty (as in already saved to ssd), or, zero use-count, chunk to evict
	for (i=0; i<1024; ++i) {
		for (j=0; j<1024; ++j) {
			if (CB_BITS[oldest].cbx[i][j].cbs == 0 &&
					CB_BITS[prev].cbx[i][j].cbs == 0 &&
					CB_BITS[cur].cbx[i][j].cbs == 0 &&
					CB_BITG.cbx[i][j].cbs != 0)
			{
				/* may evict the oldest reference */
				ch = (chunk_header_t *)(CB_VOL[i][j].cc_start_ptr);
				if (ch == NULL)
					continue;
				data = CB_VOL[i][j].cc_start_ptr + sizeof (chunk_header_t);
				chunk_buf_evict(cb, ch, data);

				CB_VOL[i][j].cc_start_ptr = NULL;
				CB_BITS[oldest].cbx[i][j].cbs = 0;
				cb->fmap[ch->ch_bindx] = 0;
				--cb->inuse;
				++processed;
				log_info("evict2:%d) %d/%d  %d [%d-%d-%d]\n", processed, i, j, ch->ch_bindx, oldest, prev, cur);
			}
		}
	}
	for (i=0; i<1024; ++i) {
		for (j=0; j<1024; ++j) {
			if (CB_BITS[oldest].cbx[i][j].cbs != 0 &&
					CB_BITS[prev].cbx[i][j].cbs == 0 &&
					CB_BITS[cur].cbx[i][j].cbs == 0) {

				/* may evict the oldest reference */
				ch = (chunk_header_t *)(CB_VOL[i][j].cc_start_ptr);
				data = CB_VOL[i][j].cc_start_ptr + sizeof (chunk_header_t);
				chunk_buf_evict(cb, ch, data);

				CB_VOL[i][j].cc_start_ptr = NULL;
				CB_BITS[oldest].cbx[i][j].cbs = 0;
				cb->fmap[ch->ch_bindx] = 0;
				--cb->inuse;
				++processed;
				log_info("evict1:%d) %d/%d  %d [%d-%d-%d]\n", processed, i, j, ch->ch_bindx, oldest, prev, cur);
			}
		}
	}

	/* print the access bitmaps for debugging eviction */
	for (i=0; i<1024; ++i) {
		uint32_t cnt = 0;
		uint8_t buf[1024];

		for (j=0; j<1024; ++j) {
			uint8_t fl = 0;
			if (CB_BITS[oldest].cbx[i][j].cbs != 0)
				fl <<= 1;
			if (CB_BITS[prev].cbx[i][j].cbs != 0)
				fl <<= 1;
			if (CB_BITS[cur].cbx[i][j].cbs != 0)
				fl <<= 1;
			if (CB_BITG.cbx[i][j].cbs != 0)
				fl <<= 1;

			if (fl > 0)
				++cnt;
			buf[j] = fl;
		}
		if (cnt > 0) {
			log_debug("\n x:%d) set in %d slots --------------------- \n", i, cnt); 
			for (j=0; j<1024; ++j) {
				if (buf[j]>0)
					log_debug("%d.%d, ", j, buf[j]);
			}
		}
	}
	return processed;
}


char *chunk_get_buf_slot(chunk_buffer_t *cb, int *bindx, int *c1, int *c2)
{
	struct cfs_vol *vd = cb->vd;
	chunk_header_t *chh; 
	char *ptr = NULL;
	int i, j;
	int oldest, prev, cur = CB_CUR;
	if (cb->inuse < cb->size) {
		for (i=0; i<cb->size; ++i) {
			if (cb->fmap[i] == 0) {
				cb->fmap[i] = 1;
				ptr = cb->buffer + (i * (sizeof(chunk_header_t) + vd->csz));
				chh = (chunk_header_t *)ptr;
				chh->ch_bindx = i;
				*c1 = -1; *c2 = -1;
				//*c1 = i>>10;  // i/1024
			   	//*c2 = i&0x3FF; // i%1024
				break;
			}
		}
	}
	if (ptr == NULL) {
		//pick-up some non-dirty,  zero use-count, chunk to reuse
		if (cur == 2) {
			oldest = 0; prev = 1;
		} else if (cur == 1) {
			oldest = 2; prev = 0;
		} else {
			oldest = 1; prev = 2;
		}
		/* first level search, evict any data that was read */
		for (i=0; i<1024; ++i) {
			for (j=0; j<1024; ++j) {
				if (CB_BITS[oldest].cbx[i][j].cbs != 0 &&
						CB_BITS[oldest].cbx[i][j].write == 0) {
					if (CB_BITS[prev].cbx[i][j].write == 0 &&
							CB_BITS[cur].cbx[i][j].write == 0 && 
							CB_VOL[i][j].cc_start_ptr != NULL) {
						ptr = CB_VOL[i][j].cc_start_ptr;
						CB_VOL[i][j].cc_start_ptr = NULL; /* unlink from the current user */
						*c1 = i; *c2 = j;
						break;
						/*bindx = (i << 10) + j;
						if (cb->fmap[bindx] != 1) {
							cb->fmap[bindx] = 1;
							++g_bitmapissue;
						}*/
					}
				}
			}
		}
	}
	if (ptr == NULL) {
		/* first level search failed, do more aggressive evict/reuse now */
		for (i=0; i<1024; ++i) {
			for (j=0; j<1024; ++j) {
				if (CB_BITS[oldest].cbx[i][j].cbs != 0 &&
						CB_BITS[oldest].cbx[i][j].write != 0) {
					if (CB_BITS[prev].cbx[i][j].write == 0 &&
							CB_BITS[cur].cbx[i][j].write == 0 && 
							CB_VOL[i][j].cc_start_ptr != NULL) {
						ptr = CB_VOL[i][j].cc_start_ptr;
						CB_VOL[i][j].cc_start_ptr = NULL; /* unlink from the current user */
						*c1 = i; *c2 = j;
						break;
						/*bindx = (i << 10) + j;
						if (cb->fmap[bindx] != 1) {
							cb->fmap[bindx] = 1;
							++g_bitmapissue;
						}*/
					}
				}
			}
		}
	}
	if (ptr == NULL) {
		log_crit("\n couldn't find a free slot..need better eviction logic\n");
	}
	return ptr;
}

int 
chunk_buf_update(chunk_buffer_t *cb, struct write_log *wl, 
			uint8_t *data, struct cfs_evicter *evicter)
{
	struct cfs_vol *vd = cb->vd;
	chunk_header_t *chunkhead; 
	char *startptr = NULL;
	size_t sz = wl->wl_size;
	off_t off = wl->wl_offset;
	uint64_t sz_now = 0;
	cb_indx_t indx;
	int err;
	int c1, c2, cur, bindx = -1;

 	pthread_mutex_lock(&(evicter->lock));
	do {
	     sz_now = cb_off2indx(vd, sz, off, &indx);
		 sz -= sz_now ; off += sz_now;

   		pthread_mutex_lock(&(CB_VOL[indx.dn][indx.cn].cmtx));

		 //process each chunks in this write

		if (CB_VOL[indx.dn][indx.cn].cc_start_ptr == NULL) {

			startptr = chunk_get_buf_slot(cb, &bindx, &c1, &c2);
			while (startptr == NULL) {
				pthread_mutex_unlock(&(CB_VOL[indx.dn][indx.cn].cmtx));
				pthread_mutex_unlock(&(evicter->lock));
				sleep(10);
				pthread_mutex_lock(&(evicter->lock));
				pthread_mutex_lock(&(CB_VOL[indx.dn][indx.cn].cmtx));
				startptr = chunk_get_buf_slot(cb, &bindx, &c1, &c2);
			};
			++cb->inuse;
			
		 	CB_VOL[indx.dn][indx.cn].cc_start_ptr = startptr;
			cur = CB_CUR;
			CB_BITS[cur].cbx[indx.dn][indx.cn].cbs = 1;
			CB_BITG.cbx[indx.dn][indx.cn].cbs = 1;
			clock_gettime(CLOCK_REALTIME, &(CB_VOL[indx.dn][indx.cn].catime));
			
			chunkhead = (chunk_header_t *)startptr;
			chunkhead->ch_c1= indx.dn;
			chunkhead->ch_c2= indx.cn;
			chunkhead->ch_state = DIRTY;
			chunkhead->ch_size = indx.csz;
			chunkhead->ch_uniq++;
			chunkhead->ch_cksm.bc_word[0] = 0;
			chunkhead->ch_cksm.bc_word[1] = 0;
			chunkhead->ch_cksm.bc_word[2] = 0;
			chunkhead->ch_cksm.bc_word[3] = 0;
			chunkhead->ch_next_ptr = NULL;
			chunkhead->ch_fd = wl->wl_fd;
			startptr += sizeof(chunk_header_t);

			if (c1 == -1) {
				log_info("cbuf_update-1 %ld/%ld  off:%ld sz:%ld (%d)%p [bindx:%d]\n",
						indx.dn, indx.cn, indx.off, indx.sz, cur, wl, chunkhead->ch_bindx);
			} else {
				log_info("cbuf_update-1 %ld/%ld  off:%ld sz:%ld (%d)%p [bindx:%d] (reuse-old-%d/%d)\n",
						indx.dn, indx.cn, indx.off, indx.sz, cur, wl, chunkhead->ch_bindx, c1, c2);
			}

			if (CB_VOL[indx.dn][indx.cn].cfd == -1)
				CB_VOL[indx.dn][indx.cn].cfd = cfs_open_chunk(vd, indx.dn, indx.cn, &err);
			/* TODO: optimize this read */
			pread(CB_VOL[indx.dn][indx.cn].cfd, startptr, indx.csz, 0);

		} else {

			startptr = CB_VOL[indx.dn][indx.cn].cc_start_ptr;
			chunkhead = (chunk_header_t *)startptr;
			cur = CB_CUR;
			CB_BITS[cur].cbx[indx.dn][indx.cn].cbs = 1;
			CB_BITG.cbx[indx.dn][indx.cn].cbs = 1;
			clock_gettime(CLOCK_REALTIME, &(CB_VOL[indx.dn][indx.cn].catime));
			chunkhead->ch_uniq++;
			startptr += sizeof(chunk_header_t);

			log_info("cbuf_update-2 %d:%ld/%ld  off:%ld sz:%ld (%d)%p [bindx:%d]\n",
					chunkhead->ch_fd, indx.dn, indx.cn, indx.off, indx.sz, cur, wl, chunkhead->ch_bindx);

		}
   		memcpy(startptr+indx.off, data, indx.sz);
		data += indx.sz;
		//blake2b( &chunkhead->ch_cksm, startptr, NULL, sizeof(blk_cksum_t), 8192, 0);


   		pthread_mutex_unlock(&(CB_VOL[indx.dn][indx.cn].cmtx));

	} while (sz > 0);
 	pthread_mutex_unlock(&(evicter->lock));


	log_info("cbuf_update %s\n", "done");
   	
	return 0;
}
