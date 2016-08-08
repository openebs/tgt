/*
 * Chunk File Backing Store
 *
 * Copyright (C) 2016-2017 ajith.kumar@cloudbyte.com
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, version 2 of the
 * License.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA
 */

#include <errno.h>
#include <stdlib.h>
#include <libgen.h>
#include <string.h>
#include <signal.h>
#include <sys/syscall.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "log.h"
#include "cfs_impl.h"
#include "evict.h"
#include "commiter.h"

/*
 * global data
 * will change this
 */
pthread_mutex_t g_lck;
cfs_vol_t *g_data[100] = {0};
int g_inuse = 0;

int curloglvl(struct cfs_vol *vd)
{
	return vd ? vd->loglvl : 0;
}
FILE *logfile(struct cfs_vol *vd)
{
	return vd ? vd->logfile : NULL;
}

uint64_t
cb_off2indx(struct cfs_vol *vd, size_t size, off_t offset, cb_indx_t *inx)
{
	inx->dn = offset>>vd->dshift;
	inx->dstart = inx->dn<<vd->dshift;
	inx->dsz = vd->dsz;
	uint64_t doff = offset - inx->dstart;
	inx->cn = doff>>vd->cshift;
	inx->cstart = inx->cn<<vd->cshift;
	inx->csz = vd->csz;
	uint64_t coff = doff - inx->cstart;

	inx->off = coff;
	if (coff + size >= vd->csz)
		inx->sz = vd->csz - coff;
	else
		inx->sz = size;
    return inx->sz;
}

mode_t fmode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
mode_t dmode = S_IRWXU|S_IRWXG|S_IRWXO;

int cfs_open_chunk_inline(cfs_vol_t *vd, cb_indx_t *ix, int *err)
{
	if (vd == NULL || ix == NULL)
		return -1;
	char fpath[PATH_MAX];
	int ret = -1;

	if (CB_VOLB[ix->dn] == 0) {
		snprintf(fpath, 256, "%s/%s/%ld", vd->rootdir, vd->volname, ix->dn);
		ret = mkdir(fpath, dmode);
		*err = errno;
		if (ret != 0 && errno != EEXIST) {
			/* we have a problem here */
			return -2;
		}
		CB_VOLB[ix->dn] = 1;
	}
	snprintf(fpath, 256, "%s/%s/%ld/%ld", vd->rootdir, vd->volname,
			ix->dn, ix->cn);
	ret = open(fpath, O_EXCL | O_RDWR, fmode);
	if (ret < 0)
		ret = open(fpath, O_CREAT | O_EXCL | O_RDWR, fmode);
	*err = errno;
	if (ret < 0)
		return -3;

	CB_VOL[ix->dn][ix->cn].cfd = ret;

	log_info("cb_read_rd(%ld/%ld  off:%ld sz=%ld) opened fd:%d\n",
					ix->dn, ix->cn, ix->off, ix->sz, ret);
	return ret;
}

int cfs_open_chunk(cfs_vol_t *vd, uint64_t dn, uint64_t cn, int *err)
{
	if (vd == NULL || dn > 1023 || cn > 1023)
		return -1;
	char fpath[PATH_MAX];
	int ret = -1;

	if (CB_VOLB[dn] == 0) {
		snprintf(fpath, 256, "%s/%s/%ld", vd->rootdir, vd->volname, dn);
		ret = mkdir(fpath, dmode);
		*err = errno;
		if (ret != 0 && errno != EEXIST) {
			/* we have a problem here */
			return -2;
		}
		CB_VOLB[dn] = 1;
	}
	snprintf(fpath, 256, "%s/%s/%ld/%ld", vd->rootdir, vd->volname, dn, cn);
	ret = open(fpath, O_EXCL | O_RDWR, fmode);
	if (ret < 0)
		ret = open(fpath, O_CREAT | O_EXCL | O_RDWR, fmode);
	*err = errno;
	if (ret < 0)
		return -3;

	CB_VOL[dn][cn].cfd = ret;

	log_info("cb_read_rd(%ld/%ld) opened fd:%d\n", dn, cn, ret);
	return ret;
}

int cfs_read(int fd, char *buf, size_t size, off_t offset)
{
	if (fd<0 || fd>99)
		return -1;
	pthread_mutex_lock(&g_lck);
	cfs_vol_t *vd = g_data[fd];
	pthread_mutex_unlock(&g_lck);

	const char *msg;
	char *path = vd->volname;
	char *startptr = NULL;
    char *bptr = buf;

	cb_indx_t indx;
	size_t sz = size;
	off_t off = offset;
	uint64_t sz_now = 0;
	int read = 0;
	int rd = 0, cp = 0;
	uint64_t rdsz = 0, cpsz = 0;
	int err;
	int r;
	int cur;
	if (off + sz > vd->size) {
    	log_crit("cfs_read(fd=%d:%s, off:%lld, size=%d) beyond EOF %lld\n",
				fd, path, offset, size, vd->size);
		return -1;
	}

    while (sz > 0) {
		sz_now = cb_off2indx(vd, sz, off, &indx);
		sz -= sz_now ; off += sz_now;

		pthread_mutex_lock(&(CB_VOL[indx.dn][indx.cn].cmtx));
		if (CB_VOL[indx.dn][indx.cn].cc_start_ptr == NULL) {

			if (CB_VOL[indx.dn][indx.cn].cfd == -1)
				cfs_open_chunk_inline(vd, &indx, &err);

			if (CB_VOL[indx.dn][indx.cn].cfd == -1) {
				msg = "open-failed";
				goto do_crash;
			}

			++rd; rdsz += indx.sz;
			r = pread(CB_VOL[indx.dn][indx.cn].cfd, bptr, indx.sz,
						sizeof(chunk_header_t) + indx.off);
			err = errno;
			if (r<0) {
				msg = "read-failed";
				goto do_crash;
			}

			if (r > 0 && r < indx.sz)
				bzero(bptr+r, indx.sz-r);

    		log_debug("cb_read_rd(%ld/%ld  off:%ld sz=%ld) actualread:%d\n",
					indx.dn, indx.cn, indx.off, indx.sz, r);

			bptr += indx.sz;
			read += indx.sz;
		} else {
			startptr = CB_VOL[indx.dn][indx.cn].cc_start_ptr;
			cur = CB_CUR;
			CB_BITS[cur].cbx[indx.dn][indx.cn].read = 1;
			CB_BITG.cbx[indx.dn][indx.cn].read = 1;
	
    		log_debug("cb_read_cp(%ld/%ld  off:%ld sz=%ld)\n",
					indx.dn, indx.cn, indx.off, indx.sz);

			++cp; cpsz += indx.sz;
			memcpy(bptr, startptr + sizeof(chunk_header_t) + indx.off, indx.sz);

			bptr += indx.sz;
			read += indx.sz;
		}
		pthread_mutex_unlock(&(CB_VOL[indx.dn][indx.cn].cmtx));
	}

    log_info("cfs_read-done(off:%lld, size=%d) rd/cp:%d/%d (%ld/%ld) read:%d\n",
        offset, size, rd, cp, rdsz, cpsz, read);
    return read;

do_crash:
	log_crit("cfs_read-ERR(%ld/%ld  off:%ld sz=%ld) %s! %s\n",
			indx.dn, indx.cn, indx.off, indx.sz, msg, strerror(err));
	log_flush(vd);
	usleep(100);
	raise(11);
	return -1;
}

int cfs_write(int fd, const char *buf, size_t size, off_t offset)
{
	if (fd<0 || fd>99)
		return -1;
	pthread_mutex_lock(&g_lck);
	cfs_vol_t *vd= g_data[fd];
	pthread_mutex_unlock(&g_lck);
	char *path = vd->volname;
	cyclic_buffer_t *nvram_buffer = vd->nvram_buffer;
	struct cfs_commiter *commiter = vd->commiter;
    write_log_t wl;
    int rc;
	if (offset + size > vd->size) {
    	log_crit("cfs_write(fd=%d:%s, off:%lld, size=%d) beyond EOF %lld\n",
				fd, path, offset, size, vd->size);
		return -1;
	}

    wl.wl_type = IO_WRITE;
	wl.wl_fd = fd;
    wl.wl_offset = offset;
    wl.wl_size = size;

    rc = buf_insert(nvram_buffer, &wl, (uint8_t *)buf, &commiter->lock);
    if(rc != 0)
    { 
    	log_crit("cfs_write:%s size=%d, offset=%lld return(%d)\n",
				vd->volname, size, offset, rc);
		return -1;
    }
    log_debug("cfs_write:%s size=%d off:%lld\n", vd->volname, size, offset);
    return size;
}

static int
init_backing_store(char *rdir, char *vname, int *err)
{
	int rc;
	char fpath[PATH_MAX];
	mode_t mode = S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH;

	snprintf(fpath, 256, "%s/%s", rdir, vname);
	rc = mkdir(fpath, mode);
	*err = errno;
	if (rc != 0) {
		/*
		 * TODO: 
		 *  - stricter checks to ensure data in this directory belongs to cfs
		 *  - before consuming, check signatures of each chunk present,
		 *      with the metadata file that has the chunk hashes
		 */
		switch (*err) {
			case EEXIST:
				rc = 0;
				break;
			case ENOTDIR:
			case ENOSPC:
			case EROFS:
			case EACCES:
			case ENOENT:
			default:
				rc = 1;
				break;
		}
	}
	return rc;
}

cfs_vol_t *
cfs_get_vd_in_fdslot(int fd)
{
	if (fd<0 || fd>99)
		return NULL;
	pthread_mutex_lock(&g_lck);
	cfs_vol_t *vd= g_data[fd];
	pthread_mutex_unlock(&g_lck);
	return vd;
}

static int
cfs_reserve_fdslot(cfs_vol_t *vd)
{
	int i, ret=-1;
	pthread_mutex_lock(&g_lck);
	for (i=0; i<100; ++i) {
		if (g_data[i] == NULL) {
			g_data[i] = vd;
			ret = i;
			++g_inuse;
			break;
		}
	}
	pthread_mutex_unlock(&g_lck);
	if (ret == -1) {
		fprintf(stderr, "\n slots are all full, increase size of g_data array \n");
	}
	return ret;
}

static int
highestbitset(uint64_t i)
{
	int h = 1;
	if (i == 0)
		return (0);
	if (i & 0xffffffff00000000ul) {
		h += 32; i >>= 32;
	}
	if (i & 0xffff0000) {
		h += 16; i >>= 16;
	}
	if (i & 0xff00) {
		h += 8; i >>= 8;
	}
	if (i & 0xf0) {
		h += 4; i >>= 4;
	}
	if (i & 0xc) {
		h += 2; i >>= 2;
	}
	if (i & 0x2) {
		h += 1;
	}
	return (h);
}

int
cfs_open(char *path, uint64_t *size, uint32_t blksize,
		uint64_t avgwsz, uint64_t writes, uint64_t chunks)
{
	int i, j;
	int ret = -1;
	int ln;
	int err;
	uint64_t nsz;
	char *rpath;
	char *lpath1, *lpath2;
	char *src, *rdir;
	char *base;
	const char *msg = "";
	cfs_vol_t *vd;

	if (path == NULL) {
		fprintf(stderr, "volume-name missing");
		return -1;
	}
	if ((blksize & (blksize-1)) != 0) {
		fprintf(stderr, "vol:%s blksize:%d not power-of-2",
				path, blksize);
		return -2;
	}
	nsz = *size;
	/*nsz = (-(-(nsz) & -(blksize))); //round-up to blksize
	if (*size != nsz) {
		*size = nsz;
		msg = "force-blk-aligned";
	}*/

	rpath = realpath(path, NULL);  //need to free rpath
	if (rpath == NULL)
		src = path;
	else
		src = rpath;
	lpath1 = strdup(src);
	lpath2 = strdup(src);
	base = basename(lpath1);
    rdir = dirname(lpath2);
	if (rdir == NULL || base == NULL) {
		ret = -3;
		goto err_ret0;
	}
	err = 0;

	if (init_backing_store(rdir, base, &err) != 0) {
		ret = -4;
		goto err_ret0;
	}


	vd = malloc(sizeof(cfs_vol_t));
    if (vd == NULL) {
		fprintf(stderr, "\nENOMEM: vol:%s alloc cfs_vol failed\n", path);
		ret = -5;
		goto err_ret0;
    }
	bzero(vd, sizeof(cfs_vol_t));

    vd->rootdir = rdir;
	vd->verr = err;
	strncpy(vd->volname, base, sizeof(vd->volname));
	vd->volname[sizeof(vd->volname)-1] = '\0';
	ln = strlen(vd->volname);
	vd->tname[0] = vd->volname[ln-4];
	vd->tname[1] = vd->volname[ln-3];
	vd->tname[2] = vd->volname[ln-2];
	vd->tname[3] = vd->volname[ln-1];
	vd->tname[4] = 0;
    vd->logfile = log_open(src);
	vd->loglvl = 4;

	vd->size = nsz;
	vd->blksize = blksize;
  	/*
	 * calculate vd->dshift, vd->cshift, vd->dsz, vd->csz
	 * based on the size and blocksize
	 * todo: comeback for better code/logic
	 */
	vd->dshift = 30; //1GB
	vd->cshift = 20; //1MB
	vd->dsz = (uint64_t)1<<vd->dshift;
	vd->csz = (uint64_t)1<<vd->cshift;

	if (vd->size > (vd->dsz * 1024)) {
		/*
		 * size requested is more than what we can handle
		 * in the 1024 x 1024 chunks 
		 */
		uint64_t rcsz = vd->size / (1024*1024);
		vd->csz = (-(-(rcsz) & -(vd->blksize))); //round up to blksize
		vd->cshift = highestbitset(vd->csz) + (vd->csz != rcsz);
		vd->dsz = vd->csz * 1024;
		vd->dshift = highestbitset(vd->dsz);

		uint64_t ncsz = (uint64_t)1<<vd->cshift ;
		if ((ncsz * 1024 * 1024) < vd->size) {
			/* something is wrong with the calculations */
			log_crit("cfs_open: vol:%s sz:%lu blksz:%lu ERROR in calculation!\n"
					"       [dir_sz:%lu shift:%lu  chunk_sz:%lu shift:%lu]%s\n",
					vd->indx, path, vd->size, vd->blksize,
					vd->dsz, vd->dshift, vd->csz, vd->cshift, msg);
			ret = -6;
			goto err_ret;
		}
	}

	for (i=0; i<1024; ++i) {
		for (j=0; j<1024; ++j) {
			vd->c[i][j].cfd = -1;
			pthread_mutex_init(&(vd->c[i][j].cmtx), NULL);
		}
	}

    vd->nvram_buffer = buf_init(vd, writes, avgwsz);
    vd->chunk_buffer = chunk_buf_init(vd, chunks, vd->csz); 
	if (vd->nvram_buffer == NULL || vd->chunk_buffer == NULL) {
		ret = -7;
		goto err_ret;
	}

    create_thread_commiter(vd);
    create_thread_evicter(vd);

	vd->indx = cfs_reserve_fdslot(vd);

	log_notice("cfs_open: fd:%d vol:%s sz:%lu blksz:%lu "
			" [dir_sz:%lu shift:%lu  chunk_sz:%lu shift:%lu]%s\n",
			vd->indx, path, vd->size, vd->blksize,
			vd->dsz, vd->dshift, vd->csz, vd->cshift, msg);
	log_info("cfs_open: basedir: %s / %s [%s short:%s]\n",
			vd->rootdir, base, vd->volname, vd->tname);

	free(rpath);
	free(lpath1);
	return vd->indx;

err_ret:
	for (i=0; i<1024; ++i) {
		for (j=0; j<1024; ++j) {
			vd->c[i][j].cfd = -1;
			pthread_mutex_destroy(&(vd->c[i][j].cmtx));
		}
	}
	if (vd->nvram_buffer)
		buf_free(vd->nvram_buffer);
	if (vd->chunk_buffer)
		chunk_buf_free(vd->chunk_buffer);
	free(vd);

err_ret0:
	free(rpath);
	free(lpath1);
	free(lpath2);
	return ret;
}

int
cfs_close(int fd)
{
	if (fd<0 || fd>99) {
		fprintf(stderr, "\nvolume close for index:%d is out of range!\n", fd);
		return -1;
	}
	pthread_mutex_lock(&g_lck);
	cfs_vol_t *vd = g_data[fd];
	if (vd != NULL)
		--g_inuse;
	g_data[fd] = NULL;
	pthread_mutex_unlock(&g_lck);

	if (vd == NULL) {
		fprintf(stderr, "\nvolume for index:%d is already destroyed?\n", fd);
		return -2;
	}

	int i, j, rc;
	char *path = vd->volname;
	cyclic_buffer_t *nvram_buffer = vd->nvram_buffer;
	chunk_buffer_t *chunk_buffer = vd->chunk_buffer;
	struct cfs_commiter *commiter = vd->commiter;
	struct cfs_evicter *evicter = vd->evicter;
    nvram_buffer->cleanup++;
    chunk_buffer->cleanup++;

    log_info("cfs_close(%d:%s) starting.. signal chunker and evicter to stop\n",
			fd, path);

    while (nvram_buffer->cleanup > 0)
		sleep(1);
    while (chunk_buffer->cleanup > 0)
		sleep(1);

    log_notice("cfs_close(%d:%s) going to final stage...\n", fd, path);

	pthread_mutex_destroy(&commiter->lock);
	pthread_mutex_destroy(&evicter->lock);

	for (i=0; i<1024; ++i) {
		for (j=0; j<1024; ++j) {
			if (vd->c[i][j].cfd != -1) {
				rc = close(vd->c[i][j].cfd);
				if (rc != 0)
					log_crit(" close:%d.%d:%d returned %d ERROR:%s\n",
							i, j, vd->c[i][j].cfd, rc, strerror(errno));
				else
					log_debug(" close:%d.%d:%d returned %d\n",
							i, j, vd->c[i][j].cfd, rc);
			}
			vd->c[i][j].cfd = -1;
			pthread_mutex_destroy(&(vd->c[i][j].cmtx));
		}
	}

	buf_free(nvram_buffer);
	chunk_buf_free(chunk_buffer);

    free(commiter);
    free(evicter);
    free(vd);
	return 0;
}

/*
 * cfs backing store global initialization and cleanup
 */
int create_thread_timer(void);

int
cfs_libinit(void)
{
	int i;
	pthread_mutex_init(&g_lck, NULL);
	pthread_mutex_lock(&g_lck);
	g_inuse = 0;
	for (i=0; i<100; ++i)
		g_data[i] = NULL;

	create_thread_timer();
	pthread_mutex_unlock(&g_lck);
	return 0;
}

int
cfs_init(char *path, uint64_t size, uint64_t chunks, uint64_t writes)
{
	return 0;
}

int
cfs_exit(void)
{
	/* signal all threads to exit */
	int i;
	pthread_mutex_lock(&g_lck);
	for (i=0; i<100; ++i) {
		if (g_data[i] != NULL) {
			pthread_mutex_unlock(&g_lck);
			sleep(2);
			fprintf(stderr, "\nslot:%d in-use. (need to handle this)\n", i);
			sleep(2);
			pthread_mutex_lock(&g_lck);
		}
	}
	pthread_mutex_unlock(&g_lck);

	pthread_mutex_destroy(&g_lck);
	return 0;
}
