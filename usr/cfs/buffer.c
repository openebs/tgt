#include "cfs_impl.h"
//#include "commiter.h"

#include <limits.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

#include "log.h"


void buf_free(cyclic_buffer_t* cb)
{
    free(cb->buffer);
	free(cb);
}

size_t
buf_buffer_size(cyclic_buffer_t *cb)
{
	return cb->size;
}

static uint8_t *
buf_end(cyclic_buffer_t *cb)
{
	return cb->buffer_end;
}

size_t
buf_bytes_free(cyclic_buffer_t *cb)
{
	return cb->size - cb->use;
}

size_t
buf_bytes_used(cyclic_buffer_t *cb)
{
	return cb->use;
}

int
buf_is_full(cyclic_buffer_t *cb)
{
	return buf_bytes_free(cb) == 0;
}

int
buf_is_empty(cyclic_buffer_t *cb)
{
	return buf_bytes_free(cb) == buf_buffer_size(cb);
}

int g_rd_debug = 1;

cyclic_buffer_t *buf_init(struct cfs_vol *vd, int64_t count, uint64_t avgrsize)
{
    cyclic_buffer_t *cb = malloc(sizeof(cyclic_buffer_t));
	uint64_t bufsz = (sizeof(write_log_t) + avgrsize) * count + 1024;
    cb->buffer = malloc(bufsz+1);
	if (cb->buffer == NULL)
	{
    	log_crit("buf_init failed to allocate nvram_buffer sz:%lu\n", bufsz);
		free(cb);
		return NULL;
	}
	cb->vd = vd;
    cb->buffer_end = cb->buffer + bufsz;
    cb->size = bufsz;
    cb->head = cb->buffer;
    cb->tail = cb->buffer;
    cb->count = 0;
	cb->rolla = 0;
	cb->rollb = 0;
    cb->cleanup = 0;
	*(uint64_t *)cb->head = 0;
    log_notice("buf_init done [%p--%p] sz:%lu\n", cb->buffer, cb->buffer_end, cb->size);
	return cb;
}

int
buf_insert(cyclic_buffer_t *cb, write_log_t *wl, uint8_t *data, pthread_mutex_t *lock)
{
	struct cfs_vol *vd = cb->vd;
	pthread_mutex_lock(lock);
	uint8_t *bufend = buf_end(cb);
	if ((sizeof(write_log_t) * 2) + wl->wl_size  > buf_bytes_free(cb)) {
		pthread_mutex_unlock(lock);
		log_crit(" no-space to insert  hdr+%d -- free:%d [h:%d  t:%d] (%d/%d)\n",
				wl->wl_size, buf_bytes_free(cb), cb->head-cb->buffer, cb->tail-cb->buffer, cb->rolla, cb->rollb);
		/* trigger chunker */
		sleep(1);
		return -1;
	}
	size_t done = 0;
	uint8_t loop = 0;
	while (loop++ < 2) {
		size_t copied = 0;
		size_t count = (loop == 1) ? sizeof(write_log_t) : wl->wl_size;
		uint8_t *src = (loop == 1) ? (uint8_t *)wl : (uint8_t *)data; 

		while (copied != count) {
			if (cb->head == bufend) {
				++cb->rolla;
				cb->head = cb->buffer;
			}
			uint8_t *e = cb->head >= cb->tail ? bufend : cb->tail;
			size_t n = MIN(e - cb->head, count - copied);
			int x = cb->head - cb->buffer;
			if (((loop == 1) && (n < sizeof(write_log_t))) ||
				((loop == 2) && (n < wl->wl_size))) {

				log_debug(" insert: h:%d->%d    t:%d min(%d, %d-%d=%d) = %d (e:%d) [used:%d free:%d] (%d/%d) SKIP\n",
						x, x+n,
						cb->tail-cb->buffer, e - cb->head, count, copied, count-copied, n, 
						e-cb->buffer, buf_bytes_used(cb), buf_bytes_free(cb), cb->rolla, cb->rollb);

				//skip the last n bytes
				cb->head += n;
				cb->use += n;

			} else {

				log_debug(" insert: h:%d->%d    t:%d min(%d, %d-%d=%d) = %d (e:%d) [used:%d free:%d] %d->%d (%d/%d)\n",
						x, x+n,
						cb->tail-cb->buffer, e - cb->head, count, copied, count-copied, n,
						e-cb->buffer, buf_bytes_used(cb), buf_bytes_free(cb), done, done+n, cb->rolla, cb->rollb);
				memcpy(cb->head, src + copied, n);
				cb->head += n;
				cb->use += n;
				copied += n;
				done += n;
			}
		}
	}
	if (cb->use > cb->size) {
		log_crit(" read2:  use above size!!\n");
	}
	if (done == sizeof(write_log_t) + wl->wl_size) {
		cb->count++;
		pthread_mutex_unlock(lock);
		return 0;
	} else {
		pthread_mutex_unlock(lock);
		return done;
	}
}

int
buf_read(cyclic_buffer_t *cb, write_log_t **wl, uint8_t **data)
{
	struct cfs_vol *vd = cb->vd;
	uint8_t *bufend = buf_end(cb);
	*wl = NULL; *data = NULL;
	if (buf_bytes_used(cb) < sizeof(write_log_t)) {
		log_notice("read:: nothing to read!! used:%d  [h:%d  t:%d]\n",
				buf_bytes_used(cb), cb->head-cb->buffer, cb->tail-cb->buffer);
		return -1;
	}
	size_t done = 0;
	uint8_t loop = 0;
	while (loop++ < 2) {
		size_t copied = 0;
		size_t count = (loop == 1) ? sizeof(write_log_t) : (*wl)->wl_size;

		while (copied != count) {
			if (cb->tail == bufend) {
				++cb->rollb;
				cb->tail = cb->buffer;
			}
			uint8_t *e = cb->tail >= cb->head ? bufend : cb->head;
			size_t n = MIN(e - cb->tail, count - copied);
			int x = cb->tail - cb->buffer;
			if (((loop == 1) && (n < sizeof(write_log_t))) ||
				((loop == 2) && (n < (*wl)->wl_size))) {

				log_debug(" read: h:%d    t:%d->%d min(%d, %d-%d=%d) = %d (e:%d) [used:%d free:%d] skip\n",
						cb->head - cb->buffer, x, x+n,
						e - cb->tail, count, copied, count-copied, n,
						e-cb->buffer, buf_bytes_used(cb), buf_bytes_free(cb));
				cb->tail += n;
				cb->use -= n;
			} else {
				log_debug(" read: h:%d    t:%d->%d min(%d, %d-%d=%d) = %d (e:%d) [used:%d free:%d] %d (%d/%d)\n",
						cb->head - cb->buffer, x, x+n,
						e - cb->tail, count, copied, count-copied, n,
						e-cb->buffer, buf_bytes_used(cb), buf_bytes_free(cb), done, cb->rolla, cb->rollb);
				if (cb->use == 0)
					break;

				if (loop == 1)
					*wl= (write_log_t *)(cb->tail);
				else
					*data= (uint8_t *)(cb->tail);
				cb->tail += n;
				cb->use -= n;
				copied += n;
				done += n;
				if (cb->tail == cb->head) {
					/* no more data to read */
					if (!buf_is_empty(cb)) {
						log_debug(" read:not-empty?? h:%d t:%d  used:%d free:%d\n",
								cb->head- cb->buffer, cb->tail-cb->buffer, buf_bytes_used(cb), buf_bytes_free(cb));
					}
					if (copied != count) {
						log_crit(" read:could only read %d of %d \n", copied, count);
					}
					break;
				}
			}
		}
		if (cb->use < 0) {
			log_crit(" read:  use went negative!!\n");
		}
		//done += copied;
	}
	if (loop > 2)  {
		log_info("read:: h:%d    t:%d      %d/%d wl:%p:%ld-%ld  data:%p\n",
						cb->head - cb->buffer, cb->tail - cb->buffer, cb->rolla, cb->rollb,
						(*wl), (*wl)->wl_offset, (*wl)->wl_size, (*data));
	} else {
		log_info("read:: h:%d    t:%d   failed\n",
						cb->head - cb->buffer, cb->tail - cb->buffer);
	}
	if (*wl != NULL && *data != NULL && (done == sizeof(write_log_t) + (*wl)->wl_size)) {
		--cb->count;
		return 0;
	} else {
		return -2;
	}
}
