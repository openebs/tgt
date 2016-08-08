
#include "commiter.h"
#include "evict.h"
#include "cfs_impl.h"
#include "log.h"


#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <pthread.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <signal.h>
#include <sys/syscall.h>

int thread_exit;



extern int buf_read(cyclic_buffer_t *cb, write_log_t **wl, uint8_t **data);
extern int chunk_buf_update(chunk_buffer_t *cb, struct write_log *wl,
		uint8_t *data, struct cfs_evicter *er);

void 
*commiter_execute(void *arg)
{
	struct cfs_vol *vd = (struct cfs_vol *)arg;
	static int commiter_stopped = 0;
	struct cfs_commiter *commiter = vd->commiter;
	struct cfs_evicter  *evicter = vd->evicter;
	cyclic_buffer_t *nvram_buffer = vd->nvram_buffer;
	chunk_buffer_t *chunk_buffer = vd->chunk_buffer;
	write_log_t	 *wl;	
	uint8_t *data;
	int rc, i, tind=-1;
	int loop = 0, spin = 0, now=0, processed = 0;
	int toexit = 0;
	pthread_t self = pthread_self();
	pid_t tid;
	tid = syscall(SYS_gettid);
	for(i=0; i<MAX_COMMITER_THREADS;i++) {
		if (commiter->commiter_thrd[i] == self) {
			tind = i;
			snprintf(tinfo, sizeof tinfo, "c#%s.%d.%d", vd->tname, i, tid);
			pthread_setname_np(self, tinfo);
			break;
		}
	}
	if (tind == -1) {
		fprintf(stderr, "\nin-commiter thread:%lx  c#%s.%d.%d   %lx  issue!!\n",
				self, vd->tname, tind, tid, (uint64_t)(((uint64_t *)self)[0]));
		fprintf(stderr, "\n---------------------\n---------------------\n");
	}
	pthread_detach(self);

	while (evicter == NULL)
		evicter = vd->evicter;

	log_notice("commiter-thread:%lx  c#%s.%d.%d\n", self, vd->tname, tind, tid);
	for (;;) {
		toexit = 0;
		++loop;
		pthread_mutex_lock(&commiter->lock);
		if (nvram_buffer->count == 0) {
			if (nvram_buffer->cleanup != 0)
				toexit = 1;
			pthread_mutex_unlock(&commiter->lock);
			++spin;
			goto end; 
		}

		rc = buf_read(nvram_buffer, &wl, &data);
		pthread_mutex_unlock(&commiter->lock);
		if(rc != 0) {
			log_crit("error in reading buf :%d ", rc);	
			goto end; 
		}
		
		spin = 0;
		switch (wl->wl_type) {
			case IO_READ:
				raise(11); /* not expected here */
				break;
			case IO_WRITE:
			case IO_DELETE:
				++now; ++processed;
				chunk_buf_update(chunk_buffer, wl, data, evicter);
				break;
		}

end:
		if (toexit == 1) {
			/*
			 * TODO: fix the thread exit signaling,
			 *  - two stage process of signalling and acknowleding
			 *  - nvram_buffer member access with lock
			 */
			commiter_stopped++;
			if(commiter_stopped== MAX_COMMITER_THREADS) {
				nvram_buffer->cleanup--; 
			}
			log_crit(" commiter:%d/%d/%d EXITING\n", loop, spin, processed);
			pthread_exit(0);
			return NULL;
		}

		if (spin > 1) {
			if (spin > 10000)
				usleep(10000);
			else
				usleep(spin);
		}
		if ((loop % 4000 == 1) || ((spin  > 0) && (now != 0))) {
			log_info(" chunker:%d/%d  %d-of-%d\n", loop, spin, now, processed);
		}
		if (spin > 0)
			now = 0;
	}
	return NULL;
}
int 
create_thread_commiter(struct cfs_vol *vd)
{
	int rc;
    int i = 0;
	struct cfs_commiter *commiter = malloc(sizeof(struct cfs_commiter));
	commiter->vd = vd;
 	vd->commiter = commiter;

	pthread_mutex_init(&(commiter->lock), NULL);
	
	for(i=0; i<MAX_COMMITER_THREADS;i++) {
		rc = pthread_create(&(commiter->commiter_thrd[i]), NULL, &commiter_execute, vd);
		if (rc != 0) {
			log_crit("pthread_create() failed for thread %d, rc = %d\n", i, rc);
			return -1;
		}
		log_info("commiter thread %d: %lx/%ld (ret:%d)\n", i, commiter->commiter_thrd[i], 
				(uint64_t)(((uint64_t *)commiter->commiter_thrd[i])[0]), rc);
	}
        return 0;
} 





