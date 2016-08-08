
#include "evict.h"
#include "cfs.h"
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

#include <sys/syscall.h>

//#include "params.h"

struct cfs_timer *timer;

int
create_thread_timer(void)
{
 	timer = malloc(sizeof(struct cfs_timer));
	pthread_mutex_init(&(timer->lock), NULL);
	return pthread_create(&(timer->thr), NULL, &timer_execute, timer);
}


int 
create_thread_evicter(struct cfs_vol *vd)
{
	int i, rc;
 	vd->evicter = malloc(sizeof(struct cfs_evicter));

	pthread_mutex_init(&(vd->evicter->lock), NULL);

	for(i=0; i<MAX_EVICT_THREADS;i++) {
		rc = pthread_create(&(vd->evicter->evicter_thrd[i]), NULL, &evicter_execute, vd);
		if (rc != 0) {
			log_crit("pthread_create() failed for thread %d, rc = %d\n", i, rc);
			return -1;
		}

		log_notice("evict thread %d: %lx  (ret:%d)\n", i, vd->evicter->evicter_thrd[i], rc);
	}
    return 0;
} 

void 
*evicter_execute(void *arg)
{
	struct cfs_vol *vd= (struct cfs_vol *)arg;
	static int evicter_stopped = 0;
	struct cfs_evicter	 *evicter = vd->evicter;
	chunk_buffer_t *chunk_buffer = vd->chunk_buffer;	
	int i;
	int loop = 0, spin = 0, now;
	int processed = 0;
	int toexit = 0;
	int chunks = 0;
	pthread_t self = pthread_self();
	pid_t tid;
	tid = syscall(SYS_gettid);
	for(i=0; i<MAX_EVICT_THREADS;i++) {
		if (evicter->evicter_thrd[i] == self) {
			snprintf(tinfo, sizeof tinfo, "e#%s.%d.%d", vd->tname, i, tid);
			pthread_setname_np(self, tinfo);
			break;
		}
	}

	pthread_detach(self);
	log_notice(" in-evicter thread:%lx   e#%s.%d.%d  %lx\n", self, vd->tname, i, tid, (uint64_t)(((uint64_t *)self)[0]));


	for (;;) {
		++loop;
		now = 0;
		pthread_mutex_lock(&evicter->lock);
		if (chunk_buffer->cleanup != 0) {
			toexit = 1;
			pthread_mutex_unlock(&evicter->lock);
			goto end; 
		}
		chunks = chunk_buffer->inuse;
		if (chunk_buffer->inuse == 0) {
			pthread_mutex_unlock(&evicter->lock);
			++spin;
			goto end;
		}
		/* evict if
		     - not accessed in the last 30 minutes, or,
			 - free slots in chunk_buffer < threshold, evict oldest 10 chunks
			 -          while evicting, if dirty, need to write to disk.
			 -    bits for read/write/usecnt
		*/

		now = chunk_find_and_evict(chunk_buffer);
		if (now != 0)
			spin = 0;
		else
			++spin;
		processed += now;
		pthread_mutex_unlock(&evicter->lock);

end:
		if (toexit == 1) {
			pthread_mutex_lock(&evicter->lock);
			evicter_stopped++;
			if(evicter_stopped == MAX_EVICT_THREADS) {
				chunk_buffer->cleanup--;
			}
			pthread_mutex_unlock(&evicter->lock);
			log_crit(" evict:%d/%d/%d exiting\n", loop, spin, processed);
			pthread_exit(0);
		}

		if (spin > 1 || now == 0) {
			if (spin > 50000)
				usleep(50000);
			else if (spin < 2000)
				usleep(2000);
			else
				usleep(spin);
		}
		if (loop % 2000 == 1) {
			log_debug(" evict:%d/%d   now:%d total:%d  chunks:%d\n", loop, spin, now, processed, chunks);
		}
	}
	return 0;
}

extern struct cfs_vol *g_data[100];
extern pthread_mutex_t g_lck;

void 
*timer_execute(void *arg)
{
	//struct cfs_timer *timer = (struct cfs_timer *)arg;
	int loop = 0, processed = 0;
	struct cfs_vol *vd;
	int i, cur=0, vols, fd;
	pthread_t self = pthread_self();
	pid_t tid;
	tid = syscall(SYS_gettid);
	snprintf(tinfo, sizeof tinfo, "timer.%d", tid);
	pthread_setname_np(self, tinfo);
	pthread_detach(self);

	fprintf(stderr, "timer thread:%lx/%d\n", self, tid);

	for (;;) {

		vols = 0;
		++loop;
		fd = -1;
		sleep(5);
		if (1) //(loop % 3 == 2)
		{
			for (i=0; i<100 && g_data[i]!=NULL; ++i)
			{
				fd = i;
				pthread_mutex_lock(&g_lck);
				vd = g_data[i];
				++vols;
				if (CB_CUR == 2)
					CB_CUR = 0;
				else
					++(CB_CUR);
				cur = CB_CUR;
				pthread_mutex_unlock(&g_lck);
				++processed;
			}
		}
		if (loop % 2 == 1) {
			fprintf(stderr, " %d / %d -- now:%d [fd:%d cur:%d]\n", loop, processed, vols, fd, cur);
		}
	}

	return 0;
}
