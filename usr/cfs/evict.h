#ifndef _EVICT_H_
#define _EVICT_H_

#include <stdint.h>
#include <pthread.h>

#ifdef  __cplusplus
extern "C" {
#endif

#define MAX_EVICT_THREADS 1

struct cfs_timer {
	pthread_mutex_t lock;
	pthread_t 	thr;
};
extern struct cfs_timer *timer;

struct cfs_evicter {
	pthread_mutex_t lock;
	struct cfs_vol  *vd;
	pthread_t 	evicter_thrd[MAX_EVICT_THREADS];
};

extern struct cfs_evicter *evicter;

extern int create_thread_evicter(struct cfs_vol *cb_data);
void *evicter_execute(void *arg);
void *timer_execute(void *arg);
void *doSomeThing(void *arg);

#ifdef  __cplusplus
}
#endif

#endif /* _EVICT_H_ */
