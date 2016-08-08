
#ifndef _COMMITER_H_
#define _COMMITER_H_

#include <stdint.h>
#include <pthread.h>

#include "log.h"

#ifdef  __cplusplus
extern "C" {
#endif

#define MAX_COMMITER_THREADS 1

struct cfs_commiter {
	pthread_mutex_t lock;
	struct cfs_vol *vd;
	pthread_t 	   commiter_thrd[MAX_COMMITER_THREADS];
};

extern struct cfs_commiter *commiter;

extern int create_thread_commiter(struct cfs_vol *vd);
void *commiter_execute(void *arg);

#ifdef  __cplusplus
}
#endif

#endif /* _COMMITER_H_ */
