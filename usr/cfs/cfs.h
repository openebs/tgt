
#ifndef _CFS_H_
#define _CFS_H_

#include <stdint.h>
#include <sys/types.h>

#ifdef  __cplusplus
extern "C" {
#endif

typedef struct cb_indx {
    uint64_t dn;
    uint64_t cn;
    uint64_t dstart;
    uint64_t dsz;
    uint64_t cstart;
    uint64_t csz;
    off_t  off;
    size_t sz;
} cb_indx_t;


int cfs_write(int fd, const char *buf, size_t size, off_t offset);
int cfs_read(int fd, char *buf, size_t size, off_t offset);

int cfs_open(char *path, uint64_t *size, uint32_t blksize,
		uint64_t avgwsz, uint64_t writes, uint64_t chunks);
int cfs_close(int fd);

struct cfs_vol;
uint64_t cb_off2indx(struct cfs_vol *vd, size_t sz, off_t off, cb_indx_t *inx);

int cfs_libinit(void);

#ifdef  __cplusplus
}
#endif

#endif  /* _CFS_H_ */
