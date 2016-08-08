/*
*/
#include "commiter.h"
#include "evict.h"
#include "cfs_impl.h"

#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>


#include <sys/syscall.h> // for syscall - for tid
#include "log.h"
#include "cfs.h"

#if SELF_TEST
int blake2b( uint8_t *out, const void *in, const void *key, const uint8_t outlen, const uint64_t inlen, uint8_t keylen );

extern mode_t fmode;

int
in_readfile(char *path, char *lbuf, size_t sz)
{
	int ret = -1;
	int fd = open(path, O_EXCL | O_RDWR, fmode);
	if (fd < 0) {
		fprintf(stderr, "\n rerun after creating %s of size %ld bytes (with data for tests)(%d)\n", path, sz, errno);
		return ret;
	}
    ret = pread(fd, lbuf, sz, 0);
	close(fd);
	if (ret < sz) {
		fprintf(stderr, "\n rerun after creating %s of size %ld bytes (with data for tests)(read:%d err:%d)\n",
				path, sz, ret, errno);
		return ret;
	}
	return ret;
}

typedef struct cfs_tester {
	pthread_t  tester[4];
	struct cfs_vol *vd;
	size_t sz[4][20];	
	off_t off[4][20];	
	char path[PATH_MAX];
} cfs_tester_t;

#define out_base "./"

void *
test_worker(void *arg)
{
	char out[64];
	int ret, tret;
	int i, j, tndx=0;
	int size;
	cfs_tester_t *t= (cfs_tester_t *)arg;
	struct cfs_vol *vd = t->vd;
	pthread_t self = pthread_self();
	pid_t tid;
	tid = syscall(SYS_gettid);
	for (i=0; i<2; ++i) {
		if (t->tester[i]  == self) {
			tndx = i;
			snprintf(tinfo, sizeof tinfo, "tst#%s.%d.%d", vd->tname, i, tid);
			pthread_setname_np(self, tinfo);
			break;
		}
	}
	blk_cksum_t csm0, csm1;
	char pat0[] = "0123456789AA0123456789BB0123456789CC0123456789DD0123456789EE0123456789FF0123456789GG01";
	char pat1[] = "00112233445566778899AA00112233445566778899BB00112233445566778899CC00112233445566778899";
	char buf0[8192];
	char buf1[8192];
	char *bp0 = buf0, *bp1 = buf1;
	char ab[8192];
	for (i=0; i<128; ++i) {
		if (i==0) {
			snprintf(bp0+(i*64), 64, "PAT0PAT0PAT0 %d-%s", i, pat0);
			snprintf(bp1+(i*64), 64, "pat1pat1pat1 %d-%s", i, pat1);
		} else {
			snprintf(bp0+(i*64), 64, " %d-%s", i, pat0);
			snprintf(bp1+(i*64), 64, " %d-%s", i, pat1);
		}
	}
	
  	blake2b((uint8_t *)&csm0, buf0, NULL, sizeof(blk_cksum_t), 8192, 0);
  	blake2b((uint8_t *)&csm1, buf1, NULL, sizeof(blk_cksum_t), 8192, 0);
	fprintf(stderr, " csm0:%lx.%lx.%lx.%lx   csm1:%lx.%lx.%lx.%lx \n",
			csm0.bc_word[0], csm0.bc_word[1], csm0.bc_word[2], csm0.bc_word[3],
			csm1.bc_word[0], csm1.bc_word[1], csm1.bc_word[2], csm1.bc_word[3]);


	sleep(1);

	int fd;
	int ind = 0;
	size_t sz[20] = {0};
	off_t off[20] = {0};

	for (i=0; i<5; ++i) {
		sz[i] = t->sz[tndx][i];
		off[i] = t->off[tndx][i];
	}
	int slp[20] =   { 1, 2, 1, 2, 4};
	size_t rem;
	off_t sat; /* start at */

	for (i=0; i<3; ++i) {
		sleep(2);
    	fprintf(stderr, "\n-----------------------------------\ntest-%d with size %ld\n", i, sz[i]);

		for (j=0; j<4; ++j) {
			rem = sz[i]; sat= off[j];
			printf("\n\ntester::%d/%d   vol:%d  writing off:%ld sz:%ld   \n\n", i, j, ind, sat, rem);
			while (rem > 0) {
				int now = rem>8192 ? 8192 : rem; 
				tret = 0;
				do {
					ret = cfs_write(ind, buf0, now, sat);
					if (ret > 0) tret += ret;
					usleep(10);
				} while (ret == -1);
				rem -= now;
				sat += now;
			}
			if (slp[j] > 0)
				sleep(slp[j]);
		}
		sleep(3);
		log_info(" going to read and check now \n");
		fprintf(stderr, "\n going to read and check now \n");

		for (j=0; j<4; ++j) {
			bzero(ab, sizeof(ab));
			rem = sz[i]; sat = off[j];
			while (rem > 0) {
				char *bp = ab;
				int now = rem>8192 ? 8192 : rem; 
				tret = 0;
				do {
					ret = cfs_read(ind, bp, now, sat);
					if (ret > 0) {
						tret+=ret; bp+=ret;
					}
					usleep(10);
				} while (ret == -1 || tret!=now);
				if (tret != now || ((ret=memcmp(buf0, ab, now)) != 0)) {
					if (tret > 0) {
						blk_cksum_t csmx0;
						fflush(stderr); sleep(10);
						snprintf(out, 63, out_base "o-%ld-%ld--%ld-%d", off[j], sz[i], sat, now);
						fd = open(out, O_EXCL | O_CREAT | O_WRONLY, fmode);
						if (fd < 0) {
							fprintf(stderr, "\n creation of %s failed %d\n", out, errno);
							exit(1);
						}
						size = pwrite(fd, ab, tret, 0);
						close(fd);
						fprintf(stderr, "\n issue - %ld-%ld  %ld-%d  read:%d (wrote:%d) (ret:%d)\n",
								off[j], sz[i], sat, now, tret, size, ret);

						blake2b((uint8_t *)&csmx0, ab, NULL, sizeof(blk_cksum_t), now, 0);
						fprintf(stderr, " csmx0:%lx.%lx.%lx.%lx   csm0:%lx.%lx.%lx.%lx \n",
								csmx0.bc_word[0], csmx0.bc_word[1], csmx0.bc_word[2], csmx0.bc_word[3],
								csm0.bc_word[0], csm0.bc_word[1], csm0.bc_word[2], csm0.bc_word[3]);
	
						bp0 = buf0, bp1 = ab;
						int rep =  now/64;
						for (i=0; i<rep; ++i) {
							ret = memcmp(bp0+(i*64), bp1+(i*64), 64);
							if (ret!=0)
							  fprintf(stderr, "%d, [%.*s]vs[%.*s]  cmp:%d\n",
									i, 64, bp0+(i*64), 64, bp1+(i*64), ret);
						}
						exit(1);
					} else {
						fprintf(stderr, "\n issue - %ld-%ld read:%d (ret:%d)\n", off[j], sz[i], tret, ret);
							exit(1);
					}
				} else {
					if (now == 8192) {
						blk_cksum_t csmx0;
						blake2b((uint8_t *)&csmx0, ab, NULL, sizeof(blk_cksum_t), now, 0);
						if ((csm0.bc_word[0] != csmx0.bc_word[0]) ||
								(csm0.bc_word[0] != csmx0.bc_word[0]) ||
								(csm0.bc_word[0] != csmx0.bc_word[0]) ||
								(csm0.bc_word[0] != csmx0.bc_word[0])) {
							fprintf(stderr, " issue withcsum %ld-%ld (%d) "
									"csmx0:%lx.%lx.%lx.%lx   csm0:%lx.%lx.%lx.%lx \n",
									off[j], sz[i], tret,
									csmx0.bc_word[0], csmx0.bc_word[1], csmx0.bc_word[2], csmx0.bc_word[3],
									csm0.bc_word[0], csm0.bc_word[1], csm0.bc_word[2], csm0.bc_word[3]);
						} else {
							fprintf(stderr, "\n good  %ld-%ld (%d) withcsum\n", off[j], sz[i], tret);
						}
					} else {
						fprintf(stderr, "\n good  %ld-%ld (%d)\n", off[j], sz[i], tret);
					}
				}
				rem -= now;
				sat += now;
			}
		}

		fprintf(stderr, "\n going to wait for eviction\n");
		log_info(" going to wait for eviction, and read and check\n");
		sleep(50);

		fprintf(stderr, "\n after possible eviction, now read and check\n");
		log_info(" after possible eviction, now read and check\n");

		for (j=0; j<4; ++j) {
			bzero(ab, sizeof(ab));
			rem = sz[i]; sat = off[j];
			while (rem > 0) {
				char *bp = ab;
				int now = rem>8192 ? 8192 : rem; 
				tret = 0;
				do {
					ret = cfs_read(ind, bp, now, sat);
					if (ret > 0) {
						tret+=ret; bp+=ret;
					}
					usleep(10);
				} while (ret == -1 || tret!=now);
				if (tret != now || ((ret=memcmp(buf0, ab, now)) != 0)) {
					if (tret > 0) {
						blk_cksum_t csmx0;
						fflush(stderr); sleep(10);
						snprintf(out, 63, out_base "o-%ld-%ld--%ld-%d", off[j], sz[i], sat, now);
						fd = open(out, O_EXCL | O_CREAT | O_WRONLY, fmode);
						if (fd < 0) {
							fprintf(stderr, "\n creation of %s failed %d\n", out, errno);
							exit(1);
						}
						size = pwrite(fd, ab, tret, 0);
						close(fd);
						fprintf(stderr, "\n issue-2 - %ld-%ld  %ld-%d  read:%d (wrote:%d) (ret:%d)\n", off[j], sz[i], sat, now, tret, size, ret);

						blake2b((uint8_t *)&csmx0, ab, NULL, sizeof(blk_cksum_t), now, 0);
						fprintf(stderr, " csmx0:%lx.%lx.%lx.%lx   csm0:%lx.%lx.%lx.%lx \n",
								csmx0.bc_word[0], csmx0.bc_word[1], csmx0.bc_word[2], csmx0.bc_word[3],
								csm0.bc_word[0], csm0.bc_word[1], csm0.bc_word[2], csm0.bc_word[3]);
						
						bp0 = buf0, bp1 = ab;
						int rep =  now/64;
						for (i=0; i<rep; ++i) {
							ret = memcmp(bp0+(i*64), bp1+(i*64), 64);
							if (ret!=0)
							  fprintf(stderr, "%d, [%.*s]vs[%.*s]  cmp:%d\n",
									i, 64, bp0+(i*64), 64, bp1+(i*64), ret);
						}
						exit(1);
					} else {
						fprintf(stderr, "\n issue-2 - %ld-%ld read:%d (ret:%d)\n", off[j], sz[i], tret, ret);
							exit(1);
					}
				} else {
					if (now == 8192) {
						blk_cksum_t csmx0 ;
						blake2b((uint8_t *)&csmx0, ab, NULL, sizeof(blk_cksum_t), now, 0);
						if ((csmx0.bc_word[0] != csm0.bc_word[0]) || (csmx0.bc_word[0] != csm0.bc_word[0]) || 
								(csmx0.bc_word[0] != csm0.bc_word[0]) || (csmx0.bc_word[0] != csm0.bc_word[0])) {
							fprintf(stderr, " issue withcsum2 %ld-%ld (%d)  csmx0:%lx.%lx.%lx.%lx   csm0:%lx.%lx.%lx.%lx \n",
									off[j], sz[i], tret,
									csmx0.bc_word[0], csmx0.bc_word[1], csmx0.bc_word[2], csmx0.bc_word[3],
									csm0.bc_word[0], csm0.bc_word[1], csm0.bc_word[2], csm0.bc_word[3]);
						} else {
							fprintf(stderr, "\n good2  %ld-%ld (%d) withcsum2\n", off[j], sz[i], tret);
						}
					} else {
						fprintf(stderr, "\n good2  %ld-%ld (%d)\n", off[j], sz[i], tret);
					}
				}
				rem -= now;
				sat += now;
			}
		}
	}
	return NULL;
}

cfs_vol_t *cfs_get_vd_in_fdslot(int fd);
int
do_test(int fd)
{
	struct cfs_vol *vd = cfs_get_vd_in_fdslot(fd);
	char *path = CB_VOLNAME;
	size_t sz0[20] = { 9999,  vd->dsz+8192, 8192, 4096,       6400, 0 };
	off_t off0[20] = { 0,     vd->dsz+8192,    0, 8192, vd->dsz-8192, 0 };

	size_t sz1[20] = { 1024,        8192,        8192, 4096,       6400, 0 };
	off_t off1[20] = { vd->dsz, vd->dsz+8192, vd->dsz+vd->dsz, 8192,       8192, 0 };

	int i, rc;

	cfs_tester_t *t = malloc(sizeof(cfs_tester_t));
	for (i=0; i<5; ++i) {
		t->sz[0][i] = sz0[i];
		t->off[0][i] = off0[i];

		t->sz[1][i] = sz1[i];
		t->off[1][i] = off1[i];
	}
	t->vd = vd;

	for (i=0; i<2; ++i) {
	
		rc = pthread_create(&(t->tester[i]), NULL, &test_worker, t);
		if (rc != 0) {
			fprintf(stderr, "pthread_create failed for tester td:%d, rc=%d \n", i, rc);
			return -1;
		}
		log_info("tester thread for %d:%s  %d) tid:%lx\n", fd, path, i, t->tester[i]);
	}

	return 0;
}
#endif

#if DO_SELF_TEST

int cfs_init(void);

int main(int argc, char *argv[])
{
	int fds[100] = {0};

	if (argc < 7) {
		printf(" please run as  %s <path_to_volume> <blksize> <size> <w_avg_sz> <w_num> <chunks>\n", argv[0]);
		exit(4);
	}
	

	/* stage0: setup the global threads and variables */
	cfs_libinit();

	/* stage1: setup the volumes */
	uint32_t blksize = strtoul(argv[2], NULL, 0);
	uint64_t size = strtoul(argv[3], NULL, 0);
	uint64_t wavg = strtoul(argv[4], NULL, 0);
	uint64_t wnum = strtoul(argv[5], NULL, 0);
	uint64_t cnum = strtoul(argv[6], NULL, 0);

	fprintf(stderr, "\nvol:%s blksize:%u size:%lu wavg:%lu wnum:%lu chunks:%lu\n",
			argv[1], blksize, size, wavg, wnum, cnum);
	size *= blksize;	
	//fds[0] = cfs_open(argv[1], &size, blksize);

	fds[0] = cfs_open(argv[1], &size, blksize, wavg, wnum, cnum);
	if (fds[0] == -1) {
		fprintf(stderr, "vol:%s open failed\n", argv[1]);
		exit(2);
	}


	/* stage2: do the tests */
	do_test(fds[0]);


	fprintf(stderr, "\n\n\n opened:%s:fd:%d  running tests........\n", argv[1], fds[0]);
	sleep(1000);

	fprintf(stderr, "\n will start closing in 30s\n");
	sleep(30);


	/* stage3: close the volumes */
	fprintf(stderr, "\n closing %s:%d\n", argv[1], fds[0]);
	cfs_close(fds[0]);

	fprintf(stderr, "\n pid:%d all done \n", getpid());
	sleep(3);

	return 0; 
}

#endif
