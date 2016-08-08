#ifndef _CFS_LOG_H_
#define _CFS_LOG_H_

#include <stdio.h>

#ifdef  __cplusplus
extern "C" {
#endif

extern __thread char  tinfo[20];

#define log_crit(fmt, ...)    log_msg(vd, 0, "%s:"  fmt, tinfo, ##__VA_ARGS__)
#define log_config(fmt, ...)  log_msg(vd, 1, "%s:"  fmt, tinfo, ##__VA_ARGS__)
#define log_notice(fmt, ...)  log_msg(vd, 2, "%s:"  fmt, tinfo, ##__VA_ARGS__)
#define log_info(fmt, ...)    log_msg(vd, 3, "%s:"  fmt, tinfo, ##__VA_ARGS__)

#define log_debug(fmt, ...)   log_msg(vd, 4, "%s:"  fmt, tinfo, ##__VA_ARGS__)
#define log_extra_debug(fmt, ...) log_msg(vd, 5, "%s:"  fmt, tinfo, ##__VA_ARGS__)
struct cfs_vol;
FILE *log_open(char *path);
void log_flush(struct cfs_vol *vd);
void log_msg(struct cfs_vol *vd, int lvl, const char *format, ...);

#ifdef  __cplusplus
}
#endif


#endif /* _CFS_LOG_H_ */
