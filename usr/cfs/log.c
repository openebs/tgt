#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/types.h>

#include "log.h"

__thread char  tinfo[20] =  {0};

FILE *log_open(char *path)
{
    FILE *log;
    char logname[512];
    snprintf(logname, 511, "%s-%d.log", path, getpid());
	logname[511] = '\0';
    log= fopen(logname, "w");
	if (log != NULL) {
    	// set log to line buffering
    	setvbuf(log, NULL, _IOLBF, 0);
	}
    return log;
}

extern int curloglvl(struct cfs_vol *vd);
extern FILE *logfile(struct cfs_vol *vd);
struct cfs_vol;

void log_flush(struct cfs_vol *vd)
{
	if (vd && logfile(vd))
		fsync(fileno(logfile(vd)));
	else
		fsync(fileno(stderr));
}


void log_msg(struct cfs_vol *vd, int lvl, const char *format, ...)
{
	if ( (vd == NULL && lvl < 4) || (vd && (lvl <= curloglvl(vd))) ) {
    	va_list ap;
    	va_start(ap, format);
		if (vd && logfile(vd))
			vfprintf(logfile(vd), format, ap);
		else
			vfprintf(stderr, format, ap);
	}
}
