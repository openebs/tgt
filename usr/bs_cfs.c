/*
 * Synchronous I/O Chunk File backing store routine
 *
 * This is adapted from bs_rdwr.c
 *
 * Copyright (C) 2006-2007 FUJITA Tomonori <tomof@acm.org>
 * Copyright (C) 2006-2007 Mike Christie <michaelc@cs.wisc.edu>
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
#define _XOPEN_SOURCE 600

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include <linux/fs.h>
#include <sys/epoll.h>

#include "list.h"
#include "util.h"
#include "tgtd.h"
#include "scsi.h"
#include "spc.h"
#include "bs_thread.h"

static void set_medium_error(int *result, uint8_t *key, uint16_t *asc)
{
	*result = SAM_STAT_CHECK_CONDITION;
	*key = MEDIUM_ERROR;
	*asc = ASC_READ_ERROR;
}

static void bs_sync_sync_range(struct scsi_cmd *cmd, uint32_t length,
			       int *result, uint8_t *key, uint16_t *asc)
{
	/* todo
	int ret;
	ret = fdatasync(cmd->dev->fd);
	if (ret)
		set_medium_error(result, key, asc);
	*/
}

int cfs_init(void);
int cfs_exit(void);

int cfs_open(char *path, uint64_t *size, uint32_t blksize,
		uint64_t avgwsz, uint64_t writes, uint64_t chunks);
int cfs_close(int fd);

int cfs_read(int fd, char *buf, size_t size, off_t offset);
int cfs_write(int fd, const char *buf, size_t size, off_t offset);


static void bs_cfs_request(struct scsi_cmd *cmd)
{
	int ret, fd = cmd->dev->fd;
	uint32_t length;
	int result = SAM_STAT_GOOD;
	uint8_t key;
	uint16_t asc;
	//uint32_t info = 0;
	char *tmpbuf;
	size_t blocksize;
	uint64_t offset = cmd->offset;
	uint32_t tl     = cmd->tl;
	int do_verify = 0;
	int i;
	char *ptr;
	const char *write_buf = NULL;
	ret = length = 0;
	key = asc = 0;

	switch (cmd->scb[0])
	{
	case ORWRITE_16:
		length = scsi_get_out_length(cmd);

		tmpbuf = malloc(length);
		if (!tmpbuf) {
			result = SAM_STAT_CHECK_CONDITION;
			key = HARDWARE_ERROR;
			asc = ASC_INTERNAL_TGT_FAILURE;
			break;
		}

		ret = cfs_read(fd, tmpbuf, length, offset);

		if (ret != length) {
			set_medium_error(&result, &key, &asc);
			free(tmpbuf);
			break;
		}

		ptr = scsi_get_out_buffer(cmd);
		for (i = 0; i < length; i++)
			ptr[i] |= tmpbuf[i];

		free(tmpbuf);

		write_buf = scsi_get_out_buffer(cmd);
		goto write;
	case COMPARE_AND_WRITE:
		/* Blocks are transferred twice, first the set that
		 * we compare to the existing data, and second the set
		 * to write if the compare was successful.
		 */
		length = scsi_get_out_length(cmd) / 2;
		if (length != cmd->tl) {
			result = SAM_STAT_CHECK_CONDITION;
			key = ILLEGAL_REQUEST;
			asc = ASC_INVALID_FIELD_IN_CDB;
			break;
		}

		tmpbuf = malloc(length);
		if (!tmpbuf) {
			result = SAM_STAT_CHECK_CONDITION;
			key = HARDWARE_ERROR;
			asc = ASC_INTERNAL_TGT_FAILURE;
			break;
		}

		ret = cfs_read(fd, tmpbuf, length, offset);

		if (ret != length) {
			set_medium_error(&result, &key, &asc);
			free(tmpbuf);
			break;
		}

		if (memcmp(scsi_get_out_buffer(cmd), tmpbuf, length)) {
			uint32_t pos = 0;
			char *spos = scsi_get_out_buffer(cmd);
			char *dpos = tmpbuf;

			/*
			 * Data differed, this is assumed to be 'rare'
			 * so use a much more expensive byte-by-byte
			 * comparasion to find out at which offset the
			 * data differs.
			 */
			for (pos = 0; pos < length && *spos++ == *dpos++;
			     pos++)
				;
			//info = pos;
			result = SAM_STAT_CHECK_CONDITION;
			key = MISCOMPARE;
			asc = ASC_MISCOMPARE_DURING_VERIFY_OPERATION;
			free(tmpbuf);
			break;
		}

		/* todo:
		if (cmd->scb[1] & 0x10)
			posix_fadvise(fd, offset, length,
				      POSIX_FADV_NOREUSE);
		*/
		free(tmpbuf);

		write_buf = scsi_get_out_buffer(cmd) + length;
		goto write;
	case SYNCHRONIZE_CACHE:
	case SYNCHRONIZE_CACHE_16:
		/* TODO */
		length = (cmd->scb[0] == SYNCHRONIZE_CACHE) ? 0 : 0;

		if (cmd->scb[1] & 0x2) {
			result = SAM_STAT_CHECK_CONDITION;
			key = ILLEGAL_REQUEST;
			asc = ASC_INVALID_FIELD_IN_CDB;
		} else
			bs_sync_sync_range(cmd, length, &result, &key, &asc);
		break;
	case WRITE_VERIFY:
	case WRITE_VERIFY_12:
	case WRITE_VERIFY_16:
		do_verify = 1;
	case WRITE_6:
	case WRITE_10:
	case WRITE_12:
	case WRITE_16:
		length = scsi_get_out_length(cmd);
		write_buf = scsi_get_out_buffer(cmd);
write:
		ret = cfs_write(fd, write_buf, length,
			       offset);
		if (ret == length) {
			struct mode_pg *pg;

			/*
			 * it would be better not to access to pg
			 * directy.
			 */
			pg = find_mode_page(cmd->dev, 0x08, 0);
			if (pg == NULL) {
				result = SAM_STAT_CHECK_CONDITION;
				key = ILLEGAL_REQUEST;
				asc = ASC_INVALID_FIELD_IN_CDB;
				break;
			}
			if (((cmd->scb[0] != WRITE_6) && (cmd->scb[1] & 0x8)) ||
			    !(pg->mode_data[0] & 0x04))
				bs_sync_sync_range(cmd, length, &result, &key,
						   &asc);
		} else
			set_medium_error(&result, &key, &asc);

		/* todo:
		if ((cmd->scb[0] != WRITE_6) && (cmd->scb[1] & 0x10))
			posix_fadvise(fd, offset, length,
				      POSIX_FADV_NOREUSE);
					  */
		if (do_verify)
			goto verify;
		break;
	case WRITE_SAME:
	case WRITE_SAME_16:
		/* WRITE_SAME used to punch hole in file */
		if (cmd->scb[1] & 0x08) {
			/* todo:
			ret = unmap_file_region(fd, offset, tl);
			if (ret != 0) {
				eprintf("Failed to punch hole for WRITE_SAME"
					" command\n");
				result = SAM_STAT_CHECK_CONDITION;
				key = HARDWARE_ERROR;
				asc = ASC_INTERNAL_TGT_FAILURE;
				break;
			}
			*/
			break;
		}
		while (tl > 0) {
			blocksize = 1 << cmd->dev->blk_shift;
			tmpbuf = scsi_get_out_buffer(cmd);

			switch(cmd->scb[1] & 0x06) {
			case 0x02: /* PBDATA==0 LBDATA==1 */
				put_unaligned_be32(offset, tmpbuf);
				break;
			case 0x04: /* PBDATA==1 LBDATA==0 */
				/* physical sector format */
				put_unaligned_be64(offset, tmpbuf);
				break;
			}

			ret = cfs_write(fd, tmpbuf, blocksize, offset);
			if (ret != blocksize)
				set_medium_error(&result, &key, &asc);

			offset += blocksize;
			tl     -= blocksize;
		}
		break;
	case READ_6:
	case READ_10:
	case READ_12:
	case READ_16:
		length = scsi_get_in_length(cmd);
		ret = cfs_read(fd, scsi_get_in_buffer(cmd), length,
			      offset);

		if (ret != length)
			set_medium_error(&result, &key, &asc);

		/* todo:
		if ((cmd->scb[0] != READ_6) && (cmd->scb[1] & 0x10))
			posix_fadvise(fd, offset, length,
				      POSIX_FADV_NOREUSE);
		*/

		break;
	case PRE_FETCH_10:
	case PRE_FETCH_16:
		/* todo:
		ret = posix_fadvise(fd, offset, cmd->tl,
				POSIX_FADV_WILLNEED);

		if (ret != 0)
			set_medium_error(&result, &key, &asc);
		*/
		break;
	case VERIFY_10:
	case VERIFY_12:
	case VERIFY_16:
verify:
		length = scsi_get_out_length(cmd);

		tmpbuf = malloc(length);
		if (!tmpbuf) {
			result = SAM_STAT_CHECK_CONDITION;
			key = HARDWARE_ERROR;
			asc = ASC_INTERNAL_TGT_FAILURE;
			break;
		}

		ret = cfs_read(fd, tmpbuf, length, offset);

		if (ret != length)
			set_medium_error(&result, &key, &asc);
		else if (memcmp(scsi_get_out_buffer(cmd), tmpbuf, length)) {
			result = SAM_STAT_CHECK_CONDITION;
			key = MISCOMPARE;
			asc = ASC_MISCOMPARE_DURING_VERIFY_OPERATION;
		}

		/* todo:
		if (cmd->scb[1] & 0x10)
			posix_fadvise(fd, offset, length,
				      POSIX_FADV_NOREUSE);
		*/
		free(tmpbuf);
		break;
	case UNMAP:
		if (!cmd->dev->attrs.thinprovisioning) {
			result = SAM_STAT_CHECK_CONDITION;
			key = ILLEGAL_REQUEST;
			asc = ASC_INVALID_FIELD_IN_CDB;
			break;
		}

		length = scsi_get_out_length(cmd);
		tmpbuf = scsi_get_out_buffer(cmd);

		if (length < 8)
			break;

		length -= 8;
		tmpbuf += 8;

		while (length >= 16) {
			offset = get_unaligned_be64(&tmpbuf[0]);
			offset = offset << cmd->dev->blk_shift;

			tl = get_unaligned_be32(&tmpbuf[8]);
			tl = tl << cmd->dev->blk_shift;

			if (offset + tl > cmd->dev->size) {
				eprintf("UNMAP beyond EOF\n");
				result = SAM_STAT_CHECK_CONDITION;
				key = ILLEGAL_REQUEST;
				asc = ASC_LBA_OUT_OF_RANGE;
				break;
			}

			if (tl > 0) {
				/* todo:
				if (unmap_file_region(fd, offset, tl) != 0) {
					eprintf("Failed to punch hole for"
						" UNMAP at offset:%" PRIu64
						" length:%d\n",
						offset, tl);
					result = SAM_STAT_CHECK_CONDITION;
					key = HARDWARE_ERROR;
					asc = ASC_INTERNAL_TGT_FAILURE;
					break;
				}
				*/
			}

			length -= 16;
			tmpbuf += 16;
		}
		break;
	default:
		break;
	}

	dprintf("io done %p %x %d %u\n", cmd, cmd->scb[0], ret, length);

	scsi_set_result(cmd, result);

	if (result != SAM_STAT_GOOD) {
		eprintf("io error %p %x %d %d %" PRIu64 ", %m\n",
			cmd, cmd->scb[0], ret, length, offset);
		sense_data_build(cmd, key, asc);
	}
}

struct cfs_info {
	char *path;
	uint64_t size;
	uint64_t avgwsz;
	uint64_t writes;
	uint64_t chunks;
};


static int bs_cfs_open(struct scsi_lu *lu, char *path, int *fd, uint64_t *size)
{
	struct bs_thread_info *info = BS_THREAD_I(lu);
    struct cfs_info *ci = (struct cfs_info *)(info + 1);

	uint32_t blksize = lu->blk_shift > 0 ? (uint32_t)1<<lu->blk_shift : 4096;
	*size = ci->size;

	dprintf("bs_cfs_open:%s size:%lu/%lu blksize:%u\n", path, ci->size, *size, blksize);

	*fd = cfs_open(path, size, blksize, ci->avgwsz, ci->writes, ci->chunks);
	if (*fd < 0) {
		dprintf("bs_cfs_open:%s failed\n", path);
		return *fd;
	}

	if (!lu->attrs.no_auto_lbppbe)
		update_lbppbe(lu, blksize);

	return 0;
}

static void bs_cfs_close(struct scsi_lu *lu)
{
	cfs_close(lu->fd);
}

#include "parser.h"
enum {
	Opt_size, Opt_avgwsz, Opt_writes, Opt_chunks, Opt_config, Opt_err
};

static match_table_t cfs_tokens = {
	{Opt_size, "size=%s"},
	{Opt_avgwsz, "averagewritesize=%s"},
	{Opt_writes, "writes=%s"},
	{Opt_chunks, "chunks=%s"},
	{Opt_config, "config=%s"},
	{Opt_err, NULL}
};

static tgtadm_err bs_cfs_init(struct scsi_lu *lu, char *bsopts)
{
	struct bs_thread_info *info = BS_THREAD_I(lu);
    struct cfs_info *ci = (struct cfs_info *)(info + 1);

	ci->path = lu->path;
	ci->size = 6442450944; //6GB 
	ci->avgwsz = 131072; //128KB
	ci->writes = 500;
	ci->chunks = 800;

	dprintf("bs_cfs_init:%s bsopts: \"%s\"\n", lu->path, bsopts);

	// look for size= or chunks= or writes=
	char *config = NULL, *p;
	char *avgblksz = NULL, *size = NULL;
   	char *writes = NULL, *chunks = NULL;
	uint64_t avgblk, sz, ws, cnks;
	while ((p = strsep(&bsopts, ";")) != NULL) {
		substring_t args[MAX_OPT_ARGS];
		int token;
		if (!*p)
			continue;
		token = match_token(p, cfs_tokens, args);
		switch (token) {
		case Opt_size:
			size = match_strdup(&args[0]);
			break;
		case Opt_avgwsz:
			avgblksz = match_strdup(&args[0]);
			break;
		case Opt_writes:
			writes = match_strdup(&args[0]);
			break;
		case Opt_chunks:
			chunks = match_strdup(&args[0]);
			break;
		case Opt_config:
			config = match_strdup(&args[0]);
			break;
		default:
			break;
		}
	}

	ci->path=config;

	if (size) {
		sz = strtoul(size, NULL, 0);
		if (sz != ULONG_MAX)
			ci->size = sz;
		free(size);
	}
	if (avgblksz) {
		avgblk = strtoul(avgblksz, NULL, 0);
		if (avgblk != ULONG_MAX)
			ci->avgwsz = avgblk;
		free(avgblksz);
	}
	if (chunks) {
		cnks = strtoul(chunks, NULL, 0);
		if (cnks != ULONG_MAX)
			ci->chunks = cnks;
		free(chunks);
	}
	if (writes) {
		ws = strtoul(writes, NULL, 0);
		if (ws != ULONG_MAX)
			ci->writes = ws;
		free(writes);
	}
	dprintf("cfg:%s size:%lu  avgwrites:%lu  chunks:%lu writes:%lu\n",
		config ? config : "-", ci->size, ci->avgwsz, ci->chunks, ci->writes);
	
	return bs_thread_open(info, bs_cfs_request, nr_iothreads);
}

static void bs_cfs_exit(struct scsi_lu *lu)
{
	struct bs_thread_info *info = BS_THREAD_I(lu);
	bs_thread_close(info);
	cfs_exit();
}

static struct backingstore_template cfs_bst = {
	.bs_name		= "cfs",
	.bs_datasize		= sizeof(struct bs_thread_info) + sizeof(struct cfs_info),
	.bs_open		= bs_cfs_open,
	.bs_close		= bs_cfs_close,
	.bs_init		= bs_cfs_init,
	.bs_exit		= bs_cfs_exit,
	.bs_cmd_submit		= bs_thread_cmd_submit,
	.bs_oflags_supported    = O_SYNC | O_DIRECT,
};

int cfs_libinit(void);
__attribute__((constructor)) static void bs_cfs_constructor(void)
{
	unsigned char sbc_opcodes[] = {
		ALLOW_MEDIUM_REMOVAL,
		COMPARE_AND_WRITE,
		FORMAT_UNIT,
		INQUIRY,
		MAINT_PROTOCOL_IN,
		MODE_SELECT,
		MODE_SELECT_10,
		MODE_SENSE,
		MODE_SENSE_10,
		ORWRITE_16,
		PERSISTENT_RESERVE_IN,
		PERSISTENT_RESERVE_OUT,
		PRE_FETCH_10,
		PRE_FETCH_16,
		READ_10,
		READ_12,
		READ_16,
		READ_6,
		READ_CAPACITY,
		RELEASE,
		REPORT_LUNS,
		REQUEST_SENSE,
		RESERVE,
		SEND_DIAGNOSTIC,
		SERVICE_ACTION_IN,
		START_STOP,
		SYNCHRONIZE_CACHE,
		SYNCHRONIZE_CACHE_16,
		TEST_UNIT_READY,
		UNMAP,
		VERIFY_10,
		VERIFY_12,
		VERIFY_16,
		WRITE_10,
		WRITE_12,
		WRITE_16,
		WRITE_6,
		WRITE_SAME,
		WRITE_SAME_16,
		WRITE_VERIFY,
		WRITE_VERIFY_12,
		WRITE_VERIFY_16
	};
	
	sigset_t mask;
	sigemptyset(&mask);
	sigaddset(&mask, SIGUSR2);
	sigprocmask(SIG_BLOCK, &mask, NULL);


	cfs_libinit();
	bs_create_opcode_map(&cfs_bst, sbc_opcodes, ARRAY_SIZE(sbc_opcodes));
	register_backingstore_template(&cfs_bst);
}
