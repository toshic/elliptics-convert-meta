/*
 * 2008+ Copyright (c) Evgeniy Polyakov <zbr@ioremap.net>
 * 2011+ Copyright (c) Anton Kortunov <toshic.toshic@gmail.com>
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 */

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/mman.h>

#include <ctype.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <kclangc.h>

#include <elliptics/packet.h>
#include <elliptics/interface.h>
#include <eblob/blob.h>

#include "common.h"

static void mparser_usage(const char *p)
{
	fprintf(stderr, "Usage: %s args\n", p);
	fprintf(stderr, " -M                   - meta database to parse\n"
			" -N                   - new meta database (blob)\n"
			" -g                   - default groups for objects without groups in meta\n"
			" -h                   - this help\n");
	exit(-1);
}

uint64_t counter = 0;
uint64_t total = 0;

int *groups = NULL;
int group_num = 0;

struct db_ptrs {
	struct eblob_backend *newmeta;
};

static const char *mparser_visit(const char *key, size_t keysz,
			const char *mdata, size_t datasz, size_t *sp __attribute((unused)), void *opq)
{
	struct db_ptrs *ptrs = opq;
	char id_str[2 * DNET_ID_SIZE + 1];
	struct dnet_raw_id id;
	struct dnet_meta_container mc;
	struct dnet_meta_create_control ctl;
	struct dnet_meta m, *mp = NULL;
	struct dnet_meta_update *mu;
	struct dnet_meta_checksum *csum;
	void *data = (void *)mdata;
	unsigned int size = datasz;
	char tstr[64];
	time_t t;
	struct tm *tm;
	int err = 0;

	if (keysz != DNET_ID_SIZE) {
		fprintf(stdout, "Incorrect key size\n");
		goto err_out_exit;
	}

	memcpy(id.id, key, DNET_ID_SIZE);
	dnet_dump_id_len_raw(id.id, DNET_ID_SIZE, id_str);
	
	fprintf(stdout, "Processing key %.128s  ", id_str);

	memset(&ctl, 0, sizeof(ctl));
	dnet_setup_id(&ctl.id, 0, id.id);

	err = dnet_db_read_raw(ptrs->newmeta, &id, &mc.data);
	if (err != -ENOENT) {
		if (err > 0) {
			fprintf(stdout, "failed. Record with this ID already exists. Skipping.\n");
			goto err_out_free;
		} else {
			fprintf(stdout, "failed. Unable to read new meta, err %d\n", err);
			goto err_out_exit;
		}
	}

	while (size) {
		if (size < sizeof(struct dnet_meta)) {
			fprintf(stdout, "failed. Metadata size %u is too small, min %zu.\n",
					size, sizeof(struct dnet_meta));
			err = -1;
			goto err_out_exit;
			break;
		}

		mp = data;
		m = *(struct dnet_meta *)data;
		dnet_convert_meta(&m);

		if (m.size + sizeof(struct dnet_meta) > size) {
			fprintf(stdout , "failed. Metadata entry broken: entry size %u, type: 0x%x, struct size: %zu, "
					"total size left: %u.\n",
					m.size, m.type, sizeof(struct dnet_meta), size);
			err = -1;
			goto err_out_exit;
			break;
		}

		switch (m.type) {
			case DNET_META_PARENT_OBJECT:
				ctl.obj = mp->data;
				ctl.len = m.size;
				break;

			case DNET_META_GROUPS:
				ctl.groups = (int *)mp->data;
				ctl.group_num = m.size / sizeof(int);
				break;

			case DNET_META_CHECKSUM:
				csum = (struct dnet_meta_checksum *)mp->data;
				memcpy(ctl.checksum, csum->checksum, DNET_CSUM_SIZE);
				break;

			case DNET_META_UPDATE:
				mu = (struct dnet_meta_update *)mp->data;
				dnet_convert_meta_update(mu);
				ctl.ts.tv_sec = mu->tm.tsec;
				ctl.ts.tv_nsec = mu->tm.tnsec;
		}

		data += m.size + sizeof(struct dnet_meta);
		size -= m.size + sizeof(struct dnet_meta);
	}

	err = dnet_create_write_meta(&ctl, &mc.data);
	if (err <= 0) {
		fprintf(stdout, "failed to create new meta, err %d.\n", err);
		goto err_out_exit;
	}

	mc.size = err;

	err = dnet_db_write_raw(ptrs->newmeta, &id, mc.data, mc.size);
	if (err) {
		fprintf(stdout, "failed to write new meta, err %d.\n", err);
		goto err_out_free;
	}

	fprintf(stdout, "ok. ");

err_out_free:
	free(mc.data);
err_out_exit:
	counter++;
	if (!(counter % 10000)) {
		t = time(NULL);
		tm = localtime(&t);
		strftime(tstr, sizeof(tstr), "%F %R:%S %Z", tm);
		fprintf(stderr, "%s: %llu/%llu records processed\n", tstr, counter, total);
	}

	return KCVISNOP;
}

int main(int argc, char *argv[])
{
	int err, ch;
	char *meta_name = NULL, *newmeta_name = NULL;
	unsigned long long offset, size;
	KCDB *meta = NULL;
	struct eblob_backend *newmeta = NULL;
	struct eblob_config ecfg;
	struct eblob_log log;
	char tstr[64];
	time_t t;
	struct tm *tm;
	struct db_ptrs ptrs;

	size = offset = 0;

	while ((ch = getopt(argc, argv, "M:N:g:h")) != -1) {
		switch (ch) {
			case 'M':
				meta_name = optarg;
				break;
			case 'N':
				newmeta_name = optarg;
				break;
			case 'g':
				group_num = dnet_parse_groups(optarg, &groups);
				break;
			case 'h':
				mparser_usage(argv[0]);
		}
	}

	if (!meta_name || !newmeta_name) {
		fprintf(stderr, "You have to provide meta database to convert.\n");
		mparser_usage(argv[0]);
	}

	if (group_num <= 0) {
		fprintf(stderr, "You have to provide default groups.\n");
		mparser_usage(argv[0]);
	}

	memset(&ptrs, 0, sizeof(struct db_ptrs));

	printf("opening %s meta database\n", meta_name);
	meta = kcdbnew();
	err = kcdbopen(meta, meta_name, KCOREADER | KCONOREPAIR);
	if (!err) {
		fprintf(stderr, "Failed to open meta database '%s': %d.\n", meta_name, -kcdbecode(meta));
		goto err_out_exit;
	}

	printf("opening %s new meta database\n", newmeta_name);

	memset(&ecfg, 0, sizeof(ecfg));
	ecfg.file = newmeta_name;
	ecfg.hash_size = (unsigned int)kcdbcount(meta);
	ecfg.sync = 30;

	log.log = dnet_common_log;
	log.log_private = NULL;
	log.log_mask = EBLOB_LOG_ERROR | EBLOB_LOG_INFO | EBLOB_LOG_NOTICE;
	ecfg.log = &log;

	newmeta = eblob_init(&ecfg);
	if (!newmeta) {
		fprintf(stderr, "Failed to open meta database '%s'.\n", newmeta_name);
		goto err_out_dbopen;
	}

	ptrs.newmeta = newmeta;

	t = time(NULL);
	tm = localtime(&t);
	strftime(tstr, sizeof(tstr), "%F %R:%S %Z", tm);
	total = (unsigned long long)kcdbcount(meta);
	fprintf(stderr, "%s: Total %llu records in old meta DB\n", tstr, total);

	err = kcdbiterate(meta, mparser_visit, &ptrs, 0);
	if (!err) {
		fprintf(stderr, "Failed to iterate meta database '%s': %d.\n", meta_name, -kcdbecode(meta));
	}

	t = time(NULL);
	tm = localtime(&t);
	strftime(tstr, sizeof(tstr), "%F %R:%S %Z", tm);
	total = (unsigned long long)kcdbcount(meta);
	fprintf(stderr, "%s: Totally processed %llu records from history DB\n", tstr, counter);

err_out_dbopen2:
	eblob_cleanup(newmeta);

err_out_dbopen:
	err = kcdbclose(meta);
	if (!err)
		fprintf(stderr, "Failed to close meta database '%s': %d.\n", meta_name, -kcdbecode(meta));
	kcdbdel(meta);

err_out_exit:
	return err;
}
