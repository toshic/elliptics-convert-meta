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

#include "common.h"

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

static void hparser_usage(const char *p)
{
	fprintf(stderr, "Usage: %s args\n", p);
	fprintf(stderr, " -H                   - history database to parse\n"
			" -M                   - meta database (blob) to parse\n"
			" -g                   - default groups for objects without meta\n"
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

static const char *hparser_visit(const char *key, size_t keysz,
			const char *hdata, size_t datasz, size_t *sp __attribute((unused)), void *opq)
{
	struct db_ptrs *ptrs = opq;
	char id_str[2 * DNET_ID_SIZE + 1];
	struct dnet_history_map hm;
	struct dnet_meta_container mc;
	struct dnet_meta *mp, *m = NULL;
	struct dnet_meta_update *mu;
	int err;
	struct dnet_raw_id id;
	char tstr[64];
	time_t t;
	struct tm *tm;

	if (keysz != DNET_ID_SIZE) {
		fprintf(stdout, "Incorrect key size\n");
		goto err_out_exit;
	}

	memcpy(id.id, key, DNET_ID_SIZE);
	dnet_dump_id_len_raw(id.id, DNET_ID_SIZE, id_str);
	
	fprintf(stdout, "Processing key %.128s  ", id_str);

	if (datasz % (int)sizeof(struct dnet_history_entry)) {
		fprintf(stdout, "Corrupted history record, "
				"its size %d must be multiple of %zu.\n",
				datasz, sizeof(struct dnet_history_entry));
		goto err_out_exit;
	}

	hm.ent = (struct dnet_history_entry *)hdata;
	hm.num = datasz / sizeof(struct dnet_history_entry);
	hm.size = datasz;

	dnet_setup_id(&mc.id, 0, id.id);
	err = dnet_db_read_raw(ptrs->newmeta, &id, &mc.data);
	if (err == -ENOENT) {
		struct dnet_metadata_control ctl;

		fprintf(stdout, "not found. Re-creating metadata\n");

		memset(&ctl, 0, sizeof(ctl));

		ctl.obj = NULL;
		ctl.len = 0;

		ctl.groups = groups;
		ctl.group_num = group_num;

		dnet_setup_id(&ctl.id, 0, id.id);

		err = dnet_create_write_meta(&ctl, &mc.data);
		if (err <= 0) {
			fprintf(stdout, "Metadata re-creating failed!\n");
			goto err_out_exit;
		}

	} else if (err <= 0) {
		fprintf(stdout, "failed. %s: meta DB read failed, err: %d.\n",
			dnet_dump_id_str(id.id), err);
		goto err_out_exit;
	}
	mc.size = err;

	mp = dnet_meta_search_cust(&mc, DNET_META_UPDATE);
	if (!mp) {
		// Add new meta structure after the end of current metadata
		mc.data = realloc(mc.data, mc.size + sizeof(struct dnet_meta) + sizeof(struct dnet_meta_update));
		if (!mc.data) {
			fprintf(stdout, "failed. Can't realloc.\n");
			err = -ENOMEM;
			goto err_out_free;
		}

		mp = m = mc.data + mc.size;
		mc.size += sizeof(struct dnet_meta) + sizeof(struct dnet_meta_update);

		memset(m, 0, sizeof(struct dnet_meta) + sizeof(struct dnet_meta_update));
		m->type = DNET_META_UPDATE;
		m->size = sizeof(struct dnet_meta_update);
		memset(m->data, 0, m->size);
	} else {
		dnet_convert_meta(mp);
	}

	if (mp->size % sizeof(struct dnet_meta_update)) {
		fprintf(stdout, "failed. Metadata is broken: entry size %u\n", mp->size);
		goto err_out_free;
	}

	mu = (struct dnet_meta_update *)mp->data;
	dnet_convert_meta(mp);

	dnet_convert_history_entry(&hm.ent[hm.num-1]);

	mu->tm.tsec = hm.ent[hm.num-1].tsec;
	mu->tm.tnsec = hm.ent[hm.num-1].tnsec;
	mu->flags = hm.ent[hm.num-1].flags & DNET_IO_FLAGS_REMOVED;

	dnet_convert_meta_update(mu);

	err = dnet_db_write_raw(ptrs->newmeta, &id, mc.data, mc.size);
	if (err) {
		fprintf(stdout, "failed to write new meta, err %d.\n", err);
		goto err_out_free;
	}

	fprintf(stdout, "ok. Last update stamp %llu %llu\n", hm.ent[hm.num-1].tsec, hm.ent[hm.num-1].tnsec);

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
	char *history_name = NULL, *newmeta_name = NULL;
	unsigned long long offset, size;
	KCDB *history = NULL;
	struct eblob_backend *newmeta = NULL;
	struct eblob_config ecfg;
	struct eblob_log log;
	char tstr[64];
	time_t t;
	struct tm *tm;
	struct db_ptrs ptrs;

	size = offset = 0;

	while ((ch = getopt(argc, argv, "M:H:g:h")) != -1) {
		switch (ch) {
			case 'M':
				newmeta_name = optarg;
				break;
			case 'H':
				history_name = optarg;
				break;
			case 'g':
				group_num = dnet_parse_groups(optarg, &groups);
				break;
			case 'h':
				hparser_usage(argv[0]);
				break;
		}
	}

	if (!history_name || !newmeta_name) {
		fprintf(stderr, "You have to provide history and meta database to convert.\n");
		hparser_usage(argv[0]);
	}

	if (group_num <= 0) {
		fprintf(stderr, "You have to provide default groups.\n");
		hparser_usage(argv[0]);
	}

	memset(&ptrs, 0, sizeof(struct db_ptrs));

	printf("opening %s history database\n", history_name);
	history = kcdbnew();
	err = kcdbopen(history, history_name, KCOREADER | KCONOREPAIR);
	if (!err) {
		fprintf(stderr, "Failed to open history database '%s': %d.\n", history_name, -kcdbecode(history));
		goto err_out_exit;
	}

	memset(&ecfg, 0, sizeof(ecfg));
	ecfg.file = newmeta_name;
	ecfg.hash_size = (unsigned int)kcdbcount(history);

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
	total = (unsigned long long)kcdbcount(history);
	fprintf(stderr, "%s: Total %llu records in history DB\n", tstr, total);

	err = kcdbiterate(history, hparser_visit, &ptrs, 0);
	if (!err) {
		fprintf(stderr, "Failed to iterate history database '%s': %d.\n", history_name, -kcdbecode(history));
	}

	t = time(NULL);
	tm = localtime(&t);
	strftime(tstr, sizeof(tstr), "%F %R:%S %Z", tm);
	total = (unsigned long long)kcdbcount(history);
	fprintf(stderr, "%s: Totally processed %llu records from history DB\n", tstr, counter);

err_out_dbopen2:
	eblob_cleanup(newmeta);

err_out_dbopen:
	err = kcdbclose(history);
	if (!err)
		fprintf(stderr, "Failed to close history database '%s': %d.\n", history_name, -kcdbecode(history));
	kcdbdel(history);

err_out_exit:
	return err;
}
