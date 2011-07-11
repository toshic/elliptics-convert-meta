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

#include <errno.h>

#include <elliptics/packet.h>
#include <elliptics/interface.h>

#include "common.h"


int dnet_create_write_meta(struct dnet_metadata_control *ctl, void **data)
{
	struct dnet_meta_container mc;
	struct dnet_meta_check_status *c;
	struct dnet_meta_checksum *csum;
	struct dnet_meta *m;
	int size = 0, err, nsize = 0;

	size += sizeof(struct dnet_meta_check_status) + sizeof(struct dnet_meta);

	if (ctl->obj && ctl->len)
		size += ctl->len + sizeof(struct dnet_meta);

	if (ctl->groups && ctl->group_num)
		size += ctl->group_num * sizeof(int) + sizeof(struct dnet_meta);

	size += sizeof(struct dnet_meta_checksum) + sizeof(struct dnet_meta);

	size += sizeof(struct dnet_meta_update) + sizeof(struct dnet_meta);

	if (!size) {
		err = -EINVAL;
		goto err_out_exit;
	}

	memset(&mc, 0, sizeof(struct dnet_meta_container));
	mc.data = malloc(size);
	if (!mc.data) {
		err = -ENOMEM;
		goto err_out_exit;
	}
	memset(mc.data, 0, size);

	m = (struct dnet_meta *)(mc.data);

	c = (struct dnet_meta_check_status *)m->data;
	m->size = sizeof(struct dnet_meta_check_status);
	m->type = DNET_META_CHECK_STATUS;

	/* Check status is undefined for now, it will be filled during actual check */
	memset(c, 0, sizeof(struct dnet_meta_check_status));
	dnet_convert_meta(m);

	m = (struct dnet_meta *)(m->data + m->size);
	dnet_create_meta_update(m, ctl->ts.tv_sec ? &ctl->ts : NULL, 0, 0);

	m = (struct dnet_meta *)(m->data + m->size);

	if (ctl->obj && ctl->len) {
		m->size = ctl->len;
		m->type = DNET_META_PARENT_OBJECT;
		memcpy(m->data, ctl->obj, ctl->len);
		dnet_convert_meta(m);

		m = (struct dnet_meta *)(m->data + m->size);
	}

	if (ctl->groups && ctl->group_num) {
		m->size = ctl->group_num * sizeof(int);
		m->type = DNET_META_GROUPS;
		memcpy(m->data, ctl->groups, ctl->group_num * sizeof(int));
		dnet_convert_meta(m);

		m = (struct dnet_meta *)(m->data + m->size);
	}

	csum = (struct dnet_meta_checksum *)m->data;
	csum->tm.tsec = ctl->ts.tv_sec;
	csum->tm.tnsec = ctl->ts.tv_nsec;
	dnet_convert_meta_checksum(csum);
	m->size = sizeof(struct dnet_meta_checksum);
	m->type = DNET_META_CHECKSUM;
	m = (struct dnet_meta *)(m->data + m->size);
	dnet_convert_meta(m);

	mc.size = size;
	memcpy(&mc.id, &ctl->id, sizeof(struct dnet_id));

	*data = mc.data;
printf("meta size = %d\n", mc.size);
	return mc.size;

err_out_exit:
	return err;
}

struct dnet_meta * dnet_meta_search_cust(struct dnet_meta_container *mc, uint32_t type)
{
	void *data = mc->data;
	uint32_t size = mc->size;
	struct dnet_meta m, *found = NULL;

	while (size) {
		if (size < sizeof(struct dnet_meta)) {
			fprintf(stderr, "Metadata size %u is too small, min %zu, searching for 0x%x.\n",
					size, sizeof(struct dnet_meta), type);
			break;
		}

		m = *(struct dnet_meta *)data;
		dnet_convert_meta(&m);

		if (m.size + sizeof(struct dnet_meta) > size) {
			fprintf(stderr, "Metadata entry broken: entry size %u, type: 0x%x, struct size: %zu, "
					"total size left: %u, searching for 0x%x.\n",
					m.size, m.type, sizeof(struct dnet_meta), size, type);
			break;
		}

		if (m.type == type) {
			found = data;
			break;
		}

		data += m.size + sizeof(struct dnet_meta);
		size -= m.size + sizeof(struct dnet_meta);
	}

	return found;
}

void dnet_common_log(void *priv __attribute((unused)), uint32_t mask, const char *msg)
{
        char str[64];
        struct tm tm;
        struct timeval tv;

        gettimeofday(&tv, NULL);
        localtime_r((time_t *)&tv.tv_sec, &tm);
        strftime(str, sizeof(str), "%F %R:%S", &tm);

        fprintf(stderr, "%s.%06lu %1x: %s", str, tv.tv_usec, mask, msg);
	fflush(stderr);
}

int dnet_parse_groups(char *value, int **groupsp)
{
	int len = strlen(value), i, num = 0, start = 0, pos = 0;
	char *ptr = value;
	int *groups;

	if (sscanf(value, "auto%d", &num) == 1) {
		*groupsp = NULL;
		return num;
	}

	for (i=0; i<len; ++i) {
		if (value[i] == DNET_CONF_ADDR_DELIM)
			start = 0;
		else if (!start) {
			start = 1;
			num++;
		}
	}

	if (!num) {
		fprintf(stderr, "no groups found\n");
		return -ENOENT;
	}

	groups = malloc(sizeof(int) * num);
	if (!groups)
		return -ENOMEM;

	memset(groups, 0, num * sizeof(int));

	start = 0;
	for (i=0; i<len; ++i) {
		if (value[i] == DNET_CONF_ADDR_DELIM) {
			value[i] = '\0';
			if (start) {
				groups[pos] = atoi(ptr);
				pos++;
				start = 0;
			}
		} else if (!start) {
			ptr = &value[i];
			start = 1;
		}
	}

	if (start) {
		groups[pos] = atoi(ptr);
		pos++;
	}

	*groupsp = groups;
	return pos;
}


