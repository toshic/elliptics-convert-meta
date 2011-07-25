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

#ifndef __COMMON_H
#define __COMMON_H

#include <time.h>

#include <elliptics/packet.h>
#include <elliptics/interface.h>

#ifdef __cplusplus
extern "C" {
#endif

#define DNET_CONF_ADDR_DELIM ':'

struct dnet_meta_create_control {
	struct dnet_id			id;
	const char			*obj;
	int				len;

	int				*groups;
	int				group_num;

	uint64_t			update_flags;
	struct timespec			ts;

	uint8_t				checksum[DNET_CSUM_SIZE];
};

int dnet_create_write_meta(struct dnet_meta_create_control *ctl, void **data);
struct dnet_meta * dnet_meta_search_cust(struct dnet_meta_container *mc, uint32_t type);
void dnet_common_log(void *priv __attribute((unused)), uint32_t mask, const char *msg);
int dnet_parse_groups(char *value, int **groupsp);

#ifdef __cplusplus
}
#endif

#endif /* __COMMON_H */
