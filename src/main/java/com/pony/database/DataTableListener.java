/*
 * Pony SQL Database ( http://i-devel.ru )
 * Copyright (C) 2019-2020 IllayDevel.
 * SPDX-License-Identifier: GPL-2.0-only
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

package com.pony.database;

/**
 * A DataTableListener is notified of all modifications to the raw entries
 * of the data table.  This listener can be used for detecting changes in
 * VIEWs, for triggers or for caching of common queries.
 *
 * @author Tobias Downer
 */

interface DataTableListener {

    /**
     * Called before a row entry in the table is deleted.
     */
    void rowDeleted(DataTable table, int row_index);

    /**
     * Called after a row entry in the table is added.
     */
    void rowAdded(DataTable table, int row_index);


}
