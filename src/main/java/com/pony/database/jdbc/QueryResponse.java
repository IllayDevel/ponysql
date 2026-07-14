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

package com.pony.database.jdbc;

import com.pony.database.global.ColumnDescription;

/**
 * The response to a query executed via the 'execQuery' method in the
 * DatabaseInterface interface.  This contains general information about the
 * result of the query.
 *
 * @author Tobias Downer
 */

public interface QueryResponse {

    /**
     * Returns a number that identifies this query within the set of queries
     * executed on the connection.  This is used for identifying this query
     * in subsequent operations.
     */
    int getResultID();

    /**
     * The time, in milliseconds, that the query took to execute.
     */
    int getQueryTimeMillis();

    /**
     * The total number of rows in the query result.  This is known ahead of
     * time, even if no data in the query has been accessed.
     */
    int getRowCount();

    /**
     * The number of columns in the query result.
     */
    int getColumnCount();

    /**
     * The ColumnDescription object that describes column 'n' in the result.  0
     * is the first column, 1 is the second column, etc.
     */
    ColumnDescription getColumnDescription(int column);

    /**
     * Returns any warnings about the query.  If there were no warnings then
     * this can return 'null'.
     */
    String getWarnings();

}
