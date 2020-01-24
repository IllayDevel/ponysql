/*
 * Pony SQL Database ( http://i-devel.ru )
 * Copyright (C) 2019-2020 IllayDevel.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
