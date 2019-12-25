/*
 * Pony SQL Database ( http://www.ponysql.ru/ )
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
