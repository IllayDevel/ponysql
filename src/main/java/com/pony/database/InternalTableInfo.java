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

import java.util.ArrayList;

/**
 * A class that acts as a container for any system tables that are generated
 * from information inside the database engine.  For example, the database
 * statistics table is an internal system table, as well as the table that
 * describes all database table information, etc.
 * <p>
 * This object acts as a container and factory for generating such tables.
 * <p>
 * Note that implementations of this object should be thread-safe and
 * immutable so we can create static global implementations.
 *
 * @author Tobias Downer
 */

interface InternalTableInfo {

    /**
     * Returns the number of internal table sources that this object is
     * maintaining.
     */
    int getTableCount();

    /**
     * Finds the index in this container of the given table name, otherwise
     * returns -1.
     */
    int findTableName(TableName name);

    /**
     * Returns the name of the table at the given index in this container.
     */
    TableName getTableName(int i);

    /**
     * Returns the DataTableDef object that describes the table at the given
     * index in this container.
     */
    DataTableDef getDataTableDef(int i);

    /**
     * Returns true if this container contains a table with the given name.
     */
    boolean containsTableName(TableName name);

    /**
     * Returns a String that describes the type of the table at the given index.
     */
    String getTableType(int i);

    /**
     * This is the factory method for generating the internal table for the
     * given table in this container.  This should return an implementation of
     * MutableTableDataSource that is used to represent the internal data being
     * modelled.
     * <p>
     * This method is allowed to throw an exception for table objects that aren't
     * backed by a MutableTableDataSource, such as a view.
     */
    MutableTableDataSource createInternalTable(int index);

}
