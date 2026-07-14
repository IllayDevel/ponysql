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
