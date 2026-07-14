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
 * An implementation of InternalTableInfo that provides a number of methods to
 * aid in the productions of the InternalTableInfo interface.
 * <p>
 * This leaves the 'createInternalTable' method implementation to the derived
 * class.
 *
 * @author Tobias Downer
 */

abstract class AbstractInternalTableInfo implements InternalTableInfo {

    /**
     * The list of table names (as TableName) that this object maintains.
     */
    private final TableName[] table_list;

    /**
     * The list of DataTableDef objects that descibe each table in the above
     * list.
     */
    private final DataTableDef[] table_def_list;

    /**
     * The table type of table objects returned by this method.
     */
    private final String table_type;

    /**
     * Constructs the container than manages the creation of the given table
     * objects.
     */
    AbstractInternalTableInfo(String type, DataTableDef[] table_def_list) {
        this.table_def_list = table_def_list;
        this.table_type = type;
        table_list = new TableName[table_def_list.length];
        for (int i = 0; i < table_list.length; ++i) {
            table_list[i] = table_def_list[i].getTableName();
        }
    }

    /**
     * Returns the number of internal table sources that this object is
     * maintaining.
     */
    public int getTableCount() {
        return table_list.length;
    }

    /**
     * Finds the index in this container of the given table name, otherwise
     * returns -1.
     */
    public int findTableName(TableName name) {
        for (int i = 0; i < table_list.length; ++i) {
            if (table_list[i].equals(name)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns the name of the table at the given index in this container.
     */
    public TableName getTableName(int i) {
        return table_list[i];
    }

    /**
     * Returns the DataTableDef object that describes the table at the given
     * index in this container.
     */
    public DataTableDef getDataTableDef(int i) {
        return table_def_list[i];
    }

    /**
     * Returns true if this container contains a table with the given name.
     */
    public boolean containsTableName(TableName name) {
        return findTableName(name) != -1;
    }

    /**
     * Returns a String that describes the type of the table at the given index.
     */
    public String getTableType(int i) {
        return table_type;
    }

}

