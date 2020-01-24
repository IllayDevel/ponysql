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

