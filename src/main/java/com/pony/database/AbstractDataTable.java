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
 * This is the abstract class implemented by a DataTable like table.  Both
 * DataTable and DataTableFilter objects extend this object.
 * <p>
 * @author Tobias Downer
 */

public abstract class AbstractDataTable extends Table implements RootTable {

    /**
     * Returns the fully resolved table name.
     */
    public TableName getTableName() {
        return getDataTableDef().getTableName();
    }

    // ---------- Implemented from Table ----------

    /**
     * This function is used to check that two tables are identical.
     * We first check the table names are identical.  Then check the column
     * filter is the same.
     */
    public boolean typeEquals(RootTable table) {
        if (table instanceof AbstractDataTable) {
            AbstractDataTable dest = (AbstractDataTable) table;
            return (getTableName().equals(dest.getTableName()));
        } else {
            return (this == table);
        }
    }


    /**
     * Returns a string that represents this table.
     */
    public String toString() {
        return getTableName().toString() + "[" + getRowCount() + "]";
    }

}
