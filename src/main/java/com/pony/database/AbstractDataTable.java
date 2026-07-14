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
