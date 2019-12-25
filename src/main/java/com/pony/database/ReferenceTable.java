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

import com.pony.util.IntegerVector;

/**
 * This is an implementation of a Table that references a DataTable as its
 * parent.  This is a one-to-one relationship unlike the VirtualTable class
 * which is a one-to-many relationship.
 * <p>
 * The entire purpose of this class is as a filter.  We can use it to rename
 * a DataTable class to any domain we feel like.  This allows us to generate
 * unique column names.
 * <p>
 * For example, say we need to join the same table.  We can use this method
 * to ensure that the newly joined table won't have duplicate column names.
 * <p>
 * This object implements RootTable.
 *
 * @author Tobias Downer
 */

public final class ReferenceTable extends FilterTable implements RootTable {

    /**
     * This represents the new name of the table.
     */
    private final TableName table_name;

    /**
     * The modified DataTableDef object for this reference.
     */
    private final DataTableDef modified_table_def;


    /**
     * The Constructor.
     */
    ReferenceTable(Table table, TableName tname) {
        super(table);
        table_name = tname;

        // Create a modified table def based on the parent def.
        modified_table_def = new DataTableDef(table.getDataTableDef());
        modified_table_def.setTableName(tname);
        modified_table_def.setImmutable();
    }

    /**
     * Constructs the ReferenceTable given the parent table, and a new
     * DataTableDef that describes the columns in this table.  This is used if
     * we want to redefine the column names.
     * <p>
     * Note that the given DataTableDef must contain the same number of columns as
     * the parent table, and the columns must be the same type.
     */
    ReferenceTable(Table table, DataTableDef def) {
        super(table);
        table_name = def.getTableName();

        modified_table_def = def;
    }

    /**
     * Filters the name of the table.  This returns the declared name of the
     * table.
     */
    public TableName getTableName() {
        return table_name;
    }

    /**
     * Returns the 'modified' DataTableDef object for this reference.
     */
    public DataTableDef getDataTableDef() {
        return modified_table_def;
    }

    /**
     * Given a fully qualified variable field name, ie. 'APP.CUSTOMER.CUSTOMERID'
     * this will return the column number the field is at.  Returns -1 if the
     * field does not exist in the table.
     */
    public int findFieldName(Variable v) {
        TableName table_name = v.getTableName();
        if (table_name != null && table_name.equals(getTableName())) {
            return getDataTableDef().fastFindColumnName(v.getName());
        }
        return -1;
    }

    /**
     * Returns a fully qualified Variable object that represents the name of
     * the column at the given index.  For example,
     *   new Variable(new TableName("APP", "CUSTOMER"), "ID")
     */
    public Variable getResolvedVariable(int column) {
        return new Variable(getTableName(),
                getDataTableDef().columnAt(column).getName());
    }

    public boolean typeEquals(RootTable table) {
        return (this == table);
    }

}
