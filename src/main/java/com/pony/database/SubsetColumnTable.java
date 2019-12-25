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
 * This object is a filter that sits atop a Table object.  Its purpose is to
 * only provide a view of the columns that are required.  In a Select
 * query we may create a query with only the subset of columns that were
 * originally in the table set.  This object allows us to provide an
 * interface to only the columns that the Table is allowed to access.
 * <p>
 * This method implements RootTable which means a union operation will not
 * decend further past this table when searching for the roots.
 *
 * @author Tobias Downer
 */

public final class SubsetColumnTable extends FilterTable
        implements RootTable {

    /**
     * Maps from the column in this table to the column in the parent table.
     * The number of entries of this should match the number of columns in this
     * table.
     */
    private int[] column_map;

    /**
     * Maps from the column in the parent table, to the column in this table.
     * The size of this should match the number of columns in the parent
     * table.
     */
    private int[] reverse_column_map;

    /**
     * The DataTableDef object that describes the subset column of this
     * table.
     */
    private DataTableDef subset_table_def;

    /**
     * The resolved Variable aliases for this subset.  These are returned by
     * getResolvedVariable and used in searches for findResolvedVariable.  This
     * can be used to remap the variable names used to match the columns.
     */
    private Variable[] aliases;


    /**
     * The Constructor.
     */
    public SubsetColumnTable(Table parent) {
        super(parent);
    }

    /**
     * Adds a column map into this table.  The int array contains a map to the
     * column in the parent object that we want the column number to reference.
     * For example, to select columns 4, 8, 1, 2 into this new table, the
     * array would be { 4, 8, 1, 2 }.
     */
    public void setColumnMap(int[] mapping, Variable[] aliases) {
        reverse_column_map = new int[parent.getColumnCount()];
        for (int i = 0; i < reverse_column_map.length; ++i) {
            reverse_column_map[i] = -1;
        }
        column_map = mapping;

        this.aliases = aliases;

        subset_table_def = new DataTableDef();
        DataTableDef parent_def = parent.getDataTableDef();
        subset_table_def.setTableName(parent_def.getTableName());

        for (int i = 0; i < mapping.length; ++i) {
            int map_to = mapping[i];
            DataTableColumnDef col_def =
                    new DataTableColumnDef(parent.getColumnDefAt(map_to));
            col_def.setName(aliases[i].getName());
            subset_table_def.addVirtualColumn(col_def);
            reverse_column_map[map_to] = i;
        }

        subset_table_def.setImmutable();
    }


    /**
     * Returns the number of columns in the table.
     */
    public int getColumnCount() {
        return aliases.length;
    }

    /**
     * Given a fully qualified variable field name, ie. 'APP.CUSTOMER.CUSTOMERID'
     * this will return the column number the field is at.  Returns -1 if the
     * field does not exist in the table.
     */
    public int findFieldName(Variable v) {
        for (int i = 0; i < aliases.length; ++i) {
            if (v.equals(aliases[i])) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Returns the DataTableDef object that describes the columns and name
     * of this table.  For a SubsetColumnTable object, this returns the
     * columns that were mapped via the 'setColumnMap' method.
     */
    public DataTableDef getDataTableDef() {
        return subset_table_def;
    }

    /**
     * Returns a fully qualified Variable object that represents the name of
     * the column at the given index.  For example,
     *   new Variable(new TableName("APP", "CUSTOMER"), "ID")
     */
    public Variable getResolvedVariable(int column) {
        return aliases[column];
    }

    /**
     * Returns a SelectableScheme for the given column in the given VirtualTable
     * row domain.
     */
    final SelectableScheme getSelectableSchemeFor(int column,
                                                  int original_column, Table table) {

        // We need to map the original_column if the original column is a reference
        // in this subset column table.  Otherwise we leave as is.
        // The reason is because FilterTable pretends the call came from its
        // parent if a request is made on this table.
        int mapped_original_column = original_column;
        if (table == this) {
            mapped_original_column = column_map[original_column];
        }

        return super.getSelectableSchemeFor(column_map[column],
                mapped_original_column, table);
    }

    /**
     * Given a set, this trickles down through the Table hierarchy resolving
     * the given row_set to a form that the given ancestor understands.
     * Say you give the set { 0, 1, 2, 3, 4, 5, 6 }, this function may check
     * down three levels and return a new 7 element set with the rows fully
     * resolved to the given ancestors domain.
     */
    final void setToRowTableDomain(int column, IntegerVector row_set,
                                   TableDataSource ancestor) {

        super.setToRowTableDomain(column_map[column], row_set, ancestor);
    }

    /**
     * Return the list of DataTable and row sets that make up the raw information
     * in this table.
     */
    final RawTableInformation resolveToRawTable(RawTableInformation info) {
        throw new Error("Tricky to implement this method!");
        // ( for a SubsetColumnTable that is )
    }

    /**
     * Returns an object that represents the information in the given cell
     * in the table.  This will generally be an expensive algorithm, so calls
     * to it should be kept to a minimum.  Note that the offset between two
     * rows is not necessarily 1.
     */
    public final TObject getCellContents(int column, int row) {
        return parent.getCellContents(column_map[column], row);
    }

    // ---------- Implemented from RootTable ----------

    /**
     * This function is used to check that two tables are identical.  This
     * is used in operations like 'union' that need to determine that the
     * roots are infact of the same type.
     */
    public boolean typeEquals(RootTable table) {
        return (this == table);
    }


    /**
     * Returns a string that represents this table.
     */
    public String toString() {
        String name = "SCT" + hashCode();
        return name + "[" + getRowCount() + "]";
    }

}
