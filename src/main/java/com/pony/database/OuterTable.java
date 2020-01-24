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

import com.pony.util.IntegerVector;

/**
 * A Table class for forming OUTER type results.  This takes as its constructor
 * the base table (with no outer NULL fields) that is what the result is based
 * on.  It is then possible to merge in tables that are ancestors
 *
 * @author Tobias Downer
 */

class OuterTable extends VirtualTable implements RootTable {

    /**
     * The merged rows.
     */
    public final IntegerVector[] outer_rows;

    /**
     * The row count of the outer rows.
     */
    private int outer_row_count;

    /**
     * Constructs the OuterTable given the base table.
     */
    public OuterTable(Table input_table) {
        super();

        RawTableInformation base_table =
                input_table.resolveToRawTable(new RawTableInformation());
        Table[] tables = base_table.getTables();
        IntegerVector[] rows = base_table.getRows();

        outer_rows = new IntegerVector[rows.length];

        // Set up the VirtualTable with this base table information,
        init(tables);
        set(tables, rows);

    }

    /**
     * Merges the given table in with this table.
     */
    public void mergeIn(Table outside_table) {
        RawTableInformation raw_table_info =
                outside_table.resolveToRawTable(new RawTableInformation());

        // Get the base information,
        Table[] base_tables = getReferenceTables();
        IntegerVector[] base_rows = getReferenceRows();

        // The tables and rows being merged in.
        Table[] tables = raw_table_info.getTables();
        IntegerVector[] rows = raw_table_info.getRows();
        // The number of rows being merged in.
        outer_row_count = rows[0].size();

        for (int i = 0; i < base_tables.length; ++i) {
            Table btable = base_tables[i];
            int index = -1;
            for (int n = 0; n < tables.length && index == -1; ++n) {
                if (btable == tables[n]) {
                    index = n;
                }
            }

            // If the table wasn't found, then set 'NULL' to this base_table
            if (index == -1) {
                outer_rows[i] = null;
            } else {
                IntegerVector list = new IntegerVector(outer_row_count);
                outer_rows[i] = list;
                // Merge in the rows from the input table,
                IntegerVector to_merge = rows[index];
                if (to_merge.size() != outer_row_count) {
                    throw new Error("Wrong size for rows being merged in.");
                }
                list.append(to_merge);
            }

        }

    }

    // ---------- Implemented from DefaultDataTable ----------

    /**
     * Returns the modified row count.
     */
    public int getRowCount() {
        return super.getRowCount() + outer_row_count;
    }

    /**
     * Returns a SelectableScheme for the given column in the given VirtualTable
     * row domain.  This searches down through the tables ancestors until it
     * comes across a table with a SelectableScheme where the given column is
     * fully resolved.  In most cases, this will be the root DataTable.
     * <p>
     * For an OuterTable, this must also include any rows with an index of -1
     * which indicates they are NULL.  NULL rows are put at the top of the
     * index list.
     */
    SelectableScheme getSelectableSchemeFor(int column, int original_column,
                                            Table table) {

        if (column_scheme[column] == null) {
            // EFFICIENCY: We implement this with a blind search...
            SelectableScheme scheme = new BlindSearch(this, column);
            column_scheme[column] = scheme.getSubsetScheme(this, column);
        }

        if (table == this) {
            return column_scheme[column];
        } else {
            return column_scheme[column].getSubsetScheme(table, original_column);
        }

    }

    /**
     * Returns an object that represents the information in the given cell
     * in the table.
     */
    public TObject getCellContents(int column, int row) {
        int table_num = column_table[column];
        Table parent_table = reference_list[table_num];
        if (row >= outer_row_count) {
            row = row_list[table_num].intAt(row - outer_row_count);
            return parent_table.getCellContents(column_filter[column], row);
        } else {
            if (outer_rows[table_num] == null) {
                // Special case, handling outer entries (NULL)
                return new TObject(getColumnDefAt(column).getTType(), null);
            } else {
                row = outer_rows[table_num].intAt(row);
                return parent_table.getCellContents(column_filter[column], row);
            }
        }
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

}
