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
import com.pony.util.BlockIntegerList;

/**
 * A VirtualTable is a representation of a table whose rows are actually
 * physically stored in another table.  In other words, this table just
 * stores pointers to rows in other tables.
 * <p>
 * We use the VirtualTable to represent temporary tables created from select,
 * join, etc operations.
 * <p>
 * An important note about VirtualTables:  When we perform a 'select' operation
 * on a virtual table, unlike a DataTable that permanently stores information
 * about column cell relations, we must resolve column relations between the
 * sub-set at select time.  This involves asking the tables parent(s) for a
 * scheme to describe relations in a sub-set.
 *
 * @author Tobias Downer
 */

public class VirtualTable extends JoinedTable {

    /**
     * Array of IntegerVectors that represent the rows taken from the given
     * parents.
     */
    protected IntegerVector[] row_list;

    /**
     * The number of rows in the table.
     */
    private int row_count;

    /**
     * Helper function for the constructor.
     */
    protected void init(Table[] tables) {
        super.init(tables);

        int table_count = tables.length;
        row_list = new IntegerVector[table_count];
        for (int i = 0; i < table_count; ++i) {
            row_list[i] = new IntegerVector();
        }
    }

    /**
     * The Constructor.  It is constructed with a list of tables that this
     * virtual table is a sub-set or join of.
     */
    VirtualTable(Table[] tables) {
        super(tables);
    }

    VirtualTable(Table table) {
        super(table);
    }

    protected VirtualTable() {
        super();
    }

    /**
     * Returns the list of IntegerVector that represents the rows that this
     * VirtualTable references.
     */
    protected IntegerVector[] getReferenceRows() {
        return row_list;
    }

    /**
     * Returns the number of rows stored in the table.
     */
    public int getRowCount() {
        return row_count;
    }

    /**
     * Sets the rows in this table.  We should search for the
     * 'table' in the 'reference_list' however we don't for efficiency.
     */
    void set(Table table, IntegerVector rows) {
        row_list[0] = new IntegerVector(rows);
        row_count = rows.size();
    }

    /**
     * This is used in a join to set a list or joined rows and tables.  The
     * 'tables' array should be an exact mirror of the 'reference_list'.  The
     * IntegerVector[] array contains the rows to add for each respective table.
     * The given IntegerVector objects should have identical lengths.
     */
    void set(Table[] tables, IntegerVector[] rows) {
        for (int i = 0; i < tables.length; ++i) {
            row_list[i] = new IntegerVector(rows[i]);
        }
        if (rows.length > 0) {
            row_count = rows[0].size();
        }
    }

    /**
     * Sets the rows in this table as above, but uses a BlockIntegerList as an
     * argument instead.
     */
    void set(Table table, BlockIntegerList rows) {
        row_list[0] = new IntegerVector(rows);
        row_count = rows.size();
    }

    /**
     * Sets the rows in this table as above, but uses a BlockIntegerList array
     * as an argument instead.
     */
    void set(Table[] tables, BlockIntegerList[] rows) {
        for (int i = 0; i < tables.length; ++i) {
            row_list[i] = new IntegerVector(rows[i]);
        }
        if (rows.length > 0) {
            row_count = rows[0].size();
        }
    }

    // ---------- Implemented from JoinedTable ----------

    protected int resolveRowForTableAt(int row_number, int table_num) {
        return row_list[table_num].intAt(row_number);
    }

    protected void resolveAllRowsForTableAt(
            IntegerVector row_set, int table_num) {
        IntegerVector cur_row_list = row_list[table_num];
        for (int n = row_set.size() - 1; n >= 0; --n) {
            int aa = row_set.intAt(n);
            int bb = cur_row_list.intAt(aa);
            row_set.setIntAt(bb, n);
        }
    }


}
