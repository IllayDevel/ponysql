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
 * A Table that represents the result of one or more other tables joined
 * together.  VirtualTable and NaturallyJoinedTable are derived from this
 * class.
 *
 * @author Tobias Downer
 */

public abstract class JoinedTable extends Table {

    /**
     * The list of tables that make up the join.
     */
    protected Table[] reference_list;

    /**
     * The schemes to describe the entity relation in the given column.
     */
    protected SelectableScheme[] column_scheme;

    /**
     * Maps the column number in this table to the reference_list array to route
     * to.
     */
    protected int[] column_table;

    /**
     * Gives a column filter to the given column to route correctly to the
     * ancestor.
     */
    protected int[] column_filter;

    /**
     * The column that we are sorted against.  This is an optimization set by
     * the 'optimisedPostSet' method.
     */
    private int sorted_against_column = -1;

    /**
     * The DataTableDef object that describes the columns and name of this
     * table.
     */
    private DataTableDef vt_table_def;

    /**
     * Incremented when the roots are locked.
     * See the 'lockRoot' and 'unlockRoot' methods.
     * NOTE: This should only ever be 1 or 0.
     */
    private byte roots_locked;

    /**
     * Constructs the JoinedTable with the list of tables in the parent.
     */
    JoinedTable(Table[] tables) {
        super();
        init(tables);
    }

    /**
     * Constructs the JoinedTable with a single table.
     */
    JoinedTable(Table table) {
        super();
        Table[] tables = new Table[1];
        tables[0] = table;
        init(tables);
    }

    /**
     * Protected constructor.
     */
    protected JoinedTable() {
        super();
    }

    /**
     * Helper function for initializing the variables in the joined table.
     */
    protected void init(Table[] tables) {
        int table_count = tables.length;
        reference_list = tables;

        final int col_count = getColumnCount();
        column_scheme = new SelectableScheme[col_count];

        vt_table_def = new DataTableDef();

        // Generate look up tables for column_table and column_filter information

        column_table = new int[col_count];
        column_filter = new int[col_count];
        int index = 0;
        for (int i = 0; i < reference_list.length; ++i) {

            Table cur_table = reference_list[i];
            DataTableDef cur_table_def = cur_table.getDataTableDef();
            int ref_col_count = cur_table.getColumnCount();

            // For each column
            for (int n = 0; n < ref_col_count; ++n) {
                column_filter[index] = n;
                column_table[index] = i;
                ++index;

                // Add this column to the data table def of this table.
                vt_table_def.addVirtualColumn(
                        new DataTableColumnDef(cur_table_def.columnAt(n)));
            }

        }

        // Final setup the DataTableDef for this virtual table

        vt_table_def.setTableName(new TableName(null, "#VIRTUAL TABLE#"));

        vt_table_def.setImmutable();

    }

    /**
     * Returns a row reference list.  This is an IntegerVector that represents a
     * 'reference' to the rows in our virtual table.
     * <p>
     * ISSUE: We should be able to optimise these types of things out.
     */
    private IntegerVector calculateRowReferenceList() {
        int size = getRowCount();
        IntegerVector all_list = new IntegerVector(size);
        for (int i = 0; i < size; ++i) {
            all_list.addInt(i);
        }
        return all_list;
    }

    /**
     * We simply pick the first table to resolve the Database object.
     */
    public Database getDatabase() {
        return reference_list[0].getDatabase();
    }

    /**
     * Returns the number of columns in the table.  This simply returns the
     * column counts in the parent table(s).
     */
    public int getColumnCount() {
        int column_count_sum = 0;
        for (Table table : reference_list) {
            column_count_sum += table.getColumnCount();
        }
        return column_count_sum;
    }

    /**
     * Given a fully qualified variable field name, ie. 'APP.CUSTOMER.CUSTOMERID'
     * this will return the column number the field is at.  Returns -1 if the
     * field does not exist in the table.
     */
    public int findFieldName(Variable v) {
        int col_index = 0;
        for (Table table : reference_list) {
            int col = table.findFieldName(v);
            if (col != -1) {
                return col + col_index;
            }
            col_index += table.getColumnCount();
        }
        return -1;
    }

    /**
     * Returns a fully qualified Variable object that represents the name of
     * the column at the given index.  For example,
     *   new Variable(new TableName("APP", "CUSTOMER"), "ID")
     */
    public final Variable getResolvedVariable(int column) {
        Table parent_table = reference_list[column_table[column]];
        return parent_table.getResolvedVariable(column_filter[column]);
    }

    /**
     * Returns the list of Table objects that represent this VirtualTable.
     */
    protected final Table[] getReferenceTables() {
        return reference_list;
    }

    /**
     * This is an optimisation that should only be called _after_ a 'set' method
     * has been called.  Because the 'select' operation returns a set that is
     * ordered by the given column, we can very easily generate a
     * SelectableScheme object that can handle this column.
     * So 'column' is the column in which this virtual table is naturally ordered
     * by.
     * NOTE: The internals of this method may be totally commented out and the
     *   database will still operate correctly.  However this greatly speeds up
     *   situations when you perform multiple consequtive operations on the same
     *   column.
     */
    void optimisedPostSet(int column) {
        sorted_against_column = column;
    }

    /**
     * Returns a SelectableScheme for the given column in the given VirtualTable
     * row domain.  This searches down through the tables ancestors until it
     * comes across a table with a SelectableScheme where the given column is
     * fully resolved.  In most cases, this will be the root DataTable.
     */
    SelectableScheme getSelectableSchemeFor(int column, int original_column,
                                            Table table) {

        // First check if the given SelectableScheme is in the column_scheme array
        SelectableScheme scheme = column_scheme[column];
        if (scheme != null) {
            if (table == this) {
                return scheme;
            } else {
                return scheme.getSubsetScheme(table, original_column);
            }
        }

        // If it isn't then we need to calculate it
        SelectableScheme ss;

        // Optimization: The table may be naturally ordered by a column.  If it
        // is we don't try to generate an ordered set.
        if (sorted_against_column != -1 &&
                sorted_against_column == column) {
            InsertSearch isop =
                    new InsertSearch(this, column, calculateRowReferenceList());
            isop.RECORD_UID = false;
            ss = isop;
            column_scheme[column] = ss;
            if (table != this) {
                ss = ss.getSubsetScheme(table, original_column);
            }

        } else {
            // Otherwise we must generate the ordered set from the information in
            // a parent index.
            Table parent_table = reference_list[column_table[column]];
            ss = parent_table.getSelectableSchemeFor(
                    column_filter[column], original_column, table);
            if (table == this) {
                column_scheme[column] = ss;
            }
        }

        return ss;
    }

    /**
     * Given a set, this trickles down through the Table hierarchy resolving
     * the given row_set to a form that the given ancestor understands.
     * Say you give the set { 0, 1, 2, 3, 4, 5, 6 }, this function may check
     * down three levels and return a new 7 element set with the rows fully
     * resolved to the given ancestors domain.
     */
    void setToRowTableDomain(int column, IntegerVector row_set,
                             TableDataSource ancestor) {

        if (ancestor == this) {
            return;
        } else {

            int table_num = column_table[column];
            Table parent_table = reference_list[table_num];

            // Resolve the rows into the parents indices.  (MANGLES row_set)
            resolveAllRowsForTableAt(row_set, table_num);

            parent_table.setToRowTableDomain(column_filter[column], row_set, ancestor);
            return;
        }
    }

    /**
     * Returns an object that contains fully resolved, one level only information
     * about the DataTable and the row indices of the data in this table.
     * This information can be used to construct a new VirtualTable.  We need
     * to supply an empty RawTableInformation object.
     */
    RawTableInformation resolveToRawTable(RawTableInformation info,
                                          IntegerVector row_set) {

        if (this instanceof RootTable) {
            info.add((RootTable) this, calculateRowReferenceList());
        } else {
            for (int i = 0; i < reference_list.length; ++i) {

                IntegerVector new_row_set = new IntegerVector(row_set);

                // Resolve the rows into the parents indices.
                resolveAllRowsForTableAt(new_row_set, i);

                Table table = reference_list[i];
                if (table instanceof RootTable) {
                    info.add((RootTable) table, new_row_set);
                } else {
                    ((JoinedTable) table).resolveToRawTable(info, new_row_set);
                }
            }
        }

        return info;
    }

    /**
     * Return the list of DataTable and row sets that make up the raw information
     * in this table.
     */
    RawTableInformation resolveToRawTable(RawTableInformation info) {
        IntegerVector all_list = new IntegerVector();
        int size = getRowCount();
        for (int i = 0; i < size; ++i) {
            all_list.addInt(i);
        }
        return resolveToRawTable(info, all_list);
    }

    /**
     * Returns the DataTableDef object that describes the columns in this
     * table.  For a VirtualTable, this object contains the union of
     * all the columns in the children in the order set.  The name of a
     * virtual table is the concat of all the parent table names.  The
     * schema is set to null.
     */
    public DataTableDef getDataTableDef() {
        return vt_table_def;
    }

    /**
     * Returns an object that represents the information in the given cell
     * in the table.
     */
    public TObject getCellContents(int column, int row) {
        int table_num = column_table[column];
        Table parent_table = reference_list[table_num];
        row = resolveRowForTableAt(row, table_num);
        return parent_table.getCellContents(column_filter[column], row);
    }

    /**
     * Returns an Enumeration of the rows in this table.
     * The Enumeration is a fast way of retrieving consequtive rows in the table.
     */
    public RowEnumeration rowEnumeration() {
        return new SimpleRowEnumeration(getRowCount());
    }

    /**
     * Adds a DataTableListener to the DataTable objects at the root of this
     * table tree hierarchy.  If this table represents the join of a number of
     * tables then the DataTableListener is added to all the DataTable objects
     * at the root.
     * <p>
     * A DataTableListener is notified of all modifications to the raw entries
     * of the table.  This listener can be used for detecting changes in VIEWs,
     * for triggers or for caching of common queries.
     */
    void addDataTableListener(DataTableListener listener) {
        for (Table table : reference_list) {
            table.addDataTableListener(listener);
        }
    }

    /**
     * Removes a DataTableListener from the DataTable objects at the root of
     * this table tree hierarchy.  If this table represents the join of a
     * number of tables, then the DataTableListener is removed from all the
     * DataTable objects at the root.
     */
    void removeDataTableListener(DataTableListener listener) {
        for (Table table : reference_list) {
            table.removeDataTableListener(listener);
        }
    }


    /**
     * Locks the root table(s) of this table so that it is impossible to
     * overwrite the underlying rows that may appear in this table.
     * This is used when cells in the table need to be accessed 'outside' the
     * lock.  So we may have late access to cells in the table.
     * 'lock_key' is a given key that will also unlock the root table(s).
     * NOTE: This is nothing to do with the 'LockingMechanism' object.
     */
    public void lockRoot(int lock_key) {
        // For each table, recurse.
        roots_locked++;
        for (Table table : reference_list) {
            table.lockRoot(lock_key);
        }
    }

    /**
     * Unlocks the root tables so that the underlying rows may
     * once again be used if they are not locked and have been removed.  This
     * should be called some time after the rows have been locked.
     */
    public void unlockRoot(int lock_key) {
        // For each table, recurse.
        roots_locked--;
        for (Table table : reference_list) {
            table.unlockRoot(lock_key);
        }
    }

    /**
     * Returns true if the table has its row roots locked (via the lockRoot(int)
     * method.
     */
    public boolean hasRootsLocked() {
        return roots_locked != 0;
    }


    /**
     * Prints a graph of the table hierarchy to the stream.
     */
    public void printGraph(java.io.PrintStream out, int indent) {
        for (int i = 0; i < indent; ++i) {
            out.print(' ');
        }
        out.println("JT[" + getClass());

        for (Table table : reference_list) {
            table.printGraph(out, indent + 2);
        }

        for (int i = 0; i < indent; ++i) {
            out.print(' ');
        }
        out.println("]");
    }

    // ---------- Abstract methods ----------

    /**
     * Given a row and a table index (to a parent reference table), this will
     * return the row index in the given parent table for the given row.
     */
    protected abstract int resolveRowForTableAt(int row_number, int table_num);

    /**
     * Given an IntegerVector that represents a list of pointers to rows in this
     * table, this resolves the rows to row indexes in the given parent table.
     * This method changes the 'row_set' IntegerVector object.
     */
    protected abstract void
    resolveAllRowsForTableAt(IntegerVector row_set, int table_num);


}

