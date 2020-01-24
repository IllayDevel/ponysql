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
 * This object sits on top of a DataTable object filtering out certain types
 * of calls.  We could use this object to implement a ReferenceTable which
 * can be used to declare a new table name with a DataTable type.  We also
 * use this object to implement a filter for column removals.
 * <p>
 * @author Tobias Downer
 */

public class DataTableFilter extends AbstractDataTable {

    /**
     * The parent DataTable object.
     */
    protected final AbstractDataTable parent;

    /**
     * The Constructor.  A filter can only sit on top of a DataTable or
     * DataTableFilter table.
     * ISSUE: we could make an interface for this.  This is a bit of a hack.
     */
    protected DataTableFilter(AbstractDataTable table) {
        super();
        parent = table;
    }

    /**
     * Returns the Database context for this filtered table.
     */
    public Database getDatabase() {
        return parent.getDatabase();
    }

    /**
     * Returns the number of columns in the table.
     */
    public int getColumnCount() {
        return parent.getColumnCount();
    }

    /**
     * Returns the number of rows stored in the table.
     */
    public final int getRowCount() {
        return parent.getRowCount();
    }

    /**
     * Given a fully qualified variable field name, ie. 'APP.CUSTOMER.CUSTOMERID'
     * this will return the column number the field is at.  Returns -1 if the
     * field does not exist in the table.
     */
    public int findFieldName(Variable v) {
        return parent.findFieldName(v);
    }

    /**
     * Returns a fully qualified Variable object that represents the name of
     * the column at the given index.  For example,
     *   new Variable(new TableName("APP", "CUSTOMER"), "ID")
     */
    public Variable getResolvedVariable(int column) {
        return parent.getResolvedVariable(column);
    }

    /**
     * Returns a SelectableScheme for the given column in the given VirtualTable
     * row domain.  Slight Hack: When we are asking for a selectable scheme for
     * a reference table, we must defer the 'table' variable to the parent.
     */
    final SelectableScheme getSelectableSchemeFor(int column,
                                                  int original_column, Table table) {
        if (table == this) {
            return parent.getSelectableSchemeFor(column, original_column, parent);
        } else {
            return parent.getSelectableSchemeFor(column, original_column, table);
        }
    }

    /**
     * Given a set, this trickles down through the Table hierarchy resolving
     * the given row_set to a form that the given ancestor understands.
     * Say you give the set { 0, 1, 2, 3, 4, 5, 6 }, this function may check
     * down three levels and return a new 7 element set with the rows fully
     * resolved to the given ancestors domain.
     * <p>
     * Slight Hack: When we are asking for a selectable scheme for a reference
     * table, we must defer the 'table' variable to the parent.
     */
    final void setToRowTableDomain(int column, IntegerVector row_set,
                                   TableDataSource ancestor) {
        if (ancestor == this) {
            parent.setToRowTableDomain(column, row_set, parent);
        } else {
            parent.setToRowTableDomain(column, row_set, ancestor);
        }
    }

    /**
     * Return the list of DataTable and row sets that make up the raw information
     * in this table.  This is identical to the DataTable method except it
     * puts this table as the owner of the row set.
     */
    final RawTableInformation resolveToRawTable(RawTableInformation info) {
        IntegerVector row_set = new IntegerVector();
        RowEnumeration e = rowEnumeration();
        while (e.hasMoreRows()) {
            row_set.addInt(e.nextRowIndex());
        }
        info.add(this, row_set);
        return info;
    }

    /**
     * Returns an object that represents the information in the given cell
     * in the table.  This will generally be an expensive algorithm, so calls
     * to it should be kept to a minimum.  Note that the offset between two
     * rows is not necessarily 1.
     */
    public final TObject getCellContents(int column, int row) {
        return parent.getCellContents(column, row);
    }

    /**
     * Returns an Enumeration of the rows in this table.
     * The Enumeration is a fast way of retrieving consequtive rows in the table.
     */
    public final RowEnumeration rowEnumeration() {
        return parent.rowEnumeration();
    }

    /**
     * Returns a DataTableDef object that defines the name of the table and the
     * layout of the columns of the table.  Note that for tables that are joined
     * with other tables, the table name and schema for this object become
     * mangled.  For example, a table called 'PERSON' joined with a table called
     * 'MUSIC' becomes a table called 'PERSON#MUSIC' in a null schema.
     */
    public DataTableDef getDataTableDef() {
        return parent.getDataTableDef();
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
        parent.addDataTableListener(listener);
    }

    /**
     * Removes a DataTableListener from the DataTable objects at the root of
     * this table tree hierarchy.  If this table represents the join of a
     * number of tables, then the DataTableListener is removed from all the
     * DataTable objects at the root.
     */
    void removeDataTableListener(DataTableListener listener) {
        parent.removeDataTableListener(listener);
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
        parent.lockRoot(lock_key);
    }

    /**
     * Unlocks the root tables so that the underlying rows may
     * once again be used if they are not locked and have been removed.  This
     * should be called some time after the rows have been locked.
     */
    public void unlockRoot(int lock_key) {
        parent.unlockRoot(lock_key);
    }

    /**
     * Returns true if the table has its row roots locked (via the lockRoot(int)
     * method.
     */
    public boolean hasRootsLocked() {
        return parent.hasRootsLocked();
    }

}
