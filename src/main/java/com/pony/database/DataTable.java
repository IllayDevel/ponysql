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
import com.pony.debug.*;

/**
 * DataTable is a wrapper for a MutableTableDataSource that fits into the
 * query hierarchy level.  A DataTable represents a table within a
 * transaction.  Adding, removing rows to a DataTable will change the
 * contents only with the context of the transaction the table was created in.
 * <p>
 * @author Tobias Downer
 */

public final class DataTable extends DefaultDataTable {

    /**
     * The DatabaseConnection object that is the parent of this DataTable.
     */
    private final DatabaseConnection connection;

    /**
     * A low level access to the underlying transactional data source.
     */
    private final MutableTableDataSource data_source;


    /**
     * ------
     * NOTE: Following values are only kept for lock debugging reasons.  These
     *   is no technical reason why they shouldn't be removed.  They allow us
     *   to check that a data table is locked correctly when accesses are
     *   performed on it.
     * ------
     */

    final static boolean LOCK_DEBUG = true;

    /**
     * The number of read locks we have on this table.
     */
    private int debug_read_lock_count = 0;

    /**
     * The number of write locks we have on this table (this should only ever be
     * 0 or 1).
     */
    private int debug_write_lock_count = 0;


    /**
     * Cosntructs the data table.
     */
    DataTable(DatabaseConnection connection,
              MutableTableDataSource data_source) throws DatabaseException {
        super(connection.getDatabase());
        this.connection = connection;
        this.data_source = data_source;
    }

    /**
     * Convenience - used to log debug messages.
     */
    public final DebugLogger Debug() {
        return connection.getSystem().Debug();
    }

    /**
     * Overwritten from DefaultDataTable to do nothing.  All selectable
     * schemes are handled within the DataTableManager now.
     */
    protected void blankSelectableSchemes(int type) {
    }

    /**
     * Returns the SelectableScheme for the given column.
     * (Overridden from DefaultDataTable).  If the schemes are not in memory then
     * they are loaded now.  This will synchronize over the 'table_manager'
     * which will effectively block this table at the lowest layer until the
     * indices are loaded into memory.
     */
    protected SelectableScheme getRootColumnScheme(int column) {
        checkReadLock();  // Read op

        return data_source.getColumnScheme(column);
    }

    /**
     * We can declare a DataTable as a new type.  This means, instead of
     * referencing a column as 'Customer.CustomerID' we can change the 'Customer'
     * part to anything we wish such as 'C1'.
     */
    public ReferenceTable declareAs(TableName new_name) {
        return new ReferenceTable(this, new_name);
    }

    /**
     * Generates an empty RowData object for 'addRow'ing into the Table.
     * We must first call this method to retrieve a blank RowData object,
     * fill it in with the required information, and then call 'addRow'
     */
    public final RowData createRowDataObject(QueryContext context) {
        checkSafeOperation();  // safe op
        return new RowData(this);
    }

    /**
     * Returns the current row count.  This queries the DataTableManager for
     * the real value.
     */
    public int getRowCount() {
        checkReadLock();  // read op

        return data_source.getRowCount();
    }

    /**
     * Adds a given 'RowData' object to the table.  This should be used for
     * any rows added to the table.  The order that rows are added into a table
     * is not important.
     * <p>
     * This method performs some checking of the cells in the table.  It first
     * checks that all columns declared as 'not null' have a value that is not
     * null.  It then checks that a the added row will not cause any duplicates
     * in a column declared as unique.
     * <p>
     * It then uses the low level io manager to store the data.
     * <p>
     * SYNCHRONIZATION ISSUE: We are assuming this is running in a synchronized
     *   environment that is unable to add or alter rows in this object within
     *   the lifetime of this method.
     */
    public final void add(RowData row_data) throws DatabaseException {
        checkReadWriteLock();  // write op

        if (!row_data.isSameTable(this)) {
            throw new DatabaseException(
                    "Internal Error: Using RowData from different table");
        }

        // Checks passed, so add to table.
        addRow(row_data);

        // Perform a referential integrity check on any changes to the table.
        data_source.constraintIntegrityCheck();
    }

    /**
     * Adds an array of 'RowData' objects to the table.  This should be used for
     * adding a group of rows to the table.  The order that rows are added into
     * a table is not important.
     * <p>
     * This method performs some checking of the cells in the table.  It first
     * checks that all columns declared as 'not null' have a value that is not
     * null.  It then checks that a the added row will not cause any duplicates
     * in a column declared as unique.
     * <p>
     * It then uses the low level io manager to store the data.
     * <p>
     * SYNCHRONIZATION ISSUE: We are assuming this is running in a synchronized
     *   environment that is unable to add or alter rows in this object within
     *   the lifetime of this method.
     */
    public final void add(RowData[] row_data_arr) throws DatabaseException {
        checkReadWriteLock();  // write op

        for (int i = 0; i < row_data_arr.length; ++i) {
            RowData row_data = row_data_arr[i];
            if (!row_data.isSameTable(this)) {
                throw new DatabaseException(
                        "Internal Error: Using RowData from different table");
            }
            addRow(row_data);
        }

        // Perform a referential integrity check on any changes to the table.
        data_source.constraintIntegrityCheck();
    }

    /**
     * Adds a new row of data to the table.  First of all, this tells the
     * underlying database mechanism to add the data to this table.  It then
     * add the row information to each SelectableScheme.
     */
    private void addRow(RowData row) throws DatabaseException {

        // This table name (for event notification)
        TableName table_name = getTableName();

        // Fire the 'before' trigger for an insert on this table
        connection.fireTableEvent(new TableModificationEvent(connection, table_name,
                row, true));

        // Add the row to the underlying file system
        int row_number = data_source.addRow(row);

        // Fire the 'after' trigger for an insert on this table
        connection.fireTableEvent(new TableModificationEvent(connection, table_name,
                row, false));

        // NOTE: currently nothing being done with 'row_number' after it's added.
        //   The underlying table data source manages the row index.

    }

    /**
     * Removes the given row from the table.  This is called just before the
     * row is actually deleted.  The method is provided to allow for some
     * maintenance of any search structures such as B-Trees.  This is called
     * from the 'delete' method in Table.
     */
    private void removeRow(int row_number) throws DatabaseException {

        // This table name (for event notification)
        TableName table_name = getTableName();

        // Fire the 'before' trigger for the delete on this table
        connection.fireTableEvent(new TableModificationEvent(connection, table_name,
                row_number, true));

        // Delete the row from the underlying database
        data_source.removeRow(row_number);

        // Fire the 'after' trigger for the delete on this table
        connection.fireTableEvent(new TableModificationEvent(connection, table_name,
                row_number, false));

    }

    /**
     * Updates the given row with the given data in this table.  This method
     * will likely add the modified data to a new row and delete the old
     * version of the row.
     */
    private void updateRow(int row_number, RowData row)
            throws DatabaseException {

        // This table name (for event notification)
        TableName table_name = getTableName();

        // Fire the 'before' trigger for the update on this table
        connection.fireTableEvent(
                new TableModificationEvent(connection, table_name,
                        row_number, row, true));

        // Update the row in the underlying database
        data_source.updateRow(row_number, row);

        // Fire the 'after' trigger for the update on this table
        connection.fireTableEvent(
                new TableModificationEvent(connection, table_name,
                        row_number, row, false));

    }

    /**
     * This is the public method for removing a given result set from this
     * table.  Given a Table object, this will remove from this table any row
     * that are in the given table.  The given Table must have this object as
     * its distant ancestor.  If it does not then it will throw an exception.
     * Examples: table.delete(table)           -- delete the entire table.
     *           table.delete(table.select( < some condition > ));
     * It returns the number of rows that were deleted.
     * <p>
     * <strong>INTERNAL NOTE:</strong> The 'table' parameter may be the result
     *   of joins.  This may cause the same row in this table to be referenced
     *   more than once.  We must make sure that we delete any given row only
     *   once by using the 'distinct' function.
     * <p>
     * 'limit' dictates how many rows will be deleted.  If 'limit' is less than
     * 0 then this indicates there is no limit.  Keep in mind that rows are
     * picked out from top to bottom in the 'table' object.  Normally the
     * input table will be the result of an un-ordered 'where' clause so using
     * a limit does not permit deletes in a deterministic manner.
     * <p>
     * ASSUMPTION: There are no duplicate rows in the input set.
     */
    public int delete(Table table, int limit) throws DatabaseException {
        checkReadWriteLock();  // write op

        IntegerVector row_set = new IntegerVector(table.getRowCount());
        RowEnumeration e = table.rowEnumeration();
        while (e.hasMoreRows()) {
            row_set.addInt(e.nextRowIndex());
        }
        e = null;

        // HACKY: Find the first column of this table in the search table.  This
        //   will allow us to generate a row set of only the rows in the search
        //   table.
        int first_column = table.findFieldName(getResolvedVariable(0));

        if (first_column == -1) {
            throw new DatabaseException("Search table does not contain any " +
                    "reference to table being deleted from");
        }

        // Generate a row set that is in this tables domain.
        table.setToRowTableDomain(first_column, row_set, this);

        // row_set may contain duplicate row indices, therefore we must sort so
        // any duplicates are grouped and therefore easier to find.
        row_set.quickSort();

        // If limit less than zero then limit is whole set.
        if (limit < 0) {
            limit = Integer.MAX_VALUE;
        }

        // Remove each row in row set in turn.  Make sure we don't remove the
        // same row index twice.
        int len = Math.min(row_set.size(), limit);
        int last_removed = -1;
        int remove_count = 0;
        for (int i = 0; i < len; ++i) {
            int to_remove = row_set.intAt(i);
            if (to_remove < last_removed) {
                throw new DatabaseException(
                        "Internal error: row sorting error or row_set not in the range > 0");
            }

            if (to_remove != last_removed) {
                removeRow(to_remove);
                last_removed = to_remove;
                ++remove_count;
            }

        }

        if (remove_count > 0) {
            // Perform a referential integrity check on any changes to the table.
            data_source.constraintIntegrityCheck();
        }

        return remove_count;
    }

    // Unlimited delete
    public int delete(Table table) throws DatabaseException {
        return delete(table, -1);
    }

    /**
     * Updates the table by applying the assignment operations over each row
     * that is found in the input 'table' set.  The input table must be a direct
     * child of this DataTable.
     * <p>
     * This operation assumes that there is a WRITE lock on this table.  A
     * WRITE lock means no other thread may access this table while the
     * operation is being performed.  (However, a result set may still be
     * downloading from this table).
     * <p>
     * 'limit' dictates how many rows will be updated.  If 'limit' is less than
     * 0 then this indicates there is no limit.  Keep in mind that rows are
     * picked out from top to bottom in the 'table' object.  Normally the
     * input table will be the result of an un-ordered 'where' clause so using
     * a limit does not permit updates in a deterministic manner.
     * <p>
     * Returns the number of rows updated in this table.
     * <p>
     * NOTE: We assume there are no duplicate rows to the root set from the
     *   given 'table'.
     */
    public final int update(QueryContext context,
                            Table table, Assignment[] assign_list, int limit)
            throws DatabaseException {
        checkReadWriteLock();  // write op

        // Get the rows from the input table.
        IntegerVector row_set = new IntegerVector();
        RowEnumeration e = table.rowEnumeration();
        while (e.hasMoreRows()) {
            row_set.addInt(e.nextRowIndex());
        }
        e = null;

        // HACKY: Find the first column of this table in the search table.  This
        //   will allow us to generate a row set of only the rows in the search
        //   table.
        int first_column = table.findFieldName(getResolvedVariable(0));
        if (first_column == -1) {
            throw new DatabaseException("Search table does not contain any " +
                    "reference to table being updated from");
        }

        // Convert the row_set to this table's domain.
        table.setToRowTableDomain(first_column, row_set, this);

        // NOTE: Assume there's no duplicate rows.

        RowData original_data = createRowDataObject(context);
        RowData row_data = createRowDataObject(context);

        // If limit less than zero then limit is whole set.
        if (limit < 0) {
            limit = Integer.MAX_VALUE;
        }

        // Update each row in row set in turn up to the limit.
        int len = Math.min(row_set.size(), limit);
        int update_count = 0;
        for (int i = 0; i < len; ++i) {
            int to_update = row_set.intAt(i);

            // Make a RowData object from this row (plus keep the original intact
            // incase we need to roll back to it).
            original_data.setFromRow(to_update);
            row_data.setFromRow(to_update);

            // Run each assignment on the RowData.
            for (int n = 0; n < assign_list.length; ++n) {
                Assignment assignment = assign_list[n];
                row_data.evaluate(assignment, context);
            }

            // Update the row
            updateRow(to_update, row_data);

            ++update_count;
        }

        if (update_count > 0) {
            // Perform a referential integrity check on any changes to the table.
            data_source.constraintIntegrityCheck();
        }

        return update_count;

    }

    /**
     * Returns the DataTableDef object for this table.  This object describes
     * how the table is made up.
     * <p>
     * <strong>NOTE:</strong> Do not keep references to this object.  The
     *   DataTableDef is invalidated when a table is closed.
     */
    public DataTableDef getDataTableDef() {
        checkSafeOperation();  // safe op

        return data_source.getDataTableDef();
    }

    /**
     * Returns the schema that this table is within.
     */
    public String getSchema() {
        checkSafeOperation();  // safe op

        return getDataTableDef().getSchema();
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
    public void addDataTableListener(DataTableListener listener) {
        // Currently we do nothing with this info.
    }

    /**
     * Removes a DataTableListener from the DataTable objects at the root of
     * this table tree hierarchy.  If this table represents the join of a
     * number of tables, then the DataTableListener is removed from all the
     * DataTable objects at the root.
     */
    public void removeDataTableListener(DataTableListener listener) {
        // Currently we do nothing with this info.
    }


    // -------- Methods implemented for DefaultDataTable --------

    /**
     * Given a set, this trickles down through the Table hierarchy resolving
     * the given row_set to a form that the given ancestor understands.
     * Say you give the set { 0, 1, 2, 3, 4, 5, 6 }, this function may check
     * down three levels and return a new 7 element set with the rows fully
     * resolved to the given ancestors domain.
     */
    void setToRowTableDomain(int column, IntegerVector row_set,
                             TableDataSource ancestor) {
        checkReadLock();  // read op

        if (ancestor != this && ancestor != data_source) {
            throw new RuntimeException("Method routed to incorrect table ancestor.");
        }
    }

    /**
     * Returns an object that represents the information in the given cell
     * in the table.  This can be used to obtain information about the given
     * table cells.
     */
    public TObject getCellContents(int column, int row) {
        checkSafeOperation();  // safe op

        return data_source.getCellContents(column, row);
    }

    /**
     * Returns an Enumeration of the rows in this table.
     * Each call to 'nextRowIndex' returns the next valid row index in the table.
     */
    public RowEnumeration rowEnumeration() {
        checkReadLock();  // read op

        return data_source.rowEnumeration();
    }


    /**
     * Locks the root table(s) of this table so that it is impossible to
     * overwrite the underlying rows that may appear in this table.
     * This is used when cells in the table need to be accessed 'outside' the
     * lock.  So we may have late access to cells in the table.
     * 'lock_key' is a given key that will also unlock the root table(s).
     * <p>
     * NOTE: This is nothing to do with the 'LockingMechanism' object.
     */
    public void lockRoot(int lock_key) {
        checkSafeOperation();  // safe op

        data_source.addRootLock();
    }

    /**
     * Unlocks the root tables so that the underlying rows may
     * once again be used if they are not locked and have been removed.  This
     * should be called some time after the rows have been locked.
     */
    public void unlockRoot(int lock_key) {
        checkSafeOperation();  // safe op

        data_source.removeRootLock();
    }

    /**
     * Returns true if the table has its row roots locked (via the lockRoot(int)
     * method.
     */
    public boolean hasRootsLocked() {
        // There is no reason why we would need to know this information at
        // this level.
        // We need to deprecate this properly.
        throw new Error("hasRootsLocked is deprecated.");
    }


    // ------------ Lock debugging methods ----------

    /**
     * This is called by the 'Lock' class to notify this DataTable that a read/
     * write lock has been applied to this table.  This is for lock debugging
     * purposes only.
     */
    final void notifyAddRWLock(int lock_type) {
        if (LOCK_DEBUG) {
            if (lock_type == Lock.READ) {
                ++debug_read_lock_count;
            } else if (lock_type == Lock.WRITE) {
                ++debug_write_lock_count;
                if (debug_write_lock_count > 1) {
                    throw new Error(">1 write lock on table " + getTableName());
                }
            } else {
                throw new Error("Unknown lock type: " + lock_type);
            }
        }
    }

    /**
     * This is called by the 'Lock' class to notify this DataTable that a read/
     * write lock has been released from this table.  This is for lock debugging
     * purposes only.
     */
    final void notifyReleaseRWLock(int lock_type) {
        if (LOCK_DEBUG) {
            if (lock_type == Lock.READ) {
                --debug_read_lock_count;
            } else if (lock_type == Lock.WRITE) {
                --debug_write_lock_count;
            } else {
                Debug().writeException(
                        new RuntimeException("Unknown lock type: " + lock_type));
            }
        }
    }

    /**
     * Returns true if the database is in exclusive mode.
     */
    private boolean isInExclusiveMode() {
        // Check the connection locking mechanism is in exclusive mode
        return connection.getLockingMechanism().isInExclusiveMode();
    }

    /**
     * Checks the database is in exclusive mode.
     */
    private void checkInExclusiveMode() {
        if (!isInExclusiveMode()) {
            Debug().writeException(new RuntimeException(
                    "Performed exclusive operation on table and not in exclusive mode!"));
        }
    }

    /**
     * Check that we can safely read from this table.
     */
    private void checkReadLock() {
        if (LOCK_DEBUG) {
            // All system tables are given read access because they may only be
            // written under exclusive mode anyway.

            boolean is_internal_table =
                    getTableName().getSchema().equals(Database.SYSTEM_SCHEMA);

            if (!(is_internal_table ||
                    debug_read_lock_count > 0 ||
                    debug_write_lock_count > 0 ||
                    isInExclusiveMode())) {

                System.err.println();
                System.err.print(" is_internal_table = " + is_internal_table);
                System.err.print(" debug_read_lock_count = " + debug_read_lock_count);
                System.err.print(" debug_write_lock_count = " + debug_write_lock_count);
                System.err.println(" isInExclusiveMode = " + isInExclusiveMode());

                Debug().writeException(new Error(
                        "Invalid read access on table '" + getTableName() + "'"));
            }
        }
    }

    /**
     * Check that we can safely read/write from this table.  This should catch
     * any synchronization concurrent issues.
     */
    private void checkReadWriteLock() {
        if (LOCK_DEBUG) {
            // We have to own exactly one write lock, or be in exclusive mode.
            if (!(debug_write_lock_count == 1 || isInExclusiveMode())) {
                Debug().writeException(
                        new Error("Invalid read/write access on table '" +
                                getTableName() + "'"));
            }
        }
    }

    /**
     * Check that we can run a safe operation.
     */
    private void checkSafeOperation() {
        // no operation - nothing to check for...
    }


    // ---------- Overwritten to output debug info ----------
    // NOTE: These can all safely be commented out.

    public int getColumnCount() {
        checkSafeOperation();  // safe op

        return super.getColumnCount();
    }

    public Variable getResolvedVariable(int column) {
        checkSafeOperation();  // safe op

        return super.getResolvedVariable(column);
    }

    public int findFieldName(Variable v) {
        checkSafeOperation();  // safe op

        return super.findFieldName(v);
    }

    SelectableScheme getSelectableSchemeFor(int column, int original_column,
                                            Table table) {
        checkReadLock();  // read op

        return super.getSelectableSchemeFor(column, original_column, table);
    }

    RawTableInformation resolveToRawTable(RawTableInformation info) {
        checkReadLock();  // read op

        return super.resolveToRawTable(info);
    }

}
