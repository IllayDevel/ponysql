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

import com.pony.debug.*;
import com.pony.util.IntegerVector;
import com.pony.util.BigNumber;
import com.pony.database.global.ByteLongObject;
import com.pony.database.global.ObjectTranslator;

import java.io.IOException;
import java.util.ArrayList;

/**
 * An open transaction that manages all data access to the
 * TableDataConglomerate.  A transaction sees a view of the data as it was when
 * the transaction was created.  It also sees any modifications that were made
 * within the context of this transaction.  It does not see modifications made
 * by other open transactions.
 * <p>
 * A transaction ends when it is committed or rollbacked.  All operations
 * on this transaction object only occur within the context of this transaction
 * and are not permanent changes to the database structure.  Only when the
 * transaction is committed are changes reflected in the master data.
 *
 * @author Tobias Downer
 */

public class Transaction extends SimpleTransaction {

    // ---------- Constraint statics ----------
    // These statics are for managing constraints.

    /**
     * The type of deferrance.
     */
    public static final short INITIALLY_DEFERRED =
            java.sql.DatabaseMetaData.importedKeyInitiallyDeferred;
    public static final short INITIALLY_IMMEDIATE =
            java.sql.DatabaseMetaData.importedKeyInitiallyImmediate;
    public static final short NOT_DEFERRABLE =
            java.sql.DatabaseMetaData.importedKeyNotDeferrable;

    /**
     * Foreign key referential trigger actions.
     */
    public static final String NO_ACTION = "NO ACTION";
    public static final String CASCADE = "CASCADE";
    public static final String SET_NULL = "SET NULL";
    public static final String SET_DEFAULT = "SET DEFAULT";

    // ---------- Member variables ----------

    /**
     * The TableDataConglomerate that this transaction is within the context of.
     */
    private TableDataConglomerate conglomerate;

    /**
     * The commit_id that represents the id of the last commit that occurred
     * when this transaction was created.
     */
    private long commit_id;

    /**
     * All tables touched by this transaction.  (MutableTableDataSource)
     */
    private ArrayList touched_tables;

    /**
     * All tables selected from in this transaction.  (MasterTableDataSource)
     */
    private ArrayList selected_from_tables;

    /**
     * The name of all database objects that were created in this transaction.
     * This is used for a namespace collision test during commit.
     */
    private ArrayList created_database_objects;

    /**
     * The name of all database objects that were dropped in this transaction.
     * This is used for a namespace collision test during commit.
     */
    private ArrayList dropped_database_objects;

    /**
     * The journal for this transaction.  This journal describes all changes
     * made to the database by this transaction.
     */
    private TransactionJournal journal;

    /**
     * The list of InternalTableInfo objects that are containers for generating
     * internal tables (GTDataSource).
     */
    private InternalTableInfo[] internal_tables;

    /**
     * A pointer in the internal_tables list.
     */
    private int internal_tables_i;

    /**
     * True if an error should be generated on a dirty select.
     */
    private boolean transaction_error_on_dirty_select;

    /**
     * True if this transaction is closed.
     */
    private boolean closed;


    /**
     * Constructs the transaction.
     */
    Transaction(TableDataConglomerate conglomerate,
                long commit_id, ArrayList visible_tables,
                ArrayList table_indices) {

        super(conglomerate.getSystem(), conglomerate.getSequenceManager());

        this.conglomerate = conglomerate;
        this.commit_id = commit_id;
        this.closed = false;

        this.created_database_objects = new ArrayList();
        this.dropped_database_objects = new ArrayList();

        this.touched_tables = new ArrayList();
        this.selected_from_tables = new ArrayList();
        journal = new TransactionJournal();

        // Set up all the visible tables
        int sz = visible_tables.size();
        for (int i = 0; i < sz; ++i) {
            addVisibleTable((MasterTableDataSource) visible_tables.get(i),
                    (IndexSet) table_indices.get(i));
        }

        // NOTE: We currently only support 8 - internal tables to the transaction
        //  layer, and internal tables to the database connection layer.
        internal_tables = new InternalTableInfo[8];
        internal_tables_i = 0;
        addInternalTableInfo(new TransactionInternalTables());

        getSystem().stats().increment("Transaction.count");

        // Defaults to true (should be changed by called 'setErrorOnDirtySelect'
        // method.
        transaction_error_on_dirty_select = true;
    }

    /**
     * Returns the TableDataConglomerate of this transaction.
     */
    final TableDataConglomerate getConglomerate() {
        return conglomerate;
    }

    /**
     * Adds an internal table container (InternalTableInfo) used to
     * resolve internal tables.  This is intended as a way for the
     * DatabaseConnection layer to plug in 'virtual' tables, such as those
     * showing connection statistics, etc.  It also allows modelling database
     * objects as tables, such as sequences, triggers, procedures, etc.
     */
    void addInternalTableInfo(InternalTableInfo info) {
        if (internal_tables_i >= internal_tables.length) {
            throw new RuntimeException("Internal table list bounds reached.");
        }
        internal_tables[internal_tables_i] = info;
        ++internal_tables_i;
    }

    /**
     * Returns the 'commit_id' which is the last commit that occured before
     * this transaction was created.
     * <p>
     * NOTE: Don't make this synchronized over anything.  This is accessed
     *   by OpenTransactionList.
     */
    long getCommitID() {
        // REINFORCED NOTE: This absolutely must never be synchronized because
        //   it is accessed by OpenTransactionList synchronized.
        return commit_id;
    }

    // ----- Operations within the context of this transaction -----

    /**
     * Overwritten from SimpleTransaction.
     * Returns a new MutableTableDataSource for the view of the
     * MasterTableDataSource at the start of this transaction.  Note that this is
     * only ever called once per table accessed in this transaction.
     */
    public MutableTableDataSource
    createMutableTableDataSourceAtCommit(MasterTableDataSource master) {
        // Create the table for this transaction.
        MutableTableDataSource table = master.createTableDataSourceAtCommit(this);
        // Log in the journal that this table was touched by the transaction.
        journal.entryAddTouchedTable(master.getTableID());
        touched_tables.add(table);
        return table;
    }

    /**
     * Called by the query evaluation layer when information is selected
     * from this table as part of this transaction.  When there is a select
     * query on a table, when the transaction is committed we should look for
     * any concurrently committed changes to the table.  If there are any, then
     * any selects on the table should be considered incorrect and cause a
     * commit failure.
     */
    public void addSelectedFromTable(TableName table_name) {
        // Special handling of internal tables,
        if (isDynamicTable(table_name)) {
            return;
        }

        MasterTableDataSource master = findVisibleTable(table_name, false);
        if (master == null) {
            throw new StatementException(
                    "Table with name not available: " + table_name);
        }
//    System.out.println("Selected from table: " + table_name);
        synchronized (selected_from_tables) {
            if (!selected_from_tables.contains(master)) {
                selected_from_tables.add(master);
            }
        }

    }


    /**
     * Copies all the tables within this transaction view to the destination
     * conglomerate object.  Some care should be taken with security when using
     * this method.  This is useful for generating a backup of the current
     * view of the database that can work without interfering with the general
     * operation of the database.
     */
    void liveCopyAllDataTo(TableDataConglomerate dest_conglomerate) {
        // Create a new TableDataConglomerate using the same settings from this
        // TransactionSystem but on the new StoreSystem.
        int sz = getVisibleTableCount();

        // The list to copy (in the order to copy in).
        ArrayList copy_list = new ArrayList(sz);

        // The 'SEQUENCE_INFO' table is handled specially,
        MasterTableDataSource sequence_info_table = null;

        for (int i = 0; i < sz; ++i) {
            MasterTableDataSource master_table = getVisibleTable(i);
            TableName table_name = master_table.getDataTableDef().getTableName();
            if (table_name.equals(TableDataConglomerate.SYS_SEQUENCE_INFO)) {
                sequence_info_table = master_table;
            } else {
                copy_list.add(master_table);
            }
        }

        // Add the sequence info to the end of the list,
        copy_list.add(sequence_info_table);

        try {
            // For each master table,
            for (int i = 0; i < sz; ++i) {

                MasterTableDataSource master_table =
                        (MasterTableDataSource) copy_list.get(i);
                TableName table_name = master_table.getDataTableDef().getTableName();

                // Create a destination transaction
                Transaction dest_transaction = dest_conglomerate.createTransaction();

                // The view of this table within this transaction.
                IndexSet index_set = getIndexSetForTable(master_table);

                // If the table already exists then drop it
                if (dest_transaction.tableExists(table_name)) {
                    dest_transaction.dropTable(table_name);
                }

                // Copy it into the destination conglomerate.
                dest_transaction.copyTable(master_table, index_set);

                // Close and commit the transaction in the destination conglomeration.
                dest_transaction.closeAndCommit();

                // Dispose the IndexSet
                index_set.dispose();

            }

        } catch (TransactionException e) {
            Debug().writeException(e);
            throw new RuntimeException("Transaction Error when copying table: " +
                    e.getMessage());
        }
    }

    // ---------- Dynamically generated tables ----------

    /**
     * Returns true if the given table name represents a dynamically generated
     * system table.
     */
    protected boolean isDynamicTable(TableName table_name) {
        for (int i = 0; i < internal_tables.length; ++i) {
            InternalTableInfo info = internal_tables[i];
            if (info != null) {
                if (info.containsTableName(table_name)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns a list of all dynamic table names.  This method returns a
     * reference to a static, make sure you don't change the contents of the
     * array!
     */
    protected TableName[] getDynamicTableList() {
        int sz = 0;
        for (int i = 0; i < internal_tables.length; ++i) {
            InternalTableInfo info = internal_tables[i];
            if (info != null) {
                sz += info.getTableCount();
            }
        }

        TableName[] list = new TableName[sz];
        int index = 0;

        for (int i = 0; i < internal_tables.length; ++i) {
            InternalTableInfo info = internal_tables[i];
            if (info != null) {
                sz = info.getTableCount();
                for (int n = 0; n < sz; ++n) {
                    list[index] = info.getTableName(n);
                    ++index;
                }
            }
        }

        return list;
    }

    /**
     * Returns the DataTableDef for the given internal table.
     */
    protected DataTableDef getDynamicDataTableDef(TableName table_name) {

        for (int i = 0; i < internal_tables.length; ++i) {
            InternalTableInfo info = internal_tables[i];
            if (info != null) {
                int index = info.findTableName(table_name);
                if (index != -1) {
                    return info.getDataTableDef(index);
                }
            }
        }

        throw new RuntimeException("Not an internal table: " + table_name);
    }

    /**
     * Returns an instance of MutableDataTableSource that represents the
     * contents of the internal table with the given name.
     */
    protected MutableTableDataSource getDynamicTable(TableName table_name) {

        for (int i = 0; i < internal_tables.length; ++i) {
            InternalTableInfo info = internal_tables[i];
            if (info != null) {
                int index = info.findTableName(table_name);
                if (index != -1) {
                    return info.createInternalTable(index);
                }
            }
        }

        throw new RuntimeException("Not an internal table: " + table_name);
    }

    /**
     * Returns a string type describing the type of the dynamic table.
     */
    public String getDynamicTableType(TableName table_name) {
        // Otherwise we need to look up the table in the internal table list,
        for (int i = 0; i < internal_tables.length; ++i) {
            InternalTableInfo info = internal_tables[i];
            if (info != null) {
                int index = info.findTableName(table_name);
                if (index != -1) {
                    return info.getTableType(index);
                }
            }
        }
        // No internal table found, so report the error.
        throw new RuntimeException("No table '" + table_name +
                "' to report type for.");
    }


    // ---------- Transaction manipulation ----------

    /**
     * Creates a new table within this transaction with the given sector size.
     * If the table already exists then an exception is thrown.
     * <p>
     * This should only be called under an exclusive lock on the connection.
     */
    public void createTable(DataTableDef table_def,
                            int data_sector_size, int index_sector_size) {

        TableName table_name = table_def.getTableName();
        MasterTableDataSource master = findVisibleTable(table_name, false);
        if (master != null) {
            throw new StatementException(
                    "Table '" + table_name + "' already exists.");
        }

        table_def.setImmutable();

        if (data_sector_size < 27) {
            data_sector_size = 27;
        } else if (data_sector_size > 4096) {
            data_sector_size = 4096;
        }

        // Create the new master table and add to list of visible tables.
        master = conglomerate.createMasterTable(table_def, data_sector_size,
                index_sector_size);
        // Add this table (and an index set) for this table.
        addVisibleTable(master, master.createIndexSet());

        // Log in the journal that this transaction touched the table_id.
        int table_id = master.getTableID();
        journal.entryAddTouchedTable(table_id);

        // Log in the journal that we created this table.
        journal.entryTableCreate(table_id);

        // Add entry to the Sequences table for the native generator for this
        // table.
        SequenceManager.addNativeTableGenerator(this, table_name);

        // Notify that this database object has been successfully created.
        databaseObjectCreated(table_name);

    }

    /**
     * Creates a new table within this transaction.  If the table already
     * exists then an exception is thrown.
     * <p>
     * This should only be called under an exclusive lock on the connection.
     */
    public void createTable(DataTableDef table_def) {
        // data sector size defaults to 251
        // index sector size defaults to 1024
        createTable(table_def, 251, 1024);
    }

    /**
     * Given a DataTableDef, if the table exists then it is updated otherwise
     * if it doesn't exist then it is created.
     * <p>
     * This should only be used as very fine grain optimization for creating/
     * altering tables.  If in the future the underlying table model is changed
     * so that the given 'sector_size' value is unapplicable, then the value
     * will be ignored.
     */
    public void alterCreateTable(DataTableDef table_def,
                                 int data_sector_size, int index_sector_size) {
        if (!tableExists(table_def.getTableName())) {
            createTable(table_def, data_sector_size, index_sector_size);
        } else {
            alterTable(table_def.getTableName(), table_def,
                    data_sector_size, index_sector_size);
        }
    }

    /**
     * Drops a table within this transaction.  If the table does not exist then
     * an exception is thrown.
     * <p>
     * This should only be called under an exclusive lock on the connection.
     */
    public void dropTable(TableName table_name) {
//    System.out.println(this + " DROP: " + table_name);
        MasterTableDataSource master = findVisibleTable(table_name, false);

        if (master == null) {
            throw new StatementException(
                    "Table '" + table_name + "' doesn't exist.");
        }

        // Removes this table from the visible table list of this transaction
        removeVisibleTable(master);

        // Log in the journal that this transaction touched the table_id.
        int table_id = master.getTableID();
        journal.entryAddTouchedTable(table_id);

        // Log in the journal that we dropped this table.
        journal.entryTableDrop(table_id);

        // Remove the native sequence generator (in this transaction) for this
        // table.
        SequenceManager.removeNativeTableGenerator(this, table_name);

        // Notify that this database object has been dropped
        databaseObjectDropped(table_name);

    }

    /**
     * Generates an exact copy of the table within this transaction.  It is
     * recommended that the table is dropped before the copy is made.  The
     * purpose of this method is to generate a temporary table that can be
     * modified without fear of another transaction changing the contents in
     * another transaction.  This also provides a convenient way to compact
     * a table because any spare space is removed when the table is copied.  It
     * also allows us to make a copy of MasterTableDataSource into a foreign
     * conglomerate which allows us to implement a backup procedure.
     * <p>
     * This method does NOT assume the given MasterTableDataSource is contained,
     * or has once been contained within this conglomerate.
     */
    public void copyTable(
            MasterTableDataSource src_master_table, IndexSet index_set) {

        DataTableDef table_def = src_master_table.getDataTableDef();
        TableName table_name = table_def.getTableName();
        MasterTableDataSource master = findVisibleTable(table_name, false);
        if (master != null) {
            throw new StatementException(
                    "Unable to copy.  Table '" + table_name + "' already exists.");
        }

        // Copy the master table and add to the list of visible tables.
        master = conglomerate.copyMasterTable(src_master_table, index_set);
        // Add this visible table
        addVisibleTable(master, master.createIndexSet());

        // Log in the journal that this transaction touched the table_id.
        int table_id = master.getTableID();
        journal.entryAddTouchedTable(table_id);

        // Log in the journal that we created this table.
        journal.entryTableCreate(table_id);

        // Add entry to the Sequences table for the native generator for this
        // table.
        SequenceManager.addNativeTableGenerator(this, table_name);

        // Notify that this database object has been successfully created.
        databaseObjectCreated(table_name);

    }

    /**
     * Alter the table with the given name to the new definition and give the
     * copied table a new data sector size.  If the table does not exist then
     * an exception is thrown.
     * <p>
     * This copies all columns that were in the original table to the new
     * altered table if the name is the same.  Any names that don't exist are
     * set to the default value.
     * <p>
     * This should only be called under an exclusive lock on the connection.
     */
    public void alterTable(TableName table_name, DataTableDef table_def,
                           int data_sector_size, int index_sector_size) {

        table_def.setImmutable();

        // The current schema context is the schema of the table name
        String current_schema = table_name.getSchema();
        SystemQueryContext context = new SystemQueryContext(this, current_schema);

        // Get the next unique id of the unaltered table.
        long next_id = nextUniqueID(table_name);

        // Drop the current table
        MutableTableDataSource c_table = getTable(table_name);
        dropTable(table_name);
        // And create the table table
        createTable(table_def);
        MutableTableDataSource altered_table = getTable(table_name);

        // Get the new MasterTableDataSource object
        MasterTableDataSource new_master_table =
                findVisibleTable(table_name, false);
        // Set the sequence id of the table
        new_master_table.setUniqueID(next_id);

        // Work out which columns we have to copy to where
        int[] col_map = new int[table_def.columnCount()];
        DataTableDef orig_td = c_table.getDataTableDef();
        for (int i = 0; i < col_map.length; ++i) {
            String col_name = table_def.columnAt(i).getName();
            col_map[i] = orig_td.findColumnName(col_name);
        }

        try {
            // First move all the rows from the old table to the new table,
            // This does NOT update the indexes.
            try {
                RowEnumeration e = c_table.rowEnumeration();
                while (e.hasMoreRows()) {
                    int row_index = e.nextRowIndex();
                    RowData row_data = new RowData(altered_table);
                    for (int i = 0; i < col_map.length; ++i) {
                        int col = col_map[i];
                        if (col != -1) {
                            row_data.setColumnData(i,
                                    c_table.getCellContents(col, row_index));
                        }
                    }
                    row_data.setDefaultForRest(context);
                    // Note we use a low level 'addRow' method on the master table
                    // here.  This does not touch the table indexes.  The indexes are
                    // built later.
                    int new_row_number = new_master_table.addRow(row_data);
                    // Set the record as committed added
                    new_master_table.writeRecordType(new_row_number, 0x010);
                }
            } catch (DatabaseException e) {
                Debug().writeException(e);
                throw new RuntimeException(e.getMessage());
            }

            // PENDING: We need to copy any existing index definitions that might
            //   have been set on the table being altered.

            // Rebuild the indexes in the new master table,
            new_master_table.buildIndexes();

            // Get the snapshot index set on the new table and set it here
            setIndexSetForTable(new_master_table, new_master_table.createIndexSet());

            // Flush this out of the table cache
            flushTableCache(table_name);

            // Ensure the native sequence generator exists...
            SequenceManager.removeNativeTableGenerator(this, table_name);
            SequenceManager.addNativeTableGenerator(this, table_name);

            // Notify that this database object has been successfully dropped and
            // created.
            databaseObjectDropped(table_name);
            databaseObjectCreated(table_name);

        } catch (IOException e) {
            Debug().writeException(e);
            throw new RuntimeException(e.getMessage());
        }

    }

    /**
     * Alters the table with the given name within this transaction to the
     * specified table definition.  If the table does not exist then an exception
     * is thrown.
     * <p>
     * This should only be called under an exclusive lock on the connection.
     */
    public void alterTable(TableName table_name, DataTableDef table_def) {

        // Make sure we remember the current sector size of the altered table so
        // we can create the new table with the original size.
        try {

            int current_data_sector_size;
            MasterTableDataSource master = findVisibleTable(table_name, false);
            if (master instanceof V1MasterTableDataSource) {
                current_data_sector_size =
                        ((V1MasterTableDataSource) master).rawDataSectorSize();
            } else {
                current_data_sector_size = -1;
            }
            // HACK: We use index sector size of 2043 for all altered tables
            alterTable(table_name, table_def, current_data_sector_size, 2043);

        } catch (IOException e) {
            throw new RuntimeException("IO Error: " + e.getMessage());
        }

    }


    /**
     * Checks all the rows in the table for immediate constraint violations
     * and when the transaction is next committed check for all deferred
     * constraint violations.  This method is used when the constraints on a
     * table changes and we need to determine if any constraint violations
     * occurred.  To the constraint checking system, this is like adding all
     * the rows to the given table.
     */
    public void checkAllConstraints(TableName table_name) {
        // Get the table
        TableDataSource table = getTable(table_name);
        // Get all the rows in the table
        int[] rows = new int[table.getRowCount()];
        RowEnumeration row_enum = table.rowEnumeration();
        int i = 0;
        while (row_enum.hasMoreRows()) {
            rows[i] = row_enum.nextRowIndex();
            ++i;
        }
        // Check the constraints of all the rows in the table.
        TableDataConglomerate.checkAddConstraintViolations(
                this, table, rows, INITIALLY_IMMEDIATE);

        // Add that we altered this table in the journal
        MasterTableDataSource master = findVisibleTable(table_name, false);
        if (master == null) {
            throw new StatementException(
                    "Table '" + table_name + "' doesn't exist.");
        }

        // Log in the journal that this transaction touched the table_id.
        int table_id = master.getTableID();

        journal.entryAddTouchedTable(table_id);
        // Log in the journal that we dropped this table.
        journal.entryTableConstraintAlter(table_id);

    }

    /**
     * Compacts the table with the given name within this transaction.  If the
     * table doesn't exist then an exception is thrown.
     */
    public void compactTable(TableName table_name) {

        // Find the master table.
        MasterTableDataSource current_table = findVisibleTable(table_name, false);
        if (current_table == null) {
            throw new StatementException(
                    "Table '" + table_name + "' doesn't exist.");
        }

        // If the table is worth compacting, or the table is a
        // V1MasterTableDataSource
        if (current_table.isWorthCompacting()) {
            // The view of this table within this transaction.
            IndexSet index_set = getIndexSetForTable(current_table);
            // Drop the current table
            dropTable(table_name);
            // And copy to the new table
            copyTable(current_table, index_set);
        }

    }


    /**
     * Returns true if the conglomerate commit procedure should check for
     * dirty selects and produce a transaction error.  A dirty select is when
     * a query reads information from a table that is effected by another table
     * during a transaction.  This in itself will not cause data
     * consistancy problems but for strict conformance to SERIALIZABLE
     * isolation level this should return true.
     * <p>
     * NOTE; We MUST NOT make this method serialized because it is back called
     *   from within a commit lock in TableDataConglomerate.
     */
    boolean transactionErrorOnDirtySelect() {
        return transaction_error_on_dirty_select;
    }

    /**
     * Sets the transaction error on dirty select for this transaction.
     */
    void setErrorOnDirtySelect(boolean status) {
        transaction_error_on_dirty_select = status;
    }

    // ----- Setting/Querying constraint information -----
    // PENDING: Is it worth implementing a pluggable constraint architecture
    //   as described in the idea below.  With the current implementation we
    //   have tied a DataTableConglomerate to a specific constraint
    //   architecture.
    //
    // IDEA: These methods delegate to the parent conglomerate which has a
    //   pluggable architecture for setting/querying constraints.  Some uses of
    //   a conglomerate may not need integrity constraints or may implement the
    //   mechanism for storing/querying in a different way.  This provides a
    //   useful abstraction of being enable to implement constraint behaviour
    //   by only providing a way to set/query the constraint information in
    //   different conglomerate uses.

    /**
     * Convenience, given a SimpleTableQuery object this will return a list of
     * column names in sequence that represent the columns in a group constraint.
     * <p>
     * 'cols' is the unsorted list of indexes in the table that represent the
     * group.
     * <p>
     * Assumes column 2 of dt is the sequence number and column 1 is the name
     * of the column.
     */
    private static String[] toColumns(SimpleTableQuery dt, IntegerVector cols) {
        int size = cols.size();
        String[] list = new String[size];

        // for each n of the output list
        for (int n = 0; n < size; ++n) {
            // for each i of the input list
            for (int i = 0; i < size; ++i) {
                int row_index = cols.intAt(i);
                int seq_no = ((BigNumber) dt.get(2, row_index).getObject()).intValue();
                if (seq_no == n) {
                    list[n] = dt.get(1, row_index).getObject().toString();
                    break;
                }
            }
        }

        return list;
    }

    /**
     * Convenience, generates a unique constraint name.  If the given constraint
     * name is 'null' then a new one is created, otherwise the given default
     * one is returned.
     */
    private static String makeUniqueConstraintName(String name,
                                                   BigNumber unique_id) {
        if (name == null) {
            name = "_ANONYMOUS_CONSTRAINT_" + unique_id.toString();
        }
        return name;
    }


    /**
     * Notifies this transaction that a database object with the given name has
     * successfully been created.
     */
    void databaseObjectCreated(TableName table_name) {
        // If this table name was dropped, then remove from the drop list
        boolean dropped = dropped_database_objects.remove(table_name);
        // If the above operation didn't remove a table name then add to the
        // created database objects list.
        if (!dropped) {
            created_database_objects.add(table_name);
        }
    }

    /**
     * Notifies this transaction that a database object with the given name has
     * successfully been dropped.
     */
    void databaseObjectDropped(TableName table_name) {
        // If this table name was created, then remove from the create list
        boolean created = created_database_objects.remove(table_name);
        // If the above operation didn't remove a table name then add to the
        // dropped database objects list.
        if (!created) {
            dropped_database_objects.add(table_name);
        }
    }

    /**
     * Returns the normalized list of database object names created in this
     * transaction.
     */
    ArrayList getAllNamesCreated() {
        return created_database_objects;
    }

    /**
     * Returns the normalized list of database object names dropped in this
     * transaction.
     */
    ArrayList getAllNamesDropped() {
        return dropped_database_objects;
    }


    /**
     * Create a new schema in this transaction.  When the transaction is
     * committed the schema will become globally accessable.  Note that any
     * security checks must be performed before this method is called.
     * <p>
     * NOTE: We must guarentee that the transaction be in exclusive mode before
     *   this method is called.
     */
    public void createSchema(String name, String type) {
        TableName table_name = TableDataConglomerate.SCHEMA_INFO_TABLE;
        MutableTableDataSource t = getTable(table_name);
        SimpleTableQuery dt = new SimpleTableQuery(t);

        try {
            // Select entries where;
            //     SchemaInfo.name = name
            if (!dt.existsSingle(1, name)) {
                // Add the entry to the schema info table.
                RowData rd = new RowData(t);
                BigNumber unique_id = BigNumber.fromLong(nextUniqueID(table_name));
                rd.setColumnDataFromObject(0, unique_id);
                rd.setColumnDataFromObject(1, name);
                rd.setColumnDataFromObject(2, type);
                // Third (other) column is left as null
                t.addRow(rd);
            } else {
                throw new StatementException("Schema already exists: " + name);
            }
        } finally {
            dt.dispose();
        }
    }

    /**
     * Drops a schema from this transaction.  When the transaction is committed
     * the schema will be dropped perminently.  Note that any security checks
     * must be performed before this method is called.
     * <p>
     * NOTE: We must guarentee that the transaction be in exclusive mode before
     *   this method is called.
     */
    public void dropSchema(String name) {
        TableName table_name = TableDataConglomerate.SCHEMA_INFO_TABLE;
        MutableTableDataSource t = getTable(table_name);
        SimpleTableQuery dt = new SimpleTableQuery(t);

        // Drop a single entry from dt where column 1 = name
        boolean b = dt.deleteSingle(1, name);
        dt.dispose();
        if (!b) {
            throw new StatementException("Schema doesn't exists: " + name);
        }
    }

    /**
     * Returns true if the schema exists within this transaction.
     */
    public boolean schemaExists(String name) {
        TableName table_name = TableDataConglomerate.SCHEMA_INFO_TABLE;
        MutableTableDataSource t = getTable(table_name);
        SimpleTableQuery dt = new SimpleTableQuery(t);

        // Returns true if there's a single entry in dt where column 1 = name
        boolean b = dt.existsSingle(1, name);
        dt.dispose();
        return b;
    }

    /**
     * Resolves the case of the given schema name if the database is performing
     * case insensitive identifier matching.  Returns a SchemaDef object that
     * identifiers the schema.  Returns null if the schema name could not be
     * resolved.
     */
    public SchemaDef resolveSchemaCase(String name, boolean ignore_case) {
        // The list of schema
        SimpleTableQuery dt = new SimpleTableQuery(
                getTable(TableDataConglomerate.SCHEMA_INFO_TABLE));

        try {
            RowEnumeration e = dt.rowEnumeration();
            if (ignore_case) {
                SchemaDef result = null;
                while (e.hasMoreRows()) {
                    int row_index = e.nextRowIndex();
                    String cur_name = dt.get(1, row_index).getObject().toString();
                    if (name.equalsIgnoreCase(cur_name)) {
                        if (result != null) {
                            throw new StatementException(
                                    "Ambiguous schema name: '" + name + "'");
                        }
                        String type = dt.get(2, row_index).getObject().toString();
                        result = new SchemaDef(cur_name, type);
                    }
                }
                return result;

            } else {  // if (!ignore_case)
                while (e.hasMoreRows()) {
                    int row_index = e.nextRowIndex();
                    String cur_name = dt.get(1, row_index).getObject().toString();
                    if (name.equals(cur_name)) {
                        String type = dt.get(2, row_index).getObject().toString();
                        return new SchemaDef(cur_name, type);
                    }
                }
                // Not found
                return null;
            }
        } finally {
            dt.dispose();
        }

    }

    /**
     * Returns an array of SchemaDef objects for each schema currently setup in
     * the database.
     */
    public SchemaDef[] getSchemaList() {
        // The list of schema
        SimpleTableQuery dt = new SimpleTableQuery(
                getTable(TableDataConglomerate.SCHEMA_INFO_TABLE));
        RowEnumeration e = dt.rowEnumeration();
        SchemaDef[] arr = new SchemaDef[dt.getRowCount()];
        int i = 0;

        while (e.hasMoreRows()) {
            int row_index = e.nextRowIndex();
            String cur_name = dt.get(1, row_index).getObject().toString();
            String cur_type = dt.get(2, row_index).getObject().toString();
            arr[i] = new SchemaDef(cur_name, cur_type);
            ++i;
        }

        dt.dispose();
        return arr;
    }


    /**
     * Sets a persistent variable of the database that becomes a committed
     * change once this transaction is committed.  The variable can later be
     * retrieved with a call to the 'getPersistantVar' method.  A persistant
     * var is created if it doesn't exist in the DatabaseVars table otherwise
     * it is overwritten.
     */
    public void setPersistentVar(String variable, String value) {
        TableName table_name = TableDataConglomerate.PERSISTENT_VAR_TABLE;
        MutableTableDataSource t = getTable(table_name);
        SimpleTableQuery dt = new SimpleTableQuery(t);
        dt.setVar(0, new Object[]{variable, value});
        dt.dispose();
    }

    /**
     * Returns the value of the persistent variable with the given name or null
     * if it doesn't exist.
     */
    public String getPersistantVar(String variable) {
        TableName table_name = TableDataConglomerate.PERSISTENT_VAR_TABLE;
        MutableTableDataSource t = getTable(table_name);
        SimpleTableQuery dt = new SimpleTableQuery(t);
        String val = dt.getVar(1, 0, variable).toString();
        dt.dispose();
        return val;
    }

    /**
     * Creates a new sequence generator with the given TableName and
     * initializes it with the given details.  This does NOT check if the
     * given name clashes with an existing database object.
     */
    public void createSequenceGenerator(
            TableName name, long start_value, long increment_by,
            long min_value, long max_value, long cache, boolean cycle) {
        SequenceManager.createSequenceGenerator(this,
                name, start_value, increment_by, min_value, max_value, cache,
                cycle);

        // Notify that this database object has been created
        databaseObjectCreated(name);
    }

    /**
     * Drops an existing sequence generator with the given name.
     */
    public void dropSequenceGenerator(TableName name) {
        SequenceManager.dropSequenceGenerator(this, name);
        // Flush the sequence manager
        flushSequenceManager(name);

        // Notify that this database object has been dropped
        databaseObjectDropped(name);
    }

    /**
     * Adds a unique constraint to the database which becomes perminant when
     * the transaction is committed.  Columns in a table that are defined as
     * unique are prevented from being duplicated by the engine.
     * <p>
     * NOTE: Security checks for adding constraints must be checked for at a
     *   higher layer.
     * <p>
     * NOTE: We must guarentee that the transaction be in exclusive mode before
     *   this method is called.
     */
    public void addUniqueConstraint(TableName table_name,
                                    String[] cols, short deferred, String constraint_name) {

        TableName tn1 = TableDataConglomerate.UNIQUE_INFO_TABLE;
        TableName tn2 = TableDataConglomerate.UNIQUE_COLS_TABLE;
        MutableTableDataSource t = getTable(tn1);
        MutableTableDataSource tcols = getTable(tn2);

        try {

            // Insert a value into UNIQUE_INFO_TABLE
            RowData rd = new RowData(t);
            BigNumber unique_id = BigNumber.fromLong(nextUniqueID(tn1));
            constraint_name = makeUniqueConstraintName(constraint_name, unique_id);
            rd.setColumnDataFromObject(0, unique_id);
            rd.setColumnDataFromObject(1, constraint_name);
            rd.setColumnDataFromObject(2, table_name.getSchema());
            rd.setColumnDataFromObject(3, table_name.getName());
            rd.setColumnDataFromObject(4, BigNumber.fromInt(deferred));
            t.addRow(rd);

            // Insert the columns
            for (int i = 0; i < cols.length; ++i) {
                rd = new RowData(tcols);
                rd.setColumnDataFromObject(0, unique_id);            // unique id
                rd.setColumnDataFromObject(1, cols[i]);              // column name
                rd.setColumnDataFromObject(2, BigNumber.fromInt(i)); // sequence number
                tcols.addRow(rd);
            }

        } catch (DatabaseConstraintViolationException e) {
            // Constraint violation when inserting the data.  Check the type and
            // wrap around an appropriate error message.
            if (e.getErrorCode() ==
                    DatabaseConstraintViolationException.UNIQUE_VIOLATION) {
                // This means we gave a constraint name that's already being used
                // for a primary key.
                throw new StatementException(
                        "Unique constraint name '" + constraint_name +
                                "' is already being used.");
            }
            throw e;
        }

    }

    /**
     * Adds a foreign key constraint to the database which becomes perminent
     * when the transaction is committed.  A foreign key represents a referential
     * link from one table to another (may be the same table).  The 'table_name',
     * 'cols' args represents the object to link from.  The 'ref_table',
     * 'ref_cols' args represents the object to link to.  The update rules are
     * for specifying cascading delete/update rules.  The deferred arg is for
     * IMMEDIATE/DEFERRED checking.
     * <p>
     * NOTE: Security checks for adding constraints must be checked for at a
     *   higher layer.
     * <p>
     * NOTE: We must guarentee that the transaction be in exclusive mode before
     *   this method is called.
     */
    public void addForeignKeyConstraint(TableName table, String[] cols,
                                        TableName ref_table, String[] ref_cols,
                                        String delete_rule, String update_rule,
                                        short deferred, String constraint_name) {
        TableName tn1 = TableDataConglomerate.FOREIGN_INFO_TABLE;
        TableName tn2 = TableDataConglomerate.FOREIGN_COLS_TABLE;
        MutableTableDataSource t = getTable(tn1);
        MutableTableDataSource tcols = getTable(tn2);

        try {

            // If 'ref_columns' empty then set to primary key for referenced table,
            // ISSUE: What if primary key changes after the fact?
            if (ref_cols.length == 0) {
                ColumnGroup set = queryTablePrimaryKeyGroup(this, ref_table);
                if (set == null) {
                    throw new StatementException(
                            "No primary key defined for referenced table '" + ref_table + "'");
                }
                ref_cols = set.columns;
            }

            if (cols.length != ref_cols.length) {
                throw new StatementException("Foreign key reference '" + table +
                        "' -> '" + ref_table + "' does not have an equal number of " +
                        "column terms.");
            }

            // If delete or update rule is 'SET NULL' then check the foreign key
            // columns are not constrained as 'NOT NULL'
            if (delete_rule.equals("SET NULL") ||
                    update_rule.equals("SET NULL")) {
                DataTableDef table_def = getDataTableDef(table);
                for (int i = 0; i < cols.length; ++i) {
                    DataTableColumnDef column_def =
                            table_def.columnAt(table_def.findColumnName(cols[i]));
                    if (column_def.isNotNull()) {
                        throw new StatementException("Foreign key reference '" + table +
                                "' -> '" + ref_table + "' update or delete triggered " +
                                "action is SET NULL for columns that are constrained as " +
                                "NOT NULL.");
                    }
                }
            }

            // Insert a value into FOREIGN_INFO_TABLE
            RowData rd = new RowData(t);
            BigNumber unique_id = BigNumber.fromLong(nextUniqueID(tn1));
            constraint_name = makeUniqueConstraintName(constraint_name, unique_id);
            rd.setColumnDataFromObject(0, unique_id);
            rd.setColumnDataFromObject(1, constraint_name);
            rd.setColumnDataFromObject(2, table.getSchema());
            rd.setColumnDataFromObject(3, table.getName());
            rd.setColumnDataFromObject(4, ref_table.getSchema());
            rd.setColumnDataFromObject(5, ref_table.getName());
            rd.setColumnDataFromObject(6, update_rule);
            rd.setColumnDataFromObject(7, delete_rule);
            rd.setColumnDataFromObject(8, BigNumber.fromInt(deferred));
            t.addRow(rd);

            // Insert the columns
            for (int i = 0; i < cols.length; ++i) {
                rd = new RowData(tcols);
                rd.setColumnDataFromObject(0, unique_id);            // unique id
                rd.setColumnDataFromObject(1, cols[i]);              // column name
                rd.setColumnDataFromObject(2, ref_cols[i]);          // ref column name
                rd.setColumnDataFromObject(3, BigNumber.fromInt(i)); // sequence number
                tcols.addRow(rd);
            }

        } catch (DatabaseConstraintViolationException e) {
            // Constraint violation when inserting the data.  Check the type and
            // wrap around an appropriate error message.
            if (e.getErrorCode() ==
                    DatabaseConstraintViolationException.UNIQUE_VIOLATION) {
                // This means we gave a constraint name that's already being used
                // for a primary key.
                throw new StatementException("Foreign key constraint name '" +
                        constraint_name + "' is already being used.");
            }
            throw e;
        }

    }

    /**
     * Adds a primary key constraint that becomes perminent when the transaction
     * is committed.  A primary key represents a set of columns in a table
     * that are constrained to be unique and can not be null.  If the
     * constraint name parameter is 'null' a primary key constraint is created
     * with a unique constraint name.
     * <p>
     * NOTE: Security checks for adding constraints must be checked for at a
     *   higher layer.
     * <p>
     * NOTE: We must guarentee that the transaction be in exclusive mode before
     *   this method is called.
     */
    public void addPrimaryKeyConstraint(TableName table_name, String[] cols,
                                        short deferred, String constraint_name) {

        TableName tn1 = TableDataConglomerate.PRIMARY_INFO_TABLE;
        TableName tn2 = TableDataConglomerate.PRIMARY_COLS_TABLE;
        MutableTableDataSource t = getTable(tn1);
        MutableTableDataSource tcols = getTable(tn2);

        try {

            // Insert a value into PRIMARY_INFO_TABLE
            RowData rd = new RowData(t);
            BigNumber unique_id = BigNumber.fromLong(nextUniqueID(tn1));
            constraint_name = makeUniqueConstraintName(constraint_name, unique_id);
            rd.setColumnDataFromObject(0, unique_id);
            rd.setColumnDataFromObject(1, constraint_name);
            rd.setColumnDataFromObject(2, table_name.getSchema());
            rd.setColumnDataFromObject(3, table_name.getName());
            rd.setColumnDataFromObject(4, BigNumber.fromInt(deferred));
            t.addRow(rd);

            // Insert the columns
            for (int i = 0; i < cols.length; ++i) {
                rd = new RowData(tcols);
                rd.setColumnDataFromObject(0, unique_id);            // unique id
                rd.setColumnDataFromObject(1, cols[i]);              // column name
                rd.setColumnDataFromObject(2, BigNumber.fromInt(i)); // Sequence number
                tcols.addRow(rd);
            }

        } catch (DatabaseConstraintViolationException e) {
            // Constraint violation when inserting the data.  Check the type and
            // wrap around an appropriate error message.
            if (e.getErrorCode() ==
                    DatabaseConstraintViolationException.UNIQUE_VIOLATION) {
                // This means we gave a constraint name that's already being used
                // for a primary key.
                throw new StatementException("Primary key constraint name '" +
                        constraint_name + "' is already being used.");
            }
            throw e;
        }

    }

    /**
     * Adds a check expression that becomes perminent when the transaction
     * is committed.  A check expression is an expression that must evaluate
     * to true for all records added/updated in the database.
     * <p>
     * NOTE: Security checks for adding constraints must be checked for at a
     *   higher layer.
     * <p>
     * NOTE: We must guarentee that the transaction be in exclusive mode before
     *   this method is called.
     */
    public void addCheckConstraint(TableName table_name,
                                   Expression expression, short deferred, String constraint_name) {

        TableName tn = TableDataConglomerate.CHECK_INFO_TABLE;
        MutableTableDataSource t = getTable(tn);
        int col_count = t.getDataTableDef().columnCount();

        try {

            // Insert check constraint data.
            BigNumber unique_id = BigNumber.fromLong(nextUniqueID(tn));
            constraint_name = makeUniqueConstraintName(constraint_name, unique_id);
            RowData rd = new RowData(t);
            rd.setColumnDataFromObject(0, unique_id);
            rd.setColumnDataFromObject(1, constraint_name);
            rd.setColumnDataFromObject(2, table_name.getSchema());
            rd.setColumnDataFromObject(3, table_name.getName());
            rd.setColumnDataFromObject(4, new String(expression.text()));
            rd.setColumnDataFromObject(5, BigNumber.fromInt(deferred));
            if (col_count > 6) {
                // Serialize the check expression
                ByteLongObject serialized_expression =
                        ObjectTranslator.serialize(expression);
                rd.setColumnDataFromObject(6, serialized_expression);
            }
            t.addRow(rd);

        } catch (DatabaseConstraintViolationException e) {
            // Constraint violation when inserting the data.  Check the type and
            // wrap around an appropriate error message.
            if (e.getErrorCode() ==
                    DatabaseConstraintViolationException.UNIQUE_VIOLATION) {
                // This means we gave a constraint name that's already being used.
                throw new StatementException("Check constraint name '" +
                        constraint_name + "' is already being used.");
            }
            throw e;
        }

    }

    /**
     * Drops all the constraints defined for the given table.  This is a useful
     * function when dropping a table from the database.
     * <p>
     * NOTE: Security checks that the user can drop constraints must be checke at
     *   a higher layer.
     * <p>
     * NOTE: We must guarentee that the transaction be in exclusive mode before
     *   this method is called.
     */
    public void dropAllConstraintsForTable(TableName table_name) {
        ColumnGroup primary = queryTablePrimaryKeyGroup(this, table_name);
        ColumnGroup[] uniques = queryTableUniqueGroups(this, table_name);
        CheckExpression[] expressions =
                queryTableCheckExpressions(this, table_name);
        ColumnGroupReference[] refs =
                queryTableForeignKeyReferences(this, table_name);

        if (primary != null) {
            dropPrimaryKeyConstraintForTable(table_name, primary.name);
        }
        for (int i = 0; i < uniques.length; ++i) {
            dropUniqueConstraintForTable(table_name, uniques[i].name);
        }
        for (int i = 0; i < expressions.length; ++i) {
            dropCheckConstraintForTable(table_name, expressions[i].name);
        }
        for (int i = 0; i < refs.length; ++i) {
            dropForeignKeyReferenceConstraintForTable(table_name, refs[i].name);
        }

    }

    /**
     * Drops the named constraint from the transaction.  Used when altering
     * table schema.  Returns the number of constraints that were removed from
     * the system.  If this method returns 0 then it indicates there is no
     * constraint with the given name in the table.
     * <p>
     * NOTE: Security checks that the user can drop constraints must be checke at
     *   a higher layer.
     * <p>
     * NOTE: We must guarentee that the transaction be in exclusive mode before
     *   this method is called.
     */
    public int dropNamedConstraint(TableName table_name,
                                   String constraint_name) {

        int drop_count = 0;
        if (dropPrimaryKeyConstraintForTable(table_name, constraint_name)) {
            ++drop_count;
        }
        if (dropUniqueConstraintForTable(table_name, constraint_name)) {
            ++drop_count;
        }
        if (dropCheckConstraintForTable(table_name, constraint_name)) {
            ++drop_count;
        }
        if (dropForeignKeyReferenceConstraintForTable(table_name,
                constraint_name)) {
            ++drop_count;
        }
        return drop_count;
    }

    /**
     * Drops the primary key constraint for the given table.  Used when altering
     * table schema.  If 'constraint_name' is null this method will search for
     * the primary key of the table name.  Returns true if the primary key
     * constraint was dropped (the constraint existed).
     * <p>
     * NOTE: Security checks that the user can drop constraints must be checke at
     *   a higher layer.
     * <p>
     * NOTE: We must guarentee that the transaction be in exclusive mode before
     *   this method is called.
     */
    public boolean dropPrimaryKeyConstraintForTable(
            TableName table_name, String constraint_name) {

        MutableTableDataSource t =
                getTable(TableDataConglomerate.PRIMARY_INFO_TABLE);
        MutableTableDataSource t2 =
                getTable(TableDataConglomerate.PRIMARY_COLS_TABLE);
        SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
        SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

        try {
            IntegerVector data;
            if (constraint_name != null) {
                // Returns the list of indexes where column 1 = constraint name
                //                               and column 2 = schema name
                data = dt.selectIndexesEqual(1, constraint_name,
                        2, table_name.getSchema());
            } else {
                // Returns the list of indexes where column 3 = table name
                //                               and column 2 = schema name
                data = dt.selectIndexesEqual(3, table_name.getName(),
                        2, table_name.getSchema());
            }

            if (data.size() > 1) {
                throw new Error("Assertion failed: multiple primary key for: " +
                        table_name);
            } else if (data.size() == 1) {
                int row_index = data.intAt(0);
                // The id
                TObject id = dt.get(0, row_index);
                // All columns with this id
                IntegerVector ivec = dtcols.selectIndexesEqual(0, id);
                // Delete from the table
                dtcols.deleteRows(ivec);
                dt.deleteRows(data);
                return true;
            }
            // data.size() must be 0 so no constraint was found to drop.
            return false;

        } finally {
            dtcols.dispose();
            dt.dispose();
        }

    }

    /**
     * Drops a single named unique constraint from the given table.  Returns
     * true if the unique constraint was dropped (the constraint existed).
     * <p>
     * NOTE: Security checks that the user can drop constraints must be checke at
     *   a higher layer.
     * <p>
     * NOTE: We must guarentee that the transaction be in exclusive mode before
     *   this method is called.
     */
    public boolean dropUniqueConstraintForTable(
            TableName table, String constraint_name) {

        MutableTableDataSource t =
                getTable(TableDataConglomerate.UNIQUE_INFO_TABLE);
        MutableTableDataSource t2 =
                getTable(TableDataConglomerate.UNIQUE_COLS_TABLE);
        SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
        SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

        try {
            // Returns the list of indexes where column 1 = constraint name
            //                               and column 2 = schema name
            IntegerVector data = dt.selectIndexesEqual(1, constraint_name,
                    2, table.getSchema());

            if (data.size() > 1) {
                throw new Error("Assertion failed: multiple unique constraint name: " +
                        constraint_name);
            } else if (data.size() == 1) {
                int row_index = data.intAt(0);
                // The id
                TObject id = dt.get(0, row_index);
                // All columns with this id
                IntegerVector ivec = dtcols.selectIndexesEqual(0, id);
                // Delete from the table
                dtcols.deleteRows(ivec);
                dt.deleteRows(data);
                return true;
            }
            // data.size() == 0 so the constraint wasn't found
            return false;
        } finally {
            dtcols.dispose();
            dt.dispose();
        }

    }

    /**
     * Drops a single named check constraint from the given table.  Returns true
     * if the check constraint was dropped (the constraint existed).
     * <p>
     * NOTE: Security checks that the user can drop constraints must be checke at
     *   a higher layer.
     * <p>
     * NOTE: We must guarentee that the transaction be in exclusive mode before
     *   this method is called.
     */
    public boolean dropCheckConstraintForTable(
            TableName table, String constraint_name) {

        MutableTableDataSource t =
                getTable(TableDataConglomerate.CHECK_INFO_TABLE);
        SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table

        try {
            // Returns the list of indexes where column 1 = constraint name
            //                               and column 2 = schema name
            IntegerVector data = dt.selectIndexesEqual(1, constraint_name,
                    2, table.getSchema());

            if (data.size() > 1) {
                throw new Error("Assertion failed: multiple check constraint name: " +
                        constraint_name);
            } else if (data.size() == 1) {
                // Delete the check constraint
                dt.deleteRows(data);
                return true;
            }
            // data.size() == 0 so the constraint wasn't found
            return false;
        } finally {
            dt.dispose();
        }

    }

    /**
     * Drops a single named foreign key reference from the given table.  Returns
     * true if the foreign key reference constraint was dropped (the constraint
     * existed).
     * <p>
     * NOTE: Security checks that the user can drop constraints must be checke at
     *   a higher layer.
     * <p>
     * NOTE: We must guarentee that the transaction be in exclusive mode before
     *   this method is called.
     */
    public boolean dropForeignKeyReferenceConstraintForTable(
            TableName table, String constraint_name) {

        MutableTableDataSource t =
                getTable(TableDataConglomerate.FOREIGN_INFO_TABLE);
        MutableTableDataSource t2 =
                getTable(TableDataConglomerate.FOREIGN_COLS_TABLE);
        SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
        SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

        try {
            // Returns the list of indexes where column 1 = constraint name
            //                               and column 2 = schema name
            IntegerVector data = dt.selectIndexesEqual(1, constraint_name,
                    2, table.getSchema());

            if (data.size() > 1) {
                throw new Error("Assertion failed: multiple foreign key constraint " +
                        "name: " + constraint_name);
            } else if (data.size() == 1) {
                int row_index = data.intAt(0);
                // The id
                TObject id = dt.get(0, row_index);
                // All columns with this id
                IntegerVector ivec = dtcols.selectIndexesEqual(0, id);
                // Delete from the table
                dtcols.deleteRows(ivec);
                dt.deleteRows(data);
                return true;
            }
            // data.size() == 0 so the constraint wasn't found
            return false;
        } finally {
            dtcols.dispose();
            dt.dispose();
        }

    }

    /**
     * Returns the list of tables (as a TableName array) that are dependant
     * on the data in the given table to maintain referential consistancy.  The
     * list includes the tables referenced as foreign keys, and the tables
     * that reference the table as a foreign key.
     * <p>
     * This is a useful query for determining ahead of time the tables that
     * require a read lock when inserting/updating a table.  A table will require
     * a read lock if the operation needs to query it for potential referential
     * integrity violations.
     */
    public static TableName[] queryTablesRelationallyLinkedTo(
            SimpleTransaction transaction, TableName table) {
        ArrayList list = new ArrayList();
        ColumnGroupReference[] refs =
                queryTableForeignKeyReferences(transaction, table);
        for (int i = 0; i < refs.length; ++i) {
            TableName tname = refs[i].ref_table_name;
            if (!list.contains(tname)) {
                list.add(tname);
            }
        }
        refs = queryTableImportedForeignKeyReferences(transaction, table);
        for (int i = 0; i < refs.length; ++i) {
            TableName tname = refs[i].key_table_name;
            if (!list.contains(tname)) {
                list.add(tname);
            }
        }
        return (TableName[]) list.toArray(new TableName[list.size()]);
    }

    /**
     * Returns a set of unique groups that are constrained to be unique for
     * the given table in this transaction.  For example, if columns ('name')
     * and ('number', 'document_rev') are defined as unique, this will return
     * an array of two groups that represent unique columns in the given
     * table.
     */
    public static ColumnGroup[] queryTableUniqueGroups(
            SimpleTransaction transaction, TableName table_name) {
        TableDataSource t =
                transaction.getTableDataSource(TableDataConglomerate.UNIQUE_INFO_TABLE);
        TableDataSource t2 =
                transaction.getTableDataSource(TableDataConglomerate.UNIQUE_COLS_TABLE);
        SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
        SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

        ColumnGroup[] groups;
        try {
            // Returns the list indexes where column 3 = table name
            //                            and column 2 = schema name
            IntegerVector data = dt.selectIndexesEqual(3, table_name.getName(),
                    2, table_name.getSchema());
            groups = new ColumnGroup[data.size()];

            for (int i = 0; i < data.size(); ++i) {
                TObject id = dt.get(0, data.intAt(i));

                // Select all records with equal id
                IntegerVector cols = dtcols.selectIndexesEqual(0, id);

                // Put into a group.
                ColumnGroup group = new ColumnGroup();
                // constraint name
                group.name = dt.get(1, data.intAt(i)).getObject().toString();
                group.columns = toColumns(dtcols, cols);   // the list of columns
                group.deferred = ((BigNumber) dt.get(4,
                        data.intAt(i)).getObject()).shortValue();
                groups[i] = group;
            }
        } finally {
            dt.dispose();
            dtcols.dispose();
        }

        return groups;
    }

    /**
     * Returns a set of primary key groups that are constrained to be unique
     * for the given table in this transaction (there can be only 1 primary
     * key defined for a table).  Returns null if there is no primary key
     * defined for the table.
     */
    public static ColumnGroup queryTablePrimaryKeyGroup(
            SimpleTransaction transaction, TableName table_name) {
        TableDataSource t =
                transaction.getTableDataSource(TableDataConglomerate.PRIMARY_INFO_TABLE);
        TableDataSource t2 =
                transaction.getTableDataSource(TableDataConglomerate.PRIMARY_COLS_TABLE);
        SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
        SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

        try {
            // Returns the list indexes where column 3 = table name
            //                            and column 2 = schema name
            IntegerVector data = dt.selectIndexesEqual(3, table_name.getName(),
                    2, table_name.getSchema());

            if (data.size() > 1) {
                throw new Error("Assertion failed: multiple primary key for: " +
                        table_name);
            } else if (data.size() == 1) {
                int row_index = data.intAt(0);
                // The id
                TObject id = dt.get(0, row_index);
                // All columns with this id
                IntegerVector ivec = dtcols.selectIndexesEqual(0, id);
                // Make it in to a columns object
                ColumnGroup group = new ColumnGroup();
                group.name = dt.get(1, row_index).getObject().toString();
                group.columns = toColumns(dtcols, ivec);
                group.deferred = ((BigNumber) dt.get(4,
                        row_index).getObject()).shortValue();
                return group;
            } else {
                return null;
            }
        } finally {
            dt.dispose();
            dtcols.dispose();
        }

    }

    /**
     * Returns a set of check expressions that are constrained over all new
     * columns added to the given table in this transaction.  For example,
     * we may want a column called 'serial_number' to be constrained as
     * CHECK serial_number LIKE '___-________-___'.
     */
    public static CheckExpression[] queryTableCheckExpressions(
            SimpleTransaction transaction, TableName table_name) {
        TableDataSource t =
                transaction.getTableDataSource(TableDataConglomerate.CHECK_INFO_TABLE);
        SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table

        CheckExpression[] checks;
        try {
            // Returns the list indexes where column 3 = table name
            //                            and column 2 = schema name
            IntegerVector data = dt.selectIndexesEqual(3, table_name.getName(),
                    2, table_name.getSchema());
            checks = new CheckExpression[data.size()];

            for (int i = 0; i < checks.length; ++i) {
                int row_index = data.intAt(i);

                CheckExpression check = new CheckExpression();
                check.name = dt.get(1, row_index).getObject().toString();
                check.deferred = ((BigNumber) dt.get(5,
                        row_index).getObject()).shortValue();
                // Is the deserialized version available?
                if (t.getDataTableDef().columnCount() > 6) {
                    ByteLongObject sexp =
                            (ByteLongObject) dt.get(6, row_index).getObject();
                    if (sexp != null) {
                        try {
                            // Deserialize the expression
                            check.expression =
                                    (Expression) ObjectTranslator.deserialize(sexp);
                        } catch (Throwable e) {
                            // We weren't able to deserialize the expression so report the
                            // error to the log
                            transaction.Debug().write(Lvl.WARNING, Transaction.class,
                                    "Unable to deserialize the check expression.  " +
                                            "The error is: " + e.getMessage());
                            transaction.Debug().write(Lvl.WARNING, Transaction.class,
                                    "Parsing the check expression instead.");
                            check.expression = null;
                        }
                    }
                }
                // Otherwise we need to parse it from the string
                if (check.expression == null) {
                    Expression exp = Expression.parse(
                            dt.get(4, row_index).getObject().toString());
                    check.expression = exp;
                }
                checks[i] = check;
            }

        } finally {
            dt.dispose();
        }

        return checks;
    }

    /**
     * Returns an array of column references in the given table that represent
     * foreign key references.  For example, say a foreign reference has been
     * set up in the given table as follows;<p><pre>
     *   FOREIGN KEY (customer_id) REFERENCES Customer (id)
     * </pre><p>
     * This method will return the column group reference
     * Order(customer_id) -> Customer(id).
     * <p>
     * This method is used to check that a foreign key reference actually points
     * to a valid record in the referenced table as expected.
     */
    public static ColumnGroupReference[] queryTableForeignKeyReferences(
            SimpleTransaction transaction, TableName table_name) {

        TableDataSource t =
                transaction.getTableDataSource(TableDataConglomerate.FOREIGN_INFO_TABLE);
        TableDataSource t2 =
                transaction.getTableDataSource(TableDataConglomerate.FOREIGN_COLS_TABLE);
        SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
        SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

        ColumnGroupReference[] groups;
        try {
            // Returns the list indexes where column 3 = table name
            //                            and column 2 = schema name
            IntegerVector data = dt.selectIndexesEqual(3, table_name.getName(),
                    2, table_name.getSchema());
            groups = new ColumnGroupReference[data.size()];

            for (int i = 0; i < data.size(); ++i) {
                int row_index = data.intAt(i);

                // The foreign key id
                TObject id = dt.get(0, row_index);

                // The referenced table
                TableName ref_table_name = new TableName(
                        dt.get(4, row_index).getObject().toString(),
                        dt.get(5, row_index).getObject().toString());

                // Select all records with equal id
                IntegerVector cols = dtcols.selectIndexesEqual(0, id);

                // Put into a group.
                ColumnGroupReference group = new ColumnGroupReference();
                // constraint name
                group.name = dt.get(1, row_index).getObject().toString();
                group.key_table_name = table_name;
                group.ref_table_name = ref_table_name;
                group.update_rule = dt.get(6, row_index).getObject().toString();
                group.delete_rule = dt.get(7, row_index).getObject().toString();
                group.deferred = ((BigNumber) dt.get(8,
                        row_index).getObject()).shortValue();

                int cols_size = cols.size();
                String[] key_cols = new String[cols_size];
                String[] ref_cols = new String[cols_size];
                for (int n = 0; n < cols_size; ++n) {
                    for (int p = 0; p < cols_size; ++p) {
                        int cols_index = cols.intAt(p);
                        if (((BigNumber) dtcols.get(3,
                                cols_index).getObject()).intValue() == n) {
                            key_cols[n] = dtcols.get(1, cols_index).getObject().toString();
                            ref_cols[n] = dtcols.get(2, cols_index).getObject().toString();
                            break;
                        }
                    }
                }
                group.key_columns = key_cols;
                group.ref_columns = ref_cols;

                groups[i] = group;
            }
        } finally {
            dt.dispose();
            dtcols.dispose();
        }

        return groups;
    }

    /**
     * Returns an array of column references in the given table that represent
     * foreign key references that reference columns in the given table.  This
     * is a reverse mapping of the 'queryTableForeignKeyReferences' method.  For
     * example, say a foreign reference has been set up in any table as follows;
     * <p><pre>
     *   [ In table Order ]
     *   FOREIGN KEY (customer_id) REFERENCE Customer (id)
     * </pre><p>
     * And the table name we are querying is 'Customer' then this method will
     * return the column group reference
     * Order(customer_id) -> Customer(id).
     * <p>
     * This method is used to check that a reference isn't broken when we remove
     * a record (for example, removing a Customer that has references to it will
     * break integrity).
     */
    public static ColumnGroupReference[] queryTableImportedForeignKeyReferences(
            SimpleTransaction transaction, TableName ref_table_name) {

        TableDataSource t =
                transaction.getTableDataSource(TableDataConglomerate.FOREIGN_INFO_TABLE);
        TableDataSource t2 =
                transaction.getTableDataSource(TableDataConglomerate.FOREIGN_COLS_TABLE);
        SimpleTableQuery dt = new SimpleTableQuery(t);        // The info table
        SimpleTableQuery dtcols = new SimpleTableQuery(t2);   // The columns

        ColumnGroupReference[] groups;
        try {
            // Returns the list indexes where column 5 = ref table name
            //                            and column 4 = ref schema name
            IntegerVector data = dt.selectIndexesEqual(5, ref_table_name.getName(),
                    4, ref_table_name.getSchema());
            groups = new ColumnGroupReference[data.size()];

            for (int i = 0; i < data.size(); ++i) {
                int row_index = data.intAt(i);

                // The foreign key id
                TObject id = dt.get(0, row_index);

                // The referencee table
                TableName table_name = new TableName(
                        dt.get(2, row_index).getObject().toString(),
                        dt.get(3, row_index).getObject().toString());

                // Select all records with equal id
                IntegerVector cols = dtcols.selectIndexesEqual(0, id);

                // Put into a group.
                ColumnGroupReference group = new ColumnGroupReference();
                // constraint name
                group.name = dt.get(1, row_index).getObject().toString();
                group.key_table_name = table_name;
                group.ref_table_name = ref_table_name;
                group.update_rule = dt.get(6, row_index).getObject().toString();
                group.delete_rule = dt.get(7, row_index).getObject().toString();
                group.deferred = ((BigNumber) dt.get(8,
                        row_index).getObject()).shortValue();

                int cols_size = cols.size();
                String[] key_cols = new String[cols_size];
                String[] ref_cols = new String[cols_size];
                for (int n = 0; n < cols_size; ++n) {
                    for (int p = 0; p < cols_size; ++p) {
                        int cols_index = cols.intAt(p);
                        if (((BigNumber) dtcols.get(3,
                                cols_index).getObject()).intValue() == n) {
                            key_cols[n] = dtcols.get(1, cols_index).getObject().toString();
                            ref_cols[n] = dtcols.get(2, cols_index).getObject().toString();
                            break;
                        }
                    }
                }
                group.key_columns = key_cols;
                group.ref_columns = ref_cols;

                groups[i] = group;
            }
        } finally {
            dt.dispose();
            dtcols.dispose();
        }

        return groups;
    }


    // ----- Transaction close operations -----

    /**
     * Closes and marks a transaction as committed.  Any changes made by this
     * transaction are seen by all transactions created after this method
     * returns.
     * <p>
     * This method will fail under the following circumstances:
     * <ol>
     * <li> There are any rows deleted in this transaction that were deleted
     *  by another successfully committed transaction.
     * <li> There were rows added in another committed transaction that would
     *  change the result of the search clauses committed by this transaction.
     * </ol>
     * The first check is not too difficult to check for.  The second is very
     * difficult however we need it to ensure TRANSACTION_SERIALIZABLE isolation
     * is enforced.  We may have to simplify this by throwing a transaction
     * exception if the table has had any changes made to it during this
     * transaction.
     * <p>
     * This should only be called under an exclusive lock on the connection.
     */
    public void closeAndCommit() throws TransactionException {

        if (!closed) {
            try {
                closed = true;
                // Get the conglomerate to do this commit.
                conglomerate.processCommit(this, getVisibleTables(),
                        selected_from_tables,
                        touched_tables, journal);
            } finally {
                cleanup();
            }
        }

    }

    /**
     * Closes and rolls back a transaction as if the commands the transaction ran
     * never happened.  This will not throw a transaction exception.
     * <p>
     * This should only be called under an exclusive lock on the connection.
     */
    public void closeAndRollback() {

        if (!closed) {
            try {
                closed = true;
                // Notify the conglomerate that this transaction has closed.
                conglomerate.processRollback(this, touched_tables, journal);
            } finally {
                cleanup();
            }
        }

    }

    /**
     * Cleans up this transaction.
     */
    private void cleanup() {
        getSystem().stats().decrement("Transaction.count");
        // Dispose of all the IndexSet objects created by this transaction.
        disposeAllIndices();

        // Dispose all the table we touched
        try {
            for (int i = 0; i < touched_tables.size(); ++i) {
                MutableTableDataSource source =
                        (MutableTableDataSource) touched_tables.get(i);
                source.dispose();
            }
        } catch (Throwable e) {
            Debug().writeException(e);
        }

        getSystem().stats().increment("Transaction.cleanup");
        conglomerate = null;
        touched_tables = null;
        journal = null;
    }

    /**
     * Disposes this transaction without rolling back or committing the changes.
     * Care should be taken when using this - it must only be used for simple
     * transactions that are short lived and have not modified the database.
     */
    void dispose() {
        if (!isReadOnly()) {
            throw new RuntimeException(
                    "Assertion failed - tried to dispose a non read-only transaction.");
        }
        if (!closed) {
            closed = true;
            cleanup();
        }
    }

    /**
     * Finalize, we should close the transaction.
     */
    public void finalize() throws Throwable {
        super.finalize();
        if (!closed) {
            Debug().write(Lvl.ERROR, this, "Transaction not closed!");
            closeAndRollback();
        }
    }


    // ---------- Transaction inner classes ----------

    /**
     * A list of DataTableDef system table definitions for tables internal to
     * the transaction.
     */
    private final static DataTableDef[] INTERNAL_DEF_LIST;

    static {
        INTERNAL_DEF_LIST = new DataTableDef[3];
        INTERNAL_DEF_LIST[0] = GTTableColumnsDataSource.DEF_DATA_TABLE_DEF;
        INTERNAL_DEF_LIST[1] = GTTableInfoDataSource.DEF_DATA_TABLE_DEF;
        INTERNAL_DEF_LIST[2] = GTProductDataSource.DEF_DATA_TABLE_DEF;
    }

    /**
     * A static internal table info for internal tables to the transaction.
     * This implementation includes all the dynamically generated system tables
     * that are tied to information in a transaction.
     */
    private class TransactionInternalTables extends AbstractInternalTableInfo {

        /**
         * Constructor.
         */
        public TransactionInternalTables() {
            super("SYSTEM TABLE", INTERNAL_DEF_LIST);
        }

        // ---------- Implemented ----------

        public MutableTableDataSource createInternalTable(int index) {
            if (index == 0) {
                return new GTTableColumnsDataSource(Transaction.this).init();
            } else if (index == 1) {
                return new GTTableInfoDataSource(Transaction.this).init();
            } else if (index == 2) {
                return new GTProductDataSource(Transaction.this).init();
            } else {
                throw new RuntimeException();
            }
        }

    }

    /**
     * A group of columns as used by the constraint system.  A ColumnGroup is
     * a simple list of columns in a table.
     */
    public static class ColumnGroup {

        /**
         * The name of the group (the constraint name).
         */
        public String name;

        /**
         * The list of columns that make up the group.
         */
        public String[] columns;

        /**
         * Whether this is deferred or initially immediate.
         */
        public short deferred;

    }

    /**
     * Represents a constraint expression to check.
     */
    public static class CheckExpression {

        /**
         * The name of the check expression (the constraint name).
         */
        public String name;

        /**
         * The expression to check.
         */
        public Expression expression;

        /**
         * Whether this is deferred or initially immediate.
         */
        public short deferred;

    }

    /**
     * Represents a reference from a group of columns in one table to a group of
     * columns in another table.  The is used to represent a foreign key
     * reference.
     */
    public static class ColumnGroupReference {

        /**
         * The name of the group (the constraint name).
         */
        public String name;

        /**
         * The key table name.
         */
        public TableName key_table_name;

        /**
         * The list of columns that make up the key.
         */
        public String[] key_columns;

        /**
         * The referenced table name.
         */
        public TableName ref_table_name;

        /**
         * The list of columns that make up the referenced group.
         */
        public String[] ref_columns;

        /**
         * The update rule.
         */
        public String update_rule;

        /**
         * The delete rule.
         */
        public String delete_rule;

        /**
         * Whether this is deferred or initially immediate.
         */
        public short deferred;

    }

}
