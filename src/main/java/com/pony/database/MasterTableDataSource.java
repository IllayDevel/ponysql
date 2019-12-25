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

import java.io.*;

import com.pony.util.IntegerListInterface;
import com.pony.util.IntegerIterator;
import com.pony.util.IntegerVector;
import com.pony.debug.*;

/**
 * A master table data source provides facilities for read/writing and
 * maintaining low level data in a table.  It provides primitive table
 * operations such as retrieving a cell from a table, accessing the table's
 * DataTableDef, accessing indexes, and providing views of transactional
 * versions of the data.
 * <p>
 * Logically, a master table data source contains a dynamic number of rows and
 * a fixed number of columns.  Each row has an associated state - either
 * DELETED, UNCOMMITTED, COMMITTED_ADDED or COMMITTED_REMOVED.  A DELETED
 * row is a row that can be reused by a new row added to the table.
 * <p>
 * When a new row is added to the table, it is marked as UNCOMMITTED.  It is
 * later tagged as COMMITTED_ADDED when the transaction that caused the row
 * addition is committed.  If a row commits a row removal, the row is tagged
 * as COMMITTED_REMOVED and later the row garbage collector marks the row as
 * DELETED when there are no remaining references to the row.
 * <p>
 * A master table also maintains a list of indexes for the table.
 * <p>
 * How the master table logical structure is translated to a form that is
 * stored persistantly is implementation specific.  This allows us flexibility
 * with different types of storage schemes.
 *
 * @author Tobias Downer
 */

abstract class MasterTableDataSource {


    // ---------- System information ----------

    /**
     * The global TransactionSystem object that points to the global system
     * that this table source belongs to.
     */
    private final TransactionSystem system;

    /**
     * The StoreSystem implementation that represents the data persistence
     * layer.
     */
    private final StoreSystem store_system;

    // ---------- State information ----------

    /**
     * An integer that uniquely identifies this data source within the
     * conglomerate.
     */
    protected int table_id;

    /**
     * True if this table source is closed.
     */
    protected boolean is_closed;

    // ---------- Root locking ----------

    /**
     * The number of root locks this table data source has on it.
     * <p>
     * While a MasterTableDataSource has at least 1 root lock, it may not
     * reclaim deleted space in the data store.  A root lock means that data
     * is still being pointed to in this file (even possibly committed deleted
     * data).
     */
    private int root_lock;

    // ---------- Persistant data ----------

    /**
     * A DataTableDef object that describes the table topology.  This includes
     * the name and columns of the table.
     */
    protected DataTableDef table_def;

    /**
     * A DataIndexSetDef object that describes the indexes on the table.
     */
    protected DataIndexSetDef index_def;

    /**
     * A cached TableName for this data source.
     */
    private TableName cached_table_name;

    /**
     * A multi-version representation of the table indices kept for this table
     * including the row list and the scheme indices.  This contains the
     * transaction journals.
     */
    protected MultiVersionTableIndices table_indices;

    /**
     * The list of RIDList objects for each column in this table.  This is
     * a sorting optimization.
     */
    protected RIDList[] column_rid_list;

    // ---------- Cached information ----------

    /**
     * Set to false to disable cell caching.
     */
    protected boolean DATA_CELL_CACHING = true;

    /**
     * A reference to the DataCellCache object.
     */
    protected final DataCellCache cache;

    /**
     * The number of columns in this table.  This is a cached optimization.
     */
    protected int column_count;


    // --------- Parent information ----------

    // ---------- Row garbage collection ----------

    /**
     * Manages scanning and deleting of rows marked as deleted within this
     * data source.
     */
    protected final MasterTableGarbageCollector garbage_collector;

    // ---------- Blob management ----------

    /**
     * An abstracted reference to a BlobStore for managing blob references and
     * blob data.
     */
    protected final BlobStoreInterface blob_store_interface;

    // ---------- Stat keys ----------

    /**
     * The keys we use for Database.stats() for information for this table.
     */
    protected String root_lock_key;
    protected String total_hits_key;
    protected String file_hits_key;
    protected String delete_hits_key;
    protected String insert_hits_key;

    /**
     * Constructs the MasterTableDataSource.  The argument is a reference
     * to an object that manages the list of open transactions in the
     * conglomerate.  This object uses this information to determine how journal
     * entries are to be merged with the master indices.
     */
    MasterTableDataSource(TransactionSystem system,
                          StoreSystem store_system,
                          OpenTransactionList open_transactions,
                          BlobStoreInterface blob_store_interface) {

        this.system = system;
        this.store_system = store_system;
        this.blob_store_interface = blob_store_interface;
        this.garbage_collector = new MasterTableGarbageCollector(this);
        this.cache = system.getDataCellCache();
        is_closed = true;

        if (DATA_CELL_CACHING) {
            DATA_CELL_CACHING = (cache != null);
        }

    }

    /**
     * Returns the TransactionSystem for this table.
     */
    public final TransactionSystem getSystem() {
        return system;
    }

    /**
     * Returns the DebugLogger object that can be used to log debug messages.
     */
    public final DebugLogger Debug() {
        return getSystem().Debug();
    }

    /**
     * Returns the TableName of this table source.
     */
    public TableName getTableName() {
        return getDataTableDef().getTableName();
    }

    /**
     * Returns the name of this table source.
     */
    public String getName() {
        return getDataTableDef().getName();
    }

    /**
     * Returns the schema name of this table source.
     */
    public String getSchema() {
        return getDataTableDef().getSchema();
    }

    /**
     * Returns a cached TableName for this data source.
     */
    synchronized TableName cachedTableName() {
        if (cached_table_name != null) {
            return cached_table_name;
        }
        cached_table_name = getTableName();
        return cached_table_name;
    }

    /**
     * Updates the master records from the journal logs up to the given
     * 'commit_id'.  This could be a fairly expensive operation if there are
     * a lot of modifications because each change could require a lookup
     * of records in the data source.
     * <p>
     * NOTE: It's extremely important that when this is called, there are no
     *  transaction open that are using the merged journal.  If there is, then
     *  a transaction may be able to see changes in a table that were made
     *  after the transaction started.
     * <p>
     * After this method is called, it's best to update the index file
     * with a call to 'synchronizeIndexFiles'
     */
    synchronized void mergeJournalChanges(long commit_id) {

        boolean all_merged = table_indices.mergeJournalChanges(commit_id);
        // If all journal entries merged then schedule deleted row collection.
        if (all_merged && !isReadOnly()) {
            checkForCleanup();
        }

    }

    /**
     * Returns a list of all MasterTableJournal objects that have been
     * successfully committed against this table that have an 'commit_id' that
     * is greater or equal to the given.
     * <p>
     * This is part of the conglomerate commit check phase and will be on a
     * commit_lock.
     */
    synchronized MasterTableJournal[] findAllJournalsSince(long commit_id) {
        return table_indices.findAllJournalsSince(commit_id);
    }

    // ---------- Getters ----------

    /**
     * Returns table_id - the unique identifier for this data source.
     */
    int getTableID() {
        return table_id;
    }

    /**
     * Returns the DataTableDef object that represents the topology of this
     * table data source (name, columns, etc).  Note that this information
     * can't be changed during the lifetime of a data source.
     */
    DataTableDef getDataTableDef() {
        return table_def;
    }

    /**
     * Returns the DataIndexSetDef object that represents the indexes on this
     * table.
     */
    DataIndexSetDef getDataIndexSetDef() {
        return index_def;
    }

    // ---------- Convenient statics ----------

    /**
     * Creates a unique table name to give a file.  This could be changed to suit
     * a particular OS's style of filesystem namespace.  Or it could return some
     * arbitarily unique number.  However, for debugging purposes it's often
     * a good idea to return a name that a user can recognise.
     * <p>
     * The 'table_id' is a guarenteed unique number between all tables.
     */
    protected static String makeTableFileName(TransactionSystem system,
                                              int table_id, TableName table_name) {

        // NOTE: We may want to change this for different file systems.
        //   For example DOS is not able to handle more than 8 characters
        //   and is case insensitive.
        String tid = Integer.toString(table_id);
        int pad = 3 - tid.length();
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < pad; ++i) {
            buf.append('0');
        }

        String str = table_name.toString().replace('.', '_');

        // Go through each character and remove each non a-z,A-Z,0-9,_ character.
        // This ensure there are no strange characters in the file name that the
        // underlying OS may not like.
        StringBuffer osified_name = new StringBuffer();
        int count = 0;
        for (int i = 0; i < str.length() || count > 64; ++i) {
            char c = str.charAt(i);
            if ((c >= 'a' && c <= 'z') ||
                    (c >= 'A' && c <= 'Z') ||
                    (c >= '0' && c <= '9') ||
                    c == '_') {
                osified_name.append(c);
                ++count;
            }
        }

        return new String(buf) + tid + new String(osified_name);
    }


    // ---------- Abstract methods ----------

    /**
     * Returns a string that uniquely identifies this table within the
     * conglomerate context.  For example, the filename of the table.  This
     * string can be used to open and initialize the table also.
     */
    abstract String getSourceIdent();

    /**
     * Sets the record type for the given record in the table and returns the
     * previous state of the record.  This is used to change the state of a
     * row in the table.
     */
    abstract int writeRecordType(int row_index, int row_state)
            throws IOException;

    /**
     * Reads the record state for the given record in the table.
     */
    abstract int readRecordType(int row_index) throws IOException;

    /**
     * Returns true if the record with the given index is deleted from the table.
     * A deleted row can not be read.
     */
    abstract boolean recordDeleted(int row_index) throws IOException;

    /**
     * Returns the raw count or rows in the table, including uncommited,
     * committed and deleted rows.  This is basically the maximum number of rows
     * we can iterate through.
     */
    abstract int rawRowCount() throws IOException;

    /**
     * Removes the row at the given index so that any resources associated with
     * the row may be immediately available to be recycled.
     */
    abstract void internalDeleteRow(int row_index) throws IOException;

    /**
     * Creates and returns an IndexSet object that is used to create indices
     * for this table source.  The IndexSet represents a snapshot of the
     * table and the given point in time.
     * <p>
     * NOTE: Not synchronized because we synchronize in the IndexStore object.
     */
    abstract IndexSet createIndexSet();

    /**
     * Commits changes made to an IndexSet returned by the 'createIndexSet'
     * method.  This method also disposes the IndexSet so it is no longer
     * valid.
     */
    abstract void commitIndexSet(IndexSet index_set);

    /**
     * Adds a new row to this table and returns an index that is used to
     * reference this row by the 'getCellContents' method.
     * <p>
     * Note that this method will not effect the master index or column schemes.
     * This is a low level mechanism for adding unreferenced data into a
     * conglomerate.  The data is referenced by committing the change where it
     * eventually migrates into the master index and schemes.
     */
    abstract int internalAddRow(RowData data) throws IOException;

    /**
     * Returns the cell contents of the given cell in the table.  It is the
     * responsibility of the implemented method to perform caching as it deems
     * fit.  Some representations may not require such extensive caching as
     * others.
     */
    abstract TObject internalGetCellContents(int column, int row);

    /**
     * Atomically returns the current 'unique_id' value for this table.
     */
    abstract long currentUniqueID();

    /**
     * Atomically returns the next 'unique_id' value from this table.
     */
    abstract long nextUniqueID();

    /**
     * Sets the unique id for this store.  This must only be used under
     * extraordinary circumstances, such as restoring from a backup, or
     * converting from one file to another.
     */
    abstract void setUniqueID(long value);

    /**
     * Disposes of all in-memory resources associated with this table and
     * invalidates this object.  If 'pending_drop' is true then the table is
     * to be disposed pending a call to 'drop'.  If 'pending_drop' is true then
     * any persistant resources that are allocated may be freed.
     */
    abstract void dispose(boolean pending_drop) throws IOException;

    /**
     * Disposes and drops this table.  If the dispose failed for any reason,
     * it returns false, otherwise true.  If the drop failed, it should be
     * retried at a later time.
     */
    abstract boolean drop() throws IOException;

    /**
     * Called by the 'shutdown hook' on the conglomerate.  This method should
     * block until the table can by put into a safe mode and then prevent any
     * further access to the object after it returns.  It must operate very
     * quickly.
     */
    abstract void shutdownHookCleanup();


    /**
     * Returns true if a compact table is necessary.  By default, we return
     * true however it is recommended this method is overwritten and the table
     * tested.
     */
    boolean isWorthCompacting() {
        return true;
    }

    /**
     * Creates a SelectableScheme object for the given column in this table.
     * This reads the index from the index set (if there is one) then wraps
     * it around the selectable schema as appropriate.
     * <p>
     * NOTE: This needs to be deprecated in support of composite indexes.
     */
    synchronized SelectableScheme createSelectableSchemeForColumn(
            IndexSet index_set, TableDataSource table, int column) {
        // What's the type of scheme for this column?
        DataTableColumnDef column_def = getDataTableDef().columnAt(column);

        // If the column isn't indexable then return a BlindSearch object
        if (!column_def.isIndexableType()) {
            return new BlindSearch(table, column);
        }

        String scheme_type = column_def.getIndexScheme();
        if (scheme_type.equals("InsertSearch")) {
            // Search the TableIndexDef for this column
            DataIndexSetDef index_set_def = getDataIndexSetDef();
            int index_i = index_set_def.findIndexForColumns(
                    new String[]{column_def.getName()});
            return createSelectableSchemeForIndex(index_set, table, index_i);
        } else if (scheme_type.equals("BlindSearch")) {
            return new BlindSearch(table, column);
        } else {
            throw new Error("Unknown scheme type");
        }
    }

    /**
     * Creates a SelectableScheme object for the given index in the index set def
     * in this table.
     * This reads the index from the index set (if there is one) then wraps
     * it around the selectable schema as appropriate.
     */
    synchronized SelectableScheme createSelectableSchemeForIndex(
            IndexSet index_set, TableDataSource table, int index_i) {

        // Get the IndexDef object
        DataIndexDef index_def = getDataIndexSetDef().indexAt(index_i);

        if (index_def.getType().equals("BLIST")) {
            String[] cols = index_def.getColumnNames();
            DataTableDef table_def = getDataTableDef();
            if (cols.length == 1) {
                // If a single column
                int col_index = table_def.findColumnName(cols[0]);
                // Get the index from the index set and set up the new InsertSearch
                // scheme.
                IntegerListInterface index_list =
                        index_set.getIndex(index_def.getPointer());
                InsertSearch iis = new InsertSearch(table, col_index, index_list);
                return iis;
            } else {
                throw new RuntimeException(
                        "Multi-column indexes not supported at this time.");
            }
        } else {
            throw new RuntimeException("Unrecognised type.");
        }

    }

    /**
     * Creates a minimal TableDataSource object that represents this
     * MasterTableDataSource.  It does not implement the 'getColumnScheme'
     * method.
     */
    protected TableDataSource minimalTableDataSource(
            final IntegerListInterface master_index) {
        // Make a TableDataSource that represents the master table over this
        // index.
        return new TableDataSource() {
            public TransactionSystem getSystem() {
                return system;
            }

            public DataTableDef getDataTableDef() {
                return MasterTableDataSource.this.getDataTableDef();
            }

            public int getRowCount() {
                // NOTE: Returns the number of rows in the master index before journal
                //   entries have been made.
                return master_index.size();
            }

            public RowEnumeration rowEnumeration() {
                // NOTE: Returns iterator across master index before journal entry
                //   changes.
                // Get an iterator across the row list.
                final IntegerIterator iterator = master_index.iterator();
                // Wrap it around a RowEnumeration object.
                return new RowEnumeration() {
                    public boolean hasMoreRows() {
                        return iterator.hasNext();
                    }

                    public int nextRowIndex() {
                        return iterator.next();
                    }
                };
            }

            public SelectableScheme getColumnScheme(int column) {
                throw new Error("Not implemented.");
            }

            public TObject getCellContents(int column, int row) {
                return MasterTableDataSource.this.getCellContents(column, row);
            }
        };
    }

    /**
     * Builds a complete index set on the data in this table.  This must only be
     * called when either, a) we are under a commit lock, or b) there is a
     * guarentee that no concurrect access to the indexing information can happen
     * (such as when we are creating the table).
     * <p>
     * NOTE: We assume that the index information for this table is blank before
     *   this method is called.
     */
    synchronized void buildIndexes() throws IOException {
        IndexSet index_set = createIndexSet();

        DataIndexSetDef index_set_def = getDataIndexSetDef();

        final int row_count = rawRowCount();

        // Master index is always on index position 0
        IntegerListInterface master_index = index_set.getIndex(0);

        // First, update the master index
        for (int row_index = 0; row_index < row_count; ++row_index) {
            // If this row isn't deleted, set the index information for it,
            if (!recordDeleted(row_index)) {
                // First add to master index
                boolean inserted = master_index.uniqueInsertSort(row_index);
                if (!inserted) {
                    throw new RuntimeException(
                            "Assertion failed: Master index entry was duplicated.");
                }
            }
        }

        // Commit the master index
        commitIndexSet(index_set);

        // Now go ahead and build each index in this table
        int index_count = index_set_def.indexCount();
        for (int i = 0; i < index_count; ++i) {
            buildIndex(i);
        }

    }

    /**
     * Builds the given index number (from the DataIndexSetDef).  This must only
     * be called when either, a) we are under a commit lock, or b) there is a
     * guarentee that no concurrect access to the indexing information can happen
     * (such as when we are creating the table).
     * <p>
     * NOTE: We assume that the index number in this table is blank before this
     *   method is called.
     */
    synchronized void buildIndex(final int index_number) throws IOException {
        DataIndexSetDef index_set_def = getDataIndexSetDef();

        IndexSet index_set = createIndexSet();

        // Master index is always on index position 0
        IntegerListInterface master_index = index_set.getIndex(0);
        // A minimal TableDataSource for constructing the indexes
        TableDataSource min_table_source = minimalTableDataSource(master_index);

        // Set up schemes for the index,
        SelectableScheme scheme = createSelectableSchemeForIndex(index_set,
                min_table_source, index_number);

        // Rebuild the entire index
        int row_count = rawRowCount();
        for (int row_index = 0; row_index < row_count; ++row_index) {

            // If this row isn't deleted, set the index information for it,
            if (!recordDeleted(row_index)) {
                scheme.insert(row_index);
            }

        }

        // Commit the index
        commitIndexSet(index_set);

    }


    /**
     * Adds a new transaction modification to this master table source.  This
     * information represents the information that was added/removed in the
     * table in this transaction.  The IndexSet object represents the changed
     * index information to commit to this table.
     * <p>
     * It's guarenteed that 'commit_id' additions will be sequential.
     */
    synchronized void commitTransactionChange(long commit_id,
                                              MasterTableJournal change, IndexSet index_set) {
        // ASSERT: Can't do this if source is read only.
        if (isReadOnly()) {
            throw new Error("Can't commit transaction journal, table is read only.");
        }

        change.setCommitID(commit_id);

        try {

            // Add this journal to the multi version table indices log
            table_indices.addTransactionJournal(change);

            // Write the modified index set to the index store
            // (Updates the index file)
            commitIndexSet(index_set);

            // Update the state of the committed added data to the file system.
            // (Updates data to the allocation file)
            //
            // ISSUE: This can add up to a lot of changes to the allocation file and
            //   the Java runtime could potentially be terminated in the middle of
            //   the update.  If an interruption happens the allocation information
            //   may be incorrectly flagged.  The type of corruption this would
            //   result in would be;
            //   + From an 'update' the updated record may disappear.
            //   + From a 'delete' the deleted record may not delete.
            //   + From an 'insert' the inserted record may not insert.
            //
            // Note, the possibility of this type of corruption occuring has been
            // minimized as best as possible given the current architecture.
            // Also note that is not possible for a table file to become corrupted
            // beyond recovery from this issue.

            int size = change.entries();
            for (int i = 0; i < size; ++i) {
                byte b = change.getCommand(i);
                int row_index = change.getRowIndex(i);
                // Was a row added or removed?
                if (MasterTableJournal.isAddCommand(b)) {

                    // Record commit added
                    int old_type = writeRecordType(row_index, 0x010);
                    // Check the record was in an uncommitted state before we changed
                    // it.
                    if ((old_type & 0x0F0) != 0) {
                        writeRecordType(row_index, old_type & 0x0F0);
                        throw new Error("Record " + row_index + " of table " + this +
                                " was not in an uncommitted state!");
                    }

                } else if (MasterTableJournal.isRemoveCommand(b)) {

                    // Record commit removed
                    int old_type = writeRecordType(row_index, 0x020);
                    // Check the record was in an added state before we removed it.
                    if ((old_type & 0x0F0) != 0x010) {
                        writeRecordType(row_index, old_type & 0x0F0);
//            System.out.println(change);
                        throw new Error("Record " + row_index + " of table " + this +
                                " was not in an added state!");
                    }
                    // Notify collector that this row has been marked as deleted.
                    garbage_collector.markRowAsDeleted(row_index);

                }
            }

        } catch (IOException e) {
            Debug().writeException(e);
            throw new Error("IO Error: " + e.getMessage());
        }

    }

    /**
     * Rolls back a transaction change in this table source.  Any rows added
     * to the table will be uncommited rows (type_key = 0).  Those rows must be
     * marked as committed deleted.
     */
    synchronized void rollbackTransactionChange(MasterTableJournal change) {

        // ASSERT: Can't do this is source is read only.
        if (isReadOnly()) {
            throw new Error(
                    "Can't rollback transaction journal, table is read only.");
        }

        // Any rows added in the journal are marked as committed deleted and the
        // journal is then discarded.

        try {
            // Mark all rows in the data_store as appropriate to the changes.
            int size = change.entries();
            for (int i = 0; i < size; ++i) {
                byte b = change.getCommand(i);
                int row_index = change.getRowIndex(i);
                // Make row as added or removed.
                if (MasterTableJournal.isAddCommand(b)) {
                    // Record commit removed (we are rolling back remember).
//          int old_type = data_store.writeRecordType(row_index + 1, 0x020);
                    int old_type = writeRecordType(row_index, 0x020);
                    // Check the record was in an uncommitted state before we changed
                    // it.
                    if ((old_type & 0x0F0) != 0) {
//            data_store.writeRecordType(row_index + 1, old_type & 0x0F0);
                        writeRecordType(row_index, old_type & 0x0F0);
                        throw new Error("Record " + row_index + " was not in an " +
                                "uncommitted state!");
                    }
                    // Notify collector that this row has been marked as deleted.
                    garbage_collector.markRowAsDeleted(row_index);
                } else if (MasterTableJournal.isRemoveCommand(b)) {
                    // Any journal entries marked as TABLE_REMOVE are ignored because
                    // we are rolling back.  This means the row is not logically changed.
                }
            }

            // The journal entry is discarded, the indices do not need to be updated
            // to reflect this rollback.
        } catch (IOException e) {
            Debug().writeException(e);
            throw new Error("IO Error: " + e.getMessage());
        }
    }

    /**
     * Returns a MutableTableDataSource object that represents this data source
     * at the time the given transaction started.  Any modifications to the
     * returned table are logged in the table journal.
     * <p>
     * This is a key method in this object because it allows us to get a data
     * source that represents the data in the table before any modifications
     * may have been committed.
     */
    MutableTableDataSource createTableDataSourceAtCommit(
            SimpleTransaction transaction) {
        return createTableDataSourceAtCommit(transaction,
                new MasterTableJournal(getTableID()));
    }

    /**
     * Returns a MutableTableDataSource object that represents this data source
     * at the time the given transaction started, and also also makes any
     * modifications that are described by the journal in the table.
     * <p>
     * This method is useful for merging the changes made by a transaction into
     * a view of the table.
     */
    MutableTableDataSource createTableDataSourceAtCommit(
            SimpleTransaction transaction, MasterTableJournal journal) {
        return new MMutableTableDataSource(transaction, journal);
    }

    // ---------- File IO level table modification ----------

    /**
     * Sets up the DataIndexSetDef object from the information set in this object.
     * This will only setup a default IndexSetDef on the information in the
     * DataTableDef.
     */
    protected synchronized void setupDataIndexSetDef() {
        // Create the initial DataIndexSetDef object.
        index_def = new DataIndexSetDef(table_def.getTableName());
        for (int i = 0; i < table_def.columnCount(); ++i) {
            DataTableColumnDef col_def = table_def.columnAt(i);
            if (col_def.isIndexableType() &&
                    col_def.getIndexScheme().equals("InsertSearch")) {
                index_def.addDataIndexDef(new DataIndexDef("ANON-COLUMN:" + i,
                        new String[]{col_def.getName()}, i + 1,
                        "BLIST", false));
            }
        }
    }

    /**
     * Sets up the DataTableDef.  This would typically only ever be called from
     * the 'create' method.
     */
    protected synchronized void setupDataTableDef(DataTableDef table_def) {

        // Check table_id isn't too large.
        if ((table_id & 0x0F0000000) != 0) {
            throw new Error("'table_id' exceeds maximum possible keys.");
        }

        this.table_def = table_def;

        // The name of the table to create,
        TableName table_name = table_def.getTableName();

        // Create table indices
        table_indices = new MultiVersionTableIndices(getSystem(),
                table_name, table_def.columnCount());
        // The column rid list cache
        column_rid_list = new RIDList[table_def.columnCount()];

        // Setup the DataIndexSetDef
        setupDataIndexSetDef();
    }

    /**
     * Loads the internal variables.
     */
    protected synchronized void loadInternal() {
        // Set up the stat keys.
        String table_name = table_def.getName();
        String schema_name = table_def.getSchema();
        String n = table_name;
        if (schema_name.length() > 0) {
            n = schema_name + "." + table_name;
        }
        root_lock_key = "MasterTableDataSource.RootLocks." + n;
        total_hits_key = "MasterTableDataSource.Hits.Total." + n;
        file_hits_key = "MasterTableDataSource.Hits.File." + n;
        delete_hits_key = "MasterTableDataSource.Hits.Delete." + n;
        insert_hits_key = "MasterTableDataSource.Hits.Insert." + n;

        column_count = table_def.columnCount();

        is_closed = false;

    }

    /**
     * Returns true if this table source is closed.
     */
    synchronized boolean isClosed() {
        return is_closed;
    }

    /**
     * Returns true if the source is read only.
     */
    boolean isReadOnly() {
        return system.readOnlyAccess();
    }

    /**
     * Returns the StoreSystem object used to manage stores in the persistence
     * system.
     */
    protected StoreSystem storeSystem() {
        return store_system;
    }

    /**
     * Adds a new row to this table and returns an index that is used to
     * reference this row by the 'getCellContents' method.
     * <p>
     * Note that this method will not effect the master index or column schemes.
     * This is a low level mechanism for adding unreferenced data into a
     * conglomerate.  The data is referenced by committing the change where it
     * eventually migrates into the master index and schemes.
     */
    int addRow(RowData data) throws IOException {
        int row_number;

        synchronized (this) {

            row_number = internalAddRow(data);

        } // synchronized

        // Update stats
        getSystem().stats().increment(insert_hits_key);

        // Return the record index of the new data in the table
        return row_number;
    }

    /**
     * Actually deletes the row from the table.  This is a permanent removal of
     * the row from the table.  After this method is called, the row can not
     * be retrieved again.  This is generally only used by the row garbage
     * collector.
     * <p>
     * There is no checking in this method.
     */
    private synchronized void doHardRowRemove(int row_index) throws IOException {

        // If we have a rid_list for any of the columns, then update the indexing
        // there,
        for (int i = 0; i < column_count; ++i) {
            RIDList rid_list = column_rid_list[i];
            if (rid_list != null) {
                rid_list.removeRID(row_index);
            }
        }

        // Internally delete the row,
        internalDeleteRow(row_index);

        // Update stats
        system.stats().increment(delete_hits_key);

    }

    /**
     * Permanently removes a row from this table.  This must only be used when
     * it is determined that a transaction does not reference this row, and
     * that an open result set does not reference this row.  This will remove
     * the row permanently from the underlying file representation.  Calls to
     * 'getCellContents(col, row)' where row is deleted will be undefined after
     * this method is called.
     * <p>
     * Note that the removed row must not be contained within the master index,
     * or be referenced by the index schemes, or be referenced in the
     * transaction modification list.
     */
    synchronized void hardRemoveRow(final int record_index) throws IOException {
        // ASSERTION: We are not under a root lock.
        if (!isRootLocked()) {
//      int type_key = data_store.readRecordType(record_index + 1);
            int type_key = readRecordType(record_index);
            // Check this record is marked as committed removed.
            if ((type_key & 0x0F0) == 0x020) {
                doHardRowRemove(record_index);
            } else {
                throw new Error(
                        "Row isn't marked as committed removed: " + record_index);
            }
        } else {
            throw new Error("Assertion failed: " +
                    "Can't remove row, table is under a root lock.");
        }
    }

    /**
     * Checks the given record index, and if it's possible to reclaim it then
     * it does so here.  Rows are only removed if they are marked as committed
     * removed.
     */
    synchronized boolean hardCheckAndReclaimRow(final int record_index)
            throws IOException {
        // ASSERTION: We are not under a root lock.
        if (!isRootLocked()) {
            // Row already deleted?
            if (!recordDeleted(record_index)) {
                int type_key = readRecordType(record_index);
                // Check this record is marked as committed removed.
                if ((type_key & 0x0F0) == 0x020) {
//          System.out.println("[" + getName() + "] " +
//                             "Hard Removing: " + record_index);
                    doHardRowRemove(record_index);
                    return true;
                }
            }
            return false;
        } else {
            throw new Error("Assertion failed: " +
                    "Can't remove row, table is under a root lock.");
        }
    }

    /**
     * Returns the record type of the given record index.  Returns a type that
     * is compatible with RawDiagnosticTable record type.
     */
    synchronized int recordTypeInfo(int record_index) throws IOException {
//    ++record_index;
        if (recordDeleted(record_index)) {
            return RawDiagnosticTable.DELETED;
        }
        int type_key = readRecordType(record_index) & 0x0F0;
        if (type_key == 0) {
            return RawDiagnosticTable.UNCOMMITTED;
        } else if (type_key == 0x010) {
            return RawDiagnosticTable.COMMITTED_ADDED;
        } else if (type_key == 0x020) {
            return RawDiagnosticTable.COMMITTED_REMOVED;
        }
        return RawDiagnosticTable.RECORD_STATE_ERROR;

    }

    /**
     * This is called by the 'open' method.  It performs a scan of the records
     * and marks any rows that are uncommitted as deleted.  It also checks
     * that the row is not within the master index.
     */
    protected synchronized void doOpeningScan() throws IOException {
        long in_time = System.currentTimeMillis();

        // ASSERTION: No root locks and no pending transaction changes,
        //   VERY important we assert there's no pending transactions.
        if (isRootLocked() || hasTransactionChangesPending()) {
            // This shouldn't happen if we are calling from 'open'.
            throw new RuntimeException(
                    "Odd, we are root locked or have pending journal changes.");
        }

        // This is pointless if we are in read only mode.
        if (!isReadOnly()) {
            // A journal of index changes during this scan...
            MasterTableJournal journal = new MasterTableJournal();

            // Get the master index of rows in this table
            IndexSet index_set = createIndexSet();
            IntegerListInterface master_index = index_set.getIndex(0);

            // NOTE: We assume the index information is correct and that the
            //   allocation information is potentially bad.

            int row_count = rawRowCount();
            for (int i = 0; i < row_count; ++i) {
                // Is this record marked as deleted?
                if (!recordDeleted(i)) {
                    // Get the type flags for this record.
                    int type = recordTypeInfo(i);
                    // Check if this record is marked as committed removed, or is an
                    // uncommitted record.
                    if (type == RawDiagnosticTable.COMMITTED_REMOVED ||
                            type == RawDiagnosticTable.UNCOMMITTED) {
                        // Check it's not in the master index...
                        if (!master_index.contains(i)) {
                            // Delete it.
                            doHardRowRemove(i);
                        } else {
                            Debug().write(Lvl.ERROR, this,
                                    "Inconsistant: Row is indexed but marked as " +
                                            "removed or uncommitted.");
                            Debug().write(Lvl.ERROR, this,
                                    "Row: " + i + " Type: " + type +
                                            " Table: " + getTableName());
                            // Mark the row as committed added because it is in the index.
                            writeRecordType(i, 0x010);

                        }
                    } else {
                        // Must be committed added.  Check it's indexed.
                        if (!master_index.contains(i)) {
                            // Not indexed, so data is inconsistant.
                            Debug().write(Lvl.ERROR, this,
                                    "Inconsistant: Row committed added but not in master index.");
                            Debug().write(Lvl.ERROR, this,
                                    "Row: " + i + " Type: " + type +
                                            " Table: " + getTableName());
                            // Mark the row as committed removed because it is not in the
                            // index.
                            writeRecordType(i, 0x020);

                        }
                    }
                } else {  // if deleted
                    // Check this record isn't in the master index.
                    if (master_index.contains(i)) {
                        // It's in the master index which is wrong!  We should remake the
                        // indices.
                        Debug().write(Lvl.ERROR, this,
                                "Inconsistant: Row is removed but in index.");
                        Debug().write(Lvl.ERROR, this,
                                "Row: " + i + " Table: " + getTableName());
                        // Mark the row as committed added because it is in the index.
                        writeRecordType(i, 0x010);

                    }
                }
            }   // for (int i = 0 ; i < row_count; ++i)

            // Dispose the index set
            index_set.dispose();

        }

        long bench_time = System.currentTimeMillis() - in_time;
        if (Debug().isInterestedIn(Lvl.INFORMATION)) {
            Debug().write(Lvl.INFORMATION, this,
                    "Opening scan for " + toString() + " (" + getTableName() + ") took " +
                            bench_time + "ms.");
        }

    }


    /**
     * Returns an implementation of RawDiagnosticTable that we can use to
     * diagnose problems with the data in this source.
     */
    RawDiagnosticTable getRawDiagnosticTable() {
        return new MRawDiagnosticTable();
    }


    /**
     * Returns the cell contents of the given cell in the table.  This will
     * look up the cell in the file if it can't be found in the cell cache.  This
     * method is undefined if row has been removed or was not returned by
     * the 'addRow' method.
     */
    TObject getCellContents(int column, int row) {
        if (row < 0) {
            throw new Error("'row' is < 0");
        }
        return internalGetCellContents(column, row);
    }

    /**
     * Grabs a root lock on this table.
     * <p>
     * While a MasterTableDataSource has at least 1 root lock, it may not
     * reclaim deleted space in the data store.  A root lock means that data
     * is still being pointed to in this file (even possibly committed deleted
     * data).
     */
    synchronized void addRootLock() {
        system.stats().increment(root_lock_key);
        ++root_lock;
    }

    /**
     * Removes a root lock from this table.
     * <p>
     * While a MasterTableDataSource has at least 1 root lock, it may not
     * reclaim deleted space in the data store.  A root lock means that data
     * is still being pointed to in this file (even possibly committed deleted
     * data).
     */
    synchronized void removeRootLock() {
        if (!is_closed) {
            system.stats().decrement(root_lock_key);
            if (root_lock == 0) {
                throw new Error("Too many root locks removed!");
            }
            --root_lock;
            // If the last lock is removed, schedule a possible collection.
            if (root_lock == 0) {
                checkForCleanup();
            }
        }
    }

    /**
     * Returns true if the table is currently under a root lock (has 1 or more
     * root locks on it).
     */
    synchronized boolean isRootLocked() {
        return root_lock > 0;
    }

    /**
     * Clears all root locks on the table.  Should only be used during cleanup
     * of the table and will by definition invalidate the table.
     */
    protected synchronized void clearAllRootLocks() {
        root_lock = 0;
    }

    /**
     * Checks to determine if it is safe to clean up any resources in the
     * table, and if it is safe to do so, the space is reclaimed.
     */
    abstract void checkForCleanup();


    synchronized String transactionChangeString() {
        return table_indices.transactionChangeString();
    }

    /**
     * Returns true if this table has any journal modifications that have not
     * yet been incorporated into master index.
     */
    synchronized boolean hasTransactionChangesPending() {
        return table_indices.hasTransactionChangesPending();
    }


    // ---------- Inner classes ----------

    /**
     * A RawDiagnosticTable implementation that provides direct access to the
     * root data of this table source bypassing any indexing schemes.  This
     * interface allows for the inspection and repair of data files.
     */
    private final class MRawDiagnosticTable implements RawDiagnosticTable {

        // ---------- Implemented from RawDiagnosticTable -----------

        public int physicalRecordCount() {
            try {
                return rawRowCount();
            } catch (IOException e) {
                throw new Error(e.getMessage());
            }
        }

        public DataTableDef getDataTableDef() {
            return MasterTableDataSource.this.getDataTableDef();
        }

        public int recordState(int record_index) {
            try {
                return recordTypeInfo(record_index);
            } catch (IOException e) {
                throw new Error(e.getMessage());
            }
        }

        public int recordSize(int record_index) {
            return -1;
        }

        public TObject getCellContents(int column, int record_index) {
            return MasterTableDataSource.this.getCellContents(column,
                    record_index);
        }

        public String recordMiscInformation(int record_index) {
            return null;
        }

    }

    /**
     * A MutableTableDataSource object as returned by the
     * 'createTableDataSourceAtCommit' method.
     * <p>
     * NOTE: This object is NOT thread-safe and it is assumed any use of this
     *   object will be thread exclusive.  This is okay because multiple
     *   instances of this object can be created on the same MasterTableDataSource
     *   if multi-thread access to a MasterTableDataSource is desirable.
     */
    private final class MMutableTableDataSource
            implements MutableTableDataSource {

        /**
         * The Transaction object that this MutableTableDataSource was
         * generated from.  This reference should be used only to query
         * database constraint information.
         */
        private SimpleTransaction transaction;

        /**
         * True if the transaction is read-only.
         */
        private final boolean tran_read_only;

        /**
         * The 'recovery point' to which the row index in this table source has
         * rebuilt to.
         */
        private int row_list_rebuild;

        /**
         * The index that represents the rows that are within this
         * table data source within this transaction.
         */
        private IntegerListInterface row_list;

        /**
         * The 'recovery point' to which the schemes in this table source have
         * rebuilt to.
         */
        private int[] scheme_rebuilds;

        /**
         * The IndexSet for this mutable table source.
         */
        private IndexSet index_set;

        /**
         * The SelectableScheme array that represents the schemes for the
         * columns within this transaction.
         */
        private final SelectableScheme[] column_schemes;

        /**
         * A journal of changes to this source since it was created.
         */
        private MasterTableJournal table_journal;

        /**
         * The last time any changes to the journal were check for referential
         * integrity violations.
         */
        private int last_entry_ri_check;

        /**
         * Constructs the data source.
         */
        public MMutableTableDataSource(SimpleTransaction transaction,
                                       MasterTableJournal journal) {
            this.transaction = transaction;
            this.index_set =
                    transaction.getIndexSetForTable(MasterTableDataSource.this);
            int col_count = getDataTableDef().columnCount();
            TableName table_name = getDataTableDef().getTableName();
            this.tran_read_only = transaction.isReadOnly();
            row_list_rebuild = 0;
            scheme_rebuilds = new int[col_count];
            column_schemes = new SelectableScheme[col_count];
            table_journal = journal;
            last_entry_ri_check = table_journal.entries();
        }

        /**
         * Executes an update referential action.  If the update action is
         * "NO ACTION", and the constraint is INITIALLY_IMMEDIATE, and the new key
         * doesn't exist in the referral table, an exception is thrown.
         */
        private void executeUpdateReferentialAction(
                Transaction.ColumnGroupReference constraint,
                TObject[] original_key, TObject[] new_key,
                QueryContext context) {

            final String update_rule = constraint.update_rule;
            if (update_rule.equals("NO ACTION") &&
                    constraint.deferred != Transaction.INITIALLY_IMMEDIATE) {
                // Constraint check is deferred
                return;
            }

            // So either update rule is not NO ACTION, or if it is we are initially
            // immediate.
            MutableTableDataSource key_table =
                    transaction.getTable(constraint.key_table_name);
            DataTableDef table_def = key_table.getDataTableDef();
            int[] key_cols = TableDataConglomerate.findColumnIndices(
                    table_def, constraint.key_columns);
            IntegerVector key_entries =
                    TableDataConglomerate.findKeys(key_table, key_cols, original_key);

            // Are there keys effected?
            if (key_entries.size() > 0) {
                if (update_rule.equals("NO ACTION")) {
                    // Throw an exception;
                    throw new DatabaseConstraintViolationException(
                            DatabaseConstraintViolationException.FOREIGN_KEY_VIOLATION,
                            TableDataConglomerate.deferredString(constraint.deferred) +
                                    " foreign key constraint violation on update (" +
                                    constraint.name + ") Columns = " +
                                    constraint.key_table_name.toString() + "( " +
                                    TableDataConglomerate.stringColumnList(constraint.key_columns) +
                                    " ) -> " + constraint.ref_table_name.toString() + "( " +
                                    TableDataConglomerate.stringColumnList(constraint.ref_columns) +
                                    " )");
                } else {
                    // Perform a referential action on each updated key
                    int sz = key_entries.size();
                    for (int i = 0; i < sz; ++i) {
                        int row_index = key_entries.intAt(i);
                        RowData row_data = new RowData(key_table);
                        row_data.setFromRow(row_index);
                        switch (update_rule) {
                            case "CASCADE":
                                // Update the keys
                                for (int n = 0; n < key_cols.length; ++n) {
                                    row_data.setColumnData(key_cols[n], new_key[n]);
                                }
                                key_table.updateRow(row_index, row_data);
                                break;
                            case "SET NULL":
                                for (int key_col : key_cols) {
                                    row_data.setColumnToNull(key_col);
                                }
                                key_table.updateRow(row_index, row_data);
                                break;
                            case "SET DEFAULT":
                                for (int key_col : key_cols) {
                                    row_data.setColumnToDefault(key_col, context);
                                }
                                key_table.updateRow(row_index, row_data);
                                break;
                            default:
                                throw new RuntimeException(
                                        "Do not understand referential action: " + update_rule);
                        }
                    }
                    // Check referential integrity of modified table,
                    key_table.constraintIntegrityCheck();
                }
            }
        }

        /**
         * Executes a delete referential action.  If the delete action is
         * "NO ACTION", and the constraint is INITIALLY_IMMEDIATE, and the new key
         * doesn't exist in the referral table, an exception is thrown.
         */
        private void executeDeleteReferentialAction(
                Transaction.ColumnGroupReference constraint,
                TObject[] original_key, QueryContext context) {

            final String delete_rule = constraint.delete_rule;
            if (delete_rule.equals("NO ACTION") &&
                    constraint.deferred != Transaction.INITIALLY_IMMEDIATE) {
                // Constraint check is deferred
                return;
            }

            // So either delete rule is not NO ACTION, or if it is we are initially
            // immediate.
            MutableTableDataSource key_table =
                    transaction.getTable(constraint.key_table_name);
            DataTableDef table_def = key_table.getDataTableDef();
            int[] key_cols = TableDataConglomerate.findColumnIndices(
                    table_def, constraint.key_columns);
            IntegerVector key_entries =
                    TableDataConglomerate.findKeys(key_table, key_cols, original_key);

            // Are there keys effected?
            if (key_entries.size() > 0) {
                if (delete_rule.equals("NO ACTION")) {
                    // Throw an exception;
                    throw new DatabaseConstraintViolationException(
                            DatabaseConstraintViolationException.FOREIGN_KEY_VIOLATION,
                            TableDataConglomerate.deferredString(constraint.deferred) +
                                    " foreign key constraint violation on delete (" +
                                    constraint.name + ") Columns = " +
                                    constraint.key_table_name.toString() + "( " +
                                    TableDataConglomerate.stringColumnList(constraint.key_columns) +
                                    " ) -> " + constraint.ref_table_name.toString() + "( " +
                                    TableDataConglomerate.stringColumnList(constraint.ref_columns) +
                                    " )");
                } else {
                    // Perform a referential action on each updated key
                    int sz = key_entries.size();
                    for (int i = 0; i < sz; ++i) {
                        int row_index = key_entries.intAt(i);
                        RowData row_data = new RowData(key_table);
                        row_data.setFromRow(row_index);
                        switch (delete_rule) {
                            case "CASCADE":
                                // Cascade the removal of the referenced rows
                                key_table.removeRow(row_index);
                                break;
                            case "SET NULL":
                                for (int key_col : key_cols) {
                                    row_data.setColumnToNull(key_col);
                                }
                                key_table.updateRow(row_index, row_data);
                                break;
                            case "SET DEFAULT":
                                for (int key_col : key_cols) {
                                    row_data.setColumnToDefault(key_col, context);
                                }
                                key_table.updateRow(row_index, row_data);
                                break;
                            default:
                                throw new RuntimeException(
                                        "Do not understand referential action: " + delete_rule);
                        }
                    }
                    // Check referential integrity of modified table,
                    key_table.constraintIntegrityCheck();
                }
            }
        }

        /**
         * Returns the entire row list for this table.  This will request this
         * information from the master source.
         */
        private IntegerListInterface getRowIndexList() {
            if (row_list == null) {
                row_list = index_set.getIndex(0);
            }
            return row_list;
        }

        /**
         * Ensures that the row list is as current as the latest journal change.
         * We can be assured that when this is called, no journal changes will
         * occur concurrently.  However we still need to synchronize because
         * multiple reads are valid.
         */
        private void ensureRowIndexListCurrent() {
            int rebuild_index = row_list_rebuild;
            int journal_count = table_journal.entries();
            while (rebuild_index < journal_count) {
                byte command = table_journal.getCommand(rebuild_index);
                int row_index = table_journal.getRowIndex(rebuild_index);
                if (MasterTableJournal.isAddCommand(command)) {
                    // Add to 'row_list'.
                    boolean b = getRowIndexList().uniqueInsertSort(row_index);
                    if (b == false) {
                        throw new Error(
                                "Row index already used in this table (" + row_index + ")");
                    }
                } else if (MasterTableJournal.isRemoveCommand(command)) {
                    // Remove from 'row_list'
                    boolean b = getRowIndexList().removeSort(row_index);
                    if (b == false) {
                        throw new Error("Row index removed that wasn't in this table!");
                    }
                } else {
                    throw new Error("Unrecognised journal command.");
                }
                ++rebuild_index;
            }
            // It's now current (row_list_rebuild == journal_count);
            row_list_rebuild = rebuild_index;
        }

        /**
         * Ensures that the scheme column index is as current as the latest
         * journal change.
         */
        private void ensureColumnSchemeCurrent(int column) {
            SelectableScheme scheme = column_schemes[column];
            // NOTE: We should be assured that no write operations can occur over
            //   this section of code because writes are exclusive operations
            //   within a transaction.
            // Are there journal entries pending on this scheme since?
            int rebuild_index = scheme_rebuilds[column];
            int journal_count = table_journal.entries();
            while (rebuild_index < journal_count) {
                byte command = table_journal.getCommand(rebuild_index);
                int row_index = table_journal.getRowIndex(rebuild_index);
                if (MasterTableJournal.isAddCommand(command)) {
                    scheme.insert(row_index);
                } else if (MasterTableJournal.isRemoveCommand(command)) {
                    scheme.remove(row_index);
                } else {
                    throw new Error("Unrecognised journal command.");
                }
                ++rebuild_index;
            }
            scheme_rebuilds[column] = rebuild_index;
        }

        // ---------- Implemented from MutableTableDataSource ----------

        public TransactionSystem getSystem() {
            return MasterTableDataSource.this.getSystem();
        }

        public DataTableDef getDataTableDef() {
            return MasterTableDataSource.this.getDataTableDef();
        }

        public int getRowCount() {
            // Ensure the row list is up to date.
            ensureRowIndexListCurrent();
            return getRowIndexList().size();
        }

        public RowEnumeration rowEnumeration() {
            // Ensure the row list is up to date.
            ensureRowIndexListCurrent();
            // Get an iterator across the row list.
            final IntegerIterator iterator = getRowIndexList().iterator();
            // Wrap it around a RowEnumeration object.
            return new RowEnumeration() {
                public boolean hasMoreRows() {
                    return iterator.hasNext();
                }

                public int nextRowIndex() {
                    return iterator.next();
                }
            };
        }

        public TObject getCellContents(int column, int row) {
            return MasterTableDataSource.this.getCellContents(column, row);
        }

        // NOTE: Returns an immutable version of the scheme...
        public SelectableScheme getColumnScheme(int column) {
            SelectableScheme scheme = column_schemes[column];
            // Cache the scheme in this object.
            if (scheme == null) {
                scheme = createSelectableSchemeForColumn(index_set, this, column);
                column_schemes[column] = scheme;
            }

            // Update the underlying scheme to the most current version.
            ensureColumnSchemeCurrent(column);

            return scheme;
        }

        // ---------- Table Modification ----------

        public int addRow(RowData row_data) {

            // Check the transaction isn't read only.
            if (tran_read_only) {
                throw new RuntimeException("Transaction is read only.");
            }

            // Check this isn't a read only source
            if (isReadOnly()) {
                throw new Error("Can not add row - table is read only.");
            }

            // Add to the master.
            int row_index;
            try {
                row_index = MasterTableDataSource.this.addRow(row_data);
            } catch (IOException e) {
                Debug().writeException(e);
                throw new Error("IO Error: " + e.getMessage());
            }

            // Note this doesn't need to be synchronized because we are exclusive on
            // this table.
            // Add this change to the table journal.
            table_journal.addEntry(MasterTableJournal.TABLE_ADD, row_index);

            return row_index;
        }

        public void removeRow(int row_index) {

            // Check the transaction isn't read only.
            if (tran_read_only) {
                throw new RuntimeException("Transaction is read only.");
            }

            // Check this isn't a read only source
            if (isReadOnly()) {
                throw new Error("Can not remove row - table is read only.");
            }

            // NOTE: This must <b>NOT</b> call 'removeRow' in MasterTableDataSource.
            //   We do not want to delete a row permanently from the underlying
            //   file because the transaction using this data source may yet decide
            //   to roll back the change and not delete the row.

            // Note this doesn't need to be synchronized because we are exclusive on
            // this table.
            // Add this change to the table journal.
            table_journal.addEntry(MasterTableJournal.TABLE_REMOVE, row_index);

        }

        public int updateRow(int row_index, RowData row_data) {

            // Check the transaction isn't read only.
            if (tran_read_only) {
                throw new RuntimeException("Transaction is read only.");
            }

            // Check this isn't a read only source
            if (isReadOnly()) {
                throw new Error("Can not update row - table is read only.");
            }

            // Note this doesn't need to be synchronized because we are exclusive on
            // this table.
            // Add this change to the table journal.
            table_journal.addEntry(MasterTableJournal.TABLE_UPDATE_REMOVE, row_index);

            // Add to the master.
            int new_row_index;
            try {
                new_row_index = MasterTableDataSource.this.addRow(row_data);
            } catch (IOException e) {
                Debug().writeException(e);
                throw new Error("IO Error: " + e.getMessage());
            }

            // Note this doesn't need to be synchronized because we are exclusive on
            // this table.
            // Add this change to the table journal.
            table_journal.addEntry(MasterTableJournal.TABLE_UPDATE_ADD, new_row_index);

            return new_row_index;
        }


        public void flushIndexChanges() {
            ensureRowIndexListCurrent();
            // This will flush all of the column schemes
            for (int i = 0; i < column_schemes.length; ++i) {
                getColumnScheme(i);
            }
        }

        public void constraintIntegrityCheck() {
            try {

                // Early exit condition
                if (last_entry_ri_check == table_journal.entries()) {
                    return;
                }

                // This table name
                DataTableDef table_def = getDataTableDef();
                TableName table_name = table_def.getTableName();
                QueryContext context =
                        new SystemQueryContext(transaction, table_name.getSchema());

                // Are there any added, deleted or updated entries in the journal since
                // we last checked?
                IntegerVector rows_updated = new IntegerVector();
                IntegerVector rows_deleted = new IntegerVector();
                IntegerVector rows_added = new IntegerVector();

                int size = table_journal.entries();
                for (int i = last_entry_ri_check; i < size; ++i) {
                    byte tc = table_journal.getCommand(i);
                    int row_index = table_journal.getRowIndex(i);
                    if (tc == MasterTableJournal.TABLE_REMOVE ||
                            tc == MasterTableJournal.TABLE_UPDATE_REMOVE) {
                        rows_deleted.addInt(row_index);
                        // If this is in the rows_added list, remove it from rows_added
                        int ra_i = rows_added.indexOf(row_index);
                        if (ra_i != -1) {
                            rows_added.removeIntAt(ra_i);
                        }
                    } else if (tc == MasterTableJournal.TABLE_ADD ||
                            tc == MasterTableJournal.TABLE_UPDATE_ADD) {
                        rows_added.addInt(row_index);
                    }

                    if (tc == MasterTableJournal.TABLE_UPDATE_REMOVE) {
                        rows_updated.addInt(row_index);
                    } else if (tc == MasterTableJournal.TABLE_UPDATE_ADD) {
                        rows_updated.addInt(row_index);
                    }
                }

                // Were there any updates or deletes?
                if (rows_deleted.size() > 0) {
                    // Get all references on this table
                    Transaction.ColumnGroupReference[] foreign_constraints =
                            Transaction.queryTableImportedForeignKeyReferences(transaction,
                                    table_name);

                    // For each foreign constraint
                    for (Transaction.ColumnGroupReference constraint : foreign_constraints) {
                        // For each deleted/updated record in the table,
                        for (int i = 0; i < rows_deleted.size(); ++i) {
                            int row_index = rows_deleted.intAt(i);
                            // What was the key before it was updated/deleted
                            int[] cols = TableDataConglomerate.findColumnIndices(
                                    table_def, constraint.ref_columns);
                            TObject[] original_key = new TObject[cols.length];
                            int null_count = 0;
                            for (int p = 0; p < cols.length; ++p) {
                                original_key[p] = getCellContents(cols[p], row_index);
                                if (original_key[p].isNull()) {
                                    ++null_count;
                                }
                            }
                            // Check the original key isn't null
                            if (null_count != cols.length) {
                                // Is is an update?
                                int update_index = rows_updated.indexOf(row_index);
                                if (update_index != -1) {
                                    // Yes, this is an update
                                    int row_index_add = rows_updated.intAt(update_index + 1);
                                    // It must be an update, so first see if the change caused any
                                    // of the keys to change.
                                    boolean key_changed = false;
                                    TObject[] key_updated_to = new TObject[cols.length];
                                    for (int p = 0; p < cols.length; ++p) {
                                        key_updated_to[p] = getCellContents(cols[p], row_index_add);
                                        if (original_key[p].compareTo(key_updated_to[p]) != 0) {
                                            key_changed = true;
                                        }
                                    }
                                    if (key_changed) {
                                        // Allow the delete, and execute the action,
                                        // What did the key update to?
                                        executeUpdateReferentialAction(constraint,
                                                original_key, key_updated_to, context);
                                    }
                                    // If the key didn't change, we don't need to do anything.
                                } else {
                                    // No, so it must be a delete,
                                    // This will look at the referencee table and if it contains
                                    // the key, work out what to do with it.
                                    executeDeleteReferentialAction(constraint, original_key,
                                            context);
                                }

                            }  // If the key isn't null

                        }  // for each deleted rows

                    }  // for each foreign key reference to this table

                }

                // Were there any rows added (that weren't deleted)?
                if (rows_added.size() > 0) {
                    int[] row_indices = rows_added.toIntArray();

                    // Check for any field constraint violations in the added rows
                    TableDataConglomerate.checkFieldConstraintViolations(
                            transaction, this, row_indices);
                    // Check this table, adding the given row_index, immediate
                    TableDataConglomerate.checkAddConstraintViolations(
                            transaction, this,
                            row_indices, Transaction.INITIALLY_IMMEDIATE);
                }

            } catch (DatabaseConstraintViolationException e) {

                // If a constraint violation, roll back the changes since the last
                // check.
                int rollback_point = table_journal.entries() - last_entry_ri_check;
                if (row_list_rebuild <= rollback_point) {
                    table_journal.rollbackEntries(rollback_point);
                } else {
                    System.out.println(
                            "WARNING: rebuild_pointer is after rollback point so we can't " +
                                    "rollback to the point before the constraint violation.");
                }

                throw e;

            } finally {
                // Make sure we update the 'last_entry_ri_check' variable
                last_entry_ri_check = table_journal.entries();
            }

        }

        public MasterTableJournal getJournal() {
            return table_journal;
        }

        public void dispose() {
            // Dispose and invalidate the schemes
            // This is really a safety measure to ensure the schemes can't be
            // used outside the scope of the lifetime of this object.
            for (int i = 0; i < column_schemes.length; ++i) {
                SelectableScheme scheme = column_schemes[i];
                if (scheme != null) {
                    scheme.dispose();
                    column_schemes[i] = null;
                }
            }
            row_list = null;
            table_journal = null;
            scheme_rebuilds = null;
            index_set = null;
            transaction = null;
        }

        public void addRootLock() {
            MasterTableDataSource.this.addRootLock();
        }

        public void removeRootLock() {
            MasterTableDataSource.this.removeRootLock();
        }

    }

}
