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

import java.util.ArrayList;
import java.io.*;

import com.pony.util.IntegerListInterface;
import com.pony.util.ByteArrayUtil;
import com.pony.util.UserTerminal;
import com.pony.debug.*;

/**
 * A MasterTableDataSource that uses IndexStore and VariableSizeDataStore as
 * its backing mechanism for representing the table structure in a file on
 * disk.
 * <p>
 * The MasterTableDataSource is basically backed by a VariableSizeDataStore
 * for data and an IndexStore for storing indexing information.
 *
 * @author Tobias Downer
 */

public final class V1MasterTableDataSource extends MasterTableDataSource {

    // ---------- State information ----------

    /**
     * The file name of this store in the conglomerate path.
     */
    private String file_name;

    /**
     * A VariableSizeDataStore object that physically contains the information
     * stored in the file system in the contents of the data source.
     */
    private VariableSizeDataStore data_store;

    /**
     * An IndexStore object that manages the indexes for this table.
     */
    private IndexStore index_store;

    /**
     * The object we use to serialize TObject objects.
     */
    private final DataCellSerialization data_cell_serializer =
            new DataCellSerialization();

    /**
     * The persistent object we use to read information from a row stream.
     */
    private final CellInputStream cell_in;


    /**
     * The Constructor.
     */
    public V1MasterTableDataSource(TransactionSystem system,
                                   StoreSystem store_system,
                                   OpenTransactionList open_transactions) {
        super(system, store_system, open_transactions, null);
        cell_in = new CellInputStream(null);
    }

    /**
     * Returns the name of the file in the conglomerate that represents this
     * store in the file system.
     */
    String getFileName() {
        return file_name;
    }

    /**
     * Returns the path of where this table is located.
     */
    File getPath() {
        return getSystem().getDatabasePath();
    }

    /**
     * Physically create this master table in the file system at the given
     * path.  This will initialise the various file objects and result in a
     * new empty master table to store data in.
     * <p>
     * The 'data_sector_size' and 'index_sector_size' are for fine grain
     * performance and size optimization of the data files.  The default
     * 'index_sector_size' is 1024.
     *
     * @param data_sector_size used to configure the size of the sectors in the
     *   data files.  For tables with small records this number should be low.
     * @param index_sector_size used to configure the size of the sectors in the
     *   index file.  For small tables it is best to keep the index sector size
     *   low.  Recommend 1024 for normal use, 128 for minimalist use.
     */
    synchronized void create(int table_id, DataTableDef table_def,
                             int data_sector_size, int index_sector_size)
            throws IOException {

        // Setup the internal methods
        setupDataTableDef(table_def);

        // Generate the name of the store file name.
        this.file_name = makeTableFileName(getSystem(), table_id, getTableName());

        // Create the store.
        data_store = new VariableSizeDataStore(new File(getPath(), file_name),
                data_sector_size, Debug());
        // Open the store in read/write mode
        data_store.open(false);

        // Open table indices
        index_store = new IndexStore(
                new File(getPath(), file_name + ".iid"), Debug());
        // Open the table index file.
        index_store.create(index_sector_size);
        index_store.init();
        // Make room for columns+1 indices in the index store file
        index_store.addIndexLists(table_def.columnCount() + 1, (byte) 1);
        index_store.flush();

        // Save the table definition to the new store.
        saveDataTableDef(table_def);

        // Write the 'table_id' of this table to the reserved area of the data
        // store.
        byte[] reserved_buffer = new byte[64];
        ByteArrayUtil.setInt(table_id, reserved_buffer, 0);
        data_store.writeReservedBuffer(reserved_buffer, 0, 64);

        // Set up internal state of this object
        this.table_id = table_id;

        // Load internal state
        loadInternal();

    }

    /**
     * Returns true if the master table data source with the given filename
     * exists.
     */
    synchronized boolean exists(String file_name) throws IOException {
        VariableSizeDataStore data_store =
                new VariableSizeDataStore(new File(getPath(), file_name), Debug());
        return data_store.exists();
    }

    /**
     * Opens an existing master table from the file system at the path of the
     * conglomerate this belongs to.  This will set up the internal state of
     * this object with the data read in.
     */
    synchronized void open(String file_name) throws IOException {

        // Open the store.
        data_store = new VariableSizeDataStore(
                new File(getPath(), file_name), Debug());
        boolean need_check = data_store.open(isReadOnly());

        // Set up the internal state of this object
        // Get the 'table_id' of this table from the reserved area of the data
        // store.
        byte[] reserved_buffer = new byte[64];
        data_store.readReservedBuffer(reserved_buffer, 0, 64);
        table_id = ByteArrayUtil.getInt(reserved_buffer, 0);

        // Set the file name.
        this.file_name = file_name;

        // Load the table definition from the store.
        table_def = loadDataTableDef();

        // Set the column count
        column_count = table_def.columnCount();

        // Open table indices
        table_indices = new MultiVersionTableIndices(getSystem(),
                table_def.getTableName(), table_def.columnCount());
        // The column rid list cache
        column_rid_list = new RIDList[table_def.columnCount()];

        // Open table indices
        index_store = new IndexStore(
                new File(getPath(), file_name + ".iid"), Debug());
        // If the index store doesn't exist then create it.
        if (!index_store.exists()) {
            if (!isReadOnly()) {
                // Does the original .ijf file exist?
                File original_ijf = new File(getPath(), file_name + ".ijf");
                if (original_ijf.exists()) {
                    // Message
                    String str = "Converting index file for: " + file_name;
                    System.out.println(str);
                    Debug().write(Lvl.INFORMATION, this, str);
                    // NOTE: The following method leaves the index store open.
                    ArrayList transaction_journals =
                            ConvertUtils.convertIndexFiles1(original_ijf, index_store,
                                    table_def, Debug());
                    if (transaction_journals.size() > 0) {
                        // Notify the user that this may be a problem
                        Debug().write(Lvl.ERROR, this,
                                "There are uncommitted changes that were not " +
                                        "converted because the pre 0.92 database was not closed " +
                                        "cleanly.");
                    }
                    // Force a full table scan
                    need_check = true;
                } else {
                    throw new IOException("The index file for '" + file_name +
                            "' does not exist.");
                }
            } else {
                throw new IOException(
                        "Can not create .iid index file in read-only mode.");
            }
        } else {
            // Open the table index file.
            index_store.open(isReadOnly());
            index_store.init();
        }

        // Load internal state
        loadInternal();

        // Setup a DataIndexSetDef from the information here
        setupDataIndexSetDef();

        if (need_check) {
            // Do an opening scan of the table.  Any records that are uncommited
            // must be marked as deleted.
            doOpeningScan();
        }

    }

    /**
     * Opens this source in the most minimal way.  This should only be used
     * for diagnostics of the data.  This will not load the index.
     */
    synchronized void dirtyOpen(String file_name) throws IOException {

        // We have to open this...
        // Open the store.
        data_store = new VariableSizeDataStore(
                new File(getPath(), file_name), Debug());
        data_store.open(false);

        // Set up the internal state of this object
        // Get the 'table_id' of this table from the reserved area of the data
        // store.
        byte[] reserved_buffer = new byte[64];
        data_store.readReservedBuffer(reserved_buffer, 0, 64);
        table_id = ByteArrayUtil.getInt(reserved_buffer, 0);

        // Set the file name.
        this.file_name = file_name;

        // Load the table definition from the store.
        table_def = loadDataTableDef();

    }

    /**
     * Closes this master table in the file system.  This frees up all the
     * resources associated with this master table.
     * <p>
     * This method is typically called when the database is shut down.
     */
    synchronized void close() throws IOException {
        if (table_indices != null) {
            // Merge all journal changes when we close
            mergeJournalChanges(Integer.MAX_VALUE);

            if (!isReadOnly()) {
                // Synchronize the current state with the file system.
                index_store.flush();
                //table_indices.synchronizeIndexFile();
            }
        }

        // Close the index store.
        index_store.close();
        data_store.close();

        table_id = -1;
//    file_name = null;
        table_def = null;
        table_indices = null;
        column_rid_list = null;
        is_closed = true;
    }

    /**
     * Returns the number of bytes the row takes up in the data file.  This is
     * the actual space used.  If a cell is compressed then it includes the
     * compressed size, not the uncompressed.
     */
    synchronized int rawRecordSize(int row_number) throws IOException {

        int size = 2;

        ++row_number;

        // Open a stream for this row.
        InputStream in = data_store.getRecordInputStream(row_number);
        cell_in.setParentStream(in);

        cell_in.skip(2);

        for (int i = 0; i < column_count; ++i) {
            int len = data_cell_serializer.skipSerialization(cell_in);
            if (len <= 0) {
                throw new Error("Corrupt data - cell size is <= 0");
            }
            cell_in.skip(len);
            size += 4 + len;
        }

        cell_in.close();

        return size;

    }

    /**
     * Returns the current sector size for this data source.
     */
    synchronized int rawDataSectorSize() throws IOException {
        return data_store.sectorSize();
    }

    /**
     * This may only be called from the 'fix' method.  It performs a full scan of
     * the records and rebuilds all the index information from the information.
     * <p>
     * This should only be used as a recovery mechanism and may not accurately
     * rebuild in some cases (but should rebuild as best as possible non the
     * less).
     */
    private synchronized void rebuildAllIndices(File path, String file_name)
            throws IOException {

        // Temporary name of the index store
        File temporary_name = new File(path, file_name + ".id2");
        // Actual name of the index store
        File actual_name = new File(path, file_name + ".iid");

        // Make a new blank index store
        IndexStore temp_store = new IndexStore(temporary_name, Debug());
        // Copy the same block size as the original
        temp_store.create(index_store.getBlockSize());
        temp_store.init();
        temp_store.addIndexLists(column_count + 1, (byte) 1);

        // Get the index of rows in this table
        IndexSet index_set = temp_store.getSnapshotIndexSet();

        // The master index,
        IntegerListInterface master_index = index_set.getIndex(0);

        // The selectable schemes for the table.
        TableDataSource table = minimalTableDataSource(master_index);

        // Create a set of index for this table.
        SelectableScheme[] cols = new SelectableScheme[column_count];
        for (int i = 0; i < column_count; ++i) {
            cols[i] = createSelectableSchemeForColumn(index_set, table, i);
        }

        // For each row
        int row_count = rawRowCount();
        for (int i = 0; i < row_count; ++i) {
            // Is this record marked as deleted?
            if (!recordDeleted(i)) {
                // Get the type flags for this record.
                int type = recordTypeInfo(i);
                // Check if this record is marked as committed removed, or is an
                // uncommitted record.
                if (type == RawDiagnosticTable.COMMITTED_ADDED) {
                    // Insert into the master index
                    master_index.uniqueInsertSort(i);
                    // Insert into schemes
                    for (int n = 0; n < column_count; ++n) {
                        cols[n].insert(i);
                    }
                }
            }  // if not deleted
        }  // for each row

        // Commit the index store

        // Write the modified index set to the index store
        // (Updates the index file)
        temp_store.commitIndexSet(index_set);
        index_set.dispose();
        temp_store.flush();

        // Close and delete the original index_store
        index_store.close();
        index_store.delete();
        // Close the temporary store
        temp_store.close();
        // Rename temp file to the actual file
        boolean b = temporary_name.renameTo(actual_name);
        if (b == false) {
            throw new IOException("Unable to rename " +
                    temporary_name + " to " + actual_name);
        }
        temp_store = null;

        // Copy and open the new reference
        index_store = new IndexStore(actual_name, Debug());
        index_store.open(false);
        index_store.init();

    }

    /**
     * Copies the persistant information in this table data source to the given
     * directory in the file system.  This makes an exact copy of the table as
     * it currently is.  It is recommended that when this is used, there is a
     * lock to prevent committed changes to the database.
     */
    synchronized void copyTo(File path) throws IOException {
        data_store.copyTo(path);
        index_store.copyTo(path);
    }


    // ---------- Diagnostic and repair ----------

    /**
     * Performs a complete check and repair of the table.  The table must not
     * have been opened before this method is called.  The given UserTerminal
     * parameter is an implementation of a user interface that is used to ask
     * any questions and output the results of the check.
     */
    public synchronized void checkAndRepair(String file_name,
                                            UserTerminal terminal) throws IOException {

        // Open the store.
        data_store = new VariableSizeDataStore(
                new File(getPath(), file_name), Debug());
        boolean need_check = data_store.open(isReadOnly());
//    if (need_check) {
        data_store.fix(terminal);
//    }

        // Set up the internal state of this object
        // Get the 'table_id' of this table from the reserved area of the data
        // store.
        byte[] reserved_buffer = new byte[64];
        data_store.readReservedBuffer(reserved_buffer, 0, 64);
        table_id = ByteArrayUtil.getInt(reserved_buffer, 0);

        // Set the file name.
        this.file_name = file_name;

        // Load the table definition from the store.
        table_def = loadDataTableDef();


        // Table journal information
        table_indices = new MultiVersionTableIndices(getSystem(),
                table_def.getTableName(), table_def.columnCount());
        // The column rid list cache
        column_rid_list = new RIDList[table_def.columnCount()];

        // Open table indices
        index_store = new IndexStore(
                new File(getPath(), file_name + ".iid"), Debug());
        // Open the table index file.
        need_check = index_store.open(isReadOnly());
        // Attempt to fix the table index file.
        boolean index_store_stable = index_store.fix(terminal);

        // Load internal state
        loadInternal();

        // Merge all journal changes when we open
        mergeJournalChanges(Integer.MAX_VALUE);

        // If the index store is not stable then clear it and rebuild the
        // indices.
//    if (!index_store_stable) {
        terminal.println("+ Rebuilding all index information for table!");
        rebuildAllIndices(getPath(), file_name);
//    }

        // Do an opening scan of the table.  Any records that are uncommited
        // must be marked as deleted.
        doOpeningScan();

    }


    public synchronized void checkForCleanup() {
        // No-op
    }


    // ---------- Implemented from AbstractMasterTableDataSource ----------

    String getSourceIdent() {
        return getFileName();
    }


    synchronized void synchAll() throws IOException {

        // Flush the indices.
        index_store.flush();

        // Synchronize the data store.
        if (!getSystem().dontSynchFileSystem()) {
            data_store.hardSynch();
        }

        // Synchronize the file handle.  When this returns, we are guarenteed that
        // the index store and the data store are nore persistantly stored in the
        // file system.
        if (!getSystem().dontSynchFileSystem()) {
            index_store.hardSynch();
        }

    }


    synchronized int writeRecordType(int row_index, int row_state)
            throws IOException {
        return data_store.writeRecordType(row_index + 1, row_state);
    }


    synchronized int readRecordType(int row_index) throws IOException {
        return data_store.readRecordType(row_index + 1);
    }


    synchronized boolean recordDeleted(int row_index) throws IOException {
        return data_store.recordDeleted(row_index + 1);
    }


    synchronized int rawRowCount() throws IOException {
        return data_store.rawRecordCount() - 1;
    }


    synchronized void internalDeleteRow(int row_index) throws IOException {
        // Delete the row permanently from the data store.
        data_store.delete(row_index + 1);
    }


    IndexSet createIndexSet() {
        return index_store.getSnapshotIndexSet();
    }


    synchronized void commitIndexSet(IndexSet index_set) {
        index_store.commitIndexSet(index_set);
        index_set.dispose();
    }


    synchronized DataTableDef loadDataTableDef() throws IOException {

        // Read record 0 which contains all this info.
        byte[] d = new byte[65536];
        int read = data_store.read(0, d, 0, 65536);
        if (read == 65536) {
            throw new IOException(
                    "Buffer overflow when reading table definition, > 64k");
        }
        ByteArrayInputStream bin = new ByteArrayInputStream(d, 0, read);

        DataTableDef def;

        DataInputStream din = new DataInputStream(bin);
        int mn = din.readInt();
        // This is the latest format...
        if (mn == 0x0bebb) {
            // Read the DataTableDef object from the input stream,
            def = DataTableDef.read(din);
        } else {
            // Legacy no longer supported...
            throw new IOException(
                    "Couldn't find magic number for table definition data.");
        }

        return def;

    }


    synchronized void saveDataTableDef(DataTableDef def) throws IOException {

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);

        dout.writeInt(0x0bebb);
        def.write(dout);

        // Write the byte array to the data store,

        byte[] d = bout.toByteArray();
        int rindex = data_store.write(d, 0, d.length);

        // rindex MUST be 0 else we buggered.
        if (rindex != 0) {
            throw new IOException("Couldn't write table fields to record 0.");
        }

    }


    synchronized int internalAddRow(RowData data) throws IOException {

        OutputStream out = data_store.getRecordOutputStream();
        DataOutputStream temp_out = new DataOutputStream(out);

        // Reserved for future use.
        temp_out.writeShort(0);

        int row_cells = data.getColumnCount();

        // Write out the data,
        for (int i = 0; i < row_cells; ++i) {
            TObject cell = data.getCellData(i);
            data_cell_serializer.setToSerialize(cell);
            data_cell_serializer.writeSerialization(temp_out);
        }

        // Close the stream and complete it.
        temp_out.close();
        int record_index = data_store.completeRecordStreamWrite();

        // Update the cell cache as appropriate
        if (DATA_CELL_CACHING) {
            for (int i = 0; i < row_cells; ++i) {
                // Put the row/column/TObject into the cache.
                cache.put(table_id, record_index, i, data.getCellData(i));
            }
        }

        // Record index is -1 because sector 0 is DataTableDef.
        int row_number = record_index - 1;

        // If we have a rid_list for any of the columns, then update the indexing
        // there,
        for (int i = 0; i < column_count; ++i) {
            RIDList rid_list = column_rid_list[i];
            if (rid_list != null) {
                rid_list.insertRID(data.getCellData(i), row_number);
            }
        }

        // Return the record index of the new data in the table
        return row_number;

    }


    // ---- getCellContents ----

    private short s_run_total_hits = 0;
    private short s_run_file_hits = 0;

    // ---- Optimization that saves some cycles -----

    /**
     * Some variables that are used for optimization in the 'getCellContents'
     * method.
     */
    private int OPT_last_row = -1;
    private int OPT_last_col = -1;
    private int OPT_last_skip_offset = -1;

    synchronized TObject internalGetCellContents(int column, int row) {

        // NOTES:
        // This is called *A LOT*.  It's a key part of the 20% of the program
        // that's run 80% of the time.
        // This performs very nicely for rows that are completely contained within
        // 1 sector.  However, rows that contain large cells (eg. a large binary
        // or a large string) and spans many sectors will not be utilizing memory
        // as well as it could.
        // The reason is because all the data for a row is read from the store even
        // if only 1 cell of the column is requested.  This will have a big
        // impact on column scans and searches.  The cell cache takes some of this
        // performance bottleneck away.
        // However, a better implementation of this method is made difficult by
        // the fact that sector spans can be compressed.  We should perhaps
        // revise the low level data storage so only sectors can be compressed.

        // If the database stats need updating then do so now.
        if (s_run_total_hits >= 1600) {
            getSystem().stats().add(s_run_total_hits, total_hits_key);
            getSystem().stats().add(s_run_file_hits, file_hits_key);
            s_run_total_hits = 0;
            s_run_file_hits = 0;
        }

        // Increment the total hits counter
        ++s_run_total_hits;

        // Row 0 is reserved for DataTableDef
        ++row;

        // First check if this is within the cache before we continue.
        TObject cell;
        if (DATA_CELL_CACHING) {
            cell = cache.get(table_id, row, column);
            if (cell != null) {
                return cell;
            }
        }

        // Increment the file hits counter
        ++s_run_file_hits;

        // We maintain a cache of byte[] arrays that contain the rows read in
        // from the file.  If consequtive reads are made to the same row, then
        // this will cause lots of fast cache hits.

        try {

            // Open a stream for this row.
            InputStream in = data_store.getRecordInputStream(row);
            cell_in.setParentStream(in);

            // NOTE: This is an optimization for a common sequence of pulling cells
            //   from a row.  It remembers the index of the last column read in, and
            //   if the next column requested is > than the last column read, then
            //   it trivially skips the file pointer to the old point.
            //   Variables starting with 'OPT_' are member variables used for
            //   keeping the optimization state information.

            int start_col;
            if (OPT_last_row == row && column >= OPT_last_col) {
                cell_in.skip(OPT_last_skip_offset);
                start_col = OPT_last_col;
            } else {
                cell_in.skip(2);
                OPT_last_row = row;
                OPT_last_skip_offset = 2;
                OPT_last_col = 0;
                start_col = 0;
            }

            for (int i = start_col; i < column; ++i) {
                int len = data_cell_serializer.skipSerialization(cell_in);
                if (len <= 0) {
                    throw new Error("Corrupt data - cell size is <= 0");
                }
                cell_in.skip(len);
                ++OPT_last_col;
                OPT_last_skip_offset += len + 4;     // ( +4 for the header )
            }
            // Read the cell
            Object ob = data_cell_serializer.readSerialization(cell_in);
            // Get the TType for this column
            // NOTE: It's possible this call may need optimizing?
            TType ttype = getDataTableDef().columnAt(column).getTType();
            // Wrap it around a TObject
            cell = new TObject(ttype, ob);

            // And close the reader.
            cell_in.close();

            // And put in the cache and return it.
            if (DATA_CELL_CACHING) {
                cache.put(table_id, row, column, cell);
            }
            return cell;

        } catch (IOException e) {
            Debug().writeException(e);
            throw new Error("IOError getting cell at (" + column + ", " +
                    row + ").");
        }

    }


    synchronized long currentUniqueID() {
        return index_store.currentUniqueID();
    }

    synchronized long nextUniqueID() {
        return index_store.nextUniqueID();
    }

    synchronized void setUniqueID(long value) {
        index_store.setUniqueID(value);
    }


    synchronized void dispose(boolean pending_close) throws IOException {
        close();
    }

    synchronized boolean drop() throws IOException {
        if (!is_closed) {
            close();
        }

        Debug().write(Lvl.MESSAGE, this, "Dropping: " + getFileName());
        data_store.delete();
        index_store.delete();

        return true;
    }

    void shutdownHookCleanup() {
        // This does nothing...
    }

    /**
     * For diagnostic.
     */
    public String toString() {
        return "[V1MasterTableDataSource: " + file_name + "]";
    }

}

