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

import com.pony.store.*;
import com.pony.debug.*;
import com.pony.util.IntegerListInterface;
import com.pony.util.UserTerminal;
import com.pony.database.global.ObjectTransfer;
import com.pony.database.global.Ref;

import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.io.*;

/**
 * A MasterTableDataSource that is backed by a non-shared com.pony.store.Store
 * object.  The store interface allows us a great deal of flexibility because
 * we can map a store around different underlying devices.  For example, a
 * store could map to a memory region, a memory mapped file, or a standard
 * random access file.
 * <p>
 * Unlike V1MasterTableDataSource, this manages data and index information in
 * a single store which can be backed by a single file in the file system.
 * <p>
 * The structure of the store comprises of a header block that contains the
 * following information;
 * <p><pre>
 *       HEADER BLOCK
 *   +-------------------------------+
 *   | version                       |
 *   | table id                      |
 *   | table sequence id             |
 *   | pointer to DataTableDef       |
 *   | pointer to DataIndexSetDef    |
 *   | pointer to index block        |
 *   | LIST BLOCK HEADER pointer     |
 *   +-------------------------------+
 * </pre>
 * <p>
 * Each record is comprised of a header which contains offsets to the fields
 * in the record, and a serializable of the fields themselves.
 *
 * @author Tobias Downer
 */

public final class V2MasterTableDataSource extends MasterTableDataSource {

    /**
     * The file name of this store in the conglomerate path.
     */
    private String file_name;

    /**
     * The backing store object.
     */
    private Store store;

    /**
     * An IndexSetStore object that manages the indexes for this table.
     */
    private IndexSetStore index_store;

    /**
     * The current sequence id.
     */
    private long sequence_id;


    // ---------- Pointers into the store ----------

//  /**
//   * Points to the store header area.
//   */
//  private long header_p;

    /**
     * Points to the index header area.
     */
    private long index_header_p;

    /**
     * Points to the block list header area.
     */
    private long list_header_p;

    /**
     * The header area itself.
     */
    private MutableArea header_area;


    /**
     * The structure that manages the pointers to the records.
     */
    private FixedRecordList list_structure;

    /**
     * The first delete chain element.
     */
    private long first_delete_chain_record;


    /**
     * Set to true when the VM has shutdown and writes should no longer be
     * possible on the object.
     */
    private boolean has_shutdown;


    /**
     * The Constructor.
     */
    public V2MasterTableDataSource(TransactionSystem system,
                                   StoreSystem store_system,
                                   OpenTransactionList open_transactions,
                                   BlobStoreInterface blob_store_interface) {
        super(system, store_system, open_transactions, blob_store_interface);
        first_delete_chain_record = -1;
        has_shutdown = false;
    }

    /**
     * Convenience - wraps the given output stream around a buffered data output
     * stream.
     */
    private static DataOutputStream getDOut(OutputStream out) {
//    return new DataOutputStream(out);
        return new DataOutputStream(new BufferedOutputStream(out, 512));
    }

    /**
     * Convenience - wraps the given input stream around a buffered data input
     * stream.
     */
    private static DataInputStream getDIn(InputStream in) {
//    return new DataInputStream(in);
        return new DataInputStream(new BufferedInputStream(in, 512));
    }

    /**
     * Sets up an initial store (should only be called from the 'create' method).
     */
    private void setupInitialStore() throws IOException {
        // Serialize the DataTableDef object
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        DataOutputStream dout = new DataOutputStream(bout);
        dout.writeInt(1);
        getDataTableDef().write(dout);
        // Convert to a byte array
        byte[] data_table_def_buf = bout.toByteArray();

        // Serialize the DataIndexSetDef object
        bout = new ByteArrayOutputStream();
        dout = new DataOutputStream(bout);
        dout.writeInt(1);
        getDataIndexSetDef().write(dout);
        // Convert to byte array
        byte[] index_set_def_buf = bout.toByteArray();

        bout = null;
        dout = null;

        try {
            store.lockForWrite();

            // Allocate an 80 byte header
            AreaWriter header_writer = store.createArea(80);
            long header_p = header_writer.getID();
            // Allocate space to store the DataTableDef serialization
            AreaWriter data_table_def_writer =
                    store.createArea(data_table_def_buf.length);
            long data_table_def_p = data_table_def_writer.getID();
            // Allocate space to store the DataIndexSetDef serialization
            AreaWriter data_index_set_writer =
                    store.createArea(index_set_def_buf.length);
            long data_index_set_def_p = data_index_set_writer.getID();

            // Allocate space for the list header
            list_header_p = list_structure.create();
            list_structure.setReservedLong(-1);
            first_delete_chain_record = -1;

            // Create the index store
            index_store = new IndexSetStore(store, getSystem());
            index_header_p = index_store.create();

            // Write the main header
            header_writer.putInt(1);                  // Version
            header_writer.putInt(table_id);           // table_id
            header_writer.putLong(sequence_id);       // initial sequence id
            header_writer.putLong(data_table_def_p);  // pointer to DataTableDef
            header_writer.putLong(data_index_set_def_p); // pointer to DataIndexSetDef
            header_writer.putLong(index_header_p);    // index header pointer
            header_writer.putLong(list_header_p);     // list header pointer
            header_writer.finish();

            // Write the data_table_def
            data_table_def_writer.put(data_table_def_buf);
            data_table_def_writer.finish();

            // Write the data_index_set_def
            data_index_set_writer.put(index_set_def_buf);
            data_index_set_writer.finish();

            // Set the pointer to the header in the reserved area.
            MutableArea fixed_area = store.getMutableArea(-1);
            fixed_area.putLong(header_p);
            fixed_area.checkOut();

            // Set the header area
            header_area = store.getMutableArea(header_p);

        } finally {
            store.unlockForWrite();
        }

    }

    /**
     * Read the store headers and initialize any internal object state.  This is
     * called by the 'open' method.
     */
    private void readStoreHeaders() throws IOException {
        // Read the fixed header
        Area fixed_area = store.getArea(-1);
        // Set the header area
        header_area = store.getMutableArea(fixed_area.getLong());

        // Open a stream to the header
        int version = header_area.getInt();              // version
        if (version != 1) {
            throw new IOException("Incorrect version identifier.");
        }
        this.table_id = header_area.getInt();         // table_id
        this.sequence_id = header_area.getLong();     // sequence id
        long def_p = header_area.getLong();           // pointer to DataTableDef
        long index_def_p = header_area.getLong();     // pointer to DataIndexSetDef
        this.index_header_p = header_area.getLong();  // pointer to index header
        this.list_header_p = header_area.getLong();   // pointer to list header

        // Read the data table def
        DataInputStream din = getDIn(store.getAreaInputStream(def_p));
        version = din.readInt();
        if (version != 1) {
            throw new IOException("Incorrect DataTableDef version identifier.");
        }
        table_def = DataTableDef.read(din);
        din.close();

        // Read the data index set def
        din = getDIn(store.getAreaInputStream(index_def_p));
        version = din.readInt();
        if (version != 1) {
            throw new IOException("Incorrect DataIndexSetDef version identifier.");
        }
        index_def = DataIndexSetDef.read(din);
        din.close();

        // Read the list header
        list_structure.init(list_header_p);
        first_delete_chain_record = list_structure.getReservedLong();

        // Init the index store
        index_store = new IndexSetStore(store, getSystem());
        try {
            index_store.init(index_header_p);
        } catch (IOException e) {
            // If this failed try writing out a new empty index set.
            // ISSUE: Should this occur here?  This is really an attempt at repairing
            //   the index store.
            index_store = new IndexSetStore(store, getSystem());
            index_header_p = index_store.create();
            index_store.addIndexLists(table_def.columnCount() + 1, (byte) 1, 1024);
            header_area.position(32);
            header_area.putLong(index_header_p);
            header_area.position(0);
            header_area.checkOut();
        }

    }

    /**
     * Create this master table in the file system at the given path.  This will
     * initialise the various file objects and result in a new empty master table
     * to store data in.
     */
    void create(int table_id, DataTableDef table_def) throws IOException {

        // Set the data table def object
        setupDataTableDef(table_def);

        // Initially set the table sequence_id to 1
        this.sequence_id = 1;

        // Generate the name of the store file name.
        this.file_name = makeTableFileName(getSystem(), table_id, getTableName());

        // Create and open the store.
        store = storeSystem().createStore(file_name);

        try {
            store.lockForWrite();

            // Setup the list structure
            list_structure = new FixedRecordList(store, 12);
        } finally {
            store.unlockForWrite();
        }

        // Set up internal state of this object
        this.table_id = table_id;

        // Initialize the store to an empty state,
        setupInitialStore();
        index_store.addIndexLists(table_def.columnCount() + 1, (byte) 1, 1024);

        // Load internal state
        loadInternal();

//    synchAll();

    }

    /**
     * Returns true if the master table data source with the given source
     * identity exists.
     */
    boolean exists(String identity) throws IOException {
        return storeSystem().storeExists(identity);
    }

    /**
     * Opens an existing master table from the file system at the path of the
     * conglomerate this belongs to.  This will set up the internal state of
     * this object with the data read in.
     */
    public void open(String file_name) throws IOException {

        // Set read only flag.
        this.file_name = file_name;

        // Open the store.
        store = storeSystem().openStore(file_name);
        boolean need_check = !store.lastCloseClean();

        // Setup the list structure
        list_structure = new FixedRecordList(store, 12);

        // Read and setup the pointers
        readStoreHeaders();

        // Set the column count
        column_count = table_def.columnCount();

        // Open table indices
        table_indices = new MultiVersionTableIndices(getSystem(),
                table_def.getTableName(), table_def.columnCount());
        // The column rid list cache
        column_rid_list = new RIDList[table_def.columnCount()];

        // Load internal state
        loadInternal();

        if (need_check) {
            // Do an opening scan of the table.  Any records that are uncommited
            // must be marked as deleted.
            doOpeningScan();

            // Scan for any leaks in the file,
            Debug().write(Lvl.INFORMATION, this,
                    "Scanning File: " + file_name + " for leaks.");
            scanForLeaks();
        }

//    HashMap properties = new HashMap();
//    file_store.statsScan(properties);
//    System.out.println("File: " + file_name);
//    Iterator key_i = properties.keySet().iterator();
//    while (key_i.hasNext()) {
//      String key = (String) key_i.next();
//      System.out.print(key);
//      System.out.print(" = ");
//      System.out.println(properties.get(key));
//    }

    }

    /**
     * Closes this master table in the file system.  This frees up all the
     * resources associated with this master table.
     * <p>
     * This method is typically called when the database is shut down.
     */
    synchronized void close(boolean pending_drop) throws IOException {
        // NOTE: This method MUST be synchronized over the table to prevent
        //   establishing a root lock on this table.  If a root lock is established
        //   then the collection event could fail.

        synchronized (list_structure) {

            // If we are root locked, we must become un root locked.
            clearAllRootLocks();

            try {
                try {
                    store.lockForWrite();

                    // Force a garbage collection event.
                    if (!isReadOnly()) {
                        garbage_collector.performCollectionEvent(true);
                    }

                    // If we are closing pending a drop, we need to remove all blob
                    // references in the table.
                    // NOTE: This must only happen after the above collection event.
                    if (pending_drop) {
                        // Scan and remove all blob references for this dropped table.
                        dropAllBlobReferences();
                    }
                } finally {
                    store.unlockForWrite();
                }
            } catch (Throwable e) {
                Debug().write(Lvl.ERROR, this,
                        "Exception during table (" + toString() + ") close: " +
                                e.getMessage());
                Debug().writeException(e);
            }

            // Synchronize the store
            index_store.close();
//      store.flush();

            // Close the store in the store system.
            storeSystem().closeStore(store);

            table_def = null;
            table_indices = null;
            column_rid_list = null;
            is_closed = true;
        }
    }

    /**
     * Creates a new v2 master table data source that is a copy of the given
     * MasterTableDataSource object.
     *
     * @param table_id the table id to given the new table.
     * @param src_master_table the table to copy.
     * @param index_set the view of the table to be copied.
     */
    void copy(int table_id, MasterTableDataSource src_master_table,
              IndexSet index_set) throws IOException {

        // Basically we need to copy all the data and then set the new index view.
        create(table_id, src_master_table.getDataTableDef());

        // The record list.
        IntegerListInterface master_index = index_set.getIndex(0);

        // For each row in the master table
        int sz = src_master_table.rawRowCount();
        for (int i = 0; i < sz; ++i) {
            // Is this row in the set we are copying from?
            if (master_index.contains(i)) {
                // Yes so copy the record into this table.
                copyRecordFrom(src_master_table, i);
            }
        }

        // Copy the index set
        if (src_master_table instanceof V2MasterTableDataSource) {
            index_store.copyAllFrom(index_set);
        } else if (src_master_table instanceof V1MasterTableDataSource) {
            // HACK: This is a bit of a hack.  We should copy the index_set into
            //   this newly created object but instead we rebuild the indexes from
            //   scratch.
            //   This is only used when converting a 0.93 database to the
            //   V2MasterTableDataSource format.
            buildIndexes();
        }

        // Finally set the unique id
        long un_id = src_master_table.nextUniqueID();
        setUniqueID(un_id);

    }

    // ---------- Low level operations ----------

    /**
     * Writes a record to the store and returns a pointer to the area that
     * represents the new record.  This does not manipulate the fixed structure
     * in any way.  This method only allocates an area to store the record and
     * serializes the record.  It is the responsibility of the callee to add the
     * record into the general file structure.
     * <p>
     * Note that if the RowData contains any references to Blob objects then a
     * reference count to the blob is generated at this point.
     */
    private long writeRecordToStore(RowData data) throws IOException {

        // Calculate how much space this record will use
        int row_cells = data.getColumnCount();

        int[] cell_sizes = new int[row_cells];
        int[] cell_type = new int[row_cells];

        try {
            store.lockForWrite();

            // Establish a reference to any blobs in the record
            int all_records_size = 0;
            for (int i = 0; i < row_cells; ++i) {
                TObject cell = data.getCellData(i);
                int sz;
                int ctype;
                if (cell.getObject() instanceof Ref) {
                    Ref large_object_ref = (Ref) cell.getObject();
                    // TBinaryType that are BlobRef objects have to be handled separately.
                    sz = 16;
                    ctype = 2;
                    if (large_object_ref != null) {
                        // Tell the blob store interface that we've made a static reference
                        // to this blob.
                        blob_store_interface.establishReference(large_object_ref.getID());
                    }
                } else {
                    sz = ObjectTransfer.exactSize(cell.getObject());
                    ctype = 1;
                }
                cell_sizes[i] = sz;
                cell_type[i] = ctype;
                all_records_size += sz;
            }

            long record_p;

            // Allocate space for the record,
            AreaWriter writer =
                    store.createArea(all_records_size + (row_cells * 8) + 4);
            record_p = writer.getID();

            // The record output stream
            DataOutputStream dout = getDOut(writer.getOutputStream());

            // Write the record header first,
            dout.writeInt(0);        // reserved for future use
            int cell_skip = 0;
            for (int i = 0; i < row_cells; ++i) {
                dout.writeInt((int) cell_type[i]);
                dout.writeInt(cell_skip);
                cell_skip += cell_sizes[i];
            }

            // Now write a serialization of the cells themselves,
            for (int i = 0; i < row_cells; ++i) {
                TObject t_object = data.getCellData(i);
                int ctype = cell_type[i];
                if (ctype == 1) {
                    // Regular object
                    ObjectTransfer.writeTo(dout, t_object.getObject());
                } else if (ctype == 2) {
                    // This is a binary large object and must be represented as a ref
                    // to a blob in the BlobStore.
                    Ref large_object_ref = (Ref) t_object.getObject();
                    if (large_object_ref == null) {
                        // null value
                        dout.writeInt(1);
                        dout.writeInt(0);                  // Reserved for future use
                        dout.writeLong(-1);
                    } else {
                        dout.writeInt(0);
                        dout.writeInt(0);                  // Reserved for future use
                        dout.writeLong(large_object_ref.getID());
                    }
                } else {
                    throw new IOException("Unrecognised cell type.");
                }
            }

            // Flush the output
            dout.flush();

            // Finish the record
            writer.finish();

            // Return the record
            return record_p;

        } finally {
            store.unlockForWrite();
        }

    }

    /**
     * Copies the record at the given index in the source table to the same
     * record index in this table.  Note that this may need to expand the
     * fixed list record heap as necessary to copy the record into the given
     * position.  The record is NOT copied into the first free record position.
     */
    private void copyRecordFrom(MasterTableDataSource src_master_table,
                                int record_id) throws IOException {

        // Copy the record from the source table in a RowData object,
        int sz = src_master_table.getDataTableDef().columnCount();
        RowData row_data = new RowData(getSystem(), sz);
        for (int i = 0; i < sz; ++i) {
            TObject tob = src_master_table.getCellContents(i, record_id);
            row_data.setColumnDataFromTObject(i, tob);
        }

        try {
            store.lockForWrite();

            // Write record to this table but don't update any structures for the new
            // record.
            long record_p = writeRecordToStore(row_data);

            // Add this record into the table structure at the given index
            addToRecordList(record_id, record_p);

            // Set the record type for this record (committed added).
            writeRecordType(record_id, 0x010);

        } finally {
            store.unlockForWrite();
        }

    }

    /**
     * Removes all blob references in the record area pointed to by 'record_p'.
     * This should only be used when the record is be reclaimed.
     */
    private void removeAllBlobReferencesForRecord(long record_p)
            throws IOException {
        // NOTE: Does this need to be optimized?
        Area record_area = store.getArea(record_p);
        int reserved = record_area.getInt();  // reserved
        // Look for any blob references in the row
        for (int i = 0; i < column_count; ++i) {
            int ctype = record_area.getInt();
            int cell_offset = record_area.getInt();
            if (ctype == 1) {
                // Type 1 is not a large object
            } else if (ctype == 2) {
                int cur_p = record_area.position();
                record_area.position(cell_offset + 4 + (column_count * 8));
                int btype = record_area.getInt();
                record_area.getInt();    // (reserved)
                if (btype == 0) {
                    long blob_ref_id = record_area.getLong();
                    // Release this reference
                    blob_store_interface.releaseReference(blob_ref_id);
                }
                // Revert the area pointer
                record_area.position(cur_p);
            } else {
                throw new RuntimeException("Unrecognised type.");
            }
        }
    }

    /**
     * Scans the table and drops ALL blob references in this table.  This is
     * used when a table is dropped when is still contains elements referenced
     * in the BlobStore.  This will decrease the reference count in the BlobStore
     * for all blobs.  In effect, this is like calling 'delete' on all the data
     * in the table.
     * <p>
     * This method should only be called when the table is about to be deleted
     * from the file system.
     */
    private void dropAllBlobReferences() throws IOException {

        synchronized (list_structure) {
            long elements = list_structure.addressableNodeCount();
            for (long i = 0; i < elements; ++i) {
                Area a = list_structure.positionOnNode(i);
                int status = a.getInt();
                // Is the record not deleted?
                if ((status & 0x020000) == 0) {
                    // Get the record pointer
                    long record_p = a.getLong();
                    removeAllBlobReferencesForRecord(record_p);
                }
            }
        }

    }

    // ---------- Diagnostic and repair ----------

    /**
     * Looks for any leaks in the file.  This works by walking through the
     * file and index area graph and 'remembering' all areas that were read.
     * The store is then checked that all other areas except these are deleted.
     * <p>
     * Assumes the master table is open.
     */
    public void scanForLeaks() throws IOException {

        synchronized (list_structure) {

            // The list of pointers to areas (as Long).
            ArrayList used_areas = new ArrayList();

            // Add the header_p pointer
            used_areas.add(new Long(header_area.getID()));

            header_area.position(16);
            // Add the DataTableDef and DataIndexSetDef objects
            used_areas.add(new Long(header_area.getLong()));
            used_areas.add(new Long(header_area.getLong()));

            // Add all the used areas in the list_structure itself.
            list_structure.addAllAreasUsed(used_areas);

            // Adds all the user areas in the index store.
            index_store.addAllAreasUsed(used_areas);

            // Search the list structure for all areas
            long elements = list_structure.addressableNodeCount();
            for (long i = 0; i < elements; ++i) {
                Area a = list_structure.positionOnNode(i);
                int status = a.getInt();
                if ((status & 0x020000) == 0) {
                    long pointer = a.getLong();
//          System.out.println("Not deleted = " + pointer);
                    // Record is not deleted,
                    used_areas.add(new Long(pointer));
                }
            }

            // Following depends on store implementation
            if (store instanceof AbstractStore) {
                AbstractStore a_store = (AbstractStore) store;
                ArrayList leaked_areas = a_store.findAllocatedAreasNotIn(used_areas);
                if (leaked_areas.size() == 0) {
                    Debug().write(Lvl.INFORMATION, this, "No leaked areas.");
                } else {
                    Debug().write(Lvl.INFORMATION, this, "There were " +
                            leaked_areas.size() + " leaked areas found.");
                    for (int n = 0; n < leaked_areas.size(); ++n) {
                        Long area_pointer = (Long) leaked_areas.get(n);
                        store.deleteArea(area_pointer.longValue());
                    }
                    Debug().write(Lvl.INFORMATION, this,
                            "Leaked areas successfully freed.");
                }
            }

        }

    }

    /**
     * Performs a complete check and repair of the table.  The table must not
     * have been opened before this method is called.  The given UserTerminal
     * parameter is an implementation of a user interface that is used to ask
     * any questions and output the results of the check.
     */
    public void checkAndRepair(String file_name,
                               UserTerminal terminal) throws IOException {

        this.file_name = file_name;

        terminal.println("+ Repairing V2MasterTableDataSource " + file_name);

        store = storeSystem().openStore(file_name);
        // If AbstractStore then fix
        if (store instanceof AbstractStore) {
            ((AbstractStore) store).openScanAndFix(terminal);
        }

        // Setup the list structure
        list_structure = new FixedRecordList(store, 12);

        try {
            // Read and setup the pointers
            readStoreHeaders();
            // Set the column count
            column_count = table_def.columnCount();
        } catch (IOException e) {
            // If this fails, the table is not recoverable.
            terminal.println(
                    "! Table is not repairable because the file headers are corrupt.");
            terminal.println("  Error reported: " + e.getMessage());
            e.printStackTrace();
            return;
        }

        // From here, we at least have intact headers.
        terminal.println("- Checking record integrity.");

        // Get the sorted list of all areas in the file.
        List all_areas = store.getAllAreas();
        // The list of all records generated when we check each record
        ArrayList all_records = new ArrayList();

        // Look up each record and check it's intact,  Any records that are deleted
        // are added to the delete chain.
        first_delete_chain_record = -1;
        int record_count = 0;
        int free_count = 0;
        int sz = rawRowCount();
        for (int i = sz - 1; i >= 0; --i) {
            boolean record_valid = checkAndRepairRecord(i, all_areas, terminal);
            if (record_valid) {
                all_records.add(new Long(i));
                ++record_count;
            } else {
                ++free_count;
            }
        }
        // Set the reserved area
        list_structure.setReservedLong(first_delete_chain_record);

        terminal.print("* Record count = " + record_count);
        terminal.println(" Free count = " + free_count);

        // Check indexes
        terminal.println("- Rebuilding all table index information.");

        int index_count = table_def.columnCount() + 1;
        for (int i = 0; i < index_count; ++i) {
            index_store.commitDropIndex(i);
        }
//    store.flush();
        buildIndexes();

        terminal.println("- Table check complete.");
//    // Flush any changes
//    store.flush();

    }

    /**
     * Checks and repairs a record if it requires repairing.  Returns true if the
     * record is valid, or false otherwise (record is/was deleted).
     */
    private boolean checkAndRepairRecord(
            int row_index, List all_areas, UserTerminal terminal)
            throws IOException {
        synchronized (list_structure) {
            // Position in the list structure
            MutableArea block_area = list_structure.positionOnNode(row_index);
            int p = block_area.position();
            int status = block_area.getInt();
            // If it is not deleted,
            if ((status & 0x020000) == 0) {
                long record_p = block_area.getLong();
//        System.out.println("row_index = " + row_index + " record_p = " + record_p);
                // Is this pointer valid?
                int i = Collections.binarySearch(all_areas, new Long(record_p));
                if (i >= 0) {
                    // Pointer is valid in the store,
                    // Try reading from column 0
                    try {
                        internalGetCellContents(0, row_index);
                        // Return because the record is valid.
                        return true;
                    } catch (Throwable e) {
                        // If an exception is generated when accessing the data, delete the
                        // record.
                        terminal.println("+ Error accessing record: " + e.getMessage());
                    }

                }

                // If we get here, the record needs to be deleted and added to the delete
                // chain
                terminal.println("+ Record area not valid: row = " + row_index +
                        " pointer = " + record_p);
                terminal.println("+ Deleting record.");
            }
            // Put this record in the delete chain
            block_area.position(p);
            block_area.putInt(0x020000);
            block_area.putLong(first_delete_chain_record);
            block_area.checkOut();
            first_delete_chain_record = row_index;

            return false;

        }

    }


    /**
     * Grows the list structure to accomodate more entries.  The new entries
     * are added to the free chain pool.  Assumes we are synchronized over
     * list_structure.
     */
    private void growListStructure() throws IOException {
        try {
            store.lockForWrite();

            // Increase the size of the list structure.
            list_structure.increaseSize();
            // The start record of the new size
            int new_block_number = list_structure.listBlockCount() - 1;
            long start_index =
                    list_structure.listBlockFirstPosition(new_block_number);
            long size_of_block = list_structure.listBlockNodeCount(new_block_number);

            // The Area object for the new position
            MutableArea a = list_structure.positionOnNode(start_index);

            // Set the rest of the block as deleted records
            for (long n = 0; n < size_of_block - 1; ++n) {
                a.putInt(0x020000);
                a.putLong(start_index + n + 1);
            }
            // The last block is end of delete chain.
            a.putInt(0x020000);
            a.putLong(first_delete_chain_record);
            a.checkOut();
            // And set the new delete chain
            first_delete_chain_record = start_index;
            // Set the reserved area
            list_structure.setReservedLong(first_delete_chain_record);

        } finally {
            store.unlockForWrite();
        }

    }

    /**
     * Adds a record to the given position in the fixed structure.  If the place
     * is already used by a record then an exception is thrown, otherwise the
     * record is set.
     */
    private long addToRecordList(long index, long record_p) throws IOException {
        synchronized (list_structure) {
            if (has_shutdown) {
                throw new IOException("IO operation while VM shutting down.");
            }

            long addr_count = list_structure.addressableNodeCount();
            // First make sure there are enough nodes to accomodate this entry,
            while (index >= addr_count) {
                growListStructure();
                addr_count = list_structure.addressableNodeCount();
            }

            // Remove this from the delete chain by searching for the index in the
            // delete chain.
            long prev = -1;
            long chain = first_delete_chain_record;
            while (chain != -1 && chain != index) {
                Area a = list_structure.positionOnNode(chain);
                if (a.getInt() == 0x020000) {
                    prev = chain;
                    chain = a.getLong();
                } else {
                    throw new IOException("Not deleted record is in delete chain!");
                }
            }
            // Wasn't found
            if (chain == -1) {
                throw new IOException(
                        "Unable to add record because index is not available.");
            }
            // Read the next entry in the delete chain.
            Area a = list_structure.positionOnNode(chain);
            if (a.getInt() != 0x020000) {
                throw new IOException("Not deleted record is in delete chain!");
            }
            long next_p = a.getLong();

            try {
                store.lockForWrite();

                // If prev == -1 then first_delete_chain_record points to this record
                if (prev == -1) {
                    first_delete_chain_record = next_p;
                    list_structure.setReservedLong(first_delete_chain_record);
                } else {
                    // Otherwise we need to set the previous node to point to the next node
                    MutableArea ma = list_structure.positionOnNode(prev);
                    ma.putInt(0x020000);
                    ma.putLong(next_p);
                    ma.checkOut();
                }

                // Finally set the record_p
                MutableArea ma = list_structure.positionOnNode(index);
                ma.putInt(0);
                ma.putLong(record_p);
                ma.checkOut();

            } finally {
                store.unlockForWrite();
            }

        }

        return index;
    }

    /**
     * Finds a free place to add a record and returns an index to the record here.
     * This may expand the record space as necessary if there are no free record
     * slots to use.
     */
    private long addToRecordList(long record_p) throws IOException {
        synchronized (list_structure) {
            if (has_shutdown) {
                throw new IOException("IO operation while VM shutting down.");
            }

            // If there are no free deleted records in the delete chain,
            if (first_delete_chain_record == -1) {
                // Grow the fixed structure to allow more nodes,
                growListStructure();
            }

            // Pull free block from the delete chain and recycle it.
            long recycled_record = first_delete_chain_record;
            MutableArea block = list_structure.positionOnNode(recycled_record);
            int rec_pos = block.position();
            // Status of the recycled block
            int status = block.getInt();
            if ((status & 0x020000) == 0) {
                throw new Error("Assertion failed: record is not deleted.  " +
                        "status = " + status + ", rec_pos = " + rec_pos);
            }
            // The pointer to the next in the chain.
            long next_chain = block.getLong();
            first_delete_chain_record = next_chain;

            try {

                store.lockForWrite();

                // Update the first_delete_chain_record field in the header
                list_structure.setReservedLong(first_delete_chain_record);
                // Update the block
                block.position(rec_pos);
                block.putInt(0);
                block.putLong(record_p);
                block.checkOut();

            } finally {
                store.unlockForWrite();
            }

            return recycled_record;

        }

    }


    // ---------- Implemented from AbstractMasterTableDataSource ----------

    String getSourceIdent() {
        return file_name;
    }


    int writeRecordType(int row_index, int row_state) throws IOException {
        synchronized (list_structure) {
            if (has_shutdown) {
                throw new IOException("IO operation while VM shutting down.");
            }

            // Find the record entry in the block list.
            MutableArea block_area = list_structure.positionOnNode(row_index);
            int pos = block_area.position();
            // Get the status.
            int old_status = block_area.getInt();
            int mod_status = (old_status & 0x0FFFF0000) | (row_state & 0x0FFFF);

            // Write the new status
            try {

                store.lockForWrite();

                block_area.position(pos);
                block_area.putInt(mod_status);
                block_area.checkOut();

            } finally {
                store.unlockForWrite();
            }

            return old_status & 0x0FFFF;
        }
    }


    int readRecordType(int row_index) throws IOException {
        synchronized (list_structure) {
            // Find the record entry in the block list.
            Area block_area = list_structure.positionOnNode(row_index);
            // Get the status.
            return block_area.getInt() & 0x0FFFF;
        }
    }


    boolean recordDeleted(int row_index) throws IOException {
        synchronized (list_structure) {
            // Find the record entry in the block list.
            Area block_area = list_structure.positionOnNode(row_index);
            // If the deleted bit set for the record
            return (block_area.getInt() & 0x020000) != 0;
        }
    }


    int rawRowCount() throws IOException {
        synchronized (list_structure) {
            long total = list_structure.addressableNodeCount();
            // 32-bit row limitation here - we should return a long.
            return (int) total;
        }
    }


    void internalDeleteRow(int row_index) throws IOException {
        long record_p;
        synchronized (list_structure) {
            if (has_shutdown) {
                throw new IOException("IO operation while VM shutting down.");
            }

            // Find the record entry in the block list.
            MutableArea block_area = list_structure.positionOnNode(row_index);
            int p = block_area.position();
            int status = block_area.getInt();
            // Check it is not already deleted
            if ((status & 0x020000) != 0) {
                throw new IOException("Record is already marked as deleted.");
            }
            record_p = block_area.getLong();

            // Update the status record.
            try {
                store.lockForWrite();

                block_area.position(p);
                block_area.putInt(0x020000);
                block_area.putLong(first_delete_chain_record);
                block_area.checkOut();
                first_delete_chain_record = row_index;
                // Update the first_delete_chain_record field in the header
                list_structure.setReservedLong(first_delete_chain_record);

                // If the record contains any references to blobs, remove the reference
                // here.
                removeAllBlobReferencesForRecord(record_p);

                // Free the record from the store
                store.deleteArea(record_p);

            } finally {
                store.unlockForWrite();
            }

        }

    }


    IndexSet createIndexSet() {
        return index_store.getSnapshotIndexSet();
    }


    void commitIndexSet(IndexSet index_set) {
        index_store.commitIndexSet(index_set);
        index_set.dispose();
    }

    int internalAddRow(RowData data) throws IOException {

        long row_number;
        int int_row_number;

        // Write the record to the store.
        synchronized (list_structure) {
            long record_p = writeRecordToStore(data);
            // Now add this record into the record block list,
            row_number = addToRecordList(record_p);
            int_row_number = (int) row_number;
        }

        // Update the cell cache as appropriate
        if (DATA_CELL_CACHING) {
            int row_cells = data.getColumnCount();
            for (int i = 0; i < row_cells; ++i) {
                // Put the row/column/TObject into the cache.
                cache.put(table_id, int_row_number, i, data.getCellData(i));
            }
        }

        // Return the record index of the new data in the table
        // NOTE: We are casting this from a long to int which means we are limited
        //   to ~2 billion record references.
        return (int) row_number;

    }


    synchronized void checkForCleanup() {
//    index_store.cleanUpEvent();
        garbage_collector.performCollectionEvent(false);
    }


    // ---- getCellContents ----

    private void skipStream(InputStream in, final long amount)
            throws IOException {
        long count = amount;
        long skipped = 0;
        while (skipped < amount) {
            long last_skipped = in.skip(count);
            skipped += last_skipped;
            count -= last_skipped;
        }
    }


    //  private short s_run_total_hits = 0;
    private short s_run_file_hits = Short.MAX_VALUE;

    // ---- Optimization that saves some cycles -----

    TObject internalGetCellContents(int column, int row) {

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

//    // If the database stats need updating then do so now.
//    if (s_run_total_hits >= 1600) {
//      getSystem().stats().add(s_run_total_hits, total_hits_key);
//      getSystem().stats().add(s_run_file_hits, file_hits_key);
//      s_run_total_hits = 0;
//      s_run_file_hits = 0;
//    }

//    // Increment the total hits counter
//    ++s_run_total_hits;

        // First check if this is within the cache before we continue.
        TObject cell;
        if (DATA_CELL_CACHING) {
            cell = cache.get(table_id, row, column);
            if (cell != null) {
                return cell;
            }
        }

        // We maintain a cache of byte[] arrays that contain the rows read in
        // from the file.  If consequtive reads are made to the same row, then
        // this will cause lots of fast cache hits.

        long record_p = -1;
        try {
            synchronized (list_structure) {

                // Increment the file hits counter
                ++s_run_file_hits;

                if (s_run_file_hits >= 100) {
                    getSystem().stats().add(s_run_file_hits, file_hits_key);
                    s_run_file_hits = 0;
                }

                // Get the node for the record
                Area list_block = list_structure.positionOnNode(row);
                int status = list_block.getInt();
                // Check it's not deleted
                if ((status & 0x020000) != 0) {
                    throw new Error("Unable to read deleted record.");
                }
                // Get the pointer to the record we are reading
                record_p = list_block.getLong();

            }

            // Open a stream to the record
            DataInputStream din = getDIn(store.getAreaInputStream(record_p));

            skipStream(din, 4 + (column * 8));
            int cell_type = din.readInt();
            int cell_offset = din.readInt();

            int cur_at = 8 + 4 + (column * 8);
            int be_at = 4 + (column_count * 8);
            int skip_amount = (be_at - cur_at) + cell_offset;

            skipStream(din, skip_amount);

            Object ob;
            if (cell_type == 1) {
                // If standard object type
                ob = ObjectTransfer.readFrom(din);
            } else if (cell_type == 2) {
                // If reference to a blob in the BlobStore
                int f_type = din.readInt();
                int f_reserved = din.readInt();
                long ref_id = din.readLong();
                if (f_type == 0) {
                    // Resolve the reference
                    ob = blob_store_interface.getLargeObject(ref_id);
                } else if (f_type == 1) {
                    ob = null;
                } else {
                    throw new RuntimeException("Unknown blob type.");
                }
            } else {
                throw new RuntimeException("Unrecognised cell type in data.");
            }

            // Get the TType for this column
            // NOTE: It's possible this call may need optimizing?
            TType ttype = getDataTableDef().columnAt(column).getTType();
            // Wrap it around a TObject
            cell = new TObject(ttype, ob);

            // And close the reader.
            din.close();

        } catch (IOException e) {
            Debug().writeException(e);
//      System.out.println("Pointer = " + row_pointer);
            throw new RuntimeException("IOError getting cell at (" + column + ", " +
                    row + ") pointer = " + record_p + ".");
        }

        // And put in the cache and return it.
        if (DATA_CELL_CACHING) {
            cache.put(table_id, row, column, cell);
        }

        return cell;

    }


    long currentUniqueID() {
        synchronized (list_structure) {
            return sequence_id - 1;
        }
    }


    long nextUniqueID() {
        synchronized (list_structure) {
            long v = sequence_id;
            ++sequence_id;
            if (has_shutdown) {
                throw new RuntimeException("IO operation while VM shutting down.");
            }
            try {
                try {
                    store.lockForWrite();
                    header_area.position(4 + 4);
                    header_area.putLong(sequence_id);
                    header_area.checkOut();
                } finally {
                    store.unlockForWrite();
                }
            } catch (IOException e) {
                Debug().writeException(e);
                throw new Error("IO Error: " + e.getMessage());
            }
            return v;
        }
    }


    void setUniqueID(long value) {
        synchronized (list_structure) {
            sequence_id = value;
            if (has_shutdown) {
                throw new RuntimeException("IO operation while VM shutting down.");
            }
            try {
                try {
                    store.lockForWrite();
                    header_area.position(4 + 4);
                    header_area.putLong(sequence_id);
                    header_area.checkOut();
                } finally {
                    store.unlockForWrite();
                }
            } catch (IOException e) {
                Debug().writeException(e);
                throw new Error("IO Error: " + e.getMessage());
            }
        }
    }

    synchronized void dispose(boolean pending_drop) throws IOException {
        synchronized (list_structure) {
            if (!is_closed) {
                close(pending_drop);
            }
        }
    }

    synchronized boolean drop() throws IOException {
        synchronized (list_structure) {

            if (!is_closed) {
                close(true);
            }

            boolean b = storeSystem().deleteStore(store);
            if (b) {
                Debug().write(Lvl.MESSAGE, this, "Dropped: " + getSourceIdent());
            }
            return b;

        }
    }

    void shutdownHookCleanup() {
//    try {
        synchronized (list_structure) {
            index_store.close();
//        store.synch();
            has_shutdown = true;
        }
//    }
//    catch (IOException e) {
//      Debug().write(Lvl.ERROR, this, "IO Error during shutdown hook.");
//      Debug().writeException(e);
//    }
    }

    boolean isWorthCompacting() {
        // PENDING: We should perform some analysis on the data to decide if a
        //   compact is necessary or not.
        return true;
    }


    /**
     * For diagnostic.
     */
    public String toString() {
        return "[V2MasterTableDataSource: " + file_name + "]";
    }

}

