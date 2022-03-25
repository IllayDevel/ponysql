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

import java.io.IOException;
import java.io.File;
import java.io.DataOutputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;

import com.pony.util.IntegerListInterface;
import com.pony.util.AbstractBlockIntegerList;
import com.pony.util.BlockIntegerList;
import com.pony.util.BlockIntegerList.IntArrayListBlock;
import com.pony.util.IntegerListBlockInterface;
import com.pony.util.ByteBuffer;
import com.pony.util.ByteArrayUtil;
import com.pony.util.IntegerVector;
import com.pony.util.UserTerminal;
import com.pony.util.Cache;
import com.pony.debug.*;

/**
 * A class that manages the storage of a set of transactional index lists in a
 * way that is fast to modify.  This class has a number of objectives;
 * <p>
 * <ul>
 * <li>To prevent corruption as best as possible.</li>
 * <li>To be able to modify lists of integers very fast and persistantly.</li>
 * <li>To have size optimization features such as defragmentation.</li>
 * <li>To provide very fast searches on sorted lists (caching features).</li>
 * <li>To be able to map a list to an IntegerListInterface interface.</li>
 * </ul>
 * <p>
 * None intuitively, this object also handles unique ids.
 *
 * @author Tobias Downer
 */

public final class IndexStore {

    /**
     * A DebugLogger object used to log debug messages to.
     */
    private final DebugLogger debug;

    /**
     * The name of the index store file.
     */
    private final File file;

    /**
     * The size of a 'block' element of index information in a list.  This has
     * a direct relation to the size of the sectors in the store.  This value
     * can be tuned for specific tables.  For example, a table that will only
     * ever contain a few items can save disk space by having a smaller block
     * size.
     */
    private int block_size;

    /**
     * The FixedSizeDataStore that contains all the data of the index store.
     */
    private FixedSizeDataStore index_store;

    /**
     * The list of table sector entries that are currently committed.  Each
     * entry of this list points to a table index list.  The list is formatted
     * as follows;
     * <p><pre>
     *   0 (byte)         - the type of block.
     *   1 (int)          - the number of blocks in this list.
     *   5 (int)          - the sector of column status information or -1 if
     *                      no stats available.
     *   9 to (n * (4 + 4 + 4 + 2))
     *                    - the sector (int), the first and last entry of the
     *                      block and the number of indices in the block
     *                      (short) of each block in this list.
     *   9 + (n * (4 + 4 + 4 + 2)) .... [ next block ] ....
     * </pre>
     */
    private ByteBuffer index_table_list;
    private byte[] index_table_list_buf;

    /**
     * The start sector where the block allocation information is currently
     * stored.
     */
    private int allocation_sector;

    /**
     * The current of the allocation information.
     */
    private int allocation_length;

    /**
     * The list of SnapshotIndexSet objects returned via the
     * 'getSnapshotIndexSet' method.  This can be inspected to find all sectors
     * currently being used to store index information.
     */
    private ArrayList memory_index_set_list;

    /**
     * The list of SnapshotIndexSet objects that have been deleted and are
     * ready for garbage collection.
     */
    private ArrayList index_set_garbage;


    /**
     * Unique id field that contains a unique number that can be incremented
     * atomically.
     */
    private long unique_id;

    /**
     * A cache of int[] array blocks that are accessed by this store.
     */
    private Cache sector_cache;
//  private long cache_hit = 0, cache_miss = 0, cache_access = 0;

    /**
     * Constructs the IndexStore.
     *
     * @param file_name the path to the file of the index store in the file
     *   system.
     */
    public IndexStore(File file_name, DebugLogger logger) {
        this.debug = logger;
        this.file = file_name;
        this.memory_index_set_list = new ArrayList();
        this.index_set_garbage = new ArrayList();
        this.sector_cache = new Cache(47, 47, 10);
    }

    // ---------- Private methods ----------

    /**
     * Reads the index table allocation list in to the ByteBuffer object.  The
     * position of the table allocation list can be determined by looking in the
     * reserved area of the index file.
     */
    private synchronized void readIndexTableList() throws IOException {
        // Read the reserved area for the sector of the allocation information
        byte[] buf = new byte[32];
        index_store.readReservedBuffer(buf, 0, 32);
        allocation_sector = ByteArrayUtil.getInt(buf, 0);
        allocation_length = ByteArrayUtil.getInt(buf, 4);
        unique_id = ByteArrayUtil.getLong(buf, 8);
        // Read the entire allocation information into the ByteBuffer
        buf = new byte[allocation_length];
        index_store.readAcross(allocation_sector, buf, 0, allocation_length);
        index_table_list_buf = new byte[allocation_length];
        index_table_list = new ByteBuffer(index_table_list_buf);
        index_table_list.put(buf);
    }

    /**
     * Initializes the index store to a blank state.
     */
    private synchronized void initBlank() throws IOException {
        // Write the blank allocation area first
        allocation_length = 0;
        byte[] buf = new byte[allocation_length];
        allocation_sector = index_store.writeAcross(buf, 0, buf.length);
        // Write the reserved area
        buf = new byte[32];
        ByteArrayUtil.setInt(allocation_sector, buf, 0);
        ByteArrayUtil.setInt(allocation_length, buf, 4);
        ByteArrayUtil.setLong(1, buf, 8);
        index_store.writeReservedBuffer(buf, 0, 32);
    }


    // ---------- Public methods ----------

    /**
     * Returns true if the index store file exists.
     */
    public synchronized boolean exists() throws IOException {
        return file.exists();
    }

    /**
     * Creates a new black index store and returns leaving the newly created
     * store in an open state.  This method initializes the various data in
     * the index store for a blank set of index tables.  Must call the 'init'
     * method after this is called.
     * <p>
     * @param block_size the number of ints stored in each block.  This
     *   can be optimized for specific use.  Must be between 0 and 32768.
     */
    public synchronized void create(int block_size) throws IOException {
        // Make sure index store is closed
        if (index_store != null && !index_store.isClosed()) {
            throw new Error("Index store is already open.");
        }

        if (block_size > 32767) {
            throw new Error("block_size must be less than 32768");
        }
        if (exists()) {
            throw new IOException("Index store file '" + file +
                    "' already exists.");
        }

        // 'unique_id' now starts at 1 as requested
        unique_id = 1;

        // Set the block size
        this.block_size = block_size;
        // Calculate the size of a sector.  The sector size is block_size * 4
        int sector_size = block_size * 4;
        // NOTE: We don't cache access because the IndexStore manages caching
        this.index_store = new FixedSizeDataStore(file, sector_size, false, debug);

        // Create the index store file
        index_store.open(false);

        // Initialize the index store with blank information.
        initBlank();

    }

    /**
     * Opens this index store.  If 'read_only' is set to true then the store
     * is opened in read only mode.
     * <p>
     * Returns true if opening the store was dirty (was not closed properly) and
     * may need repair.
     * <p>
     * If the index store does not exist before this method is called then it
     * is created.
     */
    public synchronized boolean open(boolean read_only) throws IOException {
        // Make sure index store is closed
        if (index_store != null && !index_store.isClosed()) {
            throw new Error("Index store is already open.");
        }

        if (index_store == null) {
            // NOTE: We don't cache access because the IndexStore manages caching
            this.index_store = new FixedSizeDataStore(file, -1, false, debug);
        }

        // Open the index store file
        boolean dirty_open = index_store.open(read_only);

        // What's the sector size?
        int sector_size = index_store.getSectorSize();
        // Assert that sector_size is divisible by 4
        if (sector_size % 4 != 0) {
            throw new Error("Assert failed, sector size must be divisible by 4");
        }
        // The block size
        this.block_size = sector_size / 4;

        return dirty_open;
    }

    /**
     * Initializes the IndexStore.  Must be called after it is opened for
     * normal use, however it should not be called if we are fixing or repairing
     * the store.
     */
    public synchronized void init() throws IOException {
        // Read the index store and set up this store with the information.
        readIndexTableList();
    }

    /**
     * Performs checks to determine that the index store
     * is stable.  If an IndexStore is not stable and can not be fixed
     * cleanly then it deletes all information in the store and returns false
     * indicating the index information must be rebuilt.
     * <p>
     * Assumes the index store has been opened previous to calling this.
     * <p>
     * Returns true if the IndexStore is stable.
     */
    public synchronized boolean fix(UserTerminal terminal) throws IOException {

        // Open the index store file
        index_store.fix(terminal);

        // Read the index store and set up this store with the information.
        readIndexTableList();

        // Check that at least the reserved area is stable
        try {
            // Read the reserved area for the sector of the allocation information
            byte[] buf = new byte[32];
            index_store.readReservedBuffer(buf, 0, 32);
        } catch (IOException e) {
            terminal.println("! Index store is irrepairable - " +
                    "reserved area is missing.");
            // An IOException here means the table file is lost because we've lost
            // the unique sequence key for the table.
            throw new IOException("Irrepairable index store.");
        }

        return indexReader(terminal);

    }

    synchronized boolean indexReader(UserTerminal terminal)throws IOException{
        // The number of sectors (used and deleted) in the store.
        int raw_sector_count = index_store.rawSectorCount();
        try {
            readIndexTableList();

            // A running count of all index items in all lists
            long used_block_count = 0;
            // A running count of all block sizes
            long total_block_count = 0;

            // Contains a list of all the sectors referenced
            BlockIntegerList sector_list = new BlockIntegerList();

            // Set to the start of the buffer
            index_table_list.position(0);

            // Look at all the information in index_table_list and make sure it
            // is correct.
            while (index_table_list.position() < index_table_list.limit()) {
                byte type = index_table_list.getByte();
                int block_count = index_table_list.getInt();
                int stat_sector = index_table_list.getInt();

                if (stat_sector != -1) {
                    boolean b = sector_list.uniqueInsertSort(stat_sector);
                    if (b == false) {
                        terminal.println("! Index store is not stable - " +
                                "double reference to stat_sector.");
                        return false;
                    }

                    // Check this sector exists and is not deleted.
                    if (stat_sector < 0 || stat_sector >= raw_sector_count ||
                            index_store.isSectorDeleted(stat_sector)) {
                        terminal.println("! Index store is not stable - " +
                                "referenced sector is deleted.");
                        return false;
                    }

                }

                for (int i = 0; i < block_count; ++i) {
                    int first_entry = index_table_list.getInt();
                    int last_entry = index_table_list.getInt();
                    int block_sector = index_table_list.getInt();
                    short int_count = index_table_list.getShort();

                    // Update statistics
                    used_block_count += int_count;
                    total_block_count += block_size;

                    // Block sector not double referenced?
                    boolean b = sector_list.uniqueInsertSort(block_sector);
                    if (b == false) {
                        terminal.println("! Index store is not stable - " +
                                "double reference to block sector.");
                        return false;
                    }

                    // Block sector is present and not deleted.
                    if (block_sector < 0 || block_sector >= raw_sector_count ||
                            index_store.isSectorDeleted(block_sector)) {
                        terminal.println("! Index store is not stable - " +
                                "referenced sector is deleted.");
                        return false;
                    }

                    // Read the block
                    byte[] block_contents = index_store.getSector(block_sector);
                    // Check the first and last entry are the same as in the header.
                    if (int_count > 0) {
                        if (ByteArrayUtil.getInt(block_contents, 0) != first_entry ||
                                ByteArrayUtil.getInt(block_contents, (int_count - 1) * 4) !=
                                        last_entry) {
                            terminal.println("! A block of an index list does not " +
                                    "correctly correspond to its header info.");
                            return false;
                        }
                    }

                } // For each block in a list

            } // while (position < limit)

            // Everything is good
            terminal.println("- Index store is stable.");
            // The total count of all index entries in the store
            terminal.println("- Total used block count = " + used_block_count);
            // The total space available in the store
            terminal.println("- Total available block count = " + total_block_count);
            // Calculate utilization
            if (total_block_count != 0) {
                double utilization = ((float) used_block_count /
                        (float) total_block_count) * 100f;
                terminal.println("- Index store utilization = " + utilization + "%");
            }

            return true;
        } catch (IOException e) {
            terminal.println("! IO Error scanning index store: " + e.getMessage());
            return false;
        }
    }

    /**
     * Returns true if this store is read only.
     */
    public synchronized boolean isReadOnly() {
        return index_store.isReadOnly();
    }

    /**
     * Deletes the store.  Must have been closed before this is called.
     */
    public synchronized void delete() {
        index_store.delete();
    }

    /**
     * Copies the persistant part of this to another store.  Must be open
     * when this is called.
     */
    public synchronized void copyTo(File path) throws IOException {
        index_store.copyTo(path);
    }

    /**
     * Cleanly closes the index store.
     */
    public synchronized void close() throws IOException {
        index_store.close();
        sector_cache = null;
        memory_index_set_list = null;
        index_set_garbage = null;
    }

    /**
     * Flushes all information in this index store to the file representing this
     * store in the file system.  This is called to persistantly update the
     * state of the index store.
     */
    public synchronized void flush() throws IOException {
        // Grab hold of the old allocation information
        int old_sector = allocation_sector;
        int old_length = allocation_length;
        // Write the index_table_list to the store
        allocation_length = index_table_list_buf.length;
        allocation_sector =
                index_store.writeAcross(index_table_list_buf, 0, allocation_length);

        // Write to the reserved area thus 'committing' the changes
        ByteArrayUtil.setInt(allocation_sector, flush_buffer, 0);
        ByteArrayUtil.setInt(allocation_length, flush_buffer, 4);
        ByteArrayUtil.setLong(unique_id, flush_buffer, 8);
        index_store.writeReservedBuffer(flush_buffer, 0, 32);

        // Delete the old allocation information
        index_store.deleteAcross(old_sector);
    }

    private final byte[] flush_buffer = new byte[32];

    /**
     * Performs a hard synchronization of this index store.  This will force the
     * OS to synchronize the contents of the data store.
     * <p>
     * For this to be useful, 'flush' should be called before a hardSynch.
     */
    public synchronized void hardSynch() throws IOException {
        index_store.hardSynch();
    }

    /**
     * The current unique id.
     */
    long currentUniqueID() {
        return unique_id - 1;
    }

    /**
     * Atomically returns the next 'unique_id' value from this file.
     */
    long nextUniqueID() {
        return unique_id++;
    }

    /**
     * Sets the unique id for this store.  This must only be used under
     * extraordinary circumstances, such as restoring from a backup, or
     * converting from one file to another.
     */
    void setUniqueID(long value) {
        unique_id = value + 1;
    }

    /**
     * Returns the block size of this store.
     */
    int getBlockSize() {
        return block_size;
    }


    /**
     * Adds a number of blank index tables to the index store.  For example,
     * we may want this store to contain 16 index lists.
     * <p>
     * NOTE: This doesn't write the updated information to the file.  You must
     *   call 'flush' to write the information to the store.
     */
    public synchronized void addIndexLists(int count, byte type) {

        int add_size = count * (1 + 4 + 4);
        ByteBuffer old_buffer = index_table_list;

        // Create a new buffer
        index_table_list_buf = new byte[old_buffer.limit() + add_size];
        index_table_list = new ByteBuffer(index_table_list_buf);
        // Put the old buffer in to the new buffer
        old_buffer.position(0);
        index_table_list.put(old_buffer);
        // For each new list
        for (int i = 0; i < count; ++i) {
            // The type of the block
            index_table_list.putByte(type);
            // The number of blocks in the table list
            index_table_list.putInt(0);
            // The sector of statistics information (defaults to -1)
            index_table_list.putInt(-1);
        }
    }

    /**
     * Adds a SnapshotIndexSet to the list of sets that this store has
     * dispatched.
     */
    private synchronized void addIndexSetToList(IndexSet index_set) {
        memory_index_set_list.add(index_set);
    }

    /**
     * Removes a SnapshotIndexSet from the list of sets that this store
     * is managing.
     * <p>
     * NOTE: This may be called by the finalizer of the IndexSet object if the
     *   index_set is not disposed.
     */
    private synchronized void removeIndexSetFromList(IndexSet index_set) {
        // If the store is closed, just return.
        if (index_set_garbage == null) {
            return;
        }

        SnapshotIndexSet s_index_set = (SnapshotIndexSet) index_set;
        // Remove from the set list
        boolean b = memory_index_set_list.remove(index_set);
        if (!b) {
            throw new Error("IndexSet was not in the list!");
        }

        // Add to the list of garbage if it has deleted sectors
        if (s_index_set.hasDeletedSectors()) {
            index_set_garbage.add(index_set);

            // Do a garbage collection cycle.  The lowest id that's currently open.
            long lowest_id = -1; //Integer.MAX_VALUE;
            if (memory_index_set_list.size() > 0) {
                lowest_id = ((SnapshotIndexSet) memory_index_set_list.get(0)).getID();
            }

            // Delete all sectors in the garbage list that have an id lower than
            // this.
            boolean deleted;
            do {
                SnapshotIndexSet set = (SnapshotIndexSet) index_set_garbage.get(0);
                deleted = set.getID() < lowest_id;
                if (deleted) {
                    // The list of sectors to delete
                    IntegerVector to_delete = set.allDeletedSectors();

                    // For each sector to delete
                    final int sz = to_delete.size();
                    int n = 0;
                    try {
                        for (n = 0; n < sz; ++n) {
                            int sector = to_delete.intAt(n);
                            index_store.deleteSector(sector);
                        }

                    } catch (IOException e) {
                        debug.write(Lvl.ERROR, this,
                                "Error deleting index " + n + " of list " + to_delete);
                        debug.writeException(e);
                        throw new Error("IO Error: " + e.getMessage());
                    }
                    index_set_garbage.remove(0);

                }  // if (deleted)

            } while (deleted && index_set_garbage.size() > 0);

        }

    }

    /**
     * Returns a current snapshot of the current indexes that are committed in
     * this store.  The returned object can be used to create mutable
     * IntegerListInterface objects.  The created index lists are isolated from
     * changes made to the rest of the indexes after this method returns.
     * <p>
     * A transaction must grab an IndexSet object when it opens.
     * <p>
     * NOTE: We MUST guarentee that the IndexSet is disposed when the
     *   transaction finishes.
     */
    public synchronized IndexSet getSnapshotIndexSet() {
        // We must guarentee that we can't generate SnapshotIndexSet
        // concurrently because it maintains its own ID key system.
        IndexSet index_set =
                new SnapshotIndexSet(index_table_list_buf, allocation_length);
        addIndexSetToList(index_set);
        return index_set;
    }

    /**
     * Commits changes made to a snapshop of an IndexSet as being permanent
     * changes to the state of the index store.  This will generate an error if
     * the given IndexSet is not the last set returned from the
     * 'getSnapshotIndexSet' method.
     * <p>
     * For this to be used, during the transaction commit function a
     * 'getSnapshopIndexSet' must be obtained, changes made to it from info in
     * the journal, then a call to this method.  There must be a guarentee that
     * 'getSnapshotIndexSet' is not called again during this process.
     * <p>
     * NOTE: This doesn't write the updated information to the file.  You must
     *   call 'flush' to write the information to the store.
     * <p>
     * NOTE: We must be guarenteed that when this method is called no other
     *   calls to other methods in this object can be called.
     */
    public synchronized void commitIndexSet(IndexSet index_set) {

        // index_set must be the last in the list of memory_index_set_list
        if (memory_index_set_list.get(memory_index_set_list.size() - 1) !=
                index_set) {
            throw new Error("Can not commit IndexSet because it is not current.");
        }

        SnapshotIndexSet iset = (SnapshotIndexSet) index_set;

        byte[] new_buffer = iset.commit();
        index_table_list_buf = new_buffer;
        index_table_list =
                new ByteBuffer(index_table_list_buf, 0, index_table_list_buf.length);

    }

    /**
     * Returns a string that contains diagnostic information.
     */
    public synchronized String statusString() throws IOException {
        return index_store.statusString();
    }


    // ---------- Inner classes ----------

    /**
     * A unique key that is incremented each time a new IndexSet object is
     * created.
     */
    private long SET_ID_KEY = 0;

    /**
     * A convenience static empty integer list array.
     */
    private static final IndexIntegerList[] EMPTY_INTEGER_LISTS =
            new IndexIntegerList[0];


    /**
     * The implementation of IndexSet which represents a mutable snapshot of
     * the indices stored in this set.
     */
    private class SnapshotIndexSet implements IndexSet {

        /**
         * A unique id given to this index set.
         */
        private final long set_id;

        /**
         * A snapshot of the allocation table.
         */
        private ByteBuffer buf;

        /**
         * The list of IndexIntegerList objects that have been returned via the
         * 'getIndex(n)' method.
         */
        private ArrayList integer_lists;

        /**
         * The sectors that are to be deleted when a garbage collection cycle
         * occurs.
         */
        private IntegerVector deleted_sectors;


        /**
         * Constructor.
         */
        public SnapshotIndexSet(byte[] in_buf, int length) {

            this.set_id = SET_ID_KEY;
            ++SET_ID_KEY;

            // Wrap around a new ByteBuffer but we DON'T make a copy of the byte
            // array itself.  We must be careful that the underlying byte[] array
            // is protected from modifications (it's immutable).
            this.buf = new ByteBuffer(in_buf);

        }

        /**
         * Returns all the lists that have been created by calls to
         * 'getIndex'
         */
        public IndexIntegerList[] getAllLists() {
            if (integer_lists == null) {
                return EMPTY_INTEGER_LISTS;
            } else {
                return (IndexIntegerList[]) integer_lists.toArray(
                        new IndexIntegerList[integer_lists.size()]);
            }
        }

        /**
         * Returns the ByteBuffer for the snapshot of this store when it was
         * created.
         */
        private ByteBuffer getByteBuffer() {
            return buf;
        }

        /**
         * Returns the unique id associated with this index store.
         */
        long getID() {
            return set_id;
        }

        /**
         * Returns true if this store has deleted items.
         */
        boolean hasDeletedSectors() {
            return (deleted_sectors != null && deleted_sectors.size() > 0);
        }

        /**
         * Returns the sectors that were deleted when this store committed.
         */
        IntegerVector allDeletedSectors() {
            return deleted_sectors;
        }

        /**
         * Creates a new buffer for an index store if it is committed.  This
         * also sets up the 'deleted_sectors' list which is a list of records
         * deleted when this store commits.
         */
        byte[] commit() {

            if (deleted_sectors != null) {
                throw new Error("'deleted_sectors' contains sectors to delete.");
            }

            deleted_sectors = new IntegerVector();

            // Look for any indices that have changed in the IndexSet.
            IndexIntegerList[] lists = getAllLists();

            // Make all the lists immutable.
            int sz = lists.length;
            for (IndexIntegerList indexIntegerList : lists) {
                indexIntegerList.setImmutable();
            }

            // The new buffer we are making
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream dout = new DataOutputStream(bout);

            // The original snapshot buffer
            ByteBuffer snapshot_buf = getByteBuffer();

            synchronized (snapshot_buf) {
                int buf_size = snapshot_buf.limit();
                snapshot_buf.position(0);

                try {

                    int index_num = 0;
                    while (snapshot_buf.position() < buf_size) {
                        // Read the information for this block
                        byte list_type = snapshot_buf.getByte();
                        int blocks_count = snapshot_buf.getInt();
                        int stat_sector = snapshot_buf.getInt();
                        byte[] buf = new byte[blocks_count * (4 + 4 + 4 + 2)];
                        snapshot_buf.get(buf, 0, buf.length);

//          System.out.println("blocks_count = " + blocks_count);
//          System.out.println("blocks_capacity = " + blocks_capacity);

                        // Is this index in the list of tables that changed?
                        IndexIntegerList list = null;
                        for (int i = 0; i < sz && list == null; ++i) {
                            if (lists[i].getIndexNumber() == index_num) {
                                // Found this number
                                list = lists[i];
                            }
                        }

                        // We found the list in the set
                        if (list != null) {

                            // The blocks that were deleted (if any).
                            MappedListBlock[] deleted_blocks = list.getDeletedBlocks();
                            for (MappedListBlock deleted_block : deleted_blocks) {
                                // Put all deleted blocks on the list to GC
                                MappedListBlock block = deleted_block;
                                // Make sure the block is mapped to a sector
                                int sector = block.getIndexSector();
                                if (sector != -1) {
                                    deleted_sectors.addInt(sector);
                                }
                            }

                            // So we need to construct a new set.
                            // The blocks in the list,
                            MappedListBlock[] blocks = list.getAllBlocks();
                            blocks_count = blocks.length;
                            dout.writeByte(list_type);
                            dout.writeInt(blocks_count);
                            dout.writeInt(stat_sector);
                            // For each block
                            for (int n = 0; n < blocks_count; ++n) {
                                MappedListBlock block = blocks[n];
                                int bottom_int = 0;
                                int top_int = 0;
                                short block_size = (short) block.size();
                                if (block_size > 0) {
                                    bottom_int = block.bottomInt();
                                    top_int = block.topInt();
                                }
                                int block_sector = block.getIndexSector();
                                // Is the block new or was it changed?
                                if (block_sector == -1 || block.hasChanged()) {
                                    // If this isn't -1 then put this sector on the list of
                                    // sectors to delete during GC.
                                    if (block_sector != -1) {
                                        deleted_sectors.addInt(block_sector);
                                    }
                                    // This is a new block or a block that's been changed
                                    // Write the block to the file system
                                    block_sector = block.writeToStore();
                                }
                                // Write the sector
                                dout.writeInt(bottom_int);
                                dout.writeInt(top_int);
                                dout.writeInt(block_sector);
                                dout.writeShort(block_size);
                            }

                        }
                        // We didn't find the list
                        else {

                            // So what we do is copy the contents of the buffer
                            dout.writeByte(list_type);
                            dout.writeInt(blocks_count);
                            dout.writeInt(stat_sector);
                            dout.write(buf, 0, buf.length);

                        }

                        ++index_num;
                    }

                    // Flush the stream (strictly not necessary).
                    dout.flush();

                } catch (IOException e) {
                    debug.writeException(e);
                    throw new Error(e.getMessage());
                }

            }  // synchronized (snapshot_buf)

            // The finished array
            byte[] arr = bout.toByteArray();

            // return the new buffer.
            return arr;

        }


        // ---------- Implemented from IndexSet ----------

        public IntegerListInterface getIndex(int n) {
            int original_n = n;
            // Synchronize 'buf' for safe access.
            synchronized (buf) {

                // Create if not exist.
                if (integer_lists == null) {
                    integer_lists = new ArrayList();
                } else {
                    // Assertion: If the list already contains this value throw an error.
                    for (Object integer_list : integer_lists) {
                        if (((IndexIntegerList) integer_list).getIndexNumber() ==
                                original_n) {
                            throw new Error(
                                    "IntegerListInterface already created for this n.");
                        }
                    }
                }

                buf.position(0);
                while (n > 0) {
                    byte list_type = buf.getByte();  // Ignore
                    int offset = buf.getInt();
                    int stat_sector = buf.getInt();  // Ignore
                    buf.position(buf.position() + (offset * (4 + 4 + 4 + 2)));
                    --n;
                }
                int list_type = buf.getByte();
                int list_size = buf.getInt();
                int list_stat_sector = buf.getInt();

                // sector_list is an ordered list of all sectors of blocks in the index
                // list.
                // Read in each sector and construct a MappedListBlock for each one.
                MappedListBlock[] list_blocks = new MappedListBlock[list_size];

                for (int i = 0; i < list_size; ++i) {
                    int first_entry = buf.getInt();
                    int last_entry = buf.getInt();
                    int block_sector = buf.getInt();
                    short block_size = buf.getShort();

                    list_blocks[i] = new MappedListBlock(
                            first_entry, last_entry, block_sector, block_size);

                }

                // Create and return the mapped index integer list.
                IndexIntegerList ilist = new IndexIntegerList(original_n, list_blocks);
                integer_lists.add(ilist);
                return ilist;

            } // synchronized(buf)

        }

        public void dispose() {
            // Dispose all the integer lists created by this object.
            synchronized (buf) {
                if (integer_lists != null) {
                    for (Object integer_list : integer_lists) {
                        IndexIntegerList ilist = (IndexIntegerList) integer_list;
                        ilist.dispose();
                    }
                    integer_lists = null;
                }
            }
            buf = null;
            removeIndexSetFromList(this);
        }

        public void finalize() {
            if (buf != null) {
                debug.write(Lvl.WARNING, this, "IndexStore was not disposed!");
                // We remove it manually from the index set list
                removeIndexSetFromList(this);
//        debug.writeException(DEBUG_CONSTRUCTOR);
            }
        }

    }

    /**
     * An IntegerListBlockInterface implementation that maps a block of a list
     * to an underlying file system representation.
     */
    private final class MappedListBlock extends IntArrayListBlock {

        /**
         * The first entry in the block.
         */
        private int first_entry;

        /**
         * The last entry in the block.
         */
        private int last_entry;

        /**
         * The sector in the index file that this block can be found.
         */
        private int index_sector;

        /**
         * Lock object.
         */
        private Object lock = new Object();

        /**
         * Set to true if the loaded block is mutable.
         */
        private boolean mutable_block;

        /**
         * Constructor.
         */
        public MappedListBlock(int first_int, int last_int,
                               int mapped_sector, int size) {
            this.first_entry = first_int;
            this.last_entry = last_int;
            this.index_sector = mapped_sector;
            count = size;
            array = null;
        }

        /**
         * Creates an empty block.
         */
        public MappedListBlock(int block_size_in) {
            super(block_size_in);
            this.index_sector = -1;
        }

        /**
         * Returns the sector in the file of this block.
         */
        public int getIndexSector() {
            return index_sector;
        }

        /**
         * Writes this block to a new sector in the index file and updates the
         * information in this object accordingly.
         * <p>
         * Returns the sector the block was written to.
         */
        public int writeToStore() throws IOException {
            // Convert the int[] array to a byte[] array.
            int block_count = block_size;
            byte[] arr = new byte[block_count * 4];
            int p = 0;
            for (int i = 0; i < block_count; ++i) {
                int v = array[i];
                ByteArrayUtil.setInt(v, arr, p);
                p += 4;
            }

            // Write the sector to the store
            synchronized (IndexStore.this) {
                index_sector = index_store.addSector(arr, 0, arr.length);
            }

            // Write this sector to the cache
            synchronized (sector_cache) {
                sector_cache.put(index_sector, array);
            }

            // Once written, the block is invalidated
            lock = null;

            return index_sector;
        }

        /**
         * Overwritten from IntArrayListBlock, this returns the int[] array that
         * contains the contents of the block.  In this implementation, we
         * determine if the array has been read from the index file.  If it
         * hasn't we read it in, otherwise we use the version in memory.
         */
        public int[] getArray(boolean immutable) {
            // We must synchronize this entire block because otherwise we could
            // return a partially loaded array.
            synchronized (lock) {

                if (array != null) {
                    prepareMutate(immutable);
                    return array;
                }

                // Pull this from a cache
                Object elem;
                synchronized (sector_cache) {
                    elem = sector_cache.get(index_sector);
                }
                if (elem != null) {
                    array = (int[]) elem;
                    mutable_block = false;
                    prepareMutate(immutable);
                    return array;
                }

                int block_count = block_size;
                // Read the sector from the index file.
                array = new int[block_count];
                synchronized (IndexStore.this) {
                    try {
                        array = index_store.getSectorAsIntArray(index_sector, array);
                    } catch (IOException e) {
                        debug.writeException(e);
                        throw new Error("IO Error: " + e.getMessage());
                    }
                }
                // Put in the cache
                synchronized (sector_cache) {
                    sector_cache.put(index_sector, array);
                }
                mutable_block = false;
                prepareMutate(immutable);
                return array;

            }

        }

        /**
         * Overwritten from IntArrayListBlock, returns the capacity of the block.
         */
        public int getArrayLength() {
            return block_size;
        }

        /**
         * Makes the block mutable if it is immutable.  We must be synchronized on
         * 'lock' before this method is called.
         */
        private void prepareMutate(boolean immutable) {
            // If list is to be mutable
            if (!immutable && !mutable_block) {
                array = array.clone();
                mutable_block = true;
            }
        }

        /**
         * Overwritten from IntArrayListBlock, returns the last entry of the block.
         */
        public int topInt() {
            if (count == 0) {
                throw new Error("No first int in block.");
            }

            synchronized (lock) {
                if (array == null) {
                    return last_entry;
                } else {
                    return array[count - 1];
                }
            }
        }

        /**
         * Overwritten from IntArrayListBlock, returns the first entry of the
         * block.
         */
        public int bottomInt() {
            if (count == 0) {
                throw new Error("No first int in block.");
            }

            synchronized (lock) {
                if (array == null) {
                    return first_entry;
                } else {
                    return array[0];
                }
            }
        }

    }

    /**
     * The IntegerListInterface implementation that is used to represent a
     * mutable snapshop of the indices at a given point in time.
     */
    private final class IndexIntegerList extends AbstractBlockIntegerList {

        /**
         * The number of the index in the store that this list represents.
         */
        private final int index_num;

        /**
         * Set to true when disposed.
         */
        private boolean disposed = false;

        /**
         * The mapped elements that were deleted.
         */
        private final ArrayList deleted_blocks = new ArrayList();


        /**
         * Constructs the list with the given set of blocks.
         */
        public IndexIntegerList(int index_num, MappedListBlock[] blocks) {
            super(blocks);
            this.index_num = index_num;
        }

        /**
         * Creates a new block for the list.
         */
        protected IntegerListBlockInterface newListBlock() {
            if (!disposed) {
                return new MappedListBlock(block_size);
            }
            throw new Error("Integer list has been disposed.");
        }

        /**
         * We must maintain a list of deleted blocks.
         */
        protected void deleteListBlock(IntegerListBlockInterface list_block) {
            deleted_blocks.add(list_block);
        }

        /**
         * Returns the index number of this list.
         */
        public int getIndexNumber() {
            return index_num;
        }

        /**
         * Returns the array of all MappedListBlock that are in this list.
         */
        public MappedListBlock[] getAllBlocks() {
            return block_list.toArray(new MappedListBlock[block_list.size()]);
        }

        /**
         * Returns the array of all MappedListBlock that were deleted from this
         * list.
         */
        public MappedListBlock[] getDeletedBlocks() {
            return (MappedListBlock[])
                    deleted_blocks.toArray(new MappedListBlock[deleted_blocks.size()]);
        }


        public void dispose() {
            disposed = true;
            block_list = null;
        }

    }

}
