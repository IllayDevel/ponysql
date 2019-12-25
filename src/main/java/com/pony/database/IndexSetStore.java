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

import java.io.IOException;
import java.util.ArrayList;

import com.pony.util.IntegerListInterface;
import com.pony.util.AbstractBlockIntegerList;
import com.pony.util.BlockIntegerList.IntArrayListBlock;
import com.pony.util.IntegerListBlockInterface;
import com.pony.store.Store;
import com.pony.store.Area;
import com.pony.store.MutableArea;
import com.pony.store.AreaWriter;
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
 * This object uses a com.pony.store.Store instance as its backing medium.
 * <p>
 * This store manages three types of areas; 'Index header', 'Index block' and
 * 'Index element'.
 * <p>
 * Index header: This area type contains an entry for each index being stored.
 * The Index header contains a pointer to an 'Index block' area for each index.
 * The pointer to the 'Index block' in this area changes whenever an index
 * changes, or when new indexes are added or deleted from the store.
 * <p>
 * Index block: This area contains a number of pointers to Index element blocks.
 * The number of entries depends on the number of indices in the list.  Each
 * entry contains the size of the block, the first and last entry of the block,
 * and a pointer to the element block itself.  If an element of the index
 * changes or elements are removed or deleted, this block does NOT change.
 * This should be considered an immutable area.
 * <p>
 * Index element: This area simply contains the actual values in a block of the
 * index.  An Index element area does not change and should be considered an
 * immutable area.
 *
 * @author Tobias Downer
 */

final class IndexSetStore {

    /**
     * The magic value that we use to mark the start area.
     */
    private static final int MAGIC = 0x0CA90291;


    /**
     * A DebugLogger object used to log debug messages to.
     */
    private final DebugLogger debug;

    /**
     * The Store that contains all the data of the index store.
     */
    private Store store;

    /**
     * The starting header of this index set.  This is a very small area that
     * simply contains a magic value and a pointer to the index header.  This
     * is the only MutableArea object that is required by the index set.
     */
    private MutableArea start_area;

    /**
     * The index header area.  The index header area contains an entry for each
     * index being stored.  Each entry is 16 bytes in size and has a 16 byte
     * header.
     * <p>
     * HEADER: ( version (int), reserved (int), index count (long) ) <br>
     * ENTRY: ( type (int), block_size (int), index block pointer (long) )
     */
    private long index_header_p;
    private Area index_header_area;

    /**
     * The index blocks - one for each index being stored.  An index block area
     * contains an entry for each index element in an index.  Each entry is 28
     * bytes in size and the area has a 16 byte header.
     * <p>
     * HEADER: ( version (int), reserved (int), index size (long) ) <br>
     * ENTRY: ( first entry (long), last entry (long),
     *          index element pointer (long), type/element size (int) )
     * <p>
     * type/element size contains the number of elements in the block, and the
     * block compaction factor.  For example, type 1 means the block contains
     * short sized index values, 2 is int sized index values, and 3 is long
     * sized index values.
     */
    private IndexBlock[] index_blocks;


    /**
     * Constructs the IndexSetStore over the given Store object.
     */
    public IndexSetStore(Store store, final TransactionSystem system) {
        this.store = store;
        /**
         * The TransactionSystem for this index set.
         */
        this.debug = system.Debug();
    }


    /**
     * Delete all areas specified in the list (as a list of Long).
     */
    private synchronized void deleteAllAreas(ArrayList list) {

        if (store != null) {

            try {
                store.lockForWrite();

                int sz = list.size();
                for (int i = 0; i < sz; ++i) {
                    long id = ((Long) list.get(i)).longValue();
                    store.deleteArea(id);
                }

            } catch (IOException e) {
                debug.write(Lvl.ERROR, this, "Error when freeing old index block.");
                debug.writeException(e);
            } finally {
                store.unlockForWrite();
            }

        }
    }


    // ---------- Private methods ----------

    /**
     * Creates a new blank index block in the store and returns a pointer to the
     * area.
     */
    private long createBlankIndexBlock() throws IOException {
        // Allocate the area
        AreaWriter a = store.createArea(16);
        long index_block_p = a.getID();
        // Setup the header
        a.putInt(1);     // version
        a.putInt(0);     // reserved
        a.putLong(0);    // block entries
        a.finish();

        return index_block_p;
    }

    // ---------- Public methods ----------

    /**
     * Creates a new black index set store and returns a pointer to a static
     * area that is later used to reference this index set in this store.
     * Remember to synch after this is called.
     */
    public synchronized long create() throws IOException {

        // Create an empty index header area
        AreaWriter a = store.createArea(16);
        index_header_p = a.getID();
        a.putInt(1);  // version
        a.putInt(0);  // reserved
        a.putLong(0); // number of indexes in the set
        a.finish();

        // Set up the local Area object for the index header
        index_header_area = store.getArea(index_header_p);

        index_blocks = new IndexBlock[0];

        // Allocate the starting header
        AreaWriter start_a = store.createArea(32);
        long start_p = start_a.getID();
        // The magic
        start_a.putInt(MAGIC);
        // The version
        start_a.putInt(1);
        // Pointer to the index header
        start_a.putLong(index_header_p);
        start_a.finish();

        // Set the 'start_area' value.
        start_area = store.getMutableArea(start_p);

        return start_p;
    }

    /**
     * Initializes this index set.  This must be called during general
     * initialization of the table object.
     */
    public synchronized void init(long start_p) throws IOException {

        // Set up the start area
        start_area = store.getMutableArea(start_p);

        int magic = start_area.getInt();
        if (magic != MAGIC) {
            throw new IOException("Magic value for index set does not match.");
        }
        int version = start_area.getInt();
        if (version != 1) {
            throw new IOException("Unknown version for index set.");
        }

        // Setup the index_header area
        index_header_p = start_area.getLong();
        index_header_area = store.getArea(index_header_p);

        // Read the index header area
        version = index_header_area.getInt();         // version
        if (version != 1) {
            throw new IOException("Incorrect version");
        }
        int reserved = index_header_area.getInt();    // reserved
        int index_count = (int) index_header_area.getLong();
        index_blocks = new IndexBlock[index_count];

        // Initialize each index block
        for (int i = 0; i < index_count; ++i) {
            int type = index_header_area.getInt();
            int block_size = index_header_area.getInt();
            long index_block_p = index_header_area.getLong();
            if (type == 1) {
                index_blocks[i] = new IndexBlock(i, block_size, index_block_p);
                index_blocks[i].addReference();
            } else {
                throw new IOException("Do not understand index type: " + type);
            }
        }

    }

    /**
     * Closes this index set (cleans up).
     */
    public synchronized void close() {
        if (store != null) {
            for (int i = 0; i < index_blocks.length; ++i) {
                index_blocks[i].removeReference();
            }
            store = null;
            index_blocks = null;
        }
    }

    /**
     * Overwrites all existing index information in this store and sets it to a
     * copy of the given IndexSet object.  The 'source_index' must be a snapshot
     * as returned by the getSnapshotIndexSet method but not necessarily
     * generated from this index set.
     * <p>
     * This will create a new structure within this store that contains the copied
     * index data.  This overwrites any existing data in this store so care should
     * be used when using this method.
     * <p>
     * This method is an optimized method of copying all the index data in an
     * index set and only requires a small buffer in memory.  The index data
     * in 'index_set' is not altered in any way by using this.
     */
    public synchronized void copyAllFrom(IndexSet index_set) throws IOException {

        // Assert that IndexSetStore is initialized
        if (index_blocks == null) {
            throw new RuntimeException(
                    "Can't copy because this IndexSetStore is not initialized.");
        }

        // Drop any indexes in this index store.
        for (int i = 0; i < index_blocks.length; ++i) {
            commitDropIndex(i);
        }

        if (index_set instanceof SnapshotIndexSet) {
            // Cast to SnapshotIndexSet
            SnapshotIndexSet s_index_set = (SnapshotIndexSet) index_set;

            // The number of IndexBlock items to copy.
            int index_count = s_index_set.snapshot_index_blocks.length;

            // Record the old index_header_p
            long old_index_header_p = index_header_p;

            // Create the header in this store
            AreaWriter a = store.createArea(16 + (16 * index_count));
            index_header_p = a.getID();
            a.putInt(1);            // version
            a.putInt(0);            // reserved
            a.putLong(index_count); // number of indexes in the set

            // Fill in the information from the index_set
            for (int i = 0; i < index_count; ++i) {
                IndexBlock source_block = s_index_set.snapshot_index_blocks[i];

                long index_block_p = source_block.copyTo(store);

                a.putInt(1);    // NOTE: Only support for block type 1
                a.putInt(source_block.getBlockSize());
                a.putLong(index_block_p);
            }

            // The header area has now been initialized.
            a.finish();

            // Modify the start area header to point to this new structure.
            start_area.position(8);
            start_area.putLong(index_header_p);
            // Check out the change
            start_area.checkOut();

            // Free space associated with the old header_p
            store.deleteArea(old_index_header_p);
        } else {
            throw new RuntimeException("Can not copy non-IndexSetStore IndexSet");
        }

        // Re-initialize the index
        init(start_area.getID());
    }

    /**
     * Adds to the given ArrayList all the areas in the store that are used by
     * this structure (as Long).
     */
    public void addAllAreasUsed(ArrayList list) throws IOException {
        list.add(new Long(start_area.getID()));
        list.add(new Long(index_header_p));
        for (int i = 0; i < index_blocks.length; ++i) {
            IndexBlock block = index_blocks[i];
            list.add(new Long(block.getPointer()));
            long[] block_pointers = block.getAllBlockPointers();
            for (int n = 0; n < block_pointers.length; ++n) {
                list.add(new Long(block_pointers[n]));
            }
        }
    }

    /**
     * Adds a number of blank index tables to the index store.  For example,
     * we may want this store to contain 16 index lists.
     * <p>
     * NOTE: This doesn't write the updated information to the file.  You must
     *   call 'flush' to write the information to the store.
     */
    public synchronized void addIndexLists(int count, int type, int block_size)
            throws IOException {

        try {
            store.lockForWrite();

            // Allocate a new area for the list
            int new_size = 16 + ((index_blocks.length + count) * 16);
            AreaWriter new_index_area = store.createArea(new_size);
            long new_index_p = new_index_area.getID();
            IndexBlock[] new_index_blocks =
                    new IndexBlock[(index_blocks.length + count)];

            // Copy the existing area
            index_header_area.position(0);
            int version = index_header_area.getInt();
            int reserved = index_header_area.getInt();
            long icount = index_header_area.getLong();
            new_index_area.putInt(version);
            new_index_area.putInt(reserved);
            new_index_area.putLong(icount + count);

            for (int i = 0; i < index_blocks.length; ++i) {
                int itype = index_header_area.getInt();
                int iblock_size = index_header_area.getInt();
                long index_block_p = index_header_area.getLong();

                new_index_area.putInt(itype);
                new_index_area.putInt(iblock_size);
                new_index_area.putLong(index_block_p);

                new_index_blocks[i] = index_blocks[i];
            }

            // Add the new entries
            for (int i = 0; i < count; ++i) {
                long new_blank_block_p = createBlankIndexBlock();

                new_index_area.putInt(type);
                new_index_area.putInt(block_size);
                new_index_area.putLong(new_blank_block_p);

                IndexBlock i_block = new IndexBlock(index_blocks.length + i,
                        block_size, new_blank_block_p);
                i_block.addReference();
                new_index_blocks[index_blocks.length + i] = i_block;

            }

            // Finished initializing the index.
            new_index_area.finish();

            // The old index header pointer
            long old_index_header_p = index_header_p;

            // Update the state of this object,
            index_header_p = new_index_p;
            index_header_area = store.getArea(new_index_p);
            index_blocks = new_index_blocks;

            // Update the start pointer
            start_area.position(8);
            start_area.putLong(new_index_p);
            start_area.checkOut();

            // Free the old header
            store.deleteArea(old_index_header_p);

        } finally {
            store.unlockForWrite();
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
        // Clone the blocks list.  This represents the current snapshot of the
        // index state.
        IndexBlock[] snapshot_index_blocks = (IndexBlock[]) index_blocks.clone();

        // Add this as the reference
        for (int i = 0; i < snapshot_index_blocks.length; ++i) {
            snapshot_index_blocks[i].addReference();
        }

        return new SnapshotIndexSet(snapshot_index_blocks);
    }

    /**
     * Commits the index header with the current values set in 'index_blocks'.
     */
    private synchronized void commitIndexHeader() throws IOException {

        // Make a new index header area for the changed set.
        AreaWriter a = store.createArea(16 + (index_blocks.length * 16));
        long a_p = a.getID();

        a.putInt(1);                      // version
        a.putInt(0);                      // reserved
        a.putLong(index_blocks.length);   // count

        for (int i = 0; i < index_blocks.length; ++i) {
            IndexBlock ind_block = index_blocks[i];
            a.putInt(1);
            a.putInt(ind_block.getBlockSize());
            a.putLong(ind_block.getPointer());
        }

        // Finish creating the updated header
        a.finish();

        // The old index header pointer
        long old_index_header_p = index_header_p;

        // Set the new index header
        index_header_p = a_p;
        index_header_area = store.getArea(index_header_p);

        // Write the change to 'start_p'
        start_area.position(8);
        start_area.putLong(index_header_p);
        start_area.checkOut();

        // Free the old header index
        store.deleteArea(old_index_header_p);

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
     * NOTE: We must be guarenteed that when this method is called no other
     *   calls to other methods in this object can be called.
     */
    public void commitIndexSet(IndexSet index_set) {

        ArrayList removed_blocks = new ArrayList();

        synchronized (this) {

            SnapshotIndexSet s_index_set = (SnapshotIndexSet) index_set;
            IndexIntegerList[] lists = s_index_set.getAllLists();

            try {

                try {
                    store.lockForWrite();

                    // For each IndexIntegerList in the index set,
                    for (int n = 0; n < lists.length; ++n) {
                        // Get the list
                        IndexIntegerList list = (IndexIntegerList) lists[n];
                        int index_num = list.getIndexNumber();
                        // The IndexBlock we are changing
                        IndexBlock cur_index_block = index_blocks[index_num];
                        // Get all the blocks in the list
                        MappedListBlock[] blocks = list.getAllBlocks();

                        // Make up a new block list for this index set.
                        AreaWriter a = store.createArea(16 + (blocks.length * 28));
                        long block_p = a.getID();
                        a.putInt(1);               // version
                        a.putInt(0);               // reserved
                        a.putLong(blocks.length);  // block count
                        for (int i = 0; i < blocks.length; ++i) {
                            MappedListBlock b = blocks[i];

                            long bottom_int = 0;
                            long top_int = 0;
                            int block_size = b.size();
                            if (block_size > 0) {
                                bottom_int = b.bottomInt();
                                top_int = b.topInt();
                            }
                            long b_p = b.getBlockPointer();
                            // Is the block new or was it changed?
                            if (b_p == -1 || b.hasChanged()) {
                                // If this isn't -1 then put this sector on the list of
                                // sectors to delete during GC.
                                if (b_p != -1) {
                                    cur_index_block.addDeletedArea(b_p);
                                }
                                // This is a new block or a block that's been changed
                                // Write the block to the file system
                                b_p = b.writeToStore();
                            }
                            a.putLong(bottom_int);
                            a.putLong(top_int);
                            a.putLong(b_p);
                            a.putInt(block_size | (((int) b.getCompactType()) << 24));

                        }

                        // Finish initializing the area
                        a.finish();

                        // Add the deleted blocks
                        MappedListBlock[] deleted_blocks = list.getDeletedBlocks();
                        for (int i = 0; i < deleted_blocks.length; ++i) {
                            long del_block_p = deleted_blocks[i].getBlockPointer();
                            if (del_block_p != -1) {
                                cur_index_block.addDeletedArea(del_block_p);
                            }
                        }

                        // Mark the current block as deleted
                        cur_index_block.markAsDeleted();

                        // Now create a new IndexBlock object
                        IndexBlock new_index_block =
                                new IndexBlock(index_num, cur_index_block.getBlockSize(), block_p);
                        new_index_block.setParentIndexBlock(cur_index_block);

                        // Add reference to the new one
                        new_index_block.addReference();
                        // Update the index_blocks list
                        index_blocks[index_num] = new_index_block;

                        // We remove this later.
                        removed_blocks.add(cur_index_block);

                    }

                    // Commit the new index header (index_blocks)
                    commitIndexHeader();

                } finally {
                    store.unlockForWrite();
                }

                // Commit finished.

            } catch (IOException e) {
                debug.writeException(e);
                throw new Error("IO Error: " + e.getMessage());
            }

        } // synchronized

        // Remove all the references for the changed blocks,
        int sz = removed_blocks.size();
        for (int i = 0; i < sz; ++i) {
            IndexBlock block = (IndexBlock) removed_blocks.get(i);
            block.removeReference();
        }

    }

    /**
     * Commits a change that drops an index from the index set.  This must be
     * called from within the conglomerate commit.  The actual implementation of
     * this overwrites the index with with a 0 length index.  This is also useful
     * if you want to reindex a column.
     */
    public synchronized void commitDropIndex(int index_num) throws IOException {
        // The IndexBlock we are dropping
        IndexBlock cur_index_block = index_blocks[index_num];
        int block_size = cur_index_block.getBlockSize();

        try {
            store.lockForWrite();

            // Add all the elements to the deleted areas in the block
            long[] all_block_pointers = cur_index_block.getAllBlockPointers();
            for (int i = 0; i < all_block_pointers.length; ++i) {
                cur_index_block.addDeletedArea(all_block_pointers[i]);
            }

            // Mark the current block as deleted
            cur_index_block.markAsDeleted();

            // Make up a new blank block list for this index set.
            long block_p = createBlankIndexBlock();

            // Now create a new IndexBlock object
            IndexBlock new_index_block = new IndexBlock(index_num, block_size, block_p);

            // Add reference to the new one
            new_index_block.addReference();
            // Remove reference to the old
            cur_index_block.removeReference();
            // Update the index_blocks list
            index_blocks[index_num] = new_index_block;

            // Commit the new index header (index_blocks)
            commitIndexHeader();

        } finally {
            store.unlockForWrite();
        }

    }


    // ---------- Inner classes ----------


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
         * The list of IndexBlock object that represent the view of the index set
         * when the view was created.
         */
        private IndexBlock[] snapshot_index_blocks;

        /**
         * The list of IndexIntegerList objects that have been returned via the
         * 'getIndex(n)' method.
         */
        private ArrayList integer_lists;

        /**
         * Set to true when this object is disposed.
         */
        private boolean disposed;


        /**
         * Constructor.
         */
        public SnapshotIndexSet(IndexBlock[] blocks) {

            this.snapshot_index_blocks = blocks;

            // Not disposed.
            disposed = false;

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

        // ---------- Implemented from IndexSet ----------

        public IntegerListInterface getIndex(int n) {
            // Create if not exist.
            if (integer_lists == null) {
                integer_lists = new ArrayList();
            } else {
                // If this list has already been created, return it
                for (int o = 0; o < integer_lists.size(); ++o) {
                    IndexIntegerList i_list = (IndexIntegerList) integer_lists.get(o);
                    if (i_list.getIndexNumber() == n) {
                        return i_list;
//            throw new Error(
//                        "IntegerListInterface already created for this n.");
                    }
                }
            }

            try {

                IndexIntegerList ilist =
                        snapshot_index_blocks[n].createIndexIntegerList();
                integer_lists.add(ilist);
                return ilist;

            } catch (IOException e) {
                debug.writeException(e);
                throw new RuntimeException("IO Error: " + e.getMessage());
            }

        }

        public void dispose() {
            if (!disposed) {

                if (integer_lists != null) {
                    for (int i = 0; i < integer_lists.size(); ++i) {
                        IndexIntegerList ilist = (IndexIntegerList) integer_lists.get(i);
                        ilist.dispose();
                    }
                    integer_lists = null;
                }

                // Release reference to the index_blocks;
                for (int i = 0; i < snapshot_index_blocks.length; ++i) {
                    IndexBlock iblock = snapshot_index_blocks[i];
                    iblock.removeReference();
                }
                snapshot_index_blocks = null;

                disposed = true;
            }
        }

        public void finalize() {
            try {
                if (!disposed) {
                    dispose();
                }
            } catch (Throwable e) {
                debug.write(Lvl.ERROR, this, "Finalize error: " + e.getMessage());
                debug.writeException(e);
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
        private long first_entry;

        /**
         * The last entry in the block.
         */
        private long last_entry;

        /**
         * A pointer to the area where this block can be found.
         */
        private long block_p;

        /**
         * Lock object.
         */
        private Object lock = new Object();

        /**
         * Set to true if the loaded block is mutable.
         */
        private boolean mutable_block;

        /**
         * How this block is compacted in the store.  If this is 1 the elements are
         * stored as shorts, if it is 2 - ints, and if it is 3 - longs.
         */
        private byte compact_type;

        /**
         * The maximum size of the block.
         */
        private final int max_block_size;

        /**
         * Constructor.
         */
        public MappedListBlock(long first_e, long last_e,
                               long mapped_p, int size, byte compact_type,
                               int max_block_size) {
            this.first_entry = first_e;
            this.last_entry = last_e;
            this.block_p = mapped_p;
            this.compact_type = compact_type;
            this.max_block_size = max_block_size;
            count = size;
            array = null;
        }

        /**
         * Creates an empty block.
         */
        public MappedListBlock(int block_size_in) {
            super(block_size_in);
            this.block_p = -1;
            this.max_block_size = block_size_in;
        }

        /**
         * Returns a pointer to the area that contains this block.
         */
        public long getBlockPointer() {
            return block_p;
        }

        /**
         * Returns the compact type of this block.
         */
        public byte getCompactType() {
            return compact_type;
        }

        /**
         * Copies the index data in this block to a new block in the given store
         * and returns a pointer to the new block.
         */
        public long copyTo(Store dest_store) throws IOException {
            // The number of bytes per entry
            int entry_size = compact_type;
            // The total size of the entry.
            int area_size = (count * entry_size);

            // Allocate the destination area
            AreaWriter dest = dest_store.createArea(area_size);
            long dest_block_p = dest.getID();
            store.getArea(block_p).copyTo(dest, area_size);
            dest.finish();

            return dest_block_p;
        }

        /**
         * Writes this block to a new sector in the index file and updates the
         * information in this object accordingly.
         * <p>
         * Returns the sector the block was written to.
         */
        public long writeToStore() throws IOException {
            // Convert the int[] array to a byte[] array.

            // First determine how we compact this int array into a byte array.  If
            // all the values are < 32768 then we store as shorts
            long largest_val = 0;
            for (int i = 0; i < count; ++i) {
                long v = (long) array[i];
                if (Math.abs(v) > Math.abs(largest_val)) {
                    largest_val = v;
                }
            }

            long lv = largest_val;
            if (lv >> 7 == 0 || lv >> 7 == -1) {
                compact_type = 1;
            } else if (lv >> 15 == 0 || lv >> 15 == -1) {
                compact_type = 2;
            } else if (lv >> 23 == 0 || lv >> 23 == -1) {
                compact_type = 3;
            }
            // NOTE: in the future we'll want to determine if we are going to store
            //   as an int or long array.
            else {
                compact_type = 4;
            }

            // The number of bytes per entry
            int entry_size = compact_type;
            // The total size of the entry.
            int area_size = (count * entry_size);

            // Allocate an array to buffer the block to
            byte[] arr = new byte[area_size];
            // Fill the array
            int p = 0;
            for (int i = 0; i < count; ++i) {
                int v = array[i];
                for (int n = entry_size - 1; n >= 0; --n) {
                    arr[p] = (byte) ((v >>> (n * 8)) & 0x0FF);
                    ++p;
                }
            }

            // Create an area to store this
            AreaWriter a = store.createArea(area_size);
            block_p = a.getID();
            // Write to the area
            a.put(arr, 0, area_size);
            // And finish the area initialization
            a.finish();

            // Once written, the block is invalidated
            lock = null;

            return block_p;
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

                // Create the int array
                array = new int[max_block_size];

                // The number of bytes per entry
                int entry_size = compact_type;
                // The total size of the entry.
                int area_size = (count * entry_size);

                // Read in the byte array
                byte[] buf = new byte[area_size];
                try {
                    store.getArea(block_p).get(buf, 0, area_size);
                } catch (IOException e) {
                    debug.write(Lvl.ERROR, this, "block_p = " + block_p);
                    debug.writeException(e);
                    throw new Error("IO Error: " + e.getMessage());
                }

                // Uncompact it into the int array
                int p = 0;
                for (int i = 0; i < count; ++i) {
                    int v = (((int) buf[p]) << ((entry_size - 1) * 8));
                    ++p;
                    for (int n = entry_size - 2; n >= 0; --n) {
                        v = v | ((((int) buf[p]) & 0x0FF) << (n * 8));
                        ++p;
                    }
                    array[i] = v;
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
            return max_block_size;
        }

        /**
         * Makes the block mutable if it is immutable.  We must be synchronized on
         * 'lock' before this method is called.
         */
        private void prepareMutate(boolean immutable) {
            // If list is to be mutable
            if (!immutable && !mutable_block) {
                array = (int[]) array.clone();
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
                    return (int) last_entry;
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
                    return (int) first_entry;
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
         * The maximum block size.
         */
        private final int max_block_size;

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
        public IndexIntegerList(int index_num, int max_block_size,
                                MappedListBlock[] blocks) {
            super(blocks);
            this.index_num = index_num;
            this.max_block_size = max_block_size;
        }

        /**
         * Creates a new block for the list.
         */
        protected IntegerListBlockInterface newListBlock() {
            if (!disposed) {
                return new MappedListBlock(max_block_size);
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
            return (MappedListBlock[])
                    block_list.toArray(new MappedListBlock[block_list.size()]);
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

    /**
     * Represents a single 'Index block' area in the store.
     * <p>
     * An index block area contains an entry for each index element in an index.
     * Each entry is 28 bytes in size and the area has a 16 byte header.
     * <p>
     * HEADER: ( version (int), reserved (int), index size (long) ) <br>
     * ENTRY: ( first entry (long), last entry (long),
     *          index element pointer (long), type/element size (int) )
     * <p>
     * type/element size contains the number of elements in the block, and the
     * block compaction factor.  For example, type 1 means the block contains
     * short sized index values, 2 is int sized index values, and 3 is long
     * sized index values.
     */
    private class IndexBlock {

        /**
         * The number of references to this object.  When this reaches 0, it is
         * safe to free any resources that this block deleted.
         */
        private int reference_count;

        /**
         * The index of this block in the index set.
         */
        private final int index_num;

        /**
         * A pointer that references the area in the store.
         */
        private final long index_block_p;

        /**
         * The total number of entries in the index block.
         */
        private long block_entries;

        /**
         * The block size of elements in this block.
         */
        private final int block_size;

        /**
         * The list of deleted areas that can safely be disposed when this object
         * is disposed.
         */
        private ArrayList deleted_areas;

        /**
         * True if this block is marked as deleted.
         */
        private boolean deleted = false;

        /**
         * Set to true when this index block is freed from the index store.
         */
        private boolean freed = false;

        /**
         * The parent IndexBlock.  This block is a child modification of the parent.
         */
        private IndexBlock parent_block;

        /**
         * Constructs the IndexBlock.
         */
        IndexBlock(int index_num, int block_size, long index_block_p)
                throws IOException {
            this.index_num = index_num;
            this.block_size = block_size;
            this.index_block_p = index_block_p;

            // Read the index count
            Area index_block_area = store.getArea(index_block_p);
            index_block_area.position(8);
            block_entries = index_block_area.getLong();

            reference_count = 0;

        }

        /**
         * Sets the parent IndexBlock, the index that this index block succeeded.
         */
        void setParentIndexBlock(IndexBlock parent) {
            this.parent_block = parent;
        }

        /**
         * Returns a list of pointers to all mapped blocks.
         */
        long[] getAllBlockPointers() throws IOException {
            // Create an area for the index block pointer
            Area index_block_area = store.getArea(index_block_p);

            // First create the list of block entries for this list
            long[] blocks = new long[(int) block_entries];
            if (block_entries != 0) {
                index_block_area.position(16);
                for (int i = 0; i < block_entries; ++i) {
                    // NOTE: We cast to 'int' here because of internal limitations.
                    index_block_area.getLong();
                    index_block_area.getLong();
                    long element_p = index_block_area.getLong();
                    index_block_area.getInt();

                    blocks[i] = element_p;
                }
            }

            return blocks;
        }

        /**
         * Creates and returns an array of all the MappedListBlock objects that make
         * up this view of the index integer list.
         */
        private MappedListBlock[] createMappedListBlocks() throws IOException {
            // Create an area for the index block pointer
            Area index_block_area = store.getArea(index_block_p);
            // First create the list of block entries for this list
            MappedListBlock[] blocks = new MappedListBlock[(int) block_entries];
            if (block_entries != 0) {
                index_block_area.position(16);
                for (int i = 0; i < block_entries; ++i) {
                    // NOTE: We cast to 'int' here because of internal limitations.
                    long first_entry = index_block_area.getLong();
                    long last_entry = index_block_area.getLong();
                    long element_p = index_block_area.getLong();
                    int type_size = index_block_area.getInt();

                    // size is the first 24 bits (max size = 16MB)
                    int element_count = type_size & 0x0FFF;
                    byte type = (byte) ((type_size >>> 24) & 0x0F);

                    blocks[i] = new MappedListBlock(first_entry, last_entry, element_p,
                            element_count, type, block_size);
                }
            }
            return blocks;
        }

        /**
         * Creates and returns a mutable IndexIntegerList object based on this
         * view of the index.
         */
        IndexIntegerList createIndexIntegerList() throws IOException {
            // Create the MappedListBlock objects for this view
            MappedListBlock[] blocks = createMappedListBlocks();
            // And return the IndexIntegerList
            return new IndexIntegerList(index_num, block_size, blocks);
        }

        /**
         * Copies this index block to the given Store and returns a pointer to the
         * block within the store.
         */
        long copyTo(Store dest_store) throws IOException {
            // Create the MappedListBlock object list for this view
            MappedListBlock[] blocks = createMappedListBlocks();
            try {
                dest_store.lockForWrite();
                // Create the header area in the store for this block
                AreaWriter a = dest_store.createArea(16 + (blocks.length * 28));
                long block_p = a.getID();

                a.putInt(1);               // version
                a.putInt(0);               // reserved
                a.putLong(blocks.length);  // block count
                for (int i = 0; i < blocks.length; ++i) {
                    MappedListBlock entry = blocks[i];
                    long b_p = entry.copyTo(dest_store);
                    int block_size = entry.size();
                    a.putLong(entry.first_entry);
                    a.putLong(entry.last_entry);
                    a.putLong(b_p);
                    a.putInt(block_size | (((int) entry.getCompactType()) << 24));
                }

                // Now finish the area initialization
                a.finish();

                // Return pointer to the new area in dest_store.
                return block_p;

            } finally {
                dest_store.unlockForWrite();
            }

        }

        /**
         * Recursively calls through the block hierarchy and deletes and blocks
         * that can be deleted.
         */
        private boolean deleteBlockChain() {
            boolean parent_deleted = true;
            if (parent_block != null) {
                parent_deleted = parent_block.deleteBlockChain();
                if (parent_deleted) {
                    parent_block = null;
                }
            }

            // If the parent is deleted,
            if (parent_deleted) {
                // Can we delete this block?
                if (reference_count <= 0) {
                    if (deleted && deleted_areas != null) {
                        deleteAllAreas(deleted_areas);
                    }
                    deleted_areas = null;
                } else {
                    // We can't delete this block so return false
                    return false;
                }
            }

            return parent_deleted;
        }

        /**
         * Adds a reference to this object.
         */
        public synchronized void addReference() {
            if (freed) {
                throw new RuntimeException("Assertion failed: Block was freed.");
            }
            ++reference_count;
        }

        /**
         * Removes a reference to this object.
         */
        public void removeReference() {
            boolean pending_delete = false;
            synchronized (this) {
                --reference_count;
                if (reference_count <= 0) {
                    if (freed) {
                        throw new RuntimeException(
                                "Assertion failed: remove reference called too many times.");
                    }
                    if (!deleted && deleted_areas != null) {
                        throw new RuntimeException(
                                "Assertion failed: !deleted and deleted_areas != null");
                    }
                    freed = true;
                    if (deleted) {
                        addDeletedArea(index_block_p);
                        // Delete these areas
                        pending_delete = true;
                    }
                }
            } // synchronized(this)
            if (pending_delete) {
                synchronized (IndexSetStore.this) {
                    deleteBlockChain();
                }
            }
        }

        /**
         * Returns the number of references to this object.
         */
        public synchronized int getReferenceCount() {
            return reference_count;
        }

        /**
         * Returns the block size that has been set on this list.
         */
        public int getBlockSize() {
            return block_size;
        }

        /**
         * Returns the pointer to this index block in the store.
         */
        public long getPointer() {
            return index_block_p;
        }

        /**
         * Marks this block as deleted.
         */
        public synchronized void markAsDeleted() {
            deleted = true;
        }

        /**
         * Adds to the list of deleted areas in this block.
         */
        public synchronized void addDeletedArea(long pointer) {
            if (deleted_areas == null) {
                deleted_areas = new ArrayList();
            }

            deleted_areas.add(new Long(pointer));

        }

    }

}

