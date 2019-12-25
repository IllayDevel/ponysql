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

import java.util.ArrayList;
import java.io.IOException;

import com.pony.store.Store;
import com.pony.store.AreaWriter;
import com.pony.store.MutableArea;

/**
 * A structure that provides a fast way to read and write fixed sized nodes in
 * a Store object.  Each node in the list is of fixed size.
 * <p>
 * This structure can locate a node in the list very quickly.  However, the
 * structure can not be mutated.  For example, deleting node '4' will make the
 * node available for recycling but will not shift any nodes after 4 in the
 * list up by one.
 * <p>
 * Once a node is allocated from the list its position will not change.
 * <p>
 * This structure does not provide versioning features.
 * <p>
 * The structure is composed of two element types - the header and the list
 * block elements.  The header is resembled by the following diagram;
 * <p>
 *      LIST BLOCK HEADER
 *   +-------------------------------+
 *   | 4 MAGIC                       |
 *   | 4 list block count            |
 *   | 8 (reserved for delete chain) |
 *   | 8 pointer to list block 0     |
 *   | 8 pointer to list block 1     |
 *   .  ... etc ...                  .
 *   | 8 pointer to list block 63    |
 *   +-------------------------------+
 * </pre>
 * <p>
 * The first list block element is 32 entries in size, the second list block is
 * 64 entries in size, etc.  Each entry of the list block element is of fixed
 * size.
 * <p>
 * This class is NOT thread safe.
 *
 * @author Tobias Downer
 */

public class FixedRecordList {

    /**
     * The magic value for fixed record list structures.
     */
    private final static int MAGIC = 0x087131AA;

    /**
     * The backing Store object that persistantly stores the structure.
     */
    private final Store store;

    /**
     * The fixed size of the elements in the list.
     */
    private final int element_size;


    /**
     * A pointer to the list header area.
     */
    private long list_header_p;

    /**
     * The header for the list blocks.
     */
    private MutableArea list_header_area;

    /**
     * The number of blocks in the list block.
     */
    private int list_block_count;

    /**
     * Pointers to the blocks in the list block.
     */
    private final long[] list_block_element;
    private final MutableArea[] list_block_area;


    /**
     * Constructs the structure.
     */
    public FixedRecordList(Store store, int element_size) {
        this.store = store;
        this.element_size = element_size;
        list_block_element = new long[64];
        list_block_area = new MutableArea[64];
    }

    /**
     * Creates the structure in the store, and returns a pointer to the structure.
     */
    public long create() throws IOException {
        // Allocate space for the list header (8 + 8 + (64 * 8))
        AreaWriter writer = store.createArea(528);
        list_header_p = writer.getID();
        writer.putInt(MAGIC);
        writer.finish();

        list_header_area = store.getMutableArea(list_header_p);
        list_block_count = 0;
        updateListHeaderArea();

        return list_header_p;
    }

    /**
     * Initializes the structure from the store.
     */
    public void init(long list_pointer) throws IOException {
        list_header_p = list_pointer;
        list_header_area = store.getMutableArea(list_header_p);

        int magic = list_header_area.getInt();          // MAGIC
        if (magic != MAGIC) {
            throw new IOException("Incorrect magic for list block. [magic=" +
                    magic + "]");
        }
        this.list_block_count = list_header_area.getInt();
        list_header_area.getLong();
        for (int i = 0; i < list_block_count; ++i) {
            long block_pointer = list_header_area.getLong();
            list_block_element[i] = block_pointer;
            list_block_area[i] = store.getMutableArea(block_pointer);
        }
    }

    /**
     * Adds to the given ArrayList all the areas in the store that are used by
     * this structure (as Long).
     */
    public void addAllAreasUsed(ArrayList list) throws IOException {
        list.add(list_header_p);
        for (int i = 0; i < list_block_count; ++i) {
            list.add(list_block_element[i]);
        }
    }

    /**
     * Returns the 8 byte long that is reserved for storing the delete chain
     * (if there is one).
     */
    public long getReservedLong() throws IOException {
        list_header_area.position(8);
        return list_header_area.getLong();
    }

    /**
     * Sets the 8 byte long that is reserved for storing the delete chain
     * (if there is one).
     */
    public void setReservedLong(long v) throws IOException {
        list_header_area.position(8);
        list_header_area.putLong(v);
        list_header_area.checkOut();
    }

    /**
     * Updates the list header area from the information store within the
     * state of this object.  This should only be called when a new block is
     * added to the list block, or the store is created.
     */
    private void updateListHeaderArea() throws IOException {
        list_header_area.position(4);
        list_header_area.putInt(list_block_count);
        list_header_area.position(16);
        for (int i = 0; i < list_block_count; ++i) {
            list_header_area.putLong(list_block_element[i]);
        }
        list_header_area.checkOut();
    }

    /**
     * Returns an Area object from the list block area with the position over
     * the record entry requested.  Note that the Area object can only be safely
     * used if there is a guarentee that no other access to this object while the
     * area object is accessed.
     */
    public MutableArea positionOnNode(final long record_number)
            throws IOException {
        // What block is this record in?
        int bit = 0;
        long work = record_number + 32;
        while (work != 0) {
            work = work >> 1;
            ++bit;
        }
        long start_val = (1 << (bit - 1)) - 32;
        int block_offset = bit - 6;
        long record_offset = record_number - start_val;

        // Get the pointer to the block that contains this record status
        MutableArea block_area = list_block_area[block_offset];
//    long tempv = (record_offset * element_size);
//    int position_to = (int) tempv;
//    if (tempv == 1) {
//      ++tempv;
//    }
//    block_area.position(position_to);
        block_area.position((int) (record_offset * element_size));

        return block_area;
    }

    /**
     * Returns the number of block elements in this list structure.  This will
     * return a number between 0 and 63 (inclusive).
     */
    public int listBlockCount() {
        return list_block_count;
    }

    /**
     * Returns the total number of nodes that are currently addressable by this
     * list structure.  For example, if the list contains 0 blocks then there are
     * no addressable nodes.  If it contains 1 block, there are 32 addressable
     * nodes.  If it contains 2 blocks, there are 64 + 32 = 96 nodes.  3 blocks =
     * 128 + 64 + 32 = 224 nodes.
     */
    public long addressableNodeCount() {
        return listBlockFirstPosition(list_block_count);
    }

    /**
     * Returns the number of nodes that can be stored in the given block, where
     * block 0 is the first block (32 addressable nodes).
     */
    public long listBlockNodeCount(int block_number) {
        return 32L << block_number;
    }

    /**
     * Returns the index of the first node in the given block number.  For
     * example, this first node of block 0 is 0, the first node of block 1 is
     * 32, the first node of block 2 is 96, etc.
     */
    public long listBlockFirstPosition(int block_number) {
        long start_index = 0;
        int i = block_number;
        long diff = 32;
        while (i > 0) {
            start_index = start_index + diff;
            diff = diff << 1;
            --i;
        }
        return start_index;
    }

    /**
     * Increases the size of the list structure so it may accomodate more record
     * entries.  This simply adds a new block for more nodes.
     */
    public void increaseSize() throws IOException {
        // The size of the block
        long size_of_block = 32L << list_block_count;
        // Allocate the new block in the store
        AreaWriter writer = store.createArea(size_of_block * element_size);
        long nblock_p = writer.getID();
        writer.finish();
        MutableArea nblock_area = store.getMutableArea(nblock_p);
        // Update the block list
        list_block_element[list_block_count] = nblock_p;
        list_block_area[list_block_count] = nblock_area;
        ++list_block_count;
        // Update the list header,
        updateListHeaderArea();
    }

    /**
     * Decreases the size of the list structure.  This should be used with care
     * because it deletes all nodes in the last block.
     */
    public void decreaseSize() throws IOException {
        --list_block_count;
        // Free the top block
        store.deleteArea(list_block_element[list_block_count]);
        // Help the GC
        list_block_area[list_block_count] = null;
        list_block_element[list_block_count] = 0;
        // Update the list header.
        updateListHeaderArea();
    }

}

