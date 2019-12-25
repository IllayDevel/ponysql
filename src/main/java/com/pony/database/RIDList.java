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
import java.util.ArrayList;

import com.pony.util.IntegerVector;
import com.pony.util.IntegerIterator;
import com.pony.util.IndexComparator;
import com.pony.util.BlockIntegerList;
import com.pony.debug.*;

/**
 * This is an optimization to help sorting over a column in a table.  It is
 * an aid for sorting rows in a query without having to resort to cell
 * lookup.  It uses memory to speed up sorting.
 * <p>
 * Sorting data is a central part of any database system.  This object
 * maintains a list of values that represent each cell in a column
 * relationally.
 * <p>
 * For example, consider the following data in a column:<p>
 *   { 'a', 'g', 'i', 'b', 'a' }
 * <p>
 * A RID list is a set of integer values that represents a column relationally.
 * So the above column data could be represented in a RID list as:<p>
 *   { 1, 3, 4, 2, 1 }
 * <p>
 * If 'c' is inserted into the above list, there is not an integer value that
 * we can use to represent this cell.  In this case, the RID list is
 * renumbered to make room for the insertion.
 *
 * @author Tobias Downer
 */

final class RIDList {

    /**
     * The TransactionSystem that we are in.
     */
    private final TransactionSystem system;

    /**
     * The master table for the column this is in.
     */
    private final MasterTableDataSource master_table;

    /**
     * The TableName of the table.
     */
    private final TableName table_name;

    /**
     * The name of the column of this rid list.
     */
    private final String column_name;

    /**
     * The column in the master table.
     */
    private final int column;

    /**
     * The sorted list of rows in this set.  This is sorted from min to max
     * (not sorted by row number - sorted by entity row value).
     */
    private BlockIntegerList set_list;

    /**
     * The contents of our list.
     */
    private IntegerVector rid_list;

    /**
     * The difference between each hash when the uid_list was last created or
     * rehashed.
     */
    private int hash_rid_difference;

    /**
     * The IndexComparator that we use to refer elements in the set to actual
     * data objects.
     */
    private IndexComparator set_comparator;

    /**
     * Set to true if this list has been fully built.
     */
    private boolean is_built;

    /**
     * The RID list build state.
     * 0 - list not built.
     * 1 - stage 1 (set_list being built).
     * 2 - state 2 (rid_list being built).
     * 3 - pending modifications.
     * 4 - finished
     */
    private int build_state = 0;

    /**
     * A list of modifications made to the index while it is being built.
     */
    private IntegerVector concurrent_modification_info;
    private ArrayList concurrent_modification_data;
    private final Object modification_lock = new Object();

    /**
     * Set to true if a request to build the rid list is on the event dispatcher.
     */
    private boolean request_processing = false;

    /**
     * Constructs the object.
     */
    RIDList(MasterTableDataSource master_table, int column) {
//    rid_list = new IntegerVector();
        this.master_table = master_table;
        this.system = master_table.getSystem();
        this.column = column;

        DataTableDef table_def = master_table.getDataTableDef();
        table_name = table_def.getTableName();
        column_name = table_def.columnAt(column).getName();

        is_built = false;
        setupComparator();
    }

    /**
     * Returns a DebugLogger object that we can use to log debug messages.
     */
    public final DebugLogger Debug() {
        return master_table.Debug();
    }

    /**
     * Sets the internal comparator that enables us to sort and lookup on the
     * data in this column.
     */
    private void setupComparator() {
        set_comparator = new IndexComparator() {

            private int internalCompare(int index, TObject cell2) {
                TObject cell1 = getCellContents(index);
                return cell1.compareTo(cell2);
            }

            public int compare(int index, Object val) {
                return internalCompare(index, (TObject) val);
            }

            public int compare(int index1, int index2) {
                TObject cell = getCellContents(index2);
                return internalCompare(index1, cell);
            }
        };
    }

    /**
     * Gets the cell at the given row in the column of the master table.
     */
    private TObject getCellContents(int row) {
        return master_table.getCellContents(column, row);
    }

    /**
     * Calculates the 'hash_rid_difference' variable.  This dictates the
     * difference between hashing entries.
     */
    private void calcHashRIDDifference(int size) {
        if (size == 0) {
            hash_rid_difference = 32;
        } else {
            hash_rid_difference = (65536 * 4096) / size;
            if (hash_rid_difference > 16384) {
                hash_rid_difference = 16384;
            } else if (hash_rid_difference < 8) {
                hash_rid_difference = 8;
            }
        }

//    hash_rid_difference = 2;
//    System.out.println(hash_rid_difference);
    }


    /**
     * Rehashes the entire rid list.  This goes through the entire list from
     * first sorted entry to last and spaces out each rid so that there's 16
     * numbers between each entry.
     */
    private int rehashRIDList(int old_rid_place) {
        calcHashRIDDifference(set_list.size());

        int new_rid_place = -1;

        int cur_rid = 0;
        int old_rid = 0;
        IntegerIterator iterator = set_list.iterator();

        while (iterator.hasNext()) {
            int row_index = iterator.next();
            if (row_index >= 0 && row_index < rid_list.size()) {
                int old_value = rid_list.intAt(row_index);
                int new_value;

                if (old_value == 0) {
                    cur_rid += hash_rid_difference;
                    new_rid_place = cur_rid;
                } else {
                    if (old_value != old_rid) {
                        old_rid = old_value;
                        cur_rid += hash_rid_difference;
                        new_value = cur_rid;
                    } else {
                        new_value = cur_rid;
                    }
                    rid_list.placeIntAt(new_value, row_index);
                }
            }
        }

        if (new_rid_place == -1) {
            throw new Error(
                    "Post condition not correct - new_rid_place shouldn't be -1");
        }

        system.stats().increment("RIDList.rehash_rid_table");

        return new_rid_place;
    }


    /**
     * Algorithm for inserting a new row into the rid table.  For most cases
     * this should be a very fast method.
     * <p>
     * NOTE: This must never be called from anywhere except inside
     *   MasterTableDataStore.
     *
     * @param cell the cell to insert into the list.
     * @param row the row number.
     */
    void insertRID(TObject cell, int row) {
        // NOTE: We are guarenteed to be synchronized on master_table when this
        //   is called.

        synchronized (modification_lock) {

            // If state isn't pre-build or finished, then note this modification.
            if (build_state > 0 && build_state < 4) {
                concurrent_modification_info.addInt(1);
                concurrent_modification_info.addInt(row);
                concurrent_modification_data.add(cell);
                return;
            }

            // Only register if this list has been created.
            if (rid_list == null) {
                return;
            }

        }

        // Place a zero to mark the new row
        rid_list.placeIntAt(0, row);

        // Insert this into the set_list.
        set_list.insertSort(cell, row, set_comparator);

        int given_rid = -1;
        TObject previous_cell;

        // The index of this cell in the list
        int set_index = set_list.searchLast(cell, set_comparator);

        if (set_list.get(set_index) != row) {
            throw new Error(
                    "set_list.searchLast(cell) didn't turn up expected row.");
        }

        int next_set_index = set_index + 1;
        if (next_set_index >= set_list.size()) {
            next_set_index = -1;
        }
        int previous_set_index = set_index - 1;

        int next_rid;
        if (next_set_index > -1) {
            next_rid = rid_list.intAt(set_list.get(next_set_index));
        } else {
            if (previous_set_index > -1) {
                // If at end and there's a previous set then use that as the next
                // rid.
                next_rid = rid_list.intAt(set_list.get(previous_set_index)) +
                        (hash_rid_difference * 2);
            } else {
                next_rid = (hash_rid_difference * 2);
            }
        }
        int previous_rid;
        if (previous_set_index > -1) {
            previous_rid = rid_list.intAt(set_list.get(previous_set_index));
        } else {
            previous_rid = 0;
        }

        // Are we the same as the previous or next cell in the list?
        if (previous_set_index > -1) {
            previous_cell = getCellContents(set_list.get(previous_set_index));
            if (previous_cell.compareTo(cell) == 0) {
                given_rid = previous_rid;
            }
        }

        // If not given a rid yet,
        if (given_rid == -1) {
            if (previous_rid + 1 == next_rid) {
                // There's no room so we have to rehash the rid list.
                given_rid = rehashRIDList(next_rid);
            } else {
                given_rid = ((next_rid + 1) + (previous_rid - 1)) / 2;
            }
        }

        // Finally (!!) - set the rid for this row.
        rid_list.placeIntAt(given_rid, row);

    }

    /**
     * Removes a RID entry from the given row.  This <b>MUST</b> only be
     * called when the row is perminantly removed from the table (eg. by the
     * row garbage collector).
     * <p>
     * NOTE: This must never be called from anywhere except inside
     *   MasterTableDataStore.
     */
    void removeRID(int row) {
        // NOTE: We are guarenteed to be synchronized on master_table when this
        //   is called.

        synchronized (modification_lock) {

            // If state isn't pre-build or finished, then note this modification.
            if (build_state > 0 && build_state < 4) {
                concurrent_modification_info.addInt(2);
                concurrent_modification_info.addInt(row);
                return;
            }

            // Only register if this list has been created.
            if (rid_list == null) {
                return;
            }

        }

        try {
            // Remove from the set_list index.
            TObject cell = getCellContents(row);
            int removed = set_list.removeSort(cell, row, set_comparator);
        } catch (Error e) {
            System.err.println("RIDList: " + table_name + "." + column_name);
            throw e;
        }

    }

    /**
     * Requests that a rid_list should be built for this column.  The list will
     * be built on the database dispatcher thread.
     */
    void requestBuildRIDList() {
        if (!isBuilt()) {
            if (!request_processing) {
                request_processing = true;
                // Wait 10 seconds to build rid list.
                system.postEvent(10000, system.createEvent(this::createRIDCache));
            }
        }
    }

    /**
     * If rid_list is null then create it now.
     * <p>
     * NOTE: This must never be called from anywhere except inside
     *   MasterTableDataStore.
     */
    private void createRIDCache() {

        try {

            // If the master table is closed then return
            // ISSUE: What if this happens while we are constructing the list?
            if (master_table.isClosed()) {
                return;
            }

            long time_start = System.currentTimeMillis();
            long time_took;
            int rid_list_size;

            int set_size;

            synchronized (master_table) {
                synchronized (modification_lock) {

                    if (is_built) {
                        return;
                    }

                    // Set the build state
                    build_state = 1;
                    concurrent_modification_info = new IntegerVector();
                    concurrent_modification_data = new ArrayList();

                    // The set_list (complete index of the master table).
                    set_size = master_table.rawRowCount();
                    set_list = new BlockIntegerList();
                    // Go through the master table and build set_list.
                    for (int r = 0; r < set_size; ++r) {
                        if (!master_table.recordDeleted(r)) {
                            TObject cell = getCellContents(r);
                            set_list.insertSort(cell, r, set_comparator);
                        }
                    }
                    // Now we have a complete/current index, including uncommitted,
                    // and committed added and removed rows, of the given column

                    // Add a root lock to the table
                    master_table.addRootLock();

                } // synchronized (modification_lock)
            } // synchronized master_table

            try {

                // Go through and work out the rid values for the list.  We know
                // that 'set_list' is correct and no entries can be deleted from it
                // until we relinquish the root lock.

                calcHashRIDDifference(set_size);

                rid_list = new IntegerVector(set_size + 128);

                // Go through 'set_list'.  All entries that are equal are given the
                // same rid.
                if (set_list.size() > 0) {   //set_size > 0) {
                    int cur_rid = hash_rid_difference;
                    IntegerIterator iterator = set_list.iterator();
                    int row_index = iterator.next();
                    TObject last_cell = getCellContents(row_index);
                    rid_list.placeIntAt(cur_rid, row_index);

                    while (iterator.hasNext()) {
                        row_index = iterator.next();
                        TObject cur_cell = getCellContents(row_index);
                        int cmp = cur_cell.compareTo(last_cell);
                        if (cmp > 0) {
                            cur_rid += hash_rid_difference;
                        } else if (cmp < 0) {  // ASSERTION
                            // If current cell is less than last cell then the list ain't
                            // sorted!
                            throw new Error("Internal Database Error: Index is corrupt " +
                                    " - InsertSearch list is not sorted.");
                        }
                        rid_list.placeIntAt(cur_rid, row_index);

                        last_cell = cur_cell;
                    }
                }

                // Final stage, insert final changes,
                // We lock the master_table so we are guarenteed no changes to the
                // table can happen during the final stage.
                synchronized (master_table) {
                    synchronized (modification_lock) {
                        build_state = 4;

                        // Make any modifications to the list that occured during the time
                        // we were building the RID list.
                        int mod_size = concurrent_modification_info.size();
                        int i = 0;
                        int m_data = 0;
                        int insert_count = 0;
                        int remove_count = 0;
                        while (i < mod_size) {
                            int type = concurrent_modification_info.intAt(i);
                            int row = concurrent_modification_info.intAt(i + 1);
                            // An insert
                            if (type == 1) {
                                TObject cell =
                                        (TObject) concurrent_modification_data.get(m_data);
                                insertRID(cell, row);
                                ++m_data;
                                ++insert_count;
                            }
                            // A remove
                            else if (type == 2) {
                                removeRID(row);
                                ++remove_count;
                            } else {
                                throw new Error("Unknown modification type.");
                            }

                            i += 2;
                        }

                        if (remove_count > 0) {
                            Debug().write(Lvl.ERROR, this,
                                    "Assertion failed: It should not be possible to remove " +
                                            "rows during a root lock when building a RID list.");
                        }

                        concurrent_modification_info = null;
                        concurrent_modification_data = null;

                        // Log the time it took
                        time_took = System.currentTimeMillis() - time_start;
                        rid_list_size = rid_list.size();

                        is_built = true;

                    }
                } // synchronized (modification_lock)

            } finally {
                // Must guarentee we remove the root lock from the master table
                master_table.removeRootLock();
            }

            Debug().write(Lvl.MESSAGE, this,
                    "RID List " + table_name.toString() + "." + column_name +
                            " Initial Size = " + rid_list_size);
            Debug().write(Lvl.MESSAGE, this,
                    "RID list built in " + time_took + "ms.");

            // The number of rid caches created.
            system.stats().increment(
                    "{session} RIDList.rid_caches_created");
            // The total size of all rid indices that we have created.
            system.stats().add(rid_list_size,
                    "{session} RIDList.rid_indices");

        } catch (IOException e) {
            throw new Error("IO Error: " + e.getMessage());
        }

    }

    /**
     * Quick way of determining if the RID list has been built.
     */
    boolean isBuilt() {
        synchronized (modification_lock) {
            return is_built;
        }
    }

    /**
     * Given an unsorted set of rows in this table, this will return the row
     * list sorted in descending order.  This only uses the information from
     * within this list to make up the sorted result, and does not reference any
     * data in the master table.
     * <p>
     * SYNCHRONIZATION: This does not lock the master_table because it doesn't
     *   use any information in it.
     */
    BlockIntegerList sortedSet(final IntegerVector row_set) {

        // The length of the set to order
        int row_set_length = row_set.size();

        // This will be 'row_set' sorted by its entry lookup.  This must only
        // contain indices to row_set entries.
        BlockIntegerList new_set = new BlockIntegerList();

        // The comparator we use to sort
        IndexComparator comparator = new IndexComparator() {
            public int compare(int index, Object val) {
                int rid_val = rid_list.intAt(row_set.intAt(index));
                int rid_val2 = (Integer) val;
                return rid_val - rid_val2;
            }

            public int compare(int index1, int index2) {
                throw new Error("Shouldn't be called!");
            }
        };

        // This synchronized statement is required because a RID list may be
        // altered when a row is deleted from the dispatcher thread.  Inserts and
        // deletes on a table at this level do not necessarily only happen when
        // the table is under a lock.  For this reason this block of code must
        // be synchronized.
        synchronized (master_table) {

            // Fill new_set with the set { 0, 1, 2, .... , row_set_length }
            for (int i = 0; i < row_set_length; ++i) {
                Integer rid_val =
                        rid_list.intAt(row_set.intAt(i));
                new_set.insertSort(rid_val, i, comparator);
            }

            return new_set;

        }  // synchronized (master_table)

    }


}

