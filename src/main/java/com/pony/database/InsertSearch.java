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
import com.pony.util.BlockIntegerList;
import com.pony.util.IntegerListInterface;
import com.pony.util.IndexComparator;
import com.pony.util.IntegerIterator;

import java.util.Comparator;
import java.util.Arrays;
import java.io.*;

/**
 * This is a SelectableScheme similar in some ways to the binary tree.  When
 * a new row is added, it is inserted into a sorted list of rows.  We can then
 * use this list to select out the sorted list of elements.
 * <p>
 * This requires less memory than the BinaryTree, however it is not as fast.
 * Even though, it should still perform fairly well on medium size data sets.
 * On large size data sets, insert and remove performance may suffer.
 * <p>
 * This object retains knowledge of all set elements unlike BlindSearch which
 * has no memory overhead.
 * <p>
 * Performance should be very comparable to BinaryTree for sets that aren't
 * altered much.
 *
 * @author Tobias Downer
 */

public final class InsertSearch extends CollatedBaseSearch {

    /**
     * The sorted list of rows in this set.  This is sorted from min to max
     * (not sorted by row number - sorted by entity row value).
     */
    private IntegerListInterface set_list;

    /**
     * If this is true, then this SelectableScheme records additional rid
     * information that can be used to very quickly identify whether a value is
     * greater, equal or less.
     */
    boolean RECORD_UID;

    /**
     * The IndexComparator that we use to refer elements in the set to actual
     * data objects.
     */
    private IndexComparator set_comparator;


    // ----- DEBUGGING -----

    /**
     * If this is immutable, this stores the number of entries in 'set_list'
     * when this object was made.
     */
    private int DEBUG_immutable_set_size;


    /**
     * The Constructor.
     */
    public InsertSearch(TableDataSource table, int column) {
        super(table, column);
        set_list = new BlockIntegerList();

        // The internal comparator that enables us to sort and lookup on the data
        // in this column.
        setupComparator();
    }

    /**
     * Constructor sets the scheme with a pre-sorted list.  The Vector 'vec'
     * should not be used again after this is called.  'vec' must be sorted from
     * low key to high key.
     */
    public InsertSearch(TableDataSource table, int column, IntegerVector vec) {
        this(table, column);
        for (int i = 0; i < vec.size(); ++i) {
            set_list.add(vec.intAt(i));
        }

        // NOTE: This must be removed in final, this is a post condition check to
        //   make sure 'vec' is infact sorted
        checkSchemeSorted();

    }

    /**
     * Constructor sets the scheme with a pre-sorted list.  The list 'list'
     * should not be used again after this is called.  'list' must be sorted from
     * low key to high key.
     */
    InsertSearch(TableDataSource table, int column, IntegerListInterface list) {
        this(table, column);
        this.set_list = list;

        // NOTE: This must be removed in final, this is a post condition check to
        //   make sure 'vec' is infact sorted
        checkSchemeSorted();

    }

    /**
     * Constructs this as a copy of the given, either mutable or immutable
     * copy.
     */
    private InsertSearch(TableDataSource table, InsertSearch from,
                         boolean immutable) {
        super(table, from.getColumn());

        if (immutable) {
            setImmutable();
        }

        if (immutable) {
            // Immutable is a shallow copy
            set_list = from.set_list;
            DEBUG_immutable_set_size = set_list.size();
        } else {
            set_list = new BlockIntegerList(from.set_list);
        }

        // Do we generate lookup caches?
        RECORD_UID = from.RECORD_UID;

        // The internal comparator that enables us to sort and lookup on the data
        // in this column.
        setupComparator();

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
     * Inserts a row into the list.  This will always be thread safe, table
     * changes cause a write lock which prevents reads while we are writing to
     * the table.
     */
    public void insert(int row) {
        if (isImmutable()) {
            throw new Error("Tried to change an immutable scheme.");
        }

        final TObject cell = getCellContents(row);
        set_list.insertSort(cell, row, set_comparator);

    }

    /**
     * Removes a row from the list.  This will always be thread safe, table
     * changes cause a write lock which prevents reads while we are writing to
     * the table.
     */
    public void remove(int row) {
        if (isImmutable()) {
            throw new Error("Tried to change an immutable scheme.");
        }

        TObject cell = getCellContents(row);
        int removed = set_list.removeSort(cell, row, set_comparator);

        if (removed != row) {
            throw new Error("Removed value different than row asked to remove.  " +
                    "To remove: " + row + "  Removed: " + removed);
        }

    }

    /**
     * This needs to be called to access 'set_comparator' in thread busy
     * methods.  Because creating a UID cache will modify set_comparator, we
     * need to make sure we access this variable safely.
     * <p>
     * NOTE: This is a throwback method for an idea I had to speed up the
     *   'select*' methods, but it proved unworkable.  The reason being that
     *   the UID only contains knowledge of relations between rows, and the
     *   'select*' methods find the relationship of a TObject in the column
     *   set.
     */
    private IndexComparator safeSetComparator() {
//    synchronized (uid_lock) {
        return set_comparator;
//    }
    }

    /**
     * Reads the entire state of the scheme from the input stream.  Throws an
     * exception if the scheme is not empty.
     */
    public void readFrom(InputStream in) throws IOException {
        if (set_list.size() != 0) {
            throw new RuntimeException("Error reading scheme, already a set in the Scheme");
        }
        DataInputStream din = new DataInputStream(in);
        int vec_size = din.readInt();

        int row_count = getTable().getRowCount();
        // Check we read in as many indices as there are rows in the table
        if (row_count != vec_size) {
            throw new IOException(
                    "Different table row count to indices in scheme. " +
                            "table=" + row_count +
                            ", vec_size=" + vec_size);
        }

        for (int i = 0; i < vec_size; ++i) {
            int row = din.readInt();
            if (row < 0) { // || row >= row_count) {
                set_list = new BlockIntegerList();
                throw new IOException("Scheme contains out of table bounds index.");
            }
            set_list.add(row);
        }

        getSystem().stats().add(vec_size, "{session} InsertSearch.read_indices");

        // NOTE: This must be removed in final, this is a post condition check to
        //   make sure 'vec' is infact sorted
        checkSchemeSorted();
    }

    /**
     * Writes the entire state of the scheme to the output stream.
     */
    public void writeTo(OutputStream out) throws IOException {
        DataOutputStream dout = new DataOutputStream(out);
        int list_size = set_list.size();
        dout.writeInt(list_size);

        IntegerIterator i = set_list.iterator(0, list_size - 1);
        while (i.hasNext()) {
            dout.writeInt(i.next());
        }
    }

    /**
     * Returns an exact copy of this scheme including any optimization
     * information.  The copied scheme is identical to the original but does not
     * share any parts.  Modifying any part of the copied scheme will have no
     * effect on the original and vice versa.
     */
    public SelectableScheme copy(TableDataSource table, boolean immutable) {
        // ASSERTION: If immutable, check the size of the current set is equal to
        //   when the scheme was created.
        if (isImmutable()) {
            if (DEBUG_immutable_set_size != set_list.size()) {
                throw new Error("Assert failed: " +
                        "Immutable set size is different from when created.");
            }
        }

        // We must create a new InsertSearch object and copy all the state
        // information from this object to the new one.
        return new InsertSearch(table, this, immutable);
    }

    /**
     * Disposes this scheme.
     */
    public void dispose() {
        // Close and invalidate.
        set_list = null;
        set_comparator = null;
    }

    /**
     * Checks that the scheme is in sorted order.  This is a debug check to
     * ensure we maintain a sorted index.
     * NOTE: This *MUST* be removed in a release version because it uses up
     *   many cycles for each check.
     */
    private void checkSchemeSorted() {
//    int list_size = set_list.size();
//    DataCell last_cell = null;
//    for (int i = 0; i < list_size; ++i) {
//      int row = set_list.intAt(i);
//      DataCell this_cell = getCellContents(row);
//      if (last_cell != null) {
//        if (this_cell.compareTo(last_cell) < 0) {
//          throw new Error("checkSchemeSorted failed.  Corrupt index.");
//        }
//      }
//      last_cell = this_cell;
//    }
//    if (Debug().isInterestedIn(Lvl.WARNING)) {
//      StringBuffer info_string = new StringBuffer();
//      info_string.append("POST CONDITION CHECK - Checked index of size: ");
//      info_string.append(list_size);
//      info_string.append(".  Sorted correctly (REMOVE THIS CHECK IN FINAL)");
//      Debug().write(Lvl.WARNING, this, new String(info_string));
//    }
    }

    // ---------- Implemented/Overwritten from CollatedBaseSearch ----------

    protected int searchFirst(TObject val) {
        return set_list.searchFirst(val, safeSetComparator());
    }

    protected int searchLast(TObject val) {
        return set_list.searchLast(val, safeSetComparator());
    }

    protected int setSize() {
        return set_list.size();
    }

    protected TObject firstInCollationOrder() {
        return getCellContents(set_list.get(0));
    }

    protected TObject lastInCollationOrder() {
        return getCellContents(set_list.get(setSize() - 1));
    }

    protected IntegerVector addRangeToSet(int start, int end,
                                          IntegerVector ivec) {
        if (ivec == null) {
            ivec = new IntegerVector((end - start) + 2);
        }
        IntegerIterator i = set_list.iterator(start, end);
        while (i.hasNext()) {
            ivec.addInt(i.next());
        }
        return ivec;
    }

    /**
     * The select operations for this scheme.
     */

    public IntegerVector selectAll() {
        IntegerVector ivec = new IntegerVector(set_list);
        return ivec;
    }

}
