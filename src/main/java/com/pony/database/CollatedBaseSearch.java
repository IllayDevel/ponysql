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

import com.pony.util.IntegerVector;

/**
 * An implementation of SelectableScheme that is based on some collated set of
 * data.  This can be used to implement more advanced types of selectable
 * schemes based on presistant indexes (see InsertSearch).
 * <p>
 * The default implementation maintains no state,
 * <p>
 * Derived classes are required to implement 'copy', 'searchFirst' and
 * 'searchLast' methods.  With these basic methods, a selectable scheme can
 * be generated provided the column is sorted in ascending order (value of row i
 * is <= value of row i+1).  Overwrite 'firstInCollationOrder',
 * 'lastInCollationOrder' and 'addRangeToSet' methods for non sorted underlying
 * sets.
 * <p>
 * Assumptions - the underlying column is sorted low to high (value of row i
 * is <= value of row i+1).
 *
 * @author Tobias Downer
 */

public abstract class CollatedBaseSearch extends SelectableScheme {

    /**
     * The Constructor.
     */
    public CollatedBaseSearch(TableDataSource table, int column) {
        super(table, column);
    }

    /**
     * This scheme doesn't take any notice of insertions or removals.
     */
    public void insert(int row) {
        // Ignore insert (no state to maintain)
        if (isImmutable()) {
            throw new Error("Tried to change an immutable scheme.");
        }
    }

    /**
     * This scheme doesn't take any notice of insertions or removals.
     */
    public void remove(int row) {
        // Ignore remove (no state to maintain)
        if (isImmutable()) {
            throw new Error("Tried to change an immutable scheme.");
        }
    }

    /**
     * Reads the entire state of the scheme from the input stream.
     * This is a trivial case for BlindSearch which doesn't require any
     * data to be stored.
     */
    public void readFrom(InputStream in) throws IOException {
    }

    /**
     * Writes the entire state of the scheme to the output stream.
     * This is a trivial case for BlindSearch which doesn't require any
     * data to be stored.
     */
    public void writeTo(OutputStream out) throws IOException {
    }

    /**
     * Disposes and invalidates the BlindSearch.
     */
    public void dispose() {
        // Nothing to do!
    }


    // -------- Abstract or overwrittable methods ----------

    /**
     * Finds the position in the collated set of the first value in the column
     * equal to the given value.  If the value is not to be found in the column,
     * it returns -(insert_position + 1).
     */
    protected abstract int searchFirst(TObject val);

    /**
     * Finds the position in the collated set of the last value in the column
     * equal to the given value.  If the value is not to be found in the column,
     * it returns -(insert_position + 1).
     */
    protected abstract int searchLast(TObject val);

    /**
     * The size of the set (the number of rows in this column).
     */
    protected int setSize() {
        return getTable().getRowCount();
    }

    /**
     * Returns the first value of this column (in collated order).  For
     * example, if the column contains (1, 4, 8} then '1' is returned.
     */
    protected TObject firstInCollationOrder() {
        return getCellContents(0);
    }

    /**
     * Returns the last value of this column (in collated order).    For
     * example, if the column contains (1, 4, 8} then '8' is returned.
     */
    protected TObject lastInCollationOrder() {
        return getCellContents(setSize() - 1);
    }

    /**
     * Adds the set indexes to the list that represent the range of values
     * between the start (inclusive) and end offset (inclusive) given.
     */
    protected IntegerVector addRangeToSet(int start, int end,
                                          IntegerVector ivec) {
        if (ivec == null) {
            ivec = new IntegerVector((end - start) + 2);
        }
        for (int i = start; i <= end; ++i) {
            ivec.addInt(i);
        }
        return ivec;
    }

    // ---------- Range search methods ----------

    public IntegerVector selectAll() {
        return addRangeToSet(0, setSize() - 1, null);
    }

    /**
     * Given a flag (FIRST_VALUE, LAST_VALUE, BEFORE_FIRST_VALUE or
     * AFTER_LAST_VALUE) and a value which is either a place marker (first, last
     * in set) or a TObject object, this will determine the position in this
     * set of the range point.  For example, we may want to know the index of
     * the last instance of a particular number in a set of numbers which
     * would be 'positionOfRangePoint(SelectableRange.LAST_VALUE,
     * [number TObject])'.
     * <p>
     * Note how the position is determined if the value is not found in the set.
     */
    private int positionOfRangePoint(byte flag, TObject val) {
        int p;
        TObject cell;

        switch (flag) {

            case (SelectableRange.FIRST_VALUE):
                if (val == SelectableRange.FIRST_IN_SET) {
                    return 0;
                }
                if (val == SelectableRange.LAST_IN_SET) {
                    // Get the last value and search for the first instance of it.
                    cell = lastInCollationOrder();
                } else {
                    cell = val;
                }
                p = searchFirst(cell);
                // (If value not found)
                if (p < 0) {
                    return -(p + 1);
                }
                return p;

            case (SelectableRange.LAST_VALUE):
                if (val == SelectableRange.LAST_IN_SET) {
                    return setSize() - 1;
                }
                if (val == SelectableRange.FIRST_IN_SET) {
                    // Get the first value.
                    cell = firstInCollationOrder();
                } else {
                    cell = val;
                }
                p = searchLast(cell);
                // (If value not found)
                if (p < 0) {
                    return -(p + 1) - 1;
                }
                return p;

            case (SelectableRange.BEFORE_FIRST_VALUE):
                if (val == SelectableRange.FIRST_IN_SET) {
                    return -1;
                }
                if (val == SelectableRange.LAST_IN_SET) {
                    // Get the last value and search for the first instance of it.
                    cell = lastInCollationOrder();
                } else {
                    cell = val;
                }
                p = searchFirst(cell);
                // (If value not found)
                if (p < 0) {
                    return -(p + 1) - 1;
                }
                return p - 1;

            case (SelectableRange.AFTER_LAST_VALUE):
                if (val == SelectableRange.LAST_IN_SET) {
                    return setSize();
                }
                if (val == SelectableRange.FIRST_IN_SET) {
                    // Get the first value.
                    cell = firstInCollationOrder();
                } else {
                    cell = val;
                }
                p = searchLast(cell);
                // (If value not found)
                if (p < 0) {
                    return -(p + 1);
                }
                return p + 1;

            default:
                throw new Error("Unrecognised flag.");
        }

    }

    /**
     * Adds a range from this set to the given IntegerVector.  IntegerVector may
     * be null if a list has not yet been allocated for the range.
     */
    private IntegerVector addRange(SelectableRange range, IntegerVector ivec) {
        int r1, r2;

        // Select the range specified.
        byte start_flag = range.getStartFlag();
        TObject start = range.getStart();
        byte end_flag = range.getEndFlag();
        TObject end = range.getEnd();

        r1 = positionOfRangePoint(start_flag, start);
        r2 = positionOfRangePoint(end_flag, end);

        if (r2 < r1) {
            return ivec;
        }

        // Add the range to the set
        return addRangeToSet(r1, r2, ivec);

    }

    public IntegerVector selectRange(SelectableRange range) {
        // If no items in the set return an empty set
        if (setSize() == 0) {
            return new IntegerVector(0);
        }

        IntegerVector ivec = addRange(range, null);
        if (ivec == null) {
            return new IntegerVector(0);
        }

        return ivec;
    }

    public IntegerVector selectRange(SelectableRange[] ranges) {
        // If no items in the set return an empty set
        if (setSize() == 0) {
            return new IntegerVector(0);
        }

        IntegerVector ivec = null;
        for (SelectableRange range : ranges) {
            ivec = addRange(range, ivec);
        }

        if (ivec == null) {
            return new IntegerVector(0);
        }
        return ivec;

    }

}

