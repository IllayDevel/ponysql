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
import java.util.Arrays;
import java.util.Comparator;

import com.pony.util.IntegerVector;
import com.pony.util.BlockIntegerList;

/**
 * This is a scheme that performs a blind search of a given set.  It records
 * no information about how a set element relates to the rest.  It blindly
 * searches through the set to find elements that match the given criteria.
 * <p>
 * This scheme performs badly on large sets because it requires that the
 * database is queried often for information.  However since it records no
 * information about the set, memory requirements are non-existant.
 * <p>
 * This scheme should not be used for anything other than small domain sets
 * because the performance suffers very badly with larger sets.  It is ideal
 * for small domain sets because of its no memory overhead.  For any select
 * operation this algorithm must check every element in the set.
 *
 * @author Tobias Downer
 */

public final class BlindSearch extends SelectableScheme {

    /**
     * The Constructor.
     */
    public BlindSearch(TableDataSource table, int column) {
        super(table, column);
    }

    /**
     * This scheme doesn't take any notice of insertions or removals.
     */
    public void insert(int row) {
        if (isImmutable()) {
            throw new Error("Tried to change an immutable scheme.");
        }
    }

    /**
     * This scheme doesn't take any notice of insertions or removals.
     */
    public void remove(int row) {
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
     * Returns an exact copy of this scheme including any optimization
     * information.  The copied scheme is identical to the original but does not
     * share any parts.  Modifying any part of the copied scheme will have no
     * effect on the original and vice versa.
     */
    public SelectableScheme copy(TableDataSource table, boolean immutable) {
        // Return a fresh object.  This implementation has no state so we can
        // ignore the 'immutable' flag.
        return new BlindSearch(table, getColumn());
    }

    /**
     * Disposes and invalidates the BlindSearch.
     */
    public void dispose() {
        // Nothing to do!
    }

    /**
     * Selection methods for obtaining various sub-sets of information from the
     * set.
     */

    /**
     * We implement an insert sort algorithm here.  Each new row is inserted
     * into our row vector at the sorted corrent position.
     * The algorithm assumes the given vector is already sorted.  We then just
     * subdivide the set until we can insert at the required position.
     */
    private int search(TObject ob, IntegerVector vec, int lower, int higher) {
        if (lower >= higher) {
            if (ob.compareTo(getCellContents(vec.intAt(lower))) > 0) {
                return lower + 1;
            } else {
                return lower;
            }
        }

        int mid = lower + ((higher - lower) / 2);
        int comp_result = ob.compareTo(getCellContents(vec.intAt(mid)));

        if (comp_result == 0) {
            return mid;
        } else if (comp_result < 0) {
            return search(ob, vec, lower, mid - 1);
        } else {
            return search(ob, vec, mid + 1, higher);
        }

    }

    /**
     * Searches for a given TObject (ob) in the row list between the two
     * bounds.  This will return the highest row of the set of values that are
     * equal to 'ob'.
     * <p>
     * This returns the place to insert ob into the vector, it should not be
     * used to determine if ob is in the list or not.
     */
    private int highestSearch(TObject ob, IntegerVector vec,
                              int lower, int higher) {

        if ((higher - lower) <= 5) {
            // Start from the bottom up until we find the highest val
            for (int i = higher; i >= lower; --i) {
                int res = ob.compareTo(getCellContents(vec.intAt(i)));
                if (res >= 0) {
                    return i + 1;
                }
            }
            // Didn't find return lowest
            return lower;
        }

        int mid = (lower + higher) / 2;
        int comp_result = ob.compareTo(getCellContents(vec.intAt(mid)));

        if (comp_result == 0) {
            // We know the bottom is between 'mid' and 'higher'
            return highestSearch(ob, vec, mid, higher);
        } else if (comp_result < 0) {
            return highestSearch(ob, vec, lower, mid - 1);
        } else {
            return highestSearch(ob, vec, mid + 1, higher);
        }
    }


    private void doInsertSort(IntegerVector vec, int row) {
        int list_size = vec.size();
        if (list_size == 0) {
            vec.addInt(row);
        } else {
            int point = highestSearch(getCellContents(row), vec, 0, list_size - 1);
            if (point == list_size) {
                vec.addInt(row);
            } else {
                vec.insertIntAt(row, point);
            }
        }
    }

    public IntegerVector selectAll() {
        IntegerVector row_list = new IntegerVector(getTable().getRowCount());
        RowEnumeration e = getTable().rowEnumeration();
        while (e.hasMoreRows()) {
            doInsertSort(row_list, e.nextRowIndex());
        }
        return row_list;
    }


    public IntegerVector selectRange(SelectableRange range) {
        int set_size = getTable().getRowCount();
        // If no items in the set return an empty set
        if (set_size == 0) {
            return new IntegerVector(0);
        }

        return selectRange(new SelectableRange[]{range});
    }

    public IntegerVector selectRange(SelectableRange[] ranges) {
        int set_size = getTable().getRowCount();
        // If no items in the set return an empty set
        if (set_size == 0) {
            return new IntegerVector(0);
        }

        RangeChecker checker = new RangeChecker(ranges);
        return checker.resolve();
    }


    // ---------- Inner classes ----------

    /**
     * Object used to during range check loop.
     */
    final class RangeChecker {

        /**
         * The sorted list of all items in the set created as a cache for finding
         * the first and last values.
         */
        private IntegerVector sorted_set = null;

        /**
         * The list of flags for each check in the range.
         * Either 0 for no check, 1 for < or >, 2 for <= or >=.
         */
        private final byte[] lower_flags;
        private final byte[] upper_flags;

        /**
         * The TObject objects to check against.
         */
        private final TObject[] lower_cells;
        private final TObject[] upper_cells;

        /**
         * Constructs the checker.
         */
        public RangeChecker(SelectableRange[] ranges) {
            int size = ranges.length;
            lower_flags = new byte[size];
            upper_flags = new byte[size];
            lower_cells = new TObject[size];
            upper_cells = new TObject[size];
            for (int i = 0; i < ranges.length; ++i) {
                setupRange(i, ranges[i]);
            }
        }

        private void resolveSortedSet() {
            if (sorted_set == null) {
//        System.out.println("SLOW RESOLVE SORTED SET ON BLIND SEARCH.");
                sorted_set = selectAll();
            }
        }

        /**
         * Resolves a cell.
         */
        private TObject resolveCell(TObject ob) {
            if (ob == SelectableRange.FIRST_IN_SET) {
                resolveSortedSet();
                return getCellContents(sorted_set.intAt(0));

            } else if (ob == SelectableRange.LAST_IN_SET) {
                resolveSortedSet();
                return getCellContents(sorted_set.intAt(sorted_set.size() - 1));
            } else {
                return ob;
            }
        }

        /**
         * Set up a range.
         */
        public void setupRange(int i, SelectableRange range) {
            TObject l = range.getStart();
            byte lf = range.getStartFlag();
            TObject u = range.getEnd();
            byte uf = range.getEndFlag();

            // Handle lower first
            if (l == SelectableRange.FIRST_IN_SET &&
                    lf == SelectableRange.FIRST_VALUE) {
                // Special case no lower check
                lower_flags[i] = 0;
            } else {
                if (lf == SelectableRange.FIRST_VALUE) {
                    lower_flags[i] = 2;  // >=
                } else if (lf == SelectableRange.AFTER_LAST_VALUE) {
                    lower_flags[i] = 1;  // >
                } else {
                    throw new Error("Incorrect lower flag.");
                }
                lower_cells[i] = resolveCell(l);
            }

            // Now handle upper
            if (u == SelectableRange.LAST_IN_SET &&
                    uf == SelectableRange.LAST_VALUE) {
                // Special case no upper check
                upper_flags[i] = 0;
            } else {
                if (uf == SelectableRange.LAST_VALUE) {
                    upper_flags[i] = 2;  // <=
                } else if (uf == SelectableRange.BEFORE_FIRST_VALUE) {
                    upper_flags[i] = 1;  // <
                } else {
                    throw new Error("Incorrect upper flag.");
                }
                upper_cells[i] = resolveCell(u);
            }

        }

        /**
         * Resolves the ranges.
         */
        public IntegerVector resolve() {
            // The idea here is to only need to scan the column once to find all
            // the cells that meet our criteria.
            IntegerVector ivec = new IntegerVector();
            RowEnumeration e = getTable().rowEnumeration();

            int compare_tally = 0;

            int size = lower_flags.length;
            while (e.hasMoreRows()) {
                int row = e.nextRowIndex();
                // For each range
                range_set:
                for (int i = 0; i < size; ++i) {
                    boolean result = true;
                    byte lf = lower_flags[i];
                    if (lf != 0) {
                        ++compare_tally;
                        TObject v = getCellContents(row);
                        int compare = lower_cells[i].compareTo(v);
                        if (lf == 1) {  // >
                            result = (compare < 0);
                        } else if (lf == 2) {  // >=
                            result = (compare <= 0);
                        } else {
                            throw new Error("Incorrect flag.");
                        }
                    }
                    if (result) {
                        byte uf = upper_flags[i];
                        if (uf != 0) {
                            ++compare_tally;
                            TObject v = getCellContents(row);
                            int compare = upper_cells[i].compareTo(v);
                            if (uf == 1) {  // <
                                result = (compare > 0);
                            } else if (uf == 2) {  // >=
                                result = (compare >= 0);
                            } else {
                                throw new Error("Incorrect flag.");
                            }
                        }
                        // Pick this row
                        if (result) {
                            doInsertSort(ivec, row);
                            break range_set;
                        }
                    }
                }
            }

//      System.out.println("Blind Search compare tally: " + compare_tally);

            return ivec;
        }

    }

}

