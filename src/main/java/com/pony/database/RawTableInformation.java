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

import java.util.Vector;
//import com.pony.util.Comparable;
import com.pony.util.SortUtil;
import com.pony.util.IntegerVector;

/**
 * This object represents the lowest level DataTable information of a given
 * VirtualTable.  Since it is possible to make any level of VirtualTable's,
 * it is useful to be able to resolve an 'n leveled' VirtualTable to a
 * single level table.  This object is used to collect information as the
 * 'VirtualTable.resolveToRawTable' method is walking throught the
 * VirtualTable's ancestors.
 * <p>
 * @author Tobias Downer
 */

final class RawTableInformation {

    /**
     * A Vector containing a list of DataTables, and 'row index' IntegerVectors
     * of the given rows in the table.
     */
    private Vector raw_info;

    /**
     * The constructor.
     */
    RawTableInformation() {
        raw_info = new Vector();
    }

    /**
     * Adds a new DataTable or ReferenceTable, and IntegerVector row set into
     * the object.  We can not add VirtualTable objects into this object.
     */
    void add(RootTable table, IntegerVector row_set) {
        RawTableElement elem = new RawTableElement();
        elem.table = table;
        elem.row_set = row_set;
        raw_info.addElement(elem);
    }

    /**
     * Returns an AbstractDataTable[] array of all the tables that have been
     * added.
     */
    Table[] getTables() {
        int size = raw_info.size();
        Table[] list = new Table[size];
        for (int i = 0; i < size; ++i) {
            list[i] = (Table) ((RawTableElement) raw_info.elementAt(i)).table;
        }
        return list;
    }

    /**
     * Returns a IntegerVector[] list of the rows in the table that have been
     * added.
     */
    IntegerVector[] getRows() {
        int size = raw_info.size();
        IntegerVector[] list = new IntegerVector[size];
        for (int i = 0; i < size; ++i) {
            list[i] = ((RawTableElement) raw_info.elementAt(i)).row_set;
        }
        return list;
    }

    /**
     * Returns an array of RawTableElement sorted into a consistant order.
     */
    protected RawTableElement[] getSortedElements() {
        RawTableElement[] list = new RawTableElement[raw_info.size()];
        raw_info.copyInto(list);
        SortUtil.quickSort(list);
        return list;
    }

    /**
     * Finds the union of this information with the given information.
     * It does the following:
     *   + Sorts the unioned tables into a consistant order.
     *   + Merges each row in the tables row_set.
     *   + Sorts the resultant merge.
     *   + Makes a new set with the resultant merge minus any duplicates.
     */
    void union(RawTableInformation info) {

        // Number of Table 'columns'

        int col_count = raw_info.size();

        // Get the sorted RawTableElement[] from each raw table information object.

        RawTableElement[] merge1 = getSortedElements();
        RawTableElement[] merge2 = info.getSortedElements();

        // Validates that both tables being merges are of identical type.

        int size1 = -1;
        int size2 = -1;

        // First check number of tables in each merge is correct.

        if (merge1.length != merge2.length) {
            throw new Error("Incorrect format in table union");
        }

        // Check each table in the merge1 set has identical length row_sets

        for (int i = 0; i < merge1.length; ++i) {
            if (size1 == -1) {
                size1 = merge1[i].row_set.size();
            } else {
                if (size1 != merge1[i].row_set.size()) {
                    throw new Error("Incorrect format in table union");
                }
            }
        }

        // Check each table in the merge2 set has identical length row_sets

        for (int i = 0; i < merge2.length; ++i) {

            // Check the tables in merge2 are identical to the tables in merge1
            // (Checks the names match, and the validColumns filters are identical
            //  see AbstractDataTable.typeEquals method).

            if (!merge2[i].table.typeEquals(merge1[i].table)) {
                throw new Error("Incorrect format in table union");
            }

            if (size2 == -1) {
                size2 = merge2[i].row_set.size();
            } else {
                if (size2 != merge2[i].row_set.size()) {
                    throw new Error("Incorrect format in table union");
                }
            }
        }

        // If size1 or size2 are -1 then we have a corrupt table.  (It will be
        // 0 for an empty table).

        if (size1 == -1 || size2 == -1) {
            throw new Error("Incorrect format in table union");
        }

        // We don't need information in 'raw_info' vector anymore so clear it.
        // This may help garbage collection.

        raw_info.removeAllElements();

        // Merge the two together into a new list of RawRowElement[]

        int merge_size = size1 + size2;
        RawRowElement[] elems = new RawRowElement[merge_size];
        int elems_index = 0;

        for (int i = 0; i < size1; ++i) {
            RawRowElement e = new RawRowElement();
            e.row_vals = new int[col_count];

            for (int n = 0; n < col_count; ++n) {
                e.row_vals[n] = merge1[n].row_set.intAt(i);
            }
            elems[elems_index] = e;
            ++elems_index;
        }

        for (int i = 0; i < size2; ++i) {
            RawRowElement e = new RawRowElement();
            e.row_vals = new int[col_count];

            for (int n = 0; n < col_count; ++n) {
                e.row_vals[n] = merge2[n].row_set.intAt(i);
            }
            elems[elems_index] = e;
            ++elems_index;
        }

        // Now sort the row elements into order.

        SortUtil.quickSort(elems);

        // Set up the 'raw_info' vector with the new RawTableElement[] removing
        // any duplicate rows.

        for (int i = 0; i < col_count; ++i) {
            RawTableElement e = merge1[i];
            e.row_set.clear();
        }
        RawRowElement previous = null;
        RawRowElement current = null;
        for (int n = 0; n < merge_size; ++n) {
            current = elems[n];

            // Check that the current element in the set is not a duplicate of the
            // previous.

            if (previous == null || previous.compareTo(current) != 0) {
                for (int i = 0; i < col_count; ++i) {
                    merge1[i].row_set.addInt(current.row_vals[i]);
                }
                previous = current;
            }
        }

        for (int i = 0; i < col_count; ++i) {
            raw_info.addElement(merge1[i]);
        }

    }

    /**
     * Removes any duplicate rows from this RawTableInformation object.
     */
    void removeDuplicates() {

        // If no tables in duplicate then return

        if (raw_info.size() == 0) {
            return;
        }

        // Get the length of the first row set in the first table.  We assume that
        // the row set length is identical across each table in the Vector.

        RawTableElement elen = (RawTableElement) raw_info.elementAt(0);
        int len = elen.row_set.size();
        if (len == 0) {
            return;
        }

        // Create a new row element to sort.

        RawRowElement[] elems = new RawRowElement[len];
        int width = raw_info.size();

        // Create an array of RawTableElement so we can quickly access the data

        RawTableElement[] rdup = new RawTableElement[width];
        raw_info.copyInto(rdup);

        // Run through the data building up a new RawTableElement[] array with
        // the information in every raw span.

        for (int i = 0; i < len; ++i) {
            RawRowElement e = new RawRowElement();
            e.row_vals = new int[width];
            for (int n = 0; n < width; ++n) {
                e.row_vals[n] = rdup[n].row_set.intAt(i);
            }
            elems[i] = e;
        }

        // Now 'elems' it an array of individual RawRowElement objects which
        // represent each individual row in the table.

        // Now sort and remove duplicates to make up a new set.

        SortUtil.quickSort(elems);

        // Remove all elements from the raw_info Vector.

        raw_info.removeAllElements();

        // Make a new set of RawTableElement[] objects

        RawTableElement[] table_elements = rdup;

        // Set up the 'raw_info' vector with the new RawTableElement[] removing
        // any duplicate rows.

        for (int i = 0; i < width; ++i) {
            table_elements[i].row_set.clear();
        }
        RawRowElement previous = null;
        RawRowElement current = null;
        for (int n = 0; n < len; ++n) {
            current = elems[n];

            // Check that the current element in the set is not a duplicate of the
            // previous.

            if (previous == null || previous.compareTo(current) != 0) {
                for (int i = 0; i < width; ++i) {
                    table_elements[i].row_set.addInt(current.row_vals[i]);
                }
                previous = current;
            }
        }

        for (int i = 0; i < width; ++i) {
            raw_info.addElement(table_elements[i]);
        }

    }

}

/**
 * A container class to hold the DataTable and IntegerVector row set of a
 * given table in the list.
 */
final class RawTableElement implements Comparable {

    RootTable table;
    IntegerVector row_set;

    public int compareTo(Object o) {
        RawTableElement rte = (RawTableElement) o;
        return table.hashCode() - rte.table.hashCode();
    }

}

/**
 * A container class to hold each row of a list of tables.
 * table_elems is a reference to the merged set the 'row_index' is in.
 * row_index is the row index of the row this element refers to.
 */
final class RawRowElement implements Comparable {

    int[] row_vals;

    public int compareTo(Object o) {
        RawRowElement rre = (RawRowElement) o;

        int size = row_vals.length;
        for (int i = 0; i < size; ++i) {
            int v1 = row_vals[i];
            int v2 = rre.row_vals[i];
            if (v1 != v2) {
                return v1 - v2;
            }
        }
        return 0;
    }

}
