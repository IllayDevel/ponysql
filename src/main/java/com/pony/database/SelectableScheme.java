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

import com.pony.util.IntegerVector;
import com.pony.util.IndexComparator;
import com.pony.util.BlockIntegerList;
import com.pony.debug.DebugLogger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Represents a base class for a mechanism to select ranges from a given set.
 * Such schemes could include BinaryTree, Hashtable or just a blind search.
 * <p>
 * A given element in the set is specified through a 'row' integer whose
 * contents can be obtained through the 'table.getCellContents(column, row)'.
 * Every scheme is given a table and column number that the set refers to.
 * While a given set element is refered to as a 'row', the integer is really
 * only a pointer into the set list which can be de-referenced with a call to
 * table.getCellContents(row).  Better performance schemes will keep such
 * calls to a minimum.
 * <p>
 * A scheme may choose to retain knowledge about a given element when it is
 * added or removed from the set, such as a BinaryTree that catalogs all
 * elements with respect to each other.
 *
 * @author Tobias Downer
 */

public abstract class SelectableScheme {

    /**
     * Some statics.
     */
    protected static final BlockIntegerList EMPTY_LIST;
    protected static final BlockIntegerList ONE_LIST;

    static {
        EMPTY_LIST = new BlockIntegerList();
        EMPTY_LIST.setImmutable();
        ONE_LIST = new BlockIntegerList();
        ONE_LIST.add(0);
        ONE_LIST.setImmutable();
    }

    /**
     * The table data source with the column this scheme indexes.
     */
    private final TableDataSource table;

    /**
     * The column number in the tree this tree helps.
     */
    private final int column;

    /**
     * Set to true if this scheme is immutable (can't be changed).
     */
    private boolean immutable = false;

    /**
     * The constructor for all schemes.
     */
    public SelectableScheme(TableDataSource table, int column) {
        this.table = table;
        this.column = column;
    }

    /**
     * Returns the Table.
     */
    protected final TableDataSource getTable() {
        return table;
    }

    /**
     * Returns the global transaction system.
     */
    protected final TransactionSystem getSystem() {
        return table.getSystem();
    }

    /**
     * Returns the DebugLogger object to log debug messages to.
     */
    protected final DebugLogger Debug() {
        return getSystem().Debug();
    }

    /**
     * Returns the column this scheme is indexing in the table.
     */
    protected final int getColumn() {
        return column;
    }

    /**
     * Obtains the given cell in the row from the table.
     */
    protected final TObject getCellContents(int row) {
        return table.getCellContents(column, row);
    }

    /**
     * Sets this scheme to immutable.
     */
    public final void setImmutable() {
        immutable = true;
    }

    /**
     * Returns true if this scheme is immutable.
     */
    public final boolean isImmutable() {
        return immutable;
    }

    /**
     * Diagnostic information.
     */
    public String toString() {
        // Name of the table
        String table_name;
        if (table instanceof DefaultDataTable) {
            table_name = ((DefaultDataTable) table).getTableName().toString();
        } else {
            table_name = "VirtualTable";
        }

        StringBuffer buf = new StringBuffer();
        buf.append("[ SelectableScheme ");
        buf.append(super.toString());
        buf.append(" for table: ");
        buf.append(table_name);
        buf.append("]");

        return new String(buf);
    }

    /**
     * Writes the entire contents of the scheme to an OutputStream object.
     */
    public abstract void writeTo(OutputStream out) throws IOException;

    /**
     * Reads the entire contents of the scheme from a InputStream object.  If the
     * scheme is full of any information it throws an exception.
     */
    public abstract void readFrom(InputStream in) throws IOException;

    /**
     * Returns an exact copy of this scheme including any optimization
     * information.  The copied scheme is identical to the original but does not
     * share any parts.  Modifying any part of the copied scheme will have no
     * effect on the original and vice versa.
     * <p>
     * The newly copied scheme can be given a new table source.  If
     * 'immutable' is true, then the resultant scheme is an immutable version
     * of the parent.  An immutable version may share information with the
     * copied version so can not be changed.
     * <p>
     * NOTE: Even if the scheme maintains no state you should still be careful
     *   to ensure a fresh SelectableScheme object is returned here.
     */
    public abstract SelectableScheme copy(TableDataSource table,
                                          boolean immutable);

    /**
     * Dispose and invalidate this scheme.
     */
    public abstract void dispose();


    /**
     * Inserts the given element into the set.  This is called just after a
     * row has been initially added to a table.
     */
    abstract void insert(int row);

    /**
     * Removes the given element from the set.  This is called just before the
     * row is removed from the table.
     */
    abstract void remove(int row);

    /**
     * Returns a BlockIntegerList that represents the given row_set sorted
     * in the order of this scheme.  The values in 'row_set' must be references
     * to rows in the domain of the table this scheme represents.
     * <p>
     * The returned set must be stable, meaning if values are equal they keep
     * the same ordering.
     * <p>
     * Note that the default implementation of this method can often be optimized.
     * For example, InsertSearch uses a secondary RID list to sort items if the
     * given list is over a certain size.
     */
    public BlockIntegerList internalOrderIndexSet(final IntegerVector row_set) {
        // The length of the set to order
        int row_set_length = row_set.size();

        // Trivial cases where sorting is not required:
        // NOTE: We use immutable objects to save some memory.
        if (row_set_length == 0) {
            return EMPTY_LIST;
        } else if (row_set_length == 1) {
            return ONE_LIST;
        }

        // This will be 'row_set' sorted by its entry lookup.  This must only
        // contain indices to row_set entries.
        BlockIntegerList new_set = new BlockIntegerList();

        if (row_set_length <= 250000) {
            // If the subset is less than or equal to 250,000 elements, we generate
            // an array in memory that contains all values in the set and we sort
            // it.  This requires use of memory from the heap but is faster than
            // the no heap use method.
            final TObject[] subset_list = new TObject[row_set_length];
            for (int i = 0; i < row_set_length; ++i) {
                subset_list[i] = getCellContents(row_set.intAt(i));
            }

            // The comparator we use to sort
            IndexComparator comparator = new IndexComparator() {
                public int compare(int index, Object val) {
                    TObject cell = subset_list[index];
                    return cell.compareTo((TObject) val);
                }

                public int compare(int index1, int index2) {
                    throw new Error("Shouldn't be called!");
                }
            };

            // Fill new_set with the set { 0, 1, 2, .... , row_set_length }
            for (int i = 0; i < row_set_length; ++i) {
                TObject cell = subset_list[i];
                new_set.insertSort(cell, i, comparator);
            }

        } else {
            // This is the no additional heap use method to sorting the sub-set.

            // The comparator we use to sort
            IndexComparator comparator = new IndexComparator() {
                public int compare(int index, Object val) {
                    TObject cell = getCellContents(row_set.intAt(index));
                    return cell.compareTo((TObject) val);
                }

                public int compare(int index1, int index2) {
                    throw new Error("Shouldn't be called!");
                }
            };

            // Fill new_set with the set { 0, 1, 2, .... , row_set_length }
            for (int i = 0; i < row_set_length; ++i) {
                TObject cell = getCellContents(row_set.intAt(i));
                new_set.insertSort(cell, i, comparator);
            }

        }

        return new_set;

    }

    /**
     * Asks the Scheme for a SelectableScheme abject that describes a sub-set
     * of the set handled by this Scheme.  Since a Table stores a subset
     * of a given DataTable, we pass this as the argument.  It returns a
     * new SelectableScheme that orders the rows in the given columns order.
     * The 'column' variable specifies the column index of this column in the
     * given table.
     */
    public SelectableScheme getSubsetScheme(Table subset_table,
                                            int subset_column) {

        // Resolve table rows in this table scheme domain.
        IntegerVector row_set = new IntegerVector(subset_table.getRowCount());
        RowEnumeration e = subset_table.rowEnumeration();
        while (e.hasMoreRows()) {
            row_set.addInt(e.nextRowIndex());
        }
        subset_table.setToRowTableDomain(subset_column, row_set, getTable());

        // Generates an IntegerVector which contains indices into 'row_set' in
        // sorted order.
        BlockIntegerList new_set = internalOrderIndexSet(row_set);

        // Our 'new_set' should be the same size as 'row_set'
        if (new_set.size() != row_set.size()) {
            throw new RuntimeException("Internal sort error in finding sub-set.");
        }

        // Set up a new SelectableScheme with the sorted index set.
        // Move the sorted index set into the new scheme.
        InsertSearch is = new InsertSearch(subset_table, subset_column, new_set);
        // Don't let subset schemes create uid caches.
        is.RECORD_UID = false;
        return is;

    }

    /**
     * These are the select operations that are the main purpose of the scheme.
     * They retrieve the given information from the set.  Different schemes will
     * have varying performance on different types of data sets.
     * The select operations must *always* return a resultant row set that
     * is sorted from lowest to highest.
     */
    public IntegerVector selectAll() {
        return selectRange(new SelectableRange(
                SelectableRange.FIRST_VALUE, SelectableRange.FIRST_IN_SET,
                SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET));
    }

    public IntegerVector selectFirst() {
        // NOTE: This will find NULL at start which is probably wrong.  The
        //   first value should be the first non null value.
        return selectRange(new SelectableRange(
                SelectableRange.FIRST_VALUE, SelectableRange.FIRST_IN_SET,
                SelectableRange.LAST_VALUE, SelectableRange.FIRST_IN_SET));
    }

    public IntegerVector selectNotFirst() {
        // NOTE: This will find NULL at start which is probably wrong.  The
        //   first value should be the first non null value.
        return selectRange(new SelectableRange(
                SelectableRange.AFTER_LAST_VALUE, SelectableRange.FIRST_IN_SET,
                SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET));
    }

    public IntegerVector selectLast() {
        return selectRange(new SelectableRange(
                SelectableRange.FIRST_VALUE, SelectableRange.LAST_IN_SET,
                SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET));
    }

    public IntegerVector selectNotLast() {
        return selectRange(new SelectableRange(
                SelectableRange.FIRST_VALUE, SelectableRange.FIRST_IN_SET,
                SelectableRange.BEFORE_FIRST_VALUE, SelectableRange.LAST_IN_SET));
    }

    /**
     * Selects all values in the column that are not null.
     */
    public IntegerVector selectAllNonNull() {
        return selectRange(new SelectableRange(
                SelectableRange.AFTER_LAST_VALUE, TObject.nullVal(),
                SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET));
    }

    public IntegerVector selectEqual(TObject ob) {
        if (ob.isNull()) {
            return new IntegerVector(0);
        }
        return selectRange(new SelectableRange(
                SelectableRange.FIRST_VALUE, ob,
                SelectableRange.LAST_VALUE, ob));
    }

    public IntegerVector selectNotEqual(TObject ob) {
        if (ob.isNull()) {
            return new IntegerVector(0);
        }
        return selectRange(new SelectableRange[]{
                new SelectableRange(
                        SelectableRange.AFTER_LAST_VALUE, TObject.nullVal(),
                        SelectableRange.BEFORE_FIRST_VALUE, ob)
                , new SelectableRange(
                SelectableRange.AFTER_LAST_VALUE, ob,
                SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET)
        });
    }

    public IntegerVector selectGreater(TObject ob) {
        if (ob.isNull()) {
            return new IntegerVector(0);
        }
        return selectRange(new SelectableRange(
                SelectableRange.AFTER_LAST_VALUE, ob,
                SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET));
    }

    public IntegerVector selectLess(TObject ob) {
        if (ob.isNull()) {
            return new IntegerVector(0);
        }
        return selectRange(new SelectableRange(
                SelectableRange.AFTER_LAST_VALUE, TObject.nullVal(),
                SelectableRange.BEFORE_FIRST_VALUE, ob));
    }

    public IntegerVector selectGreaterOrEqual(TObject ob) {
        if (ob.isNull()) {
            return new IntegerVector(0);
        }
        return selectRange(new SelectableRange(
                SelectableRange.FIRST_VALUE, ob,
                SelectableRange.LAST_VALUE, SelectableRange.LAST_IN_SET));
    }

    public IntegerVector selectLessOrEqual(TObject ob) {
        if (ob.isNull()) {
            return new IntegerVector(0);
        }
        return selectRange(new SelectableRange(
                SelectableRange.AFTER_LAST_VALUE, TObject.nullVal(),
                SelectableRange.LAST_VALUE, ob));
    }

    // Inclusive of rows that are >= ob1 and < ob2
    // NOTE: This is not compatible with SQL BETWEEN predicate which is all
    //   rows that are >= ob1 and <= ob2
    public IntegerVector selectBetween(TObject ob1, TObject ob2) {
        if (ob1.isNull() || ob2.isNull()) {
            return new IntegerVector(0);
        }
        return selectRange(new SelectableRange(
                SelectableRange.FIRST_VALUE, ob1,
                SelectableRange.BEFORE_FIRST_VALUE, ob2));
    }

    /**
     * Selects the given range of values from this index.  The SelectableRange
     * must contain a 'start' value that compares <= to the 'end' value.
     * <p>
     * This must guarentee that the returned set is sorted from lowest to
     * highest value.
     */
    abstract IntegerVector selectRange(SelectableRange range);

    /**
     * Selects a set of ranges from this index.  The ranges must not overlap and
     * each range must contain a 'start' value that compares <= to the 'end'
     * value.  Every range in the array must represent a range that's lower than
     * the preceeding range (if it exists).
     * <p>
     * If the above rules are enforced (as they must be) then this method will
     * return a set that is sorted from lowest to highest value.
     * <p>
     * This must guarentee that the returned set is sorted from lowest to
     * highest value.
     */
    abstract IntegerVector selectRange(SelectableRange[] ranges);

}
