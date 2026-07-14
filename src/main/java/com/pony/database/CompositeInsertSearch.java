/*
 * Pony SQL Database ( http://i-devel.ru )
 * Copyright (C) 2019-2020 IllayDevel.
 * SPDX-License-Identifier: GPL-2.0-only
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <https://www.gnu.org/licenses/>.
 */

package com.pony.database;

import com.pony.util.BlockIntegerList;
import com.pony.util.IndexComparator;
import com.pony.util.IntegerIterator;
import com.pony.util.IntegerListInterface;
import com.pony.util.IntegerVector;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A BLIST-backed selectable scheme over more than one table column.
 * <p>
 * The persisted structure is still the familiar sorted integer list of row
 * identifiers; ordering is provided by a comparator that compares the row's
 * tuple values lexicographically.
 */
final class CompositeInsertSearch extends SelectableScheme {

    private final int[] columns;
    private IntegerListInterface set_list;
    private final boolean unique;
    private IndexComparator set_comparator;
    private int DEBUG_immutable_set_size;

    CompositeInsertSearch(TableDataSource table, int[] columns,
                          IntegerListInterface list, boolean unique) {
        super(table, columns[0]);
        this.columns = columns.clone();
        this.set_list = list;
        this.unique = unique;
        setupComparator();
    }

    private CompositeInsertSearch(TableDataSource table,
                                  CompositeInsertSearch from,
                                  boolean immutable) {
        super(table, from.getColumn());
        this.columns = from.columns.clone();
        this.unique = from.unique;
        if (immutable) {
            setImmutable();
            set_list = from.set_list;
            DEBUG_immutable_set_size = set_list.size();
        } else {
            set_list = new BlockIntegerList(from.set_list);
        }
        setupComparator();
    }

    private void setupComparator() {
        set_comparator = new IndexComparator() {
            public int compare(int index, Object val) {
                return compareRowToKey(index, (CompositeKey) val);
            }

            public int compare(int index1, int index2) {
                return compareRows(index1, index2);
            }
        };
    }

    public SelectableScheme copy(TableDataSource table, boolean immutable) {
        if (isImmutable() && DEBUG_immutable_set_size != set_list.size()) {
            throw new IllegalStateException(
                    "Assert failed: " +
                    "Immutable set size is different from when created.");
        }
        return new CompositeInsertSearch(table, this, immutable);
    }

    public void dispose() {
        set_list = null;
        set_comparator = null;
    }

    public void insert(int row) {
        if (isImmutable()) {
            throw new IllegalStateException("Tried to change an immutable scheme.");
        }
        CompositeKey key = keyForRow(row);
        if (unique && set_list.searchFirst(key, set_comparator) >= 0) {
            throw new DatabaseConstraintViolationException(
                    DatabaseConstraintViolationException.UNIQUE_VIOLATION,
                    "Unique composite index violation.");
        }
        set_list.insertSort(key, row, set_comparator);
    }

    public void remove(int row) {
        if (isImmutable()) {
            throw new IllegalStateException("Tried to change an immutable scheme.");
        }
        CompositeKey key = keyForRow(row);
        int removed = set_list.removeSort(key, row, set_comparator);
        if (removed != row) {
            throw new IllegalStateException(
                    "Removed value different than row asked to remove.  " +
                    "To remove: " + row + "  Removed: " + removed);
        }
    }

    public IntegerVector selectEqual(TObject[] values) {
        CompositeKey key = new CompositeKey(values);
        int first = set_list.searchFirst(key, set_comparator);
        if (first < 0) {
            return new IntegerVector(0);
        }
        int last = set_list.searchLast(key, set_comparator);
        IntegerVector result = new IntegerVector((last - first) + 1);
        IntegerIterator iterator = set_list.iterator(first, last);
        while (iterator.hasNext()) {
            result.addInt(iterator.next());
        }
        return result;
    }

    IntegerVector selectRange(SelectableRange range) {
        throw new StatementException(
                "Composite indexes support equality lookup only.");
    }

    IntegerVector selectRange(SelectableRange[] ranges) {
        throw new StatementException(
                "Composite indexes support equality lookup only.");
    }

    public void readFrom(InputStream in) throws IOException {
        if (set_list.size() != 0) {
            throw new RuntimeException(
                    "Error reading scheme, already a set in the Scheme");
        }
        DataInputStream din = new DataInputStream(in);
        int vec_size = din.readInt();
        int row_count = getTable().getRowCount();
        if (row_count != vec_size) {
            throw new IOException(
                    "Different table row count to indices in scheme. " +
                            "table=" + row_count +
                            ", vec_size=" + vec_size);
        }
        for (int i = 0; i < vec_size; ++i) {
            int row = din.readInt();
            if (row < 0) {
                set_list = new BlockIntegerList();
                throw new IOException("Scheme contains out of table bounds index.");
            }
            set_list.add(row);
        }
    }

    public void writeTo(OutputStream out) throws IOException {
        DataOutputStream dout = new DataOutputStream(out);
        int list_size = set_list.size();
        dout.writeInt(list_size);
        IntegerIterator iterator = set_list.iterator(0, list_size - 1);
        while (iterator.hasNext()) {
            dout.writeInt(iterator.next());
        }
    }

    private CompositeKey keyForRow(int row) {
        TObject[] values = new TObject[columns.length];
        for (int i = 0; i < columns.length; ++i) {
            values[i] = getTable().getCellContents(columns[i], row);
        }
        return new CompositeKey(values);
    }

    private int compareRowToKey(int row, CompositeKey key) {
        for (int i = 0; i < columns.length; ++i) {
            TObject cell = getTable().getCellContents(columns[i], row);
            int result = cell.compareTo(key.values[i]);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    private int compareRows(int row1, int row2) {
        for (int column : columns) {
            TObject cell1 = getTable().getCellContents(column, row1);
            TObject cell2 = getTable().getCellContents(column, row2);
            int result = cell1.compareTo(cell2);
            if (result != 0) {
                return result;
            }
        }
        return 0;
    }

    private static final class CompositeKey {
        private final TObject[] values;

        private CompositeKey(TObject[] values) {
            this.values = values.clone();
        }
    }

}
