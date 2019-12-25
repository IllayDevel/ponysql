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

/**
 * A simple convenience interface for querying a MutableTableDataSource
 * instance.  This is used as a very lightweight interface for changing a
 * table.  It is most useful for internal low level users of a database
 * table which doesn't need the overhead of the Pony table hierarchy
 * mechanism.
 *
 * @author Tobias Downer
 */

public final class SimpleTableQuery {

    /**
     * The DataTableDef for this table.
     */
    private DataTableDef table_def;

    /**
     * The TableDataSource we are wrapping.
     */
    private TableDataSource table;

    /**
     * Constructs the SimpleTableQuery with the given MutableTableDataSource
     * object.
     */
    public SimpleTableQuery(TableDataSource in_table) {
//    in_table.addRootLock();
        this.table = in_table;
        this.table_def = table.getDataTableDef();
    }

    /**
     * Returns a RowEnumeration that is used to iterate through the entire list
     * of valid rows in the table.
     */
    public RowEnumeration rowEnumeration() {
        return table.rowEnumeration();
    }

    /**
     * Returns the total number of rows in this table.
     */
    public int getRowCount() {
        return table.getRowCount();
    }

    /**
     * Gets the TObject at the given cell in the table.
     * Note that the offset between one valid row and the next may not necessily
     * be 1.  It is possible for there to be gaps in the data.  For an iterator
     * that returns successive row indexes, use the 'rowEnumeration' method.
     */
    public TObject get(int column, int row) {
        return table.getCellContents(column, row);
    }

    /**
     * Finds the index of all the rows in the table where the given column is
     * equal to the given object.
     */
    public IntegerVector selectIndexesEqual(int column, TObject cell) {
        return table.getColumnScheme(column).selectEqual(cell);
    }

    /**
     * Finds the index of all the rows in the table where the given column is
     * equal to the given object.
     * <p>
     * We assume value is not null, and it is either a BigNumber to represent
     * a number, a String, a java.util.Date or a ByteLongObject.
     */
    public IntegerVector selectIndexesEqual(int column, Object value) {
        TType ttype = table_def.columnAt(column).getTType();
        TObject cell = new TObject(ttype, value);
        return selectIndexesEqual(column, cell);
    }

    /**
     * Finds the index of all the rows in the table where the given column is
     * equal to the given object for both of the clauses.  This implies an
     * AND for the two searches.
     */
    public IntegerVector selectIndexesEqual(int col1, TObject cell1,
                                            int col2, TObject cell2) {

        // All the indexes that equal the first clause
        IntegerVector ivec = table.getColumnScheme(col1).selectEqual(cell1);

        // From this, remove all the indexes that don't equals the second clause.
        int index = ivec.size() - 1;
        while (index >= 0) {
            // If the value in column 2 at this index is not equal to value then
            // remove it from the list and move to the next.
            if (get(col2, ivec.intAt(index)).compareTo(cell2) != 0) {
                ivec.removeIntAt(index);
            }
            --index;
        }

        return ivec;
    }

    /**
     * Finds the index of all the rows in the table where the given column is
     * equal to the given object for both of the clauses.  This implies an
     * AND for the two searches.
     * <p>
     * We assume value is not null, and it is either a BigNumber to represent
     * a number, a String, a java.util.Date or a ByteLongObject.
     */
    public IntegerVector selectIndexesEqual(int col1, Object val1,
                                            int col2, Object val2) {
        TType t1 = table_def.columnAt(col1).getTType();
        TType t2 = table_def.columnAt(col2).getTType();

        TObject cell1 = new TObject(t1, val1);
        TObject cell2 = new TObject(t2, val2);

        return selectIndexesEqual(col1, cell1, col2, cell2);
    }

    /**
     * Returns true if there is a single row in the table where the given column
     * is equal to the given value, otherwise returns false.  If there are 2 or
     * more rows an assertion exception is thrown.
     */
    public boolean existsSingle(int col, Object val) {
        IntegerVector ivec = selectIndexesEqual(col, val);
        if (ivec.size() == 0) {
            return false;
        } else if (ivec.size() == 1) {
            return true;
        } else {
            throw new Error("Assertion failed: existsSingle found multiple values.");
        }
    }

    /**
     * Assuming the table stores a key/value mapping, this returns the contents
     * of value_column for any rows where key_column is equal to the key_value.
     * An assertion exception is thrown if there is more than 2 rows that match
     * the key.  If no rows match the key then null is returned.
     */
    public Object getVar(int value_column, int key_column, Object key_value) {
        // All indexes in the table where the key value is found.
        IntegerVector ivec = selectIndexesEqual(key_column, key_value);
        if (ivec.size() > 1) {
            throw new Error("Assertion failed: getVar found multiple key values.");
        } else if (ivec.size() == 0) {
            // Key found so return the value
            return get(value_column, ivec.intAt(0));
        } else {
            // Key not found so return null
            return null;
        }
    }

    // ---------- Table mutable methods ---------

    /**
     * Adds a new key/value mapping in this table.  If the key already exists
     * the old key/value row is deleted first.  This method accepts two
     * arguments, the column that contains the key value, and an Object[] array
     * that is the list of cells to insert into the table.  The Object[] array
     * must be the size of the number of columns in this tbale.
     * <p>
     * NOTE: Change will come into effect globally at the next commit.
     * <p>
     * NOTE: This method must be assured of exlusive access to the table within
     *   the transaction.
     * <p>
     * NOTE: This only works if the given table implements MutableTableDataSource.
     */
    public void setVar(int key_column, Object[] vals) {
        // Cast to a MutableTableDataSource
        MutableTableDataSource mtable = (MutableTableDataSource) table;

        // All indexes in the table where the key value is found.
        IntegerVector ivec = selectIndexesEqual(key_column, vals[key_column]);
        if (ivec.size() > 1) {
            throw new Error("Assertion failed: setVar found multiple key values.");
        } else if (ivec.size() == 1) {
            // Remove the current key
            mtable.removeRow(ivec.intAt(0));
        }
        // Insert the new key
        RowData row_data = new RowData(table);
        for (int i = 0; i < table_def.columnCount(); ++i) {
            row_data.setColumnDataFromObject(i, vals[i]);
        }
        mtable.addRow(row_data);
    }

    /**
     * Deletes a single entry from the table where the given column equals the
     * given value.  If there are multiple values found an assertion exception
     * is thrown.  If a single value was found and deleted 'true' is returned
     * otherwise false.
     * <p>
     * NOTE: This only works if the given table implements MutableTableDataSource.
     */
    public boolean deleteSingle(int col, Object val) {
        // Cast to a MutableTableDataSource
        MutableTableDataSource mtable = (MutableTableDataSource) table;

        IntegerVector ivec = selectIndexesEqual(col, val);
        if (ivec.size() == 0) {
            return false;
        } else if (ivec.size() == 1) {
            mtable.removeRow(ivec.intAt(0));
            return true;
        } else {
            throw new Error("Assertion failed: deleteSingle found multiple values.");
        }
    }

    /**
     * Deletes all the given indexes in this table.
     * <p>
     * NOTE: This only works if the given table implements MutableTableDataSource.
     */
    public void deleteRows(IntegerVector list) {
        // Cast to a MutableTableDataSource
        MutableTableDataSource mtable = (MutableTableDataSource) table;

        for (int i = 0; i < list.size(); ++i) {
            mtable.removeRow(list.intAt(i));
        }
    }


    /**
     * Disposes this object and frees any resources associated with it.  This
     * should be called when the query object is no longer being used.
     */
    public void dispose() {
        if (table != null) {
//      table.removeRootLock();
            table = null;
        }
    }

    /**
     * To be save we call dispose from the finalize method.
     */
    public void finalize() {
        dispose();
    }

}
