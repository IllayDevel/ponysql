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

/**
 * This class represents a temporary table that is built from data that is
 * not related to any underlying DataTable object from the database.
 * <p>
 * For example, an aggregate function generates data would be put into a
 * TemporaryTable.
 *
 * @author Tobias Downer
 */

public final class TemporaryTable extends DefaultDataTable {

    /**
     * The DataTableDef object that describes the columns in this table.
     */
    private final DataTableDef table_def;

    /**
     * A Vector that represents the storage of TObject[] arrays for each row
     * of the table.
     */
    private ArrayList table_storage;

    /**
     * The Constructor.
     */
    public TemporaryTable(Database database,
                          String name, DataTableColumnDef[] fields) {
        super(database);

        table_storage = new ArrayList();

        table_def = new DataTableDef();
        table_def.setTableName(new TableName(null, name));
        for (DataTableColumnDef field : fields) {
            table_def.addVirtualColumn(new DataTableColumnDef(field));
        }
        table_def.setImmutable();
    }

    /**
     * Constructs this TemporaryTable based on the fields from the given
     * Table object.
     */
    public TemporaryTable(String name, Table based_on) {
        super(based_on.getDatabase());

        table_def = new DataTableDef(based_on.getDataTableDef());
        table_def.setTableName(new TableName(null, name));
        table_def.setImmutable();
    }

    /**
     * Constructs this TemporaryTable based on the given Table object.
     */
    public TemporaryTable(DefaultDataTable based_on) {
        super(based_on.getDatabase());

        table_def = new DataTableDef(based_on.getDataTableDef());
        table_def.setImmutable();
    }



    /* ====== Methods that are only for TemporaryTable interface ====== */

    /**
     * Resolves the given column name (eg 'id' or 'Customer.id' or
     * 'APP.Customer.id') to a column in this table.
     */
    private Variable resolveToVariable(String col_name) {
        Variable partial = Variable.resolve(col_name);
        return partial;
//    return partial.resolveTableName(TableName.resolve(getName()));
    }

    /**
     * Creates a new row where cells can be inserted into.
     */
    public void newRow() {
        table_storage.add(new TObject[getColumnCount()]);
        ++row_count;
    }

    /**
     * Sets the cell in the given column / row to the given value.
     */
    public void setRowCell(TObject cell, int column, int row) {
        TObject[] cells = (TObject[]) table_storage.get(row);
        cells[column] = cell;
    }

    /**
     * Sets the cell in the column of the last row of this table to the given
     * TObject.
     */
    public void setRowCell(TObject cell, String col_name) {
        Variable v = resolveToVariable(col_name);
        setRowCell(cell, findFieldName(v), row_count - 1);
    }

    /**
     * Sets the cell in the column of the last row of this table to the given
     * TObject.
     */
    public void setRowObject(TObject ob, int col_index, int row) {
        setRowCell(ob, col_index, row);
    }

    /**
     * Sets the cell in the column of the last row of this table to the given
     * TObject.
     */
    public void setRowObject(TObject ob, String col_name) {
        Variable v = resolveToVariable(col_name);
        setRowObject(ob, findFieldName(v));
    }

    /**
     * Sets the cell in the column of the last row of this table to the given
     * TObject.
     */
    public void setRowObject(TObject ob, int col_index) {
        setRowObject(ob, col_index, row_count - 1);
    }

    /**
     * Copies the cell from the given table (src_col, src_row) to the last row
     * of the column specified of this table.
     */
    public void setCellFrom(Table table, int src_col, int src_row,
                            String to_col) {
        Variable v = resolveToVariable(to_col);
        TObject cell = table.getCellContents(src_col, src_row);
        setRowCell(cell, findFieldName(v), row_count - 1);
    }

    /**
     * Copies the contents of the row of the given Table onto the end of this
     * table.  Only copies columns that exist in both tables.
     */
    public void copyFrom(Table table, int row) {
        newRow();

        Variable[] vars = new Variable[table.getColumnCount()];
        for (int i = 0; i < vars.length; ++i) {
            vars[i] = table.getResolvedVariable(i);
        }

        for (int i = 0; i < getColumnCount(); ++i) {
            Variable v = getResolvedVariable(i);
            String col_name = v.getName();
            try {
                int tcol_index = -1;
                for (int n = 0; n < vars.length || tcol_index == -1; ++n) {
                    if (vars[n].getName().equals(col_name)) {
                        tcol_index = n;
                    }
                }
                setRowCell(table.getCellContents(tcol_index, row), i, row_count - 1);
            } catch (Exception e) {
                Debug().writeException(e);
                throw new Error(e.getMessage());
            }
        }

    }


    /**
     * This should be called if you want to perform table operations on this
     * TemporaryTable.  It should be called *after* all the rows have been set.
     * It generates SelectableScheme object which sorts the columns of the table
     * and lets us execute Table operations on this table.
     * NOTE: After this method is called, the table must not change in any way.
     */
    public void setupAllSelectableSchemes() {
        blankSelectableSchemes(1);   // <- blind search
        for (int row_number = 0; row_number < row_count; ++row_number) {
            addRowToColumnSchemes(row_number);
        }
    }

    /* ====== Methods that are implemented for Table interface ====== */

    public DataTableDef getDataTableDef() {
        return table_def;
    }

    /**
     * Returns an object that represents the information in the given cell
     * in the table.  This can be used to obtain information about the given
     * table cells.
     */
    public TObject getCellContents(int column, int row) {
        TObject[] cells = (TObject[]) table_storage.get(row);
        TObject cell = cells[column];
        if (cell == null) {
            throw new Error("NULL cell!  (" + column + ", " + row + ")");
        }
        return cell;
    }

    /**
     * Returns an Enumeration of the rows in this table.
     * Each call to 'nextRowIndex' returns the next valid row index in the table.
     */
    public RowEnumeration rowEnumeration() {
        return new SimpleRowEnumeration(row_count);
    }

    /**
     * Adds a DataTableListener to the DataTable objects at the root of this
     * table tree hierarchy.  If this table represents the join of a number of
     * tables then the DataTableListener is added to all the DataTable objects
     * at the root.
     * <p>
     * A DataTableListener is notified of all modifications to the raw entries
     * of the table.  This listener can be used for detecting changes in VIEWs,
     * for triggers or for caching of common queries.
     */
    void addDataTableListener(DataTableListener listener) {
        // Nothing to be notified on with a Temporary table...
    }

    /**
     * Removes a DataTableListener from the DataTable objects at the root of
     * this table tree hierarchy.  If this table represents the join of a
     * number of tables, then the DataTableListener is removed from all the
     * DataTable objects at the root.
     */
    void removeDataTableListener(DataTableListener listener) {
        // No listeners can be in a TemporaryTable.
    }

    /**
     * Locks the root table(s) of this table so that it is impossible to
     * overwrite the underlying rows that may appear in this table.
     * This is used when cells in the table need to be accessed 'outside' the
     * lock.  So we may have late access to cells in the table.
     * 'lock_key' is a given key that will also unlock the root table(s).
     * NOTE: This is nothing to do with the 'LockingMechanism' object.
     */
    public void lockRoot(int lock_key) {
        // We don't need to do anything for temporary tables, because they have
        // no root to lock.
    }

    /**
     * Unlocks the root tables so that the underlying rows may
     * once again be used if they are not locked and have been removed.  This
     * should be called some time after the rows have been locked.
     */
    public void unlockRoot(int lock_key) {
        // We don't need to do anything for temporary tables, because they have
        // no root to unlock.
    }

    /**
     * Returns true if the table has its row roots locked (via the lockRoot(int)
     * method.
     */
    public boolean hasRootsLocked() {
        // A temporary table _always_ has its roots locked.
        return true;
    }


    // ---------- Static convenience methods ----------

    /**
     * Creates a table with a single column with the given name and type.
     */
    static TemporaryTable singleColumnTable(Database database,
                                            String col_name, Class c) {
        TType ttype = TType.fromClass(c);
        DataTableColumnDef col_def = new DataTableColumnDef();
        col_def.setName(col_name);
        col_def.setFromTType(ttype);
        TemporaryTable table = new TemporaryTable(database, "single",
                new DataTableColumnDef[]{col_def});

//      int type = TypeUtil.toDBType(c);
//      TableField[] fields =
//                 { new TableField(col_name, type, Integer.MAX_VALUE, false) };
//      TemporaryTable table = new TemporaryTable(database, "single", fields);
        return table;
    }

}
