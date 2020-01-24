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

/**
 * This class provides very limited access to a Table object.  The purpose of
 * this object is to define the functionality of a table when the root table(s)
 * are locked via the 'Table.lockRoot(int)' method, and when the Table is no
 * longer READ or WRITE locked via the 'LockingMechanism' system.  During these
 * conditions, the table is in a semi-volatile state, so this class provides
 * a safe way to access the table without having to worry about using some
 * functionality of Table which isn't supported at this time.
 *
 * @author Tobias Downer
 */

public final class TableAccessState {

    /**
     * The underlying Table object.
     */
    private final Table table;

    /**
     * Set to true when the table is first locked.
     */
    private boolean been_locked;

    /**
     * The Constructor.
     */
    TableAccessState(Table table) {
        this.table = table;
        been_locked = false;
    }

    /**
     * Returns the cell at the given row/column coordinates in the table.
     * This method is valid because it doesn't use any of the SelectableScheme
     * information in any of its parent tables which could change at any time
     * when there is no READ or WRITE lock on the table.
     */
    public TObject getCellContents(int column, int row) {
        return table.getCellContents(column, row);
    }

    /**
     * Returns the DataTableDef object that contains information on the columns
     * of the table.
     */
    public DataTableDef getDataTableDef() {
        return table.getDataTableDef();
    }

    /**
     * Returns the TableName of the given column of this table.  This, together
     * with 'getDataTableDef' is used to find the fully qualified name of a
     * column of the table.
     */
    public Variable getResolvedVariable(int column) {
        return table.getResolvedVariable(column);
    }

//  /**
//   * Returns the TableField object of the given column.
//   * This information is constant per table.
//   */
//  public TableField getFieldAt(int column) {
//    return table.getFieldAt(column);
//  }

//  /**
//   * Returns a fully resolved name of the given column.
//   */
//  public String getResolvedColumnName(int column) {
//    return table.getResolvedColumnName(column);
//  }

    /**
     * Locks the root rows of the table.
     * This method is a bit of a HACK - why should the contract include being
     * able to lock the root rows?
     * This method only permits the roots to be locked once.
     */
    public void lockRoot(int key) {
        if (!been_locked) {
            table.lockRoot(key);
            been_locked = true;
        }
    }

    /**
     * Unlocks the root rows of the table.
     */
    public void unlockRoot(int key) {
        if (been_locked) { // && table.hasRootsLocked()) {
            table.unlockRoot(key);
            been_locked = false;
        } else {
            throw new RuntimeException("The root rows aren't locked.");
        }
    }

}
