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

import java.io.IOException;

/**
 * This interface handles the abstraction of retreiving information from a
 * database file.  It knows the fixed length of the data fields and can
 * deduce the topology of the file and retreive and store information to/
 * from it.
 * <p>
 * The callee of this interface must ensure that all calls to implementations
 * of this interface are sequential and not concurrent.  It is not expected
 * that implementations are thread safe.
 * <p>
 * See VariableSizeDataTableFile for an implementation of this interface.
 *
 * @author Tobias Downer
 */

interface DataTableFile extends TableDataSource {

    /**
     * Creates a new file of the given table.  The table is initialised and
     * contains 0 row entries.  If the table already exists in the database then
     * this will throw an exception.
     * <p>
     * On exit, the object will be initialised and loaded with the given table.
     *
     * @param def the definition of the table.
     */
    void create(DataTableDef def) throws IOException;

    /**
     * Updates a file of the given table.  If the table does not exist, then it
     * is created.  If the table already exists but is different, then the
     * existing table is modified to incorporate the new fields structure.
     * <p>
     * The DataTableFile must have previously been 'load(table_name)' before
     * this call.
     * <p>
     * Implementations of this method may choose to reorganise information that
     * the relational schemes are dependant on (the row order for example).  If
     * this method returns 'true' then we must also reindex the schemes.
     * <p>
     * <strong>NOTE:</strong> If the new format has columns that are not
     *   included in the new format then the columns are deleted.
     *
     * @param def the definition of the table.
     * @return true if the table topology has changed.
     */
    boolean update(DataTableDef def) throws IOException;

    /**
     * This is called periodically when this data table file requires some
     * maintenance.  It is recommended that this method is called every
     * time the table is initialized (loaded).
     * <p>
     * The DataTableFile must have previously been 'load(table_name)' before
     * this call.
     * <p>
     * This method may change the topology of the rows (delete rows that are
     * marked as deleted), therefore if the method returns true you need to
     * re-index the schemes.
     *
     * @return true if the table topology was changed.
     */
    boolean doMaintenance() throws IOException;

//  /**
//   * A recovery method that returns a DataTableDef object for this data
//   * table file that was last used in a call to 'create' or 'update'.  This
//   * information should be kept in a secondary table topology store but it
//   * is useful to keep this information in the data table file just incase
//   * something bad happens, or tables are moved to another database.
//   */
//  DataTableDef recoverLastDataTableDef() throws IOException;

    /**
     * Loads a previously created table.  A table can be loaded in read only
     * mode, in which case any methods that write to the DataTableFile will
     * throw an IOException.
     *
     * @param table_name the name of the table.
     * @param read_only if true then the table file is opened as read-only.
     */
    void load(String table_name, boolean read_only) throws IOException;

    /**
     * Shuts down the table.  This is called when the table is closed and the
     * resources it uses are to be freed back to the system.  This is called
     * as part of the database shut down procedure or called when we want to
     * free the resources associated with this table.
     */
    void shutdown() throws IOException;

    /**
     * Deletes the data table file in the file system.  This is used to clear
     * up resources after a table has been dropped.  The table must be shut
     * down before this method is called.
     * <p>
     * NOTE: Use this with care.  All data is lost!
     */
    void drop();

    /**
     * Flushes all information that may be cached in memory to disk.  This
     * includes any relational data, any cached data that hasn't made it to
     * the file system yet.  It will write out all persistant information
     * and leave the table in a state where it is fully represented in the
     * file system.
     */
    void updateFile() throws IOException;

    /**
     * Locks the data in the file to prevent the system overwritting entries
     * that have been marked as removed.  This is necessary so we may still
     * safely read removed entries from the table while the table is locked.
     */
    void addRowsLock();

    /**
     * Unlocks the data in the file to indicate that the system may safely
     * overwrite removed entries in the file.
     */
    void removeRowsLock();

    /**
     * Returns true if the file currently has all of its rows locked.
     */
    boolean hasRowsLocked();

//  /**
//   * The number of rows that are currently stored in this table.  This number
//   * does not include the rows that have been marked as removed.
//   */
//  int rowCount();

    /**
     * Returns true if the given row index points to a valid and available
     * row entry.  Returns false if the row entry has been marked as removed,
     * or the index goes outside the bounds of the table.
     */
    boolean isRowValid(int record_index) throws IOException;

    /**
     * Adds a complete new row into the table.  If the table is in a row locked
     * state, then this will always add a new entry to the end of the table.
     * Otherwise, new entries are added where entries were previously removed.
     * <p>
     * This will update any column indices that are set.
     *
     * @returns the raw row index of the row that was added.
     */
    int addRow(RowData row_data) throws IOException;

    /**
     * Removes a row from the table at the given index.  This will only mark
     * the entry as removed, and will not actually remove the data.  This is
     * because a process is allowed to read the data even after the row has been
     * marked as removed (if the rows have been locked).
     * <p>
     * This will update any column indices that are set.
     *
     * @param row_index the raw row index of the entry to be marked as removed.
     */
    void removeRow(int row_index) throws IOException;

//  /**
//   * Returns a DataCell object of the entry at the given column, row
//   * index in the table.  This will always work provided there was once data
//   * stored at that index, even if the row has been marked as deleted.
//   */
//  DataCell getCellAt(int column, int row) throws IOException;

    /**
     * Returns a unique number.  This is incremented each time it is accessed.
     */
    long nextUniqueKey() throws IOException;

}
