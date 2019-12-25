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

/**
 * An interface that allows for the inspection and repair of the raw data
 * in a file.  This is used for table debugging and the repair of damaged
 * files.
 *
 * @author Tobias Downer
 */

public interface RawDiagnosticTable {

    /**
     * Statics that represent the various states of a record.
     */
    int UNCOMMITTED = 1,
            COMMITTED_ADDED = 2,
            COMMITTED_REMOVED = 3,
            DELETED = 4;     // ie. available for reclaimation.

    /**
     * Denotes an erroneous record state.
     */
    int RECORD_STATE_ERROR = 0;

    // ---------- Query Methods ----------

    /**
     * Returns the number of physical records in the table.  This includes
     * records that are uncommitted, deleted, committed removed and committed
     * added.
     */
    int physicalRecordCount();

    /**
     * Returns the DataTableDef object that describes the logical topology of
     * the columns in this table.
     */
    DataTableDef getDataTableDef();

    /**
     * Returns the state of the given record index.  The state of a row is
     * either UNCOMMITTED, COMMITTED ADDED, COMMITTED REMOVED or DELETED.
     * record_index should be between 0 and physicalRecordCount.
     */
    int recordState(int record_index);

    /**
     * The number of bytes the record takes up on the underlying media.
     */
    int recordSize(int record_index);

    /**
     * Returns the contents of the given cell in this table.  If the system is
     * unable to return a valid cell then an exception is thrown.
     */
    TObject getCellContents(int column, int record_index);

    /**
     * Returns any misc information regarding this row as a human readable
     * string.  May return null if there is no misc information associated with
     * this record.
     */
    String recordMiscInformation(int record_index);

}
