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
 * An object that encapsulates all row modification information about a table
 * when a change to the table is about to be committed.  The object provides
 * information about what rows in the table were changed
 * (inserted/updated/deleted).
 *
 * @author Tobias Downer
 */

public class TableCommitModificationEvent {

    /**
     * A SimpleTransaction that can be used to query tables in the database -
     * the view of which will be the view when the transaction is committed.
     */
    private final SimpleTransaction transaction;

    /**
     * The name of the table that is being changed.
     */
    private final TableName table_name;

    /**
     * A normalized list of all rows that were added by the transaction being
     * committed.
     */
    private final int[] added_rows;

    /**
     * A normalized list of all rows that were removed by the transaction being
     * committed.
     */
    private final int[] removed_rows;

    /**
     * Constructs the event.
     */
    public TableCommitModificationEvent(SimpleTransaction transaction,
                                        TableName table_name, int[] added, int[] removed) {
        this.transaction = transaction;
        this.table_name = table_name;
        this.added_rows = added;
        this.removed_rows = removed;
    }

    /**
     * Returns the Transaction that represents the view of the database when
     * the changes to the table have been committed.
     */
    public SimpleTransaction getTransaction() {
        return transaction;
    }

    /**
     * Returns the name of the table.
     */
    public TableName getTableName() {
        return table_name;
    }

    /**
     * Returns the normalized list of all rows that were inserted or updated
     * in this table of the transaction being committed.  This is a normalized
     * list which means if a row is inserted and then deleted in the transaction
     * then it is not considered important and does not appear in this list.
     */
    public int[] getAddedRows() {
        return added_rows;
    }

    /**
     * Returns the normalized list of all rows that were deleted or updated
     * in this table of the transaction being committed.  This is a normalized
     * list which means if a row is inserted and then deleted in the transaction
     * then it is not considered important and does not appear in this list.
     */
    public int[] getRemovedRows() {
        return removed_rows;
    }

}
