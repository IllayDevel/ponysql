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
 * A mutable data source that allows for the addition and removal of rows.
 *
 * @author Tobias Downer
 */

public interface MutableTableDataSource extends TableDataSource {

    /**
     * Adds a row to the source.  This will add a permanent record into the
     * the underlying data structure.  It will also update the indexing
     * schemes as appropriate, and also add the row into the set returned by
     * the 'rowEnumeration' iterator.
     * <p>
     * It returns a row index that is used to reference this data in future
     * queries.  Throws an exception if the row additional was not possible
     * because of IO reasons.
     */
    int addRow(RowData row_data);

    /**
     * Completely removes a row from the source.  This will permanently remove
     * the record from the underlying data structure.  It also updates the
     * indexing schemes and removes the row index from the set returned by
     * the 'rowEnumeration' iterator.
     * <p>
     * Throws an exception if the row index does not reference a valid row within
     * the context of this data source.
     */
    void removeRow(int row_index);

    /**
     * Updates a row in the source.  This will make a permanent change to the
     * underlying data structure.  It will update the indexing schemes as
     * appropriate, and also add the row into the set returned by the
     * 'rowEnumeration' iterator.
     * <p>
     * It returns a row index for the new updated records.  Throws an exception
     * if the row update was not possible because of IO reasons or the row
     * index not being a valid reference to a record in this data source.
     */
    int updateRow(int row_index, RowData row_data);

    /**
     * Flushes all changes made on this MutableTableDataSource to the backing
     * index scheme (IndexSet).  This is used during the commit phase of this
     * objects lifetime.  The transaction control mechanism has found that there
     * are no clashes and now we need to commit the current table view to the
     * conglomerate.  Because this object may not update index information
     * immediately, we call this to flush all the changes to the table to the
     * backing index set.
     * <p>
     * When this method returns, the backing IndexSet of this view will be
     * completely up to date.
     */
    void flushIndexChanges();

    /**
     * Performs all constraint integrity checks and actions to any modifications
     * based on any changes that happened to the table since that last call to
     * this method.  It is important that is called after any call to 'addRow',
     * 'removeRow' or 'updateRow'.
     * <p>
     * Any constraints that are marked as INITIALLY_IMMEDIATE are checked when
     * this is called, otherwise the constraint is checked at commit time.
     * <p>
     * Any referential actions are performed when this method is called.  If a
     * referential action causes a modification to another table, this method
     * is recursively called on the table modified.
     * <p>
     * If a referential integrity constraint is violated and a referential action
     * is unable to maintain the integrity of the database, any changes made to
     * the table are reverted.
     */
    void constraintIntegrityCheck();

    /**
     * Returns a journal that details the changes to this data source since it
     * was created.  This method may return a 'null' object to denote that no
     * logging is being done.  If this returns a MasterTableJournal, then all
     * 'addRow' and 'removeRow' method calls and their relative order will be
     * described in this journal.
     */
    MasterTableJournal getJournal();

    /**
     * Disposes this table data source.  After this method is called, most use
     * of this object is undefined, except for the 'getCellContent' and
     * 'compareCellContent' methods which are valid provided the source is
     * under a root lock.
     */
    void dispose();

    /**
     * Puts this source under a 'root lock'.  A root lock means the root row
     * structure of this object must not change.  A root lock is obtained on
     * a table when a ResultSet keeps hold of an object outside the life of
     * the transaction that created the table.  It is important that the order
     * of the rows stays constant (committed deleted rows are not really
     * deleted and reused, etc) while a table holds at least 1 root lock.
     */
    void addRootLock();

    /**
     * Removes a root lock from this source.
     */
    void removeRootLock();

}
