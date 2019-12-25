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

import com.pony.util.Cache;
import com.pony.util.IntegerVector;

/**
 * A TableBackedCache is a special type of a cache in a DataTableConglomerate
 * that is backed by a table in the database.  The purpose of this object is to
 * provide efficient access to some specific information in a table via a
 * cache.
 * <p>
 * For example, we use this object to provide cached access to the system
 * privilege tables.  The engine often performs identical types of priv
 * queries on the database and it's desirable to cache the access to this
 * table.
 * <p>
 * This class provides the following services;
 * 1) Allows for an instance of this object to be attached to a single
 *    DatabaseConnection
 * 2) Listens for any changes that are committed to the table(s) and flushes the
 *    cache as neccessary.
 * <p>
 * Note that this object is designed to fit into the pure serializable
 * transaction isolation system that Pony employs.  This object will provide
 * a view of the table as it was when the transaction started.  When the
 * transaction commits (or rollsback) the view is updated to the most current
 * version.  If a change is committed to the tables this cache is backed by,
 * the cache is only flushed when there are no open transactions on the
 * connection.
 *
 * @author Tobias Downer
 */

abstract class TableBackedCache {

    /**
     * The table that this cache is backed by.
     */
    private final TableName backed_by_table;

    /**
     * The list of added rows to the table above when a change is
     * committed.
     */
    private final IntegerVector added_list;

    /**
     * The list of removed rows from the table above when a change is
     * committed.
     */
    private final IntegerVector removed_list;

    /**
     * Set to true when the backing DatabaseConnection has a transaction open.
     */
    private boolean transaction_active;

    /**
     * The listener object.
     */
    private TransactionModificationListener listener;

    /**
     * Constructs this object.
     */
    protected TableBackedCache(TableName table) {
        this.backed_by_table = table;

        added_list = new IntegerVector();
        removed_list = new IntegerVector();
    }

    /**
     * Adds new row ids to the given list.
     */
    private void addRowsToList(int[] from, IntegerVector list) {
        if (from != null) {
            for (int i = 0; i < from.length; ++i) {
                list.addInt(from[i]);
            }
        }
    }

    /**
     * Attaches this object to a conglomerate.  This applies the appropriate
     * listeners to the tables.
     */
    final void attachTo(TableDataConglomerate conglomerate) {
//    TableDataConglomerate conglomerate = connection.getConglomerate();
        TableName table_name = backed_by_table;
        listener = new TransactionModificationListener() {
            public void tableChange(TableModificationEvent evt) {
                // Ignore.
            }

            public void tableCommitChange(TableCommitModificationEvent evt) {
                TableName table_name = evt.getTableName();
                if (table_name.equals(backed_by_table)) {
                    synchronized (removed_list) {
                        addRowsToList(evt.getAddedRows(), added_list);
                        addRowsToList(evt.getRemovedRows(), removed_list);
                    }
                }
            }
        };
        conglomerate.addTransactionModificationListener(table_name, listener);
    }

    /**
     * Call to detach this object from a TableDataConglomerate.
     */
    final void detatchFrom(TableDataConglomerate conglomerate) {
//    TableDataConglomerate conglomerate = connection.getConglomerate();
        TableName table_name = backed_by_table;
        conglomerate.removeTransactionModificationListener(table_name, listener);
    }

    /**
     * Called from DatabaseConnection to notify this object that a new transaction
     * has been started.  When a transaction has started, any committed changes
     * to the table must NOT be immediately reflected in this cache.  Only
     * when the transaction commits is there a possibility of the cache
     * information being incorrect.
     */
    final void transactionStarted() {
        transaction_active = true;
        internalPurgeCache();
    }

    /**
     * Called from DatabaseConnection to notify that object that a transaction
     * has closed.  When a transaction is closed, information in the cache may
     * be invalidated.  For example, if rows 10 - 50 were delete then any
     * information in the cache that touches this data must be flushed from the
     * cache.
     */
    final void transactionFinished() {
        transaction_active = false;
        internalPurgeCache();
    }

    /**
     * Internal method which copies the 'added' and 'removed' row lists and
     * calls the 'purgeCacheOfInvalidatedEntries' method.
     */
    private void internalPurgeCache() {
        // Make copies of the added_list and removed_list
        IntegerVector add, remove;
        synchronized (removed_list) {
            add = new IntegerVector(added_list);
            remove = new IntegerVector(removed_list);
            // Clear the added and removed list
            added_list.clear();
            removed_list.clear();
        }
        // Make changes to the cache
        purgeCacheOfInvalidatedEntries(add, remove);
    }

    /**
     * This method is called when the transaction starts and finishes and must
     * purge the cache of all invalidated entries.
     * <p>
     * Note that this method must NOT make any queries on the database.  It must
     * only, at the most, purge the cache of invalid entries.  A trivial
     * implementation of this might completely clear the cache of all data if
     * removed_row.size() > 0.
     */
    abstract void purgeCacheOfInvalidatedEntries(
            IntegerVector added_rows, IntegerVector removed_rows);

}

