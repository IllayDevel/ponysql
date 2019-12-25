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

import com.pony.debug.*;

/**
 * This class manages a set of indices for a table over different versions.
 * The indices include the list of rows (required), and any index data
 * (optional).  This object manages table indexes at multiple revision levels.
 * When indexes are requested, what is returned is an isolated version of the
 * current indexes.  Index changes automatically create a new version and
 * each version of the index found is isolated from any concurrent changes.
 * <p>
 * This class is not thread safe, but it assumes thread safety by the
 * callee.  It is not safe for multi-threaded access.
 *
 * @author Tobias Downer
 */

final class MultiVersionTableIndices {

    /**
     * The name of the table.
     */
    private final TableName table_name;

    /**
     * The system object.
     */
    private final TransactionSystem system;


    /**
     * A list of MasterTableJournal objects that represent the changes
     * that have occurred to this master index after various transactions
     * have been committed.
     * <p>
     * This list can be used to build the indices and a table row enumerator for
     * snapshots of the table at various transaction check points.
     */
    private final ArrayList transaction_mod_list;


    // ---------- Stat keys ----------

    private final String journal_count_stat_key;

    /**
     * Constructs this object with the given number of column.
     */
    MultiVersionTableIndices(TransactionSystem system,
                             TableName table_name, int column_count) {
        this.system = system;
        this.table_name = table_name;
        /**
         * The number of columns in the referenced table.
         */

        transaction_mod_list = new ArrayList();

        journal_count_stat_key = "MultiVersionTableIndices.journal_entries." +
                table_name;

    }

    private long TS_merge_count = 0;
    private long TS_merge_size = 0;

    /**
     * Returns the DebugLogger object used to log debug messages.
     */
    public final DebugLogger Debug() {
        return system.Debug();
    }

    /**
     * Updates the master records from the journal logs up to the given
     * 'commit_id'.  This could be a fairly expensive operation if there are
     * a lot of modifications because each change could require a lookup
     * of records in the data source.
     * <p>
     * NOTE: It's extremely important that when this is called, there are no
     *  transactions open that are using the merged journal.  If there is, then
     *  a transaction may be able to see changes in a table that were made
     *  after the transaction started.
     * <p>
     * Returns true if all journal changes were merged.
     */
    boolean mergeJournalChanges(long commit_id) {

        // Average size of pending transactions when this method is called...
        ++TS_merge_count;
        TS_merge_size += transaction_mod_list.size();
        if ((TS_merge_count % 32) == 0) {
            system.stats().set(
                    (int) ((TS_merge_size * 1000000L) / TS_merge_count),
                    "MultiVersionTableIndices.average_journal_merge_mul_1000000");
//      DatabaseSystem.stats().set(
//          TS_merge_size / TS_merge_count,
//          "MultiVersionTableIndices.average_journal_merge");
//      DatabaseSystem.stats().set(
//          TS_merge_size,
//          "MultiVersionTableIndices.TS_merge_size");
//      DatabaseSystem.stats().set(
//          TS_merge_count,
//          "MultiVersionTableIndices.TS_merge_count");
        }

        int merge_count = 0;
        int size = transaction_mod_list.size();
        while (transaction_mod_list.size() > 0) {

            MasterTableJournal journal =
                    (MasterTableJournal) transaction_mod_list.get(0);

            if (commit_id > journal.getCommitID()) {

                ++merge_count;
                if (Debug().isInterestedIn(Lvl.INFORMATION)) {
                    Debug().write(Lvl.INFORMATION, this,
                            "Merging '" + table_name + "' journal: " + journal);
                }

                // Remove the top journal entry from the list.
                transaction_mod_list.remove(0);
                system.stats().decrement(journal_count_stat_key);

            } else { // If (commit_id <= journal.getCommitID())
                return false;
            }
        }

        return true;

    }

    /**
     * Returns a list of all MasterTableJournal objects that have been
     * successfully committed against this table that have an 'commit_id' that
     * is greater or equal to the given.
     * <p>
     * This is part of the conglomerate commit check phase and will be on a
     * commit_lock.
     */
    MasterTableJournal[] findAllJournalsSince(long commit_id) {

        ArrayList all_since = new ArrayList();

        int size = transaction_mod_list.size();
        for (int i = 0; i < size; ++i) {
            MasterTableJournal journal =
                    (MasterTableJournal) transaction_mod_list.get(i);
            long journal_commit_id = journal.getCommitID();
            // All journals that are greater or equal to the given commit id
            if (journal_commit_id >= commit_id) {
                all_since.add(journal);
            }
        }

        return (MasterTableJournal[])
                all_since.toArray(new MasterTableJournal[all_since.size()]);
    }

    /**
     * Adds a transaction journal to the list of modifications on the indices
     * kept here.
     */
    void addTransactionJournal(MasterTableJournal change) {
        transaction_mod_list.add(change);
        system.stats().increment(journal_count_stat_key);
    }

    /**
     * Returns true if this table has any journal modifications that have not
     * yet been incorporated into master index.
     */
    boolean hasTransactionChangesPending() {
//    System.out.println(transaction_mod_list);
        return transaction_mod_list.size() > 0;
    }

    /**
     * Returns a string describing the transactions pending on this table.
     */
    String transactionChangeString() {
        return transaction_mod_list.toString();
    }

}
